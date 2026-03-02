"""Feishu wiki reader, markdown exporter, and OAuth token management.

Provides authenticated access to the Feishu Open API for reading wiki pages,
converting docx blocks to markdown, and exporting pages with images.

Usage as CLI:
    python feishu_wiki.py <wiki_url_or_token> [-o output_dir]

Usage as library:
    from feishu_wiki import get_valid_user_token, get_wiki_node, get_all_blocks
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import time
import urllib.parse
import zipfile

import requests
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

BASE_URL = "https://open.feishu.cn"
REDIRECT_URI = "http://localhost:7777/callback"
TOKEN_CACHE_FILE = os.path.join(os.path.dirname(__file__) or ".", ".feishu_token_cache.json")


# ── Token cache ──────────────────────────────────────────────────────────────


def load_cached_token() -> str | None:
    """Load access token from cache if not expired."""
    if not os.path.exists(TOKEN_CACHE_FILE):
        return None
    with open(TOKEN_CACHE_FILE) as f:
        cache = json.load(f)
    if time.time() < cache.get("expires_at", 0):
        return cache["access_token"]
    return None


def save_token_cache(
    access_token: str, expires_in: int, refresh_token: str | None = None,
) -> None:
    """Cache token with expiration (60s safety buffer)."""
    cache: dict = {
        "access_token": access_token,
        "expires_at": time.time() + expires_in - 60,
    }
    if refresh_token:
        cache["refresh_token"] = refresh_token
    with open(TOKEN_CACHE_FILE, "w") as f:
        json.dump(cache, f)


def load_refresh_token() -> str | None:
    if not os.path.exists(TOKEN_CACHE_FILE):
        return None
    with open(TOKEN_CACHE_FILE) as f:
        return json.load(f).get("refresh_token")


# ── Request helpers ──────────────────────────────────────────────────────────

_REQUEST_TIMEOUT = 30  # seconds
_MAX_RETRIES = 5
_INITIAL_BACKOFF = 1  # seconds
_RATE_LIMIT_CODES = {99991400, 99991429}


def _api_request(method: str, url: str, **kwargs) -> requests.Response:
    """HTTP request with timeout, retry on connection errors and rate limits."""
    kwargs.setdefault("timeout", _REQUEST_TIMEOUT)
    backoff = _INITIAL_BACKOFF
    last_exc = None
    for attempt in range(_MAX_RETRIES):
        try:
            resp = requests.request(method, url, **kwargs)
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_exc = exc
            if attempt < _MAX_RETRIES - 1:
                log.warning("Request failed (%s), retrying in %ds …", exc, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            raise
        # Retry on HTTP 429
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", backoff))
            log.debug("Rate limited (HTTP 429), retrying in %ds …", retry_after)
            time.sleep(retry_after)
            backoff = min(backoff * 2, 30)
            continue
        # Retry on Feishu rate-limit error codes
        try:
            data = resp.json()
            if data.get("code") in _RATE_LIMIT_CODES:
                log.debug("Rate limited (code=%d), retrying in %ds …", data["code"], backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
        except ValueError:
            pass
        return resp
    return resp  # return last response even if still rate-limited


# ── Feishu API ───────────────────────────────────────────────────────────────


def get_app_access_token(app_id: str, app_secret: str) -> str:
    resp = _api_request(
        "POST", f"{BASE_URL}/open-apis/auth/v3/app_access_token/internal",
        json={"app_id": app_id, "app_secret": app_secret},
    )
    data = resp.json()
    if data.get("code") != 0:
        raise Exception(f"Failed to get app token: {data}")
    return data["app_access_token"]


def get_user_token_by_code(
    app_access_token: str, code: str,
) -> tuple[str, int, str | None]:
    resp = _api_request(
        "POST", f"{BASE_URL}/open-apis/authen/v1/access_token",
        json={
            "app_access_token": app_access_token,
            "code": code,
            "grant_type": "authorization_code",
        },
    )
    data = resp.json()
    if data.get("code") != 0:
        raise Exception(f"Failed to get user token: {data}")
    d = data["data"]
    return d["access_token"], d.get("expires_in", 7200), d.get("refresh_token")


def refresh_user_token(
    app_access_token: str, refresh_token: str,
) -> tuple[str | None, int | None, str | None]:
    resp = _api_request(
        "POST", f"{BASE_URL}/open-apis/authen/v1/refresh_access_token",
        json={
            "app_access_token": app_access_token,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
    )
    data = resp.json()
    if data.get("code") != 0:
        return None, None, None
    d = data["data"]
    return d["access_token"], d.get("expires_in", 7200), d.get("refresh_token")


def get_wiki_node(user_token: str, node_token: str) -> dict:
    """Fetch wiki node metadata (title, obj_token, space_id, etc.)."""
    resp = _api_request(
        "GET", f"{BASE_URL}/open-apis/wiki/v2/spaces/get_node",
        headers={"Authorization": f"Bearer {user_token}"},
        params={"token": node_token},
    )
    data = resp.json()
    if data.get("code") != 0:
        raise Exception(f"Failed to get wiki node: {data}")
    return data["data"]["node"]


def get_all_blocks(user_token: str, document_id: str) -> list[dict]:
    """Fetch all blocks from a docx document (paginated)."""
    blocks: list[dict] = []
    page_token = None
    while True:
        params: dict = {"page_size": 500}
        if page_token:
            params["page_token"] = page_token
        resp = _api_request(
            "GET", f"{BASE_URL}/open-apis/docx/v1/documents/{document_id}/blocks",
            headers={"Authorization": f"Bearer {user_token}"},
            params=params,
        )
        data = resp.json()
        if data.get("code") != 0:
            raise Exception(f"Failed to get blocks: {data}")
        blocks.extend(data["data"]["items"])
        if not data["data"].get("has_more"):
            break
        page_token = data["data"].get("page_token")
    return blocks


def get_wiki_children(
    user_token: str, space_id: str, parent_node_token: str,
) -> list[dict]:
    """List child nodes under a wiki node."""
    items: list[dict] = []
    page_token = None
    while True:
        params: dict = {"parent_node_token": parent_node_token, "page_size": 50}
        if page_token:
            params["page_token"] = page_token
        resp = _api_request(
            "GET", f"{BASE_URL}/open-apis/wiki/v2/spaces/{space_id}/nodes",
            headers={"Authorization": f"Bearer {user_token}"},
            params=params,
        )
        data = resp.json()
        if data.get("code") != 0:
            raise Exception(f"Failed to get wiki children: {data}")
        items.extend(data["data"].get("items", []))
        if not data["data"].get("has_more"):
            break
        page_token = data["data"].get("page_token")
    return items


# ── Markdown conversion ──────────────────────────────────────────────────────

# Feishu code block language ID → markdown fence name
_LANG_MAP = {
    1: "", 7: "bash", 8: "csharp", 9: "c", 10: "cpp", 11: "clojure",
    12: "coffeescript", 13: "css", 18: "dockerfile", 24: "go",
    25: "groovy", 26: "html", 28: "haskell", 29: "json", 30: "java",
    31: "javascript", 32: "julia", 33: "kotlin", 34: "latex", 35: "lisp",
    37: "lua", 38: "matlab", 39: "makefile", 40: "markdown", 41: "nginx",
    42: "objc", 44: "php", 45: "plantuml", 46: "powershell", 48: "protobuf",
    49: "python", 50: "r", 52: "ruby", 53: "rust", 55: "scala",
    56: "scss", 57: "bash", 58: "sql", 59: "swift", 61: "typescript",
    65: "xml", 66: "yaml",
}


def _render_elements(elements: list[dict], raw: bool = False) -> str:
    """Convert a list of Feishu text elements to a markdown string."""
    parts: list[str] = []
    for elem in elements:
        if "text_run" in elem:
            run = elem["text_run"]
            text = run.get("content", "")
            if not raw:
                style = run.get("text_element_style", {})
                link_url = urllib.parse.unquote(style.get("link", {}).get("url", ""))
                if style.get("bold"):
                    text = f"**{text}**"
                if style.get("italic"):
                    text = f"*{text}*"
                if style.get("inline_code"):
                    text = f"`{text}`"
                if style.get("strikethrough"):
                    text = f"~~{text}~~"
                if link_url:
                    text = f"[{text}]({link_url})"
            parts.append(text)
        elif "mention_doc" in elem:
            doc = elem["mention_doc"]
            title = doc.get("title", "")
            url = doc.get("url", "")
            parts.append(f"[{title}]({url})" if url else title)
        elif "equation" in elem:
            content = elem["equation"].get("content", "")
            if content:
                parts.append(f" ${content}$ ")
        elif "mention_user" in elem:
            user_id = elem["mention_user"].get("user_id", "user")
            parts.append(f"@{user_id}")
    return "".join(parts)


def blocks_to_markdown(blocks: list[dict], title: str = "") -> str:
    """Convert a flat list of Feishu docx blocks to markdown text."""
    block_map = {b["block_id"]: b for b in blocks}
    root = next((b for b in blocks if b["block_type"] == 1), None)
    if not root:
        return ""

    img_counter = [0]

    def render(block_id: str, prefix: str = "") -> list[str]:
        b = block_map.get(block_id)
        if not b:
            return []
        bt = b["block_type"]
        lines: list[str] = []

        if bt == 1:  # Page root
            if title:
                lines += [f"# {title}", ""]
            for cid in b.get("children", []):
                lines += render(cid)

        elif bt == 2:  # Paragraph
            text = _render_elements(b.get("text", {}).get("elements", []))
            lines += [prefix + text, ""]

        elif 3 <= bt <= 11:  # Heading 1-9
            level = min(bt - 2, 6)
            key = f"heading{bt - 2}"
            hb = b.get(key, {})
            text = _render_elements(hb.get("elements", []))
            lines += ["", prefix + "#" * level + " " + text, ""]

        elif bt == 12:  # Bullet
            text = _render_elements(b.get("bullet", {}).get("elements", []))
            lines.append(prefix + "- " + text)
            for cid in b.get("children", []):
                lines += render(cid, prefix=prefix + "  ")

        elif bt == 13:  # Ordered list
            text = _render_elements(b.get("ordered", {}).get("elements", []))
            lines.append(prefix + "1. " + text)
            for cid in b.get("children", []):
                lines += render(cid, prefix=prefix + "   ")

        elif bt == 14:  # Code block
            lang_id = b.get("code", {}).get("style", {}).get("language", 0)
            lang = _LANG_MAP.get(lang_id, "")
            code = _render_elements(b.get("code", {}).get("elements", []), raw=True)
            lines += ["", f"```{lang}", code.rstrip("\n"), "```", ""]

        elif bt == 15:  # Quote
            text = _render_elements(b.get("quote", {}).get("elements", []))
            if text:
                lines.append(prefix + "> " + text)
            for cid in b.get("children", []):
                for line in render(cid, prefix=""):
                    lines.append(prefix + "> " + line if line.strip() else prefix + ">")
            lines.append("")

        elif bt == 16:  # Block equation
            eq = b.get("equation", {}).get("content", "").rstrip()
            lines += ["", "$$", eq, "$$", ""]

        elif bt == 17:  # Todo
            todo = b.get("todo", {})
            text = _render_elements(todo.get("elements", []))
            done = todo.get("style", {}).get("done", False)
            checkbox = "[x]" if done else "[ ]"
            lines.append(prefix + f"- {checkbox} " + text)
            for cid in b.get("children", []):
                lines += render(cid, prefix=prefix + "  ")

        elif bt == 19:  # Callout
            emoji = b.get("callout", {}).get("emoji_id", "")
            if emoji:
                lines.append(f"> **:{emoji}:**")
            else:
                lines.append(">")
            for cid in b.get("children", []):
                for line in render(cid, prefix=""):
                    lines.append("> " + line if line.strip() else ">")
            lines.append("")

        elif bt == 22:  # Divider
            lines += ["", "---", ""]

        elif bt == 27:  # Image
            img_counter[0] += 1
            lines += [f"![image_{img_counter[0]}](images/image_{img_counter[0]}.png)", ""]

        elif bt in (24, 25):  # Grid / GridColumn — transparent, recurse
            for cid in b.get("children", []):
                lines += render(cid, prefix=prefix)

        else:  # Fallback: recurse into children
            for cid in b.get("children", []):
                lines += render(cid, prefix=prefix)

        return lines

    return "\n".join(render(root["block_id"]))


# ── Image export ─────────────────────────────────────────────────────────────


def export_docx_and_extract_images(
    user_token: str, obj_token: str, img_dir: str,
) -> list[str]:
    """Export a Feishu docx as .docx file, extract images into img_dir."""
    # 1. Create export task
    resp = _api_request(
        "POST", f"{BASE_URL}/open-apis/drive/v1/export_tasks",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"file_extension": "docx", "token": obj_token, "type": "docx"},
    )
    data = resp.json()
    if data.get("code") != 0:
        raise Exception(f"Export task creation failed: {data}")
    ticket = data["data"]["ticket"]

    # 2. Poll until done
    file_token = None
    for _ in range(20):
        time.sleep(2)
        r = _api_request(
            "GET", f"{BASE_URL}/open-apis/drive/v1/export_tasks/{ticket}",
            headers={"Authorization": f"Bearer {user_token}"},
            params={"token": obj_token},
        )
        result = r.json().get("data", {}).get("result", {})
        status = result.get("job_status")
        if status == 0:
            file_token = result["file_token"]
            break
        elif status == 3:
            raise Exception(f"Export task failed: {r.text}")
    if not file_token:
        raise Exception("Export timed out")

    # 3. Download the exported .docx file
    r = _api_request(
        "GET", f"{BASE_URL}/open-apis/drive/v1/export_tasks/{ticket}/file",
        headers={"Authorization": f"Bearer {user_token}"},
        params={"token": obj_token},
        stream=True,
        timeout=60,
    )
    if r.status_code != 200:
        raise Exception(f"Export file download failed [{r.status_code}]")

    tmp_docx = os.path.join(img_dir, "_export.docx")
    os.makedirs(img_dir, exist_ok=True)
    with open(tmp_docx, "wb") as f:
        for chunk in r.iter_content(8192):
            f.write(chunk)

    # 4. Extract images from the .docx zip (word/media/)
    extracted: list[str] = []
    with zipfile.ZipFile(tmp_docx) as z:
        media_files = sorted(
            [n for n in z.namelist() if n.startswith("word/media/")],
        )
        for i, name in enumerate(media_files, 1):
            ext = os.path.splitext(name)[1] or ".png"
            out_path = os.path.join(img_dir, f"image_{i}{ext}")
            with z.open(name) as src, open(out_path, "wb") as dst:
                dst.write(src.read())
            extracted.append(out_path)

    os.remove(tmp_docx)
    return extracted


# ── Auth flow ────────────────────────────────────────────────────────────────


def do_oauth_login(app_id: str) -> str:
    """Prompt user for OAuth authorization code."""
    auth_url = (
        f"{BASE_URL}/open-apis/authen/v1/index"
        f"?app_id={app_id}&redirect_uri={REDIRECT_URI}&state=feishu_wiki"
    )
    print("\n1. Open this URL in your browser:")
    print(f"\n   {auth_url}\n")
    print("2. Log in to Feishu.")
    print("3. You'll be redirected to a URL like:")
    print("     http://localhost:7777/callback?code=XXXX&state=...")
    print("4. Copy the 'code' value from that URL and paste it below.\n")
    return input("Paste the code here: ").strip()


def get_valid_user_token(app_id: str, app_secret: str) -> str:
    """Get a valid user token, refreshing or re-authorizing as needed."""
    # 1. Try cached access token
    token = load_cached_token()
    if token:
        log.info("Using cached token.")
        return token

    app_token = get_app_access_token(app_id, app_secret)

    # 2. Try refreshing with saved refresh_token
    rt = load_refresh_token()
    if rt:
        log.info("Refreshing token...")
        token, expires_in, new_refresh = refresh_user_token(app_token, rt)
        if token and expires_in:
            save_token_cache(token, expires_in, new_refresh)
            log.info("Token refreshed.")
            return token

    # 3. Full OAuth login
    code = do_oauth_login(app_id)
    token, expires_in, refresh_token = get_user_token_by_code(app_token, code)
    save_token_cache(token, expires_in, refresh_token)
    log.info("Token obtained and cached.")
    return token


# ── Wiki download ────────────────────────────────────────────────────────────


def _safe_name(title: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "_", title).strip() or "untitled"


def _parse_node_token(url_or_token: str) -> str:
    """Extract node token from a Feishu wiki URL or return as-is."""
    m = re.search(r"/wiki/([A-Za-z0-9]+)", url_or_token)
    if m:
        return m.group(1)
    return url_or_token.strip().split("/")[-1].split("?")[0]


def download_wiki_node(
    user_token: str, node_token: str, out_dir: str, depth: int = 0,
) -> None:
    """Download a wiki node (and children recursively) as markdown + images."""
    indent = "  " * depth
    node = get_wiki_node(user_token, node_token)
    title = node.get("title", "untitled")
    obj_token = node.get("obj_token")
    obj_type = node.get("obj_type")
    has_child = node.get("has_child", False)
    space_id = node.get("space_id")

    log.info("%s[%s] (type=%s)", indent, title, obj_type)

    node_dir = os.path.join(out_dir, _safe_name(title))
    os.makedirs(node_dir, exist_ok=True)

    if obj_type == "docx" and obj_token:
        try:
            blocks = get_all_blocks(user_token, obj_token)
        except Exception as e:
            log.warning("%s  Failed to fetch blocks: %s", indent, e)
            blocks = []

        if blocks:
            try:
                content = blocks_to_markdown(blocks, title=title)
                md_path = os.path.join(node_dir, f"{_safe_name(title)}.md")
                with open(md_path, "w", encoding="utf-8") as f:
                    f.write(content)
                log.info("%s  Saved: %s", indent, md_path)
            except Exception as e:
                log.warning("%s  Markdown conversion failed: %s", indent, e)

        has_images = any(b.get("block_type") == 27 for b in blocks)
        if has_images:
            img_dir = os.path.join(node_dir, "images")
            try:
                paths = export_docx_and_extract_images(user_token, obj_token, img_dir)
                log.info("%s  Images: %d extracted via export", indent, len(paths))
            except Exception as e:
                msg = str(e)
                if "{" in msg:
                    msg = msg[:msg.index("{")].strip()
                log.warning("%s  Images skipped: %s", indent, msg)

    if has_child and space_id:
        try:
            children = get_wiki_children(user_token, space_id, node_token)
            for child in children:
                download_wiki_node(user_token, child["node_token"], node_dir, depth + 1)
        except Exception as e:
            log.warning("%s  Children failed: %s", indent, e)


# ── Main ─────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export a Feishu wiki page to markdown with images",
    )
    parser.add_argument("source", help="Wiki URL or node token to export")
    parser.add_argument(
        "-o", "--output", default="wiki_output",
        help="Output directory (default: wiki_output)",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Verbose output",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(message)s",
    )

    app_id = os.getenv("FEISHU_APP_ID")
    app_secret = os.getenv("FEISHU_APP_SECRET")
    if not app_id or not app_secret:
        print("Set FEISHU_APP_ID and FEISHU_APP_SECRET in .env")
        raise SystemExit(1)

    node_token = _parse_node_token(args.source)
    user_token = get_valid_user_token(app_id, app_secret)

    os.makedirs(args.output, exist_ok=True)
    log.info("Exporting wiki node %s → %s", node_token, args.output)
    download_wiki_node(user_token, node_token, args.output)
    log.info("Done.")


if __name__ == "__main__":
    main()
