#!/usr/bin/env python3
"""Copy full content of a Feishu wiki page to a new wiki page.

Creates a new child wiki page under the target node and copies all blocks
from the source page, preserving text, headings, lists, code, images,
tables, and layout containers.

Usage:
    python feishu_copy_page.py <source_url> <target_url> [--title "Custom Title"]
    python feishu_copy_page.py <source_url> <target_url> --recursive

Examples:
    python feishu_copy_page.py https://my.feishu.cn/wiki/ABC123 https://my.feishu.cn/wiki/DEF456
    python feishu_copy_page.py ABC123 DEF456 --title "My Copy"
    python feishu_copy_page.py ABC123 DEF456 --recursive

Environment variables (via .env):
    FEISHU_APP_ID      - Feishu app ID
    FEISHU_APP_SECRET  - Feishu app secret
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from dotenv import load_dotenv

from feishu_wiki import (
    BASE_URL,
    get_all_blocks,
    get_valid_user_token,
    get_wiki_children,
    get_wiki_node,
)

try:
    from playwright.sync_api import sync_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

load_dotenv()

log = logging.getLogger(__name__)

# block_type → content key in the block dict
_CONTENT_KEY = {
    2: "text",
    3: "heading1", 4: "heading2", 5: "heading3", 6: "heading4",
    7: "heading5", 8: "heading6", 9: "heading7", 10: "heading8", 11: "heading9",
    12: "bullet", 13: "ordered", 14: "code", 15: "quote",
    16: "equation", 17: "todo", 18: "table", 19: "callout", 22: "divider",
    24: "grid", 25: "grid_column", 27: "image",
}


def _parse_node_token(url_or_token: str) -> str:
    """Extract node token from a Feishu wiki URL or return as-is."""
    m = re.search(r"/wiki/([A-Za-z0-9]+)", url_or_token)
    if m:
        return m.group(1)
    return url_or_token.strip().split("/")[-1].split("?")[0]


# ── Token refresh helper ────────────────────────────────────────────────────


class TokenManager:
    """Wraps user token with automatic refresh before expiry."""

    def __init__(self, app_id: str, app_secret: str) -> None:
        self._app_id = app_id
        self._app_secret = app_secret
        self._token: str | None = None
        self._obtained_at: float = 0

    def get(self) -> str:
        """Return a valid user token, refreshing if needed."""
        # Refresh if token is older than 90 minutes (tokens last ~2h)
        if self._token and (time.time() - self._obtained_at) < 5400:
            return self._token
        self._token = get_valid_user_token(self._app_id, self._app_secret)
        self._obtained_at = time.time()
        return self._token


# ── Rate-limit retry ─────────────────────────────────────────────────────────

# Feishu rate-limit error codes (99991400 = frequency limit, 99991429 = too many requests)
_RATE_LIMIT_CODES = {99991400, 99991429}
_MAX_RETRIES = 5
_INITIAL_BACKOFF = 1  # seconds


def _request_with_retry(method: str, url: str, **kwargs) -> requests.Response:
    """Make an HTTP request with automatic retry on rate-limit errors."""
    backoff = _INITIAL_BACKOFF
    for attempt in range(_MAX_RETRIES):
        resp = requests.request(method, url, **kwargs)
        # Retry on HTTP 429
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", backoff))
            log.debug("  Rate limited (HTTP 429), retrying in %ds …", retry_after)
            time.sleep(retry_after)
            backoff = min(backoff * 2, 30)
            continue
        # Retry on Feishu rate-limit error codes
        try:
            data = resp.json()
            if data.get("code") in _RATE_LIMIT_CODES:
                log.debug("  Rate limited (code=%d), retrying in %ds …", data["code"], backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
        except ValueError:
            pass
        return resp
    return resp  # return last response even if still rate-limited


# ── API helpers ──────────────────────────────────────────────────────────────


def create_document(user_token: str, title: str) -> str:
    """Create a standalone Feishu document (fallback). Returns document_id."""
    resp = _request_with_retry(
        "POST", f"{BASE_URL}/open-apis/docx/v1/documents",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"title": title},
    )
    data = resp.json()
    if data.get("code") != 0:
        raise Exception(f"Create document failed: {data}")
    return data["data"]["document"]["document_id"]


def create_wiki_node(
    user_token: str, space_id: str, parent_node_token: str, title: str,
) -> tuple[str, str]:
    """Create a new wiki page (docx) under parent. Returns (node_token, obj_token)."""
    resp = _request_with_retry(
        "POST", f"{BASE_URL}/open-apis/wiki/v2/spaces/{space_id}/nodes",
        headers={"Authorization": f"Bearer {user_token}"},
        json={
            "obj_type": "docx",
            "parent_node_token": parent_node_token,
            "node_type": "origin",
            "title": title,
        },
    )
    data = resp.json()
    if data.get("code") != 0:
        raise Exception(f"Create wiki node failed: {data}")
    node = data["data"]["node"]
    return node["node_token"], node["obj_token"]


def create_children(
    user_token: str, doc_id: str, parent_id: str, blocks: list[dict], index: int = 0,
) -> list[dict]:
    """Insert blocks as children of parent_id. Returns created blocks with new IDs."""
    if not blocks:
        return []
    resp = _request_with_retry(
        "POST", f"{BASE_URL}/open-apis/docx/v1/documents/{doc_id}/blocks/{parent_id}/children",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"children": blocks, "index": index},
        timeout=30,
    )
    data = resp.json()
    if data.get("code") != 0:
        raise Exception(
            f"Create blocks failed [code={data.get('code')}]: {data.get('msg', '')}"
        )
    return data["data"].get("children", [])


def get_block(user_token: str, doc_id: str, block_id: str) -> dict:
    """Fetch a single block (used to read auto-created table cells)."""
    resp = _request_with_retry(
        "GET", f"{BASE_URL}/open-apis/docx/v1/documents/{doc_id}/blocks/{block_id}",
        headers={"Authorization": f"Bearer {user_token}"},
    )
    data = resp.json()
    if data.get("code") != 0:
        raise Exception(f"Get block failed: {data}")
    return data["data"]["block"]


def delete_children_tail(
    user_token: str, doc_id: str, parent_id: str, start_index: int, end_index: int,
) -> None:
    """Delete children of a block by index range [start_index, end_index)."""
    resp = _request_with_retry(
        "DELETE", f"{BASE_URL}/open-apis/docx/v1/documents/{doc_id}/blocks/{parent_id}/children/batch_delete",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"start_index": start_index, "end_index": end_index},
        timeout=10,
    )
    data = resp.json()
    if data.get("code") != 0:
        raise Exception(f"batch_delete failed: {data}")


def download_media(user_token: str, file_token: str) -> bytes:
    """Download image/file by file_token via Open API."""
    resp = _request_with_retry(
        "GET", f"{BASE_URL}/open-apis/drive/v1/medias/{file_token}/download",
        headers={"Authorization": f"Bearer {user_token}"},
        timeout=30,
    )
    if resp.status_code != 200:
        raise Exception(f"API download failed [{resp.status_code}]")
    return resp.content


# ── Browser-based image download ─────────────────────────────────────────────

_browser_ctx = None  # reuse across calls
_browser_failed = False  # don't retry if browser launch failed
_BROWSER_STATE_FILE = os.path.join(os.path.dirname(__file__) or ".", ".feishu_browser_state.json")


def _get_browser_context(source_node_token: str) -> dict:
    """Launch browser with cached session, or prompt login and save cookies."""
    global _browser_ctx, _browser_failed
    if _browser_failed:
        raise Exception("Browser previously failed to launch")
    if _browser_ctx is not None:
        return _browser_ctx

    try:
        pw = sync_playwright().start()
    except Exception as e:
        _browser_failed = True
        raise Exception(f"Cannot start Playwright: {e}")

    has_state = os.path.exists(_BROWSER_STATE_FILE)

    if has_state:
        # Reuse saved cookies — headless is fine
        log.info("  [Browser] Using cached session …")
        try:
            browser = pw.chromium.launch(headless=True)
            ctx = browser.new_context(storage_state=_BROWSER_STATE_FILE)
            _browser_ctx = {"ctx": ctx, "browser": browser, "pw": pw, "page": None}
            return _browser_ctx
        except Exception:
            log.info("  [Browser] Cached session invalid, re-launching …")
            try:
                browser.close()
            except Exception:
                pass

    # No cached state (or invalid) — need headed browser for login
    doc_url = f"https://my.feishu.cn/wiki/{source_node_token}"
    log.info("[Browser] Launching browser for Feishu login …")
    log.info("[Browser] Please log in, then images will download automatically.")
    try:
        browser = pw.chromium.launch(headless=False)
    except Exception as e:
        _browser_failed = True
        pw.stop()
        raise Exception(f"Cannot launch browser (need display): {e}")

    ctx = browser.new_context()
    page = ctx.new_page()
    page.goto(doc_url, wait_until="domcontentloaded", timeout=60000)

    # Wait for login to complete
    for _ in range(180):  # up to 3 minutes
        url = page.url
        if "passport" not in url and "login" not in url and "accounts" not in url:
            break
        page.wait_for_timeout(1000)

    page.wait_for_timeout(3000)

    # Save session for next time
    ctx.storage_state(path=_BROWSER_STATE_FILE)
    log.info("  [Browser] Session saved to %s", _BROWSER_STATE_FILE)
    log.info("  [Browser] Logged in. Downloading images …")

    _browser_ctx = {"ctx": ctx, "browser": browser, "pw": pw, "page": page}
    return _browser_ctx


def _close_browser() -> None:
    """Close the browser if it was opened."""
    global _browser_ctx
    if _browser_ctx:
        try:
            _browser_ctx["browser"].close()
            _browser_ctx["pw"].stop()
        except Exception:
            pass
        _browser_ctx = None


def download_media_browser(file_token: str, source_node_token: str) -> bytes:
    """Download full-resolution image via authenticated browser session."""
    bc = _get_browser_context(source_node_token)
    ctx = bc["ctx"]

    # preview_type=16 returns original resolution (used by Feishu image viewer)
    url = (
        f"https://internal-api-drive-stream.feishu.cn/space/api/box/stream/download"
        f"/preview/{file_token}/?preview_type=16"
    )

    resp = ctx.request.get(url, timeout=30000)
    if resp.status != 200:
        raise Exception(f"Browser download failed [{resp.status}]")
    data = resp.body()
    if len(data) < 100:
        raise Exception(f"Browser download too small [{len(data)} bytes]")
    return data


# ── Parallel image pre-download ──────────────────────────────────────────────


def _prefetch_images(
    user_token: str, src_blocks: list[dict], source_node_token: str,
) -> dict[str, tuple[bytes, int, int]]:
    """Download all images upfront in parallel. Returns {file_token: (bytes, w, h)}."""
    images = []
    for b in src_blocks:
        if b["block_type"] == 27:
            img = b.get("image", {})
            tok = img.get("token", "")
            if tok:
                images.append((tok, img.get("width", 0), img.get("height", 0)))

    if not images:
        return {}

    log.info("  Downloading %d images …", len(images))
    cache: dict[str, tuple[bytes, int, int]] = {}

    # Browser context is NOT thread-safe, so we can't truly parallelize browser
    # downloads. But API downloads can be parallel, and browser falls back serial.
    # Try API first in parallel, collect failures for browser fallback.
    api_fail: list[tuple[str, int, int]] = []

    with ThreadPoolExecutor(max_workers=4) as pool:
        def _try_api(tok: str) -> tuple[str, bytes | None]:
            try:
                return tok, download_media(user_token, tok)
            except Exception:
                return tok, None

        futures = {pool.submit(_try_api, tok): (tok, w, h) for tok, w, h in images}
        for fut in as_completed(futures):
            tok, w, h = futures[fut]
            ftok, data = fut.result()
            if data:
                cache[ftok] = (data, w, h)
                log.debug("    Downloaded image (API)")
            else:
                api_fail.append((tok, w, h))

    # Browser fallback (sequential — Playwright is not thread-safe)
    for tok, w, h in api_fail:
        if HAS_PLAYWRIGHT and source_node_token:
            try:
                data = download_media_browser(tok, source_node_token)
                cache[tok] = (data, w, h)
                log.debug("    Downloaded image (browser)")
            except Exception as e:
                log.warning("    Image skip [%s…]: %s", tok[:12], e)
        else:
            log.warning("    Image skip [%s…]: no browser fallback", tok[:12])

    log.info("  %d/%d images downloaded", len(cache), len(images))
    return cache


# ── Image upload ─────────────────────────────────────────────────────────────


def upload_image_to_block(
    user_token: str, doc_id: str, block_id: str, data_bytes: bytes,
    width: int = 0, height: int = 0, filename: str = "image.png",
) -> str:
    """Upload image to an empty image block and associate it.

    1. Upload file with parent_node=block_id → get file_token
    2. PATCH block with replace_image to set the token and dimensions
    """
    # Step 1: Upload (with retry on timeout/connection errors)
    for attempt in range(3):
        try:
            resp = _request_with_retry(
                "POST", f"{BASE_URL}/open-apis/drive/v1/medias/upload_all",
                headers={"Authorization": f"Bearer {user_token}"},
                data={
                    "parent_type": "docx_image",
                    "parent_node": block_id,
                    "file_name": filename,
                    "size": str(len(data_bytes)),
                },
                files={"file": (filename, data_bytes)},
                timeout=30,
            )
            break
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            if attempt < 2:
                time.sleep(2)
                continue
            raise
    data = resp.json()
    if data.get("code") != 0:
        raise Exception(f"Upload failed: {data}")
    file_token = data["data"]["file_token"]

    # Step 2: Associate with block via replace_image (include dimensions)
    replace_body = {"token": file_token}
    if width and height:
        replace_body["width"] = width
        replace_body["height"] = height
    resp2 = _request_with_retry(
        "PATCH", f"{BASE_URL}/open-apis/docx/v1/documents/{doc_id}/blocks/{block_id}",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"replace_image": replace_body},
    )
    data2 = resp2.json()
    if data2.get("code") != 0:
        raise Exception(f"replace_image failed: {data2}")
    return file_token


def _upload_pending_images(
    user_token: str, doc_id: str, created_blocks: list[dict],
    chunk: list[tuple[dict, dict]],
) -> None:
    """After creating blocks, upload image data to any image blocks."""
    for j, new_blk in enumerate(created_blocks):
        src_prepared = chunk[j][0]
        image_data = src_prepared.pop("_image_data", None)
        if image_data and new_blk.get("block_id"):
            w = src_prepared.pop("_image_width", 0)
            h = src_prepared.pop("_image_height", 0)
            try:
                upload_image_to_block(
                    user_token, doc_id, new_blk["block_id"], image_data,
                    width=w, height=h,
                )
                log.debug("    Uploaded image to block")
            except Exception as e:
                log.warning("    Image upload failed: %s", e)


# ── Block preparation ────────────────────────────────────────────────────────


def _clean(obj: object) -> object:
    """Deep-copy, removing known read-only fields."""
    if isinstance(obj, dict):
        return {k: _clean(v) for k, v in obj.items() if k not in ("comment_ids",)}
    if isinstance(obj, list):
        return [_clean(v) for v in obj]
    return obj


def _compute_heading_numbers(src_blocks: list[dict]) -> dict[str, str]:
    """Walk source blocks in document order and assign hierarchical numbers.

    Returns {block_id: "1.2 "} for each heading block.
    """
    bmap = {b["block_id"]: b for b in src_blocks}
    root = next((b for b in src_blocks if b["block_type"] == 1), None)
    if not root:
        return {}

    # Check if any headings exist
    has_headings = any(3 <= b["block_type"] <= 11 for b in src_blocks)
    if not has_headings:
        return {}

    counters = [0] * 10  # counters[1..9]
    result: dict[str, str] = {}

    def walk(block_ids: list[str]) -> None:
        for bid in block_ids:
            b = bmap.get(bid)
            if not b:
                continue
            bt = b["block_type"]

            if 3 <= bt <= 11:
                level = bt - 2  # 1..9
                counters[level] += 1
                for i in range(level + 1, 10):
                    counters[i] = 0
                # Build number, skipping leading unseen levels (counter=0)
                parts = [str(counters[i]) for i in range(1, level + 1)]
                while parts and parts[0] == "0":
                    parts.pop(0)
                result[bid] = ".".join(parts) + " " if parts else ""

            if b.get("children"):
                walk(b["children"])

    walk(root.get("children", []))
    return result


def _prepare(
    src: dict, image_cache: dict[str, tuple[bytes, int, int]],
    heading_numbers: dict[str, str] | None = None,
) -> dict | None:
    """Convert a read-API block to create-API format (without children).

    image_cache: {file_token: (bytes, width, height)} from _prefetch_images.
    heading_numbers: optional {block_id: "1.2 "} to prepend to heading text.
    """
    bt = src["block_type"]
    if bt in (1, 25):  # page root or grid_column (auto-created) — skip
        return None

    key = _CONTENT_KEY.get(bt)
    if key is None:
        return None

    block = {"block_type": bt}
    if key in src:
        content = _clean(json.loads(json.dumps(src[key])))

        # Prepend heading number if available (styled blue like Feishu auto-numbering)
        if 3 <= bt <= 11 and heading_numbers:
            num = heading_numbers.get(src.get("block_id", ""))
            if num:
                elements = content.get("elements", [])
                # Strip existing manual numbers (e.g., "1.1.4.1. ") from first element
                if elements and "text_run" in elements[0]:
                    first = elements[0]["text_run"]
                    old = first.get("content", "")
                    stripped = re.sub(r"^(\d+\.)+\d*\s*", "", old)
                    if stripped != old:
                        first["content"] = stripped
                num_element = {
                    "text_run": {
                        "content": num,
                        "text_element_style": {"text_color": 5},
                    }
                }
                content["elements"] = [num_element] + elements

        if bt == 18:  # Table: drop source-specific cell IDs
            content.pop("cells", None)

        if bt == 27:  # Image: look up pre-downloaded data
            old_token = content.get("token", "")
            if old_token and old_token in image_cache:
                raw, w, h = image_cache[old_token]
                block["_image_data"] = raw
                block["_image_width"] = w
                block["_image_height"] = h
            elif old_token:
                log.warning("    Image not in cache [%s…], skipping", old_token[:12])
                return None
            content = {}

        block[key] = content

    return block


# ── Block copying (BFS) ─────────────────────────────────────────────────────


def _get_auto_children(new_blk: dict, user_token: str, dst_doc_id: str) -> list[str]:
    """Get auto-created children IDs from a newly created block."""
    ids = new_blk.get("children", [])
    if not ids:
        try:
            blk = get_block(user_token, dst_doc_id, new_blk["block_id"])
            ids = blk.get("children", [])
        except Exception:
            pass
    return ids


def _queue_children(
    new_blk: dict, src_blk: dict, bmap: dict[str, dict],
    nxt: list[tuple[list[str], str]], user_token: str, dst_doc_id: str,
) -> None:
    """After creating a block, queue its source children for the next BFS level."""
    bt = src_blk["block_type"]

    if bt == 18:
        # Table: the API auto-creates empty cells; map source cells → new cells
        new_cell_ids = _get_auto_children(new_blk, user_token, dst_doc_id)
        src_cells = src_blk.get("table", {}).get("cells", [])
        flat = [c for row in src_cells for c in row]
        for k, scid in enumerate(flat):
            if k < len(new_cell_ids):
                sc = bmap.get(scid)
                if sc and sc.get("children"):
                    nxt.append((sc["children"], new_cell_ids[k]))

    elif bt == 24:
        # Grid: the API auto-creates grid_column children; map them by position
        new_col_ids = _get_auto_children(new_blk, user_token, dst_doc_id)
        src_col_ids = src_blk.get("children", [])
        for k, scid in enumerate(src_col_ids):
            if k < len(new_col_ids):
                sc = bmap.get(scid)
                if sc and sc.get("children"):
                    nxt.append((sc["children"], new_col_ids[k]))

    elif src_blk.get("children"):
        nxt.append((src_blk["children"], new_blk["block_id"]))


def copy_blocks(
    token_mgr: TokenManager, src_blocks: list[dict], dst_doc_id: str,
    image_cache: dict[str, tuple[bytes, int, int]],
    heading_numbers: dict[str, str] | None = None,
) -> int:
    """Reproduce the source block tree inside the destination document."""
    bmap = {b["block_id"]: b for b in src_blocks}
    root = next((b for b in src_blocks if b["block_type"] == 1), None)
    if not root:
        raise Exception("No root block in source document")

    # BFS queue: (list_of_source_child_ids, destination_parent_block_id)
    queue = [(root.get("children", []), dst_doc_id)]
    total = 0
    skipped = 0

    while queue:
        nxt = []
        user_token = token_mgr.get()  # refresh if needed at each BFS level

        for child_ids, parent_id in queue:
            if not child_ids:
                continue

            # Prepare blocks for creation; promote children of unsupported blocks
            expanded_ids = []
            for cid in child_ids:
                s = bmap.get(cid)
                if not s:
                    continue
                if s["block_type"] not in _CONTENT_KEY and s["block_type"] != 1:
                    expanded_ids.extend(s.get("children", []))
                else:
                    expanded_ids.append(cid)

            pairs = []  # (create_block, source_block)
            for cid in expanded_ids:
                s = bmap.get(cid)
                if not s:
                    continue
                b = _prepare(s, image_cache, heading_numbers=heading_numbers)
                if b:
                    pairs.append((b, s))

            # Create in batches of 10, tracking actual insert position
            insert_pos = 0
            for i in range(0, len(pairs), 10):
                chunk = pairs[i : i + 10]
                batch = [p[0] for p in chunk]
                batch_clean = [
                    {k: v for k, v in b.items() if not k.startswith("_image")}
                    for b in batch
                ]
                try:
                    created = create_children(
                        user_token, dst_doc_id, parent_id, batch_clean,
                        index=insert_pos,
                    )
                    total += len(created)
                    insert_pos += len(created)
                    _upload_pending_images(user_token, dst_doc_id, created, chunk)
                    for j, new_blk in enumerate(created):
                        _queue_children(
                            new_blk, chunk[j][1], bmap, nxt, user_token, dst_doc_id
                        )
                except Exception as e:
                    log.warning("  Batch failed (%d blocks): %s", len(batch), e)
                    log.info("  Retrying one-by-one …")
                    for _idx, (single, src_blk) in enumerate(chunk):
                        single_clean = {
                            k: v for k, v in single.items()
                            if not k.startswith("_image")
                        }
                        try:
                            created = create_children(
                                user_token, dst_doc_id, parent_id,
                                [single_clean], index=insert_pos,
                            )
                            total += len(created)
                            insert_pos += len(created)
                            img_data = single.pop("_image_data", None)
                            if img_data and created:
                                iw = single.pop("_image_width", 0)
                                ih = single.pop("_image_height", 0)
                                try:
                                    upload_image_to_block(
                                        user_token, dst_doc_id,
                                        created[0]["block_id"], img_data,
                                        width=iw, height=ih,
                                    )
                                    log.debug("    Uploaded image to block")
                                except Exception as ue:
                                    log.warning("    Image upload failed: %s", ue)
                            for new_blk in created:
                                _queue_children(
                                    new_blk, src_blk, bmap, nxt,
                                    user_token, dst_doc_id,
                                )
                        except Exception as e2:
                            bt = src_blk["block_type"]
                            log.warning("    Skipped block_type=%d: %s", bt, e2)
                            skipped += 1
                        time.sleep(0.2)

                time.sleep(0.3)  # rate limit

        queue = nxt

    if skipped:
        log.warning("  %d block(s) could not be copied", skipped)
    return total


# ── Post-copy cleanup ────────────────────────────────────────────────────────


def _cleanup_empty_tails(user_token: str, dst_doc_id: str, src_blocks: list[dict]) -> None:
    """Delete auto-created trailing empty text blocks in callouts and grid columns."""
    dst_blocks = get_all_blocks(user_token, dst_doc_id)
    dst_bmap = {b["block_id"]: b for b in dst_blocks}

    # Pair up source and dest containers by type and order
    src_containers = [b for b in src_blocks if b["block_type"] in (19, 25)]
    dst_containers = [b for b in dst_blocks if b["block_type"] in (19, 25)]

    deleted = 0
    for si, sc in enumerate(src_containers):
        if si >= len(dst_containers):
            break
        dc = dst_containers[si]
        if sc["block_type"] != dc["block_type"]:
            continue

        src_n = len(sc.get("children", []))
        dst_kids = dc.get("children", [])
        dst_n = len(dst_kids)

        if dst_n > src_n:
            # Verify trailing children are all empty text blocks
            all_empty = True
            for kid_id in dst_kids[src_n:]:
                kid = dst_bmap.get(kid_id)
                if not kid or kid["block_type"] != 2:
                    all_empty = False
                    break
                elems = kid.get("text", {}).get("elements", [])
                text = "".join(
                    e.get("text_run", {}).get("content", "") for e in elems
                )
                if text.strip():
                    all_empty = False
                    break
            if all_empty:
                try:
                    delete_children_tail(
                        user_token, dst_doc_id, dc["block_id"],
                        src_n, dst_n,
                    )
                    deleted += dst_n - src_n
                except Exception:
                    pass

    if deleted:
        log.info("  Cleaned up %d auto-created empty paragraph(s)", deleted)


# ── Single-page copy helper ──────────────────────────────────────────────────


def _copy_single_page(
    token_mgr: TokenManager,
    source_node_token: str,
    target_space_id: str,
    target_node_token: str,
    title: str | None = None,
    depth: int = 0,
    heading_numbering: bool = False,
) -> tuple[str, str]:
    """Copy one wiki page's content under the target node.

    Returns (new_node_token, new_obj_token / dst_doc_id).
    """
    indent = "  " * depth
    user_token = token_mgr.get()

    # Read source
    src_node = get_wiki_node(user_token, source_node_token)
    src_title = src_node.get("title", "Untitled")
    obj_token = src_node["obj_token"]
    obj_type = src_node.get("obj_type")

    page_title = title or src_title
    log.info("%s  Copying page: %s", indent, page_title)

    if obj_type != "docx":
        log.warning("%s  Skipping non-docx page (type=%s): %s", indent, obj_type, src_title)
        return "", ""

    # Fetch blocks
    blocks = get_all_blocks(user_token, obj_token)
    log.info("%s  %d blocks", indent, len(blocks))

    # Log first heading block for API field discovery
    for b in blocks:
        if 3 <= b["block_type"] <= 11:
            log.debug("  Sample heading block: %s", json.dumps(b, ensure_ascii=False, indent=2))
            break

    # Compute heading numbers if requested
    heading_numbers = None
    if heading_numbering:
        heading_numbers = _compute_heading_numbers(blocks)
        if heading_numbers:
            log.info("%s  Heading numbers computed for %d headings", indent, len(heading_numbers))

    # Pre-download images
    try:
        image_cache = _prefetch_images(user_token, blocks, source_node_token)
    except Exception:
        image_cache = {}

    # Create target wiki node
    node_token, dst_doc_id = create_wiki_node(
        user_token, target_space_id, target_node_token, page_title,
    )
    log.info("%s  → %s", indent, f"https://my.feishu.cn/wiki/{node_token}")

    # Copy blocks
    n = copy_blocks(token_mgr, blocks, dst_doc_id, image_cache, heading_numbers=heading_numbers)
    log.info("%s  %d blocks created", indent, n)

    # Cleanup empty tails
    try:
        user_token = token_mgr.get()
        _cleanup_empty_tails(user_token, dst_doc_id, blocks)
    except Exception:
        pass

    return node_token, dst_doc_id


# ── Recursive copy ───────────────────────────────────────────────────────────


def _copy_recursive(
    token_mgr: TokenManager,
    source_node_token: str,
    source_space_id: str,
    target_space_id: str,
    target_node_token: str,
    title: str | None = None,
    depth: int = 0,
    heading_numbering: bool = False,
) -> int:
    """Recursively copy a wiki page and all its subpages.

    Returns total number of pages copied.
    """
    indent = "  " * depth

    # Copy this page
    new_node_token, _ = _copy_single_page(
        token_mgr, source_node_token, target_space_id, target_node_token,
        title=title, depth=depth, heading_numbering=heading_numbering,
    )
    if not new_node_token:
        return 0

    count = 1

    # Get children of source node
    user_token = token_mgr.get()
    src_node = get_wiki_node(user_token, source_node_token)
    if not src_node.get("has_child"):
        return count

    children = get_wiki_children(user_token, source_space_id, source_node_token)
    if children:
        log.info("%s  %d subpage(s) found", indent, len(children))

    for i, child in enumerate(children):
        child_token = child.get("node_token", "")
        if not child_token:
            continue
        if i > 0:
            time.sleep(1)  # pause between pages to avoid rate limits
        count += _copy_recursive(
            token_mgr, child_token, source_space_id,
            target_space_id, new_node_token,
            depth=depth + 1, heading_numbering=heading_numbering,
        )

    return count


# ── Main ─────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Copy a Feishu wiki page to a new wiki page"
    )
    parser.add_argument("source", help="Source wiki URL or node token")
    parser.add_argument("target", help="Target parent wiki URL or node token")
    parser.add_argument("--title", help="New page title (default: original title)")
    parser.add_argument(
        "-r", "--recursive", action="store_true",
        help="Recursively copy all subpages",
    )
    parser.add_argument(
        "--heading-numbers", action="store_true",
        help="Prepend auto-generated hierarchical numbers to headings (e.g., 1.1, 1.2)",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(message)s",
    )

    app_id = os.getenv("FEISHU_APP_ID")
    app_secret = os.getenv("FEISHU_APP_SECRET")
    if not app_id or not app_secret:
        log.error("Set FEISHU_APP_ID and FEISHU_APP_SECRET in .env")
        raise SystemExit(1)

    source_node_token = _parse_node_token(args.source)
    target_node_token = _parse_node_token(args.target)

    token_mgr = TokenManager(app_id, app_secret)
    user_token = token_mgr.get()

    # Resolve target space_id
    target_node = get_wiki_node(user_token, target_node_token)
    target_space_id = target_node["space_id"]

    if args.recursive:
        # ── Recursive mode ───────────────────────────────────────────────
        src_node = get_wiki_node(user_token, source_node_token)
        source_space_id = src_node["space_id"]
        new_title = args.title  # None means use original title per page

        log.info("Recursively copying %s → %s …", source_node_token, target_node_token)
        try:
            total_pages = _copy_recursive(
                token_mgr, source_node_token, source_space_id,
                target_space_id, target_node_token,
                title=new_title,
                heading_numbering=args.heading_numbers,
            )
            log.info("Done — %d page(s) copied", total_pages)
        finally:
            _close_browser()
    else:
        # ── Single-page mode (original behaviour) ────────────────────────
        log.info("[1/5] Reading source node %s …", source_node_token)
        src_node = get_wiki_node(user_token, source_node_token)
        src_title = src_node.get("title", "Untitled")
        obj_token = src_node["obj_token"]
        obj_type = src_node.get("obj_type")
        log.info("  Title: %s", src_title)
        log.info("  Type : %s  obj_token: %s", obj_type, obj_token)

        if obj_type != "docx":
            log.error("Only docx is supported (got %s)", obj_type)
            raise SystemExit(1)

        log.info("[2/5] Fetching blocks …")
        blocks = get_all_blocks(user_token, obj_token)
        log.info("  %d blocks", len(blocks))

        log.info("[3/5] Downloading images …")
        try:
            image_cache = _prefetch_images(user_token, blocks, source_node_token)
        except Exception:
            image_cache = {}

        new_title = args.title or src_title
        log.info("[4/5] Creating new wiki page under %s …", target_node_token)
        try:
            node_token, dst_doc_id = create_wiki_node(
                user_token, target_space_id, target_node_token, new_title,
            )
            result_url = f"https://my.feishu.cn/wiki/{node_token}"
            log.info("  node_token: %s", node_token)
            log.info("  doc_id:     %s", dst_doc_id)
        except Exception as e:
            log.warning("  Wiki node creation failed: %s", e)
            log.info("  Falling back to standalone document …")
            try:
                dst_doc_id = create_document(user_token, new_title)
                result_url = f"https://my.feishu.cn/docx/{dst_doc_id}"
                log.info("  doc_id: %s", dst_doc_id)
            except Exception:
                log.error(
                    "Cannot create wiki node or document.\n"
                    "Please add the required permissions to your Feishu app:\n"
                    "  - wiki:wiki (or wiki:node:create)  → create wiki pages\n"
                    "  - docx:document (or docx:document:create) → create documents\n"
                    "  - docx:document:write → write blocks\n"
                    "  - drive:drive (or drive:file:upload) → upload images\n"
                    "\nGo to: https://open.feishu.cn/app/%s/auth\n"
                    "Then delete .feishu_token_cache.json and re-run.",
                    app_id,
                )
                raise SystemExit(1)

        heading_numbers = None
        if args.heading_numbers:
            heading_numbers = _compute_heading_numbers(blocks)
            if heading_numbers:
                log.info("  Heading numbers computed for %d headings", len(heading_numbers))

        log.info("  Copying blocks …")
        try:
            n = copy_blocks(token_mgr, blocks, dst_doc_id, image_cache, heading_numbers=heading_numbers)
            log.info("  %d blocks created", n)
        finally:
            _close_browser()

        log.info("[5/5] Cleaning up …")
        try:
            user_token = token_mgr.get()
            _cleanup_empty_tails(user_token, dst_doc_id, blocks)
        except Exception as e:
            log.warning("  Cleanup skipped: %s", e)

        log.info("Done → %s", result_url)


if __name__ == "__main__":
    main()
