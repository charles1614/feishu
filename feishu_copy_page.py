#!/usr/bin/env python3
"""Copy / sync Feishu wiki pages with full content preservation.

Modes:
    Single page:   feishu_copy_page.py SOURCE TARGET
    Full copy:     feishu_copy_page.py SOURCE TARGET -r
    Daily sync:    feishu_copy_page.py SOURCE TARGET -s [-n]

The -s (--sync) flag is the recommended way to keep a wiki tree in sync.
First run copies everything; subsequent runs only update new/changed pages
and print a structured change summary. Logs are saved to sync_logs/.

Examples:
    # Daily sync with heading numbers (recommended):
    feishu_copy_page.py SOURCE TARGET -s -n

    # One-time full copy:
    feishu_copy_page.py SOURCE TARGET -r --fix-refs -n

    # Single page:
    feishu_copy_page.py SOURCE TARGET --title "My Copy"

Environment variables (via .env):
    FEISHU_APP_ID      - Feishu app ID
    FEISHU_APP_SECRET  - Feishu app secret
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import hashlib
import re
import time
import urllib.parse
from datetime import datetime, timezone
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


def _update_block_elements(
    user_token: str, doc_id: str, block_id: str, elements: list[dict],
) -> None:
    """Update the text elements of a block via PATCH API."""
    resp = _request_with_retry(
        "PATCH", f"{BASE_URL}/open-apis/docx/v1/documents/{doc_id}/blocks/{block_id}",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"update_text_elements": {"elements": elements}},
    )
    data = resp.json()
    if data.get("code") != 0:
        raise Exception(
            f"update_text_elements failed [code={data.get('code')}]: {data.get('msg', '')}"
        )


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
_SYNC_STATE_FILE = os.path.join(os.path.dirname(__file__) or ".", ".feishu_sync_state.json")
_DEFAULT_LOG_DIR = os.path.join(os.path.dirname(__file__) or ".", "sync_logs")


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


# ── Reference fixup ─────────────────────────────────────────────────────────


def _remap_elements(
    elements: list[dict],
    node_map: dict[str, str],
    obj_map: dict[str, str],
) -> bool:
    """Modify elements in-place, replacing source doc references with target.

    node_map: {source_node_token: new_node_token} — for URLs like /wiki/{token}
    obj_map:  {source_obj_token: new_obj_token}   — for mention_doc.token

    Returns True if any changes were made.
    """
    changed = False
    for elem in elements:
        if "mention_doc" in elem:
            doc = elem["mention_doc"]
            # Remap URL (contains node_token after /wiki/)
            url = doc.get("url", "")
            for src_tok, dst_tok in node_map.items():
                if src_tok in url:
                    doc["url"] = url.replace(src_tok, dst_tok)
                    changed = True
                    break
            # Remap obj_token
            token = doc.get("token", "")
            if token in obj_map:
                doc["token"] = obj_map[token]
                changed = True

        elif "text_run" in elem:
            style = elem["text_run"].get("text_element_style", {})
            link = style.get("link", {})
            url = link.get("url", "")
            if url:
                # Node tokens are alphanumeric — appear as-is in encoded URLs
                for src_tok, dst_tok in node_map.items():
                    if src_tok in url:
                        link["url"] = url.replace(src_tok, dst_tok)
                        changed = True
                        break

    return changed


def _fixup_references(
    token_mgr: TokenManager,
    node_map: dict[str, str],
    obj_map: dict[str, str],
    doc_map: dict[str, str],
) -> int:
    """Post-copy pass: update document references in all copied pages.

    node_map: {source_node_token: new_node_token}
    obj_map:  {source_obj_token: new_doc_id}
    doc_map:  {new_node_token: new_doc_id}

    Returns number of blocks updated.
    """
    if not node_map:
        return 0

    log.info("Fixing document references across %d page(s) …", len(doc_map))
    fixed_blocks = 0
    fixed_pages = 0

    for new_node_token, new_doc_id in doc_map.items():
        user_token = token_mgr.get()

        try:
            blocks = get_all_blocks(user_token, new_doc_id)
        except Exception as e:
            log.warning("  Failed to read blocks for fixup (node=%s…): %s",
                        new_node_token[:12], e)
            continue

        page_fixed = 0
        for block in blocks:
            bt = block["block_type"]
            key = _CONTENT_KEY.get(bt)
            if not key or key not in block:
                continue

            content = block[key]
            elements = content.get("elements")
            if not elements:
                continue

            # Deep copy elements before modifying
            new_elements = json.loads(json.dumps(elements))
            if not _remap_elements(new_elements, node_map, obj_map):
                continue

            # Clean elements for API (remove read-only fields)
            new_elements = _clean(new_elements)
            try:
                _update_block_elements(
                    user_token, new_doc_id, block["block_id"], new_elements,
                )
                page_fixed += 1
                log.debug("  Fixed ref in block %s (type=%d)", block["block_id"], bt)
            except Exception as e:
                # Fallback: convert mention_doc to text_run links and retry
                has_mention = any("mention_doc" in el for el in new_elements)
                if has_mention:
                    for i, el in enumerate(new_elements):
                        if "mention_doc" in el:
                            doc = el["mention_doc"]
                            title = doc.get("title", "link")
                            url = doc.get("url", "")
                            new_elements[i] = {
                                "text_run": {
                                    "content": title,
                                    "text_element_style": {
                                        "link": {"url": urllib.parse.quote(url, safe="")},
                                    },
                                },
                            }
                    try:
                        _update_block_elements(
                            user_token, new_doc_id, block["block_id"], new_elements,
                        )
                        page_fixed += 1
                        log.debug("  Fixed ref (fallback) in block %s", block["block_id"])
                    except Exception as e2:
                        log.warning("  Failed to fix block %s: %s", block["block_id"], e2)
                else:
                    log.warning("  Failed to fix block %s: %s", block["block_id"], e)

            time.sleep(0.3)

        if page_fixed:
            fixed_blocks += page_fixed
            fixed_pages += 1
            log.info("  Fixed %d ref(s) in %s",
                     page_fixed, f"https://my.feishu.cn/wiki/{new_node_token}")

    log.info("Reference fixup done: %d block(s) in %d page(s)", fixed_blocks, fixed_pages)
    return fixed_blocks


# ── Logging setup ───────────────────────────────────────────────────────────


def _setup_file_logging(log_dir: str) -> str | None:
    """Set up dual logging: console (INFO) + file (DEBUG).

    Creates log_dir if needed, generates a dated log file like
    sync_logs/26-03-02-001.log (auto-incrementing within same day).
    Returns the log file path, or None if setup failed.
    """
    try:
        os.makedirs(log_dir, exist_ok=True)
    except OSError as e:
        log.warning("Cannot create log dir %s: %s", log_dir, e)
        return None

    today = datetime.now().strftime("%y-%m-%d")
    # Find next sequence number for today
    seq = 1
    while True:
        log_file = os.path.join(log_dir, f"{today}-{seq:03d}.log")
        if not os.path.exists(log_file):
            break
        seq += 1

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)-5s %(message)s", datefmt="%H:%M:%S",
    ))
    logging.getLogger().addHandler(file_handler)
    return log_file


# ── Incremental sync ────────────────────────────────────────────────────────


def _load_sync_state() -> dict | None:
    """Load sync state from disk. Returns None if no state file exists."""
    if not os.path.exists(_SYNC_STATE_FILE):
        return None
    with open(_SYNC_STATE_FILE) as f:
        return json.load(f)


def _save_sync_state(state: dict) -> None:
    """Write sync state to disk atomically."""
    tmp = _SYNC_STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)
    os.replace(tmp, _SYNC_STATE_FILE)


def _compute_content_hash(blocks: list[dict]) -> str:
    """Compute SHA-256 hash of block content for change detection.

    Strips volatile identity fields, keeping only structural content.
    """
    def _strip(obj: object) -> object:
        if isinstance(obj, dict):
            return {
                k: _strip(v) for k, v in sorted(obj.items())
                if k not in ("block_id", "parent_id", "children", "comment_ids")
            }
        if isinstance(obj, list):
            return [_strip(v) for v in obj]
        return obj

    stripped = [_strip(b) for b in blocks]
    raw = json.dumps(stripped, sort_keys=True, ensure_ascii=False)
    return "sha256:" + hashlib.sha256(raw.encode()).hexdigest()


def _scan_source_tree(
    token_mgr: TokenManager,
    source_node_token: str,
    source_space_id: str,
    depth: int = 0,
) -> list[dict]:
    """Walk source wiki tree collecting node metadata (without fetching blocks)."""
    user_token = token_mgr.get()
    node = get_wiki_node(user_token, source_node_token)

    entry = {
        "node_token": node["node_token"],
        "title": node.get("title", "Untitled"),
        "obj_token": node.get("obj_token", ""),
        "obj_type": node.get("obj_type", ""),
        "obj_edit_time": str(node.get("obj_edit_time", "")),
        "has_child": node.get("has_child", False),
        "depth": depth,
        "children_tokens": [],
    }

    result = [entry]

    if node.get("has_child"):
        children = get_wiki_children(user_token, source_space_id, source_node_token)
        entry["children_tokens"] = [c["node_token"] for c in children]
        for child in children:
            result.extend(_scan_source_tree(
                token_mgr, child["node_token"], source_space_id, depth + 1,
            ))

    return result


def _extract_headings(blocks: list[dict]) -> list[str]:
    """Extract heading text from blocks for summary display."""
    headings: list[str] = []
    for b in blocks:
        bt = b["block_type"]
        if 3 <= bt <= 11:
            key = f"heading{bt - 2}"
            elements = b.get(key, {}).get("elements", [])
            text = "".join(
                e.get("text_run", {}).get("content", "") for e in elements
            )
            text = text.strip()
            if text:
                # Strip manual numbering for cleaner display
                text = re.sub(r"^(\d+\.)+\d*\s*", "", text)
                headings.append(text)
    return headings


def _find_target_parent(
    snode: dict,
    source_nodes: list[dict],
    pages_state: dict[str, dict],
    target_root: str,
) -> str:
    """Find the target parent node_token for a new page.

    Walks up the source tree to find the nearest ancestor already in pages_state.
    """
    parent_map: dict[str, str] = {}
    for n in source_nodes:
        for child_tok in n.get("children_tokens", []):
            parent_map[child_tok] = n["node_token"]

    current = snode["node_token"]
    while current in parent_map:
        parent_src = parent_map[current]
        if parent_src in pages_state:
            return pages_state[parent_src]["target_node_token"]
        current = parent_src

    return target_root


def _update_existing_page(
    token_mgr: TokenManager,
    source_node_token: str,
    target_obj_token: str,
    heading_numbering: bool = False,
) -> tuple[int, list[dict]]:
    """Clear target page blocks and re-copy from source.

    Returns (blocks_created, source_blocks).
    """
    user_token = token_mgr.get()

    # Read source
    src_node = get_wiki_node(user_token, source_node_token)
    src_obj_token = src_node["obj_token"]
    src_blocks = get_all_blocks(user_token, src_obj_token)

    heading_numbers = None
    if heading_numbering:
        heading_numbers = _compute_heading_numbers(src_blocks)

    try:
        image_cache = _prefetch_images(user_token, src_blocks, source_node_token)
    except Exception:
        image_cache = {}

    # Clear target: delete all root children
    dst_blocks = get_all_blocks(user_token, target_obj_token)
    dst_root = next((b for b in dst_blocks if b["block_type"] == 1), None)
    if dst_root and dst_root.get("children"):
        n_children = len(dst_root["children"])
        if n_children > 0:
            delete_children_tail(
                user_token, target_obj_token, target_obj_token,
                0, n_children,
            )
            time.sleep(0.5)

    # Re-copy from source
    n = copy_blocks(
        token_mgr, src_blocks, target_obj_token, image_cache,
        heading_numbers=heading_numbers,
    )

    try:
        user_token = token_mgr.get()
        _cleanup_empty_tails(user_token, target_obj_token, src_blocks)
    except Exception:
        pass

    return n, src_blocks


def _build_initial_state(
    source_root: str,
    target_root: str,
    source_space_id: str,
    target_space_id: str,
    source_nodes: list[dict],
    node_map: dict[str, str],
    doc_map: dict[str, str],
    obj_map: dict[str, str],
    heading_numbering: bool,
    fix_refs: bool,
) -> dict:
    """Build sync state after the first full copy."""
    pages: dict[str, dict] = {}
    for snode in source_nodes:
        stok = snode["node_token"]
        if stok not in node_map:
            continue
        target_ntok = node_map[stok]
        target_otok = doc_map.get(target_ntok, "")

        pages[stok] = {
            "target_node_token": target_ntok,
            "target_obj_token": target_otok,
            "source_obj_token": snode["obj_token"],
            "title": snode["title"],
            "obj_edit_time": snode.get("obj_edit_time", ""),
            "content_hash": "",  # computed on first incremental run
            "last_synced": datetime.now(timezone.utc).isoformat(),
            "children_order": snode.get("children_tokens", []),
        }

    return {
        "version": 1,
        "source_root": source_root,
        "target_root": target_root,
        "source_space_id": source_space_id,
        "target_space_id": target_space_id,
        "last_sync_time": datetime.now(timezone.utc).isoformat(),
        "options": {
            "heading_numbers": heading_numbering,
            "fix_refs": fix_refs,
        },
        "pages": pages,
    }


def _print_sync_summary(summary: dict, log_dir: str = _DEFAULT_LOG_DIR) -> None:
    """Print sync summary to console, log file, AND a dedicated summary file.

    The summary is appended to sync_logs/sync_summary.log so all sync
    results are in one easy-to-read file, newest at the bottom.
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines: list[str] = []
    lines.append("")
    lines.append("=" * 60)
    lines.append(f"  SYNC SUMMARY  ({now})")
    lines.append("=" * 60)

    if summary["new"]:
        lines.append(f"\n  NEW ({len(summary['new'])} page(s)):")
        for title, headings in summary["new"]:
            lines.append(f"    + {title}")
            for h in headings[:5]:
                lines.append(f"        {h}")
            if len(headings) > 5:
                lines.append(f"        ... and {len(headings) - 5} more")

    if summary["modified"]:
        lines.append(f"\n  MODIFIED ({len(summary['modified'])} page(s)):")
        for title, sections in summary["modified"]:
            lines.append(f"    ~ {title}")
            for line in sections:
                lines.append(f"      {line}")

    if summary["deleted"]:
        lines.append(f"\n  DELETED FROM SOURCE ({len(summary['deleted'])} page(s)):")
        for title in summary["deleted"]:
            lines.append(f"    - {title}  (target kept)")

    lines.append(f"\n  UNCHANGED: {summary['unchanged']} page(s)")
    total = len(summary["new"]) + len(summary["modified"]) + summary["unchanged"]
    lines.append(f"\n  Total: {total} page(s) in source tree")
    lines.append("=" * 60)
    lines.append("")

    text = "\n".join(lines)

    # Console + log file
    log.info(text)

    # Append to dedicated summary file for easy review
    try:
        os.makedirs(log_dir, exist_ok=True)
        summary_file = os.path.join(log_dir, "sync_summary.log")
        with open(summary_file, "a", encoding="utf-8") as f:
            f.write(text + "\n")
    except OSError:
        pass


def _sync_recursive(
    token_mgr: TokenManager,
    source_node_token: str,
    source_space_id: str,
    target_space_id: str,
    target_node_token: str,
    heading_numbering: bool = False,
    fix_refs: bool = True,
    title: str | None = None,
    log_dir: str = _DEFAULT_LOG_DIR,
) -> None:
    """Incremental sync: compare source tree against saved state, process changes.

    First run (no state file): full copy like --recursive, then save state.
    Subsequent runs: only process new/modified pages, print summary.
    State is saved even on error so partial progress is never lost.
    """
    state = _load_sync_state()
    is_first_sync = state is None

    if state and (state.get("source_root") != source_node_token
                  or state.get("target_root") != target_node_token):
        log.error(
            "Sync state mismatch. State: %s → %s, Args: %s → %s. "
            "Delete %s to start fresh.",
            state.get("source_root"), state.get("target_root"),
            source_node_token, target_node_token, _SYNC_STATE_FILE,
        )
        raise SystemExit(1)

    # Inherit options from first run (user only needs to set flags once)
    if state and not is_first_sync:
        saved_opts = state.get("options", {})
        if saved_opts.get("heading_numbers") and not heading_numbering:
            heading_numbering = True
            log.debug("  Inherited --heading-numbers from saved state")
        if saved_opts.get("fix_refs") and not fix_refs:
            fix_refs = True

    # ── Phase 1: Scan source tree ────────────────────────────────────
    log.info("Scanning source tree …")
    source_nodes = _scan_source_tree(token_mgr, source_node_token, source_space_id)
    source_tokens = {n["node_token"] for n in source_nodes}
    log.info("  %d page(s) in source tree", len(source_nodes))

    if is_first_sync:
        # ── First sync: full copy ────────────────────────────────────
        log.info("First sync — copying all pages …")
        node_map: dict[str, str] = {}
        doc_map: dict[str, str] = {}
        obj_map: dict[str, str] = {}

        try:
            total_pages = _copy_recursive(
                token_mgr, source_node_token, source_space_id,
                target_space_id, target_node_token,
                title=title, heading_numbering=heading_numbering,
                node_map=node_map, doc_map=doc_map, obj_map=obj_map,
            )
        except Exception:
            # Save partial state so next run can resume
            if node_map:
                log.warning("Copy interrupted — saving partial state …")
                partial = _build_initial_state(
                    source_node_token, target_node_token,
                    source_space_id, target_space_id,
                    source_nodes, node_map, doc_map, obj_map,
                    heading_numbering, fix_refs,
                )
                _save_sync_state(partial)
            raise

        if fix_refs and node_map:
            _fixup_references(token_mgr, node_map, obj_map, doc_map)

        state = _build_initial_state(
            source_node_token, target_node_token,
            source_space_id, target_space_id,
            source_nodes, node_map, doc_map, obj_map,
            heading_numbering, fix_refs,
        )
        _save_sync_state(state)
        log.info("Done — %d page(s) copied. State saved.", total_pages)
        return

    # ── Phase 2: Classify pages ──────────────────────────────────────
    pages_state = state.get("pages", {})

    new_pages: list[dict] = []
    modified_pages: list[dict] = []
    unchanged_pages: list[dict] = []
    deleted_pages: list[dict] = []

    for snode in source_nodes:
        stok = snode["node_token"]
        if stok not in pages_state:
            new_pages.append(snode)
        else:
            stored = pages_state[stok]
            if snode["obj_edit_time"] != stored.get("obj_edit_time", ""):
                modified_pages.append(snode)
            else:
                unchanged_pages.append(snode)

    for stok, pstate in pages_state.items():
        if stok not in source_tokens:
            deleted_pages.append(pstate)

    if not new_pages and not modified_pages and not deleted_pages:
        log.info("  Everything up to date — no changes detected.")
        _print_sync_summary({
            "new": [], "modified": [], "deleted": [],
            "unchanged": len(unchanged_pages),
        }, log_dir=log_dir)
        return

    log.info("  %d new, %d possibly modified, %d unchanged, %d deleted",
             len(new_pages), len(modified_pages),
             len(unchanged_pages), len(deleted_pages))

    # Rebuild maps from state for fix-refs
    node_map = {}
    doc_map = {}
    obj_map = {}
    for stok, pstate in pages_state.items():
        node_map[stok] = pstate["target_node_token"]
        doc_map[pstate["target_node_token"]] = pstate["target_obj_token"]
        obj_map[pstate["source_obj_token"]] = pstate["target_obj_token"]

    sync_summary: dict = {
        "new": [],
        "modified": [],
        "unchanged": len(unchanged_pages),
        "deleted": [],
    }

    # Wrap processing in try/finally to save state even on error
    try:
        # ── Phase 3a: Process NEW pages ──────────────────────────────
        for snode in new_pages:
            stok = snode["node_token"]
            if snode["obj_type"] != "docx":
                log.warning("  Skipping non-docx new page: %s (type=%s)",
                            snode["title"], snode["obj_type"])
                continue

            target_parent = _find_target_parent(
                snode, source_nodes, pages_state, target_node_token,
            )

            log.info("  [NEW] %s", snode["title"])
            new_ntok, new_otok = _copy_single_page(
                token_mgr, stok, target_space_id, target_parent,
                depth=snode["depth"], heading_numbering=heading_numbering,
                node_map=node_map, doc_map=doc_map, obj_map=obj_map,
            )
            if new_ntok:
                user_token = token_mgr.get()
                blocks = get_all_blocks(user_token, snode["obj_token"])
                headings = _extract_headings(blocks)

                pages_state[stok] = {
                    "target_node_token": new_ntok,
                    "target_obj_token": new_otok,
                    "source_obj_token": snode["obj_token"],
                    "title": snode["title"],
                    "obj_edit_time": snode["obj_edit_time"],
                    "content_hash": _compute_content_hash(blocks),
                    "last_synced": datetime.now(timezone.utc).isoformat(),
                    "children_order": snode.get("children_tokens", []),
                }
                sync_summary["new"].append((snode["title"], headings))

            time.sleep(1)

        # ── Phase 3b: Process MODIFIED pages ─────────────────────────
        for snode in modified_pages:
            stok = snode["node_token"]
            stored = pages_state[stok]
            target_otok = stored["target_obj_token"]

            user_token = token_mgr.get()
            src_blocks = get_all_blocks(user_token, snode["obj_token"])
            new_hash = _compute_content_hash(src_blocks)

            if new_hash == stored.get("content_hash", "") and stored.get("content_hash"):
                log.info("  [SKIP] %s (timestamp changed, content identical)",
                         snode["title"])
                pages_state[stok]["obj_edit_time"] = snode["obj_edit_time"]
                sync_summary["unchanged"] += 1
                continue

            log.info("  [MOD] %s", snode["title"])
            n, _ = _update_existing_page(
                token_mgr, stok, target_otok,
                heading_numbering=heading_numbering,
            )
            log.info("    %d blocks re-created", n)

            headings = _extract_headings(src_blocks)
            sections_str = ", ".join(headings[:6]) if headings else "(no headings)"
            if len(headings) > 6:
                sections_str += f" … +{len(headings) - 6} more"
            sync_summary["modified"].append(
                (snode["title"], [f"Sections: {sections_str}"])
            )

            pages_state[stok].update({
                "title": snode["title"],
                "obj_edit_time": snode["obj_edit_time"],
                "content_hash": new_hash,
                "last_synced": datetime.now(timezone.utc).isoformat(),
                "children_order": snode.get("children_tokens", []),
            })

            time.sleep(1)

        # ── Phase 3c: Handle DELETED pages ───────────────────────────
        for dpage in deleted_pages:
            dtitle = dpage.get("title", "Unknown")
            sync_summary["deleted"].append(dtitle)
            log.warning("  [DEL] %s (target kept: %s)",
                         dtitle, dpage.get("target_node_token", "?"))

        # Remove deleted entries from state
        for stok in list(pages_state.keys()):
            if stok not in source_tokens:
                del pages_state[stok]

        # ── Phase 3d: Fix refs if new pages added ────────────────────
        if fix_refs and new_pages and node_map:
            _fixup_references(token_mgr, node_map, obj_map, doc_map)

    finally:
        # Always save state — even on error, partial progress is preserved
        state["pages"] = pages_state
        state["last_sync_time"] = datetime.now(timezone.utc).isoformat()
        _save_sync_state(state)
        log.info("State saved.")

    # ── Phase 5: Print summary ───────────────────────────────────────
    _print_sync_summary(sync_summary, log_dir=log_dir)


# ── Single-page copy helper ──────────────────────────────────────────────────


def _copy_single_page(
    token_mgr: TokenManager,
    source_node_token: str,
    target_space_id: str,
    target_node_token: str,
    title: str | None = None,
    depth: int = 0,
    heading_numbering: bool = False,
    node_map: dict[str, str] | None = None,
    doc_map: dict[str, str] | None = None,
    obj_map: dict[str, str] | None = None,
) -> tuple[str, str]:
    """Copy one wiki page's content under the target node.

    Returns (new_node_token, new_obj_token / dst_doc_id).
    If node_map/doc_map/obj_map are provided, populates them with the mapping.
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

    # Populate reference mapping tables
    if node_map is not None:
        node_map[source_node_token] = node_token
    if doc_map is not None:
        doc_map[node_token] = dst_doc_id
    if obj_map is not None:
        obj_map[obj_token] = dst_doc_id

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
    node_map: dict[str, str] | None = None,
    doc_map: dict[str, str] | None = None,
    obj_map: dict[str, str] | None = None,
) -> int:
    """Recursively copy a wiki page and all its subpages.

    Returns total number of pages copied.
    """
    indent = "  " * depth

    # Copy this page
    new_node_token, _ = _copy_single_page(
        token_mgr, source_node_token, target_space_id, target_node_token,
        title=title, depth=depth, heading_numbering=heading_numbering,
        node_map=node_map, doc_map=doc_map, obj_map=obj_map,
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
            node_map=node_map, doc_map=doc_map, obj_map=obj_map,
        )

    return count


# ── Main ─────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Copy / sync Feishu wiki pages",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  %(prog)s SOURCE TARGET -s -n          # daily sync with heading numbers\n"
            "  %(prog)s SOURCE TARGET -r --fix-refs   # one-time full copy\n"
            "  %(prog)s SOURCE TARGET                 # single page copy\n"
        ),
    )
    parser.add_argument("source", help="Source wiki URL or node token")
    parser.add_argument("target", help="Target parent wiki URL or node token")
    parser.add_argument("--title", help="Custom page title (default: original)")
    parser.add_argument(
        "-s", "--sync", action="store_true",
        help="Incremental sync (implies --fix-refs; remembers flags from first run)",
    )
    parser.add_argument(
        "-r", "--recursive", action="store_true",
        help="One-time recursive copy of all subpages",
    )
    parser.add_argument(
        "-n", "--numbers", action="store_true", dest="heading_numbers",
        help="Prepend auto-numbered headings (e.g., 1.1, 1.2)",
    )
    parser.add_argument(
        "--fix-refs", action="store_true",
        help="Fix doc references to point to copied pages (auto with -s)",
    )
    parser.add_argument(
        "--log-dir", default=_DEFAULT_LOG_DIR, metavar="DIR",
        help=f"Directory for sync log files (default: {_DEFAULT_LOG_DIR})",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    args = parser.parse_args()

    # --sync implies --fix-refs
    if args.sync:
        args.fix_refs = True

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(message)s",
    )

    # Set up file logging for sync mode
    log_file = None
    if args.sync:
        log_file = _setup_file_logging(args.log_dir)
        if log_file:
            log.info("Log file: %s", log_file)

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

    if args.sync:
        # ── Incremental sync mode ────────────────────────────────────────
        src_node = get_wiki_node(user_token, source_node_token)
        source_space_id = src_node["space_id"]

        try:
            _sync_recursive(
                token_mgr, source_node_token, source_space_id,
                target_space_id, target_node_token,
                heading_numbering=args.heading_numbers,
                fix_refs=args.fix_refs,
                title=args.title,
                log_dir=args.log_dir,
            )
        finally:
            _close_browser()

    elif args.recursive:
        # ── Recursive mode ───────────────────────────────────────────────
        src_node = get_wiki_node(user_token, source_node_token)
        source_space_id = src_node["space_id"]
        new_title = args.title  # None means use original title per page

        log.info("Recursively copying %s → %s …", source_node_token, target_node_token)
        node_map: dict[str, str] = {}
        doc_map: dict[str, str] = {}
        obj_map: dict[str, str] = {}
        try:
            total_pages = _copy_recursive(
                token_mgr, source_node_token, source_space_id,
                target_space_id, target_node_token,
                title=new_title,
                heading_numbering=args.heading_numbers,
                node_map=node_map, doc_map=doc_map, obj_map=obj_map,
            )
            log.info("Done — %d page(s) copied", total_pages)

            if args.fix_refs and node_map:
                _fixup_references(token_mgr, node_map, obj_map, doc_map)
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
