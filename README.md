# feishu-wiki-tools

Copy and export Feishu (Lark) wiki pages via the Open API, preserving text, formatting, images, tables, grids, callouts, and equations.

## Features

- **Incremental sync** (`-s`) — daily sync that only copies new/changed pages, with structured change summary
- **Copy wiki pages** between spaces with full fidelity (text, images, layout)
- **Auto-numbered headings** (`-n`) — prepends hierarchical numbers (1.1, 1.2.1) styled in blue
- **Reference remapping** — internal links auto-updated to point to copied pages
- **Export to markdown** with inline LaTeX, code blocks, nested lists, and callouts
- **Full-resolution image transfer** via browser session fallback when API returns 403
- **Parallel image download** (ThreadPoolExecutor for API, sequential Playwright fallback)
- **Auto token refresh** for long-running copies (>90 min)
- **Sync logging** — each sync run saved to `sync_logs/YY-MM-DD-NNN.log` for audit

## Quick Start

```bash
# Install dependencies
pip install -e .

# Install Playwright browsers (optional, for image download fallback)
playwright install chromium

# Configure credentials
cp .env.example .env
# Edit .env with your Feishu app credentials

# Daily sync (recommended) — first run copies everything, subsequent runs are incremental
feishu-copy SOURCE TARGET -s -n

# One-time full copy with heading numbers and reference fixing
feishu-copy SOURCE TARGET -r -n --fix-refs

# Copy a single page
feishu-copy SOURCE TARGET --title "My Copy"

# Export a wiki page to markdown
feishu-export https://my.feishu.cn/wiki/NODE_TOKEN -o output/
```

## Setup

### 1. Create a Feishu App

Go to [Feishu Open Platform](https://open.feishu.cn/app) and create an app.

### 2. Add Required Permissions

Under your app's **Permissions & Scopes**, add:

| Scope | Purpose |
|-------|---------|
| `wiki:wiki` | Read wiki tree structure |
| `wiki:node:create` | Create new wiki pages |
| `docx:document` | Read document blocks |
| `docx:document:write` | Create/update blocks |
| `drive:drive` | Upload images |
| `drive:file:upload` | Upload image files |

### 3. Configure Environment

```bash
cp .env.example .env
```

Edit `.env`:
```
FEISHU_APP_ID=cli_xxxxxxxxxxxxx
FEISHU_APP_SECRET=xxxxxxxxxxxxxxxxxxxxxxxx
```

### 4. First Run - OAuth Login

On first run, the script opens a browser for Feishu OAuth login. After login:
- **Access token** is cached (valid ~2 hours)
- **Refresh token** is cached (valid ~30 days, auto-renews on each use)
- **Browser session** is cached for image downloads (`.feishu_browser_state.json`)

You only need to re-login if inactive for >30 days.

## Usage

### Daily Sync (Recommended)

```bash
# First run — copies everything, saves state
feishu-copy SOURCE TARGET -s -n

# Subsequent runs — only updates new/changed pages
feishu-copy SOURCE TARGET -s
```

On incremental runs, a structured summary is printed:

```
============================================================
  SYNC SUMMARY
============================================================

  NEW (1 page(s)):
    + Chapter 5: Advanced Topics
        Overview
        Deep Dive

  MODIFIED (1 page(s)):
    ~ Chapter 2: Getting Started
      Sections: Introduction, Setup, Configuration

  UNCHANGED: 12 page(s)

  Total: 14 page(s) in source tree
============================================================
```

Flags like `-n` (heading numbers) are remembered from the first run — no need to repeat them.

Each sync run is logged to `sync_logs/YY-MM-DD-NNN.log` (auto-incrementing).

### CLI Reference

| Flag | Long | Description |
|------|------|-------------|
| `-s` | `--sync` | Incremental sync (implies `--fix-refs`) |
| `-r` | `--recursive` | One-time full recursive copy |
| `-n` | `--numbers` | Auto-numbered headings (1.1, 1.2) |
| | `--fix-refs` | Remap internal links (auto with `-s`) |
| | `--title` | Custom page title |
| | `--log-dir` | Custom log directory (default: `sync_logs/`) |
| `-v` | `--verbose` | Debug output |

### Copy a Single Page

```bash
feishu-copy ABC123 DEF456 --title "My Copy"
```

### Recursive Copy (One-Time)

```bash
feishu-copy SOURCE TARGET -r -n --fix-refs
```

### Export to Markdown

```bash
feishu-export https://my.feishu.cn/wiki/ABC123 -o output/
```

Exports the wiki page (and child pages recursively) as markdown files with images.

## Sync Internals

### How Incremental Sync Works

1. **Scan** — walk the source wiki tree, collect metadata (title, edit timestamp) for every page
2. **Classify** — compare against saved state:
   - **New**: source page not in state file → copy it
   - **Modified**: `obj_edit_time` changed → verify via content hash → re-copy if truly changed
   - **Deleted**: state entry missing from source → log warning (target kept for safety)
   - **Unchanged**: skip entirely
3. **Update** — for modified pages: clear all target blocks → re-copy from source
4. **Fix refs** — update internal links in all copied pages to point to target pages
5. **Save state** — persist mapping to `.feishu_sync_state.json` (also saved on error for resumability)

### Change Detection (Two-Tier)

- **Tier 1 — Timestamp** (`obj_edit_time`): fast check, avoids fetching blocks for unchanged pages
- **Tier 2 — Content hash** (SHA-256): catches metadata-only edits that don't affect content

### State File

The `.feishu_sync_state.json` file maps every source page to its target copy, including edit timestamps and content hashes. Deleting this file triggers a fresh full copy on the next sync.

## Architecture

### Block Copy Strategy (BFS)

```
Source Page (307 blocks)
    |
    v
[1] Scan all image blocks, collect tokens
[2] Download images in parallel (API first, browser fallback)
[3] BFS traversal: for each level of the block tree
    |-- Prepare blocks (strip read-only fields, attach image data)
    |-- Batch create (10 blocks per API call)
    |-- Upload images to newly created empty image blocks
    |-- Map auto-created children (table cells, grid columns)
    |-- Queue children for next BFS level
[4] Clean up trailing empty paragraphs
```

### Image Transfer (3-step process)

Feishu's docx API requires a specific flow for images:

1. **Create** an empty image block (`{"block_type": 27, "image": {}}`)
2. **Upload** image data to that block (`POST medias/upload_all` with `parent_node=block_id`)
3. **Associate** the uploaded file with the block (`PATCH` with `replace_image` including dimensions)

Image download tries the Open API first, then falls back to an authenticated
Playwright browser session using the `/preview/{token}/?preview_type=16`
endpoint for full original resolution.

### Supported Block Types

| Type | Name | Copy | Markdown |
|------|------|------|----------|
| 2 | Text/Paragraph | Yes | Yes |
| 3-11 | Heading 1-9 | Yes | Yes (capped at H6) |
| 12 | Bullet List | Yes | Yes (nested) |
| 13 | Ordered List | Yes | Yes (numbered) |
| 14 | Code Block | Yes | Yes (26 languages) |
| 15 | Quote | Yes | Yes (blockquote) |
| 16 | Equation | Yes | Yes (LaTeX $$) |
| 17 | Todo | Yes | Yes (checkbox) |
| 18 | Table | Yes | - |
| 19 | Callout | Yes | Yes (emoji + blockquote) |
| 22 | Divider | Yes | Yes (---) |
| 24 | Grid | Yes | Yes (transparent) |
| 25 | Grid Column | Yes* | Yes (transparent) |
| 27 | Image | Yes | Yes (placeholder) |
| 34 | View/Embed | No** | - |

\* Grid columns are auto-created by the API; children are mapped by position.
\** View/embed blocks cannot be created via API. Their children are promoted to the parent level so no content is lost.

### Inline Text Formatting

Bold, italic, inline code, strikethrough, links, mentions, document
references, and inline equations are all preserved during both copy and
markdown export.

## Token Lifecycle

| Token | Lifetime | Renewal |
|-------|----------|---------|
| Access token | ~2 hours | Auto-refresh via refresh token |
| Refresh token | ~30 days | Renewed on each use (rolling) |
| Browser session | Until cookies expire | Cached in `.feishu_browser_state.json` |

The `TokenManager` class in the copy script auto-refreshes before each BFS
level if the token is older than 90 minutes.

## Troubleshooting

### "Failed to get wiki node" / Permission errors
Add the required scopes at `https://open.feishu.cn/app/<your_app_id>/auth`,
then delete `.feishu_token_cache.json` and re-run to authorize with new scopes.

### Images show as 100x100 thumbnails
The `replace_image` API call must include `width` and `height`. This is handled
automatically. If you see this, your script version may be outdated.

### Browser fails to launch
Playwright requires a display. On first run, use a machine with a GUI. After
login, the session is cached and subsequent runs work headlessly.

### Token expired / refresh failed
Delete `.feishu_token_cache.json` and re-run. You'll be prompted to log in again.

## Project Structure

```
.
├── feishu_copy_page.py           # Wiki page copier/syncer (CLI: feishu-copy)
├── feishu_wiki.py                # Wiki reader, markdown exporter (CLI: feishu-export)
├── pyproject.toml                # Package configuration
├── .env.example                  # Credentials template
├── .env                          # Your credentials (gitignored)
├── .feishu_token_cache.json      # OAuth tokens (gitignored)
├── .feishu_browser_state.json    # Playwright cookies (gitignored)
├── .feishu_sync_state.json       # Sync state: source↔target mapping (gitignored)
└── sync_logs/                    # Per-run log files (auto-created)
    ├── 26-03-01-001.log
    └── 26-03-02-001.log
```

## License

MIT
