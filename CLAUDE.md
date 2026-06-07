# E.C.H.O — Project Context & Session Handoff

> Project-level notes for Claude Code. Read this first when resuming work.
> My global preferences (Knowledge Snapshots, clarity-first, ask before architectural
> changes) live in `~/.claude/CLAUDE.md` and still apply.

## What E.C.H.O is
Efficient Contextualized History Orchestrator — a **local-first personal memory
system**. It captures *my own* digital activity (Gmail, Chrome browsing, YouTube)
into a single searchable memory store. This is a consensual self-tracking tool: I am
both the developer and the only subject; everything reports to my own local backend
(`http://localhost:8000`). It is **not** spyware.

Modules: GMC (Gmail), CHC (Chrome), YTC (YouTube), STE (storage engine),
ENP (enrichment pipeline), RSE (retrieval/search), WBA, UIL.

**My ownership:** the ingestion connectors + the storage engine + the Chrome
extension. ENP / faiss_manager are a teammate's (pulled from GitHub — don't touch
faiss_manager location or schema.sql location).

## Stack
- **PostgreSQL** (Neon cloud) — single source of truth, 16-table schema (`scripts/setup_db.py`)
- **FAISS** (384-dim, local) — vector index, owned by ENP
- **Redis/Upstash** — revisit signals (24h TTL), attachment cache (1h TTL)
- **SQLAlchemy** — engine singleton, `transaction()` context mgr via `engine.begin()`
- **FastAPI** + Pydantic — routers per module, lifespan background workers
- **Chrome Extension MV3** — service worker (`background.js`, module) + content scripts
- Requires **psycopg3** (not psycopg2). Entry point: `python -m uvicorn` on the app.

## Architecture/design docs
`docs/Phase II/Echo_Architecture_Design.docx` and `docs/Phase II/Echo_DB_Design.docx`.
(`.docx` unpack via plain `zipfile` + `xml.etree` — `defusedxml` is unavailable here.)

---

## Completed this session (Phase 2 alignment)
- **STE `fetch_retrieval_candidates()`** — now JOINs chrome/gmail/youtube metadata
  tables and selects the per-source fields; removed the redundant EXISTS subquery.
- **Deleted `upsert_embedding_record()`** — dead code; ENP owns `embedding_index` writes.
- **Chrome intent thresholds** (`ingestion/chrome/intent_filter.py`) fixed to arch spec:
  `PHASE1_MIN_SECONDS=30`, `PHASE2_MIN_FOREGROUND=120`, `PHASE2_MIN_SCROLL=0.5`,
  `PHASE2_MIN_INTERACTIONS=3`.
- **YouTube intent gate** (`ingestion/youtube/youtube_connector.py` + `extension/content/youtube_tracker.js`):
  Shorts 15s / regular 60s, **Option B completion_rate ≥ 0.5** wired client→backend
  (client computes from `video.duration`, backend re-validates). Manual-interaction
  trigger left as-is by request.
- **Gmail engagement (new feature, full stack):**
  - Backend: `update_gmail_engagement()` in `ste/storage_engine.py` + `POST /gmail/engagement`
    in `ingestion/gmail/router.py` (`EmailOpenedEvent{email_id, dwell_seconds}`, 404 if no row).
  - Extension: new `extension/content/gmail_tracker.js` — detects open email via DOM
    `data-legacy-message-id` (the hex API id; URL-hash permalink ids do NOT match),
    4-condition "visited properly" timer, `MIN_VISIT_SECONDS=3`, sends one
    `GMC_ENGAGEMENT` per reading session (on email switch / `beforeunload`).
  - Registered in `manifest.json` (`https://mail.google.com/*`) and routed in
    `background.js` (`GMC_ENGAGEMENT → /gmail/engagement`).
  - **Passive reading credit:** `FOCUS_CREDIT_SECONDS=60` — a long email read with no
    clicks still earns a full minute once it holds focus for 60s uninterrupted.
- **Gmail attachments** (`store_gmail_message()`):
  - `lightweight_extract` = first 500 chars of the **email body** (not attachment content).
  - `is_processed` now `= lightweight is not None` (False unless the extract was populated)
    — so a future PDF/OCR extractor can find unprocessed rows via `WHERE is_processed=False`.
  - filename / mime_type / file_size still stored as separate columns (from MIME headers).

---

## Remaining tasks / TODO

### Security audit findings (fix in priority order)

**CRITICAL**
1. `backend/retrieval.py` — `GET` and `DELETE /session/{session_id}` have **zero
   authentication**. Anyone can read or delete any session's conversation history.
2. `ingestion/gmail/gmail_api.py:101` — `payload["body"]` direct access can crash on
   malformed emails (KeyError).

**HIGH**
3. `backend/main.py` — no CORS middleware configured.
4. Raw exceptions exposed to HTTP clients (`str(e)` in 500 responses) — info leak.
5. `QueryRequest` — no `session_id` length validation.
6. Gmail body extraction — unhandled `base64.urlsafe_b64decode` decode errors.

**MEDIUM / LOW**
7. Gmail auth — exception messages can leak filesystem path info.
8. Gmail API — no rate limiting / exponential backoff.
9. Chrome connector — silent `pass` on exceptions (swallowed errors).
10. Hardcoded session id `"20c712"` left in a debug log file.

### Verification deltas (LOW — naming/location only, not bugs)
- `playback_tracker.js` lives in `extension/`, not `ingestion/youtube/` (YouTube).
- `gmail_connector.py`, `email_processor.py`, `gmail_auth.py` don't exist as named
  files (Gmail) — functionality is split across other files.

## Conventions / gotchas
- `UNIQUE(source_type, source_id)` + `ON CONFLICT DO NOTHING` for idempotent inserts.
- `COALESCE` for write-once / accumulate semantics (engagement counters, `first_opened_at`).
- `UPDATE ... FROM` join + `rowcount > 0` is how 404 (memory-not-found) is detected.
- On Gmail, BOTH `chrome_tracker.js` (app-domain, tab-level, no content) and
  `gmail_tracker.js` (per-email reading) run — they write to different records, so this
  is intentional, not double-counting.
- Quick check after Python edits: `python -m py_compile <file>`.
