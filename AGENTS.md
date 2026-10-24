# AGENTS.md — E.C.H.O Project Instructions & Agent Guidelines

Instructions for any AI agent (Antigravity, Cursor, Claude Code, etc.) working on this repository.
**Owner:** Mohammed Shehzaad Khan — 3rd-year CS student, intermediate developer. The goal of this project is both a production-quality working capstone AND his deep understanding of every line in it.

---

## 1. What This Project Is

**E.C.H.O (Efficient Contextualized History Orchestrator)** is a local-first, single-user personal memory system. It captures Gmail, Chrome, and YouTube activity through strict intent gates, enriches content in the background, and answers natural-language multi-turn queries through a deterministic LangGraph retrieval graph with **exactly 2 LLM calls per query**. A digital wellbeing module reflects time patterns without judging.

**Source of Truth:**
- `docs/Timeless_docs/Echo_System_Design.md` (architecture + database design in one file; replaced `Echo_Architecture_Design.md` and `Echo_DB_Design.md` on 2026-10-21)

> [!IMPORTANT]
> When code and documentation disagree, check **§3 (Deliberate Deviations)** first — some deviations are deliberate and approved. **Never edit the design document under `docs/Timeless_docs/` unless the owner asks.**

---

## 2. Hard Constraints — Never Violate

1. **Database access is allowed (owner's standing permission, 2026-10-22, final-build run before the 28 Oct review):**
   - The live database is LOCAL PostgreSQL 16 (`echo_db`, role `echo`) and LOCAL Redis (WSL Ubuntu), since 2026-10-24. Neon and Upstash are frozen fallbacks: do not point work at them.
   - Live queries, test data and migrations need no per-task ask. Before any destructive step (DROP, bulk DELETE/UPDATE): take a `pg_dump -Fc` of `echo_db` into `C:\Users\mdshe\echo_backups\`, count the affected rows first, and report what ran.
   - Never search for or expose secret credentials, tokens, or raw `.env` values.
   - At the end of a session also verify by compilation:
     ```bash
     python -m compileall -q backend enp ingestion rse ste wba scripts
     cd frontend && npm run build
     ```
2. **The schema is open, but every change must belong to a decided item** (owner's decision A31.3). It has 17 tables since 2026-10-24 (`scripts/setup_db.py`):
   - Each change is written twice: in `scripts/setup_db.py` AND as migration SQL (`scripts/migrations/`) for the restored database; all schema changes happen before the full re-embed (B5).
   - `system_group_id` must never be `NULL` (fallback `'misc'`).
   - `UNIQUE(source_type, source_id)` deduplication is sacred.
   - There is deliberately no settings table — runtime settings live in Redis (`ste/settings_store.py`).
3. **Prohibited RSE features (Owner's explicit architectural decision):**
   - MMR / diversity re-ranking.
   - Dynamic context compression.
   - Memory decay / recency-based score boosting.
   - *Do not add these even if generic RAG guides suggest them.*
4. **Human-in-the-loop for user groups is architecturally enforced:**
   - No row enters `memory_user_groups` except through `group_suggestions` with an explicit accepted decision.
   - `wba/group_manager.py` is the only write path. Never bypass it.
5. **Exactly 2 LLM calls per retrieval query:**
   - `parse_intent` and `synthesize`.
   - Regardless of widen-loop iterations, everything between them is deterministic Python.
6. **Privacy boundaries:**
   - The LLM receives only query text, short snippets, and aggregated numbers — never raw email/page bodies, full transcripts, or full database rows.
   - Incognito browsing, typed text, and app-page content (Slack/Notion/Jira) are never captured.
   - Wellbeing language must remain descriptive, observational, and non-judgmental (no scores, no "wasted time" phrasing).
7. **Preserve existing structure and naming:**
   - Modify only the module the task is about; do not refactor neighboring components opportunistically.

---

## 3. Deliberate Deviations from the Architecture Doc (Do NOT "Fix" These)

- **Re-ranking:** Reciprocal Rank Fusion (RRF, $k=60$) + local cross-encoder (`ms-marco-MiniLM-L-6-v2`) + effort score replaces §10.5's weighted formula. The 20% recency term was intentionally dropped (see prohibited features).
- **`group_suggestions.decision` defaults to `'pending'`** (DB design v5) — not §15.3's `auto_accepted`. Stricter human-in-the-loop wins.
- **Tier-1 attachment `lightweight_extract`:** Stores filename, MIME type, and size (not the first 500 characters of content) to keep the ingestion hot path fast. Tier-2 (`enp/attachment_processor.py`) extracts full text on demand.
- **Regret POST toggles:** 1:N event rows (odd count = regretted) instead of separate POST/DELETE routes.
- **File naming:** File names may differ slightly from the doc's "Key Files" list (e.g. `gmail_api.py` instead of `gmail_connector.py`); functionality is what matters.

---

## 4. Current State & Known Items

Every feature in the architecture doc is implemented and wired:
- Tier-2 attachment extraction + real `fetch_attachment` LangGraph node.
- `fetch_api` node (live Gmail polling for "today / just received" queries).
- Capture settings (Redis) enforced across all three connectors.
- `DELETE /items/{id}`, `GET /export`, and `GET|POST /settings` API routes.
- Extension popup (search, recent 5 items, pause toggle).
- Centroid recomputation (`recompute_centroids()` in `enp/system_group_classifier.py`) runs at ENP startup when `centroids.npz` is missing or older than 30 days; it persists the updated centroids.
- Rule-suggestion surfacing (step 9) and weekly review UI in the frontend.

`rsr/` — the orphaned duplicate of `rse/` — was **deleted on 2026-08-31**. It was mounted nowhere and its internal imports (`retrival.*`) had always been broken, so `import rsr` never succeeded. `rse/` is and always was the live retrieval engine. Its only non-duplicate content was a retrieval-quality evaluation harness; recover it from history if needed: `git checkout 16bde99 -- rsr/quality_evaluator.py rsr/query_quality_eval_set.py` (its `_calculate_diversity` metric measures a prohibited feature — drop that part).

### Known Open Items
- **OCR for image attachments is optional:** `pytesseract` is commented in `requirements.txt` (requires the Tesseract system binary).
- **Scheduling:** Weekly auto-assign is manual / OS-scheduler; monthly centroid recomputation runs at ENP startup when its file is missing or older than 30 days. There is no in-app cron.
- **Frontend settings toggle:** "Memory indexing" in Settings is localStorage-only (ENP worker has no runtime switch); the three capture toggles ARE backend-wired.

---

## 5. Codebase Conventions and Gotchas

- **Database access differs by module — match the file you are in:**
  - `ste/postgresql_manager.py` (SQLAlchemy, `:name` params) is used by `wba/`, `enp/`, `backend/items.py`, and `ingestion/gmail/`.
  - `rse/search_coordinator.py` uses `psycopg2` + `%s` params directly.
  - *Never mix styles within the same file.*
- **FAISS index:** Uses `IndexFlatL2` over **unit-length** MiniLM vectors (C14: the model's pipeline ends in a Normalize layer; every saved vector has norm 1.0). For unit vectors L2 order equals cosine order, but an L2 distance is still not a similarity score. Anywhere a cosine threshold matters (0.35 quality gate, KNN group scoring, centroids), compute true cosine from the vectors (`get_manager().get_vector()` / `search_all()`; the manager's state is private). When the embedding model changes (A40), check the new model also outputs unit-length vectors, or normalize explicitly.
- **FastAPI route ordering:** Static paths (e.g., `/groups/review`) must be declared before parametrized ones (`/groups/{group_id}/...`) in `backend/wellbeing.py`.
- **Port bindings:** Backend CORS allows ports 3000 and 8000; the Vite dev server must stay on port 3000.
- **Frontend conventions:** Strict TypeScript, custom Tailwind tokens only (no default arbitrary Tailwind palette), all backend calls go through `src/lib/api.ts` with `withFallback` (demo mode). Respect `prefers-reduced-motion`. Fonts are bundled locally — never add a font CDN link.
- **LLM configuration:** Pluggable via `rse/config.py` `LLM_CONFIG`. Stage-4 classifier and insight generator reuse it. Never hardcode an LLM provider elsewhere.
- **Redis keys:** Namespaced `echo:*` (`revisit`, `attachment cache`, `regret reminders`, `echo:settings:capture`). TTLs: revisit = 24h, attachment text = 1h.
- **Extension network egress:** All egress is gated through `postToBackend()` in `background.js` where the pause flag (`chrome.storage.local.echo_tracking_paused`) lives. Route any new network path behind that same function.

---

## 6. Verification Checklist Before Ending Any Session

Run the compilation checks:
```bash
# Backend & Scripts syntax and compilation check (must be completely silent)
python -m compileall -q backend enp ingestion rse ste wba scripts

# Frontend build check (tsc strict + vite bundle, 0 errors)
cd frontend && npm run build
```

**In your final response, always clearly state:**
1. What changed and why.
2. Exact manual verification steps the owner must run against the live DB / services.
3. Any new dependencies added to `requirements.txt` or `frontend/package.json`.
4. Clear explanations of any architectural or logical nuances to support Mohammed's understanding.
