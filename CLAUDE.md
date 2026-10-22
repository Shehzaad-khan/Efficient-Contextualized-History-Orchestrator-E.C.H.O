# CLAUDE.md — E.C.H.O Project Instructions

Instructions for any Claude Code session (local or cloud) working on this repository.
Owner: Mohammed Shehzaad Khan — 3rd-year CS student, intermediate developer. The goal of
this project is both a working capstone AND his understanding of every line in it.

---

## 1. What this project is

E.C.H.O (Efficient Contextualized History Orchestrator) — a local-first, single-user
personal memory system. It captures Gmail / Chrome / YouTube activity through intent
gates, enriches it in the background, and answers natural-language multi-turn queries
through a LangGraph retrieval graph with exactly 2 LLM calls per query. A wellbeing
module reflects time patterns without judging. See `README.md` for the module map.

**Source of truth:** `docs/Timeless_docs/Echo_System_Design.md` (architecture + database
design in one file, v3.0, 2026-10-21; it replaced `Echo_Architecture_Design.md` and
`Echo_DB_Design.md`, recoverable with `git checkout HEAD -- docs/Timeless_docs/<file>`).
When code and doc disagree, check §3 below first — some deviations are deliberate and
approved. The owner has given standing permission (2026-10-22) to update the design document
whenever a decision or change makes it wrong: edit it directly, no need to ask.

---

## 2. Hard constraints — never violate

1. **Database execution disabled by default (explicit user request required).**
   No connecting to PostgreSQL/Redis, inserting test data, or running live queries/migrations
   by default. Live DB inspection, SQL queries, and Neon operations are permitted ONLY
   when the user explicitly asks. Never search for or expose credentials/tokens/.env values.
   Verify by compilation only unless requested:
   `python -m compileall -q backend enp ingestion rse ste wba scripts` and
   `cd frontend && npm run build` (tsc strict + vite).
2. **The schema is open, but every change must belong to a decided item** (owner's
   decision A31.3, 2026-10-21; the old 16-table lock is lifted). Each change is written
   twice: in `scripts/setup_db.py` (fresh installs) AND as an ALTER/migration SQL for the
   restored Neon dump; all schema changes happen Thursday, before data cleanup and re-embed.
   Unless a decision says otherwise: `system_group_id` must never be NULL (fallback 'misc'),
   and `UNIQUE(source_type, source_id)` dedup is sacred. There is deliberately no settings
   table — runtime settings live in Redis (`ste/settings_store.py`).
3. **Prohibited RSE features** (owner's explicit decision): MMR/diversity re-ranking,
   memory decay / recency-based score boosting. Do not
   add these even though generic RAG advice suggests them.
4. **User-group membership has one write path**: no row enters `memory_user_groups` except
   through `group_suggestions` (`wba/group_manager.py` is the only write path). Never bypass it.
   An item needs his explicit accept, EXCEPT the confident path (owner's decision A42,
   2026-10-21): a rule match AND KNN >= the shared KNN bar is written `decision='auto_accepted'`,
   `reviewed=FALSE` and shown in the weekly review, where Remove undoes it. Only items he
   confirmed (`decision='accepted'`) are used as KNN examples.
5. **LLM calls per retrieval query: usually 2, max 6** (owner's decision A26, 2026-10-18:
   Luna calls a `search_memories` tool up to 5 rounds; after round 5 the next call must
   answer — cap enforced in code). The search itself is deterministic Python.
6. **Privacy boundaries**: LLM receives only query text, short snippets, and aggregated
   numbers. Never raw bodies/transcripts/full rows. Incognito, typed text, and app-page
   content (Slack/Notion/Jira) are never captured. Wellbeing language must stay
   descriptive and non-judgmental — no scores, no "wasted time" phrasing.
7. **Preserve existing structure and naming.** Modify only the module the task is
   about; do not refactor neighbors opportunistically.

---

## 3. Deliberate deviations from the architecture doc (do NOT "fix" these)

- **Re-ranking**: RRF (k=60) fusion + local cross-encoder (ms-marco-MiniLM-L-6-v2)
  + effort score replaces §10.5's weighted formula. The 20% recency term was
  intentionally dropped (see prohibited features).
- **`group_suggestions.decision` defaults to `'pending'`** (DB design v5) — not
  §15.3's `auto_accepted` for everything. Only the A42 confident path (rule AND KNN agree)
  writes `'auto_accepted'`, and those rows stay in the weekly review for undo.
- **Embeddable text = short hints first, long parts last, cut to the embedding model's own
  token window with its tokenizer** (owner's decisions A16 + A40, 2026-10-21): the stored
  `embeddable_text` is exactly what was embedded. Embedding model = snowflake-arctic-embed-xs
  (512 tokens, 384-d), replacing all-MiniLM-L6-v2 from Thursday's re-embed.
- **Tier-1 attachment `lightweight_extract`** stores filename/mime/size, not the first
  500 chars of content (keeps the ingestion hot path fast). Tier-2
  (`enp/attachment_processor.py`) extracts full text on demand.
- **Regret POST toggles** (1:N event rows, odd count = regretted) instead of separate
  POST/DELETE routes.
- **Session gap is 20 minutes**, not §11.5's 5 (owner's decision, 2026-10-18;
  `wba/wellbeing_analytics.py` `SESSION_GAP_MINUTES`). Each item is one point in time
  (its last access), so a 15-minute read before the next page split one sitting into
  two sessions under the 5-minute rule.
- **No email signature removal** (owner's decision A12, 2026-10-21), not §5.2/§9.1's
  "remove signatures". The heuristic cut real body text in 6 of 8 test mails; a leftover
  signature is small noise, a lost body is a lost memory.
- **App pages (Slack/Notion/Jira) are stored as per-page memory items** with their tab title and
  `raw_text = NULL` (owner's decision A8, 2026-10-21), not §6.5's "session duration only". Their text is
  never captured; the title is embedded and searchable.
- File names differ from the doc's "Key Files" lists in places (e.g. `gmail_api.py`
  instead of `gmail_connector.py`); functionality is what matters.

---

## 4. Current state (as of 2026-07-07 — full audit complete)

Every feature in the architecture doc is implemented and wired. Recent additions:
Tier-2 attachment extraction + real `fetch_attachment` node, `fetch_api` node (live
Gmail poll for "today/just received" queries), capture settings (Redis) enforced in
all three connectors, `DELETE /items/{id}` + `GET /export` + `GET|POST /settings`,
extension popup (search / recent 5 / pause toggle), centroid recomputation
(`recompute_centroids()` in `enp/system_group_classifier.py`, persists `centroids.npz`;
nothing calls it yet — decision A36: run it at ENP start when the file is missing or
older than 30 days), rule-suggestion surfacing (step 9) + weekly review UI in the frontend.

`rsr/` — the orphaned duplicate of `rse/` — was **deleted on 2026-08-31**. It was
mounted nowhere and its internal imports (`retrival.*`) had always been broken, so
`import rsr` never succeeded. `rse/` is and always was the live engine. Its only
non-duplicate content was a retrieval-quality evaluation harness; recover that from
history if Review 2 needs it:
`git checkout 16bde99 -- rsr/quality_evaluator.py rsr/query_quality_eval_set.py`
(note its `_calculate_diversity` metric measures a prohibited feature — drop that part).

Known open items:
- OCR for image attachments is optional (pytesseract commented in requirements.txt —
  needs the Tesseract system binary).
- Scheduling (weekly auto-assign, monthly centroid recompute) is manual/OS-scheduler;
  there is no in-app cron.
- Frontend "Memory indexing" toggle in Settings is localStorage-only (ENP worker has
  no runtime switch); the three capture toggles ARE backend-wired.

---

## 5. Codebase conventions and gotchas

- **DB access differs by module — match the file you're in.** `ste/postgresql_manager`
  (SQLAlchemy, `:name` params) is used by wba/, enp/, backend/items.py, ingestion/gmail.
  `rse/search_coordinator.py` uses psycopg2 + `%s` params directly. Don't mix styles
  within a file.
- **FAISS is IndexFlatL2 over unit-length MiniLM vectors** (C14: the model's pipeline ends
  in a Normalize layer; every saved vector has norm 1.0). For unit vectors, L2 order equals
  cosine order, but an L2 distance is still not a similarity score. Anywhere a cosine
  threshold matters (0.35 quality gate, KNN group scoring, centroids), compute cosine from
  `manager.vectors`. When the embedding model changes (A40), check the new model also
  outputs unit-length vectors, or normalize explicitly.
- **FastAPI route ordering**: static paths (`/groups/review`) must be declared before
  parametrized ones (`/groups/{group_id}/...`) in `backend/wellbeing.py`.
- Backend CORS allows ports 3000/8000; the Vite dev server must stay on 3000.
- Frontend: strict TS, custom Tailwind tokens only (no default palette), all backend
  calls go through `src/lib/api.ts` `withFallback` (demo mode). Respect
  `prefers-reduced-motion`. Fonts are bundled locally — never add a font CDN.
- LLM provider is plug-and-play via `rse/config.py` `LLM_CONFIG`; Stage-4 classifier
  and insight generator reuse it. Never hardcode a provider elsewhere.
- Redis keys are namespaced `echo:*` (revisit, attachment cache, regret reminders,
  `echo:settings:capture`). TTLs: revisit 24h, attachment text 1h.
- The extension gates ALL egress through `postToBackend()` in `background.js` — the
  pause flag (`chrome.storage.local.echo_tracking_paused`) lives there. Keep any new
  network path behind that same function.

---

## 7. Quick verification checklist before ending any session

```bash
python -m compileall -q backend enp ingestion rse ste wba scripts   # must be silent
cd frontend && npm run build                                        # tsc strict + vite, 0 errors
```

Then state: what changed, what the owner must test against the live DB, and any new
dependencies to `pip install` / `npm install`.
