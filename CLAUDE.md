# CLAUDE.md — E.C.H.O Project Instructions

Instructions for any Claude Code session (local or cloud) working on this repository.
Owner: Mohammed Shehzaad Khan — 3rd-year CS student, intermediate developer. The goal of
this project is both a working capstone AND his understanding of every line in it.

---

## 1. What this project is

E.C.H.O (Efficient Contextualized History Orchestrator) — a local-first, single-user
personal memory system. It captures Gmail / Chrome / YouTube activity through intent
gates, enriches it in the background, and answers natural-language multi-turn queries
through a LangGraph retrieval graph with usually 2 LLM calls per query, at most 6 (A26). A wellbeing
module reflects time patterns without judging. See `README.md` for the module map.

**Source of truth:** `docs/Timeless_docs/Echo_System_Design.md` (architecture + database
design in one file, v3.0, 2026-10-21; it replaced `Echo_Architecture_Design.md` and
`Echo_DB_Design.md`, recoverable with `git checkout HEAD -- docs/Timeless_docs/<file>`).
When code and doc disagree, check §3 below first — some deviations are deliberate and
approved. The owner has given standing permission (2026-10-22) to update the design document
whenever a decision or change makes it wrong: edit it directly, no need to ask.

**Current plan (read first):** `docs/review/plan_2026-10-26_to_28.md` is the owner's hour-by-hour plan up to the review on
Wed 28 Oct 2026 (tool loop + MCP today, tests + CI/CD, results, research paper, manual testing, thresholds, D6). Follow it and
tick its boxes as work is done.

**The LLD is stale on purpose (owner, 2026-10-24):** `docs/Phase III/` (the Low Level Design
`.docx` files and `build/lld/build_lld.py`) was a Phase 3 review requirement and the owner has no
current need for it. Do NOT keep it up to date and do not list it as pending work. If an LLD is
ever wanted again, it will be recreated from `Echo_System_Design.md`, which is the up-to-date source.

---

## 2. Hard constraints — never violate

1. **Database access is allowed (owner's standing permission, 2026-10-22, final-build run before the 28 Oct review).**
   Local PostgreSQL and Redis: live queries, test data and migrations need no per-task ask. Guardrails that stay:
   never search for or expose credentials/tokens/.env values; before any destructive step (DROP, bulk DELETE/UPDATE)
   take a `pg_dump -Fc` of `echo_db` into `C:\Users\mdshe\echo_backups\`, count the affected rows first, and report
   what ran (see Section B of `docs/review/ste_open_items.md`).
   **Current state (2026-10-24): the live database is LOCAL PostgreSQL 16 (`echo_db`, role `echo`, sessions in UTC)
   and Redis is LOCAL (WSL Ubuntu, `redis://127.0.0.1:6379/0`).** The A31 restore from Neon is done (16 tables, counts
   and md5 identical). Neon `echo-dev` and Upstash are frozen fallbacks: do not point any work at them. WSL stops when
   no process runs in it, and Redis stops with it; keep it up with `wsl -d Ubuntu -u root -- sleep infinity`.
   At the end of a session also verify by compilation:
   `python -m compileall -q backend enp ingestion rse ste wba scripts` and
   `cd frontend && npm run build` (tsc strict + vite).
2. **The schema is open, but every change must belong to a decided item** (owner's
   decision A31.3, 2026-10-21; the old 16-table lock is lifted). Each change is written
   twice: in `scripts/setup_db.py` (fresh installs) AND as an ALTER/migration SQL for the
   restored local database; all schema changes happen before the full re-embed (B5).
   The schema is now 17 tables: the 2026-10-24 batch (`scripts/migrations/2026-10-24_schema_batch.sql`) added
   `visits` (Table 17), `memory_items.search_vector` (+ GIN) and `gmail_attachments.extracted_text`, and dropped
   `memory_items.classification_confidence`. The code that fills and reads them was wired the same day.
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
   Luna calls a `search_memories` tool up to 5 searches; the 6th call has no tool and must
   answer — cap enforced in `rse/agent_nodes.py`, built 2026-10-26). The search itself is deterministic Python.
6. **Privacy boundaries**: LLM receives only query text, bounded text excerpts, and aggregated
   numbers (owner's decision 2026-10-26, A23/A24: the first 8,000 chars of each of the top 3 results, 2,000 of the
   others, up to 40,000 chars per attachment; the old "short snippets" limit is lifted). Never database rows or the
   whole store. Incognito, typed text, and app-page
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
  (512 tokens, 384-d), replacing all-MiniLM-L6-v2. Built in code 2026-10-25 (`fit_to_window`, query path,
  `EMBEDDING_VERSION = "snowflake-arctic-embed-xs-v1"`) and applied by the B5 re-embed on 2026-10-25 (111 items).
  `FAISSManager.load_index` still refuses an index of another version (ValueError with the rebuild steps).
- **Tier-1 attachment `lightweight_extract`** stores filename/mime/size, not the first
  500 chars of content (keeps the ingestion hot path fast). The full text is extracted
  later by the ENP worker (`enp/attachment_processor.extract_full_text`, A4) into
  `gmail_attachments.extracted_text`; the on-demand Tier-2 path still exists for the query.
- **Regret POST toggles** (1:N event rows, odd count = regretted) instead of separate
  POST/DELETE routes.
- **Session gap is 20 minutes**, not §11.5's 5 (owner's decision, 2026-10-18;
  `wba/wellbeing_analytics.py` `SESSION_GAP_MINUTES`). Each activity is one point in time
  (a Chrome/YouTube visit, or a Gmail item's last open), so a 15-minute read before the
  next page split one sitting into two sessions under the 5-minute rule.
- **No email signature removal** (owner's decision A12, 2026-10-21), not §5.2/§9.1's
  "remove signatures". The heuristic cut real body text in 6 of 8 test mails; a leftover
  signature is small noise, a lost body is a lost memory.
- **App pages (Slack/Notion/Jira) are stored as per-page memory items** with their tab title and
  `raw_text = NULL` (owner's decision A8, 2026-10-21), not §6.5's "session duration only". Their text is
  never captured; the title is embedded and searchable.
- **Stage-4 classification runs per ambiguous item**, inside the ENP batch, rather than one batched LLM call
  as §9.4 describes (A19). Classification sends the source, a title capped at 200 characters, and a clean-text
  snippet capped at 300 characters for every source, including Gmail. It never sends full emails, pages, or
  transcripts. Missing LLM, call failure, or an invalid category falls back to `misc` with
  `classified_by = 'fallback'`.
- **A23 and A24 are simplified (owner, 2026-10-26)**: no query-matched passages or windows. The synthesizer gets the leading
  text of each result (8,000 chars for the top 3, 2,000 for the rest; `rse/config.py` `SYNTHESIS_EXCERPT_*`) and of each
  attachment (`MAX_CHARS_PER_ATTACHMENT = 40000`, `MAX_CHARS_TOTAL = 80000`). Sizes were chosen from gpt-6-luna's published
  limits (1.05M window, 272K long-context price tier), not from a measured long-context test.
- **A26 agent loop, as built (2026-10-26)**: the graph is `agent ⇄ search → finish` (`rse/agent_nodes.py`, `rse/search_tool.py`). It differs from the
  decision text on purpose: no `time_phrase` / `sender_or_domain` arguments (no resolver or sender filter exists; Luna passes a `since` date copied from a
  calendar in the prompt, and the sender goes into `query`); `parse_intent` is gone, its checker lives on as `rse/query_parser.py`
  `intent_from_tool_args`; `fetch_api` / `check_attachments` are gone (A45 dropped the live poll; `read_attachment` is a tool argument);
  `previous_response_id` chaining is deferred (item numbers restart each question). History the agent reads: 12 messages, 1,000 chars per question,
  3,000 per answer (`rse/config.py`). Checked offline with the real OpenAI client against a fake endpoint, NOT yet against the live service.
- **Conversation history keeps the `PostgresChatMessageHistory` table format, not the class.**
  `rse/conversation_memory.py` reads and writes the `message_store` table directly through the RSE psycopg2 pool.
- File names differ from the doc's "Key Files" lists in places (e.g. `gmail_api.py`
  instead of `gmail_connector.py`); functionality is what matters.

---

## 4. Current state (as of 2026-10-26)

Every feature in the architecture doc is implemented and wired, except the two items under "Known open items" below.
The build status of every decided item is in `docs/review/ste_open_items.md` (85 of 86 top-level items complete).
Earlier additions (2026-07):
Tier-2 attachment extraction + real `fetch_attachment` node, `fetch_api` node (live
Gmail poll for "today/just received" queries; removed 2026-10-26 with the A26 loop, A45), capture settings (Redis) enforced in
all three connectors, `DELETE /items/{id}` + `GET /export` + `GET|POST /settings`,
extension popup (search / recent 5 / pause toggle), centroid recomputation
(`recompute_centroids()` in `enp/system_group_classifier.py`, persists `centroids.npz`;
ENP runs it at startup when the file is missing or older than 30 days), rule-suggestion
surfacing (step 9) + weekly review UI in the frontend.

Added 2026-10-23 to 2026-10-26: arctic-embed-xs + token-window embedding and the B5 re-embed (A16/A40), the visits table and
stored full-text `search_vector` (A1, A24.3), ENP attachment extraction with OCR for images and scanned PDFs (A4, D2),
incremental Gmail polling (A5, A38), retention cleanup (A37), chat UI with memory pills, past chats and progress lines
(A27, A28), regret reminder card with snooze (D3), group-rule pause/resume screen (D5), A42 auto-accept of confident group
suggestions, the simplified A23/A24 text limits (see §3), and on 2026-10-26 the A26 agent loop (Luna calls `search_memories` up to 5 searches;
usually 2 LLM calls, max 6) with `tests/test_agent_loop.py`, and the E1 MCP server (`mcp_server/`, `tests/test_mcp_server.py`).

`rsr/` — the orphaned duplicate of `rse/` — was **deleted on 2026-08-31**. It was
mounted nowhere and its internal imports (`retrival.*`) had always been broken, so
`import rsr` never succeeded. `rse/` is and always was the live engine. Its only
non-duplicate content was a retrieval-quality evaluation harness; recover that from
history if Review 2 needs it:
`git checkout 16bde99 -- rsr/quality_evaluator.py rsr/query_quality_eval_set.py`
(note its `_calculate_diversity` metric measures a prohibited feature — drop that part).

Known open items:
- **A26 and E1 are built, with their first live run still to be done by the owner.** E1 is `mcp_server/` (tools `search_memories` and `get_memory`,
  stdio; setup in `mcp_server/README.md`; run it from Claude Desktop). A26: (`python -m rse.retrieval_engine "find my TechCorp interview email"`: expect `llm_calls=2 searches=1` in the log;
  if the API rejects the forced first tool choice, set `FORCE_FIRST_SEARCH = False` in `rse/config.py`).
- **D6, four frontend gaps** (backend routes exist, nothing calls them): old chats show no memory pills, no way to delete a chat,
  no form to add a group rule, no screen to pick items for a group by hand. Details in `docs/review/ste_open_items.md` D6.
- Threshold values are start values, to be re-measured on live data: A18 part 2 (seed-centroid threshold), A40
  (`MIN_TOP_SIMILARITY` 0.55) and A42 (`KNN_ONLY_THRESHOLD` 0.65).
- OCR for images and scanned PDFs is built (`pytesseract` in requirements.txt) but needs the Tesseract system binary
  (Windows: `winget install UB-Mannheim.TesseractOCR`); without it OCR stays off.
- Weekly auto-assign is manual/OS-scheduler; monthly centroid recomputation runs at ENP
  startup when its file is missing or older than 30 days. There is no in-app cron.
- Frontend "Memory indexing" toggle in Settings is localStorage-only (ENP worker has
  no runtime switch); the three capture toggles ARE backend-wired.

---

## 5. Codebase conventions and gotchas

- **DB access differs by module — match the file you're in.** `ste/postgresql_manager`
  (SQLAlchemy, `:name` params) is used by wba/, enp/, backend/items.py, ingestion/gmail.
  `rse/search_coordinator.py` uses psycopg2 + `%s` params directly. Don't mix styles
  within a file.
- **FAISS is IndexFlatL2 over unit-length arctic-embed-xs vectors** (C14: the model's pipeline ends
  in a Normalize layer; `embedding_generator` raises on any vector whose norm is not 1.0 ± 0.001). For unit vectors, L2 order equals
  cosine order, but an L2 distance is still not a similarity score. Anywhere a cosine
  threshold matters (0.55 quality gate, KNN group scoring, centroids), compute cosine from
  the vectors (`get_manager().get_vector()` / `search_all()`; the manager's state is private). When the embedding model changes again, `EMBEDDING_VERSION` must change with it
  and `centroids.npz` must be rebuilt too; the unit-length check in `embedding_generator` will catch a
  model that does not normalize.
- **FastAPI route ordering**: static paths (`/groups/review`) must be declared before
  parametrized ones (`/groups/{group_id}/...`) in `backend/wellbeing.py`.
- Backend CORS allows ports 3000/8000; the Vite dev server must stay on 3000.
- Frontend: strict TS, custom Tailwind tokens only (no default palette), all backend
  calls go through `src/lib/api.ts` `withFallback` (demo mode). Respect
  `prefers-reduced-motion`. Fonts are bundled locally — never add a font CDN.
- LLM provider is plug-and-play via `rse/config.py` `LLM_CONFIG`; Stage-4 classifier
  and insight generator reuse it. Never hardcode a provider elsewhere.
- Redis keys are namespaced `echo:*` (revisit, attachment cache, regret reminders,
  `echo:settings:capture`, `echo:gmail:history_id`). TTLs: revisit 24h, attachment text 1h.
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
