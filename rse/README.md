# RSE — Retrieval & Synthesis Engine (Hybrid Redesign)

Entry point: `rse.retrieval_engine.run_query(user_query, session_id)` — used by
`backend/retrieval.py` (`POST /retrieval/query`).

This module supersedes the linear Postgres → FAISS pipeline from the v2.1
architecture document. The LLM budget is unchanged: **exactly 2 LLM calls per
query** (parse_intent + synthesize); everything in between is deterministic
local Python. (Decision A26 — an agent loop with a `search_memories` tool, up to
5 rounds — will relax this in Step 2.)

## Graph topology

```
parse_intent                     LLM Call 1 — intent + query_variants + time anchor fields
    ↓
resolve_time_anchor              deterministic T_anchor lookup (no-op when unanchored)
    ↓            ↓
postgres_    faiss_semantic_     PARALLEL: keyword branch (Postgres FTS + ILIKE,
keyword_     search              ts_rank ordered) ∥ semantic branch (FAISS full-index
search           ↓               search per query variant, true cosine, pooled + deduped)
    ↓            ↓
merge_and_rrf                    Reciprocal Rank Fusion: Σ 1/(60 + rank); semantic-only hits
    ↓                            hydrated lazily and held to the same source/time/anchor filters
evaluate_quality                 5 checks: empty / source / time window / count / similarity
    ├─ strong → rerank_cross_encoder    cross-encoder/ms-marco-MiniLM-L-6-v2 (top 50):
    │                                   final = 0.8·sigmoid(ce) + 0.2·effort_score
    ├─ weak/empty (<3 tries) → widen_scope → loops back to BOTH branches
    ├─ weak, tries used up → rerank_cross_encoder (closest matches, answered honestly)
    └─ empty, tries used up → no_results_found → END
    ↓
extend_neighborhood              top 3: Gmail same thread; Chrome/YouTube same WBA session,
    ↓                            then ±5 min of Chrome/YouTube activity; closest first
check_attachments → [fetch_attachment | fetch_api] → synthesize   LLM Call 2 → END
```

`rse_langgraph.png` is the same graph drawn by LangGraph (still accurate as of the
2026-10-18 review — no nodes or edges changed).

## File map

| File | Role |
|---|---|
| `retrieval_engine.py` | Graph assembly, `run_query()` |
| `state.py` | `EchoState` / `ParsedIntent` TypedDicts |
| `config.py` | All tuning constants and `LLM_CONFIG` (read-only) |
| `llm_factory.py` | Builds every LLM client: plug-in for all providers, OpenAI extras (reasoning effort, Responses API) |
| `query_parser.py` | LLM Call 1 — `query_variants`, `time_anchor_query`, `time_relation` |
| `time_anchor.py` | `resolve_time_anchor` — FTS lookup (source words become a hint) with a thresholded semantic fallback |
| `search_coordinator.py` | Both retrieval branches, row hydration, today's items, the shared connection pool, `local_to_utc_naive` |
| `fusion.py` | `merge_and_rrf` |
| `reranker.py` | Cross-encoder re-ranking + effort score |
| `neighborhood.py` | `extend_neighborhood` |
| `llm_synthesizer.py` | LLM Call 2 — chat answer, ⟦n⟧ citation markers → memory pills, deterministic fallback |
| `graph_nodes.py` | Node wrappers, `evaluate_quality`, `widen_scope`, `fetch_attachment`, `fetch_api`, `no_results_found` |
| `graph_routing.py` | Conditional edge functions |
| `conversation_memory.py` | Chat history in `message_store` (LangChain message JSON) over the RSE pool |

## Output of `run_query`

- `final_answer` — the chat answer with citations rendered as text pills, e.g. `(✉ Priya · 14 Oct)`.
- `answer_markdown` — the same answer with validated `⟦n⟧` markers, for the memory-pill UI.
- `answer_sources` — the cited items: `anchor` (sender / site / channel), `date_label`, `title`,
  `link` (opens the original mail / page / video), `memory_id`, `source_type`.
- plus `session_id`, `result_count`, `no_results` and the rest of the state.

## Design decisions

- **Rank fusion, not score fusion.** `ts_rank` and cosine live on incomparable
  scales; RRF uses only rank positions, so no scale calibration is needed.
  Ties are broken by best single-branch rank, then id — never by recency.
- **True cosine.** arctic-embed-xs vectors are unit-length, so FAISS's L2 order equals
  cosine order; `FAISSManager.search_all` (locked against ENP writes) still
  reports true cosine, so the 0.55 quality threshold stays meaningful.
- **Query expansion is free.** Variants come out of the same parse_intent call;
  embeddings are local sentence-transformers. Zero extra API calls.
- **Anchor constraint is enforced twice**: as SQL in the keyword branch and as
  a Python post-filter on hydrated semantic-only hits.
- **Local dates vs UTC.** `created_at` is naive UTC; the parser's dates are the
  user's local dates and are converted with `local_to_utc_naive` before every comparison.
- **No recency boosting anywhere.** The effort score is engagement depth only
  (dwell/watch time, scroll, revisits, interactions, completion); the keyword
  branch never falls back to a "most recent items" list. Recency decay, MMR and
  context compression are explicitly out of scope.
- **Graceful degradation everywhere**: FAISS unavailable → keyword-only
  retrieval; cross-encoder unavailable → RRF order + effort; synthesis LLM
  failure → deterministic list in the same answer format; dead pooled DB
  connection after a Neon suspend → pinged and replaced.
- **Privacy in logs.** No module logs the query, titles, snippets or answers —
  only ids, counts, flags, scores and timings.

## Dependencies

Local models (also listed in root `requirements.txt`): `sentence-transformers`
(bi-encoder `Snowflake/snowflake-arctic-embed-xs` via `enp.embedding_generator`; questions take its query path, plus
`cross-encoder/ms-marco-MiniLM-L-6-v2` loaded once in `reranker.py`),
`faiss-cpu` via `ste.faiss_manager`, `numpy`. LLM: `langchain` +
`langchain-openai` (≥1.7.2; GPT-6 Luna on the Responses API).

## Smoke test

`python -m rse.retrieval_engine "find my TechCorp interview email"` — runs one query
against the live database and LLM and prints the intent, top results, answer and sources.

## Not yet wired

- Backend `QueryResponse` does not return `answer_markdown` / `answer_sources` yet, and
  the frontend memory pills + moment strip are Step 2 (decision A27).
- Agent loop with tool calls (A26) and live progress streaming (A28) — Step 2.
- End-to-end testing against Neon + a populated FAISS index.
