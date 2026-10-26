# RSE — Retrieval & Synthesis Engine (Hybrid Redesign)

Entry point: `rse.retrieval_engine.run_query(user_query, session_id)` — used by
`backend/retrieval.py` (`POST /retrieval/query`).

This module supersedes the linear Postgres → FAISS pipeline from the v2.1
architecture document. Since decision A26 (built 2026-10-26) the LLM runs an **agent
loop**: Luna calls a `search_memories` tool, reads the numbered results and either
searches again or answers. **Usually 2 LLM calls per query, at most 6** (up to 5
searches, then a call without the tool); the cap is enforced in code. Everything a search
does is deterministic local Python.

## Graph topology

```
agent                    Luna with the search_memories tool bound (LLM call)
  ├─ asks for searches → search → back to agent        (≤ 5 searches, ≤ 3 per step)
  └─ answers → finish → END                            (validates ⟦n⟧ citations)
```

One search (`search_tool.run_search`) runs the stages below on a private state; no LLM call:

```
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
    ├─ weak/empty (<3 tries) → widen_scope → loops back to BOTH branches (no LLM call)
    ├─ weak, tries used up → rerank_cross_encoder (closest matches, answered honestly)
    └─ empty, tries used up → empty result (Luna says so, or no_results_found)
    ↓
extend_neighborhood              top 3: Gmail same thread; Chrome/YouTube same WBA session,
    ↓                            then ±5 min of Chrome/YouTube activity; closest first
fetch_attachment                 only when the tool call set read_attachment
```

Items are numbered across all searches of one question ([n] stays stable; an item seen
before comes back as "(already shown above)"). The tool arguments are checked by
`query_parser.intent_from_tool_args` before any search runs.

`rse_langgraph.png` shows the graph as it was before A26 (parse_intent … synthesize) and is out of date.

## File map

| File | Role |
|---|---|
| `retrieval_engine.py` | Graph assembly (`agent ⇄ search → finish`), `run_query()`, `run_query_stream()` |
| `agent_nodes.py` | The `agent`, `search` and `finish` nodes; the 5-search / 6-call cap |
| `search_tool.py` | The `search_memories` tool definition (what Luna reads) and `run_search` |
| `state.py` | `EchoState` / `ParsedIntent` TypedDicts |
| `config.py` | All tuning constants and `LLM_CONFIG` (read-only) |
| `llm_factory.py` | Builds every LLM client: plug-in for all providers, OpenAI extras (reasoning effort, Responses API) |
| `query_parser.py` | Argument checker: tool call → `ParsedIntent` (`intent_from_tool_args`), fallback intent |
| `time_anchor.py` | `resolve_time_anchor` — FTS lookup (source words become a hint) with a thresholded semantic fallback |
| `search_coordinator.py` | Both retrieval branches, row hydration, today's items, the shared connection pool, `local_to_utc_naive` |
| `fusion.py` | `merge_and_rrf` |
| `reranker.py` | Cross-encoder re-ranking + effort score |
| `neighborhood.py` | `extend_neighborhood` |
| `llm_synthesizer.py` | The agent prompt, result text for Luna, ⟦n⟧ citation markers → memory pills, deterministic fallback |
| `graph_nodes.py` | The search stages: wrappers, `evaluate_quality`, `widen_scope`, `fetch_attachment`, `no_results_found` |
| `graph_routing.py` | Conditional edge functions (`route_after_agent`, `route_after_search`, `route_after_evaluate_quality`) |
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
- **Query expansion is free.** Luna writes the variants (`alternative_phrasings`) in the same call that asks for the search;
  embeddings are local sentence-transformers. Zero extra API calls.
- **Anchor constraint is enforced twice**: as SQL in the keyword branch and as
  a Python post-filter on hydrated semantic-only hits.
- **Local dates vs UTC.** `created_at` is naive UTC; Luna's `since` dates are the
  user's local dates and are converted with `local_to_utc_naive` before every comparison.
- **No recency boosting anywhere.** The effort score is engagement depth only
  (dwell/watch time, scroll, revisits, interactions, completion); the keyword
  branch never falls back to a "most recent items" list. Recency decay and MMR
  are explicitly out of scope (query-matched passage selection, A23/A24, was allowed on 2026-10-21).
- **Graceful degradation everywhere**: FAISS unavailable → keyword-only
  retrieval; cross-encoder unavailable → RRF order + effort; agent LLM
  failure → one search on the raw question and a deterministic list in the same answer format; dead pooled DB
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
against the live database and LLM and prints the last search, the items shown to the model, the answer and sources.
The log line `run_query: complete … llm_calls=2 searches=1` shows the call count.

## Tests

`python -m pytest tests` — `tests/test_agent_loop.py` runs the real graph with a scripted model and a fake search (no database, index or API key).

## Not yet done

- The first run of the agent loop against the live OpenAI service (checked so far with the real client against a local fake endpoint).
- `previous_response_id` chaining (deferred, A26) and the MCP server (E1, will reuse `search_tool.py`).
- End-to-end testing against the local PostgreSQL + a populated FAISS index with the loop.
