# RSE — Retrieval & Synthesis Engine (Hybrid Redesign)

Entry point: `rse.retrieval_engine.run_query(user_query, session_id)` — used by
`backend/retrieval.py` (`POST /retrieval/query`).

This module supersedes the linear Postgres → FAISS pipeline from the v2.1
architecture document. The LLM budget is unchanged: **exactly 2 LLM calls per
query** (parse_intent + synthesize); everything in between is deterministic
local Python.

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
merge_and_rrf                    Reciprocal Rank Fusion: Σ 1/(60 + rank), hydration of
    ↓                            semantic-only hits, deterministic source/time post-filters
evaluate_quality                 5 checks: empty / source / time window / count / similarity
    ├─ strong → rerank_cross_encoder    cross-encoder/ms-marco-MiniLM-L-6-v2 (top 50):
    │                                   final = 0.8·sigmoid(ce) + 0.2·effort_score
    ├─ weak/empty (<3 tries) → widen_scope → loops back to BOTH branches
    └─ exhausted → no_results_found → END
    ↓
extend_neighborhood              top 3: Gmail same thread_id; Chrome/YouTube same
    ↓                            session or ±5 min window
check_attachments → [fetch_attachment (stub)] → synthesize   LLM Call 2 → END
```

## File map

| File | Role |
|---|---|
| `retrieval_engine.py` | Graph assembly, `run_query()` |
| `state.py` | `EchoState` / `ParsedIntent` TypedDicts |
| `config.py` | All tuning constants (RRF_K, pool sizes, weights, windows) |
| `query_parser.py` | LLM Call 1 — extended schema with `query_variants`, `time_anchor_query`, `time_relation` |
| `time_anchor.py` | `resolve_time_anchor` — FTS lookup with semantic fallback |
| `search_coordinator.py` | Both retrieval branches + row hydration |
| `fusion.py` | `merge_and_rrf` |
| `reranker.py` | Cross-encoder re-ranking + effort score |
| `neighborhood.py` | `extend_neighborhood` |
| `llm_synthesizer.py` | LLM Call 2 — context assembly, deterministic fallback |
| `graph_nodes.py` | Node wrappers, real `evaluate_quality`, `widen_scope` |
| `graph_routing.py` | Conditional edge functions |
| `conversation_memory.py` | PostgresChatMessageHistory (`message_store`) |

## Design decisions

- **Rank fusion, not score fusion.** `ts_rank` and cosine live on incomparable
  scales; RRF uses only rank positions, so no scale calibration is needed.
- **Cosine computed in-module.** The shared FAISS index is `IndexFlatL2` over
  unnormalized MiniLM vectors, so `search_coordinator` computes true cosine
  from the manager's vector cache; the 0.35 quality threshold from the
  architecture doc stays meaningful.
- **Query expansion is free.** Variants come out of the same parse_intent call;
  embeddings are local sentence-transformers. Zero extra API calls.
- **Anchor constraint is enforced twice**: as SQL in the keyword branch and as
  a Python post-filter on hydrated semantic-only hits, so both branches obey
  the same deterministic window.
- **Effort score has no recency term.** Only engagement depth (dwell/watch
  time, scroll, revisits, interactions, completion). Recency decay, MMR, and
  context compression are explicitly out of scope at this stage.
- **Graceful degradation everywhere**: FAISS unavailable → keyword-only
  retrieval; cross-encoder unavailable → RRF order + effort; synthesis LLM
  failure → deterministic result listing. Retrieval never throws away work.

## Dependencies

Local models (also listed in root `requirements.txt`): `sentence-transformers`
(bi-encoder `all-MiniLM-L6-v2` via `enp.embedding_generator`, plus
`cross-encoder/ms-marco-MiniLM-L-6-v2` loaded lazily in `reranker.py`),
`faiss-cpu` via `ste.faiss_manager`, `numpy`.

## Not yet wired (future sessions)

- `fetch_attachment` remains a stub (Gmail API + Redis 1-hour cache).
- `fetch_api` intent flag is parsed but no live-API node exists yet.
- End-to-end integration testing against Neon + a populated FAISS index.
