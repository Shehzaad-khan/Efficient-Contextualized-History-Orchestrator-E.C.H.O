"""
EchoState — the typed state object shared across all LangGraph nodes.

Every node reads from this dict and returns a partial dict of updated keys.
LangGraph merges partial updates; fields not returned stay unchanged.
The two search branches run in parallel but write different keys
(postgres_results / faiss_results), so no merge function (reducer) is needed.

Flow through the state (graph built in rse/retrieval_engine.py):
    parse_intent            → parsed_intent (query_variants + time anchor fields)
    resolve_time_anchor     → anchor_time, anchor_item
    postgres_keyword_search → postgres_results   (parallel branch A)
    faiss_semantic_search   → faiss_results      (parallel branch B)
    merge_and_rrf           → merged_candidates
    evaluate_quality        → result_quality
      weak/empty → widen_scope → attempt_count, parsed_intent (widened)
                   → back to both search branches; after MAX_WIDEN_ATTEMPTS
                   → no_results_found → final_answer, no_results=True
      strong     → rerank_cross_encoder → ranked_results
    extend_neighborhood     → neighbor_items
    check_attachments       → routes only (no state written)
    fetch_attachment        → attachment_content   (optional)
    fetch_api               → api_results          (optional)
    synthesize              → final_answer, answer_markdown, answer_sources, no_results
"""
from typing import Any, Optional

from langchain_core.messages import BaseMessage
from typing_extensions import TypedDict


class ParsedIntent(TypedDict, total=False):
    """Structured output from the parse_intent node (LLM Call 1)."""
    sources: list[str]          # subset of ['gmail', 'chrome', 'youtube']; all three when broad
    time_filter: Optional[str]  # ISO-8601 date: keep items created on/after it; None = no bound
    fetch_attachment: bool       # True when user wants file content
    fetch_api: bool              # True when user wants live/fresh data
    query_clean: str             # Distilled core topic, stripped of meta-language
    query_variants: list[str]    # query_clean + two semantic variations/synonyms
    time_anchor_query: Optional[str]  # Reference item for relative time ("interview email")
    time_relation: Optional[str]      # 'after' | 'before' | None
    is_ambiguous: bool           # True if parse failed or query is unresolvable
    original_query: str          # Verbatim user query
    skip_postgres_filter: bool   # True on attempt 3: bypass dynamic WHERE clauses
    full_faiss_scan: bool        # True on attempt 3: widen FAISS k per variant


class EchoState(TypedDict, total=False):
    """
    Carrier for all data flowing between LangGraph nodes in the RSE pipeline.

    Fields are total=False so nodes can return partial updates without supplying
    every key. run_query (rse/retrieval_engine.py) fills every key with a default
    before the graph starts.
    """
    # ── Input ─────────────────────────────────────────────────────────────────
    user_query: str
    conversation_history: list[BaseMessage]   # loaded by rse/conversation_memory.py

    # ── Parsed intent (written by parse_intent, widened by widen_scope) ───────
    parsed_intent: ParsedIntent

    # ── Time anchor (written by resolve_time_anchor node) ─────────────────────
    anchor_time: Optional[str]              # ISO timestamp of the resolved anchor item
    anchor_item: Optional[dict[str, Any]]   # Resolved anchor row (memory_id, title, created_at)

    # ── Retrieval results ─────────────────────────────────────────────────────
    postgres_results: list[dict[str, Any]]      # keyword branch, FTS-rank ordered
    faiss_results: list[tuple[str, float]]      # [(memory_id, cosine_similarity), ...] best first
    merged_candidates: list[dict[str, Any]]     # RRF-fused + hydrated candidate pool
    ranked_results: list[dict[str, Any]]        # cross-encoder + effort final ranking
    neighbor_items: dict[str, list[dict[str, Any]]]  # memory_id → neighboring items
    attachment_content: Optional[str]           # Tier-2 extracted text (fetch_attachment)
    api_results: list[dict[str, Any]]           # live Gmail items (fetch_api)

    # ── Control flow ──────────────────────────────────────────────────────────
    result_quality: str    # 'strong' | 'weak' | 'empty'
    attempt_count: int     # 0–3; incremented by widen_scope on each loop pass

    # ── Output ────────────────────────────────────────────────────────────────
    final_answer: str                          # readable chat answer, citations as text pills
    answer_markdown: str                       # same answer with validated ⟦n⟧ markers (pill UI)
    answer_sources: list[dict[str, Any]]       # cited items: anchor, date_label, title, link …
    no_results: bool
