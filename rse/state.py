"""
EchoState — the typed state object shared across all LangGraph nodes.

Every node reads from this dict and returns a partial dict of updated keys.
LangGraph merges partial updates; fields not returned stay unchanged.

Hybrid retrieval flow through the state:
    parse_intent          → parsed_intent (with query_variants + time anchor fields)
    resolve_time_anchor   → anchor_time, anchor_item
    postgres_keyword_search → postgres_results   (parallel branch A)
    faiss_semantic_search   → faiss_results      (parallel branch B)
    merge_and_rrf         → merged_candidates
    evaluate_quality      → result_quality
    rerank_cross_encoder  → ranked_results
    extend_neighborhood   → neighbor_items
    synthesize            → final_answer
"""
from typing import Any, Optional
from typing_extensions import TypedDict


class ParsedIntent(TypedDict, total=False):
    """Structured output from the parse_intent node (LLM Call 1)."""
    sources: list[str]          # ['gmail'] | ['chrome'] | ['youtube'] | ['all']
    time_filter: Optional[str]  # ISO-8601 datetime string or None
    fetch_attachment: bool       # True when user wants file content
    fetch_api: bool              # True when user wants live/fresh data
    query_clean: str             # Distilled core topic, stripped of meta-language
    query_variants: list[str]    # query_clean + two semantic variations/synonyms
    time_anchor_query: Optional[str]  # Reference item for relative time ("interview email")
    time_relation: Optional[str]      # 'after' | 'before' | None
    scope_level: int             # 0=tight | 1=wider | 2=widest (set by widen_scope)
    is_ambiguous: bool           # True if parse failed or query is unresolvable
    original_query: str          # Verbatim user query, preserved for logging
    skip_postgres_filter: bool   # True on attempt 3: bypass dynamic WHERE clauses
    full_faiss_scan: bool        # True on attempt 3: widen FAISS k per variant


class EchoState(TypedDict, total=False):
    """
    Carrier for all data flowing between LangGraph nodes in the RSE pipeline.

    Fields are total=False so nodes can return partial updates without supplying
    every key. LangGraph merges returned dicts onto the running state.
    """
    # ── Input ─────────────────────────────────────────────────────────────────
    user_query: str
    conversation_history: list[Any]   # List of LangChain BaseMessage objects

    # ── Parsed intent (written by parse_intent node) ──────────────────────────
    parsed_intent: ParsedIntent

    # ── Time anchor (written by resolve_time_anchor node) ─────────────────────
    anchor_time: Optional[str]        # ISO timestamp of the resolved anchor item
    anchor_item: Optional[dict]       # Resolved anchor row (memory_id, title, created_at)

    # ── Retrieval results ─────────────────────────────────────────────────────
    postgres_results: list[dict[str, Any]]      # keyword branch, FTS-rank ordered
    faiss_results: list[tuple[str, float]]      # [(memory_id, cosine_similarity), ...]
    merged_candidates: list[dict[str, Any]]     # RRF-fused + hydrated candidate pool
    ranked_results: list[dict[str, Any]]        # cross-encoder + effort final ranking
    neighbor_items: dict[str, list[dict[str, Any]]]  # memory_id → neighboring items
    attachment_content: Optional[str]
    api_results: list[dict[str, Any]]

    # ── Control flow ──────────────────────────────────────────────────────────
    result_quality: str    # 'strong' | 'weak' | 'empty'
    attempt_count: int     # 0–3; incremented by widen_scope on each loop pass

    # ── Output ────────────────────────────────────────────────────────────────
    final_answer: str
    no_results: bool
