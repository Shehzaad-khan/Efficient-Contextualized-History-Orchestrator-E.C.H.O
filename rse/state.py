"""
EchoState — the typed state object shared across all LangGraph nodes.

Every node reads from this dict and returns a partial dict of updated keys.
LangGraph merges partial updates; fields not returned stay unchanged.
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
    scope_level: int             # 0=tight | 1=wider | 2=widest (set by widen_scope)
    is_ambiguous: bool           # True if parse failed or query is unresolvable
    original_query: str          # Verbatim user query, preserved for logging
    skip_postgres_filter: bool   # True on attempt 3: bypass dynamic WHERE clauses
    full_faiss_scan: bool        # True on attempt 3: search all vectors, not just candidates


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

    # ── Retrieval results ─────────────────────────────────────────────────────
    postgres_results: list[dict[str, Any]]
    faiss_results: list[tuple[str, float]]   # [(memory_id, cosine_score), ...]
    attachment_content: Optional[str]
    api_results: list[dict[str, Any]]

    # ── Control flow ──────────────────────────────────────────────────────────
    result_quality: str    # 'strong' | 'weak' | 'empty'
    attempt_count: int     # 0–3; incremented by widen_scope on each loop pass

    # ── Output ────────────────────────────────────────────────────────────────
    final_answer: str
    no_results: bool
