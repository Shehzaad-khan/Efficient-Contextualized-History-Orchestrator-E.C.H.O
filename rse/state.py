"""
<<<<<<< HEAD
state.py — RSE Module
Echo Personal Memory System

Defines EchoState and ParsedIntent TypedDicts.
These are the shared state carriers between all LangGraph nodes.
Every node reads from and writes partial updates to EchoState.
"""

from typing import Any, Dict, List, Optional, TypedDict


class ParsedIntent(TypedDict):
    """
    Structured output from parse_intent node (LLM Call 1).
    The LLM fills this JSON from the user's natural language query.
    All retrieval decisions downstream depend on this structure.
    """

    # Which sources to search — defaults to 'all' if not specified
    sources: List[str]  # subset of ['gmail', 'chrome', 'youtube', 'all']

    # ISO-8601 datetime string used as lower bound on created_at, or None
    # Examples: "2025-01-01T00:00:00", "2025-03-01T00:00:00", None
    time_filter: Optional[str]

    # True if user is asking about an attachment (e.g. "that PDF from the email")
    fetch_attachment: bool

    # True if user wants live API data (e.g. current video stats) — future use
    fetch_api: bool

    # The semantic core of the query, stripped of meta-language
    # "find the OS tutorial I watched before the interview email" ->
    # "OS tutorial operating systems"
    query_clean: str

    # Scope widening level — set by widen_scope node, not by parse_intent
    # 0=tight, 1=wider time window, 2=drop time filter
    scope_level: int

    # True if the query is genuinely ambiguous or unclear
    # Routes to a clarification response instead of retrieval
    is_ambiguous: bool

    # Original unmodified user query — preserved for conversation history
    original_query: str

    # Set to True by widen_scope when all Postgres filters should be dropped
    skip_postgres_filter: bool

    # Set to True by widen_scope for a full FAISS index scan (level 3)
    full_faiss_scan: bool


class EchoState(TypedDict):
    """
    The state carrier for the LangGraph RSE graph.
    Every node reads from this dict and returns a partial update.
    LangGraph merges updates via TypedDict field-by-field.
    """

    # Raw query from the user
    user_query: str

    # Full conversation history for the current session
    # List of dicts: [{"role": "human", "content": "..."}, {"role": "ai", "content": "..."}]
    conversation_history: List[Dict[str, str]]

    # Parsed structured intent — filled by parse_intent node
    parsed_intent: Optional[ParsedIntent]

    # Results from Postgres search — list of row dicts
    postgres_results: List[Dict[str, Any]]

    # Results from FAISS semantic search — list of (memory_id, distance) tuples
    faiss_results: List[Dict[str, Any]]

    # Fetched attachment full text — from Redis cache or live extraction
    attachment_content: Optional[str]

    # External API results — placeholder for future live data enrichment
    api_results: List[Dict[str, Any]]

    # Quality signal after evaluate_quality node runs
    # "strong" — enough relevant results, proceed to synthesize
    # "weak"   — some results but confidence is low, try widening
    # "empty"  — no results at all
    result_quality: str

    # Number of retrieval attempts made so far (max 3 before no_results_found)
    attempt_count: int

    # The final synthesised answer returned to the user
    final_answer: str

    # Set to True when all widen attempts are exhausted with no results
=======
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
>>>>>>> 4842d1a3060d3dea11ed107e06e4212e96c74fb4
    no_results: bool
