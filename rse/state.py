"""
EchoState — the typed state object shared across all LangGraph nodes.

Every node reads from this dict and returns a partial dict of updated keys.
LangGraph merges partial updates; fields not returned stay unchanged. There are
no reducers (last write wins), so a node that changes a list returns the whole
new list.

Flow through the state (graph built in rse/retrieval_engine.py, A26 agent loop):

    agent    → agent_messages, llm_calls, pending_calls (Luna's tool calls, if any)
    search   → found_items, neighbor_items, search_count, parsed_intent (last
               search), last_search_count, agent_messages (tool results)
    finish   → final_answer, answer_markdown, answer_sources, no_results

    agent ⇄ search repeats up to MAX_SEARCHES searches; then finish ends the run.

Inside one search (rse/search_tool.py run_search) the same pipeline as before
runs on a private EchoState of its own: parsed_intent → anchor_time/anchor_item
→ postgres_results + faiss_results → merged_candidates → result_quality
(→ widen_scope → attempt_count) → ranked_results → neighbor_items →
attachment_content. Those keys are working keys of one search, not of the graph.
"""
from typing import Any, Optional

from langchain_core.messages import BaseMessage
from typing_extensions import TypedDict


class ParsedIntent(TypedDict, total=False):
    """The arguments of one search, as the search stages read them.

    Built from the tool call's arguments by rse/query_parser.py (the checker)."""
    sources: list[str]          # subset of ['gmail', 'chrome', 'youtube']; all three when broad
    time_filter: Optional[str]  # ISO-8601 date: keep items created on/after it; None = no bound
    fetch_attachment: bool       # True when user wants file content (tool argument read_attachment)
    fetch_api: bool              # unused since A45 (live poll dropped); kept False for the search stages
    query_clean: str             # Distilled core topic, stripped of meta-language
    query_variants: list[str]    # query_clean + up to two semantic variations/synonyms
    time_anchor_query: Optional[str]  # Reference item for relative time ("interview email")
    time_relation: Optional[str]      # 'after' | 'before' | None
    is_ambiguous: bool           # True if the intent is the fallback for an unusable call
    original_query: str          # Verbatim user query
    skip_postgres_filter: bool   # True on widen attempt 3: bypass dynamic WHERE clauses
    full_faiss_scan: bool        # True on widen attempt 3: widen FAISS k per variant


class EchoState(TypedDict, total=False):
    """
    Carrier for all data flowing between LangGraph nodes in the RSE pipeline.

    Fields are total=False so nodes can return partial updates without supplying
    every key. run_query (rse/retrieval_engine.py) fills every graph key with a
    default before the graph starts.
    """
    # ── Input ─────────────────────────────────────────────────────────────────
    user_query: str
    conversation_history: list[BaseMessage]   # loaded by rse/conversation_memory.py

    # ── Agent loop (A26) ──────────────────────────────────────────────────────
    agent_messages: list[BaseMessage]         # system + question, Luna's replies, tool results
    llm_calls: int                            # agent calls made so far (max MAX_SEARCHES + 1)
    pending_calls: list[dict[str, Any]]       # tool calls from the last agent reply: {id, name, args}
    agent_failed: bool                        # the agent call raised: finish with the deterministic list
    search_count: int                         # searches run so far (max MAX_SEARCHES)
    found_items: list[dict[str, Any]]         # every item shown to Luna, in number order: [n] = index + 1
    last_search_count: int                    # items the latest search returned (progress line)

    # ── Parsed intent (the last search's arguments; also returned to the UI) ──
    parsed_intent: ParsedIntent

    # ── Working keys of ONE search (rse/search_tool.py run_search) ─────────────
    anchor_time: Optional[str]              # ISO timestamp of the resolved anchor item
    anchor_item: Optional[dict[str, Any]]   # Resolved anchor row (memory_id, title, created_at)
    postgres_results: list[dict[str, Any]]      # keyword branch, FTS-rank ordered
    faiss_results: list[tuple[str, float]]      # [(memory_id, cosine_similarity), ...] best first
    merged_candidates: list[dict[str, Any]]     # RRF-fused + hydrated candidate pool
    ranked_results: list[dict[str, Any]]        # cross-encoder + effort final ranking
    neighbor_items: dict[str, list[dict[str, Any]]]  # memory_id → neighboring items (all searches)
    attachment_content: Optional[str]           # extracted attachment text (read_attachment)
    attachment_item_id: Optional[str]           # memory_id the attachment text belongs to
    result_quality: str    # 'strong' | 'weak' | 'empty'
    attempt_count: int     # widen_scope passes (per search; summed over the query for the log)

    # ── Output ────────────────────────────────────────────────────────────────
    final_answer: str                          # readable chat answer, citations as text pills
    answer_markdown: str                       # same answer with validated ⟦n⟧ markers (pill UI)
    answer_sources: list[dict[str, Any]]       # cited items: anchor, date_label, title, link …
    no_results: bool
