"""
All 9 LangGraph node implementations for the RSE pipeline.

This module is the single import point for the graph assembly in
retrieval_engine.py. Nodes that are fully implemented this phase:
  - parse_intent    (LLM Call 1 — query_parser.py)
  - postgres_search (SQL — search_coordinator.py)
  - faiss_search    (stub — search_coordinator.py)
  - evaluate_quality (unconditionally returns 'strong' this phase)
  - widen_scope     (full logic from architecture Section 10.3 Node 5)
  - check_attachments (routing signal logic)
  - fetch_attachment (stub)
  - synthesize      (stub)
  - no_results_found (structured message)
"""
import logging
from datetime import datetime, timedelta

from rse.state import EchoState
from rse.query_parser import parse_intent
from rse.search_coordinator import postgres_search, faiss_search

logger = logging.getLogger(__name__)


# ── Re-export real implementations ──────────────────────────────────────────
# parse_intent and the two search nodes live in their own modules and are
# imported here so retrieval_engine.py only needs to import from graph_nodes.

def node_parse_intent(state: EchoState) -> dict:
    """
    Node 1 — parse_intent.

    LLM Call 1: parses the user query and conversation history into a
    structured ParsedIntent JSON object. Calls Gemini 2.5 Flash (or the
    configured provider). Falls back gracefully on parse failure.

    Args:
        state: EchoState carrying user_query and conversation_history.

    Returns:
        Partial state dict with parsed_intent.
    """
    logger.info("NODE: parse_intent")
    return parse_intent(state)


def node_postgres_search(state: EchoState) -> dict:
    """
    Node 2 — postgres_search.

    Executes parameterised SQL against Neon PostgreSQL. Dynamically applies
    source, time, and keyword filters from parsed_intent.

    Args:
        state: EchoState carrying parsed_intent.

    Returns:
        Partial state dict with postgres_results.
    """
    logger.info("NODE: postgres_search")
    return postgres_search(state)


def node_faiss_search(state: EchoState) -> dict:
    """
    Node 3 — faiss_search.

    Semantic similarity search stub. Returns empty faiss_results this phase.
    Real implementation requires Mir's faiss_manager and ENP embeddings.

    Args:
        state: EchoState carrying parsed_intent and postgres_results.

    Returns:
        Partial state dict with faiss_results (empty list this phase).
    """
    logger.info("NODE: faiss_search")
    return faiss_search(state)


def node_evaluate_quality(state: EchoState) -> dict:
    """
    Node 4 — evaluate_quality.

    Deterministic quality check with five sequential checks. This phase stub
    unconditionally sets result_quality='strong' so the graph can be tested
    end-to-end before real embeddings exist.

    Real logic (Phase 3):
      1. Empty check: len(merged_results) == 0 → 'empty'
      2. Source match: requested source not in results → 'empty'
      3. Time window: no results within the requested window → 'weak'
      4. Minimum count: fewer than 2 results → 'weak'
      5. Top result similarity: results[0].similarity_score < 0.35 → 'weak'
      Otherwise → 'strong'

    Args:
        state: EchoState carrying postgres_results and faiss_results.

    Returns:
        Partial state dict with result_quality='strong'.
    """
    logger.info("NODE: evaluate_quality (stub — unconditionally strong)")
    return {"result_quality": "strong"}


def node_widen_scope(state: EchoState) -> dict:
    """
    Node 5 — widen_scope.

    Called when evaluate_quality returns weak/empty and attempt_count < 3.
    Each attempt widens one parameter and the graph loops back to postgres_search.

    Attempt 1: widen time window by 4 days, or open all sources if no time filter.
    Attempt 2: remove time filter, open all sources, keep only first keyword.
    Attempt 3: set skip_postgres_filter=True and full_faiss_scan=True.

    Args:
        state: EchoState carrying parsed_intent and attempt_count.

    Returns:
        Partial state dict with updated parsed_intent and incremented attempt_count.
    """
    logger.info("NODE: widen_scope")
    attempt = state.get("attempt_count", 0)
    intent = dict(state.get("parsed_intent", {}))

    if attempt == 0:
        # Attempt 1: widen time window or open sources
        if intent.get("time_filter"):
            try:
                original_dt = datetime.fromisoformat(intent["time_filter"])
                intent["time_filter"] = (original_dt - timedelta(days=4)).isoformat()
                logger.info("widen_scope attempt 1: time window extended by 4 days")
            except ValueError:
                intent["time_filter"] = None
                logger.info("widen_scope attempt 1: invalid time_filter cleared")
        elif intent.get("sources") not in [["gmail", "chrome", "youtube"], ["all"]]:
            intent["sources"] = ["gmail", "chrome", "youtube"]
            logger.info("widen_scope attempt 1: sources opened to all")
        else:
            # Sources already open, trim query to first keyword
            query_clean = intent.get("query_clean", "")
            intent["query_clean"] = query_clean.split()[0] if query_clean else query_clean
            logger.info("widen_scope attempt 1: query trimmed to first keyword")

    elif attempt == 1:
        # Attempt 2: remove all filters, core keyword only
        intent["time_filter"] = None
        intent["sources"] = ["gmail", "chrome", "youtube"]
        query_clean = intent.get("query_clean", "")
        intent["query_clean"] = query_clean.split()[0] if query_clean else query_clean
        logger.info("widen_scope attempt 2: all filters removed, core keyword only")

    elif attempt >= 2:
        # Attempt 3: bypass postgres filter, full FAISS scan
        intent["skip_postgres_filter"] = True
        intent["full_faiss_scan"] = True
        logger.info("widen_scope attempt 3: skip_postgres_filter and full_faiss_scan enabled")

    return {
        "parsed_intent": intent,
        "attempt_count": attempt + 1,
    }


def node_check_attachments(state: EchoState) -> dict:
    """
    Node 6 — check_attachments.

    Inspects top-3 results for attachment presence. Routing is handled by
    route_after_check_attachments in graph_routing.py. This node itself is a
    pure pass-through — its only role is to be a named routing point.

    Args:
        state: EchoState carrying postgres_results and parsed_intent.

    Returns:
        Unchanged state (empty dict — no fields to update).
    """
    logger.info("NODE: check_attachments")
    return {}


def node_fetch_attachment(state: EchoState) -> dict:
    """
    Node 7 — fetch_attachment.

    STUB for this phase.

    Real implementation (Phase 3): calls Gmail API to fetch attachment binary,
    extracts text using PyPDF2/pdfplumber, caches result in Redis (1-hour TTL).
    Binary file is never stored permanently.

    Args:
        state: EchoState carrying postgres_results and parsed_intent.

    Returns:
        Partial state dict with attachment_content=None (stub).
    """
    logger.info("NODE: fetch_attachment (stub)")
    return {"attachment_content": None}


def node_synthesize(state: EchoState) -> dict:
    """
    Node 8 — synthesize.

    STUB for this phase.

    Real implementation (Phase 3, LLM Call 2): assembles context from top-10
    re-ranked results, attachment_content, and conversation history. Calls the
    configured synthesizer LLM to generate a readable answer with source
    citations and temporal context.

    Args:
        state: EchoState carrying postgres_results, faiss_results,
               attachment_content, and conversation_history.

    Returns:
        Partial state dict with final_answer placeholder.
    """
    logger.info("NODE: synthesize (stub)")
    postgres_results = state.get("postgres_results", [])
    count = len(postgres_results)
    query = state.get("user_query", "")
    final_answer = (
        f"[STUB] Retrieved {count} candidate(s) for query: '{query}'. "
        "Synthesis will be implemented in Phase 3."
    )
    return {"final_answer": final_answer, "no_results": False}


def node_no_results_found(state: EchoState) -> dict:
    """
    Node 9 — no_results_found.

    Triggered after 3 failed widen_scope attempts. Returns a structured
    descriptive message explaining what was searched and suggesting alternatives.

    Args:
        state: EchoState carrying parsed_intent and attempt_count.

    Returns:
        Partial state dict with final_answer and no_results=True.
    """
    logger.info("NODE: no_results_found")
    intent = state.get("parsed_intent", {})
    query = intent.get("original_query", state.get("user_query", ""))
    sources = intent.get("sources", [])
    time_filter = intent.get("time_filter")

    sources_str = ", ".join(sources) if sources else "all sources"
    time_str = f" from around {time_filter}" if time_filter else ""

    message = (
        f"No results found for '{query}'{time_str} across {sources_str}. "
        "Echo searched progressively broader filters across 3 attempts. "
        "Suggestions: try a shorter keyword, remove the time constraint, "
        "or check that the relevant content has been ingested and processed."
    )
    return {"final_answer": message, "no_results": True}
