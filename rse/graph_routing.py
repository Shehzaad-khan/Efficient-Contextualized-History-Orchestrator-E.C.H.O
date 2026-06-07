"""
<<<<<<< HEAD
graph_routing.py — RSE Module
Echo Personal Memory System

Pure Python conditional edge routing functions for the LangGraph graph.
No LLM calls. No DB calls. Only reads from EchoState.

Design: routing functions receive state and return the name of the next node
as a string. These are passed as the `path_map` argument to
StateGraph.add_conditional_edges().
"""

import logging

from .config import MAX_WIDEN_ATTEMPTS
from .state import EchoState
=======
Conditional edge routing functions for the LangGraph RSE graph.

Each function receives the current EchoState and returns the name of the next
node to route to. All routing is pure Python — no LLM calls here.
"""
import logging
from rse.state import EchoState
from rse.config import MAX_WIDEN_ATTEMPTS
>>>>>>> 4842d1a3060d3dea11ed107e06e4212e96c74fb4

logger = logging.getLogger(__name__)


<<<<<<< HEAD
def route_after_parse_intent(state: EchoState) -> str:
    """
    After parse_intent: check if query is ambiguous.

    Ambiguous queries skip retrieval and go straight to synthesize,
    which will return a clarification request.
    """
    parsed_intent = state.get("parsed_intent", {})
    if parsed_intent.get("is_ambiguous"):
        logger.info("route_after_parse_intent: ambiguous → synthesize")
        return "synthesize"
    logger.info("route_after_parse_intent: clear → postgres_search")
    return "postgres_search"


def route_after_evaluate_quality(state: EchoState) -> str:
    """
    After evaluate_quality: branch on result quality and attempt count.

    "strong"             → check_attachments (proceed to synthesis)
    "weak" or "empty"    + attempts remaining → widen_scope
    "weak" or "empty"    + no attempts left   → no_results_found
=======
def route_after_evaluate_quality(state: EchoState) -> str:
    """
    Route after evaluate_quality based on result_quality and attempt_count.

    - strong → check_attachments
    - weak/empty and attempts remaining → widen_scope
    - weak/empty and no attempts remaining → no_results_found

    Args:
        state: Current EchoState.

    Returns:
        Name of the next node.
>>>>>>> 4842d1a3060d3dea11ed107e06e4212e96c74fb4
    """
    quality = state.get("result_quality", "empty")
    attempt_count = state.get("attempt_count", 0)

<<<<<<< HEAD
    if quality == "strong":
        logger.info("route_after_evaluate_quality: strong → check_attachments")
        return "check_attachments"

    if attempt_count < MAX_WIDEN_ATTEMPTS:
        logger.info(
            f"route_after_evaluate_quality: {quality} + attempt {attempt_count} "
            f"< max {MAX_WIDEN_ATTEMPTS} → widen_scope"
        )
        return "widen_scope"

    logger.info(
        f"route_after_evaluate_quality: {quality} + attempts exhausted "
        f"({attempt_count}/{MAX_WIDEN_ATTEMPTS}) → no_results_found"
    )
=======
    logger.info(
        "route_after_evaluate_quality: quality=%s attempt_count=%d",
        quality,
        attempt_count,
    )

    if quality == "strong":
        return "check_attachments"

    # weak or empty
    if attempt_count < MAX_WIDEN_ATTEMPTS:
        return "widen_scope"

>>>>>>> 4842d1a3060d3dea11ed107e06e4212e96c74fb4
    return "no_results_found"


def route_after_check_attachments(state: EchoState) -> str:
    """
<<<<<<< HEAD
    After check_attachments: decide if we need to fetch attachment text.

    Routes to fetch_attachment only if:
      1. parsed_intent.fetch_attachment = True
    AND
      2. At least one of the top-3 postgres_results has has_attachments=True

    Otherwise routes directly to synthesize.
    """
    parsed_intent = state.get("parsed_intent", {})
    fetch_requested = parsed_intent.get("fetch_attachment", False)

    if not fetch_requested:
        logger.info("route_after_check_attachments: no attachment requested → synthesize")
        return "synthesize"

    postgres_results = state.get("postgres_results", [])
    top3 = postgres_results[:3]
    has_attachments_in_top = any(r.get("has_attachments") for r in top3)

    if has_attachments_in_top:
        logger.info("route_after_check_attachments: attachment found in top-3 → fetch_attachment")
        return "fetch_attachment"

    logger.info("route_after_check_attachments: attachment requested but none in top-3 → synthesize")
=======
    Route after check_attachments.

    Routes to fetch_attachment when:
        - parsed_intent.fetch_attachment is True, AND
        - at least one top-3 postgres result has has_attachments=True

    Otherwise routes directly to synthesize.

    Args:
        state: Current EchoState.

    Returns:
        Name of the next node.
    """
    intent = state.get("parsed_intent", {})
    postgres_results = state.get("postgres_results", [])
    faiss_results = state.get("faiss_results", [])

    fetch_attachment_requested: bool = intent.get("fetch_attachment", False)

    # Determine top-3 results by FAISS score when available, else postgres order
    if faiss_results:
        faiss_ids = {mid for mid, _ in faiss_results[:3]}
        top3 = [r for r in postgres_results if str(r.get("memory_id")) in faiss_ids][:3]
    else:
        top3 = postgres_results[:3]

    has_attachments_in_top3 = any(r.get("has_attachments") for r in top3)

    logger.info(
        "route_after_check_attachments: fetch_requested=%s has_attachments_in_top3=%s",
        fetch_attachment_requested,
        has_attachments_in_top3,
    )

    if fetch_attachment_requested and has_attachments_in_top3:
        return "fetch_attachment"

>>>>>>> 4842d1a3060d3dea11ed107e06e4212e96c74fb4
    return "synthesize"
