"""
Conditional edge routing functions for the LangGraph RSE graph.

Each function receives the current EchoState and returns the name of the next
node to route to. All routing is pure Python — no LLM calls here.
"""
import logging
from rse.state import EchoState
from rse.config import MAX_WIDEN_ATTEMPTS

logger = logging.getLogger(__name__)


def route_after_evaluate_quality(state: EchoState) -> str:
    """
    Route after evaluate_quality based on result_quality and attempt_count.

    - strong → rerank_cross_encoder
    - weak/empty and attempts remaining → widen_scope (loops back to both
      search branches)
    - weak/empty and no attempts remaining → no_results_found

    Args:
        state: Current EchoState.

    Returns:
        Name of the next node.
    """
    quality = state.get("result_quality", "empty")
    attempt_count = state.get("attempt_count", 0)

    logger.info(
        "route_after_evaluate_quality: quality=%s attempt_count=%d",
        quality,
        attempt_count,
    )

    if quality == "strong":
        return "rerank_cross_encoder"

    # weak or empty
    if attempt_count < MAX_WIDEN_ATTEMPTS:
        return "widen_scope"

    return "no_results_found"


def route_after_check_attachments(state: EchoState) -> str:
    """
    Route after check_attachments (architecture §10.3 Node 6) — three-way:

    - fetch_attachment when parsed_intent.fetch_attachment is True AND a
      top-3 ranked result has attachments
    - fetch_api when parsed_intent.fetch_api is True (live-freshness queries:
      "latest", "today", "just received")
    - synthesize otherwise

    When both flags fire, the attachment wins — its content answers the
    specific document request; freshness is already covered by the ranked set.

    Args:
        state: Current EchoState.

    Returns:
        Name of the next node.
    """
    intent = state.get("parsed_intent", {})
    ranked_results = state.get("ranked_results", [])

    fetch_attachment_requested: bool = intent.get("fetch_attachment", False)
    fetch_api_requested: bool = intent.get("fetch_api", False)
    has_attachments_in_top3 = any(r.get("has_attachments") for r in ranked_results[:3])

    logger.info(
        "route_after_check_attachments: attachment=%s api=%s has_attachments_in_top3=%s",
        fetch_attachment_requested,
        fetch_api_requested,
        has_attachments_in_top3,
    )

    if fetch_attachment_requested and has_attachments_in_top3:
        return "fetch_attachment"

    if fetch_api_requested:
        return "fetch_api"

    return "synthesize"
