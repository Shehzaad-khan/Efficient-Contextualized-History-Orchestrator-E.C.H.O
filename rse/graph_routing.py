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

    - strong → check_attachments
    - weak/empty and attempts remaining → widen_scope
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
        return "check_attachments"

    # weak or empty
    if attempt_count < MAX_WIDEN_ATTEMPTS:
        return "widen_scope"

    return "no_results_found"


def route_after_check_attachments(state: EchoState) -> str:
    """
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

    return "synthesize"
