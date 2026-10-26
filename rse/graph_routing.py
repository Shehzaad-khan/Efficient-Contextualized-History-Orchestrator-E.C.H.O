"""
Conditional edge routing functions for the LangGraph RSE graph.

Each function receives the current EchoState and returns the name of the next
node to route to. All routing is pure Python — no LLM calls here.

route_after_evaluate_quality runs inside one search (rse/search_tool.py run_search);
route_after_agent and route_after_search steer the agent loop graph.
"""
import logging

from rse.config import MAX_WIDEN_ATTEMPTS
from rse.state import EchoState

logger = logging.getLogger(__name__)


def route_after_evaluate_quality(state: EchoState) -> str:
    """
    Route after evaluate_quality based on result_quality and attempt_count.

    - strong → rerank_cross_encoder
    - weak/empty and attempts remaining → widen_scope (loops back to both
      search branches)
    - weak and no attempts remaining → rerank_cross_encoder: the closest
      matches Echo has are still shown (the synthesizer is told to say plainly
      what it found and what it didn't), instead of "nothing found" while
      real candidates exist
    - empty and no attempts remaining → no_results_found

    Args:
        state: Current EchoState.

    Returns:
        Name of the next node.
    """
    quality = state.get("result_quality", "empty")
    attempt_count = state.get("attempt_count", 0)

    if quality == "strong":
        route = "rerank_cross_encoder"
    elif attempt_count < MAX_WIDEN_ATTEMPTS:
        route = "widen_scope"
    elif quality == "weak" and state.get("merged_candidates"):
        route = "rerank_cross_encoder"
    else:
        route = "no_results_found"

    logger.info("route_after_evaluate_quality: quality=%s attempt_count=%d → %s",
                quality, attempt_count, route)
    return route


def route_after_agent(state: EchoState) -> str:
    """
    Route after the agent call: search when Luna asked for searches, else finish.

    node_agent leaves pending_calls empty on every call that carried no tool (the
    cap) and on a reply with no tool call, so a model that ignores the cap still
    ends here.
    """
    route = "search" if state.get("pending_calls") else "finish"
    logger.info("route_after_agent: llm_calls=%d searches=%d → %s",
                state.get("llm_calls", 0), state.get("search_count", 0), route)
    return route


def route_after_search(state: EchoState) -> str:
    """
    Route after a round of searches: back to Luna to read the results, or straight to finish
    when the agent call itself failed (then the results are listed without Luna's answer).
    """
    route = "finish" if state.get("agent_failed") else "agent"
    logger.info("route_after_search: → %s", route)
    return route
