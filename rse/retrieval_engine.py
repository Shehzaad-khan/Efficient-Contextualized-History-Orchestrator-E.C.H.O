"""
retrieval_engine.py — RSE Module
Echo Personal Memory System

LangGraph StateGraph assembly and run_query() entry point.

The 9-node graph:
  parse_intent → [conditional] → postgres_search → faiss_search
  → evaluate_quality → [conditional] → check_attachments / widen_scope / no_results_found
  → [conditional] → fetch_attachment / synthesize
  → synthesize / no_results_found [END]

Graph compiles once at import time (singleton pattern).
run_query() is the only public API — called by the FastAPI endpoint.
"""

import logging
import uuid
import json
from pathlib import Path
import time
from typing import Any, Dict, Optional

from langgraph.graph import END, StateGraph

from .conversation_memory import load_conversation_history, save_turn
from .graph_nodes import (
    check_attachments,
    evaluate_quality,
    faiss_search,
    fetch_attachment,
    no_results_found,
    parse_intent,
    postgres_search,
    synthesize,
    widen_scope,
)
from .graph_routing import (
    route_after_check_attachments,
    route_after_evaluate_quality,
    route_after_parse_intent,
)
from .state import EchoState

logger = logging.getLogger(__name__)
_DEBUG_LOG_PATH = Path(__file__).resolve().parents[1] / "debug-20c712.log"


def _debug_log(run_id: str, hypothesis_id: str, location: str, message: str, data: dict):
    payload = {
        "sessionId": "20c712",
        "runId": run_id,
        "hypothesisId": hypothesis_id,
        "location": location,
        "message": message,
        "data": data,
        "timestamp": int(time.time() * 1000),
    }
    try:
        with _DEBUG_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=True) + "\n")
    except Exception:
        pass


# ── Graph construction ────────────────────────────────────────────────────────

def _build_graph() -> Any:
    """
    Build and compile the LangGraph StateGraph.
    Called once at module import — result is cached as a module-level singleton.
    """
    graph = StateGraph(EchoState)

    # Register all 9 nodes
    graph.add_node("parse_intent", parse_intent)
    graph.add_node("postgres_search", postgres_search)
    graph.add_node("faiss_search", faiss_search)
    graph.add_node("evaluate_quality", evaluate_quality)
    graph.add_node("widen_scope", widen_scope)
    graph.add_node("check_attachments", check_attachments)
    graph.add_node("fetch_attachment", fetch_attachment)
    graph.add_node("synthesize", synthesize)
    graph.add_node("no_results_found", no_results_found)

    # Entry point
    graph.set_entry_point("parse_intent")

    # Conditional: parse_intent → postgres_search OR synthesize (ambiguous)
    graph.add_conditional_edges(
        "parse_intent",
        route_after_parse_intent,
        {
            "postgres_search": "postgres_search",
            "synthesize": "synthesize",
        },
    )

    # Linear: postgres_search → faiss_search → evaluate_quality
    graph.add_edge("postgres_search", "faiss_search")
    graph.add_edge("faiss_search", "evaluate_quality")

    # Conditional: evaluate_quality → check_attachments / widen_scope / no_results_found
    graph.add_conditional_edges(
        "evaluate_quality",
        route_after_evaluate_quality,
        {
            "check_attachments": "check_attachments",
            "widen_scope": "widen_scope",
            "no_results_found": "no_results_found",
        },
    )

    # Widen scope loops back to postgres_search for a fresh attempt
    graph.add_edge("widen_scope", "postgres_search")

    # Conditional: check_attachments → fetch_attachment OR synthesize
    graph.add_conditional_edges(
        "check_attachments",
        route_after_check_attachments,
        {
            "fetch_attachment": "fetch_attachment",
            "synthesize": "synthesize",
        },
    )

    # fetch_attachment feeds directly into synthesize
    graph.add_edge("fetch_attachment", "synthesize")

    # Terminal nodes
    graph.add_edge("synthesize", END)
    graph.add_edge("no_results_found", END)

    compiled = graph.compile()
    logger.info("RSE LangGraph compiled — 9 nodes, all edges wired")
    return compiled


# Module-level singleton — graph is compiled once
_graph = None


def get_graph():
    global _graph
    if _graph is None:
        _graph = _build_graph()
    return _graph


# ── Public API ────────────────────────────────────────────────────────────────

def run_query(
    user_query: str,
    session_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Execute a complete retrieval cycle for a user query.

    Args:
        user_query:  The raw natural language query from the user.
        session_id:  Conversation session ID for multi-turn context.
                     If None, a new session ID is generated.

    Returns:
        Dict containing:
            final_answer   — synthesised text response
            session_id     — for use in subsequent turns
            no_results     — True if no matching items found
            parsed_intent  — the structured intent (useful for debugging)
            result_count   — number of postgres results found
    """
    if not session_id:
        session_id = str(uuid.uuid4())
        logger.info(f"run_query: new session created — session_id={session_id}")
    run_id = f"pre-fix-{int(time.time() * 1000)}"
    # #region agent log
    _debug_log(
        run_id=run_id,
        hypothesis_id="H2",
        location="rse/retrieval_engine.py:run_query",
        message="run_query entry",
        data={"session_id_present": bool(session_id), "query_len": len(user_query or "")},
    )
    # #endregion

    # Load conversation history for multi-turn context
    history = load_conversation_history(session_id)
    # #region agent log
    _debug_log(
        run_id=run_id,
        hypothesis_id="H2",
        location="rse/retrieval_engine.py:run_query",
        message="conversation history loaded",
        data={"history_count": len(history) if isinstance(history, list) else -1},
    )
    # #endregion

    # Build initial state
    initial_state: EchoState = {
        "user_query": user_query,
        "conversation_history": history,
        "parsed_intent": None,
        "postgres_results": [],
        "faiss_results": [],
        "attachment_content": None,
        "api_results": [],
        "result_quality": "empty",
        "attempt_count": 0,
        "final_answer": "",
        "no_results": False,
    }

    logger.info(f"run_query: executing — query='{user_query}' session={session_id}")

    try:
        # #region agent log
        _debug_log(
            run_id=run_id,
            hypothesis_id="H3",
            location="rse/retrieval_engine.py:run_query",
            message="invoking langgraph",
            data={},
        )
        # #endregion
        graph = get_graph()
        final_state = graph.invoke(initial_state)
    except Exception as e:
        # #region agent log
        _debug_log(
            run_id=run_id,
            hypothesis_id="H3",
            location="rse/retrieval_engine.py:run_query",
            message="graph invocation exception",
            data={"error_type": type(e).__name__, "error": str(e)},
        )
        # #endregion
        logger.error(f"run_query graph error: {e}")
        return {
            "final_answer": "An error occurred while processing your query. Please try again.",
            "session_id": session_id,
            "no_results": True,
            "parsed_intent": None,
            "result_count": 0,
            "error": str(e),
        }

    answer = final_state.get("final_answer", "")
    no_results = final_state.get("no_results", False)
    parsed_intent = final_state.get("parsed_intent")
    result_count = len(final_state.get("postgres_results", []))

    # Persist this turn to conversation memory
    if answer and not no_results:
        save_turn(session_id, user_query, answer)

    logger.info(
        f"run_query complete: result_count={result_count} "
        f"no_results={no_results} session={session_id}"
    )

    return {
        "final_answer": answer,
        "session_id": session_id,
        "no_results": no_results,
        "parsed_intent": parsed_intent,
        "result_count": result_count,
    }
