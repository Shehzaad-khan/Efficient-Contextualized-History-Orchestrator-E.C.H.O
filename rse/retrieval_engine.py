"""
<<<<<<< HEAD
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
=======
retrieval_engine.py — LangGraph RSE entry point.

Assembles the complete 9-node stateful graph and exposes run_query() as the
single callable interface for the UI layer and API gateway.

Graph topology (from architecture Section 10.4):
    parse_intent
        ↓
    postgres_search
        ↓
    faiss_search
        ↓
    evaluate_quality
        ↓
    [strong] → check_attachments → [has attachment] → fetch_attachment → synthesize → END
                                 → [no attachment]  →                   synthesize → END
    [weak/empty + attempts < 3] → widen_scope → postgres_search (loop)
    [weak/empty + attempts >= 3] → no_results_found → END
"""
import logging
import uuid
from typing import Any

from langgraph.graph import StateGraph, END

from rse.state import EchoState
from rse.graph_nodes import (
    node_parse_intent,
    node_postgres_search,
    node_faiss_search,
    node_evaluate_quality,
    node_widen_scope,
    node_check_attachments,
    node_fetch_attachment,
    node_synthesize,
    node_no_results_found,
)
from rse.graph_routing import (
    route_after_evaluate_quality,
    route_after_check_attachments,
)
from rse.conversation_memory import load_conversation_history, save_turn

logger = logging.getLogger(__name__)


def _build_graph() -> Any:
    """
    Assemble and compile the LangGraph RSE graph.

    Returns:
        A compiled LangGraph StateGraph ready for invocation.
    """
    graph = StateGraph(EchoState)

    # ── Register nodes ───────────────────────────────────────────────────────
    graph.add_node("parse_intent",      node_parse_intent)
    graph.add_node("postgres_search",   node_postgres_search)
    graph.add_node("faiss_search",      node_faiss_search)
    graph.add_node("evaluate_quality",  node_evaluate_quality)
    graph.add_node("widen_scope",       node_widen_scope)
    graph.add_node("check_attachments", node_check_attachments)
    graph.add_node("fetch_attachment",  node_fetch_attachment)
    graph.add_node("synthesize",        node_synthesize)
    graph.add_node("no_results_found",  node_no_results_found)

    # ── Linear edges ─────────────────────────────────────────────────────────
    graph.set_entry_point("parse_intent")
    graph.add_edge("parse_intent",     "postgres_search")
    graph.add_edge("postgres_search",  "faiss_search")
    graph.add_edge("faiss_search",     "evaluate_quality")

    # ── Conditional: evaluate_quality → strong/weak/empty ────────────────────
>>>>>>> 4842d1a3060d3dea11ed107e06e4212e96c74fb4
    graph.add_conditional_edges(
        "evaluate_quality",
        route_after_evaluate_quality,
        {
            "check_attachments": "check_attachments",
<<<<<<< HEAD
            "widen_scope": "widen_scope",
            "no_results_found": "no_results_found",
        },
    )

    # Widen scope loops back to postgres_search for a fresh attempt
    graph.add_edge("widen_scope", "postgres_search")

    # Conditional: check_attachments → fetch_attachment OR synthesize
=======
            "widen_scope":       "widen_scope",
            "no_results_found":  "no_results_found",
        },
    )

    # ── widen_scope always loops back to postgres_search ─────────────────────
    graph.add_edge("widen_scope", "postgres_search")

    # ── Conditional: check_attachments → fetch_attachment or synthesize ───────
>>>>>>> 4842d1a3060d3dea11ed107e06e4212e96c74fb4
    graph.add_conditional_edges(
        "check_attachments",
        route_after_check_attachments,
        {
            "fetch_attachment": "fetch_attachment",
<<<<<<< HEAD
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

    # Load conversation history for multi-turn context
    history = load_conversation_history(session_id)

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
        graph = get_graph()
        final_state = graph.invoke(initial_state)
    except Exception as e:
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
=======
            "synthesize":       "synthesize",
        },
    )

    # ── fetch_attachment feeds into synthesize ────────────────────────────────
    graph.add_edge("fetch_attachment", "synthesize")

    # ── Terminal edges ────────────────────────────────────────────────────────
    graph.add_edge("synthesize",       END)
    graph.add_edge("no_results_found", END)

    return graph.compile()


# Compile once at module import time — reused across all queries
_COMPILED_GRAPH = _build_graph()


def run_query(
    user_query: str,
    session_id: str | None = None,
) -> dict[str, Any]:
    """
    Execute the full RSE pipeline for a user query.

    Loads conversation history from PostgreSQL, runs the LangGraph graph,
    saves the completed turn to history, and returns the final state.

    Args:
        user_query: Natural language query from the user.
        session_id: Optional conversation session ID. A new UUID is generated
                    when not provided (i.e. single-turn query).

    Returns:
        Final EchoState dict containing final_answer, postgres_results,
        parsed_intent, and all other state fields.
    """
    if session_id is None:
        session_id = str(uuid.uuid4())
        logger.info("run_query: new session_id generated: %s", session_id)

    # Load conversation history before invoking the graph
    conversation_history = load_conversation_history(session_id)

    initial_state: EchoState = {
        "user_query":           user_query,
        "conversation_history": conversation_history,
        "parsed_intent":        {},
        "postgres_results":     [],
        "faiss_results":        [],
        "attachment_content":   None,
        "api_results":          [],
        "result_quality":       "empty",
        "attempt_count":        0,
        "final_answer":         "",
        "no_results":           False,
    }

    logger.info("run_query: starting graph for query=%r session=%s", user_query, session_id)

    try:
        final_state = _COMPILED_GRAPH.invoke(initial_state)
    except Exception as exc:
        logger.error("run_query: graph execution failed — %s", exc)
        final_state = dict(initial_state)
        final_state["final_answer"] = f"An error occurred during retrieval: {exc}"
        final_state["no_results"] = True

    # Persist turn to conversation history
    answer = final_state.get("final_answer", "")
    if answer:
        save_turn(session_id, user_query, answer)

    logger.info(
        "run_query: complete — no_results=%s answer_length=%d",
        final_state.get("no_results"),
        len(answer),
    )

    return final_state


# ── Module-level smoke test ───────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    import sys

    query = sys.argv[1] if len(sys.argv) > 1 else "find my TechCorp interview email"
    result = run_query(query)

    print("\n── PARSED INTENT ──────────────────────────────────────────")
    import json
    print(json.dumps(result.get("parsed_intent", {}), indent=2, default=str))

    print(f"\n── POSTGRES RESULTS: {len(result.get('postgres_results', []))} rows ──")
    for row in result.get("postgres_results", [])[:3]:
        print(f"  [{row.get('source_type')}] {row.get('title', '(no title)')[:80]}")

    print(f"\n── FINAL ANSWER ───────────────────────────────────────────")
    print(result.get("final_answer", "(no answer)"))
>>>>>>> 4842d1a3060d3dea11ed107e06e4212e96c74fb4
