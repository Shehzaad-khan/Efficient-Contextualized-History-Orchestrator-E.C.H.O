"""
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
    graph.add_conditional_edges(
        "evaluate_quality",
        route_after_evaluate_quality,
        {
            "check_attachments": "check_attachments",
            "widen_scope":       "widen_scope",
            "no_results_found":  "no_results_found",
        },
    )

    # ── widen_scope always loops back to postgres_search ─────────────────────
    graph.add_edge("widen_scope", "postgres_search")

    # ── Conditional: check_attachments → fetch_attachment or synthesize ───────
    graph.add_conditional_edges(
        "check_attachments",
        route_after_check_attachments,
        {
            "fetch_attachment": "fetch_attachment",
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
