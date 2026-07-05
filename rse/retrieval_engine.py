"""
retrieval_engine.py — LangGraph RSE entry point.

Assembles the hybrid retrieval graph and exposes run_query() as the single
callable interface for the UI layer and API gateway.

Graph topology (hybrid redesign):

    parse_intent                       ← LLM Call 1 (query expansion + anchor fields)
        ↓
    resolve_time_anchor                ← deterministic T_anchor lookup (no-op if unanchored)
        ↓          ↓
    postgres_   faiss_semantic_        ← PARALLEL fan-out: keyword (FTS) branch and
    keyword_    search                   semantic (multi-variant FAISS) branch run
    search         ↓                     in the same LangGraph superstep
        ↓          ↓
    merge_and_rrf                      ← Reciprocal Rank Fusion (k=60) + hydration
        ↓
    evaluate_quality                   ← 5 deterministic checks, no LLM
        ↓
    [strong]      → rerank_cross_encoder  ← local ms-marco cross-encoder + effort score
    [weak/empty]  → widen_scope (≤3)      → loops back to BOTH search branches
    [exhausted]   → no_results_found      → END
        ↓
    extend_neighborhood                ← thread / session context for top 3
        ↓
    check_attachments → [fetch_attachment] → synthesize   ← LLM Call 2
                                                ↓
                                               END

LLM calls per query: exactly 2, regardless of loop iterations.
"""
import logging
import uuid
from typing import Any

from langgraph.graph import StateGraph, END

from rse.state import EchoState
from rse.graph_nodes import (
    node_parse_intent,
    node_resolve_time_anchor,
    node_postgres_keyword_search,
    node_faiss_semantic_search,
    node_merge_and_rrf,
    node_evaluate_quality,
    node_widen_scope,
    node_rerank_cross_encoder,
    node_extend_neighborhood,
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
    graph.add_node("parse_intent",            node_parse_intent)
    graph.add_node("resolve_time_anchor",     node_resolve_time_anchor)
    graph.add_node("postgres_keyword_search", node_postgres_keyword_search)
    graph.add_node("faiss_semantic_search",   node_faiss_semantic_search)
    graph.add_node("merge_and_rrf",           node_merge_and_rrf)
    graph.add_node("evaluate_quality",        node_evaluate_quality)
    graph.add_node("widen_scope",             node_widen_scope)
    graph.add_node("rerank_cross_encoder",    node_rerank_cross_encoder)
    graph.add_node("extend_neighborhood",     node_extend_neighborhood)
    graph.add_node("check_attachments",       node_check_attachments)
    graph.add_node("fetch_attachment",        node_fetch_attachment)
    graph.add_node("synthesize",              node_synthesize)
    graph.add_node("no_results_found",        node_no_results_found)

    # ── Entry + anchor resolution ────────────────────────────────────────────
    graph.set_entry_point("parse_intent")
    graph.add_edge("parse_intent", "resolve_time_anchor")

    # ── PARALLEL fan-out: both retrieval branches leave resolve_time_anchor ──
    # LangGraph runs nodes reachable in the same superstep concurrently; the
    # branches write disjoint state keys (postgres_results / faiss_results),
    # and merge_and_rrf is a fan-in that waits for both.
    graph.add_edge("resolve_time_anchor", "postgres_keyword_search")
    graph.add_edge("resolve_time_anchor", "faiss_semantic_search")
    graph.add_edge("postgres_keyword_search", "merge_and_rrf")
    graph.add_edge("faiss_semantic_search",   "merge_and_rrf")

    graph.add_edge("merge_and_rrf", "evaluate_quality")

    # ── Conditional: evaluate_quality → strong/weak/empty ────────────────────
    graph.add_conditional_edges(
        "evaluate_quality",
        route_after_evaluate_quality,
        {
            "rerank_cross_encoder": "rerank_cross_encoder",
            "widen_scope":          "widen_scope",
            "no_results_found":     "no_results_found",
        },
    )

    # ── widen_scope loops back to BOTH search branches (fan-out again) ────────
    graph.add_edge("widen_scope", "postgres_keyword_search")
    graph.add_edge("widen_scope", "faiss_semantic_search")

    # ── Post-ranking chain ────────────────────────────────────────────────────
    graph.add_edge("rerank_cross_encoder", "extend_neighborhood")
    graph.add_edge("extend_neighborhood",  "check_attachments")

    # ── Conditional: check_attachments → fetch_attachment or synthesize ───────
    graph.add_conditional_edges(
        "check_attachments",
        route_after_check_attachments,
        {
            "fetch_attachment": "fetch_attachment",
            "synthesize":       "synthesize",
        },
    )
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
        Final EchoState dict containing final_answer, ranked_results,
        parsed_intent, and all other state fields, plus session_id and
        result_count.
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
        "anchor_time":          None,
        "anchor_item":          None,
        "postgres_results":     [],
        "faiss_results":        [],
        "merged_candidates":    [],
        "ranked_results":       [],
        "neighbor_items":       {},
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

    result_count = len(
        final_state.get("ranked_results") or final_state.get("merged_candidates") or []
    )

    logger.info(
        "run_query: complete — no_results=%s answer_length=%d result_count=%d",
        final_state.get("no_results"),
        len(answer),
        result_count,
    )

    return {
        **final_state,
        "session_id": session_id,
        "result_count": result_count,
    }


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

    if result.get("anchor_item"):
        print("\n── TIME ANCHOR ────────────────────────────────────────────")
        print(f"  {result['anchor_item'].get('title')} @ {result.get('anchor_time')}")

    print(f"\n── RANKED RESULTS: {len(result.get('ranked_results', []))} ──")
    for row in result.get("ranked_results", [])[:5]:
        print(
            f"  [{row.get('source_type')}] {str(row.get('title', '(no title)'))[:70]} "
            f"(final={row.get('final_score', 0):.3f} ce={row.get('ce_score')} "
            f"rrf={row.get('rrf_score', 0):.4f})"
        )

    print(f"\n── FINAL ANSWER ───────────────────────────────────────────")
    print(result.get("final_answer", "(no answer)"))
