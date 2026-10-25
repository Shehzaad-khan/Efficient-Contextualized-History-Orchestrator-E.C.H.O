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
    [strong]            → rerank_cross_encoder  ← local ms-marco cross-encoder + effort score
    [weak/empty]        → widen_scope (≤3)      → loops back to BOTH search branches
    [weak, exhausted]   → rerank_cross_encoder  (closest matches, answered honestly)
    [empty, exhausted]  → no_results_found      → END
        ↓
    extend_neighborhood                ← thread / session context for top 3
        ↓
    check_attachments → [fetch_attachment | fetch_api] → synthesize   ← LLM Call 2
                                                            ↓
                                                           END

LLM calls per query: exactly 2, regardless of loop iterations.

run_query output (A27): final_answer (readable chat answer, citations as text
pills), answer_markdown (validated ⟦n⟧ markers for the pill UI),
answer_sources (cited items with their links), plus the rest of the state.
The compiled graph is built once at import and is safe to invoke from several
request threads at once (each invoke gets its own state).
Logs carry the session id, counts and timings only — never the query or answer.
"""
import logging
import time
import uuid
from typing import Any, Iterator

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
    node_fetch_api,
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
    graph.add_node("fetch_api",               node_fetch_api)
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

    # ── Conditional: check_attachments → fetch_attachment / fetch_api / synth ─
    graph.add_conditional_edges(
        "check_attachments",
        route_after_check_attachments,
        {
            "fetch_attachment": "fetch_attachment",
            "fetch_api":        "fetch_api",
            "synthesize":       "synthesize",
        },
    )
    graph.add_edge("fetch_attachment", "synthesize")
    graph.add_edge("fetch_api",        "synthesize")

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
    session_id, initial_state = _start_session(user_query, session_id)
    started = time.perf_counter()

    graph_failed = False
    try:
        final_state = _COMPILED_GRAPH.invoke(initial_state)
    except Exception as exc:
        # Full detail goes to the log only; the user gets a plain message (an
        # exception text can carry hostnames or SQL).
        logger.exception("run_query: graph execution failed — %s", type(exc).__name__)
        graph_failed = True
        final_state = _failure_state(initial_state)

    return _finish_query(session_id, user_query, final_state, graph_failed, started)


def run_query_stream(
    user_query: str,
    session_id: str | None = None,
) -> Iterator[dict[str, Any]]:
    """
    run_query, but yields progress events while the graph runs (A28).

    Events, in order:
        {"event": "session", "session_id": ...}                       once, first
        {"event": "step", "node": ..., "label": ..., "count": n|None} after each graph node
        {"event": "done", "result": <the run_query dict>}             once, last

    The graph, the history handling and the saved turn are exactly run_query's;
    only the invocation differs (graph.stream instead of invoke). A graph failure
    ends with a "done" event carrying the same plain failure message run_query
    returns, never an exception, so the caller always gets a last event.
    Step labels are fixed texts and counts: never the query, an item title or a
    snippet, so a progress line can be shown or logged without leaking content.
    """
    session_id, initial_state = _start_session(user_query, session_id)
    started = time.perf_counter()
    yield {"event": "session", "session_id": session_id}

    final_state: dict[str, Any] = dict(initial_state)
    graph_failed = False
    try:
        # "updates" gives {node_name: the keys that node wrote}; the state has no
        # reducers (last write wins), so folding the updates in order rebuilds
        # what invoke() would return.
        for update in _COMPILED_GRAPH.stream(initial_state, stream_mode="updates"):
            for node, written in update.items():
                if written:
                    final_state.update(written)
                yield _step_event(node, written or {})
    except Exception as exc:
        logger.exception("run_query_stream: graph execution failed — %s", type(exc).__name__)
        graph_failed = True
        final_state = _failure_state(initial_state)

    yield {"event": "done", "result": _finish_query(session_id, user_query, final_state, graph_failed, started)}


# What each finished graph node says to the user, and which written key (if any)
# supplies a count to show beside it. No query text, titles or snippets.
_STEP_LABELS: dict[str, tuple[str, str | None]] = {
    "parse_intent":            ("Understood your question", None),
    "resolve_time_anchor":     ("Checked time references", None),
    "postgres_keyword_search": ("Searched by keywords", "postgres_results"),
    "faiss_semantic_search":   ("Searched by meaning", "faiss_results"),
    "merge_and_rrf":           ("Combined the matches", "merged_candidates"),
    "evaluate_quality":        ("Checked the matches", None),
    "widen_scope":             ("Widened the search", None),
    "rerank_cross_encoder":    ("Ranked the best matches", "ranked_results"),
    "extend_neighborhood":     ("Looked at what surrounds them", None),
    "check_attachments":       ("Checked attachments", None),
    "fetch_attachment":        ("Read an attachment", None),
    "fetch_api":               ("Checked Gmail for new mail", "api_results"),
    "synthesize":              ("Wrote the answer", None),
    "no_results_found":        ("Found nothing close", None),
}


def _step_event(node: str, written: dict[str, Any]) -> dict[str, Any]:
    label, count_key = _STEP_LABELS.get(node, (node.replace("_", " ").capitalize(), None))
    value = written.get(count_key) if count_key else None
    return {
        "event": "step",
        "node": node,
        "label": label,
        "count": len(value) if isinstance(value, (list, tuple)) else None,
    }


def _start_session(user_query: str, session_id: str | None) -> tuple[str, EchoState]:
    """The session id (made when missing) and the graph's starting state."""
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
        "answer_markdown":      "",
        "answer_sources":       [],
        "no_results":           False,
    }

    logger.info("run_query: starting graph (session=%s, history=%d messages)",
                session_id, len(conversation_history))
    return session_id, initial_state


def _failure_state(initial_state: EchoState) -> dict[str, Any]:
    """The state returned when the graph raised: a plain message, no results."""
    message = "Something went wrong while searching your memories. Please try again in a moment."
    failed = dict(initial_state)
    failed.update({"final_answer": message, "answer_markdown": message,
                   "answer_sources": [], "no_results": True})
    return failed


def _finish_query(
    session_id: str,
    user_query: str,
    final_state: dict[str, Any],
    graph_failed: bool,
    started: float,
) -> dict[str, Any]:
    """Save the turn, log the counts, and build the dict run_query returns."""
    # Persist the turn — but not a failure message, which would only confuse
    # the next question's history.
    answer = final_state.get("final_answer", "")
    if answer and not graph_failed:
        save_turn(session_id, user_query, answer)

    result_count = len(
        final_state.get("ranked_results") or final_state.get("merged_candidates") or []
    )

    logger.info(
        "run_query: complete in %.0f ms — no_results=%s answer_length=%d result_count=%d sources=%d "
        "attempts=%d",
        (time.perf_counter() - started) * 1000,
        final_state.get("no_results"),
        len(answer),
        result_count,
        len(final_state.get("answer_sources") or []),
        final_state.get("attempt_count", 0),
    )

    return {
        **final_state,
        "session_id": session_id,
        "result_count": result_count,
    }


# ── Command-line smoke test (python -m rse.retrieval_engine "query") ──────────
# Talks to the live database and LLM. The print() calls are the command's
# output for the developer at the terminal, not logging.
if __name__ == "__main__":
    import sys

    from backend.logging_config import setup_logging

    setup_logging()

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

    print("\n── FINAL ANSWER ───────────────────────────────────────────")
    print(result.get("final_answer", "(no answer)"))
    for source in result.get("answer_sources", []):
        print(f"  ⟦{source['n']}⟧ {source['anchor']} · {source['date_label']} → {source['link']}")
