"""
retrieval_engine.py — LangGraph RSE entry point.

Assembles the agent-loop graph (A26) and exposes run_query() as the single
callable interface for the UI layer and API gateway.

Graph topology:

    agent                         ← Luna (LLM) with the search_memories tool bound
      ↓ asks for searches    ↓ writes the answer
    search                       finish        ← validates ⟦n⟧ citations → END
      ↓ results back to Luna
    agent …                       (up to MAX_SEARCHES searches)

One search (rse/search_tool.py run_search) is the same deterministic pipeline as before A26:
time anchor → keyword (FTS) ∥ semantic (FAISS) → RRF fusion (k=60) → quality check
(→ widen_scope, up to 3 times) → local cross-encoder rerank → neighbourhood → optional
attachment text. No LLM call happens inside it.

LLM calls per query: usually 2 (Luna asks for one search, then answers), at most 6
(MAX_SEARCHES searches, then a call without the tool). The cap is enforced in
rse/agent_nodes.py, not only in the prompt.

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

from rse.agent_nodes import node_agent, node_finish, node_search
from rse.conversation_memory import load_conversation_history, save_turn
from rse.graph_routing import route_after_agent, route_after_search
from rse.state import EchoState

logger = logging.getLogger(__name__)


def _build_graph() -> Any:
    """
    Assemble and compile the LangGraph RSE graph.

    Returns:
        A compiled LangGraph StateGraph ready for invocation.
    """
    graph = StateGraph(EchoState)

    graph.add_node("agent",  node_agent)
    graph.add_node("search", node_search)
    graph.add_node("finish", node_finish)

    graph.set_entry_point("agent")
    graph.add_conditional_edges("agent", route_after_agent, {"search": "search", "finish": "finish"})
    graph.add_conditional_edges("search", route_after_search, {"agent": "agent", "finish": "finish"})
    graph.add_edge("finish", END)

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
        Final EchoState dict containing final_answer, found_items (every item
        shown to Luna, in citation-number order), parsed_intent (the last
        search's arguments), llm_calls, search_count and all other state
        fields, plus session_id and result_count.
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


# What each finished graph step says to the user. The frontend keeps one progress line per `node`
# value (it replaces a line with the same node), so every search gets its own event name.
# No query text, titles or snippets, so a progress line can be shown or logged without leaking content.
def _step_event(node: str, written: dict[str, Any]) -> dict[str, Any]:
    count = None
    if node == "agent":
        if written.get("pending_calls"):
            first = written.get("llm_calls", 1) <= 1
            event_node = "agent_plan" if first else f"agent_read_{written.get('llm_calls', 0)}"
            label = "Understood your question" if first else "Read the matches, searching again"
        else:
            event_node, label = "agent_answer", "Wrote the answer"
    elif node == "search":
        event_node = f"search_{written.get('search_count', 0)}"
        label = "Searched your memories"
        count = written.get("last_search_count")
    else:  # finish
        event_node, label = "finish", "Checked the sources"
    return {"event": "step", "node": event_node, "label": label, "count": count}


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
        "agent_messages":       [],
        "llm_calls":            0,
        "pending_calls":        [],
        "agent_failed":         False,
        "search_count":         0,
        "found_items":          [],
        "last_search_count":    0,
        "parsed_intent":        {},
        "neighbor_items":       {},
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

    result_count = len(final_state.get("found_items") or [])

    logger.info(
        "run_query: complete in %.0f ms — no_results=%s answer_length=%d result_count=%d sources=%d "
        "llm_calls=%d searches=%d widened=%d",
        (time.perf_counter() - started) * 1000,
        final_state.get("no_results"),
        len(answer),
        result_count,
        len(final_state.get("answer_sources") or []),
        final_state.get("llm_calls", 0),
        final_state.get("search_count", 0),
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

    print("\n── LAST SEARCH ────────────────────────────────────────────")
    import json
    print(json.dumps(result.get("parsed_intent", {}), indent=2, default=str))
    print(f"  llm_calls={result.get('llm_calls')} searches={result.get('search_count')} "
          f"widened={result.get('attempt_count')}")

    print(f"\n── ITEMS SHOWN TO THE MODEL: {len(result.get('found_items', []))} ──")
    for number, row in enumerate(result.get("found_items", [])[:10], start=1):
        print(
            f"  [{number}] ({row.get('source_type')}) {str(row.get('title', '(no title)'))[:70]} "
            f"(final={row.get('final_score', 0):.3f} ce={row.get('ce_score')} "
            f"rrf={row.get('rrf_score', 0):.4f})"
        )

    print("\n── FINAL ANSWER ───────────────────────────────────────────")
    print(result.get("final_answer", "(no answer)"))
    for source in result.get("answer_sources", []):
        print(f"  ⟦{source['n']}⟧ {source['anchor']} · {source['date_label']} → {source['link']}")
