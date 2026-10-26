"""
The three nodes of the A26 agent loop (decision A26, 2026-10-18; built 2026-10-26).

    agent   — one call to Luna with the search_memories tool bound. It either asks for searches
              (pending_calls) or writes the answer.
    search  — runs the searches Luna asked for (rse/search_tool.py run_search), numbers the items
              for the whole question, and hands each result back as a tool message.
    finish  — validates the ⟦n⟧ citations of Luna's last reply and builds the output fields.

    agent ──asks for searches──▶ search ──▶ agent …  ;  agent ──answers──▶ finish ──▶ END

Hard constraint 5 (usually 2 LLM calls, at most 6) is enforced HERE, in code, not only in the prompt:
calls 1..MAX_SEARCHES carry the tool, so call MAX_SEARCHES + 1 has none and must answer, and a tool
call on a call that carried no tool is ignored. One round runs at most MAX_SEARCHES_PER_ROUND searches
and the whole question at most MAX_SEARCHES.

Failures never throw away what was found: if the agent call fails, a first call is replaced by one
search on the user's raw question, and the results are listed without Luna's answer.
Logs carry counts and timings only — never the query, tool arguments or answers.
"""
import logging
import time
from typing import Any

from langchain_core.messages import ToolMessage

from rse.config import FORCE_FIRST_SEARCH, LLM_CONFIG, MAX_SEARCHES, MAX_SEARCHES_PER_ROUND
from rse.graph_nodes import node_no_results_found
from rse.llm_synthesizer import (
    build_agent_llm,
    finish_answer,
    first_messages,
    format_search_round,
    last_answer_text,
)
from rse.query_parser import intent_from_tool_args
from rse.search_tool import SEARCH_TOOL, TOOL_NAME, SearchOutcome, run_search
from rse.state import EchoState

logger = logging.getLogger(__name__)


def node_agent(state: EchoState) -> dict[str, Any]:
    """One call to Luna. Writes the grown transcript and the searches it asked for (pending_calls)."""
    logger.info("NODE: agent")
    query = state.get("user_query", "")
    history = state.get("conversation_history") or []
    messages = list(state.get("agent_messages") or []) or first_messages(query, history)
    llm_calls = state.get("llm_calls", 0)
    search_count = state.get("search_count", 0)

    # The cap: calls 1..MAX_SEARCHES carry the tool, the next one cannot ask for a search.
    tools_bound = llm_calls < MAX_SEARCHES and search_count < MAX_SEARCHES
    llm = build_agent_llm()
    if tools_bound:
        options: dict[str, Any] = {}
        if FORCE_FIRST_SEARCH and not history and search_count == 0:
            options["tool_choice"] = "required" if LLM_CONFIG["provider"] == "openai" else "any"
        llm = llm.bind_tools([SEARCH_TOOL], **options)

    started = time.perf_counter()
    try:
        response = llm.invoke(messages)
    except Exception as exc:
        logger.error("agent: LLM call %d failed after %.0f ms — %s: %s",
                     llm_calls + 1, (time.perf_counter() - started) * 1000, type(exc).__name__, exc)
        # Nothing found yet: search the raw question once so the user still gets results.
        pending = [] if search_count else [{"id": "fallback", "name": TOOL_NAME, "args": {"query": query}}]
        return {"agent_failed": True, "llm_calls": llm_calls + 1, "pending_calls": pending}

    # A tool call on a call that carried no tool is ignored, so the cap holds whatever the model does.
    calls = [
        {"id": call.get("id") or "", "name": call.get("name") or "", "args": call.get("args")}
        for call in (response.tool_calls if tools_bound else [])
    ]
    logger.info("agent: call %d ok in %.0f ms — %d search request(s)",
                llm_calls + 1, (time.perf_counter() - started) * 1000, len(calls))
    return {
        "agent_messages": messages + [response],
        "llm_calls": llm_calls + 1,
        "pending_calls": calls,
    }


def _register_items(
    outcome: SearchOutcome, found: list[dict[str, Any]], numbers: dict[str, int],
) -> list[tuple[int, dict[str, Any], bool]]:
    """Give each item of one search its number for the whole question: [n] = found[n - 1].

    An item seen in an earlier search keeps its number and comes back marked as not new."""
    numbered = []
    for item in outcome.items:
        memory_id = str(item["memory_id"])
        if memory_id in numbers:
            numbered.append((numbers[memory_id], item, False))
        else:
            found.append(item)
            numbers[memory_id] = len(found)
            numbered.append((len(found), item, True))
    return numbered


def node_search(state: EchoState) -> dict[str, Any]:
    """Run the searches Luna asked for; each gets a tool message with the numbered results."""
    logger.info("NODE: search")
    query = state.get("user_query", "")
    messages = list(state.get("agent_messages") or [])
    found = list(state.get("found_items") or [])
    numbers = {str(item["memory_id"]): number for number, item in enumerate(found, start=1)}
    neighbors = dict(state.get("neighbor_items") or {})
    search_count = state.get("search_count", 0)
    widened = 0
    ran_this_round = 0
    last_intent = state.get("parsed_intent")
    last_found = 0
    replies: list[ToolMessage] = []

    for call in state.get("pending_calls") or []:
        if call["name"] != TOOL_NAME:
            text = f"Unknown tool '{call['name']}'. The only tool is {TOOL_NAME}."
        elif search_count >= MAX_SEARCHES:
            text = f"Not run: all {MAX_SEARCHES} searches are used. Answer now from what you have."
        elif ran_this_round >= MAX_SEARCHES_PER_ROUND:
            text = (f"Not run: at most {MAX_SEARCHES_PER_ROUND} searches per step. "
                    "Read the other results first, then search again if you still need to.")
        else:
            try:
                intent = intent_from_tool_args(call["args"], query)
            except ValueError as exc:
                text = f"Error: {exc}."
            else:
                search_count += 1
                ran_this_round += 1
                try:
                    outcome = run_search(intent)
                except Exception as exc:
                    # The tool result tells Luna the search broke; the type goes to the log, never the detail.
                    logger.error("search: run_search failed — %s", type(exc).__name__)
                    text = "Error: this search failed. You can try again with different words."
                else:
                    numbered = _register_items(outcome, found, numbers)
                    neighbors.update(outcome.neighbor_items)
                    attachment_number = numbers.get(str(outcome.attachment_item_id))
                    widened += outcome.widen_attempts
                    last_intent, last_found = outcome.applied, len(outcome.items)
                    text = format_search_round(
                        search_no=search_count,
                        requested=outcome.requested,
                        applied=outcome.applied,
                        quality=outcome.quality,
                        widen_attempts=outcome.widen_attempts,
                        numbered=numbered,
                        neighbor_items=neighbors,
                        visits=outcome.visits,
                        attachment_text=outcome.attachment_text,
                        attachment_number=attachment_number,
                    )
        replies.append(ToolMessage(content=text, tool_call_id=call["id"], name=TOOL_NAME))

    logger.info("search: %d search(es) run this step (%d of %d used), %d item(s) numbered so far",
                ran_this_round, search_count, MAX_SEARCHES, len(found))
    update: dict[str, Any] = {
        "found_items": found,
        "neighbor_items": neighbors,
        "search_count": search_count,
        "attempt_count": state.get("attempt_count", 0) + widened,
        "last_search_count": last_found,
        "pending_calls": [],
    }
    if last_intent is not None:
        update["parsed_intent"] = last_intent
    # After a failed agent call there is no tool call in the transcript to answer.
    if not state.get("agent_failed"):
        update["agent_messages"] = messages + replies
    return update


def node_finish(state: EchoState) -> dict[str, Any]:
    """Luna's last reply with validated citations; the deterministic list when there is no reply."""
    logger.info("NODE: finish")
    found = state.get("found_items") or []
    answer = "" if state.get("agent_failed") else last_answer_text(state.get("agent_messages") or [])

    if not answer and not found:
        result = node_no_results_found(state)
    else:
        result = finish_answer(state.get("user_query", ""), answer, found)
        # Luna's own "I found nothing" reply is an answer, but the UI still needs to know nothing matched.
        result["no_results"] = bool(state.get("search_count", 0) and not found)

    logger.info("finish: llm_calls=%d searches=%d items=%d widened=%d agent_failed=%s",
                state.get("llm_calls", 0), state.get("search_count", 0), len(found),
                state.get("attempt_count", 0), bool(state.get("agent_failed")))
    return result
