"""
Tests for the A26 agent loop (rse/agent_nodes.py, rse/retrieval_engine.py, rse/search_tool.py,
rse/query_parser.py, rse/llm_synthesizer.py).

What is real here: the compiled LangGraph graph, the three loop nodes, the routing, the argument
checker, the citation validation and the result formatting. What is faked: Luna (a scripted model
that returns prepared replies and records every call) and the search itself (a function that returns
prepared items), so nothing needs a database, an index or an API key.

Each test says what it shows; the list is meant to be read out in the review.
"""
from datetime import datetime, timedelta

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from rse import agent_nodes, llm_synthesizer, retrieval_engine, search_tool
from rse.config import (
    HISTORY_CHARS_ANSWER,
    HISTORY_CHARS_QUESTION,
    HISTORY_MESSAGES,
    MAX_SEARCHES,
    MAX_SEARCHES_PER_ROUND,
    SYNTHESIS_EXCERPT_CHARS_REST,
    SYNTHESIS_EXCERPT_CHARS_TOP,
)
from rse.query_parser import fallback_intent, intent_from_tool_args
from rse.search_tool import SearchOutcome


# ── Fakes ─────────────────────────────────────────────────────────────────────

class FakeLuna:
    """A scripted stand-in for the agent LLM: returns the next prepared reply and records each call."""

    def __init__(self, script):
        self.script = list(script)
        self.calls = []  # one dict per call: tools_bound, options, messages

    def bind_tools(self, tools, **options):
        return _Bound(self, tools, options)

    def invoke(self, messages):
        return self._reply(messages, tools_bound=False, options={})

    def _reply(self, messages, tools_bound, options):
        self.calls.append({"tools_bound": tools_bound, "options": options, "messages": list(messages)})
        step = self.script.pop(0)
        if isinstance(step, Exception):
            raise step
        return step


class _Bound:
    def __init__(self, parent, tools, options):
        self.parent, self.tools, self.options = parent, tools, options

    def invoke(self, messages):
        return self.parent._reply(messages, tools_bound=True, options=self.options)


def search_call(query, call_id="c1", **extra):
    """A Luna reply that asks for one search."""
    return AIMessage(content="", tool_calls=[{"name": "search_memories", "args": {"query": query, **extra}, "id": call_id}])


def item(memory_id, title, source="gmail", sender="Priya <priya@example.com>", snippet="body text"):
    return {
        "memory_id": memory_id, "source_type": source, "title": title, "sender": sender,
        "created_at": datetime(2026, 10, 14, 9, 30), "raw_snippet": snippet, "thread_id": f"t-{memory_id}",
        "url": f"https://example.com/{memory_id}", "domain": "example.com", "has_attachments": False,
    }


class FakeSearch:
    """Replaces run_search: returns prepared items per query and records the intents it was given."""

    def __init__(self, table=None):
        self.table = table or {}
        self.intents = []

    def __call__(self, intent):
        self.intents.append(intent)
        items = self.table.get(intent["query_clean"], [item(f"{intent['query_clean']}-1", f"Result for {intent['query_clean']}")])
        return SearchOutcome(
            requested=intent, applied=intent, quality="strong" if items else "empty",
            widen_attempts=0, items=list(items),
        )


@pytest.fixture
def engine(monkeypatch):
    """Wire a scripted Luna and a fake search into the real graph; returns (run, luna, search, saved)."""
    saved = []
    monkeypatch.setattr(retrieval_engine, "save_turn", lambda session, question, answer: saved.append((question, answer)))

    def run(script, table=None, history=None, question="find my TechCorp interview email"):
        luna = FakeLuna(script)
        search = FakeSearch(table)
        monkeypatch.setattr(agent_nodes, "build_agent_llm", lambda: luna)
        monkeypatch.setattr(agent_nodes, "run_search", search)
        monkeypatch.setattr(retrieval_engine, "load_conversation_history", lambda session: history or [])
        result = retrieval_engine.run_query(question, session_id="s1")
        return result, luna, search

    return run, saved


# ── The loop ──────────────────────────────────────────────────────────────────

def test_simple_question_takes_two_llm_calls_and_one_search(engine):
    """A direct lookup costs 2 LLM calls (ask for a search, then answer), as before A26."""
    run, saved = engine
    result, luna, search = run([
        search_call("TechCorp interview"),
        AIMessage(content="Yes — **Priya** emailed you on 14 Oct ⟦1⟧."),
    ], table={"TechCorp interview": [item("m1", "TechCorp interview schedule")]})

    assert (result["llm_calls"], result["search_count"]) == (2, 1)
    assert len(search.intents) == 1
    assert result["answer_markdown"].endswith("⟦1⟧.")
    assert [source["memory_id"] for source in result["answer_sources"]] == ["m1"]
    assert "✉ Priya · 14 Oct" in result["final_answer"]
    assert result["result_count"] == 1
    assert saved == [("find my TechCorp interview email", result["final_answer"])]


def test_first_question_of_a_chat_forces_a_search_follow_up_does_not(engine):
    """New chat: the first call must search (Luna cannot answer from nothing). A follow-up may answer directly."""
    run, _ = engine
    _, luna, _ = run([search_call("TechCorp"), AIMessage(content="Found it ⟦1⟧.")])
    assert luna.calls[0]["tools_bound"] is True
    assert luna.calls[0]["options"] == {"tool_choice": "required"}
    assert luna.calls[1]["options"] == {}

    history = [HumanMessage(content="emails from Priya"), AIMessage(content="Here they are.")]
    result, luna, _ = run([AIMessage(content="Sure — shorter: two mails from Priya.")], history=history, question="shorten that")
    assert luna.calls[0]["options"] == {}
    assert (result["llm_calls"], result["search_count"]) == (1, 0)
    assert result["no_results"] is False


def test_never_more_than_six_llm_calls_and_five_searches(engine):
    """Hard constraint 5: even if Luna keeps searching, the 6th call has no tool and the run ends."""
    run, _ = engine
    script = [search_call(f"topic {n}", call_id=f"c{n}") for n in range(1, 6)] + [AIMessage(content="I found three mails ⟦1⟧⟦2⟧.")]
    result, luna, search = run(script)

    assert (result["llm_calls"], result["search_count"]) == (6, MAX_SEARCHES)
    assert [call["tools_bound"] for call in luna.calls] == [True] * 5 + [False]
    last_tool_message = [m for m in luna.calls[5]["messages"] if isinstance(m, ToolMessage)][-1]
    assert "No searches left" in last_tool_message.content
    assert len(search.intents) == 5


def test_a_model_that_ignores_the_cap_is_stopped_by_the_code(engine):
    """If Luna still asks for a search on the call that carried no tool, the request is ignored."""
    run, _ = engine
    script = [search_call(f"topic {n}", call_id=f"c{n}") for n in range(1, 7)]  # the 6th ignores the missing tool
    result, luna, search = run(script)

    assert result["llm_calls"] == 6
    assert len(search.intents) == 5
    assert "here's what I found" in result["final_answer"]  # deterministic list instead of a half-answer


def test_parallel_searches_are_capped_per_round_and_every_call_gets_a_reply(engine):
    """Several tool calls in one reply are allowed, but only MAX_SEARCHES_PER_ROUND run; the rest are answered 'not run'."""
    run, _ = engine
    many = AIMessage(content="", tool_calls=[
        {"name": "search_memories", "args": {"query": f"q{n}"}, "id": f"c{n}"} for n in range(1, 5)
    ])
    result, luna, search = run([many, AIMessage(content="Done ⟦1⟧.")])

    assert len(search.intents) == MAX_SEARCHES_PER_ROUND
    replies = [m for m in luna.calls[1]["messages"] if isinstance(m, ToolMessage)]
    assert [m.tool_call_id for m in replies] == ["c1", "c2", "c3", "c4"]
    assert "Not run" in replies[3].content
    assert result["llm_calls"] == 2


def test_items_keep_one_number_across_searches(engine):
    """[n] is stable for the whole question: a repeated item keeps its number and is not sent twice."""
    run, _ = engine
    a, b, c = item("a", "Offer letter"), item("b", "Interview schedule"), item("c", "Onboarding checklist")
    table = {"first": [a, b], "second": [b, c]}
    result, luna, _ = run([
        search_call("first", "c1"),
        search_call("second", "c2"),
        AIMessage(content="The checklist is item three ⟦3⟧."),
    ], table=table)

    second_reply = [m for m in luna.calls[2]["messages"] if isinstance(m, ToolMessage)][-1].content
    assert "[2] (gmail) Interview schedule — (already shown above)" in second_reply
    assert "[3] (gmail) Onboarding checklist" in second_reply and "Excerpt" in second_reply.split("[3]")[1]
    assert [i["memory_id"] for i in result["answer_sources"]] == ["c"]
    assert result["answer_sources"][0]["n"] == 3
    assert result["result_count"] == 3


def test_a_citation_to_an_item_never_shown_is_dropped(engine):
    """A hallucinated source can never reach the user: ⟦9⟧ is removed, ⟦1⟧ stays."""
    run, _ = engine
    result, _, _ = run([search_call("x"), AIMessage(content="It is here ⟦1⟧ and also here ⟦9⟧.")])
    assert "⟦9⟧" not in result["answer_markdown"]
    assert len(result["answer_sources"]) == 1


def test_empty_query_is_reported_back_to_luna_and_costs_no_search(engine):
    """A bad tool call gets an error as its result; Luna can correct itself."""
    run, _ = engine
    bad = AIMessage(content="", tool_calls=[{"name": "search_memories", "args": {"query": "  "}, "id": "bad"}])
    result, luna, search = run([bad, search_call("TechCorp", "good"), AIMessage(content="Found ⟦1⟧.")])

    first_reply = [m for m in luna.calls[1]["messages"] if isinstance(m, ToolMessage)][0]
    assert first_reply.content.startswith("Error: query is required")
    assert (result["llm_calls"], result["search_count"], len(search.intents)) == (3, 1, 1)


def test_nothing_found_is_an_honest_answer_and_flagged(engine):
    """No matches: Luna's own plain reply is used, sources stay empty, no_results is set for the UI."""
    run, _ = engine
    result, _, _ = run([search_call("xyz"), AIMessage(content="I couldn't find anything about that.")], table={"xyz": []})
    assert result["no_results"] is True
    assert result["answer_sources"] == []
    assert result["final_answer"] == "I couldn't find anything about that."


def test_agent_failure_on_the_first_call_still_returns_search_results(engine):
    """Luna down: one search on the raw question, results listed without an LLM answer, nothing thrown away."""
    run, saved = engine
    result, luna, search = run([RuntimeError("timeout")], table={"find my TechCorp interview email": [item("m1", "TechCorp interview")]})

    assert result["agent_failed"] is True
    assert (result["llm_calls"], result["search_count"]) == (1, 1)
    assert search.intents[0]["query_clean"] == "find my TechCorp interview email"
    assert search.intents[0]["sources"] == ["gmail", "chrome", "youtube"]
    assert "TechCorp interview" in result["final_answer"]
    assert saved and saved[0][0] == "find my TechCorp interview email"


def test_agent_failure_after_a_search_lists_what_was_found(engine):
    """A failure on a later call keeps the results already found."""
    run, _ = engine
    result, _, _ = run([search_call("TechCorp"), RuntimeError("rate limit")])
    assert (result["llm_calls"], result["search_count"]) == (2, 1)
    assert "TechCorp" in result["final_answer"] or "Result for" in result["final_answer"]
    assert result["answer_sources"]


def test_a_search_that_raises_becomes_a_tool_result_not_a_crash(engine, monkeypatch):
    """A database error inside one search is reported to Luna, who can still answer."""
    run, _ = engine

    def broken(intent):
        raise ConnectionError("db down")

    luna = FakeLuna([search_call("x"), AIMessage(content="Sorry, I couldn't search just now.")])
    monkeypatch.setattr(agent_nodes, "build_agent_llm", lambda: luna)
    monkeypatch.setattr(agent_nodes, "run_search", broken)
    monkeypatch.setattr(retrieval_engine, "load_conversation_history", lambda session: [])
    result = retrieval_engine.run_query("anything", session_id="s1")

    reply = [m for m in luna.calls[1]["messages"] if isinstance(m, ToolMessage)][0]
    assert reply.content.startswith("Error: this search failed")
    assert "db down" not in reply.content  # the error detail never reaches the model
    assert result["final_answer"].startswith("Sorry")


def test_progress_events_have_one_line_per_step(engine, monkeypatch):
    """The stream gives each step its own event name (the UI keeps one line per name), with counts and no text."""
    run, _ = engine
    luna = FakeLuna([search_call("a", "c1"), search_call("b", "c2"), AIMessage(content="Done ⟦1⟧.")])
    monkeypatch.setattr(agent_nodes, "build_agent_llm", lambda: luna)
    monkeypatch.setattr(agent_nodes, "run_search", FakeSearch())
    monkeypatch.setattr(retrieval_engine, "load_conversation_history", lambda session: [])
    events = list(retrieval_engine.run_query_stream("find it", session_id="s1"))

    steps = [event for event in events if event["event"] == "step"]
    assert [step["node"] for step in steps] == ["agent_plan", "search_1", "agent_read_2", "search_2", "agent_answer", "finish"]
    assert [step["count"] for step in steps if step["node"].startswith("search")] == [1, 1]
    assert events[0]["event"] == "session" and events[-1]["event"] == "done"
    assert events[-1]["result"]["llm_calls"] == 3
    assert all("find it" not in str(step) for step in steps)


# ── What Luna reads ───────────────────────────────────────────────────────────

def test_first_message_has_today_calendar_history_and_question():
    """Luna gets today's date, a calendar to copy `since` from, the history and the question."""
    messages = llm_synthesizer.first_messages("what about last week?", [HumanMessage(content="emails from Priya")])
    text = messages[1].content
    today = datetime.now().astimezone().date()
    assert f"TODAY: {today:%Y-%m-%d}" in text
    assert f"yesterday: {(today - timedelta(days=1)):%A} {(today - timedelta(days=1)).isoformat()}" in text
    assert "this week started (Monday)" in text and "last month started" in text
    assert "User: emails from Priya" in text
    assert "QUESTION: what about last week?" in text
    assert "search_memories" in messages[0].content


def test_history_limits_are_the_agreed_ones():
    """12 messages; questions cut at 1,000 chars, answers at 3,000."""
    history = []
    for n in range(10):
        history += [HumanMessage(content="Q" * 1500), AIMessage(content="A" * 4000)]
    lines = llm_synthesizer._format_history(history).splitlines()
    assert len(lines) == HISTORY_MESSAGES
    assert all(len(line) == len("User: ") + HISTORY_CHARS_QUESTION if line.startswith("User") else len(line) == len("Echo: ") + HISTORY_CHARS_ANSWER
               for line in lines)


def test_excerpt_limits_follow_the_rank_inside_a_search():
    """Top 3 results of a search get 8,000 chars, the rest 2,000, even when their numbers are high."""
    long_item = item("x", "Long", snippet="z" * 20000)
    top = llm_synthesizer._format_item(11, long_item, [], None, rank=1)
    rest = llm_synthesizer._format_item(2, long_item, [], None, rank=4)
    assert top.count("z") == SYNTHESIS_EXCERPT_CHARS_TOP
    assert rest.count("z") == SYNTHESIS_EXCERPT_CHARS_REST


def test_result_text_says_when_the_search_was_widened():
    """Luna is told honestly when Echo loosened the filters, so it can check the matches fit."""
    requested = intent_from_tool_args({"query": "offer", "sources": ["gmail"], "since": "2026-10-20"}, "q")
    applied = dict(requested, time_filter=None, sources=["gmail", "chrome", "youtube"])
    text = llm_synthesizer.format_search_round(
        search_no=1, requested=requested, applied=applied, quality="weak", widen_attempts=2,
        numbered=[(1, item("a", "Offer"), True)], neighbor_items={}, visits={},
    )
    assert "SEARCH 1 of 5" in text and "loose matches only" in text
    assert "You asked for: sources gmail; since 2026-10-20" in text
    assert "widened the search automatically" in text and "the date filter was dropped" in text


# ── The tool and its argument checker ─────────────────────────────────────────

def test_tool_schema_is_valid_and_limits_sources():
    """The tool converts to an OpenAI function with the documented arguments and a source enum."""
    from langchain_core.utils.function_calling import convert_to_openai_tool

    spec = convert_to_openai_tool(search_tool.SEARCH_TOOL)["function"]
    properties = spec["parameters"]["properties"]
    assert spec["name"] == "search_memories"
    assert set(properties) == {"query", "alternative_phrasings", "sources", "since", "anchor_item", "anchor_relation", "read_attachment"}
    assert spec["parameters"]["required"] == ["query"]
    assert "gmail" in str(properties["sources"])
    for name, schema in properties.items():
        assert schema.get("description") or "anyOf" in schema or name == "query", name


def test_argument_checker_repairs_what_the_model_gets_wrong():
    """Output of the model is still checked: bad dates, unknown sources, string booleans, duplicates, half anchors."""
    intent = intent_from_tool_args({
        "query": "  TechCorp   interview ",
        "alternative_phrasings": ["techcorp interview", "job interview invite", "x", "y"],
        "sources": ["Gmail", "tiktok"],
        "since": "yesterday-ish",
        "anchor_item": "offer email",
        "anchor_relation": None,
        "read_attachment": "false",
    }, "original question")

    assert intent["query_clean"] == "TechCorp interview"
    assert intent["query_variants"] == ["TechCorp interview", "job interview invite", "x"]
    assert intent["sources"] == ["gmail"]
    assert intent["time_filter"] is None
    assert intent["time_anchor_query"] is None and intent["time_relation"] is None
    assert intent["fetch_attachment"] is False and intent["fetch_api"] is False
    assert intent["original_query"] == "original question"

    good = intent_from_tool_args({"query": "OS", "since": "2026-10-20", "anchor_item": "interview email",
                                  "anchor_relation": "after", "read_attachment": True, "sources": "wat"}, "q")
    assert good["time_filter"] == "2026-10-20" and good["time_relation"] == "after"
    assert good["fetch_attachment"] is True and good["sources"] == ["gmail", "chrome", "youtube"]

    for bad in ({}, {"query": ""}, None, "text"):
        with pytest.raises(ValueError):
            intent_from_tool_args(bad, "q")
    assert fallback_intent("raw")["query_clean"] == "raw"


# ── One search: the existing stages, in order ─────────────────────────────────

def _patch_stages(monkeypatch, qualities, log):
    """Replace every stage of run_search with a recorder; evaluate returns the given qualities in turn."""
    qualities = list(qualities)

    def stage(name, update=None):
        def run(state):
            log.append(name)
            return update(state) if callable(update) else (update or {})
        return run

    def evaluate(state):
        log.append("evaluate")
        return {"result_quality": qualities.pop(0)}

    def widen(state):
        log.append("widen")
        intent = dict(state["parsed_intent"], time_filter=None)
        return {"parsed_intent": intent, "attempt_count": state["attempt_count"] + 1}

    def rerank(state):
        log.append("rerank")
        return {"ranked_results": [item(f"r{n}", f"R{n}") for n in range(12)]}

    monkeypatch.setattr(search_tool, "node_resolve_time_anchor", stage("anchor"))
    monkeypatch.setattr(search_tool, "node_postgres_keyword_search", stage("keyword", {"postgres_results": []}))
    monkeypatch.setattr(search_tool, "node_faiss_semantic_search", stage("semantic", {"faiss_results": []}))
    monkeypatch.setattr(search_tool, "node_merge_and_rrf", stage("merge", {"merged_candidates": [item("c", "C")]}))
    monkeypatch.setattr(search_tool, "node_evaluate_quality", evaluate)
    monkeypatch.setattr(search_tool, "node_widen_scope", widen)
    monkeypatch.setattr(search_tool, "node_rerank_cross_encoder", rerank)
    monkeypatch.setattr(search_tool, "node_extend_neighborhood", stage("neighbors", {"neighbor_items": {}}))
    monkeypatch.setattr(search_tool, "node_fetch_attachment", stage("attachment", {"attachment_content": "file text", "attachment_item_id": "r0"}))
    monkeypatch.setattr(search_tool, "visits_in_period", lambda intent, ranked: {})


def test_run_search_runs_the_stages_in_order(monkeypatch):
    """Anchor → keyword ∥ semantic → merge → quality → rerank → neighbours; top 10 only; attachment only when asked."""
    log = []
    _patch_stages(monkeypatch, ["strong"], log)
    outcome = search_tool.run_search(intent_from_tool_args({"query": "offer"}, "q"))

    assert log[0] == "anchor"
    assert sorted(log[1:3]) == ["keyword", "semantic"]
    assert log[3:] == ["merge", "evaluate", "rerank", "neighbors"]
    assert len(outcome.items) == 10 and outcome.quality == "strong" and outcome.widen_attempts == 0
    assert outcome.attachment_text is None

    log.clear()
    _patch_stages(monkeypatch, ["strong"], log)
    outcome = search_tool.run_search(intent_from_tool_args({"query": "offer", "read_attachment": True}, "q"))
    assert log[-1] == "attachment" and outcome.attachment_text == "file text" and outcome.attachment_item_id == "r0"


def test_run_search_widens_by_itself_without_an_llm_call(monkeypatch):
    """Weak results trigger widen_scope and a second pass inside the same search (no extra LLM call)."""
    log = []
    _patch_stages(monkeypatch, ["weak", "strong"], log)
    outcome = search_tool.run_search(intent_from_tool_args({"query": "offer", "since": "2026-10-20"}, "q"))

    assert log.count("widen") == 1 and log.count("merge") == 2
    assert outcome.widen_attempts == 1
    assert outcome.requested["time_filter"] == "2026-10-20" and outcome.applied["time_filter"] is None


def test_run_search_gives_up_with_an_empty_outcome(monkeypatch):
    """Nothing found even after the widening attempts: empty outcome, no rerank."""
    log = []
    _patch_stages(monkeypatch, ["empty"] * 4, log)
    outcome = search_tool.run_search(intent_from_tool_args({"query": "zzz"}, "q"))

    assert log.count("widen") == 3 and "rerank" not in log
    assert outcome.items == [] and outcome.quality == "empty" and outcome.widen_attempts == 3
