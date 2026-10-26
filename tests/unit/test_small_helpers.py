"""Unit tests for small helpers: prompt formatting, progress events, engine/client creation, startup helpers."""
import asyncio
import threading
from datetime import date, datetime


from rse import llm_synthesizer as syn
from rse import retrieval_engine as engine


# ── Prompt formatting (what the model sees) ───────────────────────────────────

def test_calendar_block_gives_the_model_dates_to_copy():
    block = syn._calendar_block(date(2026, 10, 26))            # a Monday
    assert "today: Monday 2026-10-26" in block and "yesterday: Sunday 2026-10-25" in block
    assert "this week started (Monday): 2026-10-26" in block and "last month started: 2026-09-01" in block
    assert block.count("\n") == 5 and "2026-10-12" in block


def test_timestamps_and_short_dates():
    assert syn._to_local(None) is None and syn._to_local("garbage") is None and syn._to_local(12) is None
    assert syn._to_local("2026-10-05T10:00:00") is not None
    assert syn._format_timestamp(None) == "unknown time" and "2026-10-05" in syn._format_timestamp(datetime(2026, 10, 5, 10, 0))
    assert syn._short_date(None) == ""
    assert syn._short_date(datetime(2020, 1, 14, 12)).endswith("2020")


def test_sender_site_anchor_and_link_helpers():
    assert syn._sender_name('"Priya S" <p@x.com>') == "Priya S" and syn._sender_name("p@x.com") == "p" and syn._sender_name("plain") == "plain"
    assert syn._site({"url": "https://www.example.com/a"}) == "example.com" and syn._site({"domain": "www.a.org"}) == "a.org"
    assert syn._anchor({"source_type": "gmail", "sender": "Bob <b@x.com>"}) == "Bob"
    assert syn._anchor({"source_type": "gmail"}) == "email" and syn._anchor({"source_type": "chrome"}) == "web page"
    assert syn._anchor({"source_type": "youtube"}) == "YouTube" and syn._anchor({"source_type": "other"}) == "other"
    assert syn._link({"source_type": "gmail", "thread_id": "t"}).endswith("#all/t")
    assert syn._link({"source_type": "chrome", "url": "https://a.com"}) == "https://a.com"
    assert syn._link({"source_type": "youtube", "video_id": "v"}).endswith("watch?v=v") and syn._link({"source_type": "x"}) is None
    assert syn._item_location({"source_type": "gmail"}) == "from unknown sender"
    assert syn._item_location({"source_type": "chrome"}) == "on unknown site"
    assert syn._item_location({"source_type": "youtube"}) == "channel unknown channel" and syn._item_location({}) == ""


def test_item_block_limits_the_excerpt_by_rank():
    """Shows (A23/A24): the top 3 results get 8,000 characters of text, the rest 2,000."""
    item = {"memory_id": "m", "source_type": "chrome", "title": "T", "domain": "a.com", "raw_snippet": "x" * 20000, "created_at": datetime(2026, 10, 5)}
    top = syn._format_item(1, item, [], rank=1, show_id=True)
    rest = syn._format_item(9, item, [{"title": "Neighbour", "source_type": "gmail"}], [datetime(2026, 10, 6)] * 7, rank=9)
    assert "x" * 8000 in top and "x" * 8001 not in top and "memory_id: m" in top
    assert "x" * 2000 in rest and "x" * 2001 not in rest and "Related context (gmail" in rest and "Visits in the asked period" in rest


def test_history_is_trimmed_to_the_documented_limits():
    from langchain_core.messages import AIMessage, HumanMessage

    assert syn._format_history([]) == "(first question in this chat)"
    many = [HumanMessage(content="q" * 5000), AIMessage(content="a" * 5000)] * 10
    lines = syn._format_history(many).split("\n")
    assert len(lines) == 12 and len(lines[0]) == len("User: ") + 1000 and len(lines[1]) == len("Echo: ") + 3000


def test_filter_descriptions_and_quality_words():
    assert syn.quality_words("strong") != syn.quality_words("weak")
    assert isinstance(syn.quality_words("unknown-value"), str)
    note = syn._widening_note({"sources": ["gmail"], "time_filter": "2026-10-01"}, {"sources": ["gmail", "chrome", "youtube"], "time_filter": None})
    assert "date filter was dropped" in note and "all sources were searched" in note
    assert syn._widening_note({"sources": ["gmail"]}, {"sources": ["gmail"]}) == "the search was loosened"
    assert isinstance(syn._describe_filters({"sources": ["gmail"], "time_filter": "2026-10-01", "time_anchor_query": "offer", "time_relation": "after"}), str)


def test_the_first_messages_include_today_and_history():
    from langchain_core.messages import HumanMessage

    messages = syn.first_messages("what did Priya send?", [HumanMessage(content="earlier question")])
    joined = "\n".join(str(m.content) for m in messages)
    assert "what did Priya send?" in joined and "earlier question" in joined and "TODAY" in joined


def test_memory_detail_and_last_answer():
    from langchain_core.messages import AIMessage, HumanMessage

    assert syn.last_answer_text([HumanMessage(content="q"), AIMessage(content="final"), HumanMessage(content="x")]) == "final"
    assert syn.last_answer_text([]) == ""
    detail = syn.format_memory_detail({"memory_id": "m", "source_type": "gmail", "title": "T", "raw_snippet": "body"}, None)
    assert "T" in detail and "body" in detail


# ── Progress events ───────────────────────────────────────────────────────────

def test_progress_lines_never_carry_the_question_or_titles():
    assert engine._step_event("agent", {"pending_calls": [1], "llm_calls": 1})["node"] == "agent_plan"
    assert engine._step_event("agent", {"pending_calls": [1], "llm_calls": 3})["node"] == "agent_read_3"
    assert engine._step_event("agent", {"pending_calls": []})["node"] == "agent_answer"
    search = engine._step_event("search", {"search_count": 2, "last_search_count": 7})
    assert search == {"event": "step", "node": "search_2", "label": "Searched your memories", "count": 7}
    assert engine._step_event("finish", {})["node"] == "finish"


def test_failure_state_is_a_plain_message():
    failed = engine._failure_state({"user_query": "secret question"})
    assert failed["no_results"] is True and "secret question" not in failed["final_answer"]


# ── Startup helpers in backend/main.py ────────────────────────────────────────

def test_retention_failure_never_blocks_startup(monkeypatch):
    import backend.main as main

    monkeypatch.setattr(main, "purge_expired_items", lambda: (_ for _ in ()).throw(RuntimeError("db down")))
    main._run_retention()                                       # logged, not raised
    called = []
    monkeypatch.setattr(main, "purge_expired_items", lambda: called.append(1))
    main._run_retention()
    assert called == [1]


def test_redis_connect_and_close_are_optional(monkeypatch):
    import backend.main as main

    class Client:
        def __init__(self, ok):
            self.ok = ok

        async def ping(self):
            if not self.ok:
                raise ConnectionError("down")

        async def aclose(self):
            if not self.ok:
                raise RuntimeError("close failed")

    monkeypatch.setattr(main, "get_async_client", lambda: Client(True))
    assert asyncio.run(main._connect_redis()) is True
    asyncio.run(main._close_redis())
    monkeypatch.setattr(main, "get_async_client", lambda: Client(False))
    assert asyncio.run(main._connect_redis()) is False         # Redis is optional at startup
    asyncio.run(main._close_redis())


def test_enp_worker_stops_through_its_event(monkeypatch):
    import backend.main as main

    async def scenario():
        stop = threading.Event()
        ran = []
        monkeypatch.setattr("enp.enrichment_pipeline.run_pipeline", lambda stop_event: ran.append(stop_event) or stop_event.wait(5))
        task = main.start_enp_worker(stop)
        await asyncio.sleep(0.2)
        await main._stop_enp_worker(task, stop)
        return ran, stop.is_set()

    ran, stopped = asyncio.run(scenario())
    assert len(ran) == 1 and stopped


def test_enp_worker_that_takes_too_long_does_not_hang_shutdown(monkeypatch):
    import backend.main as main

    async def scenario():
        monkeypatch.setattr(main, "ENP_STOP_TIMEOUT_SECONDS", 0.05)
        stop = threading.Event()
        task = asyncio.create_task(asyncio.sleep(1))
        await main._stop_enp_worker(task, stop)
        task.cancel()

    asyncio.run(scenario())


def test_the_whole_lifespan_starts_and_stops_cleanly(monkeypatch):
    import backend.main as main
    from fastapi.testclient import TestClient

    started = []

    async def fake_poll():
        started.append("gmail")
        await asyncio.sleep(10)

    monkeypatch.setattr(main, "gmail_poll_forever", fake_poll)
    monkeypatch.setattr(main, "_connect_redis", lambda: asyncio.sleep(0, result=False))
    monkeypatch.setattr(main, "start_enp_worker", lambda stop: asyncio.create_task(asyncio.sleep(0)))
    monkeypatch.setattr(main, "cleanup_old_sessions", lambda: 0)
    monkeypatch.setattr(main, "_run_retention", lambda: None)
    monkeypatch.setattr(main, "sign_in_youtube", lambda: started.append("yt"))
    monkeypatch.setattr(main, "sign_in_gmail", lambda: started.append("gm"))
    disposed = []
    monkeypatch.setattr(main.postgresql_manager, "dispose_engine", lambda: disposed.append(1))
    with TestClient(main.app) as client:
        assert client.get("/health").status_code == 200
    assert "gmail" in started and disposed == [1]
