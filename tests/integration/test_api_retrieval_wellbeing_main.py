"""Integration tests for the retrieval, wellbeing and main-app HTTP routes.

Real routers and real validation; the retrieval engine, wellbeing queries and the database cursor are faked.
"""
import uuid
from datetime import datetime

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from langchain_core.messages import AIMessage, HumanMessage
from sqlalchemy.exc import IntegrityError

from backend import retrieval as retrieval_routes
from backend import wellbeing as wb
from backend.retrieval import router as retrieval_router
from backend.wellbeing import router as wellbeing_router

GOOD_RESULT = {"final_answer": "Your interview is Friday ⟦1⟧", "session_id": "abc-123", "no_results": False,
               "result_count": 1, "parsed_intent": {"sources": ["gmail"]}, "answer_markdown": "Your interview is Friday ⟦1⟧",
               "answer_sources": [{"n": 1, "title": "Interview"}], "found_items": [{"raw_row": "must not leak"}],
               "agent_messages": ["must not leak"]}


@pytest.fixture
def retrieval_client():
    app = FastAPI()
    app.include_router(retrieval_router)
    return TestClient(app, raise_server_exceptions=False)


# ── Retrieval ─────────────────────────────────────────────────────────────────

def test_query_returns_only_the_public_fields(retrieval_client, monkeypatch):
    """Shows: the engine's whole internal state never reaches the browser (raw rows, transcripts)."""
    import rse.retrieval_engine as engine

    monkeypatch.setattr(engine, "run_query", lambda user_query, session_id=None: dict(GOOD_RESULT))
    response = retrieval_client.post("/retrieval/query", json={"query": "find my interview email"})
    body = response.json()
    assert response.status_code == 200 and body["final_answer"].startswith("Your interview")
    assert "found_items" not in body and "agent_messages" not in body
    assert body["answer_sources"][0]["n"] == 1


@pytest.mark.parametrize("payload", [{}, {"query": ""}, {"query": "x" * 2001}, {"query": "ok", "session_id": "bad id!"}])
def test_query_validation(retrieval_client, payload):
    assert retrieval_client.post("/retrieval/query", json=payload).status_code == 422


def test_query_failure_is_a_plain_500_without_details(retrieval_client, monkeypatch):
    import rse.retrieval_engine as engine

    monkeypatch.setattr(engine, "run_query", lambda **k: (_ for _ in ()).throw(RuntimeError("password=hunter2")))
    response = retrieval_client.post("/retrieval/query", json={"query": "hello"})
    assert response.status_code == 500 and "hunter2" not in response.text


def test_stream_sends_session_steps_and_done_events(retrieval_client, monkeypatch):
    import rse.retrieval_engine as engine

    def fake_stream(user_query, session_id=None):
        yield {"event": "session", "session_id": "abc-123"}
        yield {"event": "step", "node": "agent_plan", "label": "Understood your question", "count": None}
        yield {"event": "done", "result": dict(GOOD_RESULT)}

    monkeypatch.setattr(engine, "run_query_stream", fake_stream)
    response = retrieval_client.post("/retrieval/query/stream", json={"query": "hello"})
    assert response.headers["content-type"].startswith("text/event-stream")
    events = [block.split("\n")[0] for block in response.text.strip().split("\n\n")]
    assert events == ["event: session", "event: step", "event: done"]
    assert "found_items" not in response.text


def test_stream_turns_a_crash_into_an_error_event(retrieval_client, monkeypatch):
    import rse.retrieval_engine as engine

    def broken(user_query, session_id=None):
        yield {"event": "session", "session_id": "s"}
        raise RuntimeError("boom")

    monkeypatch.setattr(engine, "run_query_stream", broken)
    text = retrieval_client.post("/retrieval/query/stream", json={"query": "hello"}).text
    assert "event: error" in text and "boom" not in text


class FakeCursorConnection:
    """psycopg2-style connection: `with conn`, `with conn.cursor()`, fetchall, rowcount."""

    def __init__(self, rows=None, rowcount=0, fail=False):
        self.rows, self.rowcount, self.fail, self.executed = rows or [], rowcount, fail, []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self):
        return self

    def execute(self, sql, params=None):
        if self.fail:
            raise RuntimeError("db down")
        self.executed.append((sql, params))

    def fetchall(self):
        return self.rows

    def close(self):
        pass


def test_chat_list_titles_the_first_question(retrieval_client, monkeypatch):
    long_question = "what was the name of the recruiter who emailed me about the backend engineer role last week"
    rows = [("s1", long_question, datetime(2026, 10, 25, 9, 0), 3), ("s2", None, datetime(2026, 10, 24, 9, 0), 1)]
    monkeypatch.setattr(retrieval_routes, "get_connection", lambda: FakeCursorConnection(rows))
    chats = retrieval_client.get("/retrieval/sessions").json()
    assert chats[0]["title"].endswith("…") and len(chats[0]["title"]) <= 61
    assert chats[1]["title"] == "Untitled chat" and chats[0]["turns"] == 3
    assert retrieval_client.get("/retrieval/sessions?limit=0").status_code == 422


def test_chat_list_database_error(retrieval_client, monkeypatch):
    monkeypatch.setattr(retrieval_routes, "get_connection", lambda: FakeCursorConnection(fail=True))
    assert retrieval_client.get("/retrieval/sessions").status_code == 500


def test_a_past_chat_can_be_resumed_and_unknown_ones_are_404(retrieval_client, monkeypatch):
    history = [HumanMessage(content="hi"), AIMessage(content="hello")]
    monkeypatch.setattr(retrieval_routes, "load_conversation_history", lambda sid: history if sid == "abc" else [])
    messages = retrieval_client.get("/retrieval/sessions/abc").json()["messages"]
    assert [m["role"] for m in messages] == ["user", "echo"]
    assert retrieval_client.get("/retrieval/sessions/unknown").status_code == 404
    assert retrieval_client.get("/retrieval/sessions/bad id!").status_code in (400, 404)


def test_session_admin_routes_need_the_api_key(retrieval_client, monkeypatch):
    """Shows: diagnostics and deletion are locked unless ECHO_API_KEY is set and sent."""
    monkeypatch.delenv("ECHO_API_KEY", raising=False)
    assert retrieval_client.get("/retrieval/session/abc").status_code == 503
    monkeypatch.setenv("ECHO_API_KEY", "s3cret")
    assert retrieval_client.get("/retrieval/session/abc").status_code == 401
    assert retrieval_client.get("/retrieval/session/abc", headers={"X-API-Key": "wrong"}).status_code == 401
    monkeypatch.setattr(retrieval_routes, "load_conversation_history", lambda sid: [HumanMessage(content="hi")])
    ok = retrieval_client.get("/retrieval/session/abc", headers={"X-API-Key": "s3cret"})
    assert ok.status_code == 200 and ok.json()["message_count"] == 1


def test_clear_session_deletes_messages(retrieval_client, monkeypatch):
    monkeypatch.setenv("ECHO_API_KEY", "s3cret")
    conn = FakeCursorConnection(rowcount=4)
    monkeypatch.setattr(retrieval_routes, "get_connection", lambda: conn)
    response = retrieval_client.delete("/retrieval/session/abc", headers={"X-API-Key": "s3cret"})
    assert response.json() == {"session_id": "abc", "messages_deleted": 4}
    assert "DELETE FROM message_store" in conn.executed[0][0]
    assert retrieval_client.delete("/retrieval/session/bad id!", headers={"X-API-Key": "s3cret"}).status_code == 400
    monkeypatch.setattr(retrieval_routes, "get_connection", lambda: FakeCursorConnection(fail=True))
    assert retrieval_client.delete("/retrieval/session/abc", headers={"X-API-Key": "s3cret"}).status_code == 500


# ── Wellbeing ─────────────────────────────────────────────────────────────────

@pytest.fixture
def wellbeing_client():
    app = FastAPI()
    app.include_router(wellbeing_router)
    return TestClient(app, raise_server_exceptions=False)


ID = str(uuid.uuid4())


def boom(*args, **kwargs):
    raise RuntimeError("db down")


def test_wellbeing_read_routes_pass_the_data_through(wellbeing_client, monkeypatch):
    monkeypatch.setattr(wb, "daily_summary", lambda day: {"day": "2026-10-05", "total_seconds": 60})
    monkeypatch.setattr(wb, "activity_heatmap", lambda a, b: [{"hour": 1}])
    monkeypatch.setattr(wb, "recent_items", lambda limit: [{"memory_id": uuid.UUID(ID)}])
    monkeypatch.setattr(wb, "rebuild_sessions_for_day", lambda day: {"sessions": 2})
    monkeypatch.setattr(wb, "ensure_sessions_for_day", lambda day: None)
    monkeypatch.setattr(wb, "sessions_for_day", lambda day: [])
    monkeypatch.setattr(wb, "fragmentation_for_day", lambda day: {"fragmentation": None})
    assert wellbeing_client.get("/wellbeing/analytics/daily?day=2026-10-05").json()["total_seconds"] == 60
    assert len(wellbeing_client.get("/wellbeing/analytics/heatmap?days=3").json()["cells"]) == 1
    assert wellbeing_client.get("/wellbeing/recent").json()["items"][0]["memory_id"] == ID
    assert wellbeing_client.post("/wellbeing/sessions/rebuild").json() == {"sessions": 2}
    assert wellbeing_client.get("/wellbeing/sessions").json()["sessions"] == []
    assert wellbeing_client.get("/wellbeing/analytics/heatmap?days=0").status_code == 422
    assert wellbeing_client.get("/wellbeing/analytics/daily?day=nonsense").status_code == 422


@pytest.mark.parametrize("method, path, target", [
    ("get", "/wellbeing/analytics/daily", "daily_summary"), ("get", "/wellbeing/analytics/heatmap", "activity_heatmap"),
    ("get", "/wellbeing/recent", "recent_items"), ("post", "/wellbeing/sessions/rebuild", "rebuild_sessions_for_day"),
    ("get", "/wellbeing/groups", "list_groups"), ("get", "/wellbeing/groups/review", "review_queue"),
    ("post", "/wellbeing/groups/auto-assign", "run_auto_assignment"), ("get", "/wellbeing/insights/weekly", "generate_weekly_insight"),
    ("get", f"/wellbeing/groups/{ID}/items", "group_items"), ("get", f"/wellbeing/groups/{ID}/rules", "list_rules"),
    ("get", f"/wellbeing/groups/{ID}/rule-suggestions", "suggest_rules_for_group"),
])
def test_wellbeing_errors_are_plain_500s(wellbeing_client, monkeypatch, method, path, target):
    """Shows: a broken query never leaks its message to the browser."""
    monkeypatch.setattr(wb, target, boom)
    response = getattr(wellbeing_client, method)(path)
    assert response.status_code == 500 and "db down" not in response.text


def test_group_routes(wellbeing_client, monkeypatch):
    monkeypatch.setattr(wb, "list_groups", lambda include_inactive: [{"group_name": "g"}])
    monkeypatch.setattr(wb, "create_group", lambda name, description: {"group_id": ID, "group_name": name})
    monkeypatch.setattr(wb, "deactivate_group", lambda gid: gid == ID)
    assert wellbeing_client.get("/wellbeing/groups").json() == {"groups": [{"group_name": "g"}]}
    assert wellbeing_client.post("/wellbeing/groups", json={"group_name": "Capstone"}).status_code == 201
    assert wellbeing_client.post("/wellbeing/groups", json={"group_name": ""}).status_code == 422
    assert wellbeing_client.delete(f"/wellbeing/groups/{ID}").json()["is_active"] is False
    assert wellbeing_client.delete(f"/wellbeing/groups/{uuid.uuid4()}").status_code == 404
    assert wellbeing_client.delete("/wellbeing/groups/not-a-uuid").status_code == 422
    monkeypatch.setattr(wb, "create_group", lambda n, d: (_ for _ in ()).throw(ValueError("group_name must not be empty")))
    assert wellbeing_client.post("/wellbeing/groups", json={"group_name": "  "}).status_code == 400


def test_review_route_is_not_mistaken_for_a_group_id(wellbeing_client, monkeypatch):
    """Shows (CLAUDE.md §5): the static path /groups/review is declared before /groups/{group_id}."""
    monkeypatch.setattr(wb, "review_queue", lambda gid: [{"suggestion_id": "s"}])
    assert wellbeing_client.get("/wellbeing/groups/review").json() == {"suggestions": [{"suggestion_id": "s"}]}
    monkeypatch.setattr(wb, "run_auto_assignment", lambda: {"scored": 1, "suggested": 0, "auto_accepted": 0})
    assert wellbeing_client.post("/wellbeing/groups/auto-assign").json()["scored"] == 1


def test_rule_and_assignment_routes_map_errors_to_status_codes(wellbeing_client, monkeypatch):
    monkeypatch.setattr(wb, "add_rule", lambda g, t, v: {"rule_id": "r"})
    assert wellbeing_client.post(f"/wellbeing/groups/{ID}/rules", json={"rule_type": "keyword", "rule_value": "x"}).status_code == 201
    monkeypatch.setattr(wb, "add_rule", lambda g, t, v: (_ for _ in ()).throw(ValueError("bad type")))
    assert wellbeing_client.post(f"/wellbeing/groups/{ID}/rules", json={"rule_type": "x", "rule_value": "x"}).status_code == 400
    monkeypatch.setattr(wb, "add_rule", lambda g, t, v: (_ for _ in ()).throw(IntegrityError("s", {}, Exception("fk"))))
    assert wellbeing_client.post(f"/wellbeing/groups/{ID}/rules", json={"rule_type": "keyword", "rule_value": "x"}).status_code == 404
    monkeypatch.setattr(wb, "add_rule", boom)
    assert wellbeing_client.post(f"/wellbeing/groups/{ID}/rules", json={"rule_type": "keyword", "rule_value": "x"}).status_code == 500

    monkeypatch.setattr(wb, "set_rule_active", lambda g, r, active: active)
    assert wellbeing_client.patch(f"/wellbeing/groups/{ID}/rules/{ID}", json={"active": True}).status_code == 200
    assert wellbeing_client.patch(f"/wellbeing/groups/{ID}/rules/{ID}", json={"active": False}).status_code == 404

    monkeypatch.setattr(wb, "manual_assign", lambda m, g: {"memory_id": m, "member_count": 1})
    assert wellbeing_client.post(f"/wellbeing/groups/{ID}/assign", json={"memory_id": ID}).json()["member_count"] == 1
    monkeypatch.setattr(wb, "manual_assign", lambda m, g: (_ for _ in ()).throw(ValueError("No memory item")))
    assert wellbeing_client.post(f"/wellbeing/groups/{ID}/assign", json={"memory_id": ID}).status_code == 404
    monkeypatch.setattr(wb, "manual_assign", boom)
    assert wellbeing_client.post(f"/wellbeing/groups/{ID}/assign", json={"memory_id": ID}).status_code == 500

    monkeypatch.setattr(wb, "list_rules", lambda g, inactive: [{"rule_id": "r"}])
    monkeypatch.setattr(wb, "group_items", lambda g, limit: [{"memory_id": "m"}])
    monkeypatch.setattr(wb, "suggest_rules_for_group", lambda g: [])
    assert wellbeing_client.get(f"/wellbeing/groups/{ID}/rules").json()["rules"] == [{"rule_id": "r"}]
    assert wellbeing_client.get(f"/wellbeing/groups/{ID}/items").json()["items"] == [{"memory_id": "m"}]
    assert wellbeing_client.get(f"/wellbeing/groups/{ID}/rule-suggestions").json()["suggestions"] == []


def test_suggestion_decision_route(wellbeing_client, monkeypatch):
    monkeypatch.setattr(wb, "decide_suggestion", lambda sid, accept: {"suggestion_id": sid, "decision": "accepted" if accept else "rejected"})
    assert wellbeing_client.post(f"/wellbeing/suggestions/{ID}/decision", json={"accept": True}).json()["decision"] == "accepted"
    monkeypatch.setattr(wb, "decide_suggestion", lambda sid, accept: (_ for _ in ()).throw(ValueError("No suggestion")))
    assert wellbeing_client.post(f"/wellbeing/suggestions/{ID}/decision", json={"accept": False}).status_code == 404
    monkeypatch.setattr(wb, "decide_suggestion", boom)
    assert wellbeing_client.post(f"/wellbeing/suggestions/{ID}/decision", json={"accept": True}).status_code == 500
    assert wellbeing_client.post(f"/wellbeing/suggestions/{ID}/decision", json={}).status_code == 422


def test_regret_routes(wellbeing_client, monkeypatch):
    monkeypatch.setattr(wb, "toggle_regret", lambda m, note: {"memory_id": m, "regretted": True, "note": note})
    assert wellbeing_client.post(f"/wellbeing/regret/{ID}", json={"note": "late"}).json()["note"] == "late"
    assert wellbeing_client.post(f"/wellbeing/regret/{ID}").json()["note"] is None
    monkeypatch.setattr(wb, "toggle_regret", lambda m, note: (_ for _ in ()).throw(ValueError("No memory item")))
    assert wellbeing_client.post(f"/wellbeing/regret/{ID}").status_code == 404
    monkeypatch.setattr(wb, "toggle_regret", boom)
    assert wellbeing_client.post(f"/wellbeing/regret/{ID}").status_code == 500

    monkeypatch.setattr(wb, "regret_rate", lambda day: {"regret_rate_percent": 10.0})
    monkeypatch.setattr(wb, "regret_by_hour", lambda: [])
    monkeypatch.setattr(wb, "regret_by_category", lambda: [])
    monkeypatch.setattr(wb, "regretted_items", lambda: [])
    assert wellbeing_client.get("/wellbeing/regret/analytics").json()["rate"]["regret_rate_percent"] == 10.0
    monkeypatch.setattr(wb, "regret_by_hour", boom)
    assert wellbeing_client.get("/wellbeing/regret/analytics").status_code == 500


def test_reminder_routes(wellbeing_client, monkeypatch):
    monkeypatch.setattr(wb, "maybe_get_reminder", lambda source, minutes: {"your_note": "n"} if minutes >= 15 else None)
    assert wellbeing_client.get("/wellbeing/regret/reminder?source_type=youtube&session_minutes=20").json()["reminder"] == {"your_note": "n"}
    assert wellbeing_client.get("/wellbeing/regret/reminder?source_type=youtube&session_minutes=1").json()["reminder"] is None
    assert wellbeing_client.get("/wellbeing/regret/reminder?source_type=tiktok&session_minutes=20").status_code == 422
    monkeypatch.setattr(wb, "maybe_get_reminder", boom)
    assert wellbeing_client.get("/wellbeing/regret/reminder?source_type=gmail&session_minutes=20").status_code == 500

    monkeypatch.setattr(wb, "set_reminders_enabled", lambda enabled: True)
    monkeypatch.setattr(wb, "reminders_enabled", lambda: False)
    assert wellbeing_client.post("/wellbeing/regret/reminders/settings", json={"enabled": False}).json() == {"reminders_enabled": False}
    monkeypatch.setattr(wb, "set_reminders_enabled", lambda enabled: False)
    assert wellbeing_client.post("/wellbeing/regret/reminders/settings", json={"enabled": True}).status_code == 503
    monkeypatch.setattr(wb, "set_reminders_enabled", boom)
    assert wellbeing_client.post("/wellbeing/regret/reminders/settings", json={"enabled": True}).status_code == 500

    monkeypatch.setattr(wb, "snooze_reminders_today", lambda: True)
    assert wellbeing_client.post("/wellbeing/regret/reminders/snooze").json() == {"snoozed_today": True}
    monkeypatch.setattr(wb, "snooze_reminders_today", lambda: False)
    assert wellbeing_client.post("/wellbeing/regret/reminders/snooze").status_code == 503
    monkeypatch.setattr(wb, "snooze_reminders_today", boom)
    assert wellbeing_client.post("/wellbeing/regret/reminders/snooze").status_code == 500


def test_weekly_insight_route(wellbeing_client, monkeypatch):
    monkeypatch.setattr(wb, "generate_weekly_insight", lambda week_end: {"reflection": "A calm week.", "generated_by": "llm"})
    assert wellbeing_client.get("/wellbeing/insights/weekly?week_end=2026-10-07").json()["generated_by"] == "llm"


# ── Main app ──────────────────────────────────────────────────────────────────

def test_main_app_mounts_every_router_and_answers_health():
    from backend.main import app

    client = TestClient(app)        # no `with`: the background workers (Gmail poller, ENP) are not started
    assert client.get("/health").json() == {"status": "ok", "service": "echo-backend", "version": "0.2.0"}
    paths = set(app.openapi()["paths"])
    assert {"/chrome/ingest", "/retrieval/query", "/gmail/poll", "/settings", "/export", "/items/{memory_id}"} <= paths


def test_cors_allows_only_the_local_frontend():
    from backend.main import app

    client = TestClient(app)
    allowed = client.options("/health", headers={"Origin": "http://localhost:3000", "Access-Control-Request-Method": "GET"})
    assert allowed.headers.get("access-control-allow-origin") == "http://localhost:3000"
    denied = client.options("/health", headers={"Origin": "https://evil.example", "Access-Control-Request-Method": "GET"})
    assert "access-control-allow-origin" not in denied.headers
