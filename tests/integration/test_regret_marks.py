"""Regret marks end to end over HTTP (SQLite stands in for PostgreSQL): the same route marks and un-marks,
the Timeline list says which items are marked, and the Reflections list only holds marked ones."""
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.wellbeing import router as wellbeing_router
from ste import postgresql_manager as db
from wba import regret_system as rs


@pytest.fixture
def client(sqlite_db):
    app = FastAPI()
    app.include_router(wellbeing_router)
    return TestClient(app, raise_server_exceptions=False)


def seed(title):
    memory_id = str(uuid.uuid4())
    db.execute("INSERT INTO memory_items (memory_id, system_group_id, source_type, source_id, title, created_at, first_ingested_at) "
               "VALUES (:id, 5, 'chrome', :id, :title, '2026-10-05 10:00:00', '2026-10-05 10:00:00')", {"id": memory_id, "title": title})
    db.execute("INSERT INTO memory_engagement (memory_id, last_accessed_at) VALUES (:id, '2026-10-05 10:00:00')", {"id": memory_id})
    return memory_id


def recent_flags(client):
    return {row["title"]: bool(row["regretted"]) for row in client.get("/wellbeing/recent").json()["items"]}


def test_the_same_route_marks_then_un_marks_and_keeps_the_history(client):
    memory_id = seed("a page")
    assert client.post(f"/wellbeing/regret/{memory_id}", json={"note": "too late"}).json()["regretted"] is True
    assert client.post(f"/wellbeing/regret/{memory_id}").json()["regretted"] is False
    assert client.post(f"/wellbeing/regret/{memory_id}").json()["regretted"] is True      # and back on
    assert db.scalar("SELECT COUNT(*) FROM regret_events WHERE memory_id = :id", {"id": memory_id}) == 3   # append-only


def test_the_recent_list_tells_which_items_are_marked(client):
    marked, plain, undone = seed("marked"), seed("plain"), seed("undone")
    client.post(f"/wellbeing/regret/{marked}")
    client.post(f"/wellbeing/regret/{undone}")
    client.post(f"/wellbeing/regret/{undone}")
    assert recent_flags(client) == {"marked": True, "plain": False, "undone": False}


def test_an_un_marked_item_leaves_the_reflections_list(client):
    memory_id = seed("flagged then cleared")
    client.post(f"/wellbeing/regret/{memory_id}", json={"note": "scrolled too long"})
    assert [item["title"] for item in rs.regretted_items()] == ["flagged then cleared"]
    assert rs.regretted_items()[0]["group_name"] == "misc"   # the Reflections category list groups by this
    client.post(f"/wellbeing/regret/{memory_id}")
    assert rs.regretted_items() == []


def test_marking_a_missing_item_is_a_404(client):
    assert client.post(f"/wellbeing/regret/{uuid.uuid4()}").status_code == 404
