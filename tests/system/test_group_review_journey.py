"""System test: the human-in-the-loop journey for user groups, through the HTTP API with a real SQL database.

    create group -> add 5 items by hand -> add a rule -> auto-assign -> weekly review -> accept / remove
"""
import uuid

import numpy as np
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.wellbeing import router as wellbeing_router
from ste import postgresql_manager as db
from wba import group_manager as gm


def unit(*values):
    vec = np.array(values, dtype=np.float32)
    return vec / np.linalg.norm(vec)


@pytest.fixture
def journey(sqlite_db, monkeypatch):
    vectors = {}
    monkeypatch.setattr(gm, "get_manager", lambda: type("M", (), {"get_vector": lambda self, mid: vectors.get(mid)})())
    app = FastAPI()
    app.include_router(wellbeing_router, prefix="")
    client = TestClient(app, raise_server_exceptions=False)

    def new_item(domain, title="page", vector=None):
        memory_id = str(uuid.uuid4())
        db.execute("INSERT INTO memory_items (memory_id, system_group_id, source_type, source_id, title, created_at, first_ingested_at, preprocessed) "
                   "VALUES (:id, 5, 'chrome', :id, :title, '2026-10-05 10:00:00', '2026-10-05 10:00:00', 1)", {"id": memory_id, "title": title})
        db.execute("INSERT INTO chrome_metadata (memory_id, url, canonical_url, domain) VALUES (:id, :id, :id, :d)", {"id": memory_id, "d": domain})
        if vector is not None:
            vectors[memory_id] = vector
        return memory_id

    return client, new_item


def test_nothing_joins_a_group_without_the_user_except_the_confident_path(journey):
    client, new_item = journey
    group = client.post("/wellbeing/groups", json={"group_name": "Capstone", "description": "final year project"}).json()["group_id"]

    # Phase 1: the user seeds the group by hand (5 confirmed examples switch auto-assignment on)
    for _ in range(5):
        assigned = client.post(f"/wellbeing/groups/{group}/assign", json={"memory_id": new_item("github.com", "repo", unit(1, 0, 0))}).json()
    assert assigned["confirmed_count"] == 5 and assigned["auto_assignment_active"] is True

    # He writes a rule himself
    assert client.post(f"/wellbeing/groups/{group}/rules", json={"rule_type": "domain", "rule_value": "github.com"}).status_code == 201

    # New items arrive: one matches rule AND looks similar, one only looks similar, one is unrelated
    confident = new_item("github.com", "new repo page", unit(1, 0.05, 0))
    lookalike = new_item("other.org", "similar looking page", unit(1, 0.05, 0))
    unrelated = new_item("recipes.org", "pasta", unit(0, 1, 0))
    result = client.post("/wellbeing/groups/auto-assign").json()
    assert result["auto_accepted"] == 1 and result["suggested"] == 1

    members = {item["memory_id"] for item in client.get(f"/wellbeing/groups/{group}/items").json()["items"]}
    assert confident in members and lookalike not in members and unrelated not in members

    # Phase 3: the weekly review shows both; the user keeps one decision and rejects the other
    queue = {row["memory_id"]: row for row in client.get("/wellbeing/groups/review").json()["suggestions"]}
    assert set(queue) == {confident, lookalike}
    assert queue[confident]["decision"] == "auto_accepted" and queue[lookalike]["decision"] == "pending"

    client.post(f"/wellbeing/suggestions/{queue[lookalike]['suggestion_id']}/decision", json={"accept": True})
    client.post(f"/wellbeing/suggestions/{queue[confident]['suggestion_id']}/decision", json={"accept": False})   # "Remove" undoes the auto-add
    members = {item["memory_id"] for item in client.get(f"/wellbeing/groups/{group}/items").json()["items"]}
    assert lookalike in members and confident not in members

    # The review queue is now empty, and a rejected item is never suggested again
    assert client.get("/wellbeing/groups/review").json()["suggestions"] == []
    assert client.post("/wellbeing/groups/auto-assign").json()["suggested"] == 0


def test_every_membership_has_a_matching_suggestion_row(journey):
    """Shows: the audit trail is complete. Each member has a group_suggestions row explaining how it got there."""
    client, new_item = journey
    group = client.post("/wellbeing/groups", json={"group_name": "Study"}).json()["group_id"]
    for _ in range(3):
        client.post(f"/wellbeing/groups/{group}/assign", json={"memory_id": new_item("arxiv.org")})
    members = db.fetchall("SELECT memory_id FROM memory_user_groups")
    audited = {r["memory_id"] for r in db.fetchall("SELECT memory_id FROM group_suggestions WHERE decision IN ('accepted', 'auto_accepted')")}
    assert {m["memory_id"] for m in members} == audited and len(members) == 3


def test_deleting_a_group_keeps_its_history_and_hides_it(journey):
    client, new_item = journey
    group = client.post("/wellbeing/groups", json={"group_name": "Temp"}).json()["group_id"]
    client.post(f"/wellbeing/groups/{group}/assign", json={"memory_id": new_item("a.com")})
    assert client.delete(f"/wellbeing/groups/{group}").json()["is_active"] is False
    assert client.get("/wellbeing/groups").json()["groups"] == []
    assert len(client.get("/wellbeing/groups?include_inactive=true").json()["groups"]) == 1
    assert len(db.fetchall("SELECT * FROM memory_user_groups")) == 1          # soft delete: associations survive
