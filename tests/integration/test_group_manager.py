"""Integration tests for wba/group_manager.py against a real SQL database (in-memory SQLite).

They prove CLAUDE.md §2.4: no row enters memory_user_groups except through group_suggestions, and the
A42 'confident path' (rule match AND similarity) that the weekly review can undo.
"""
import re
from pathlib import Path

import numpy as np
import pytest

from ste import postgresql_manager as db
from wba import group_manager as gm

ROOT = Path(__file__).resolve().parents[2]


def seed_item(memory_id, title="A page", source="chrome", domain="github.com", preprocessed=1, deleted=0):
    db.execute(
        "INSERT INTO memory_items (memory_id, system_group_id, source_type, source_id, title, created_at, "
        "first_ingested_at, preprocessed, is_deleted) VALUES (:id, 5, :source, :id, :title, '2026-10-05 10:00:00', "
        "'2026-10-05 10:00:00', :pre, :deleted)",
        {"id": memory_id, "source": source, "title": title, "pre": preprocessed, "deleted": deleted},
    )
    if source == "chrome":
        db.execute("INSERT INTO chrome_metadata (memory_id, url, canonical_url, domain) VALUES (:id, :id, :id, :d)",
                   {"id": memory_id, "d": domain})


class FakeFaiss:
    """get_vector from a dict, like FAISSManager.get_vector."""

    def __init__(self, vectors):
        self.vectors = vectors

    def get_vector(self, memory_id):
        return self.vectors.get(memory_id)


def unit(*values):
    vec = np.array(values, dtype=np.float32)
    return vec / np.linalg.norm(vec)


@pytest.fixture
def group(sqlite_db):
    return gm.create_group("Capstone", "my project")["group_id"]


# ── Groups and rules ──────────────────────────────────────────────────────────

def test_create_list_and_deactivate_a_group(sqlite_db):
    created = gm.create_group("  Job Hunt  ")
    assert created["group_name"] == "Job Hunt"
    assert [g["group_name"] for g in gm.list_groups()] == ["Job Hunt"]
    assert gm.deactivate_group(created["group_id"]) is True
    assert gm.list_groups() == []
    assert [g["group_name"] for g in gm.list_groups(include_inactive=True)] == ["Job Hunt"]
    with pytest.raises(ValueError):
        gm.create_group("   ")


def test_add_rule_normalises_values_and_validates_them(group):
    assert gm.add_rule(group, "sender", " @Acme.COM ")["rule_value"] == "acme.com"
    assert gm.add_rule(group, "domain", "WWW.GitHub.com")["rule_value"] == "github.com"
    assert gm.add_rule(group, "time_window", "2026-10-01,2026-10-31")["rule_type"] == "time_window"
    with pytest.raises(ValueError):
        gm.add_rule(group, "colour", "red")
    with pytest.raises(ValueError):
        gm.add_rule(group, "keyword", "   ")          # an empty keyword would match every title
    with pytest.raises(ValueError):
        gm.add_rule(group, "time_window", "2026-10-31,2026-10-01")   # reversed
    with pytest.raises(ValueError):
        gm.add_rule(group, "time_window", "not-a-window")


def test_rules_can_be_paused_and_resumed(group):
    rule = gm.add_rule(group, "keyword", "python")
    assert gm.set_rule_active(group, rule["rule_id"], False) is True
    assert gm.list_rules(group) == []
    assert len(gm.list_rules(group, include_inactive=True)) == 1
    assert gm.set_rule_active(group, rule["rule_id"], True) is True
    assert len(gm.list_rules(group)) == 1
    assert gm.set_rule_active(group, "no-such-rule", True) is False


# ── The single write path ─────────────────────────────────────────────────────

def test_manual_assign_writes_membership_and_an_accepted_suggestion_together(group):
    """Shows: even the user's own 'Add to group' leaves a complete audit row in group_suggestions."""
    seed_item("m1")
    result = gm.manual_assign("m1", group)
    assert result["member_count"] == 1 and result["confirmed_count"] == 1
    assert result["auto_assignment_active"] is False
    suggestion = db.fetchall("SELECT decision, reviewed FROM group_suggestions")
    assert suggestion == [{"decision": "accepted", "reviewed": 1}]
    again = gm.manual_assign("m1", group)           # assigning twice changes nothing
    assert again["member_count"] == 1
    assert len(db.fetchall("SELECT 1 FROM group_suggestions")) == 1


def test_manual_assign_refuses_unknown_items_and_inactive_groups(group):
    seed_item("m1")
    with pytest.raises(ValueError, match="memory item"):
        gm.manual_assign("missing", group)
    seed_item("deleted", deleted=1)
    with pytest.raises(ValueError, match="memory item"):
        gm.manual_assign("deleted", group)
    gm.deactivate_group(group)
    with pytest.raises(ValueError, match="group"):
        gm.manual_assign("m1", group)


def test_five_confirmed_members_switch_auto_assignment_on(group):
    for i in range(5):
        seed_item(f"m{i}")
        result = gm.manual_assign(f"m{i}", group)
    assert result["auto_assignment_active"] is True
    assert gm.list_groups()[0]["auto_assignment_active"] is True
    assert len(gm.group_items(group)) == 5


def test_accepting_a_suggestion_adds_the_member_and_rejecting_removes_it(group):
    seed_item("m1")
    db.execute("INSERT INTO group_suggestions (suggestion_id, memory_id, group_id) VALUES ('s1', 'm1', :g)", {"g": group})
    assert db.fetchall("SELECT * FROM memory_user_groups") == []     # a pending suggestion is not membership
    assert gm.decide_suggestion("s1", accept=True) == {"suggestion_id": "s1", "decision": "accepted"}
    assert len(db.fetchall("SELECT * FROM memory_user_groups")) == 1
    gm.decide_suggestion("s1", accept=False)                          # rejecting also removes an accepted member
    assert db.fetchall("SELECT * FROM memory_user_groups") == []
    assert db.fetchall("SELECT decision, reviewed FROM group_suggestions") == [{"decision": "rejected", "reviewed": 1}]
    with pytest.raises(ValueError):
        gm.decide_suggestion("nope", accept=True)


def test_group_suggestion_default_is_pending_never_auto_accepted(group):
    """Shows (CLAUDE.md §3): a bare suggestion row is 'pending' and unreviewed."""
    seed_item("m1")
    db.execute("INSERT INTO group_suggestions (memory_id, group_id) VALUES ('m1', :g)", {"g": group})
    assert db.fetchall("SELECT decision, reviewed FROM group_suggestions") == [{"decision": "pending", "reviewed": 0}]


def test_only_group_manager_inserts_into_memory_user_groups():
    """Shows (CLAUDE.md §2.4): scanning the whole code base, the only writer is wba/group_manager.py."""
    writers = []
    for folder in ("backend", "enp", "ingestion", "mcp_server", "rse", "ste", "wba"):
        for path in (ROOT / folder).rglob("*.py"):
            if re.search(r"INSERT\s+INTO\s+memory_user_groups", path.read_text(encoding="utf-8"), re.IGNORECASE):
                writers.append(path.relative_to(ROOT).as_posix())
    assert writers == ["wba/group_manager.py"]


# ── Auto-assignment (Phase 2) and the A42 confident path ──────────────────────

def make_ready_group(monkeypatch, vectors):
    """A group with 5 confirmed members (all pointing the same way) and a rule for github.com."""
    group_id = gm.create_group("Capstone")["group_id"]
    for i in range(5):
        seed_item(f"member{i}")
        gm.manual_assign(f"member{i}", group_id)
        vectors[f"member{i}"] = unit(1, 0, 0)
    gm.add_rule(group_id, "domain", "github.com")
    monkeypatch.setattr(gm, "get_manager", lambda: FakeFaiss(vectors))
    return group_id


def test_rule_and_similarity_together_are_added_at_once_but_stay_in_the_weekly_review(sqlite_db, monkeypatch):
    """Shows (A42): rule match AND KNN >= bar -> 'auto_accepted', reviewed = FALSE, and Remove undoes it."""
    vectors = {}
    group_id = make_ready_group(monkeypatch, vectors)
    seed_item("candidate", domain="github.com")
    vectors["candidate"] = unit(1, 0.05, 0)
    result = gm.run_auto_assignment()
    assert result["auto_accepted"] == 1
    suggestion = db.fetchall("SELECT decision, reviewed FROM group_suggestions WHERE memory_id = 'candidate'")
    assert suggestion == [{"decision": "auto_accepted", "reviewed": 0}]
    assert db.fetchall("SELECT 1 FROM memory_user_groups WHERE memory_id = 'candidate'")
    queue = gm.review_queue(group_id)
    assert [row["memory_id"] for row in queue] == ["candidate"] and queue[0]["decision"] == "auto_accepted"

    # An auto-accepted member is NOT a confirmed example (A42.3) ...
    assert gm.list_groups()[0]["confirmed_count"] == 5 and gm.list_groups()[0]["member_count"] == 6
    context = gm._load_group_context(group_id, FakeFaiss(vectors))
    assert len(context.member_vectors) == 5
    # ... and "Remove" undoes it.
    gm.decide_suggestion(queue[0]["suggestion_id"], accept=False)
    assert db.fetchall("SELECT 1 FROM memory_user_groups WHERE memory_id = 'candidate'") == []


def test_similarity_alone_only_creates_a_pending_suggestion(sqlite_db, monkeypatch):
    vectors = {}
    make_ready_group(monkeypatch, vectors)
    seed_item("lookalike", domain="other.org")
    vectors["lookalike"] = unit(1, 0.05, 0)
    result = gm.run_auto_assignment()
    assert result["suggested"] == 1 and result["auto_accepted"] == 0
    assert db.fetchall("SELECT 1 FROM memory_user_groups WHERE memory_id = 'lookalike'") == []
    assert db.fetchall("SELECT decision FROM group_suggestions WHERE memory_id = 'lookalike'") == [{"decision": "pending"}]


def test_unrelated_items_get_no_suggestion_and_a_pair_is_never_suggested_twice(sqlite_db, monkeypatch):
    vectors = {}
    make_ready_group(monkeypatch, vectors)
    seed_item("unrelated", domain="other.org")
    vectors["unrelated"] = unit(0, 1, 0)
    seed_item("lookalike", domain="other.org")
    vectors["lookalike"] = unit(1, 0.05, 0)
    first = gm.run_auto_assignment()
    second = gm.run_auto_assignment()
    assert first["suggested"] == 1
    assert second["suggested"] == 0
    assert db.fetchall("SELECT 1 FROM group_suggestions WHERE memory_id = 'unrelated'") == []


def test_auto_assignment_does_nothing_without_a_ready_group(sqlite_db):
    seed_item("m1")
    assert gm.run_auto_assignment() == {"scored": 0, "suggested": 0, "auto_accepted": 0}


def test_auto_assignment_refuses_to_run_twice_at_once(sqlite_db):
    gm._auto_assign_lock.acquire()
    try:
        assert gm.run_auto_assignment()["skipped"] == "already running"
    finally:
        gm._auto_assign_lock.release()


def test_rules_still_work_when_faiss_is_unavailable(sqlite_db, monkeypatch):
    group_id = gm.create_group("Capstone")["group_id"]
    for i in range(5):
        seed_item(f"member{i}")
        gm.manual_assign(f"member{i}", group_id)
    gm.add_rule(group_id, "domain", "github.com")
    seed_item("candidate", domain="github.com")

    def broken():
        raise RuntimeError("no index")

    monkeypatch.setattr(gm, "get_manager", broken)
    assert gm.run_auto_assignment()["suggested"] == 1       # rule match -> pending (no KNN score)
