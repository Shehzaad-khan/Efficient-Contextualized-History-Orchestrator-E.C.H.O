"""Unit tests for the deterministic retrieval steps: RRF fusion, quality check, widening, routing, effort score."""
from datetime import datetime

import pytest

from rse import fusion, graph_nodes, graph_routing, reranker, search_coordinator
from rse.config import MAX_WIDEN_ATTEMPTS, MIN_TOP_SIMILARITY, RRF_K, RRF_POOL_SIZE


def pg(memory_id, created_at=datetime(2026, 10, 1), source="gmail", **extra):
    return {"memory_id": memory_id, "source_type": source, "created_at": created_at, **extra}


# ── RRF fusion (A: Reciprocal Rank Fusion, k = 60) ────────────────────────────

def test_rrf_score_adds_one_over_k_plus_rank_per_branch(monkeypatch):
    """Shows: an item in both branches outranks an item in only one, with the exact formula from the design."""
    monkeypatch.setattr(fusion, "hydrate_memory_items", lambda ids: [pg(i) for i in ids])
    state = {
        "postgres_results": [pg("a"), pg("b")],
        "faiss_results": [("b", 0.9), ("c", 0.8)],
        "parsed_intent": {"skip_postgres_filter": True},
    }
    pool = fusion.merge_and_rrf(state)["merged_candidates"]
    by_id = {item["memory_id"]: item for item in pool}
    assert [item["memory_id"] for item in pool][0] == "b"
    assert by_id["b"]["rrf_score"] == pytest.approx(1 / (RRF_K + 2) + 1 / (RRF_K + 1))
    assert by_id["a"]["rrf_score"] == pytest.approx(1 / (RRF_K + 1))
    assert by_id["c"]["faiss_rank"] == 2 and by_id["c"]["pg_rank"] is None
    assert by_id["b"]["similarity_score"] == 0.9


def test_rrf_never_boosts_recent_items(monkeypatch):
    """Shows (CLAUDE.md §2.3): recency is a prohibited feature; a newer date never changes the order."""
    monkeypatch.setattr(fusion, "hydrate_memory_items", lambda ids: [])
    old, new = datetime(2020, 1, 1), datetime(2026, 10, 25)
    state = {
        "postgres_results": [pg("old", old), pg("new", new)],
        "faiss_results": [],
        "parsed_intent": {},
    }
    ids = [item["memory_id"] for item in fusion.merge_and_rrf(state)["merged_candidates"]]
    assert ids == ["old", "new"]


def test_rrf_ties_are_broken_by_id_not_by_date(monkeypatch):
    """Shows: equal scores give a stable, deterministic order."""
    monkeypatch.setattr(fusion, "hydrate_memory_items", lambda ids: [pg(i) for i in ids])
    state = {"postgres_results": [pg("b")], "faiss_results": [("a", 0.7)], "parsed_intent": {"skip_postgres_filter": True}}
    ids = [item["memory_id"] for item in fusion.merge_and_rrf(state)["merged_candidates"]]
    assert ids == ["a", "b"]  # both rank 1 in one branch each: a < b


def test_rrf_empty_branches_and_pool_cap(monkeypatch):
    """Shows: nothing in, nothing out; the pool never grows past RRF_POOL_SIZE."""
    assert fusion.merge_and_rrf({})["merged_candidates"] == []
    monkeypatch.setattr(fusion, "hydrate_memory_items", lambda ids: [])
    many = [pg(f"id{i:03d}") for i in range(RRF_POOL_SIZE + 20)]
    pool = fusion.merge_and_rrf({"postgres_results": many, "parsed_intent": {}})["merged_candidates"]
    assert len(pool) == RRF_POOL_SIZE


def test_rrf_drops_semantic_hits_that_break_the_source_filter(monkeypatch):
    """Shows: a FAISS-only hit from the wrong source is held to the same rules as the keyword branch."""
    monkeypatch.setattr(
        fusion, "hydrate_memory_items",
        lambda ids: [pg("x", source="chrome"), pg("y", source="gmail")],
    )
    state = {
        "postgres_results": [],
        "faiss_results": [("x", 0.9), ("y", 0.8), ("gone", 0.7)],  # 'gone' = soft-deleted, not hydrated
        "parsed_intent": {"sources": ["gmail"]},
    }
    ids = [item["memory_id"] for item in fusion.merge_and_rrf(state)["merged_candidates"]]
    assert ids == ["y"]


def test_rrf_drops_semantic_hits_before_the_anchor_or_time_filter(monkeypatch):
    """Shows: time_relation 'after' and the time filter are applied to hydrated semantic hits."""
    anchor = datetime(2026, 10, 10)
    monkeypatch.setattr(
        fusion, "hydrate_memory_items",
        lambda ids: [pg("early", datetime(2026, 10, 5)), pg("late", datetime(2026, 10, 12))],
    )
    state = {
        "faiss_results": [("early", 0.9), ("late", 0.8)],
        "parsed_intent": {"time_relation": "after"},
        "anchor_time": anchor.isoformat(),
    }
    ids = [item["memory_id"] for item in fusion.merge_and_rrf(state)["merged_candidates"]]
    assert ids == ["late"]


def test_time_helpers_ignore_bad_input():
    """Shows: an unparseable timestamp or time_filter is ignored instead of crashing the search."""
    assert fusion._parse_timestamp(None) is None
    assert fusion._parse_timestamp("not a date") is None
    assert fusion._time_filter_utc({"time_filter": "garbage"}) is None
    assert isinstance(fusion._time_filter_utc({"time_filter": "2026-10-01"}), datetime)


# ── evaluate_quality (five deterministic checks) ──────────────────────────────

def candidates(n, similarity=0.9, source="gmail", created=datetime(2026, 10, 20)):
    return [{"memory_id": str(i), "source_type": source, "created_at": created, "similarity_score": similarity}
            for i in range(n)]


def quality(state):
    return graph_nodes.node_evaluate_quality(state)["result_quality"]


def test_quality_empty_pool_is_empty():
    assert quality({"merged_candidates": [], "parsed_intent": {}}) == "empty"


def test_quality_requested_source_missing_is_empty():
    state = {"merged_candidates": candidates(3, source="gmail"), "parsed_intent": {"sources": ["youtube"]}}
    assert quality(state) == "empty"


def test_quality_nothing_in_time_window_is_weak():
    state = {"merged_candidates": candidates(3, created=datetime(2020, 1, 1)),
             "parsed_intent": {"time_filter": "2026-10-01"}}
    assert quality(state) == "weak"


def test_quality_too_few_results_is_weak():
    assert quality({"merged_candidates": candidates(1), "parsed_intent": {}}) == "weak"


def test_quality_low_similarity_is_weak_and_good_similarity_is_strong():
    low = {"merged_candidates": candidates(3, similarity=MIN_TOP_SIMILARITY - 0.1), "parsed_intent": {}}
    good = {"merged_candidates": candidates(3, similarity=0.9), "parsed_intent": {}}
    assert quality(low) == "weak"
    assert quality(good) == "strong"


def test_quality_without_semantic_scores_does_not_fail_the_pool():
    pool = [{"memory_id": str(i), "source_type": "gmail", "created_at": datetime(2026, 10, 1)} for i in range(3)]
    assert quality({"merged_candidates": pool, "parsed_intent": {}}) == "strong"


# ── widen_scope (three attempts, each wider) ──────────────────────────────────

def test_widen_attempt_1_extends_the_time_window_by_four_days():
    out = graph_nodes.node_widen_scope({"attempt_count": 0, "parsed_intent": {"time_filter": "2026-10-10T00:00:00"}})
    assert out["parsed_intent"]["time_filter"].startswith("2026-10-06")
    assert out["attempt_count"] == 1


def test_widen_attempt_1_opens_sources_then_trims_the_query():
    opened = graph_nodes.node_widen_scope({"attempt_count": 0, "parsed_intent": {"sources": ["chrome"]}})
    assert set(opened["parsed_intent"]["sources"]) == {"gmail", "chrome", "youtube"}
    trimmed = graph_nodes.node_widen_scope(
        {"attempt_count": 0, "parsed_intent": {"sources": ["all"], "query_clean": "the interview schedule"}}
    )
    assert trimmed["parsed_intent"]["query_clean"] == "interview"


def test_widen_attempt_2_drops_filters_and_anchor():
    out = graph_nodes.node_widen_scope({
        "attempt_count": 1,
        "parsed_intent": {"time_filter": "2026-10-01", "sources": ["gmail"], "query_clean": "tech interview",
                          "time_anchor_query": "offer", "time_relation": "after"},
    })
    intent = out["parsed_intent"]
    assert intent["time_filter"] is None and intent["time_anchor_query"] is None
    assert intent["query_variants"] == ["tech"]
    assert out["anchor_time"] is None


def test_widen_attempt_3_skips_postgres_filters_and_scans_wider():
    out = graph_nodes.node_widen_scope({"attempt_count": 2, "parsed_intent": {}})
    assert out["parsed_intent"]["skip_postgres_filter"] and out["parsed_intent"]["full_faiss_scan"]


def test_widen_never_changes_the_original_intent_in_place():
    original = {"time_filter": "2026-10-10", "sources": ["gmail"]}
    graph_nodes.node_widen_scope({"attempt_count": 1, "parsed_intent": original})
    assert original == {"time_filter": "2026-10-10", "sources": ["gmail"]}


def test_no_results_message_names_the_question():
    out = graph_nodes.node_no_results_found({"user_query": "my lost file"})
    assert out["no_results"] is True and "my lost file" in out["final_answer"]


# ── Routing ───────────────────────────────────────────────────────────────────

@pytest.mark.parametrize(
    "state, expected",
    [
        ({"result_quality": "strong"}, "rerank_cross_encoder"),
        ({"result_quality": "weak", "attempt_count": 0}, "widen_scope"),
        ({"result_quality": "empty", "attempt_count": 1}, "widen_scope"),
        ({"result_quality": "weak", "attempt_count": MAX_WIDEN_ATTEMPTS, "merged_candidates": [{"x": 1}]}, "rerank_cross_encoder"),
        ({"result_quality": "weak", "attempt_count": MAX_WIDEN_ATTEMPTS, "merged_candidates": []}, "no_results_found"),
        ({"result_quality": "empty", "attempt_count": MAX_WIDEN_ATTEMPTS}, "no_results_found"),
    ],
)
def test_route_after_evaluate_quality(state, expected):
    """Shows: weak results are widened at most 3 times, then the closest matches are still shown."""
    assert graph_routing.route_after_evaluate_quality(state) == expected


def test_agent_loop_routing():
    assert graph_routing.route_after_agent({"pending_calls": [{"id": "1"}]}) == "search"
    assert graph_routing.route_after_agent({"pending_calls": []}) == "finish"
    assert graph_routing.route_after_search({"agent_failed": True}) == "finish"
    assert graph_routing.route_after_search({}) == "agent"


# ── Effort score (no recency term) ────────────────────────────────────────────

def test_effort_score_is_zero_for_untouched_and_one_for_maximum():
    assert reranker.compute_effort_score({}) == 0.0
    maxed = {"dwell_time_seconds": 6000, "scroll_depth": 2.0, "revisit_count": 50, "interaction_count": 99,
             "completion_rate": 1.5}
    assert reranker.compute_effort_score(maxed) == 1.0


def test_effort_score_averages_available_signals():
    item = {"dwell_time_seconds": 300, "scroll_depth": 0.5}
    assert reranker.compute_effort_score(item) == pytest.approx((0.5 + 0.5) / 2)


def test_effort_score_ignores_how_recent_the_item_is():
    """Shows (CLAUDE.md §2.3): created_at never enters the score."""
    base = {"dwell_time_seconds": 120}
    a = reranker.compute_effort_score({**base, "created_at": datetime(2020, 1, 1)})
    b = reranker.compute_effort_score({**base, "created_at": datetime(2026, 10, 25)})
    assert a == b


def test_gmail_without_reading_time_gets_the_median_of_read_gmail():
    items = [
        {"source_type": "gmail", "dwell_time_seconds": 600},
        {"source_type": "gmail", "dwell_time_seconds": 0},
        {"source_type": "chrome", "dwell_time_seconds": 0},
    ]
    scores = reranker._query_effort_scores(items)
    assert scores[1] == scores[0] == 1.0
    assert scores[2] == 0.0
    # With nobody read yet, the neutral midpoint is used.
    assert reranker._query_effort_scores([{"source_type": "gmail"}]) == [0.5]


def test_sigmoid_is_stable_for_extreme_logits():
    assert reranker._sigmoid(0) == 0.5
    assert reranker._sigmoid(1000) == pytest.approx(1.0)
    assert reranker._sigmoid(-1000) == pytest.approx(0.0)


# ── Search-coordinator helpers ────────────────────────────────────────────────

def test_escape_like_makes_wildcards_literal():
    assert search_coordinator.escape_like("50%_off\\") == "50\\%\\_off\\\\"


def test_local_to_utc_naive_gives_naive_datetimes_and_rejects_garbage():
    assert search_coordinator.local_to_utc_naive("2026-10-18").tzinfo is None
    assert search_coordinator.local_to_utc_naive("2026-10-18T00:00:00+00:00") == datetime(2026, 10, 18)
    with pytest.raises(ValueError):
        search_coordinator.local_to_utc_naive("yesterday")


def test_filter_time_uses_last_visit_for_pages_and_arrival_for_mail():
    visit = datetime(2026, 10, 20)
    arrived = datetime(2026, 10, 1)
    assert search_coordinator.filter_time_of({"source_type": "chrome", "last_visit_at": visit, "created_at": arrived}) == visit
    assert search_coordinator.filter_time_of({"source_type": "gmail", "last_visit_at": visit, "created_at": arrived}) == arrived
    assert search_coordinator.filter_time_of({"source_type": "youtube", "created_at": arrived}) == arrived
    assert search_coordinator.filter_time_of({"source_type": "chrome"}) is None


def test_dynamic_filters_build_source_and_time_clauses():
    clauses, params = search_coordinator._dynamic_filters({"sources": ["gmail"], "time_filter": "2026-10-01"})
    assert any("m.source_type IN" in c for c in clauses)
    assert len(params) == 4  # one source + the time bound three times
    assert search_coordinator._dynamic_filters({"sources": ["gmail", "chrome", "youtube"]}) == ([], [])
    clauses, _ = search_coordinator._dynamic_filters({"time_filter": "garbage"})
    assert clauses == []


def test_normalise_row_turns_ids_into_strings():
    import uuid

    uid = uuid.uuid4()
    assert search_coordinator.normalise_row({"memory_id": uid})["memory_id"] == str(uid)
