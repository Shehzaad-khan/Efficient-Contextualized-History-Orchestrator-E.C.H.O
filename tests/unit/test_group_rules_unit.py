"""Unit tests for group rule matching and the hybrid score (pure functions, no database)."""
from datetime import datetime

import numpy as np
import pytest

from wba import group_manager as gm


def rule(rule_type, value, rule_id="r"):
    return {"rule_id": rule_id, "rule_type": rule_type, "rule_value": value}


@pytest.mark.parametrize(
    "rule_item, expected",
    [
        ((rule("keyword", "python"), {"title": "Learning PYTHON fast"}), True),
        ((rule("keyword", "python"), {"title": "Cooking"}), False),
        ((rule("domain", "github.com"), {"domain": "www.github.com"}), True),
        ((rule("domain", "github.com"), {"domain": "gist.github.com"}), True),
        ((rule("domain", "github.com"), {"domain": "notgithub.com"}), False),
        ((rule("domain", "github.com"), {"domain": None}), False),
        ((rule("channel", "codech"), {"channel_name": "CodeCh"}), True),
        ((rule("channel", "codech"), {"channel_name": "CodeChannel"}), False),
        ((rule("sender", "acme.com"), {"sender": "HR <hr@acme.com>"}), True),
        ((rule("sender", "hr@acme.com"), {"sender": "HR <hr@acme.com>"}), True),
        ((rule("sender", "hr@acme.com"), {"sender": "Other <ceo@acme.com>"}), False),
        ((rule("sender", "acme.com"), {"sender": "no address here"}), False),
        ((rule("time_window", "2020-01-01,2099-12-31"), {"created_at": datetime(2026, 10, 5, 12)}), True),
        ((rule("time_window", "2020-01-01,2020-12-31"), {"created_at": datetime(2026, 10, 5, 12)}), False),
        ((rule("time_window", "garbage"), {"created_at": datetime(2026, 10, 5)}), False),
        ((rule("time_window", "2020-01-01,2099-12-31"), {}), False),
        ((rule("keyword", ""), {"title": "anything"}), False),
        ((rule("unknown", "x"), {"title": "x"}), False),
    ],
)
def test_rule_matches(rule_item, expected):
    """Shows: each of the five rule types matches the way the design says (substring, host, exact, address, window)."""
    assert gm._rule_matches(*rule_item) is expected


def test_helpers_extract_address_and_host():
    assert gm._sender_address("Priya <Priya.S+x@Acme.io>") == "priya.s+x@acme.io"
    assert gm._sender_address(None) == ""
    assert gm._host(" WWW.Example.com ") == "example.com"
    assert gm._host(None) == ""


def test_knn_score_is_the_mean_of_the_top_three_similarities():
    item = np.array([1.0, 0.0])
    members = [np.array([1.0, 0.0]), np.array([0.0, 1.0]), np.array([1.0, 1.0]), np.array([-1.0, 0.0])]
    top3 = sorted([1.0, 0.0, 2 ** -0.5, -1.0], reverse=True)[:3]
    assert gm._knn_score(item, members) == pytest.approx(sum(top3) / 3)
    assert gm._knn_score(None, members) == 0.0
    assert gm._knn_score(item, []) == 0.0


def test_hybrid_score_is_half_rule_half_knn():
    context = gm._GroupContext("g", member_vectors=[np.array([1.0, 0.0])],
                               rules=[rule("keyword", "python", "r1"), rule("keyword", "rust", "r2")])
    scores = gm.score_item_for_group({"title": "python tips"}, context, np.array([1.0, 0.0]))
    assert scores["matched_rule_ids"] == ["r1"]
    assert scores["rule_score"] == 0.5 and scores["knn_score"] == 1.0
    assert scores["final_score"] == pytest.approx(0.75)


def test_no_rules_means_a_zero_rule_score():
    scores = gm.score_item_for_group({"title": "x"}, gm._GroupContext("g"), None)
    assert scores == {"rule_score": 0.0, "knn_score": 0.0, "final_score": 0.0, "matched_rule_ids": []}


def test_time_window_parsing():
    start, end = gm._parse_time_window("2026-10-01, 2026-10-31")
    assert (start.day, end.day) == (1, 31)
    with pytest.raises(ValueError):
        gm._parse_time_window("2026-10-31,2026-10-01")


def test_thresholds_match_the_documented_values():
    """Shows: the bars named in CLAUDE.md (5 confirmed members, KNN bar 0.65) are the ones in the code."""
    assert gm.MIN_CONFIRMED_FOR_KNN == 5
    assert gm.KNN_ONLY_THRESHOLD == 0.65
    assert gm.RULE_WEIGHT + gm.KNN_WEIGHT == 1.0
