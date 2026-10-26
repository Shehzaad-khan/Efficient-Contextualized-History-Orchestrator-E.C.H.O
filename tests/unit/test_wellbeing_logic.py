"""Unit tests for the wellbeing module (WBA): sessions, time ranges, regret, reminders and the weekly insight.

The database is replaced by small fakes, so only the module's own logic runs. Wellbeing language must
stay descriptive and non-judgmental (CLAUDE.md §2.6): the insight tests prove that.
"""
from datetime import date, datetime, timedelta

import pytest

from wba import insight_generator as ig
from wba import pattern_detector as pd
from wba import regret_system as rs
from wba import time_aggregator as ta
from wba import wellbeing_analytics as wa


def point(minutes, source="chrome", group=1, seconds=60, memory_id=None):
    return {"memory_id": memory_id or f"m{minutes}", "source_type": source, "system_group_id": group,
            "activity_at": datetime(2026, 10, 5, 10, 0) + timedelta(minutes=minutes), "time_seconds": seconds}


# ── Sessions: 20-minute gap (owner's decision) ────────────────────────────────

def test_session_gap_is_twenty_minutes():
    assert wa.SESSION_GAP_MINUTES == 20


def test_items_within_twenty_minutes_form_one_session():
    sessions = wa._split_into_sessions([point(0), point(15), point(30)])
    assert [len(s) for s in sessions] == [3]


def test_a_gap_longer_than_twenty_minutes_closes_the_session():
    sessions = wa._split_into_sessions([point(0), point(10), point(31), point(40)])
    assert [len(s) for s in sessions] == [2, 2]
    assert wa._split_into_sessions([]) == []


def test_dominant_group_is_the_one_with_most_time_and_ties_go_to_the_first_seen():
    items = [point(0, group=1, seconds=30), point(1, group=3, seconds=120), point(2, group=1, seconds=30)]
    assert wa._dominant_group_id(items) == 3
    tie = [point(0, group=2, seconds=60), point(1, group=1, seconds=60)]
    assert wa._dominant_group_id(tie) == 2
    assert wa._dominant_group_id([{"system_group_id": None, "time_seconds": 5, "source_type": "x"}]) is None


def test_source_switches_are_counted_between_consecutive_items():
    items = [point(0, "chrome"), point(1, "chrome"), point(2, "youtube"), point(3, "gmail"), point(4, "gmail")]
    assert wa._source_switch_count(items) == 2


def test_focus_and_fragmentation_are_descriptive_numbers(monkeypatch):
    rows = [{"session_id": 1, "unique_sources": 2}, {"session_id": 2, "unique_sources": 0}]
    monkeypatch.setattr(wa.postgresql_manager, "fetchall", lambda *a, **k: rows)
    sessions = wa.sessions_for_day(date(2026, 10, 5))
    assert [s["focus_score"] for s in sessions] == [0.5, None]
    assert all(isinstance(s["session_id"], str) for s in sessions)

    monkeypatch.setattr(wa.postgresql_manager, "fetchone",
                        lambda *a, **k: {"session_count": 4, "avg_duration_seconds": 600})
    assert wa.fragmentation_for_day(date(2026, 10, 5))["fragmentation"] == 0.4
    monkeypatch.setattr(wa.postgresql_manager, "fetchone",
                        lambda *a, **k: {"session_count": 0, "avg_duration_seconds": 0})
    assert wa.fragmentation_for_day(date(2026, 10, 5))["fragmentation"] is None


def test_ensure_sessions_rebuilds_today_always_and_a_past_day_once(monkeypatch):
    built = []
    monkeypatch.setattr(wa, "rebuild_sessions_for_day", lambda day: built.append(day))
    monkeypatch.setattr(wa, "_past_days_built", set())
    today = date.today()
    yesterday = today - timedelta(days=1)
    wa.ensure_sessions_for_range(yesterday, today)
    wa.ensure_sessions_for_range(yesterday, today)
    assert built.count(yesterday) == 1 and built.count(today) == 2
    wa.ensure_sessions_for_range(today + timedelta(days=1), today + timedelta(days=3))  # the future is skipped
    assert built.count(today) == 2


def test_daily_summary_collects_every_number_without_scores(monkeypatch):
    monkeypatch.setattr(wa, "ensure_sessions_for_day", lambda day: None)
    monkeypatch.setattr(wa.time_aggregator, "total_time", lambda a, b: 3600)
    monkeypatch.setattr(wa.time_aggregator, "time_by_system_group", lambda a, b: [])
    monkeypatch.setattr(wa.time_aggregator, "time_by_source", lambda a, b: [])
    monkeypatch.setattr(wa.time_aggregator, "shorts_summary", lambda a, b: {"shorts_count": 0})
    monkeypatch.setattr(wa, "regret_rate", lambda day: {"regret_rate_percent": 0.0})
    monkeypatch.setattr(wa, "fragmentation_for_day", lambda day: {"fragmentation": None})
    summary = wa.daily_summary(date(2026, 10, 5))
    assert summary["day"] == "2026-10-05" and summary["total_seconds"] == 3600
    assert set(summary) >= {"by_system_group", "by_source", "shorts", "regret", "sessions"}


# ── Time aggregation ──────────────────────────────────────────────────────────

def test_local_day_range_covers_whole_local_days_as_naive_utc():
    rng = ta.local_day_range_utc(date(2026, 10, 5), date(2026, 10, 6))
    assert rng["end_utc"] - rng["start_utc"] == timedelta(days=2)
    assert rng["start_utc"].tzinfo is None
    assert isinstance(ta.utc_offset_seconds(), int)


def test_time_union_only_counts_opened_gmail_and_watched_youtube():
    """Shows (§11.2): unread mail never counts; YouTube needs dwell > 0; deleted items never count."""
    sql = " ".join(ta.TIME_UNION_SQL.split())
    assert "first_opened_at IS NOT NULL" in sql
    assert "v.dwell_seconds > 0" in sql
    assert sql.count("m.is_deleted = FALSE") == 3


def test_total_time_and_group_breakdowns_use_the_database_helpers(monkeypatch):
    monkeypatch.setattr(ta.postgresql_manager, "scalar", lambda q, p=None: 7200)
    assert ta.total_time(date(2026, 10, 5), date(2026, 10, 5)) == 7200
    assert ta.total_time_today() == 7200
    monkeypatch.setattr(ta.postgresql_manager, "scalar", lambda q, p=None: None)
    assert ta.total_time(date(2026, 10, 5), date(2026, 10, 5)) == 0
    monkeypatch.setattr(ta.postgresql_manager, "fetchall", lambda q, p=None: [{"group_name": "work"}])
    assert ta.time_by_system_group(date.today(), date.today()) == [{"group_name": "work"}]
    assert ta.time_by_source(date.today(), date.today()) == [{"group_name": "work"}]
    assert ta.recent_items(3) == [{"group_name": "work"}]
    monkeypatch.setattr(ta.postgresql_manager, "fetchone", lambda q, p=None: None)
    assert ta.shorts_summary(date.today(), date.today()) == {"shorts_count": 0, "shorts_seconds": 0}


def test_heatmap_marks_cells_that_have_a_regret(monkeypatch):
    calls = iter([
        [{"day_of_week": 1, "hour": 22, "item_count": 3, "total_seconds": 900},
         {"day_of_week": 2, "hour": 9, "item_count": 1, "total_seconds": 60}],
        [{"day_of_week": 1, "hour": 22, "regret_count": 2}],
    ])
    monkeypatch.setattr(ta.postgresql_manager, "fetchall", lambda q, p=None: next(calls))
    cells = ta.activity_heatmap(date(2026, 10, 1), date(2026, 10, 7))
    assert cells[0]["has_regret"] is True and cells[0]["regret_count"] == 2
    assert cells[1]["has_regret"] is False


# ── Regret toggles and reminders ──────────────────────────────────────────────

def test_regret_is_odd_number_of_events(monkeypatch):
    """Shows: the toggle is an append-only log; an odd count means 'currently regretted'."""
    for count, expected in [(0, False), (1, True), (2, False), (3, True), (None, False)]:
        monkeypatch.setattr(rs.postgresql_manager, "scalar", lambda q, p=None, c=count: c)
        assert rs.is_regretted("m1") is expected


def test_toggle_regret_always_inserts_never_updates(monkeypatch):
    statements = []
    monkeypatch.setattr(rs.postgresql_manager, "scalar", lambda q, p=None: 1)
    monkeypatch.setattr(rs.postgresql_manager, "execute", lambda q, p=None: statements.append((q, p)))
    result = rs.toggle_regret("m1", note="too late at night")
    assert result["memory_id"] == "m1"
    assert len(statements) == 1 and "INSERT INTO regret_events" in statements[0][0]
    assert statements[0][1]["note"] == "too late at night"


def test_toggle_regret_for_a_missing_item_raises(monkeypatch):
    monkeypatch.setattr(rs.postgresql_manager, "scalar", lambda q, p=None: None)
    with pytest.raises(ValueError):
        rs.toggle_regret("missing")


def test_regret_rate_is_a_percentage_of_total_time(monkeypatch):
    monkeypatch.setattr(rs, "total_time", lambda a, b: 1000)
    monkeypatch.setattr(rs.postgresql_manager, "scalar", lambda q, p=None: 250)
    assert rs.regret_rate(date(2026, 10, 5))["regret_rate_percent"] == 25.0
    monkeypatch.setattr(rs, "total_time", lambda a, b: 0)
    assert rs.regret_rate(date(2026, 10, 5))["regret_rate_percent"] == 0.0


@pytest.fixture
def reminders(monkeypatch, fake_redis):
    monkeypatch.setattr(rs, "get_sync_client", lambda: fake_redis)
    monkeypatch.setattr(rs, "has_habitual_regret_pattern", lambda source, hour: True)
    monkeypatch.setattr(rs, "latest_note_for_type", lambda source: "I lost an hour here")
    return fake_redis


def test_a_reminder_shows_the_users_own_note_and_nothing_else(reminders):
    reminder = rs.maybe_get_reminder("youtube", 20)
    assert reminder["your_note"] == "I lost an hour here"
    assert reminder["options"] == ["Got it", "Snooze for today", "Disable permanently"]


def test_reminders_need_a_long_session_a_known_source_and_a_habit(reminders, monkeypatch):
    assert rs.maybe_get_reminder("youtube", 5) is None
    assert rs.maybe_get_reminder("tiktok", 60) is None
    monkeypatch.setattr(rs, "has_habitual_regret_pattern", lambda source, hour: False)
    assert rs.maybe_get_reminder("youtube", 60) is None


def test_reminders_never_invent_content(reminders, monkeypatch):
    """Shows: with no note of the user's own, no reminder is made up."""
    monkeypatch.setattr(rs, "latest_note_for_type", lambda source: None)
    assert rs.maybe_get_reminder("youtube", 60) is None


def test_reminders_are_rate_limited(reminders):
    """Shows: a 2-hour cooldown, and at most one per content type per day."""
    assert rs.maybe_get_reminder("youtube", 60) is not None
    assert rs.maybe_get_reminder("chrome", 60) is None          # cooldown
    reminders.delete(rs._COOLDOWN_KEY)
    assert rs.maybe_get_reminder("youtube", 60) is None         # one per type per day
    assert rs.maybe_get_reminder("chrome", 60) is not None
    reminders.delete(rs._COOLDOWN_KEY)
    assert rs.maybe_get_reminder("gmail", 60) is None           # two in total per day


def test_user_can_disable_and_snooze_reminders(reminders):
    assert rs.reminders_enabled() is True
    assert rs.set_reminders_enabled(False) is True
    assert rs.reminders_enabled() is False
    assert rs.maybe_get_reminder("youtube", 60) is None
    rs.set_reminders_enabled(True)
    assert rs.snooze_reminders_today() is True
    assert rs.maybe_get_reminder("youtube", 60) is None


def test_reminders_are_off_when_redis_is_unavailable(monkeypatch):
    def broken():
        raise RuntimeError("no redis")

    monkeypatch.setattr(rs, "get_sync_client", broken)
    assert rs.reminders_enabled() is False
    assert rs.set_reminders_enabled(True) is False
    assert rs.snooze_reminders_today() is False
    assert rs.maybe_get_reminder("youtube", 60) is None


def test_regret_queries_return_database_rows(monkeypatch):
    monkeypatch.setattr(rs.postgresql_manager, "fetchall", lambda q, p=None: [{"x": 1}])
    monkeypatch.setattr(rs.postgresql_manager, "fetchone", lambda q, p=None: {"regret_note": "note"})
    assert rs.regret_by_hour() == rs.regret_by_category() == rs.regretted_items() == [{"x": 1}]
    assert rs.latest_note_for_type("youtube") == "note"
    monkeypatch.setattr(rs.postgresql_manager, "fetchone", lambda q, p=None: None)
    assert rs.latest_note_for_type("youtube") is None


# ── Patterns ──────────────────────────────────────────────────────────────────

def test_habit_needs_three_marks_in_a_band_that_wraps_midnight(monkeypatch):
    seen = {}

    def fake_scalar(query, params=None):
        seen.update(params)
        return 3

    monkeypatch.setattr(pd.postgresql_manager, "scalar", fake_scalar)
    assert pd.has_habitual_regret_pattern("youtube", 0) is True
    assert (seen["hour_before"], seen["hour"], seen["hour_after"]) == (23, 0, 1)
    monkeypatch.setattr(pd.postgresql_manager, "scalar", lambda q, p=None: 2)
    assert pd.has_habitual_regret_pattern("youtube", 12) is False


def test_regret_time_patterns_name_the_weekday(monkeypatch):
    answers = iter([[{"hour": 22, "regret_count": 3}], [{"day_of_week": 0, "regret_count": 3}, {"day_of_week": None, "regret_count": 1}]])
    monkeypatch.setattr(pd.postgresql_manager, "fetchall", lambda q, p=None: next(answers))
    result = pd.regret_time_patterns(7)
    assert [d["day_name"] for d in result["by_day_of_week"]] == ["Monday", "unknown"]


def test_session_sequences_collapse_repeats_and_ignore_single_source_sessions(monkeypatch):
    rows = [
        {"sources": ["gmail", "gmail", "chrome"]}, {"sources": ["gmail", "chrome"]},
        {"sources": ["chrome", "chrome"]}, {"sources": ["youtube", "chrome"]}, {"sources": None},
    ]
    monkeypatch.setattr(pd.postgresql_manager, "fetchall", lambda q, p=None: rows)
    result = pd.session_source_sequences(date(2026, 10, 1), date(2026, 10, 7))
    assert result[0] == {"sequence": "gmail → chrome", "occurrences": 2}
    assert {"sequence": "youtube → chrome", "occurrences": 1} in result
    monkeypatch.setattr(pd.postgresql_manager, "fetchall", lambda q, p=None: [{"hour": 9, "item_count": 4}])
    assert pd.peak_activity_hours(date(2026, 10, 1), date(2026, 10, 7)) == [{"hour": 9, "item_count": 4}]


# ── Weekly insight: descriptive, never judging ────────────────────────────────

AGGREGATES = {
    "week_start": "2026-10-01", "week_end": "2026-10-07", "total_hours": 12.5,
    "category_breakdown_percent": {"work": 60, "entertainment": 40, "misc": 0},
    "regret_count_by_category": [{"category": "entertainment", "count": 2}],
    "regret_peak_hours": [23], "session_patterns": [{"sequence": "gmail → chrome", "occurrences": 3}],
    "peak_activity_hours": [10], "shorts_count": 4, "shorts_time_minutes": 12,
}


def test_deterministic_summary_is_descriptive():
    summary = ig._deterministic_summary(AGGREGATES)
    assert "12.5 hours" in summary and "work 60%" in summary and "misc" not in summary
    assert "4 watched" in summary and "gmail → chrome (3×)" in summary
    assert not ig._JUDGING_PATTERN.search(summary)


@pytest.mark.parametrize("phrase", [
    "You should stop watching Shorts", "That was a waste of time", "far too much video", "an unhealthy habit",
    "you may be addicted", "try to cut back", "a bad habit", "wasted hours",
])
def test_judging_language_is_detected(phrase):
    """Shows (CLAUDE.md §2.6): phrases like 'wasted time' or 'you should' never reach the user."""
    assert ig._JUDGING_PATTERN.search(phrase)


def test_neutral_language_is_allowed():
    assert not ig._JUDGING_PATTERN.search("You spent 3 hours on work pages, mostly around 10 AM.")


class _Reply:
    def __init__(self, text):
        self.text = text


def fake_llm(reply):
    class LLM:
        def invoke(self, messages):
            self.messages = messages
            return _Reply(reply)
    return LLM()


def test_llm_reflection_only_sees_aggregated_numbers(monkeypatch):
    """Shows (CLAUDE.md §2.6): the wellbeing LLM receives aggregated JSON, no titles or URLs."""
    llm = fake_llm("Most time went to work pages around 10 AM.")
    monkeypatch.setattr(ig, "build_chat_model", lambda a, b: llm)
    assert ig._llm_reflection(AGGREGATES).startswith("Most time")
    sent = llm.messages[1].content
    assert '"total_hours": 12.5' in sent
    assert "http" not in sent and "title" not in sent


def test_a_judging_or_empty_llm_reply_is_refused(monkeypatch):
    monkeypatch.setattr(ig, "build_chat_model", lambda a, b: fake_llm("You should really stop."))
    with pytest.raises(ValueError, match="judging"):
        ig._llm_reflection(AGGREGATES)
    monkeypatch.setattr(ig, "build_chat_model", lambda a, b: fake_llm("   "))
    with pytest.raises(ValueError, match="empty"):
        ig._llm_reflection(AGGREGATES)


def test_weekly_insight_falls_back_to_the_deterministic_summary(monkeypatch):
    monkeypatch.setattr(ig, "collect_weekly_aggregates", lambda week_end=None: AGGREGATES)
    monkeypatch.setattr(ig, "build_chat_model", lambda a, b: fake_llm("You should stop."))
    result = ig.generate_weekly_insight()
    assert result["generated_by"] == "deterministic"
    assert not ig._JUDGING_PATTERN.search(result["reflection"])
    monkeypatch.setattr(ig, "build_chat_model", lambda a, b: fake_llm("A calm, descriptive week."))
    assert ig.generate_weekly_insight()["generated_by"] == "llm"


def test_collect_weekly_aggregates_builds_the_payload(monkeypatch):
    monkeypatch.setattr(ig.time_aggregator, "total_time", lambda a, b: 7200)
    monkeypatch.setattr(ig.time_aggregator, "time_by_system_group",
                        lambda a, b: [{"group_name": "work", "total_seconds": 3600}, {"group_name": "misc", "total_seconds": 3600}])
    monkeypatch.setattr(ig.time_aggregator, "shorts_summary", lambda a, b: {"shorts_count": 2, "shorts_seconds": 120})
    monkeypatch.setattr(ig, "_weekly_regrets", lambda a, b: {"by_category": [], "peak_hours": [22]})
    monkeypatch.setattr(ig, "ensure_sessions_for_range", lambda a, b: None)
    monkeypatch.setattr(ig, "session_source_sequences", lambda a, b: [])
    monkeypatch.setattr(ig, "peak_activity_hours", lambda a, b: [{"hour": 10}])
    data = ig.collect_weekly_aggregates(date(2026, 10, 7))
    assert data["total_hours"] == 2.0 and data["category_breakdown_percent"] == {"work": 50, "misc": 50}
    assert data["week_start"] == "2026-10-01" and data["peak_activity_hours"] == [10]
    assert data["shorts_time_minutes"] == 2


def test_weekly_regrets_shapes_rows(monkeypatch):
    answers = iter([[{"category": "entertainment", "regret_count": 2}], [{"hour": 23, "regret_count": 2}]])
    monkeypatch.setattr(ig.postgresql_manager, "fetchall", lambda q, p=None: next(answers))
    assert ig._weekly_regrets(date(2026, 10, 1), date(2026, 10, 7)) == {
        "by_category": [{"category": "entertainment", "count": 2}], "peak_hours": [23]}
