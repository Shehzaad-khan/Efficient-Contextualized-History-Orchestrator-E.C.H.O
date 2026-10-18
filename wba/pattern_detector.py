"""
pattern_detector.py — WBA descriptive pattern detection.

Finds recurring patterns in regret marks and session composition. Everything
here is DESCRIPTIVE: patterns are reported as counts and correlations, never
as predictions, scores, or judgments (architecture §11.1).

Consumers:
  - regret_system.maybe_get_reminder (habitual pattern precondition)
  - insight_generator (weekly aggregates)
  - the dashboard's regret analytics view

Regret patterns count MARK events of regrets that still stand — the same rule
as regret_system's analytics (an unmark is the user changing their mind, not
a regret). marked_at / regret_hour / regret_day_of_week are LOCAL time (C22), so
they are compared with local dates directly. Engagement and session timestamps
are naive UTC and go through time_aggregator's local-day helpers.

Every function is a few pooled, read-only queries; safe to call from any thread.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta
from typing import Any

from ste import postgresql_manager
from wba.time_aggregator import _utc_offset_seconds, local_day_range_utc

logger = logging.getLogger(__name__)

# A pattern is "habitual" when this many regret marks share the same
# content type within the same ±1-hour band over the lookback window.
HABITUAL_MIN_OCCURRENCES = 3
HABITUAL_LOOKBACK_DAYS = 30

# regret_day_of_week: 0 = Monday .. 6 = Sunday (Python weekday(), DB design).
_DOW_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def _standing_marks_sql() -> str:
    """WITH clause defining `standing_marks`: mark events of currently regretted items.

    Imported here, not at module top: regret_system imports this module at load
    time, so a top-level import back would be circular.
    """
    from wba.regret_system import _CURRENTLY_REGRETTED_CTE, _MARK_EVENTS_CTE

    return (
        "WITH " + _CURRENTLY_REGRETTED_CTE + "," + _MARK_EVENTS_CTE + """,
        standing_marks AS (
            SELECT mk.*
            FROM mark_events mk
            JOIN regretted r ON r.memory_id = mk.memory_id
        )
        """
    )


def _lookback_start(lookback_days: int) -> datetime:
    """Local midnight `lookback_days` ago — compared with the local marked_at."""
    return datetime.combine(date.today() - timedelta(days=lookback_days), time.min)


def regret_time_patterns(lookback_days: int = HABITUAL_LOOKBACK_DAYS) -> dict[str, Any]:
    """
    Regret marks grouped by local hour-of-day and day-of-week over the lookback
    window. Uses the precomputed regret_hour / regret_day_of_week columns.
    """
    params = {"since": _lookback_start(lookback_days)}
    by_hour = postgresql_manager.fetchall(
        _standing_marks_sql() + """
        SELECT regret_hour AS hour, COUNT(*) AS regret_count
        FROM standing_marks
        WHERE regret_hour IS NOT NULL AND marked_at >= :since
        GROUP BY regret_hour
        ORDER BY regret_count DESC, hour
        """,
        params,
    )
    by_day = postgresql_manager.fetchall(
        _standing_marks_sql() + """
        SELECT regret_day_of_week AS day_of_week, COUNT(*) AS regret_count
        FROM standing_marks
        WHERE regret_day_of_week IS NOT NULL AND marked_at >= :since
        GROUP BY regret_day_of_week
        ORDER BY regret_count DESC, day_of_week
        """,
        params,
    )
    for row in by_day:
        dow = row.get("day_of_week")
        row["day_name"] = _DOW_NAMES[dow] if dow is not None and 0 <= dow <= 6 else "unknown"
    logger.debug("regret_time_patterns %dd -> %d hours, %d days", lookback_days, len(by_hour), len(by_day))
    return {"by_hour": by_hour, "by_day_of_week": by_day, "lookback_days": lookback_days}


def has_habitual_regret_pattern(source_type: str, hour: int) -> bool:
    """
    True when the user has repeatedly regretted this content type around this
    local hour (±1) in the lookback window — the reminder system's precondition.

    The band wraps around midnight: at 00:xx it covers 23, 0 and 1, so a
    late-night habit spanning midnight is still one band.
    """
    count = postgresql_manager.scalar(
        _standing_marks_sql() + """
        SELECT COUNT(*)
        FROM standing_marks sm
        JOIN memory_items m ON m.memory_id = sm.memory_id
        WHERE m.source_type = :source_type
          AND sm.regret_hour IN (:hour_before, :hour, :hour_after)
          AND sm.marked_at >= :since
        """,
        {
            "source_type": source_type,
            "hour_before": (hour - 1) % 24,
            "hour": hour % 24,
            "hour_after": (hour + 1) % 24,
            "since": _lookback_start(HABITUAL_LOOKBACK_DAYS),
        },
    )
    found = int(count or 0) >= HABITUAL_MIN_OCCURRENCES
    logger.debug("habitual regret %s@%02d -> %s marks, habitual=%s", source_type, hour, count, found)
    return found


def peak_activity_hours(start_day: date, end_day: date, top_n: int = 3) -> list[dict[str, Any]]:
    """Local hours of day with the most engaged items — 'peak_hours' insight input.

    An item counts once, at the hour it was last accessed (same attribution as
    time_aggregator). Ties go to the earlier hour.
    """
    rows = postgresql_manager.fetchall(
        """
        SELECT EXTRACT(HOUR FROM me.last_accessed_at
                       + make_interval(secs => :utc_offset_seconds))::int AS hour,
               COUNT(*) AS item_count
        FROM memory_items m
        JOIN memory_engagement me ON m.memory_id = me.memory_id
        WHERE m.is_deleted = FALSE
          AND me.last_accessed_at >= :start_utc AND me.last_accessed_at < :end_utc
        GROUP BY hour
        ORDER BY item_count DESC, hour
        LIMIT :top_n
        """,
        {
            **local_day_range_utc(start_day, end_day),
            "utc_offset_seconds": _utc_offset_seconds(),
            "top_n": top_n,
        },
    )
    logger.debug("peak_activity_hours %s..%s -> %d hours", start_day, end_day, len(rows))
    return rows


def session_source_sequences(start_day: date, end_day: date, top_n: int = 5) -> list[dict[str, Any]]:
    """
    Most common source-type sequences across stored sessions, e.g.
    'gmail → chrome → youtube (appeared 4 times)'. Requires sessions to have
    been computed (wellbeing_analytics.rebuild_sessions_for_day).

    Items are ordered by the same time the session builder used
    (COALESCE(last_accessed_at, first_ingested_at)). If an item was opened
    again after its session was built, its position moves (A30 attribution).
    """
    rows = postgresql_manager.fetchall(
        """
        SELECT s.session_id,
               ARRAY_AGG(m.source_type
                         ORDER BY COALESCE(me.last_accessed_at, m.first_ingested_at), m.memory_id) AS sources
        FROM sessions s
        JOIN session_memory_map smm ON smm.session_id = s.session_id
        JOIN memory_items m ON m.memory_id = smm.memory_id
        LEFT JOIN memory_engagement me ON me.memory_id = m.memory_id
        WHERE s.session_start >= :start_utc AND s.session_start < :end_utc
          AND m.is_deleted = FALSE
        GROUP BY s.session_id
        """,
        local_day_range_utc(start_day, end_day),
    )

    sequence_counts: dict[str, int] = {}
    for row in rows:
        sources = row.get("sources") or []
        # Collapse consecutive repeats: [chrome, chrome, youtube] → chrome → youtube
        collapsed: list[str] = []
        for source in sources:
            if not collapsed or collapsed[-1] != source:
                collapsed.append(source)
        if len(collapsed) < 2:
            continue  # single-source sessions carry no switching pattern
        key = " → ".join(collapsed)
        sequence_counts[key] = sequence_counts.get(key, 0) + 1

    # Most frequent first; equal counts in a fixed (alphabetical) order.
    ranked = sorted(sequence_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:top_n]
    logger.debug(
        "session_source_sequences %s..%s -> %d sessions, %d sequences",
        start_day, end_day, len(rows), len(sequence_counts),
    )
    return [{"sequence": seq, "occurrences": count} for seq, count in ranked]
