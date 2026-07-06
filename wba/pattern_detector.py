"""
pattern_detector.py — WBA descriptive pattern detection.

Finds recurring patterns in regret marks and session composition. Everything
here is DESCRIPTIVE: patterns are reported as counts and correlations, never
as predictions, scores, or judgments (architecture §11.1).

Consumers:
  - regret_system.maybe_get_reminder (habitual pattern precondition)
  - insight_generator (weekly aggregates)
  - the dashboard's regret analytics view
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from ste import postgresql_manager

logger = logging.getLogger(__name__)

# A pattern is "habitual" when this many regret marks share the same
# content type within the same ±1-hour band over the lookback window.
HABITUAL_MIN_OCCURRENCES = 3
HABITUAL_LOOKBACK_DAYS = 30

_DOW_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def regret_time_patterns(lookback_days: int = HABITUAL_LOOKBACK_DAYS) -> dict[str, Any]:
    """
    Regret marks grouped by hour-of-day and day-of-week over the lookback
    window. Uses the precomputed regret_hour / regret_day_of_week columns.
    """
    since = date.today() - timedelta(days=lookback_days)
    by_hour = postgresql_manager.fetchall(
        """
        SELECT regret_hour AS hour, COUNT(*) AS regret_count
        FROM regret_events
        WHERE regret_hour IS NOT NULL AND DATE(marked_at) >= :since
        GROUP BY regret_hour
        ORDER BY regret_count DESC
        """,
        {"since": since},
    )
    by_day = postgresql_manager.fetchall(
        """
        SELECT regret_day_of_week AS day_of_week, COUNT(*) AS regret_count
        FROM regret_events
        WHERE regret_day_of_week IS NOT NULL AND DATE(marked_at) >= :since
        GROUP BY regret_day_of_week
        ORDER BY regret_count DESC
        """,
        {"since": since},
    )
    for row in by_day:
        dow = row.get("day_of_week")
        row["day_name"] = _DOW_NAMES[dow] if dow is not None and 0 <= dow <= 6 else "unknown"
    return {"by_hour": by_hour, "by_day_of_week": by_day, "lookback_days": lookback_days}


def has_habitual_regret_pattern(source_type: str, hour: int) -> bool:
    """
    True when the user has repeatedly regretted this content type around this
    hour (±1) in the lookback window — the reminder system's precondition.
    """
    since = date.today() - timedelta(days=HABITUAL_LOOKBACK_DAYS)
    count = postgresql_manager.scalar(
        """
        SELECT COUNT(*)
        FROM regret_events re
        JOIN memory_items m ON m.memory_id = re.memory_id
        WHERE m.source_type = :source_type
          AND re.regret_hour BETWEEN :hour_low AND :hour_high
          AND DATE(re.marked_at) >= :since
        """,
        {
            "source_type": source_type,
            "hour_low": max(0, hour - 1),
            "hour_high": min(23, hour + 1),
            "since": since,
        },
    )
    return int(count or 0) >= HABITUAL_MIN_OCCURRENCES


def peak_activity_hours(start_day: date, end_day: date, top_n: int = 3) -> list[dict[str, Any]]:
    """Hours of day with the most engaged items — 'peak_hours' insight input."""
    return postgresql_manager.fetchall(
        """
        SELECT EXTRACT(HOUR FROM me.last_accessed_at)::int AS hour,
               COUNT(*) AS item_count
        FROM memory_items m
        JOIN memory_engagement me ON m.memory_id = me.memory_id
        WHERE m.is_deleted = FALSE
          AND me.last_accessed_at IS NOT NULL
          AND DATE(me.last_accessed_at) BETWEEN :start_day AND :end_day
        GROUP BY hour
        ORDER BY item_count DESC
        LIMIT :top_n
        """,
        {"start_day": start_day, "end_day": end_day, "top_n": top_n},
    )


def session_source_sequences(start_day: date, end_day: date, top_n: int = 5) -> list[dict[str, Any]]:
    """
    Most common source-type sequences across stored sessions, e.g.
    'gmail → chrome → youtube (appeared 4 times)'. Requires sessions to have
    been computed (wellbeing_analytics.rebuild_sessions_for_day).
    """
    rows = postgresql_manager.fetchall(
        """
        SELECT s.session_id,
               ARRAY_AGG(m.source_type ORDER BY COALESCE(me.last_accessed_at, m.created_at)) AS sources
        FROM sessions s
        JOIN session_memory_map smm ON smm.session_id = s.session_id
        JOIN memory_items m ON m.memory_id = smm.memory_id
        LEFT JOIN memory_engagement me ON me.memory_id = m.memory_id
        WHERE DATE(s.session_start) BETWEEN :start_day AND :end_day
        GROUP BY s.session_id
        """,
        {"start_day": start_day, "end_day": end_day},
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

    ranked = sorted(sequence_counts.items(), key=lambda kv: -kv[1])[:top_n]
    return [{"sequence": seq, "occurrences": count} for seq, count in ranked]
