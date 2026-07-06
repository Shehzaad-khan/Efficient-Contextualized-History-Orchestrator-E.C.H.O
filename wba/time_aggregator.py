"""
time_aggregator.py — WBA time calculation (architecture §11.2).

Time is calculated ON-DEMAND from source tables — never stored separately.
This prevents sync bugs and double-counting.

Inclusion rules (non-negotiable):
  Gmail   — only OPENED emails count (first_opened_at IS NOT NULL).
            Unread emails never contribute to wellbeing time.
  Chrome  — all admitted pages count (they passed the intent gate).
  YouTube — videos with watch_time_seconds > 0 count.

All outputs are descriptive numbers. No scoring, no judgment.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any

from ste import postgresql_manager

logger = logging.getLogger(__name__)

# One UNION ALL block per source, each enforcing its own inclusion rule.
# Attribution day = DATE(last_accessed_at) per architecture §11.2.
_TIME_UNION = """
    SELECT m.memory_id, m.source_type, m.system_group_id,
           me.dwell_time_seconds AS time_seconds,
           me.last_accessed_at
    FROM memory_items m
    JOIN memory_engagement me ON m.memory_id = me.memory_id
    WHERE m.source_type = 'gmail'
      AND m.is_deleted = FALSE
      AND me.first_opened_at IS NOT NULL
      AND DATE(me.last_accessed_at) BETWEEN :start_day AND :end_day
    UNION ALL
    SELECT m.memory_id, m.source_type, m.system_group_id,
           me.dwell_time_seconds AS time_seconds,
           me.last_accessed_at
    FROM memory_items m
    JOIN memory_engagement me ON m.memory_id = me.memory_id
    WHERE m.source_type = 'chrome'
      AND m.is_deleted = FALSE
      AND DATE(me.last_accessed_at) BETWEEN :start_day AND :end_day
    UNION ALL
    SELECT m.memory_id, m.source_type, m.system_group_id,
           me.watch_time_seconds AS time_seconds,
           me.last_accessed_at
    FROM memory_items m
    JOIN memory_engagement me ON m.memory_id = me.memory_id
    WHERE m.source_type = 'youtube'
      AND m.is_deleted = FALSE
      AND me.watch_time_seconds > 0
      AND DATE(me.last_accessed_at) BETWEEN :start_day AND :end_day
"""


def total_time(start_day: date, end_day: date) -> int:
    """Total wellbeing seconds across all sources for the day range (inclusive)."""
    value = postgresql_manager.scalar(
        f"SELECT COALESCE(SUM(time_seconds), 0) FROM ({_TIME_UNION}) AS all_time",
        {"start_day": start_day, "end_day": end_day},
    )
    return int(value or 0)


def total_time_today() -> int:
    """Total wellbeing seconds for today (architecture §11.2 reference query)."""
    today = date.today()
    return total_time(today, today)


def time_by_system_group(start_day: date, end_day: date) -> list[dict[str, Any]]:
    """Seconds per system group (work/study/entertainment/personal/misc)."""
    return postgresql_manager.fetchall(
        f"""
        SELECT sg.group_name,
               COALESCE(SUM(t.time_seconds), 0) AS total_seconds,
               COUNT(t.memory_id) AS item_count
        FROM system_groups sg
        LEFT JOIN ({_TIME_UNION}) AS t ON t.system_group_id = sg.system_group_id
        GROUP BY sg.system_group_id, sg.group_name
        ORDER BY sg.system_group_id
        """,
        {"start_day": start_day, "end_day": end_day},
    )


def time_by_source(start_day: date, end_day: date) -> list[dict[str, Any]]:
    """Seconds per source type (gmail/chrome/youtube)."""
    return postgresql_manager.fetchall(
        f"""
        SELECT source_type,
               COALESCE(SUM(time_seconds), 0) AS total_seconds,
               COUNT(memory_id) AS item_count
        FROM ({_TIME_UNION}) AS t
        GROUP BY source_type
        ORDER BY source_type
        """,
        {"start_day": start_day, "end_day": end_day},
    )


def shorts_summary(start_day: date, end_day: date) -> dict[str, Any]:
    """
    Separate Shorts analytics (architecture §7.4): count and total watch time.
    Shorts are tracked apart from long-form in the wellbeing dashboard.
    """
    row = postgresql_manager.fetchone(
        """
        SELECT COUNT(*) AS shorts_count,
               COALESCE(SUM(me.watch_time_seconds), 0) AS shorts_seconds
        FROM memory_items m
        JOIN memory_engagement me ON m.memory_id = me.memory_id
        JOIN youtube_metadata ym ON m.memory_id = ym.memory_id
        WHERE ym.is_short = TRUE
          AND m.is_deleted = FALSE
          AND me.watch_time_seconds > 0
          AND DATE(me.last_accessed_at) BETWEEN :start_day AND :end_day
        """,
        {"start_day": start_day, "end_day": end_day},
    )
    return row or {"shorts_count": 0, "shorts_seconds": 0}


def activity_heatmap(start_day: date, end_day: date) -> list[dict[str, Any]]:
    """
    Activity level by hour × day-of-week for the Time Heatmap view (§12.2).

    Each cell: item count and engagement seconds attributed to the hour of
    last_accessed_at (approximation — cumulative engagement is attributed to
    the most recent access hour), plus a regret marker when any regret event
    was recorded in that hour/day cell.
    """
    activity = postgresql_manager.fetchall(
        f"""
        SELECT EXTRACT(DOW  FROM last_accessed_at)::int AS day_of_week,
               EXTRACT(HOUR FROM last_accessed_at)::int AS hour,
               COUNT(memory_id) AS item_count,
               COALESCE(SUM(time_seconds), 0) AS total_seconds
        FROM ({_TIME_UNION}) AS t
        GROUP BY day_of_week, hour
        ORDER BY day_of_week, hour
        """,
        {"start_day": start_day, "end_day": end_day},
    )
    regret_cells = postgresql_manager.fetchall(
        """
        SELECT EXTRACT(DOW FROM marked_at)::int AS day_of_week,
               EXTRACT(HOUR FROM marked_at)::int AS hour,
               COUNT(*) AS regret_count
        FROM regret_events
        WHERE DATE(marked_at) BETWEEN :start_day AND :end_day
        GROUP BY day_of_week, hour
        """,
        {"start_day": start_day, "end_day": end_day},
    )
    regret_map = {(r["day_of_week"], r["hour"]): r["regret_count"] for r in regret_cells}
    for cell in activity:
        cell["regret_count"] = regret_map.get((cell["day_of_week"], cell["hour"]), 0)
        cell["has_regret"] = cell["regret_count"] > 0
    return activity


def recent_items(limit: int = 10) -> list[dict[str, Any]]:
    """Last N items across all sources for the dashboard/extension popup."""
    return postgresql_manager.fetchall(
        """
        SELECT m.memory_id, m.source_type, m.title, m.created_at,
               me.last_accessed_at,
               cm.url, cm.domain,
               gm.sender,
               ym.channel_name, ym.is_short
        FROM memory_items m
        JOIN memory_engagement me ON m.memory_id = me.memory_id
        LEFT JOIN chrome_metadata  cm ON m.memory_id = cm.memory_id
        LEFT JOIN gmail_metadata   gm ON m.memory_id = gm.memory_id
        LEFT JOIN youtube_metadata ym ON m.memory_id = ym.memory_id
        WHERE m.is_deleted = FALSE
        ORDER BY COALESCE(me.last_accessed_at, m.first_ingested_at) DESC
        LIMIT :limit
        """,
        {"limit": limit},
    )
