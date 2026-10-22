"""
time_aggregator.py — WBA time calculation (architecture §11.2).

Time is calculated ON-DEMAND from source tables — never stored separately.
This prevents sync bugs and double-counting.

Inclusion rules (non-negotiable):
  Gmail   — only OPENED emails count (first_opened_at IS NOT NULL).
            Unread emails never contribute to wellbeing time.
  Chrome  — all admitted pages count (they passed the intent gate).
  YouTube — videos with watch_time_seconds > 0 count.

Where the seconds come from (written by ste/storage_engine.py):
  Gmail   dwell_time_seconds — ADDED on every open (update_gmail_engagement).
  Chrome  dwell_time_seconds — ADDED on every visit; the extension sends a page
          once, when it passes the intent gate, so later dwell is never sent.
  YouTube watch_time_seconds — the LARGEST single session (GREATEST), not a sum.
memory_engagement holds one running total per item, not one row per visit.

Attribution day = the day of last_accessed_at (§11.2). The item's WHOLE total
counts on the day it was last opened: reopening an old email moves all of its
earlier reading time to today. This is the doc's rule; a per-visit split would
need a visit table (16-table schema is locked).

Days are the user's LOCAL days. Timestamps in the DB are naive UTC (NOW() on
Neon), so each local day range becomes a UTC range [start, end) before querying.
regret_events.marked_at is the exception: regret_system writes LOCAL time, so
regret queries compare it with local days directly.

All outputs are descriptive numbers. No scoring, no judgment.
Every function is a few pooled, read-only queries (ste.postgresql_manager);
safe to call from any thread.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from ste import postgresql_manager

logger = logging.getLogger(__name__)

# One UNION ALL block per source, each enforcing its own inclusion rule.
# Attribution day = local day of last_accessed_at, given as a UTC range.
_TIME_UNION = """
    SELECT m.memory_id, m.source_type, m.system_group_id,
           me.dwell_time_seconds AS time_seconds,
           me.last_accessed_at
    FROM memory_items m
    JOIN memory_engagement me ON m.memory_id = me.memory_id
    WHERE m.source_type = 'gmail'
      AND m.is_deleted = FALSE
      AND me.first_opened_at IS NOT NULL
      AND me.last_accessed_at >= :start_utc AND me.last_accessed_at < :end_utc
    UNION ALL
    SELECT m.memory_id, m.source_type, m.system_group_id,
           me.dwell_time_seconds AS time_seconds,
           me.last_accessed_at
    FROM memory_items m
    JOIN memory_engagement me ON m.memory_id = me.memory_id
    WHERE m.source_type = 'chrome'
      AND m.is_deleted = FALSE
      AND me.last_accessed_at >= :start_utc AND me.last_accessed_at < :end_utc
    UNION ALL
    SELECT m.memory_id, m.source_type, m.system_group_id,
           me.watch_time_seconds AS time_seconds,
           me.last_accessed_at
    FROM memory_items m
    JOIN memory_engagement me ON m.memory_id = me.memory_id
    WHERE m.source_type = 'youtube'
      AND m.is_deleted = FALSE
      AND me.watch_time_seconds > 0
      AND me.last_accessed_at >= :start_utc AND me.last_accessed_at < :end_utc
"""


def local_day_range_utc(start_day: date, end_day: date) -> dict[str, datetime]:
    """Local days start_day..end_day (inclusive) -> naive UTC bounds {start_utc, end_utc}.

    end_utc is the UTC moment of the local midnight AFTER end_day, so queries
    use `>= start_utc AND < end_utc`. Example (IST): 18 Oct -> 17 Oct 18:30 to
    18 Oct 18:30 UTC.
    """
    start_local = datetime.combine(start_day, time.min).astimezone()
    end_local = datetime.combine(end_day + timedelta(days=1), time.min).astimezone()
    return {
        "start_utc": start_local.astimezone(timezone.utc).replace(tzinfo=None),
        "end_utc": end_local.astimezone(timezone.utc).replace(tzinfo=None),
    }


def utc_offset_seconds() -> int:
    """This machine's current UTC offset in seconds (IST = 19800).

    Used to turn UTC timestamps into local hours for the heatmap. A DST change
    inside the range would shift those cells by one hour (IST has no DST).
    """
    offset = datetime.now().astimezone().utcoffset()
    return int(offset.total_seconds()) if offset else 0


def total_time(start_day: date, end_day: date) -> int:
    """Total wellbeing seconds across all sources for the local day range (inclusive)."""
    value = postgresql_manager.scalar(
        f"SELECT COALESCE(SUM(time_seconds), 0) FROM ({_TIME_UNION}) AS all_time",
        local_day_range_utc(start_day, end_day),
    )
    total = int(value or 0)
    logger.debug("total_time %s..%s -> %ss", start_day, end_day, total)
    return total


def total_time_today() -> int:
    """Total wellbeing seconds for today, local day (architecture §11.2 reference query)."""
    today = date.today()
    return total_time(today, today)


def time_by_system_group(start_day: date, end_day: date) -> list[dict[str, Any]]:
    """Seconds per system group (work/study/entertainment/personal/misc).

    Every group is returned, with 0 when it has no time in the range.
    """
    rows = postgresql_manager.fetchall(
        f"""
        SELECT sg.group_name,
               COALESCE(SUM(t.time_seconds), 0) AS total_seconds,
               COUNT(t.memory_id) AS item_count
        FROM system_groups sg
        LEFT JOIN ({_TIME_UNION}) AS t ON t.system_group_id = sg.system_group_id
        GROUP BY sg.system_group_id, sg.group_name
        ORDER BY sg.system_group_id
        """,
        local_day_range_utc(start_day, end_day),
    )
    logger.debug("time_by_system_group %s..%s -> %d groups", start_day, end_day, len(rows))
    return rows


def time_by_source(start_day: date, end_day: date) -> list[dict[str, Any]]:
    """Seconds per source type (chrome/gmail/youtube).

    All three sources are always returned, with 0 when a source has no time
    (same shape as time_by_system_group).
    """
    rows = postgresql_manager.fetchall(
        f"""
        SELECT s.source_type,
               COALESCE(SUM(t.time_seconds), 0) AS total_seconds,
               COUNT(t.memory_id) AS item_count
        FROM (VALUES ('chrome'), ('gmail'), ('youtube')) AS s(source_type)
        LEFT JOIN ({_TIME_UNION}) AS t ON t.source_type = s.source_type
        GROUP BY s.source_type
        ORDER BY s.source_type
        """,
        local_day_range_utc(start_day, end_day),
    )
    logger.debug("time_by_source %s..%s -> %d sources", start_day, end_day, len(rows))
    return rows


def shorts_summary(start_day: date, end_day: date) -> dict[str, Any]:
    """
    Separate Shorts analytics (architecture §7.4): count and total watch time.
    Shorts are tracked apart from long-form in the wellbeing dashboard; they are
    a subset of the YouTube time above, not added on top of it.
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
          AND me.last_accessed_at >= :start_utc AND me.last_accessed_at < :end_utc
        """,
        local_day_range_utc(start_day, end_day),
    )
    summary = row or {"shorts_count": 0, "shorts_seconds": 0}
    logger.debug("shorts_summary %s..%s -> %s shorts", start_day, end_day, summary["shorts_count"])
    return summary


def activity_heatmap(start_day: date, end_day: date) -> list[dict[str, Any]]:
    """
    Activity level by hour × day-of-week for the Time Heatmap view (§12.2).

    Each cell: item count and engagement seconds attributed to the LOCAL hour of
    last_accessed_at (approximation — cumulative engagement is attributed to
    the most recent access hour), plus a regret marker when any regret event
    was recorded in that hour/day cell. day_of_week is Postgres DOW
    (0 = Sunday .. 6 = Saturday) for both activity and regret cells.
    """
    params: dict[str, Any] = {
        **local_day_range_utc(start_day, end_day),
        "utc_offset_seconds": utc_offset_seconds(),
    }
    activity = postgresql_manager.fetchall(
        f"""
        SELECT EXTRACT(DOW  FROM local_at)::int AS day_of_week,
               EXTRACT(HOUR FROM local_at)::int AS hour,
               COUNT(memory_id) AS item_count,
               COALESCE(SUM(time_seconds), 0) AS total_seconds
        FROM (
            SELECT t.memory_id, t.time_seconds,
                   t.last_accessed_at + make_interval(secs => :utc_offset_seconds) AS local_at
            FROM ({_TIME_UNION}) AS t
        ) AS shifted
        GROUP BY day_of_week, hour
        ORDER BY day_of_week, hour
        """,
        params,
    )
    # marked_at is already local time (regret_system writes datetime.now()),
    # so it is compared with the local days as they are.
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
    logger.debug(
        "activity_heatmap %s..%s -> %d cells, %d regret cells",
        start_day, end_day, len(activity), len(regret_cells),
    )
    return activity


def recent_items(limit: int = 10) -> list[dict[str, Any]]:
    """Last N items across all sources for the dashboard/extension popup.

    Ordered by last open; items never opened (unread Gmail) fall back to their
    ingestion time.
    """
    rows = postgresql_manager.fetchall(
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
    logger.debug("recent_items limit=%d -> %d rows", limit, len(rows))
    return rows
