"""
time_aggregator.py — WBA time calculation (architecture §11.2).

Time is calculated ON-DEMAND from source tables — never stored separately.
This prevents sync bugs and double-counting.

Inclusion rules (non-negotiable):
  Gmail   — only OPENED emails count (first_opened_at IS NOT NULL).
            Unread emails never contribute to wellbeing time.
  Chrome  — all admitted pages count (they passed the intent gate).
  YouTube — visits with dwell_seconds > 0 count.

Where the seconds come from (written by ste/storage_engine.py):
  Gmail   memory_engagement.dwell_time_seconds — ADDED on every open
          (update_gmail_engagement). One running total per item.
  Chrome  visits.dwell_seconds — one row per gated visit (the extension sends a
          page once, when it passes the intent gate).
  YouTube visits.dwell_seconds — one row per gated watch; heartbeats raise the
          latest row to that session's running watch time.

Attribution day (A30, settled by A1.1b): Chrome and YouTube time lands on the
day of EACH visit (visits.visited_at), so reopening an old page no longer moves
its earlier time to today. Gmail keeps the §11.2 rule: the item's whole total
counts on the day of last_accessed_at.

Days are the user's LOCAL days. Timestamps in the DB are naive UTC (the session
runs in UTC, A31.4), so each local day range becomes a UTC range [start, end)
before querying.
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

# One UNION ALL block per source, each enforcing its own inclusion rule. Every row
# is one (item, moment) with the seconds spent: Gmail = the item's total on the day of
# its last open; Chrome/YouTube = one visit. The day is the local day of activity_at,
# given as a UTC range. Several visits of one item are several rows, so item counts
# use COUNT(DISTINCT memory_id). Public: regret_system and pattern_detector reuse it
# so their numbers stay inside the same total.
TIME_UNION_SQL = """
    SELECT m.memory_id, m.source_type, m.system_group_id,
           me.dwell_time_seconds AS time_seconds,
           me.last_accessed_at AS activity_at
    FROM memory_items m
    JOIN memory_engagement me ON m.memory_id = me.memory_id
    WHERE m.source_type = 'gmail'
      AND me.first_opened_at IS NOT NULL
      AND me.last_accessed_at >= :start_utc AND me.last_accessed_at < :end_utc
    UNION ALL
    SELECT m.memory_id, m.source_type, m.system_group_id,
           v.dwell_seconds AS time_seconds,
           v.visited_at AS activity_at
    FROM visits v
    JOIN memory_items m ON m.memory_id = v.memory_id
    WHERE m.source_type = 'chrome'
      AND v.visited_at >= :start_utc AND v.visited_at < :end_utc
    UNION ALL
    SELECT m.memory_id, m.source_type, m.system_group_id,
           v.dwell_seconds AS time_seconds,
           v.visited_at AS activity_at
    FROM visits v
    JOIN memory_items m ON m.memory_id = v.memory_id
    WHERE m.source_type = 'youtube'
      AND v.dwell_seconds > 0
      AND v.visited_at >= :start_utc AND v.visited_at < :end_utc
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
        f"SELECT COALESCE(SUM(time_seconds), 0) FROM ({TIME_UNION_SQL}) AS all_time",
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
               COUNT(DISTINCT t.memory_id) AS item_count
        FROM system_groups sg
        LEFT JOIN ({TIME_UNION_SQL}) AS t ON t.system_group_id = sg.system_group_id
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
               COUNT(DISTINCT t.memory_id) AS item_count
        FROM (VALUES ('chrome'), ('gmail'), ('youtube')) AS s(source_type)
        LEFT JOIN ({TIME_UNION_SQL}) AS t ON t.source_type = s.source_type
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
        SELECT COUNT(DISTINCT m.memory_id) AS shorts_count,
               COALESCE(SUM(v.dwell_seconds), 0) AS shorts_seconds
        FROM visits v
        JOIN memory_items m ON m.memory_id = v.memory_id
        JOIN youtube_metadata ym ON m.memory_id = ym.memory_id
        WHERE ym.is_short = TRUE
          AND v.dwell_seconds > 0
          AND v.visited_at >= :start_utc AND v.visited_at < :end_utc
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
    the activity (a Chrome/YouTube visit's time; for Gmail the last open, where
    the cumulative total is attributed to the most recent access hour), plus a
    regret marker when any regret event was recorded in that hour/day cell.
    day_of_week is Postgres DOW (0 = Sunday .. 6 = Saturday) for both activity
    and regret cells.
    """
    params: dict[str, Any] = {
        **local_day_range_utc(start_day, end_day),
        "utc_offset_seconds": utc_offset_seconds(),
    }
    activity = postgresql_manager.fetchall(
        f"""
        SELECT EXTRACT(DOW  FROM local_at)::int AS day_of_week,
               EXTRACT(HOUR FROM local_at)::int AS hour,
               COUNT(DISTINCT memory_id) AS item_count,
               COALESCE(SUM(time_seconds), 0) AS total_seconds
        FROM (
            SELECT t.memory_id, t.time_seconds,
                   t.activity_at + make_interval(secs => :utc_offset_seconds) AS local_at
            FROM ({TIME_UNION_SQL}) AS t
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


# Same link rules as the Recall answers (rse/llm_synthesizer.py `_link`): a Gmail
# thread under #all (survives archiving), the video's watch page. /u/0/ is the
# first signed-in Google account in the browser.
GMAIL_LINK = "https://mail.google.com/mail/u/0/#all/{}"
YOUTUBE_LINK = "https://www.youtube.com/watch?v={}"


def source_link(row: dict[str, Any]) -> str | None:
    """URL that opens the original item (D6.7): the page, the Gmail thread, or the video."""
    source = row.get("source_type")
    if source == "chrome":
        return row.get("url") or None
    if source == "gmail":
        thread_or_email = row.get("thread_id") or row.get("email_id")
        return GMAIL_LINK.format(thread_or_email) if thread_or_email else None
    if source == "youtube":
        return YOUTUBE_LINK.format(row["video_id"]) if row.get("video_id") else None
    return None


def items_in_system_group(group_name: str, day: date, limit: int = 100) -> list[dict[str, Any]]:
    """Items behind one category's time on a local day, longest time first.

    Same rows as `time_by_system_group` (TIME_UNION_SQL), so these items add up to
    that category's total. An item with several visits is one row with the summed
    seconds. Each row has the display fields of `recent_items` plus `total_seconds`.
    """
    rows = postgresql_manager.fetchall(
        f"""
        SELECT m.memory_id, m.source_type, m.title, m.created_at,
               COALESCE(SUM(t.time_seconds), 0) AS total_seconds,
               MAX(t.activity_at) AS last_accessed_at,
               cm.url, cm.domain,
               gm.sender, gm.thread_id, gm.email_id,
               ym.channel_name, ym.is_short, ym.video_id
        FROM ({TIME_UNION_SQL}) AS t
        JOIN memory_items m ON m.memory_id = t.memory_id
        JOIN system_groups sg ON sg.system_group_id = t.system_group_id
        LEFT JOIN chrome_metadata  cm ON m.memory_id = cm.memory_id
        LEFT JOIN gmail_metadata   gm ON m.memory_id = gm.memory_id
        LEFT JOIN youtube_metadata ym ON m.memory_id = ym.memory_id
        WHERE sg.group_name = :group_name
        GROUP BY m.memory_id, m.source_type, m.title, m.created_at,
                 cm.url, cm.domain, gm.sender, gm.thread_id, gm.email_id,
                 ym.channel_name, ym.is_short, ym.video_id
        ORDER BY total_seconds DESC, m.memory_id
        LIMIT :limit
        """,
        {**local_day_range_utc(day, day), "group_name": group_name, "limit": limit},
    )
    for row in rows:
        row["url"] = source_link(row)
        for key in ("thread_id", "email_id", "video_id"):
            row.pop(key, None)
    logger.debug("items_in_system_group %s %s -> %d rows", group_name, day, len(rows))
    return rows


# "All time" for the user-group queries: TIME_UNION_SQL wants a range, so give it one that holds every row.
_ALL_TIME_START = datetime(1970, 1, 1)


def _all_time_range() -> dict[str, datetime]:
    return {"start_utc": _ALL_TIME_START, "end_utc": datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(days=1)}


def time_in_user_groups() -> dict[str, int]:
    """Total time spent on the items of each user (goal) group, all time: {group_id: seconds}.

    Same rows as every other time total (TIME_UNION_SQL). Groups whose items have no time are absent.
    An item in two groups counts in both, like its membership does.
    """
    rows = postgresql_manager.fetchall(
        f"""
        SELECT mug.group_id, COALESCE(SUM(t.time_seconds), 0) AS total_seconds
        FROM memory_user_groups mug
        JOIN ({TIME_UNION_SQL}) AS t ON t.memory_id = mug.memory_id
        GROUP BY mug.group_id
        """,
        _all_time_range(),
    )
    return {str(row["group_id"]): int(row["total_seconds"] or 0) for row in rows}


def items_in_user_group(group_id: str, limit: int = 200) -> list[dict[str, Any]]:
    """The members of one user (goal) group with the time spent on each, all time, most time first.

    Members with no recorded time are listed too (0 seconds). An item with several visits is one row
    with the summed seconds. Each row has the display fields of `recent_items` plus `total_seconds`.
    """
    rows = postgresql_manager.fetchall(
        f"""
        SELECT m.memory_id, m.source_type, m.title, m.created_at,
               COALESCE(SUM(t.time_seconds), 0) AS total_seconds,
               MAX(t.activity_at) AS last_accessed_at,
               cm.url, cm.domain,
               gm.sender, gm.thread_id, gm.email_id,
               ym.channel_name, ym.is_short, ym.video_id
        FROM memory_user_groups mug
        JOIN memory_items m ON m.memory_id = mug.memory_id
        LEFT JOIN ({TIME_UNION_SQL}) AS t ON t.memory_id = m.memory_id
        LEFT JOIN chrome_metadata  cm ON m.memory_id = cm.memory_id
        LEFT JOIN gmail_metadata   gm ON m.memory_id = gm.memory_id
        LEFT JOIN youtube_metadata ym ON m.memory_id = ym.memory_id
        WHERE mug.group_id = :group_id
        GROUP BY m.memory_id, m.source_type, m.title, m.created_at,
                 cm.url, cm.domain, gm.sender, gm.thread_id, gm.email_id,
                 ym.channel_name, ym.is_short, ym.video_id
        ORDER BY total_seconds DESC, m.created_at DESC, m.memory_id
        LIMIT :limit
        """,
        {**_all_time_range(), "group_id": group_id, "limit": limit},
    )
    for row in rows:
        row["url"] = source_link(row)
        for key in ("thread_id", "email_id", "video_id"):
            row.pop(key, None)
    logger.debug("items_in_user_group %s -> %d rows", group_id, len(rows))
    return rows


RECENT_SORT_TS = "COALESCE(me.last_accessed_at, m.first_ingested_at)"

# Keyset page (not OFFSET): items arrive continuously, so an offset would skip or
# repeat rows between two pages. The cursor is the (sort time, memory_id) of the
# last row already shown; memory_id breaks ties between equal timestamps.
RECENT_BEFORE_SQL = f"WHERE ({RECENT_SORT_TS}, m.memory_id) < (:before_ts, CAST(:before_id AS uuid))"


def recent_items(limit: int = 10, before: tuple[datetime, str] | None = None) -> list[dict[str, Any]]:
    """Last N items across all sources for the dashboard/extension popup.

    Ordered by last open; items never opened (unread Gmail) fall back to their
    ingestion time. `url` opens the original for every source (D6.7): the page
    for Chrome, the Gmail thread, or the YouTube video. The ids it is built from
    are not returned.

    `before` = (sort time, memory_id) of the last item already shown: the next
    page starts right after it. Every row carries `sort_ts` (naive UTC, the value
    it is ordered by) so the caller can build the next cursor; the caller removes
    it before the row goes to the browser.

    Every row also carries `regretted`: True when the item is currently marked as a
    regret (an odd number of toggle events, see wba/regret_system.py), so the page can
    offer "undo" instead of "mark regret" for items already marked.
    """
    params: dict[str, Any] = {"limit": limit}
    before_clause = ""
    if before is not None:
        params["before_ts"], params["before_id"] = before
        before_clause = RECENT_BEFORE_SQL
    rows = postgresql_manager.fetchall(
        f"""
        SELECT m.memory_id, m.source_type, m.title, m.created_at,
               me.last_accessed_at,
               {RECENT_SORT_TS} AS sort_ts,
               COALESCE(rg.marks, 0) % 2 = 1 AS regretted,
               cm.url, cm.domain,
               gm.sender, gm.thread_id, gm.email_id,
               ym.channel_name, ym.is_short, ym.video_id
        FROM memory_items m
        JOIN memory_engagement me ON m.memory_id = me.memory_id
        LEFT JOIN chrome_metadata  cm ON m.memory_id = cm.memory_id
        LEFT JOIN gmail_metadata   gm ON m.memory_id = gm.memory_id
        LEFT JOIN youtube_metadata ym ON m.memory_id = ym.memory_id
        LEFT JOIN (SELECT memory_id, COUNT(*) AS marks FROM regret_events GROUP BY memory_id) rg
               ON m.memory_id = rg.memory_id
        {before_clause}
        ORDER BY {RECENT_SORT_TS} DESC, m.memory_id DESC
        LIMIT :limit
        """,
        params,
    )
    for row in rows:
        row["url"] = source_link(row)
        for key in ("thread_id", "email_id", "video_id"):
            row.pop(key, None)
    logger.debug("recent_items limit=%d paged=%s -> %d rows", limit, before is not None, len(rows))
    return rows
