"""
wellbeing_analytics.py — WBA core: session computation + module facade.

Session model (architecture §11.5, DB Tables 12–13):
  A session is a contiguous block of activity where consecutive items are
  within SESSION_GAP_MINUTES (20) of each other; a longer gap closes the
  session. DEVIATION: §11.5 says 5 minutes — raised to 20 on the owner's
  decision (2026-10-18). Each item is ONE point in time (its last access, see
  below), so a 15-minute read followed by the next page looked like a 15-minute
  gap and split one sitting into two sessions under the 5-minute rule.
  Stored in sessions + session_memory_map with:
    total_duration_seconds — pre-computed sum (accepted denormalization),
    dominant_group_id      — system group with the most time in the session,
    source_switch_count    — descriptive count of source-type changes.

Item time = COALESCE(last_accessed_at, first_ingested_at): the same single
point the time totals use (time_aggregator, attribution = last access). Days
are the user's LOCAL days; timestamps are naive UTC, so each day becomes a UTC
range (time_aggregator.local_day_range_utc). Sessions are built per local day,
so a sitting across midnight becomes two sessions. session_start/session_end
are stored as naive UTC like every other timestamp.

Derived analytics (on-demand, never stored):
  focus_score   = 1 / unique_source_types_in_session
  fragmentation = sessions_per_day / average_session_duration_minutes

daily_summary() is the facade the dashboard consumes: time totals, category
breakdown, Shorts stats, regret rate, and focus — all descriptive.

Sessions are built on demand: ensure_sessions_for_range runs before every
reader (daily_summary, GET /sessions, the weekly insight). POST
/sessions/rebuild stays as a manual rebuild of one day.

Threading: rebuild_sessions_for_day takes a per-day Postgres advisory lock, so
two rebuilds of the same day (two requests, or two processes) run one after
the other instead of both deleting and both inserting (duplicate sessions).
The reads are pooled, read-only queries.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from ste import postgresql_manager
from wba import time_aggregator
from wba.regret_system import regret_rate

logger = logging.getLogger(__name__)

SESSION_GAP_MINUTES = 20

# First key of pg_advisory_xact_lock(namespace, day): keeps session-rebuild
# locks apart from any other advisory lock. Second key = date.toordinal().
_SESSION_REBUILD_LOCK_NAMESPACE = 4201

# Past local days already rebuilt by ensure_sessions_for_range in this process.
# Set.add is atomic in CPython, and a duplicate rebuild is harmless (idempotent).
_past_days_built: set[date] = set()


def _fetch_day_activity(connection: Connection, day: date) -> list[dict[str, Any]]:
    """
    Engaged items for one local day, ordered by activity timestamp. Follows the
    same inclusion rules as time aggregation (§11.2).
    """
    rows = connection.execute(
        text(
            """
            SELECT m.memory_id, m.source_type, m.system_group_id,
                   COALESCE(me.last_accessed_at, m.first_ingested_at) AS activity_at,
                   CASE WHEN m.source_type = 'youtube' THEN me.watch_time_seconds
                        ELSE me.dwell_time_seconds END AS time_seconds
            FROM memory_items m
            JOIN memory_engagement me ON m.memory_id = me.memory_id
            WHERE m.is_deleted = FALSE
              AND COALESCE(me.last_accessed_at, m.first_ingested_at) >= :start_utc
              AND COALESCE(me.last_accessed_at, m.first_ingested_at) <  :end_utc
              AND (
                    (m.source_type = 'gmail'   AND me.first_opened_at IS NOT NULL)
                 OR (m.source_type = 'chrome')
                 OR (m.source_type = 'youtube' AND me.watch_time_seconds > 0)
              )
            ORDER BY activity_at ASC, m.memory_id
            """
        ),
        time_aggregator.local_day_range_utc(day, day),
    )
    return [dict(row) for row in rows.mappings()]


def _split_into_sessions(items: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """Group time-ordered items into sessions using the SESSION_GAP_MINUTES rule."""
    sessions: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    gap = timedelta(minutes=SESSION_GAP_MINUTES)

    for item in items:
        if not current:
            current = [item]
            continue
        if item["activity_at"] - current[-1]["activity_at"] > gap:
            sessions.append(current)
            current = [item]
        else:
            current.append(item)
    if current:
        sessions.append(current)
    return sessions


def _dominant_group_id(items: list[dict[str, Any]]) -> int | None:
    """System group that accumulated the most time within the session.

    Equal time goes to the group seen first in the session.
    """
    totals: dict[int, int] = {}
    for item in items:
        group_id = item.get("system_group_id")
        if group_id is None:
            continue
        totals[group_id] = totals.get(group_id, 0) + int(item.get("time_seconds") or 0)
    if not totals:
        return None
    return max(totals, key=totals.get)


def _source_switch_count(items: list[dict[str, Any]]) -> int:
    """Number of times the source type changes between consecutive items."""
    switches = 0
    for previous, current in zip(items, items[1:]):
        if previous["source_type"] != current["source_type"]:
            switches += 1
    return switches


def rebuild_sessions_for_day(day: date) -> dict[str, Any]:
    """
    (Re)compute sessions for one local day and persist them. Idempotent:
    existing sessions starting on that day are deleted first
    (session_memory_map rows cascade), then rebuilt from current engagement
    data — all in one transaction under the per-day advisory lock.
    """
    day_range = time_aggregator.local_day_range_utc(day, day)
    with postgresql_manager.transaction() as connection:
        connection.execute(
            text("SELECT pg_advisory_xact_lock(:namespace, :day_key)"),
            {"namespace": _SESSION_REBUILD_LOCK_NAMESPACE, "day_key": day.toordinal()},
        )
        items = _fetch_day_activity(connection, day)
        session_groups = _split_into_sessions(items)

        connection.execute(
            text("DELETE FROM sessions WHERE session_start >= :start_utc AND session_start < :end_utc"),
            day_range,
        )
        for session_items in session_groups:
            row = connection.execute(
                text(
                    """
                    INSERT INTO sessions
                        (session_start, session_end, total_duration_seconds,
                         dominant_group_id, source_switch_count)
                    VALUES (:start, :end, :duration, :dominant, :switches)
                    RETURNING session_id
                    """
                ),
                {
                    "start": session_items[0]["activity_at"],
                    "end": session_items[-1]["activity_at"],
                    "duration": sum(int(i.get("time_seconds") or 0) for i in session_items),
                    "dominant": _dominant_group_id(session_items),
                    "switches": _source_switch_count(session_items),
                },
            ).mappings().first()
            session_id = str(row["session_id"])
            # One executemany per session instead of one round trip per item.
            connection.execute(
                text(
                    """
                    INSERT INTO session_memory_map (session_id, memory_id)
                    VALUES (:session_id, :memory_id)
                    ON CONFLICT DO NOTHING
                    """
                ),
                [{"session_id": session_id, "memory_id": str(item["memory_id"])} for item in session_items],
            )

    logger.info("Sessions rebuilt for %s: %d sessions from %d items (gap %d min)",
                day, len(session_groups), len(items), SESSION_GAP_MINUTES)
    return {"day": day.isoformat(), "sessions": len(session_groups), "items": len(items)}


def ensure_sessions_for_range(start_day: date, end_day: date) -> None:
    """
    Make sure stored sessions exist for every local day in [start_day, end_day].
    Readers (daily_summary, GET /sessions, the weekly insight) call this first,
    so nothing has to trigger a rebuild by hand.

    Today is rebuilt on every call (still changing; one day of items is cheap).
    A past day is rebuilt once per process, then skipped. A restart rebuilds it
    again, which is harmless: the rebuild is idempotent. Future days are skipped.
    Rebuild errors propagate; the route that called this reports them.
    """
    today = date.today()
    day = start_day
    while day <= min(end_day, today):
        if day == today or day not in _past_days_built:
            rebuild_sessions_for_day(day)
            if day < today:
                _past_days_built.add(day)
        day += timedelta(days=1)


def ensure_sessions_for_day(day: date) -> None:
    ensure_sessions_for_range(day, day)


def sessions_for_day(day: date) -> list[dict[str, Any]]:
    """Stored sessions for a local day with the derived focus score per session.

    Items deleted after the session was built are left out of the counts.
    """
    rows = postgresql_manager.fetchall(
        """
        SELECT s.session_id, s.session_start, s.session_end,
               s.total_duration_seconds, s.source_switch_count,
               sg.group_name AS dominant_group,
               COUNT(m.memory_id) AS item_count,
               COUNT(DISTINCT m.source_type) AS unique_sources
        FROM sessions s
        LEFT JOIN system_groups sg ON sg.system_group_id = s.dominant_group_id
        LEFT JOIN session_memory_map smm ON smm.session_id = s.session_id
        LEFT JOIN memory_items m ON m.memory_id = smm.memory_id AND m.is_deleted = FALSE
        WHERE s.session_start >= :start_utc AND s.session_start < :end_utc
        GROUP BY s.session_id, sg.group_name
        ORDER BY s.session_start ASC
        """,
        time_aggregator.local_day_range_utc(day, day),
    )
    for row in rows:
        row["session_id"] = str(row["session_id"])
        unique = row.get("unique_sources") or 0
        row["focus_score"] = round(1.0 / unique, 3) if unique else None
    logger.debug("sessions_for_day %s -> %d sessions", day, len(rows))
    return rows


def fragmentation_for_day(day: date) -> dict[str, Any]:
    """fragmentation = sessions_per_day / average_session_duration_minutes (local day)."""
    row = postgresql_manager.fetchone(
        """
        SELECT COUNT(*) AS session_count,
               COALESCE(AVG(total_duration_seconds), 0) AS avg_duration_seconds
        FROM sessions
        WHERE session_start >= :start_utc AND session_start < :end_utc
        """,
        time_aggregator.local_day_range_utc(day, day),
    )
    session_count = int(row["session_count"] or 0)
    avg_minutes = float(row["avg_duration_seconds"] or 0) / 60.0
    fragmentation = round(session_count / avg_minutes, 3) if avg_minutes > 0 else None
    return {
        "day": day.isoformat(),
        "session_count": session_count,
        "avg_session_minutes": round(avg_minutes, 1),
        "fragmentation": fragmentation,
    }


def daily_summary(day: date | None = None) -> dict[str, Any]:
    """
    Dashboard Daily Summary (§12.2): total time, category breakdown, Shorts,
    regret rate, session fragmentation. Descriptive numbers only.
    """
    day = day or date.today()
    logger.debug("daily_summary %s", day)
    ensure_sessions_for_day(day)
    return {
        "day": day.isoformat(),
        "generated_at": datetime.now().isoformat(),
        "total_seconds": time_aggregator.total_time(day, day),
        "by_system_group": time_aggregator.time_by_system_group(day, day),
        "by_source": time_aggregator.time_by_source(day, day),
        "shorts": time_aggregator.shorts_summary(day, day),
        "regret": regret_rate(day),
        "sessions": fragmentation_for_day(day),
    }
