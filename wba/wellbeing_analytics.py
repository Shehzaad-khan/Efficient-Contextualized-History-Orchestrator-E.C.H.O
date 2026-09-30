"""
wellbeing_analytics.py — WBA core: session computation + module facade.

Session model (architecture §11.5, DB Tables 12–13):
  A session is a contiguous block of activity where consecutive items are
  within 5 minutes of each other. A gap > 5 minutes closes the session.
  Stored in sessions + session_memory_map with:
    total_duration_seconds — pre-computed sum (accepted denormalization),
    dominant_group_id      — system group with the most time in the session,
    source_switch_count    — descriptive count of source-type changes.

Derived analytics (on-demand, never stored):
  focus_score   = 1 / unique_source_types_in_session
  fragmentation = sessions_per_day / average_session_duration_minutes

daily_summary() is the facade the dashboard consumes: time totals, category
breakdown, Shorts stats, regret rate, and focus — all descriptive.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any

from sqlalchemy import text

from ste import postgresql_manager
from wba import time_aggregator
from wba.regret_system import regret_rate

logger = logging.getLogger(__name__)

SESSION_GAP_MINUTES = 5


def _fetch_day_activity(day: date) -> list[dict[str, Any]]:
    """
    Engaged items for one day, ordered by activity timestamp. Follows the
    same inclusion rules as time aggregation (§11.2).
    """
    return postgresql_manager.fetchall(
        """
        SELECT m.memory_id, m.source_type, m.system_group_id,
               COALESCE(me.last_accessed_at, m.first_ingested_at) AS activity_at,
               CASE WHEN m.source_type = 'youtube' THEN me.watch_time_seconds
                    ELSE me.dwell_time_seconds END AS time_seconds
        FROM memory_items m
        JOIN memory_engagement me ON m.memory_id = me.memory_id
        WHERE m.is_deleted = FALSE
          AND DATE(COALESCE(me.last_accessed_at, m.first_ingested_at)) = :day
          AND (
                (m.source_type = 'gmail'   AND me.first_opened_at IS NOT NULL)
             OR (m.source_type = 'chrome')
             OR (m.source_type = 'youtube' AND me.watch_time_seconds > 0)
          )
        ORDER BY activity_at ASC
        """,
        {"day": day},
    )


def _split_into_sessions(items: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """Group time-ordered items into sessions using the 5-minute gap rule."""
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
    """System group that accumulated the most time within the session."""
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
    switches = 0
    for previous, current in zip(items, items[1:]):
        if previous["source_type"] != current["source_type"]:
            switches += 1
    return switches


def rebuild_sessions_for_day(day: date) -> dict[str, Any]:
    """
    (Re)compute sessions for one day and persist them. Idempotent: existing
    sessions starting on that day are deleted first (session_memory_map rows
    cascade), then rebuilt from current engagement data.
    """
    items = _fetch_day_activity(day)
    session_groups = _split_into_sessions(items)

    with postgresql_manager.transaction() as connection:
        connection.execute(
            text("DELETE FROM sessions WHERE DATE(session_start) = :day"),
            {"day": day},
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
            for item in session_items:
                connection.execute(
                    text(
                        """
                        INSERT INTO session_memory_map (session_id, memory_id)
                        VALUES (:session_id, :memory_id)
                        ON CONFLICT DO NOTHING
                        """
                    ),
                    {"session_id": session_id, "memory_id": str(item["memory_id"])},
                )

    logger.info("rebuild_sessions_for_day: %s → %d sessions from %d items",
                day, len(session_groups), len(items))
    return {"day": day.isoformat(), "sessions": len(session_groups), "items": len(items)}


def sessions_for_day(day: date) -> list[dict[str, Any]]:
    """Stored sessions for a day with the derived focus score per session."""
    rows = postgresql_manager.fetchall(
        """
        SELECT s.session_id, s.session_start, s.session_end,
               s.total_duration_seconds, s.source_switch_count,
               sg.group_name AS dominant_group,
               COUNT(smm.memory_id) AS item_count,
               COUNT(DISTINCT m.source_type) AS unique_sources
        FROM sessions s
        LEFT JOIN system_groups sg ON sg.system_group_id = s.dominant_group_id
        LEFT JOIN session_memory_map smm ON smm.session_id = s.session_id
        LEFT JOIN memory_items m ON m.memory_id = smm.memory_id
        WHERE DATE(s.session_start) = :day
        GROUP BY s.session_id, sg.group_name
        ORDER BY s.session_start ASC
        """,
        {"day": day},
    )
    for row in rows:
        row["session_id"] = str(row["session_id"])
        unique = row.get("unique_sources") or 0
        row["focus_score"] = round(1.0 / unique, 3) if unique else None
    return rows


def fragmentation_for_day(day: date) -> dict[str, Any]:
    """fragmentation = sessions_per_day / average_session_duration_minutes."""
    row = postgresql_manager.fetchone(
        """
        SELECT COUNT(*) AS session_count,
               COALESCE(AVG(total_duration_seconds), 0) AS avg_duration_seconds
        FROM sessions
        WHERE DATE(session_start) = :day
        """,
        {"day": day},
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
