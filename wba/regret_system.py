"""
regret_system.py — WBA user-declared regret (architecture §11.6, DB Table 8).

What regret is: the USER's own reflection — "I spent time on this and wish I
hadn't." Echo never decides what is regretful; it only records and reflects.

Storage design (locked): each mark AND unmark is a NEW row in regret_events.
Rows are never updated or deleted. Current status = odd row count for the
memory_id. regret_hour / regret_day_of_week are precomputed at insert time
for fast pattern queries.

Reminders (optional, rate-limited): fire only when a similar regret pattern
exists AND the current session is >= 15 minutes AND rate limits allow AND the
user hasn't disabled them. Content is always the user's OWN prior note.
Rate-limit state lives in Redis (ephemeral by design; the disable flag is a
persistent Redis key because the locked 16-table schema has no settings table).
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Optional

from ste import postgresql_manager

logger = logging.getLogger(__name__)

MIN_SESSION_MINUTES_FOR_REMINDER = 15
MAX_REMINDERS_PER_TYPE_PER_DAY = 1
MAX_REMINDERS_TOTAL_PER_DAY = 2
REMINDER_COOLDOWN_SECONDS = 2 * 3600
_DAY_TTL_SECONDS = 24 * 3600

_DISABLED_KEY = "echo:regret:reminders:disabled"


def _redis():
    """Sync Redis client; None when Redis is unreachable (reminders degrade off)."""
    try:
        from ste.redis_manager import get_sync_client
        client = get_sync_client()
        client.ping()
        return client
    except Exception as exc:
        logger.warning("regret_system: Redis unavailable — reminders disabled: %s", exc)
        return None


# ── Marking ───────────────────────────────────────────────────────────────────

def is_regretted(memory_id: str) -> bool:
    """Current regret status: odd event count = currently regretted."""
    count = postgresql_manager.scalar(
        "SELECT COUNT(*) FROM regret_events WHERE memory_id = :memory_id",
        {"memory_id": memory_id},
    )
    return int(count or 0) % 2 == 1


def toggle_regret(memory_id: str, note: Optional[str] = None) -> dict[str, Any]:
    """
    Record one regret toggle event (mark if currently unmarked, and vice versa).
    Always an INSERT — never UPDATE/DELETE (full toggle history preserved).

    Returns the new state: {"memory_id", "regretted", "marked_at"}.
    Raises ValueError when the memory item does not exist.
    """
    exists = postgresql_manager.scalar(
        "SELECT 1 FROM memory_items WHERE memory_id = :memory_id AND is_deleted = FALSE",
        {"memory_id": memory_id},
    )
    if not exists:
        raise ValueError(f"No memory item found for memory_id={memory_id}")

    now = datetime.now()
    # Python weekday(): 0=Monday..6=Sunday — matches the DB design's convention.
    postgresql_manager.execute(
        """
        INSERT INTO regret_events (memory_id, marked_at, regret_note, regret_hour, regret_day_of_week)
        VALUES (:memory_id, :marked_at, :note, :hour, :day_of_week)
        """,
        {
            "memory_id": memory_id,
            "marked_at": now,
            "note": (note or None),
            "hour": now.hour,
            "day_of_week": now.weekday(),
        },
    )
    state = is_regretted(memory_id)
    logger.info("toggle_regret: memory_id=%s → regretted=%s", memory_id, state)
    return {"memory_id": memory_id, "regretted": state, "marked_at": now.isoformat()}


def latest_note_for_type(source_type: str) -> Optional[str]:
    """Most recent regret note for a content type — reminder content source."""
    row = postgresql_manager.fetchone(
        """
        SELECT re.regret_note
        FROM regret_events re
        JOIN memory_items m ON m.memory_id = re.memory_id
        WHERE m.source_type = :source_type
          AND re.regret_note IS NOT NULL
        ORDER BY re.marked_at DESC
        LIMIT 1
        """,
        {"source_type": source_type},
    )
    return row["regret_note"] if row else None


# ── Analytics (descriptive only) ──────────────────────────────────────────────

def _currently_regretted_cte() -> str:
    """CTE selecting memory_ids whose regret event count is odd."""
    return """
        WITH regretted AS (
            SELECT memory_id
            FROM regret_events
            GROUP BY memory_id
            HAVING COUNT(*) % 2 = 1
        )
    """


def regret_rate(day: date) -> dict[str, Any]:
    """
    regret_rate = time on currently-regretted items accessed that day
                  / total wellbeing time that day × 100.
    """
    from wba.time_aggregator import total_time

    total_seconds = total_time(day, day)
    regretted_seconds = postgresql_manager.scalar(
        _currently_regretted_cte()
        + """
        SELECT COALESCE(SUM(
            CASE WHEN m.source_type = 'youtube' THEN me.watch_time_seconds
                 ELSE me.dwell_time_seconds END), 0)
        FROM regretted r
        JOIN memory_items m ON m.memory_id = r.memory_id
        JOIN memory_engagement me ON me.memory_id = r.memory_id
        WHERE m.is_deleted = FALSE
          AND DATE(me.last_accessed_at) = :day
        """,
        {"day": day},
    )
    regretted_seconds = int(regretted_seconds or 0)
    rate = (regretted_seconds / total_seconds * 100.0) if total_seconds else 0.0
    return {
        "day": day.isoformat(),
        "total_seconds": total_seconds,
        "regretted_seconds": regretted_seconds,
        "regret_rate_percent": round(rate, 1),
    }


def regret_by_hour() -> list[dict[str, Any]]:
    """Regret marks grouped by hour of day (pattern detection input)."""
    return postgresql_manager.fetchall(
        """
        SELECT regret_hour AS hour, COUNT(*) AS regret_count
        FROM regret_events
        WHERE regret_hour IS NOT NULL
        GROUP BY regret_hour
        ORDER BY regret_count DESC
        """
    )


def regret_by_category() -> list[dict[str, Any]]:
    """Regret marks and time by system group (architecture §11.6 query)."""
    return postgresql_manager.fetchall(
        """
        SELECT sg.group_name,
               COUNT(re.regret_id) AS regret_count,
               COALESCE(SUM(
                   CASE WHEN m.source_type = 'youtube' THEN me.watch_time_seconds
                        ELSE me.dwell_time_seconds END), 0) AS total_seconds
        FROM regret_events re
        JOIN memory_items m       ON re.memory_id = m.memory_id
        JOIN system_groups sg     ON m.system_group_id = sg.system_group_id
        JOIN memory_engagement me ON m.memory_id = me.memory_id
        GROUP BY sg.group_name
        ORDER BY total_seconds DESC
        """
    )


def regretted_items(limit: int = 50) -> list[dict[str, Any]]:
    """Currently regretted items with their latest note, for the analytics view."""
    return postgresql_manager.fetchall(
        _currently_regretted_cte()
        + """
        SELECT m.memory_id, m.source_type, m.title, m.created_at,
               (SELECT re.regret_note FROM regret_events re
                WHERE re.memory_id = m.memory_id AND re.regret_note IS NOT NULL
                ORDER BY re.marked_at DESC LIMIT 1) AS latest_note,
               (SELECT MAX(re.marked_at) FROM regret_events re
                WHERE re.memory_id = m.memory_id) AS last_marked_at
        FROM regretted r
        JOIN memory_items m ON m.memory_id = r.memory_id
        WHERE m.is_deleted = FALSE
        ORDER BY last_marked_at DESC
        LIMIT :limit
        """,
        {"limit": limit},
    )


# ── Reminders (rate-limited, never blocking, never judgmental) ───────────────

def set_reminders_enabled(enabled: bool) -> bool:
    """User control: enable/disable regret reminders entirely."""
    client = _redis()
    if client is None:
        return False
    if enabled:
        client.delete(_DISABLED_KEY)
    else:
        client.set(_DISABLED_KEY, "1")
    return True


def reminders_enabled() -> bool:
    client = _redis()
    if client is None:
        return False
    return not client.exists(_DISABLED_KEY)


def maybe_get_reminder(source_type: str, current_session_minutes: int) -> Optional[dict[str, Any]]:
    """
    Return a reminder payload when ALL conditions hold, else None:
      1. reminders not disabled by the user,
      2. current session >= 15 minutes,
      3. a habitual regret pattern exists for this content type at this hour,
      4. cooldown passed, max 1 per content type per day, max 2 total per day.

    The reminder content is the user's own prior note — Echo adds no opinion.
    """
    client = _redis()
    if client is None or client.exists(_DISABLED_KEY):
        return None
    if current_session_minutes < MIN_SESSION_MINUTES_FOR_REMINDER:
        return None

    from wba.pattern_detector import has_habitual_regret_pattern

    if not has_habitual_regret_pattern(source_type, datetime.now().hour):
        return None

    today = date.today().isoformat()
    cooldown_key = "echo:regret:reminder:cooldown"
    type_key = f"echo:regret:reminder:type:{source_type}:{today}"
    total_key = f"echo:regret:reminder:total:{today}"

    if client.exists(cooldown_key):
        return None
    if int(client.get(type_key) or 0) >= MAX_REMINDERS_PER_TYPE_PER_DAY:
        return None
    if int(client.get(total_key) or 0) >= MAX_REMINDERS_TOTAL_PER_DAY:
        return None

    note = latest_note_for_type(source_type)
    if not note:
        return None  # nothing of the user's own to show — never generate content

    # Record consumption of one reminder slot.
    pipe = client.pipeline()
    pipe.incr(type_key)
    pipe.expire(type_key, _DAY_TTL_SECONDS)
    pipe.incr(total_key)
    pipe.expire(total_key, _DAY_TTL_SECONDS)
    pipe.setex(cooldown_key, REMINDER_COOLDOWN_SECONDS, "1")
    pipe.execute()

    return {
        "source_type": source_type,
        "your_note": note,
        "options": ["Got it", "Snooze for today", "Disable permanently"],
    }
