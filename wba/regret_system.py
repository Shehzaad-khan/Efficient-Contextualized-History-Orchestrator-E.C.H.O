"""
regret_system.py — WBA user-declared regret (architecture §11.6, DB Table 8).

What regret is: the USER's own reflection — "I spent time on this and wish I
hadn't." Echo never decides what is regretful; it only records and reflects.

Storage design (locked): each mark AND unmark is a NEW row in regret_events.
Rows are never updated or deleted. Current status = odd row count for the
memory_id. Per item, rows alternate mark, unmark, mark, ... so the 1st, 3rd,
5th row (in time order) are MARK events and the even ones are UNMARK events.
regret_hour / regret_day_of_week are precomputed at insert time for fast
pattern queries.

Time zone: marked_at, regret_hour and regret_day_of_week are the user's LOCAL
time (datetime.now()); every other timestamp in the DB is naive UTC. Readers of
marked_at compare it with local days directly (tracker C22).

Analytics count only regrets that still stand: mark events of items that are
currently regretted and not deleted. An unmark (the user changed their mind)
never counts as a regret, and an item's time is counted once, not once per event.

Reminders (optional, rate-limited): fire only when a similar regret pattern
exists AND the current session is >= 15 minutes AND rate limits allow AND the
user hasn't disabled them. Content is always the user's OWN prior note.
Rate-limit state lives in Redis (ephemeral by design; the disable flag is a
persistent Redis key because the schema deliberately has no settings table).
A down Redis turns reminders off; it never raises to the caller.

Threading: all functions are blocking (pooled Postgres + the shared sync Redis
client) and are called from sync FastAPI routes, i.e. the threadpool.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Optional

import redis

from ste import postgresql_manager
from ste.redis_manager import get_sync_client
from wba.pattern_detector import has_habitual_regret_pattern
from wba.time_aggregator import TIME_UNION_SQL, local_day_range_utc, total_time

logger = logging.getLogger(__name__)

MIN_SESSION_MINUTES_FOR_REMINDER = 15
MAX_REMINDERS_PER_TYPE_PER_DAY = 1
MAX_REMINDERS_TOTAL_PER_DAY = 2
REMINDER_COOLDOWN_SECONDS = 2 * 3600
_DAY_TTL_SECONDS = 24 * 3600

_REMINDER_SOURCE_TYPES = frozenset({"gmail", "chrome", "youtube"})
_DISABLED_KEY = "echo:regret:reminders:disabled"
_COOLDOWN_KEY = "echo:regret:reminder:cooldown"

# Items whose regret still stands: odd event count, not deleted.
CURRENTLY_REGRETTED_CTE = """
    regretted AS (
        SELECT re.memory_id
        FROM regret_events re
        JOIN memory_items m ON m.memory_id = re.memory_id
        WHERE m.is_deleted = FALSE
        GROUP BY re.memory_id
        HAVING COUNT(*) % 2 = 1
    )
"""

# Mark events only: the odd-numbered rows of each item in time order.
MARK_EVENTS_CTE = """
    mark_events AS (
        SELECT numbered.*
        FROM (
            SELECT re.*,
                   ROW_NUMBER() OVER (PARTITION BY re.memory_id
                                      ORDER BY re.marked_at, re.regret_id) AS event_number
            FROM regret_events re
        ) AS numbered
        WHERE numbered.event_number % 2 = 1
    )
"""


def _redis() -> Optional[redis.Redis]:
    """The shared sync Redis client, or None when it cannot be created.

    No PING here (it cost an extra round trip per call); a down Redis surfaces
    as a RedisError on the first command, which each caller handles.
    """
    try:
        return get_sync_client()
    except Exception as exc:  # missing/invalid REDIS_URL
        logger.warning("Regret reminders off: Redis client unavailable (%s)", type(exc).__name__)
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

    now = datetime.now()  # local time on purpose — see module docstring (C22)
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
    logger.info("Regret toggled: memory_id=%s regretted=%s has_note=%s", memory_id, state, bool(note))
    return {"memory_id": memory_id, "regretted": state, "marked_at": now.isoformat()}


def latest_note_for_type(source_type: str) -> Optional[str]:
    """Most recent note on a regret that still stands, for one content type.

    Reminder content source. Notes on deleted or un-regretted items are never
    shown back to the user.
    """
    row = postgresql_manager.fetchone(
        "WITH " + CURRENTLY_REGRETTED_CTE + """
        SELECT re.regret_note
        FROM regret_events re
        JOIN regretted r     ON r.memory_id = re.memory_id
        JOIN memory_items m  ON m.memory_id = re.memory_id
        WHERE m.source_type = :source_type
          AND re.regret_note IS NOT NULL
        ORDER BY re.marked_at DESC
        LIMIT 1
        """,
        {"source_type": source_type},
    )
    return row["regret_note"] if row else None


# ── Analytics (descriptive only) ──────────────────────────────────────────────

def regret_rate(day: date) -> dict[str, Any]:
    """
    regret_rate = time on currently-regretted items that (local) day /
                  total wellbeing time that day × 100.

    The regretted seconds come from the same rows as total_time
    (time_aggregator.TIME_UNION_SQL: Chrome/YouTube visits by the visit's day,
    Gmail by its last open), so they are part of total_seconds and the rate
    stays <= 100.
    """
    total_seconds = total_time(day, day)
    regretted_seconds = postgresql_manager.scalar(
        "WITH " + CURRENTLY_REGRETTED_CTE + """
        SELECT COALESCE(SUM(t.time_seconds), 0)
        FROM regretted r
        JOIN (""" + TIME_UNION_SQL + """) AS t ON t.memory_id = r.memory_id
        """,
        local_day_range_utc(day, day),
    )
    regretted_seconds = int(regretted_seconds or 0)
    rate = (regretted_seconds / total_seconds * 100.0) if total_seconds else 0.0
    logger.debug("regret_rate %s -> %ss of %ss", day, regretted_seconds, total_seconds)
    return {
        "day": day.isoformat(),
        "total_seconds": total_seconds,
        "regretted_seconds": regretted_seconds,
        "regret_rate_percent": round(rate, 1),
    }


def regret_by_hour() -> list[dict[str, Any]]:
    """Mark events of standing regrets, grouped by local hour of day (pattern input)."""
    return postgresql_manager.fetchall(
        "WITH " + CURRENTLY_REGRETTED_CTE + "," + MARK_EVENTS_CTE + """
        SELECT mk.regret_hour AS hour, COUNT(*) AS regret_count
        FROM mark_events mk
        JOIN regretted r ON r.memory_id = mk.memory_id
        WHERE mk.regret_hour IS NOT NULL
        GROUP BY mk.regret_hour
        ORDER BY regret_count DESC, hour
        """
    )


def regret_by_category() -> list[dict[str, Any]]:
    """Regretted items and their time by system group (architecture §11.6).

    regret_count = currently regretted items in the group; total_seconds counts
    each item's time once. (The doc's reference query joins every event row,
    which counts unmarks as regrets and multiplies an item's time by its number
    of toggles.)
    """
    return postgresql_manager.fetchall(
        "WITH " + CURRENTLY_REGRETTED_CTE + """
        SELECT sg.group_name,
               COUNT(*) AS regret_count,
               COALESCE(SUM(
                   CASE WHEN m.source_type = 'youtube' THEN me.watch_time_seconds
                        ELSE me.dwell_time_seconds END), 0) AS total_seconds
        FROM regretted r
        JOIN memory_items m       ON m.memory_id = r.memory_id
        JOIN system_groups sg     ON m.system_group_id = sg.system_group_id
        JOIN memory_engagement me ON me.memory_id = r.memory_id
        GROUP BY sg.group_name
        ORDER BY total_seconds DESC, sg.group_name
        """
    )


def regretted_items(limit: int = 50) -> list[dict[str, Any]]:
    """Currently regretted items with their latest note, for the analytics view."""
    return postgresql_manager.fetchall(
        "WITH " + CURRENTLY_REGRETTED_CTE + """
        SELECT m.memory_id, m.source_type, m.title, m.created_at,
               (SELECT re.regret_note FROM regret_events re
                WHERE re.memory_id = m.memory_id AND re.regret_note IS NOT NULL
                ORDER BY re.marked_at DESC LIMIT 1) AS latest_note,
               (SELECT MAX(re.marked_at) FROM regret_events re
                WHERE re.memory_id = m.memory_id) AS last_marked_at
        FROM regretted r
        JOIN memory_items m ON m.memory_id = r.memory_id
        ORDER BY last_marked_at DESC
        LIMIT :limit
        """,
        {"limit": limit},
    )


# ── Reminders (rate-limited, never blocking, never judgmental) ───────────────

def set_reminders_enabled(enabled: bool) -> bool:
    """User control: enable/disable regret reminders entirely.

    Returns False when Redis is unreachable (the route answers 503).
    """
    client = _redis()
    if client is None:
        return False
    try:
        if enabled:
            client.delete(_DISABLED_KEY)
        else:
            client.set(_DISABLED_KEY, "1")
    except redis.RedisError as exc:
        logger.warning("Regret reminder setting not saved: Redis error (%s)", type(exc).__name__)
        return False
    logger.info("Regret reminders %s", "enabled" if enabled else "disabled")
    return True


def reminders_enabled() -> bool:
    """True unless the user disabled reminders; False when Redis is unreachable."""
    client = _redis()
    if client is None:
        return False
    try:
        return not client.exists(_DISABLED_KEY)
    except redis.RedisError as exc:
        logger.warning("Regret reminder setting unreadable: Redis error (%s)", type(exc).__name__)
        return False


def maybe_get_reminder(source_type: str, current_session_minutes: int) -> Optional[dict[str, Any]]:
    """
    Return a reminder payload when ALL conditions hold, else None:
      1. reminders not disabled by the user,
      2. current session >= 15 minutes,
      3. a habitual regret pattern exists for this content type at this hour,
      4. cooldown passed, max 1 per content type per day, max 2 total per day.

    The reminder content is the user's own prior note — Echo adds no opinion.
    The slot is claimed with SET NX on the cooldown key, so two concurrent
    calls can never both return a reminder.
    """
    if source_type not in _REMINDER_SOURCE_TYPES:
        return None
    if current_session_minutes < MIN_SESSION_MINUTES_FOR_REMINDER:
        return None
    client = _redis()
    if client is None:
        return None

    today = date.today().isoformat()  # local day, same as regret_hour
    type_key = f"echo:regret:reminder:type:{source_type}:{today}"
    total_key = f"echo:regret:reminder:total:{today}"
    try:
        if client.exists(_DISABLED_KEY) or client.exists(_COOLDOWN_KEY):
            return None
        if int(client.get(type_key) or 0) >= MAX_REMINDERS_PER_TYPE_PER_DAY:
            return None
        if int(client.get(total_key) or 0) >= MAX_REMINDERS_TOTAL_PER_DAY:
            return None

        if not has_habitual_regret_pattern(source_type, datetime.now().hour):
            return None
        note = latest_note_for_type(source_type)
        if not note:
            return None  # nothing of the user's own to show — never generate content

        # Claim the slot atomically; a concurrent caller that got here too loses.
        if not client.set(_COOLDOWN_KEY, "1", nx=True, ex=REMINDER_COOLDOWN_SECONDS):
            return None
        pipe = client.pipeline()
        pipe.incr(type_key)
        pipe.expire(type_key, _DAY_TTL_SECONDS)
        pipe.incr(total_key)
        pipe.expire(total_key, _DAY_TTL_SECONDS)
        pipe.execute()
    except redis.RedisError as exc:
        logger.warning("Regret reminder skipped: Redis error (%s)", type(exc).__name__)
        return None

    logger.info("Regret reminder issued for source_type=%s", source_type)
    return {
        "source_type": source_type,
        "your_note": note,
        "options": ["Got it", "Snooze for today", "Disable permanently"],
    }
