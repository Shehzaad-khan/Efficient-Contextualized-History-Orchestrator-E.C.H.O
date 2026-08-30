"""
extend_neighborhood node — relational context for the synthesizer.

For the top NEIGHBOR_TOP_ITEMS (3) ranked results, fetch the items the user
experienced AROUND them, so the synthesizer sees the episode, not an isolated
snippet:

  Gmail   — other emails in the same thread_id (conversation context).
  Chrome / YouTube — items in the same recorded session (session_memory_map)
             plus anything created within ± NEIGHBOR_WINDOW_MINUTES, closest
             first (the 5-minute rule mirrors the WBA session definition).

Neighbors are attached to state.neighbor_items keyed by the parent memory_id
and appended to the synthesis payload. They do NOT affect ranking.
"""
import logging
from typing import Any

import psycopg2.extras

from rse.config import NEIGHBOR_TOP_ITEMS, NEIGHBOR_WINDOW_MINUTES, NEIGHBORS_PER_ITEM
from rse.search_coordinator import get_connection, normalise_row
from rse.state import EchoState

logger = logging.getLogger(__name__)

# Slimmer column set than search results — neighbors are context, not answers.
_NEIGHBOR_COLUMNS = """
    m.memory_id,
    m.source_type,
    m.title,
    m.created_at,
    LEFT(COALESCE(m.raw_text, ''), 300) AS raw_snippet,
    gm.sender,
    gm.subject,
    cm.url,
    cm.domain,
    ym.channel_name
"""

_NEIGHBOR_JOINS = """
FROM memory_items m
LEFT JOIN gmail_metadata   gm ON m.memory_id = gm.memory_id
LEFT JOIN chrome_metadata  cm ON m.memory_id = cm.memory_id
LEFT JOIN youtube_metadata ym ON m.memory_id = ym.memory_id
"""

_THREAD_NEIGHBORS_SQL = f"""
SELECT {_NEIGHBOR_COLUMNS}
{_NEIGHBOR_JOINS}
WHERE m.is_deleted = FALSE
  AND gm.thread_id = %s
  AND m.memory_id::text != %s
ORDER BY m.created_at ASC
LIMIT %s
"""

_SESSION_NEIGHBORS_SQL = f"""
SELECT {_NEIGHBOR_COLUMNS}
{_NEIGHBOR_JOINS}
JOIN session_memory_map peer ON peer.memory_id = m.memory_id
WHERE m.is_deleted = FALSE
  AND m.memory_id::text != %s
  AND peer.session_id IN (
        SELECT session_id FROM session_memory_map WHERE memory_id::text = %s
  )
ORDER BY m.created_at ASC
LIMIT %s
"""

_TIME_WINDOW_NEIGHBORS_SQL = f"""
SELECT {_NEIGHBOR_COLUMNS}
{_NEIGHBOR_JOINS}
WHERE m.is_deleted = FALSE
  AND m.memory_id::text != %s
  AND m.created_at BETWEEN %s::timestamp - INTERVAL '{NEIGHBOR_WINDOW_MINUTES} minutes'
                       AND %s::timestamp + INTERVAL '{NEIGHBOR_WINDOW_MINUTES} minutes'
ORDER BY ABS(EXTRACT(EPOCH FROM (m.created_at - %s::timestamp))) ASC
LIMIT %s
"""


def _fetch(cur, sql: str, params: tuple) -> list[dict[str, Any]]:
    """Execute one neighbor query and normalize the rows."""
    cur.execute(sql, params)
    return [normalise_row(row) for row in cur.fetchall()]


def _neighbors_for_item(cur, item: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Fetch neighbors for one ranked item according to its source type.
    Chrome/YouTube combine session membership with the ±5-minute window,
    deduplicated, closest-in-time semantics preserved by query order.
    """
    memory_id: str = item["memory_id"]
    source_type: str = item.get("source_type") or ""

    if source_type == "gmail":
        thread_id = item.get("thread_id")
        if not thread_id:
            return []
        return _fetch(cur, _THREAD_NEIGHBORS_SQL, (thread_id, memory_id, NEIGHBORS_PER_ITEM))

    # Chrome / YouTube — session first, temporal window as complement.
    neighbors: list[dict[str, Any]] = []
    seen: set[str] = {memory_id}

    for row in _fetch(cur, _SESSION_NEIGHBORS_SQL, (memory_id, memory_id, NEIGHBORS_PER_ITEM)):
        if row["memory_id"] not in seen:
            seen.add(row["memory_id"])
            neighbors.append(row)

    created_at = item.get("created_at")
    if created_at is not None and len(neighbors) < NEIGHBORS_PER_ITEM:
        anchor_iso = created_at.isoformat() if hasattr(created_at, "isoformat") else str(created_at)
        window_rows = _fetch(
            cur,
            _TIME_WINDOW_NEIGHBORS_SQL,
            (memory_id, anchor_iso, anchor_iso, anchor_iso, NEIGHBORS_PER_ITEM),
        )
        for row in window_rows:
            if row["memory_id"] not in seen:
                seen.add(row["memory_id"])
                neighbors.append(row)
            if len(neighbors) >= NEIGHBORS_PER_ITEM:
                break

    return neighbors[:NEIGHBORS_PER_ITEM]


def extend_neighborhood(state: EchoState) -> dict:
    """
    Attach relational context to the top-ranked results.

    Args:
        state: EchoState carrying ranked_results.

    Returns:
        Partial state dict with neighbor_items: {memory_id: [neighbor, ...]}.
        Empty dict on error — synthesis proceeds without neighborhood context.
    """
    top_items = state.get("ranked_results", [])[:NEIGHBOR_TOP_ITEMS]
    if not top_items:
        return {"neighbor_items": {}}

    neighbor_items: dict[str, list[dict[str, Any]]] = {}
    try:
        conn = get_connection()
        try:
            with conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    for item in top_items:
                        neighbors = _neighbors_for_item(cur, item)
                        if neighbors:
                            neighbor_items[item["memory_id"]] = neighbors
        finally:
            conn.close()
    except Exception as exc:
        logger.error("extend_neighborhood: database error — %s", exc)
        return {"neighbor_items": {}}

    logger.info(
        "extend_neighborhood: %d/%d top items received neighbors (total=%d)",
        len(neighbor_items),
        len(top_items),
        sum(len(v) for v in neighbor_items.values()),
    )
    return {"neighbor_items": neighbor_items}
