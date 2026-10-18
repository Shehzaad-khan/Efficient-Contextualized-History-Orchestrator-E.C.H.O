"""
extend_neighborhood node — relational context for the synthesizer.

For the top NEIGHBOR_TOP_ITEMS (3) ranked results, fetch the items the user
experienced AROUND them, so the synthesizer sees the episode, not an isolated
snippet:

  Gmail   — other emails in the same thread_id (conversation context),
            closest in time to the ranked email first.
  Chrome / YouTube — items in the same recorded WBA session
             (session_memory_map) first, then other Chrome/YouTube items created
             within ± NEIGHBOR_WINDOW_MINUTES; each group closest-in-time first
             (the 5-minute rule mirrors the WBA session definition). Gmail is
             left out of the time window: an email's created_at is when it
             ARRIVED, not when it was read, so arrival during browsing is not
             something the user "experienced around" the page.

Neighbors are attached to state.neighbor_items keyed by the parent memory_id
and appended to the synthesis payload (title, source and time only). They do
NOT affect ranking. One query per top item; a failing lookup costs only that
item's neighbors.
Logs carry ids and counts only.
"""
import logging
from typing import Any

import psycopg2.extras

from rse.config import NEIGHBOR_TOP_ITEMS, NEIGHBOR_WINDOW_MINUTES, NEIGHBORS_PER_ITEM
from rse.search_coordinator import get_connection, normalise_row
from rse.state import EchoState

logger = logging.getLogger(__name__)

# Slimmer column set than search results — neighbors are context, not answers.
# No body text: the synthesizer only shows a neighbor's title, source and time.
_NEIGHBOR_COLUMNS = """
    m.memory_id,
    m.source_type,
    m.title,
    m.created_at,
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

# memory_id is compared as uuid (not memory_id::text) so the primary-key index is used.
_THREAD_NEIGHBORS_SQL = f"""
SELECT {_NEIGHBOR_COLUMNS}
{_NEIGHBOR_JOINS}
WHERE m.is_deleted = FALSE
  AND gm.thread_id = %(thread_id)s
  AND m.memory_id <> %(memory_id)s::uuid
ORDER BY ABS(EXTRACT(EPOCH FROM (m.created_at - %(created_at)s::timestamp))) ASC
LIMIT %(limit)s
"""

# Session peers (priority 0) and time-window items (priority 1) in ONE round
# trip; Python removes items that appear in both.
_ACTIVITY_NEIGHBORS_SQL = f"""
SELECT * FROM (
    SELECT {_NEIGHBOR_COLUMNS}, 0 AS priority,
           ABS(EXTRACT(EPOCH FROM (m.created_at - %(created_at)s::timestamp))) AS distance
    {_NEIGHBOR_JOINS}
    JOIN session_memory_map peer ON peer.memory_id = m.memory_id
    WHERE m.is_deleted = FALSE
      AND m.memory_id <> %(memory_id)s::uuid
      AND peer.session_id IN (
            SELECT session_id FROM session_memory_map WHERE memory_id = %(memory_id)s::uuid
      )
    UNION ALL
    SELECT {_NEIGHBOR_COLUMNS}, 1 AS priority,
           ABS(EXTRACT(EPOCH FROM (m.created_at - %(created_at)s::timestamp))) AS distance
    {_NEIGHBOR_JOINS}
    WHERE m.is_deleted = FALSE
      AND m.memory_id <> %(memory_id)s::uuid
      AND m.source_type IN ('chrome', 'youtube')
      AND m.created_at BETWEEN %(created_at)s::timestamp - %(window)s * INTERVAL '1 minute'
                           AND %(created_at)s::timestamp + %(window)s * INTERVAL '1 minute'
) AS candidates
ORDER BY priority ASC, distance ASC
LIMIT %(fetch_limit)s
"""


def _neighbors_for_item(cur: Any, item: dict[str, Any]) -> list[dict[str, Any]]:
    """Fetch up to NEIGHBORS_PER_ITEM neighbors for one ranked item by source type."""
    memory_id: str = item["memory_id"]
    created_at = item.get("created_at")
    if created_at is None:
        return []
    params: dict[str, Any] = {"memory_id": memory_id, "created_at": created_at}

    if item.get("source_type") == "gmail":
        thread_id = item.get("thread_id")
        if not thread_id:
            return []
        cur.execute(_THREAD_NEIGHBORS_SQL, {**params, "thread_id": thread_id, "limit": NEIGHBORS_PER_ITEM})
        return [normalise_row(row) for row in cur.fetchall()]

    cur.execute(_ACTIVITY_NEIGHBORS_SQL, {
        **params,
        "window": NEIGHBOR_WINDOW_MINUTES,
        # Twice the need: a session peer can also appear in the time window.
        "fetch_limit": NEIGHBORS_PER_ITEM * 2,
    })
    neighbors: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in cur.fetchall():
        neighbor = normalise_row(row)
        if neighbor["memory_id"] in seen:
            continue
        seen.add(neighbor["memory_id"])
        neighbor.pop("priority", None)
        neighbor.pop("distance", None)
        neighbors.append(neighbor)
        if len(neighbors) == NEIGHBORS_PER_ITEM:
            break
    return neighbors


def extend_neighborhood(state: EchoState) -> dict[str, Any]:
    """
    Attach relational context to the top-ranked results.

    Args:
        state: EchoState carrying ranked_results.

    Returns:
        Partial state dict with neighbor_items: {memory_id: [neighbor, ...]}.
        Items whose lookup fails are skipped; empty dict if the database is
        unreachable — synthesis proceeds without neighborhood context.
    """
    top_items = state.get("ranked_results", [])[:NEIGHBOR_TOP_ITEMS]
    if not top_items:
        return {"neighbor_items": {}}

    neighbor_items: dict[str, list[dict[str, Any]]] = {}
    failed = 0
    try:
        conn = get_connection()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                for item in top_items:
                    try:
                        neighbors = _neighbors_for_item(cur, item)
                    except psycopg2.Error as exc:
                        # A failed statement aborts the transaction; roll back so
                        # the next item's query can still run.
                        conn.rollback()
                        failed += 1
                        logger.error("extend_neighborhood: lookup failed for %s — %s: %s",
                                     item.get("memory_id"), type(exc).__name__, exc)
                        continue
                    if neighbors:
                        neighbor_items[item["memory_id"]] = neighbors
            conn.rollback()  # read-only: end the transaction before returning the connection
        finally:
            conn.close()
    except Exception as exc:
        logger.error("extend_neighborhood: database error — %s: %s", type(exc).__name__, exc)
        return {"neighbor_items": neighbor_items}

    logger.info(
        "extend_neighborhood: %d/%d top items received neighbors (total=%d, failed=%d)",
        len(neighbor_items),
        len(top_items),
        sum(len(v) for v in neighbor_items.values()),
        failed,
    )
    return {"neighbor_items": neighbor_items}
