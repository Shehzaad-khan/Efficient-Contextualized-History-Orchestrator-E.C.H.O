"""
resolve_time_anchor node — deterministic time-anchor resolution.

Answers queries like "OS material after the interview email":
parse_intent extracts time_anchor_query="interview email" and
time_relation="after"; this node finds the anchor item, reads its created_at,
and exposes it as T_anchor. postgres_keyword_search then injects
created_at > T_anchor (or <) as a hard SQL constraint, and merge_and_rrf
applies the same constraint to semantic-only hits.

Resolution strategy (all local, no LLM):
  1. Keyword lookup — FTS + ILIKE over title/subject, best ts_rank first,
     most recent first on ties (the user usually means the latest such item).
  2. Semantic fallback — if the keyword lookup finds nothing, embed the anchor
     phrase and take the closest FAISS hit.
"""
import logging
from typing import Any, Optional

import psycopg2.extras

from rse.config import ANCHOR_CANDIDATE_LIMIT
from rse.search_coordinator import get_connection
from rse.state import EchoState

logger = logging.getLogger(__name__)

_ANCHOR_SQL = """
SELECT
    m.memory_id,
    m.source_type,
    m.title,
    m.created_at,
    gm.subject,
    gm.thread_id,
    ts_rank(
        to_tsvector('english', COALESCE(m.title, '') || ' ' || COALESCE(gm.subject, '')),
        websearch_to_tsquery('english', %s)
    ) AS anchor_rank
FROM memory_items m
LEFT JOIN gmail_metadata gm ON m.memory_id = gm.memory_id
WHERE m.is_deleted = FALSE
  AND m.preprocessed = TRUE
  AND (
        to_tsvector('english', COALESCE(m.title, '') || ' ' || COALESCE(gm.subject, ''))
            @@ websearch_to_tsquery('english', %s)
        OR m.title ILIKE %s
        OR COALESCE(gm.subject, '') ILIKE %s
  )
ORDER BY anchor_rank DESC, m.created_at DESC
LIMIT %s
"""

_ANCHOR_BY_ID_SQL = """
SELECT m.memory_id, m.source_type, m.title, m.created_at, gm.subject, gm.thread_id
FROM memory_items m
LEFT JOIN gmail_metadata gm ON m.memory_id = gm.memory_id
WHERE m.is_deleted = FALSE
  AND m.memory_id::text = %s
"""


def _keyword_anchor_lookup(anchor_query: str) -> Optional[dict[str, Any]]:
    """FTS/ILIKE lookup for the anchor item. Returns the best row or None."""
    pattern = f"%{anchor_query}%"
    try:
        conn = get_connection()
        try:
            with conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        _ANCHOR_SQL,
                        (anchor_query, anchor_query, pattern, pattern, ANCHOR_CANDIDATE_LIMIT),
                    )
                    rows = cur.fetchall()
        finally:
            conn.close()
    except Exception as exc:
        logger.error("resolve_time_anchor: keyword lookup failed — %s", exc)
        return None

    if not rows:
        return None
    row = dict(rows[0])
    row["memory_id"] = str(row["memory_id"])
    return row


def _semantic_anchor_lookup(anchor_query: str) -> Optional[dict[str, Any]]:
    """Embed the anchor phrase and take the nearest FAISS item as fallback."""
    try:
        from enp.embedding_generator import generate_embedding
        from ste.faiss_manager import get_manager
        from rse.search_coordinator import _search_full_index

        manager = get_manager()
        vector = generate_embedding(anchor_query)
        hits = _search_full_index(manager, vector, k=1)
        if not hits:
            return None
        memory_id = hits[0][0]

        conn = get_connection()
        try:
            with conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(_ANCHOR_BY_ID_SQL, (memory_id,))
                    row = cur.fetchone()
        finally:
            conn.close()

        if not row:
            return None
        item = dict(row)
        item["memory_id"] = str(item["memory_id"])
        return item
    except Exception as exc:
        logger.error("resolve_time_anchor: semantic fallback failed — %s", exc)
        return None


def resolve_time_anchor(state: EchoState) -> dict:
    """
    Resolve the time-anchor item and expose T_anchor to the search branches.

    No-ops (returns null anchor fields) when the intent has no anchor query,
    so the node is safe to keep unconditionally in the graph's linear path.

    Args:
        state: EchoState carrying parsed_intent.

    Returns:
        Partial state dict with anchor_time (ISO string or None) and
        anchor_item (resolved row or None).
    """
    intent = state.get("parsed_intent", {})
    anchor_query = intent.get("time_anchor_query")
    relation = intent.get("time_relation")

    if not anchor_query or relation not in ("after", "before"):
        return {"anchor_time": None, "anchor_item": None}

    anchor = _keyword_anchor_lookup(anchor_query)
    if anchor is None:
        logger.info("resolve_time_anchor: keyword lookup empty, trying semantic fallback")
        anchor = _semantic_anchor_lookup(anchor_query)

    if anchor is None or anchor.get("created_at") is None:
        logger.warning(
            "resolve_time_anchor: could not resolve anchor %r — search runs unanchored",
            anchor_query,
        )
        return {"anchor_time": None, "anchor_item": None}

    anchor_time = anchor["created_at"].isoformat()
    logger.info(
        "resolve_time_anchor: %r → %s (%s, created_at=%s, relation=%s)",
        anchor_query,
        anchor["memory_id"],
        anchor.get("title"),
        anchor_time,
        relation,
    )
    return {"anchor_time": anchor_time, "anchor_item": anchor}
