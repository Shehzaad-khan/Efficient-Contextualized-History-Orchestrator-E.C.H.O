"""
search_coordinator.py — RSE Module
Echo Personal Memory System

postgres_search node — fully implemented.
faiss_search node    — stub (returns empty list, wired up after ENP fully indexes).

postgres_search builds a dynamic SQL query from the ParsedIntent and runs it
against the live Neon database. Joins memory_items with all source metadata
tables and memory_engagement. Returns up to POSTGRES_SEARCH_LIMIT rows as
a list of dicts — ready for re-ranking.

Time filter handling:
    "1_day"  → created_at >= NOW() - INTERVAL '1 day'
    "7_days" → created_at >= NOW() - INTERVAL '7 days'
    "30_days"→ created_at >= NOW() - INTERVAL '30 days'
    "90_days"→ created_at >= NOW() - INTERVAL '90 days'
    ISO-8601 → created_at >= <parsed datetime>
    null     → no time filter applied
"""

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

from .config import DATABASE_URL, POSTGRES_SEARCH_LIMIT

load_dotenv()
logger = logging.getLogger(__name__)


# ── Time filter helpers ───────────────────────────────────────────────────────

_RELATIVE_INTERVALS = {
    "1_day": "1 day",
    "7_days": "7 days",
    "30_days": "30 days",
    "90_days": "90 days",
}


def _parse_time_filter(time_filter: Optional[str]):
    """
    Convert time_filter string to a SQL fragment and params tuple.

    Returns (sql_fragment: str, params: list) where sql_fragment uses %s
    placeholders and params contains the values to bind.
    Returns (None, []) when no filter should be applied.
    """
    if not time_filter:
        return None, []

    if time_filter in _RELATIVE_INTERVALS:
        interval = _RELATIVE_INTERVALS[time_filter]
        return f"mi.created_at >= NOW() - INTERVAL %s", [interval]

    # Try ISO-8601 absolute date
    try:
        dt = datetime.fromisoformat(time_filter.replace("Z", "+00:00"))
        dt_naive = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return "mi.created_at >= %s", [dt_naive]
    except ValueError:
        logger.warning(f"Unrecognised time_filter value: {time_filter!r} — skipping")
        return None, []


# ── Source filter helper ──────────────────────────────────────────────────────

def _source_clause(sources: List[str]):
    """
    Build a WHERE clause fragment for source_type filtering.
    Returns (sql_fragment, params) or (None, []) for 'all'.
    """
    if not sources or "all" in sources:
        return None, []

    # Validate values to prevent injection (though psycopg2 handles this)
    valid = {"gmail", "chrome", "youtube"}
    filtered = [s for s in sources if s in valid]
    if not filtered:
        return None, []

    placeholders = ", ".join(["%s"] * len(filtered))
    return f"mi.source_type IN ({placeholders})", filtered


# ── Keyword search helper ─────────────────────────────────────────────────────

def _keyword_clause(query_clean: str):
    """
    Build a WHERE clause fragment for keyword matching on title + raw_text.
    Uses Postgres ILIKE for case-insensitive substring search on each word.
    Returns (sql_fragment, params) or (None, []) for empty queries.
    """
    if not query_clean or not query_clean.strip():
        return None, []

    words = [w.strip() for w in query_clean.strip().split() if len(w.strip()) > 2]
    if not words:
        return None, []

    # OR across all words — at least one keyword must match
    conditions = []
    params = []
    for word in words[:6]:  # Cap at 6 keywords to keep query fast
        conditions.append("(mi.title ILIKE %s OR mi.raw_text ILIKE %s)")
        params.extend([f"%{word}%", f"%{word}%"])

    return f"({' OR '.join(conditions)})", params


# ── Main postgres_search function ─────────────────────────────────────────────

def run_postgres_search(
    parsed_intent: Dict[str, Any],
    limit: int = POSTGRES_SEARCH_LIMIT,
) -> List[Dict[str, Any]]:
    """
    Execute dynamic SQL search against Neon DB using parsed_intent.

    Joins:
      memory_items        — source_type, source_id, title, raw_text
      memory_engagement   — dwell_time, watch_time, completion_rate
      gmail_metadata      — sender, subject, gmail_labels (LEFT JOIN)
      chrome_metadata     — url, domain, scroll_depth (LEFT JOIN)
      youtube_metadata    — video_id, channel_name, duration_seconds (LEFT JOIN)

    Returns list of row dicts.
    """
    if parsed_intent.get("skip_postgres_filter"):
        logger.info("postgres_search: skip_postgres_filter=True, running full scan")
        sources = ["all"]
        query_clean = parsed_intent.get("query_clean", "")
        time_filter = None
    else:
        sources = parsed_intent.get("sources", ["all"])
        query_clean = parsed_intent.get("query_clean", "")
        time_filter = parsed_intent.get("time_filter")

    # Build WHERE clauses
    where_clauses = ["mi.is_deleted = FALSE"]
    params: List[Any] = []

    src_clause, src_params = _source_clause(sources)
    if src_clause:
        where_clauses.append(src_clause)
        params.extend(src_params)

    time_clause, time_params = _parse_time_filter(time_filter)
    if time_clause:
        where_clauses.append(time_clause)
        params.extend(time_params)

    kw_clause, kw_params = _keyword_clause(query_clean)
    if kw_clause:
        where_clauses.append(kw_clause)
        params.extend(kw_params)

    where_sql = " AND ".join(where_clauses)

    sql = f"""
        SELECT
            mi.memory_id,
            mi.source_type,
            mi.source_id,
            mi.title,
            mi.raw_text,
            mi.system_group_id,
            mi.classification_confidence,
            mi.classified_by,
            mi.preprocessed,
            mi.created_at,
            mi.last_updated_at,

            -- Engagement signals
            me.dwell_time_seconds,
            me.watch_time_seconds,
            me.completion_rate,
            me.play_sessions_count,
            me.first_opened_at,
            me.last_accessed_at,

            -- Gmail fields (NULL for non-gmail items)
            gm.sender,
            gm.subject         AS gmail_subject,
            gm.gmail_labels,
            gm.has_attachments,
            gm.thread_id,

            -- Chrome fields (NULL for non-chrome items)
            cm.url             AS chrome_url,
            cm.canonical_url,
            cm.domain,
            cm.scroll_depth,
            cm.revisit_count,

            -- YouTube fields (NULL for non-youtube items)
            ym.video_id,
            ym.channel_name,
            ym.duration_seconds,
            ym.is_short,
            ym.youtube_category_id

        FROM memory_items mi
        LEFT JOIN memory_engagement me  ON mi.memory_id = me.memory_id
        LEFT JOIN gmail_metadata   gm  ON mi.memory_id = gm.memory_id
        LEFT JOIN chrome_metadata  cm  ON mi.memory_id = cm.memory_id
        LEFT JOIN youtube_metadata ym  ON mi.memory_id = ym.memory_id

        WHERE {where_sql}

        ORDER BY mi.last_updated_at DESC NULLS LAST
        LIMIT %s
    """
    params.append(limit)

    logger.info(
        f"postgres_search: sources={sources} time_filter={time_filter} "
        f"query_clean='{query_clean}' limit={limit}"
    )

    db_url = DATABASE_URL or os.getenv("DATABASE_URL")
    if not db_url:
        logger.error("DATABASE_URL not configured")
        return []

    conn = None
    try:
        conn = psycopg2.connect(db_url)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        conn.commit()

        results = [dict(row) for row in rows]
        logger.info(f"postgres_search: returned {len(results)} rows")
        return results

    except Exception as e:
        logger.error(f"postgres_search DB error: {e}")
        if conn:
            conn.rollback()
        return []
    finally:
        if conn:
            conn.close()


# ── FAISS search stub ─────────────────────────────────────────────────────────

def run_faiss_search(
    query_clean: str,
    candidate_ids: List[str],
    full_scan: bool = False,
    k: int = 20,
) -> List[Dict[str, Any]]:
    """
    FAISS semantic search — STUB for Phase 2.

    Will be wired to FAISSManager.search() once ENP has fully indexed
    all existing items. The filtered-search pattern is: postgres_search
    returns candidate IDs, FAISS re-ranks them by semantic similarity.

    Returns:
        Empty list (stub). Real implementation returns list of dicts:
        [{"memory_id": "...", "distance": 0.42}, ...]
    """
    logger.debug(
        f"faiss_search STUB called — query='{query_clean}' "
        f"candidates={len(candidate_ids)} full_scan={full_scan}"
    )
    return []
