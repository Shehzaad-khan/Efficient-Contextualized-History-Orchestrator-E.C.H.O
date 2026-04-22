"""
postgres_search and faiss_search node implementations.

postgres_search: builds a dynamic SQL query from parsed_intent, executes it
against Neon PostgreSQL, and returns structured result dicts.

faiss_search: stub for this phase — returns empty list. Real implementation
requires Mir's FAISS manager and pre-built embeddings from the ENP.
"""
import logging
from typing import Any

import psycopg2
import psycopg2.extras

from rse.config import DATABASE_URL, POSTGRES_RESULT_LIMIT, FAISS_TOP_K
from rse.state import EchoState, ParsedIntent

logger = logging.getLogger(__name__)


# ── postgres_search ───────────────────────────────────────────────────────────

_BASE_SELECT = """
SELECT
    m.memory_id,
    m.source_type,
    m.title,
    m.created_at,
    m.auto_keywords,
    m.system_group_id,
    m.is_deleted,
    m.preprocessed,
    me.dwell_time_seconds,
    me.watch_time_seconds,
    me.last_accessed_at,
    me.first_opened_at,
    me.play_sessions_count,
    me.completion_rate,
    cm.url,
    cm.canonical_url,
    cm.domain,
    cm.scroll_depth,
    cm.revisit_count,
    cm.interaction_count,
    gm.sender,
    gm.subject,
    gm.has_attachments,
    gm.thread_id,
    gm.recipients,
    gm.gmail_labels,
    ym.channel_name,
    ym.video_id,
    ym.is_short,
    ym.duration_seconds,
    ym.youtube_category_id
FROM memory_items m
JOIN memory_engagement me  ON m.memory_id = me.memory_id
LEFT JOIN chrome_metadata  cm ON m.memory_id = cm.memory_id
LEFT JOIN gmail_metadata   gm ON m.memory_id = gm.memory_id
LEFT JOIN youtube_metadata ym ON m.memory_id = ym.memory_id
"""

_BASE_WHERE = """
WHERE m.is_deleted = FALSE
  AND m.preprocessed = TRUE
"""

_ORDER_LIMIT = """
ORDER BY me.last_accessed_at DESC NULLS LAST
LIMIT %s
"""


def _build_dynamic_filters(
    intent: ParsedIntent,
) -> tuple[list[str], list[Any]]:
    """
    Assemble dynamic WHERE clauses and their parameterised values from
    parsed_intent. Each filter is only added when the relevant field is set.

    Args:
        intent: Parsed intent dict from parse_intent node.

    Returns:
        Tuple of (list_of_sql_clauses, list_of_param_values).
    """
    clauses: list[str] = []
    params: list[Any] = []

    # Source filter — skip if sources=['all'] or contains all three
    sources = intent.get("sources", ["gmail", "chrome", "youtube"])
    all_sources = {"gmail", "chrome", "youtube"}
    if sources and set(sources) != all_sources and "all" not in sources:
        placeholders = ", ".join(["%s"] * len(sources))
        clauses.append(f"m.source_type IN ({placeholders})")
        params.extend(sources)

    # Time filter — only added when a date string is present
    time_filter = intent.get("time_filter")
    if time_filter:
        clauses.append("m.created_at >= %s::timestamptz")
        params.append(time_filter)

    # Keyword filter — search title and raw keywords array
    query_clean = intent.get("query_clean", "").strip()
    if query_clean:
        # Use ILIKE against title; also check if any keyword matches
        clauses.append(
            "(m.title ILIKE %s OR EXISTS ("
            "  SELECT 1 FROM unnest(m.auto_keywords) kw WHERE kw ILIKE %s"
            "))"
        )
        pattern = f"%{query_clean}%"
        params.extend([pattern, pattern])

    return clauses, params


def postgres_search(state: EchoState) -> dict:
    """
    Execute a parameterised SQL query against Neon PostgreSQL and return
    candidate memory item rows.

    Dynamically adds source, time, and keyword filters from parsed_intent.
    When skip_postgres_filter is True, runs with base conditions only so the
    widen_scope Attempt 3 path can retrieve all preprocessed items.

    Args:
        state: Current EchoState. Reads parsed_intent.

    Returns:
        Partial state dict with updated postgres_results key (list of dicts).
        Returns empty list on any database error.
    """
    intent: ParsedIntent = state.get("parsed_intent", {})
    skip_filter: bool = intent.get("skip_postgres_filter", False)

    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False

        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                if skip_filter:
                    sql = _BASE_SELECT + _BASE_WHERE + _ORDER_LIMIT
                    params: list[Any] = [POSTGRES_RESULT_LIMIT]
                    logger.info("postgres_search: running base-only query (skip_postgres_filter=True)")
                else:
                    dynamic_clauses, dynamic_params = _build_dynamic_filters(intent)
                    where_block = _BASE_WHERE
                    if dynamic_clauses:
                        where_block += "  AND " + "\n  AND ".join(dynamic_clauses)
                    sql = _BASE_SELECT + where_block + _ORDER_LIMIT
                    params = dynamic_params + [POSTGRES_RESULT_LIMIT]
                    logger.info(
                        "postgres_search: query with %d dynamic filters, sources=%s",
                        len(dynamic_clauses),
                        intent.get("sources"),
                    )

                cur.execute(sql, params)
                rows = cur.fetchall()

        conn.close()

        results = [dict(row) for row in rows]
        logger.info("postgres_search: returned %d rows", len(results))
        return {"postgres_results": results}

    except Exception as exc:
        logger.error("postgres_search: database error — %s", exc)
        return {"postgres_results": []}


# ── faiss_search ──────────────────────────────────────────────────────────────

def faiss_search(state: EchoState) -> dict:
    """
    Semantic similarity search over FAISS for the top-K most relevant items
    among the PostgreSQL candidate set.

    STUB for this phase: returns empty faiss_results. Real implementation
    requires Mir's FAISS manager (faiss_manager.search) and pre-built
    embeddings from the Enrichment Pipeline.

    Interface contract with Mir's FAISS manager (do not change signature):
        faiss_manager.search(query_vector, candidate_ids, k=20)
        Returns: list[tuple[str, float]]  — [(memory_id, cosine_score), ...]

    Args:
        state: Current EchoState. Reads parsed_intent and postgres_results.

    Returns:
        Partial state dict with updated faiss_results key (empty list this phase).
    """
    logger.info("faiss_search: stub — returning empty results (FAISS not yet integrated)")
    return {"faiss_results": []}
