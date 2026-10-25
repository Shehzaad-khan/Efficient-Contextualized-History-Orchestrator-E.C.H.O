"""
Hybrid retrieval branches — postgres_keyword_search and faiss_semantic_search.

The two branches run in PARALLEL (LangGraph fan-out from resolve_time_anchor)
and are fused downstream by merge_and_rrf:

  postgres_keyword_search — PostgreSQL Full-Text Search (websearch_to_tsquery)
      over title + raw_text + auto_keywords + subject + sender + domain +
      channel, with an ILIKE safety net for partial-token matches. Ordered by
      ts_rank so the branch rank reflects textual relevance, not recency.
      Applies deterministic source / time / time-anchor SQL constraints from
      the parsed intent.

  faiss_semantic_search — embeds every query variant locally with
      sentence-transformers on the QUERY path (the model's query prompt;
      stored items use the document path), searches the FULL FAISS index per
      variant (FAISSManager.search_all, which holds the manager's lock while
      reading), deduplicates keeping the best cosine per memory_id. The vectors are
      unit-length, so the index's L2 order equals cosine order; search_all
      still reports true cosine.

hydrate_memory_items — fetches full metadata rows for memory_ids that only
the semantic branch found, so the fused pool is uniformly shaped.
fetch_recent_items — today's newest items for the fetch_api node.

get_connection — the RSE-wide psycopg2 pool (also used by time_anchor,
neighborhood and conversation_memory).

Logs carry counts, flags and timings only — never query text or item content.
"""
import logging
import threading
import time
from datetime import date, datetime, timezone
from typing import Any, Optional, Sequence

import psycopg2
import psycopg2.extras
import psycopg2.pool

from rse.config import (
    DATABASE_URL,
    FAISS_TOP_K_PER_VARIANT,
    FULL_SCAN_K_MULTIPLIER,
    PG_KEYWORD_LIMIT,
)
from rse.state import EchoState, ParsedIntent

logger = logging.getLogger(__name__)

_ALL_SOURCES: tuple[str, ...] = ("gmail", "chrome", "youtube")


# ── Shared SQL fragments ──────────────────────────────────────────────────────

# raw_snippet is a plain leading excerpt used by the cross-encoder and the
# synthesizer. Full raw_text never leaves PostgreSQL through this module.
RESULT_COLUMNS = """
    m.memory_id,
    m.source_type,
    m.title,
    m.created_at,
    m.auto_keywords,
    m.system_group_id,
    LEFT(COALESCE(m.raw_text, ''), 1000) AS raw_snippet,
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
    ym.channel_name,
    ym.video_id,
    ym.is_short,
    ym.duration_seconds,
    (SELECT MAX(v.visited_at) FROM visits v WHERE v.memory_id = m.memory_id) AS last_visit_at
"""

RESULT_JOINS = """
FROM memory_items m
JOIN memory_engagement me  ON m.memory_id = me.memory_id
LEFT JOIN chrome_metadata  cm ON m.memory_id = cm.memory_id
LEFT JOIN gmail_metadata   gm ON m.memory_id = gm.memory_id
LEFT JOIN youtube_metadata ym ON m.memory_id = ym.memory_id
"""

BASE_CONDITIONS = """
WHERE m.is_deleted = FALSE
  AND m.preprocessed = TRUE
"""

# Searchable document for FTS: the stored vector memory_items.search_vector (A24.3),
# written by ENP when it marks an item processed. It holds title, the FULL raw_text,
# keywords, subject, sender (so "emails from Neon" matches by WHO sent it), domain,
# channel and attachment text, and a GIN index serves the @@ lookup.
_FTS_DOCUMENT = "m.search_vector"


# ── Connection pool ───────────────────────────────────────────────────────────

_POOL_MIN = 1
_POOL_MAX = 10
# A connection idle longer than this is pinged before use. Neon closes
# connections when its compute suspends (after ~5 idle minutes), and a dead
# pooled connection would otherwise fail the first query after a pause.
_IDLE_PING_SECONDS = 60.0
# Stored timestamps are naive UTC and the local server runs in Asia/Calcutta, so
# each pooled connection starts in UTC through the libpq startup option (A31.4).
_SESSION_TIMEZONE_OPTION = "-c timezone=UTC"

_POOL: Optional[psycopg2.pool.ThreadedConnectionPool] = None
_POOL_LOCK = threading.Lock()
# id(raw connection) → time.monotonic() when it went back to the pool.
_returned_at: dict[int, float] = {}
_returned_at_lock = threading.Lock()


class _PooledConnection:
    """Delegates to a pooled connection, but returns it to the pool on close().

    Every retrieval call site follows `conn = get_connection() ... finally:
    conn.close()`. Handing those sites a raw pooled connection would close it
    permanently, so this proxy intercepts close() and leaves everything else --
    including `with conn:` transaction handling -- to the real connection.
    """

    def __init__(self, pool: psycopg2.pool.ThreadedConnectionPool, conn: Any) -> None:
        self._pool = pool
        self._conn = conn

    def close(self) -> None:
        conn, self._conn = self._conn, None
        if conn is None:
            return
        broken = bool(conn.closed)
        with _returned_at_lock:
            if broken:
                _returned_at.pop(id(conn), None)
            else:
                _returned_at[id(conn)] = time.monotonic()
        # A connection broken mid-query must not go back into rotation.
        self._pool.putconn(conn, close=broken)

    def __enter__(self) -> Any:
        return self._conn.__enter__()

    def __exit__(self, *exc_info: Any) -> Any:
        return self._conn.__exit__(*exc_info)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    """Create the pool once (double-checked lock: parallel branches may race here)."""
    global _POOL
    if _POOL is None:
        with _POOL_LOCK:
            if _POOL is None:
                if not DATABASE_URL:
                    raise RuntimeError("DATABASE_URL is not configured for the RSE module.")
                _POOL = psycopg2.pool.ThreadedConnectionPool(
                    _POOL_MIN,
                    _POOL_MAX,
                    DATABASE_URL,
                    # Extra keywords reach psycopg2.connect; every session starts in UTC (A31.4).
                    options=_SESSION_TIMEZONE_OPTION,
                )
                logger.info("RSE connection pool created (min=%d, max=%d)", _POOL_MIN, _POOL_MAX)
    return _POOL


def _is_alive(conn: Any) -> bool:
    """One cheap round trip; leaves the connection outside any transaction."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        conn.rollback()
        return True
    except psycopg2.Error:
        return False


def get_connection() -> _PooledConnection:
    """Lease a pooled psycopg2 connection to the configured database.

    Neon runs in us-east-1, where a fresh TCP+TLS+auth handshake measured
    ~700ms. The retrieval path opens connections from several call sites and
    the widen loop repeats them, so connecting per call dominated query latency.

    A connection that sat idle longer than _IDLE_PING_SECONDS is checked with
    SELECT 1 first; dead ones are discarded and the next one is tried, so the
    first query after a Neon suspend still succeeds.

    Raises:
        RuntimeError: DATABASE_URL is not set.
        psycopg2.pool.PoolError: all _POOL_MAX connections are in use.
    """
    pool = _get_pool()
    for _ in range(_POOL_MAX + 1):
        conn = pool.getconn()
        with _returned_at_lock:
            returned_at = _returned_at.get(id(conn))
        idle = returned_at is not None and time.monotonic() - returned_at > _IDLE_PING_SECONDS
        if not conn.closed and (not idle or _is_alive(conn)):
            return _PooledConnection(pool, conn)
        logger.info("get_connection: discarding a dead pooled connection")
        with _returned_at_lock:
            _returned_at.pop(id(conn), None)
        pool.putconn(conn, close=True)
    raise psycopg2.OperationalError("no live database connection after retries")


def normalise_row(row: dict[str, Any]) -> dict[str, Any]:
    """Convert DB-native types to plain Python so rows merge cleanly with FAISS ids."""
    item = dict(row)
    item["memory_id"] = str(item["memory_id"])
    return item


def escape_like(text: str) -> str:
    r"""Escape ILIKE wildcards so '50%' or 'file_v2' match literally (backslash is ILIKE's default escape)."""
    return text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def local_to_utc_naive(value: str) -> datetime:
    """
    Convert a parser date/datetime (the user's LOCAL time) to naive UTC.

    memory_items.created_at is stored as naive UTC (ste/storage_engine.py), but
    "2026-10-18" from the parser means local midnight — in IST that is
    2026-10-17 18:30 UTC. Comparing the raw date with created_at would drop
    everything from local midnight to 05:30. A value that already carries a
    timezone is converted as-is. The backend runs on the user's machine, so
    the process's local timezone is the user's.

    Raises:
        ValueError: the value is not an ISO date/datetime.
    """
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.astimezone()  # naive → interpreted as local time
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def filter_time_of(item: dict[str, Any]) -> Optional[datetime]:
    """The moment an item is compared with the time filter (start-only, A1.1/A1.2).

    Gmail: when the mail arrived (created_at). Chrome and YouTube: their latest
    visit, because "any visit on or after the start" is the same as "the latest
    visit is on or after the start". An item with no visit row falls back to
    created_at. Mirrors the clause in _dynamic_filters (EXISTS visit >= start, or no
    visit at all and created_at >= start).
    """
    if item.get("source_type") != "gmail":
        last_visit = item.get("last_visit_at")
        if isinstance(last_visit, datetime):
            return last_visit
    created_at = item.get("created_at")
    return created_at if isinstance(created_at, datetime) else None


def _anchor_constraint(state: EchoState) -> tuple[list[str], list[Any]]:
    """
    Deterministic time-anchor constraint resolved by resolve_time_anchor.
    Injected as a hard SQL condition: created_at > / < T_anchor.
    """
    anchor_time = state.get("anchor_time")
    relation = (state.get("parsed_intent") or {}).get("time_relation")
    if not anchor_time or relation not in ("after", "before"):
        return [], []
    operator = ">" if relation == "after" else "<"
    return [f"m.created_at {operator} %s::timestamp"], [anchor_time]


def _dynamic_filters(intent: ParsedIntent) -> tuple[list[str], list[Any]]:
    """Source and absolute-time WHERE clauses from parsed intent."""
    clauses: list[str] = []
    params: list[Any] = []

    sources = intent.get("sources", list(_ALL_SOURCES))
    if sources and set(sources) != set(_ALL_SOURCES) and "all" not in sources:
        placeholders = ", ".join(["%s"] * len(sources))
        clauses.append(f"m.source_type IN ({placeholders})")
        params.extend(sources)

    time_filter = intent.get("time_filter")
    if time_filter:
        try:
            start_utc = local_to_utc_naive(time_filter)
        except ValueError:
            logger.warning("postgres_keyword_search: invalid time_filter ignored (len=%d)", len(str(time_filter)))
        else:
            # A1.1: Gmail is filtered by when the mail arrived; Chrome and YouTube
            # match when ANY of their visits falls on or after the start. A page or
            # video with no visit row at all (captured while the visit writers did not
            # exist) falls back to created_at, the same rule as filter_time_of.
            clauses.append(
                "((m.source_type = 'gmail' AND m.created_at >= %s)"
                " OR (m.source_type <> 'gmail' AND ("
                "EXISTS (SELECT 1 FROM visits v WHERE v.memory_id = m.memory_id AND v.visited_at >= %s)"
                " OR (NOT EXISTS (SELECT 1 FROM visits v WHERE v.memory_id = m.memory_id) AND m.created_at >= %s))))"
            )
            params.extend([start_utc, start_utc, start_utc])

    return clauses, params


# ── postgres_keyword_search ───────────────────────────────────────────────────

def postgres_keyword_search(state: EchoState) -> dict[str, Any]:
    """
    Keyword branch of the hybrid retrieval (Node 2a).

    Runs PostgreSQL FTS over the stored search_vector (title, full raw_text,
    auto_keywords, subject, sender, domain, channel, attachment text) with an
    ILIKE fallback on the short fields in the same query, ranked by ts_rank.
    Applies deterministic source, absolute-time, and time-anchor constraints —
    except on widen attempt 3 (skip_postgres_filter), when the same keyword
    match runs without them. Result order defines Rank_postgres for RRF.

    With no usable keyword the branch returns nothing (the semantic branch
    still runs). It never falls back to a "most recent items" list: those would
    enter RRF ranked by recency, which is recency boosting (a prohibited feature).

    Args:
        state: EchoState carrying parsed_intent and anchor_time.

    Returns:
        Partial state dict with postgres_results (list of normalized dicts,
        FTS-rank ordered). Empty list on any database error.
    """
    intent: ParsedIntent = state.get("parsed_intent", {})
    skip_filter: bool = bool(intent.get("skip_postgres_filter", False))
    query_clean: str = (intent.get("query_clean") or "").strip()

    if not query_clean:
        logger.info("postgres_keyword_search: no keyword — branch skipped")
        return {"postgres_results": []}

    if skip_filter:
        clauses: list[str] = []
        filter_params: list[Any] = []
        has_anchor = False
    else:
        clauses, filter_params = _dynamic_filters(intent)
        anchor_clauses, anchor_params = _anchor_constraint(state)
        clauses += anchor_clauses
        filter_params += anchor_params
        has_anchor = bool(anchor_clauses)

    where_block = BASE_CONDITIONS
    if clauses:
        where_block += "  AND " + "\n  AND ".join(clauses) + "\n"

    sql = f"""
    SELECT {RESULT_COLUMNS},
           ts_rank({_FTS_DOCUMENT}, websearch_to_tsquery('english', %s)) AS keyword_rank
    {RESULT_JOINS}
    {where_block}
      AND (
            {_FTS_DOCUMENT} @@ websearch_to_tsquery('english', %s)
            OR m.title ILIKE %s
            OR COALESCE(gm.subject, '') ILIKE %s
            OR COALESCE(gm.sender, '') ILIKE %s
            OR COALESCE(cm.domain, '') ILIKE %s
            OR COALESCE(ym.channel_name, '') ILIKE %s
            OR EXISTS (SELECT 1 FROM unnest(m.auto_keywords) kw WHERE kw ILIKE %s)
      )
    ORDER BY keyword_rank DESC, me.last_accessed_at DESC NULLS LAST
    LIMIT %s
    """
    ilike_pattern = f"%{escape_like(query_clean)}%"
    # Placeholder order: SELECT's ts_rank, then WHERE filters, then the match block.
    params = [query_clean] + filter_params + [query_clean] + [ilike_pattern] * 6 + [PG_KEYWORD_LIMIT]

    started = time.perf_counter()
    try:
        conn = get_connection()
        try:
            with conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(sql, params)
                    rows = cur.fetchall()
        finally:
            conn.close()
    except Exception as exc:
        logger.error("postgres_keyword_search: database error — %s: %s", type(exc).__name__, exc)
        return {"postgres_results": []}

    results = [normalise_row(row) for row in rows]
    logger.info(
        "postgres_keyword_search: %d rows in %.0f ms (filters=%d, anchor=%s, skip_filter=%s, sources=%s)",
        len(results), (time.perf_counter() - started) * 1000, len(clauses), has_anchor, skip_filter,
        intent.get("sources"),
    )
    return {"postgres_results": results}


# ── faiss_semantic_search ─────────────────────────────────────────────────────

def faiss_semantic_search(state: EchoState) -> dict[str, Any]:
    """
    Semantic branch of the hybrid retrieval (Node 2b) with query expansion.

    Embeds every query variant locally (sentence-transformers, zero API calls),
    searches the full FAISS index once per variant, merges the hits keeping the
    best cosine similarity per memory_id, and returns them best-first. Result
    order defines Rank_faiss for Reciprocal Rank Fusion.

    Args:
        state: EchoState carrying parsed_intent (query_variants).

    Returns:
        Partial state dict with faiss_results: [(memory_id, cosine), ...].
        Empty list when the index is unavailable — the keyword branch still runs.
    """
    intent: ParsedIntent = state.get("parsed_intent", {})
    variants: list[str] = [v for v in intent.get("query_variants", []) if v and v.strip()]
    if not variants:
        fallback = (intent.get("query_clean") or "").strip()
        variants = [fallback] if fallback else []
    if not variants:
        logger.warning("faiss_semantic_search: no query variants available")
        return {"faiss_results": []}

    k = FAISS_TOP_K_PER_VARIANT
    if intent.get("full_faiss_scan"):
        k *= FULL_SCAN_K_MULTIPLIER

    started = time.perf_counter()
    try:
        from enp.embedding_generator import generate_query_embeddings
        from ste.faiss_manager import get_manager

        manager = get_manager()
        embeddings = generate_query_embeddings(variants)

        best_scores: dict[str, float] = {}
        hits_per_variant: list[int] = []
        for vector in embeddings:
            hits = manager.search_all(vector, k)
            hits_per_variant.append(len(hits))
            for memory_id, similarity in hits:
                if similarity > best_scores.get(memory_id, float("-inf")):
                    best_scores[memory_id] = similarity
    except Exception as exc:
        logger.error("faiss_semantic_search: search failed — %s: %s", type(exc).__name__, exc)
        return {"faiss_results": []}

    merged = sorted(best_scores.items(), key=lambda pair: pair[1], reverse=True)
    logger.info(
        "faiss_semantic_search: %d variants (hits %s, k=%d) pooled into %d candidates in %.0f ms",
        len(variants), hits_per_variant, k, len(merged), (time.perf_counter() - started) * 1000,
    )
    return {"faiss_results": merged}


# ── Hydration ─────────────────────────────────────────────────────────────────

def hydrate_memory_items(memory_ids: Sequence[str]) -> list[dict[str, Any]]:
    """
    Fetch full metadata rows for the given memory_ids (semantic-branch hits
    the keyword branch didn't return). Order of the result is not guaranteed;
    deleted or unprocessed items are left out.

    Args:
        memory_ids: memory_id strings (UUIDs) to hydrate.

    Returns:
        List of normalized result dicts; empty on error or empty input.
    """
    ids = [str(mid) for mid in memory_ids]
    if not ids:
        return []

    # Compare as uuid[] (not memory_id::text) so the primary-key index is used.
    sql = f"SELECT {RESULT_COLUMNS} {RESULT_JOINS} {BASE_CONDITIONS}  AND m.memory_id = ANY(%s::uuid[])"
    try:
        conn = get_connection()
        try:
            with conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(sql, (ids,))
                    rows = cur.fetchall()
        finally:
            conn.close()
    except Exception as exc:
        logger.error("hydrate_memory_items: database error — %s: %s", type(exc).__name__, exc)
        return []
    logger.info("hydrate_memory_items: %d of %d ids hydrated", len(rows), len(ids))
    return [normalise_row(row) for row in rows]


def fetch_visit_times(memory_ids: Sequence[str], since_utc: datetime) -> dict[str, list[datetime]]:
    """
    Visit times (naive UTC, oldest first) on or after `since_utc` for the given
    Chrome/YouTube items, so the answer can show which visits fall inside the asked
    period (A1.2). Items with no visit in range are left out; Gmail has no visits.

    Args:
        memory_ids: memory_id strings (UUIDs).
        since_utc: the time filter as naive UTC.

    Returns:
        {memory_id: [visited_at, ...]}; empty on error or empty input.
    """
    ids = [str(mid) for mid in memory_ids]
    if not ids:
        return {}
    sql = (
        "SELECT memory_id::text AS memory_id, visited_at FROM visits "
        "WHERE memory_id = ANY(%s::uuid[]) AND visited_at >= %s "
        "ORDER BY memory_id, visited_at"
    )
    try:
        conn = get_connection()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (ids, since_utc))
                    rows = cur.fetchall()
        finally:
            conn.close()
    except Exception as exc:
        logger.error("fetch_visit_times: database error — %s: %s", type(exc).__name__, exc)
        return {}
    visits: dict[str, list[datetime]] = {}
    for memory_id, visited_at in rows:
        visits.setdefault(memory_id, []).append(visited_at)
    return visits


def fetch_recent_items(sources: Sequence[str], limit: int = 10) -> list[dict[str, Any]]:
    """
    Freshest items from today for the fetch_api node (architecture §10.3):
    after a live source poll, surface what just arrived so the synthesizer
    can answer "did I get any emails from Google today" from current data.

    Args:
        sources: source_type values to include (e.g. ['gmail']).
        limit: maximum rows returned.

    Returns:
        Normalized result dicts, newest first; empty on error.
    """
    wanted = [s for s in sources if s in _ALL_SOURCES] or list(_ALL_SOURCES)
    # "Today" = since the user's local midnight, as naive UTC (created_at is UTC;
    # the database's CURRENT_DATE would be the UTC day instead).
    today_start_utc = local_to_utc_naive(date.today().isoformat())
    # Each fragment ends with a newline: "CURRENT_DATE" + "ORDER BY" used to
    # concatenate into "CURRENT_DATEORDER BY" (a syntax error).
    sql = (
        f"SELECT {RESULT_COLUMNS} {RESULT_JOINS} {BASE_CONDITIONS}"
        "  AND m.source_type = ANY(%s)\n"
        "  AND m.created_at >= %s\n"
        "ORDER BY m.created_at DESC LIMIT %s"
    )
    try:
        conn = get_connection()
        try:
            with conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(sql, (wanted, today_start_utc, limit))
                    rows = cur.fetchall()
        finally:
            conn.close()
    except Exception as exc:
        logger.error("fetch_recent_items: database error — %s: %s", type(exc).__name__, exc)
        return []
    logger.info("fetch_recent_items: %d rows for %s", len(rows), wanted)
    return [normalise_row(row) for row in rows]
