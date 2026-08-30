"""
Hybrid retrieval branches — postgres_keyword_search and faiss_semantic_search.

The two branches run in PARALLEL (LangGraph fan-out from resolve_time_anchor)
and are fused downstream by merge_and_rrf:

  postgres_keyword_search — PostgreSQL Full-Text Search (websearch_to_tsquery)
      over title + raw_text + auto_keywords + email subject, with an ILIKE
      safety net for partial-token matches. Ordered by ts_rank so the branch
      rank reflects textual relevance, not recency. Applies deterministic
      source / time / time-anchor SQL constraints from parsed intent.

  faiss_semantic_search — embeds every query variant locally with
      sentence-transformers, searches the FULL FAISS index per variant,
      deduplicates keeping the best score per memory_id, and reports true
      cosine similarity (the index is IndexFlatL2 over unnormalized vectors,
      so cosine is computed here explicitly).

hydrate_memory_items — fetches full metadata rows for memory_ids that only
the semantic branch found, so the fused pool is uniformly shaped.
"""
import logging
from typing import Any, Sequence

import numpy as np
import psycopg2
import psycopg2.extras

from rse.config import (
    DATABASE_URL,
    FAISS_TOP_K_PER_VARIANT,
    FULL_SCAN_K_MULTIPLIER,
    PG_KEYWORD_LIMIT,
    POSTGRES_RESULT_LIMIT,
)
from rse.state import EchoState, ParsedIntent

logger = logging.getLogger(__name__)


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
    ym.duration_seconds
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

# Searchable document for FTS — computed on the fly. raw_text is capped so the
# tsvector stays cheap; with the current dataset size no stored column or GIN
# index is needed yet (add one when the table crosses ~50k rows).
# gm.sender is included so "emails from Neon" matches by WHO sent it, not only
# by whether the sender's name happens to appear in the subject or body.
_FTS_DOCUMENT = (
    "to_tsvector('english', "
    "COALESCE(m.title, '') || ' ' || "
    "LEFT(COALESCE(m.raw_text, ''), 5000) || ' ' || "
    "array_to_string(m.auto_keywords, ' ') || ' ' || "
    "COALESCE(gm.subject, '') || ' ' || "
    "COALESCE(gm.sender, '') || ' ' || "
    "COALESCE(cm.domain, '') || ' ' || "
    "COALESCE(ym.channel_name, ''))"
)


def get_connection():
    """Open a psycopg2 connection to the configured PostgreSQL database."""
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured for the RSE module.")
    return psycopg2.connect(DATABASE_URL)


def normalise_row(row: dict[str, Any]) -> dict[str, Any]:
    """Convert DB-native types to plain Python so rows merge cleanly with FAISS ids."""
    item = dict(row)
    item["memory_id"] = str(item["memory_id"])
    return item


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

    sources = intent.get("sources", ["gmail", "chrome", "youtube"])
    all_sources = {"gmail", "chrome", "youtube"}
    if sources and set(sources) != all_sources and "all" not in sources:
        placeholders = ", ".join(["%s"] * len(sources))
        clauses.append(f"m.source_type IN ({placeholders})")
        params.extend(sources)

    time_filter = intent.get("time_filter")
    if time_filter:
        clauses.append("m.created_at >= %s::timestamp")
        params.append(time_filter)

    return clauses, params


# ── postgres_keyword_search ───────────────────────────────────────────────────

def postgres_keyword_search(state: EchoState) -> dict:
    """
    Keyword branch of the hybrid retrieval (Node 2a).

    Runs PostgreSQL FTS over title/raw_text/auto_keywords/subject with an
    ILIKE fallback in the same query, ranked by ts_rank. Applies deterministic
    source, absolute-time, and time-anchor constraints. Result order defines
    Rank_postgres for Reciprocal Rank Fusion.

    Args:
        state: EchoState carrying parsed_intent and anchor_time.

    Returns:
        Partial state dict with postgres_results (list of normalized dicts,
        FTS-rank ordered). Empty list on any database error.
    """
    intent: ParsedIntent = state.get("parsed_intent", {})
    skip_filter: bool = intent.get("skip_postgres_filter", False)
    query_clean: str = (intent.get("query_clean") or "").strip()

    try:
        conn = get_connection()
        try:
            with conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    if skip_filter or not query_clean:
                        # Widen-scope attempt 3 (or no usable keyword): base
                        # conditions only, recency-ordered as a last resort.
                        sql = (
                            f"SELECT {RESULT_COLUMNS} {RESULT_JOINS} {BASE_CONDITIONS}"
                            "ORDER BY me.last_accessed_at DESC NULLS LAST LIMIT %s"
                        )
                        params: list[Any] = [POSTGRES_RESULT_LIMIT]
                        logger.info("postgres_keyword_search: base-only query (skip_filter=%s)", skip_filter)
                    else:
                        clauses, filter_params = _dynamic_filters(intent)
                        anchor_clauses, anchor_params = _anchor_constraint(state)
                        clauses += anchor_clauses
                        filter_params += anchor_params

                        where_block = BASE_CONDITIONS
                        if clauses:
                            where_block += "  AND " + "\n  AND ".join(clauses)

                        ilike_pattern = f"%{query_clean}%"
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
                        params = (
                            [query_clean]
                            + filter_params
                            + [query_clean] + [ilike_pattern] * 6
                            + [PG_KEYWORD_LIMIT]
                        )
                        logger.info(
                            "postgres_keyword_search: FTS query, %d filters, anchor=%s, sources=%s",
                            len(clauses),
                            bool(anchor_clauses),
                            intent.get("sources"),
                        )

                    cur.execute(sql, params)
                    rows = cur.fetchall()
        finally:
            conn.close()

        results = [normalise_row(row) for row in rows]
        logger.info("postgres_keyword_search: returned %d rows", len(results))
        return {"postgres_results": results}

    except Exception as exc:
        logger.error("postgres_keyword_search: database error — %s", exc)
        return {"postgres_results": []}


# ── faiss_semantic_search ─────────────────────────────────────────────────────

def _cosine_similarity(query_vec: np.ndarray, item_vec: np.ndarray) -> float:
    """True cosine similarity — the FAISS index stores unnormalized vectors."""
    denom = float(np.linalg.norm(query_vec) * np.linalg.norm(item_vec))
    if denom == 0.0:
        return 0.0
    return float(np.dot(query_vec, item_vec) / denom)


def _search_full_index(manager: Any, query_vec: np.ndarray, k: int) -> list[tuple[str, float]]:
    """
    Search the entire FAISS index (not candidate-filtered) and return
    (memory_id, cosine_similarity) pairs, best first.

    Uses the manager's public index/vectors/memory_ids attributes read-only —
    the STE FAISSManager only exposes a candidate-filtered search, and copying
    every vector into a temp index per variant would defeat the point.
    """
    total = manager.index.ntotal
    if total == 0:
        return []

    query = np.asarray(query_vec, dtype=np.float32).reshape(1, -1)
    limit = min(max(k, 1), total)
    _, indices = manager.index.search(query, limit)

    results: list[tuple[str, float]] = []
    flat_query = query[0]
    for offset in indices[0]:
        if offset < 0:
            continue
        memory_id = manager.memory_ids[offset]
        similarity = _cosine_similarity(flat_query, manager.vectors[offset])
        results.append((str(memory_id), similarity))
    results.sort(key=lambda pair: pair[1], reverse=True)
    return results


def faiss_semantic_search(state: EchoState) -> dict:
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

    try:
        from enp.embedding_generator import generate_embeddings
        from ste.faiss_manager import get_manager

        manager = get_manager()
        embeddings = generate_embeddings(variants)

        best_scores: dict[str, float] = {}
        for variant, vector in zip(variants, embeddings):
            hits = _search_full_index(manager, vector, k)
            logger.info("faiss_semantic_search: variant=%r hits=%d", variant, len(hits))
            for memory_id, similarity in hits:
                if similarity > best_scores.get(memory_id, float("-inf")):
                    best_scores[memory_id] = similarity

        merged = sorted(best_scores.items(), key=lambda pair: pair[1], reverse=True)
        logger.info(
            "faiss_semantic_search: %d variants pooled into %d unique candidates",
            len(variants),
            len(merged),
        )
        return {"faiss_results": merged}

    except Exception as exc:
        logger.error("faiss_semantic_search: search failed — %s", exc)
        return {"faiss_results": []}


# ── Hydration ─────────────────────────────────────────────────────────────────

def hydrate_memory_items(memory_ids: Sequence[str]) -> list[dict[str, Any]]:
    """
    Fetch full metadata rows for the given memory_ids (semantic-branch hits
    the keyword branch didn't return). Order of the result is not guaranteed.

    Args:
        memory_ids: memory_id strings to hydrate.

    Returns:
        List of normalized result dicts; empty on error or empty input.
    """
    ids = [str(mid) for mid in memory_ids]
    if not ids:
        return []

    sql = (
        f"SELECT {RESULT_COLUMNS} {RESULT_JOINS} {BASE_CONDITIONS}"
        "  AND m.memory_id::text = ANY(%s)"
    )
    try:
        conn = get_connection()
        try:
            with conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(sql, (ids,))
                    rows = cur.fetchall()
        finally:
            conn.close()
        return [normalise_row(row) for row in rows]
    except Exception as exc:
        logger.error("hydrate_memory_items: database error — %s", exc)
        return []


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
    wanted = [s for s in sources if s in ("gmail", "chrome", "youtube")] or ["gmail", "chrome", "youtube"]
    sql = (
        f"SELECT {RESULT_COLUMNS} {RESULT_JOINS} {BASE_CONDITIONS}"
        "  AND m.source_type = ANY(%s)"
        "  AND m.created_at >= CURRENT_DATE"
        "ORDER BY m.created_at DESC LIMIT %s"
    )
    try:
        conn = get_connection()
        try:
            with conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(sql, (wanted, limit))
                    rows = cur.fetchall()
        finally:
            conn.close()
        return [normalise_row(row) for row in rows]
    except Exception as exc:
        logger.error("fetch_recent_items: database error — %s", exc)
        return []
