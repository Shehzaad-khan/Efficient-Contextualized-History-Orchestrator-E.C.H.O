"""
merge_and_rrf node — Reciprocal Rank Fusion of the two hybrid branches.

Takes the FTS-rank-ordered postgres_results and the cosine-ordered
faiss_results and fuses them into one candidate pool:

    Score(item) = 1 / (RRF_K + Rank_postgres) + 1 / (RRF_K + Rank_faiss)

Items appearing in only one branch receive that branch's term only. Ranks are
1-based; RRF_K defaults to 60. Rank fusion deliberately ignores the raw score
magnitudes — ts_rank and cosine live on incomparable scales, ranks don't.

Semantic-branch hits that the keyword branch didn't return are hydrated from
PostgreSQL, then held to the SAME deterministic constraints the keyword SQL
already enforced (source filter, absolute time filter, time-anchor constraint)
so both branches stay consistent and evaluate_quality sees one coherent pool.
"""
import logging
from datetime import datetime
from typing import Any, Optional

from rse.config import RRF_K, RRF_POOL_SIZE
from rse.search_coordinator import hydrate_memory_items
from rse.state import EchoState, ParsedIntent

logger = logging.getLogger(__name__)


def _parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    """Parse an ISO-8601 string to datetime; None on failure."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        logger.warning("merge_and_rrf: unparseable timestamp %r ignored", value)
        return None


def _passes_intent_constraints(
    item: dict[str, Any],
    intent: ParsedIntent,
    anchor_dt: Optional[datetime],
) -> bool:
    """
    Apply the keyword branch's deterministic SQL constraints in Python to
    hydrated semantic-only hits, keeping both branches consistent.
    """
    sources = intent.get("sources", [])
    all_sources = {"gmail", "chrome", "youtube"}
    if sources and set(sources) != all_sources and "all" not in sources:
        if item.get("source_type") not in sources:
            return False

    created_at = item.get("created_at")
    if isinstance(created_at, datetime):
        time_filter_dt = _parse_timestamp(intent.get("time_filter"))
        if time_filter_dt and created_at < time_filter_dt:
            return False

        relation = intent.get("time_relation")
        if anchor_dt and relation == "after" and created_at <= anchor_dt:
            return False
        if anchor_dt and relation == "before" and created_at >= anchor_dt:
            return False

    return True


def merge_and_rrf(state: EchoState) -> dict:
    """
    Fuse the keyword and semantic branches with Reciprocal Rank Fusion.

    Args:
        state: EchoState carrying postgres_results, faiss_results,
               parsed_intent, and anchor_time.

    Returns:
        Partial state dict with merged_candidates — hydrated result dicts
        annotated with rrf_score, pg_rank, faiss_rank, similarity_score;
        sorted by rrf_score descending and capped at RRF_POOL_SIZE.
    """
    postgres_results: list[dict[str, Any]] = state.get("postgres_results", [])
    faiss_results: list[tuple[str, float]] = state.get("faiss_results", [])
    intent: ParsedIntent = state.get("parsed_intent", {})
    anchor_dt = _parse_timestamp(state.get("anchor_time"))

    pg_ranks: dict[str, int] = {}
    for rank, item in enumerate(postgres_results, start=1):
        pg_ranks.setdefault(item["memory_id"], rank)

    faiss_ranks: dict[str, int] = {}
    similarities: dict[str, float] = {}
    for rank, (memory_id, similarity) in enumerate(faiss_results, start=1):
        faiss_ranks.setdefault(memory_id, rank)
        similarities[memory_id] = similarity

    scores: dict[str, float] = {}
    for memory_id, rank in pg_ranks.items():
        scores[memory_id] = scores.get(memory_id, 0.0) + 1.0 / (RRF_K + rank)
    for memory_id, rank in faiss_ranks.items():
        scores[memory_id] = scores.get(memory_id, 0.0) + 1.0 / (RRF_K + rank)

    if not scores:
        logger.info("merge_and_rrf: both branches empty")
        return {"merged_candidates": []}

    # Hydrate semantic-only hits so every candidate carries full metadata.
    pg_items = {item["memory_id"]: item for item in postgres_results}
    missing_ids = [mid for mid in scores if mid not in pg_items]
    hydrated = {item["memory_id"]: item for item in hydrate_memory_items(missing_ids)}

    candidates: list[dict[str, Any]] = []
    for memory_id, rrf_score in scores.items():
        item = pg_items.get(memory_id) or hydrated.get(memory_id)
        if item is None:
            # Present in FAISS but soft-deleted / unprocessed in PostgreSQL.
            continue
        # Keyword-branch rows already satisfied the SQL constraints; hydrated
        # semantic-only rows must be checked here.
        if memory_id not in pg_items and not _passes_intent_constraints(item, intent, anchor_dt):
            continue

        merged = dict(item)
        merged["rrf_score"] = rrf_score
        merged["pg_rank"] = pg_ranks.get(memory_id)
        merged["faiss_rank"] = faiss_ranks.get(memory_id)
        merged["similarity_score"] = similarities.get(memory_id)
        candidates.append(merged)

    candidates.sort(key=lambda c: c["rrf_score"], reverse=True)
    pool = candidates[:RRF_POOL_SIZE]

    logger.info(
        "merge_and_rrf: pg=%d faiss=%d fused=%d pool=%d",
        len(postgres_results),
        len(faiss_results),
        len(candidates),
        len(pool),
    )
    return {"merged_candidates": pool}
