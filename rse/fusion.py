"""
merge_and_rrf node — Reciprocal Rank Fusion of the two hybrid branches.

Takes the FTS-rank-ordered postgres_results and the cosine-ordered
faiss_results and fuses them into one candidate pool:

    Score(item) = 1 / (RRF_K + Rank_postgres) + 1 / (RRF_K + Rank_faiss)

Items appearing in only one branch receive that branch's term only. Ranks are
1-based; RRF_K defaults to 60. Rank fusion deliberately ignores the raw score
magnitudes — ts_rank and cosine live on incomparable scales, ranks don't.
Equal scores are ordered by the item's best single-branch rank, then by
memory_id — never by recency (recency boosting is a prohibited feature).

Semantic-branch hits that the keyword branch didn't return are hydrated from
PostgreSQL, then held to the SAME deterministic constraints the keyword SQL
already enforced (source filter, absolute time filter, time-anchor constraint)
so both branches stay consistent and evaluate_quality sees one coherent pool.
On widen attempt 3 (skip_postgres_filter) the keyword SQL drops those
constraints, so this check is skipped too. Hydration runs in score order and
stops once the pool (RRF_POOL_SIZE) is full, so rows that could never make the
pool are not fetched.
"""
import logging
from datetime import datetime
from typing import Any, Optional

from rse.config import RRF_K, RRF_POOL_SIZE
from rse.search_coordinator import filter_time_of, hydrate_memory_items, local_to_utc_naive
from rse.state import EchoState, ParsedIntent

logger = logging.getLogger(__name__)

_ALL_SOURCES: frozenset[str] = frozenset({"gmail", "chrome", "youtube"})


def _parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    """Parse an ISO-8601 string (already UTC, e.g. anchor_time) to datetime; None on failure."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        logger.warning("merge_and_rrf: unparseable timestamp ignored (len=%d)", len(value))
        return None


def _time_filter_utc(intent: ParsedIntent) -> Optional[datetime]:
    """The intent's local-date time_filter as naive UTC (created_at and visited_at are naive UTC)."""
    time_filter = intent.get("time_filter")
    if not time_filter:
        return None
    try:
        return local_to_utc_naive(time_filter)
    except ValueError:
        logger.warning("merge_and_rrf: invalid time_filter ignored (len=%d)", len(str(time_filter)))
        return None


def _passes_intent_constraints(
    item: dict[str, Any],
    intent: ParsedIntent,
    time_filter_utc: Optional[datetime],
    anchor_dt: Optional[datetime],
) -> bool:
    """
    Apply the keyword branch's deterministic SQL constraints in Python to
    hydrated semantic-only hits, keeping both branches consistent.
    """
    sources = intent.get("sources", [])
    if sources and set(sources) != _ALL_SOURCES and "all" not in sources:
        if item.get("source_type") not in sources:
            return False

    # A1.1: Gmail by arrival, Chrome/YouTube by their latest visit (same rule as the SQL).
    filter_time = filter_time_of(item)
    if time_filter_utc and filter_time is not None and filter_time < time_filter_utc:
        return False

    created_at = item.get("created_at")
    if isinstance(created_at, datetime):
        relation = intent.get("time_relation")
        if anchor_dt and relation == "after" and created_at <= anchor_dt:
            return False
        if anchor_dt and relation == "before" and created_at >= anchor_dt:
            return False

    return True


def merge_and_rrf(state: EchoState) -> dict[str, Any]:
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
    check_constraints = not intent.get("skip_postgres_filter", False)
    time_filter_utc = _time_filter_utc(intent) if check_constraints else None
    anchor_dt = _parse_timestamp(state.get("anchor_time")) if check_constraints else None

    # 1. Rank each branch (1-based; first occurrence wins).
    pg_ranks: dict[str, int] = {}
    for rank, item in enumerate(postgres_results, start=1):
        pg_ranks.setdefault(item["memory_id"], rank)

    faiss_ranks: dict[str, int] = {}
    similarities: dict[str, float] = {}
    for rank, (memory_id, similarity) in enumerate(faiss_results, start=1):
        if memory_id not in faiss_ranks:
            faiss_ranks[memory_id] = rank
            similarities[memory_id] = similarity

    # 2. RRF score = sum of 1 / (k + rank) over the branches the item appears in.
    scores: dict[str, float] = {}
    for ranks in (pg_ranks, faiss_ranks):
        for memory_id, rank in ranks.items():
            scores[memory_id] = scores.get(memory_id, 0.0) + 1.0 / (RRF_K + rank)

    if not scores:
        logger.info("merge_and_rrf: both branches empty")
        return {"merged_candidates": []}

    # 3. Order by score; ties by best single-branch rank, then id (deterministic).
    def best_rank(memory_id: str) -> int:
        return min(pg_ranks.get(memory_id, 10**9), faiss_ranks.get(memory_id, 10**9))

    ordered_ids = sorted(scores, key=lambda mid: (-scores[mid], best_rank(mid), mid))

    # 4. Walk in score order; hydrate semantic-only ids in batches until the pool is full.
    pg_items = {item["memory_id"]: item for item in postgres_results}
    pool: list[dict[str, Any]] = []
    hydrated: dict[str, dict[str, Any]] = {}
    dropped_missing = dropped_constraint = hydrate_calls = 0
    batch_size = RRF_POOL_SIZE * 2
    position = 0
    while position < len(ordered_ids) and len(pool) < RRF_POOL_SIZE:
        batch = ordered_ids[position:position + batch_size]
        position += len(batch)
        to_fetch = [mid for mid in batch if mid not in pg_items]
        if to_fetch:
            hydrate_calls += 1
            hydrated.update({item["memory_id"]: item for item in hydrate_memory_items(to_fetch)})

        for memory_id in batch:
            item = pg_items.get(memory_id) or hydrated.get(memory_id)
            if item is None:
                # Present in FAISS but soft-deleted / unprocessed in PostgreSQL.
                dropped_missing += 1
                continue
            # Keyword-branch rows already satisfied the SQL constraints; hydrated
            # semantic-only rows must be checked here.
            if (check_constraints and memory_id not in pg_items
                    and not _passes_intent_constraints(item, intent, time_filter_utc, anchor_dt)):
                dropped_constraint += 1
                continue

            merged = dict(item)
            merged["rrf_score"] = scores[memory_id]
            merged["pg_rank"] = pg_ranks.get(memory_id)
            merged["faiss_rank"] = faiss_ranks.get(memory_id)
            merged["similarity_score"] = similarities.get(memory_id)
            pool.append(merged)
            if len(pool) == RRF_POOL_SIZE:
                break

    both = sum(1 for c in pool if c["pg_rank"] is not None and c["faiss_rank"] is not None)
    logger.info(
        "merge_and_rrf: pg=%d faiss=%d unique=%d pool=%d (in both=%d) dropped: missing=%d constraint=%d "
        "hydrate_calls=%d",
        len(postgres_results), len(faiss_results), len(scores), len(pool), both,
        dropped_missing, dropped_constraint, hydrate_calls,
    )
    return {"merged_candidates": pool}
