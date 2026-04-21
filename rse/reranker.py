"""
Multi-signal re-ranking — stub for this phase.

Real implementation (Phase 3) applies the scoring formula:
    final_score = 0.40 * semantic_similarity
                + 0.30 * engagement_strength
                + 0.20 * recency_score
                + 0.10 * effort_score

This requires FAISS cosine scores from Mir's FAISS manager, which are not yet
available. This module is imported by llm_synthesizer.py once real scores exist.
"""
import logging
from typing import Any

logger = logging.getLogger(__name__)


def rerank(
    postgres_results: list[dict[str, Any]],
    faiss_results: list[tuple[str, float]],
) -> list[dict[str, Any]]:
    """
    Merge and re-rank postgres and FAISS results by final_score.

    Stub: returns postgres_results in their original order (by last_accessed_at
    DESC from the SQL query) since no FAISS scores are available this phase.

    Args:
        postgres_results: List of result dicts from postgres_search.
        faiss_results: List of (memory_id, cosine_score) tuples from faiss_search.

    Returns:
        Re-ranked list of result dicts (postgres order this phase).
    """
    logger.debug("reranker: stub — returning postgres order, %d results", len(postgres_results))
    return postgres_results
