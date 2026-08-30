"""
rerank_cross_encoder node — local cross-encoder re-ranking.

After Reciprocal Rank Fusion, the top RRF_POOL_SIZE (~50) candidates are
re-scored by cross-encoder/ms-marco-MiniLM-L-6-v2 against the ORIGINAL user
query. Unlike the bi-encoder used for FAISS, the cross-encoder reads the
query and the document together, so it captures interactions the vector
search cannot. It runs locally — zero API calls.

Final ranking combines the model's relevance judgement with Echo's cognitive
effort signal (how much attention the user actually gave the item):

    final_score = CROSS_ENCODER_WEIGHT * sigmoid(ce_logit)
                + EFFORT_WEIGHT * effort_score

Deliberately absent (prohibited at this stage): MMR/diversity re-ranking and
any recency-decay term.
"""
import logging
import math
from functools import lru_cache
from typing import Any

from rse.config import (
    CROSS_ENCODER_MODEL,
    CROSS_ENCODER_WEIGHT,
    EFFORT_WEIGHT,
    RERANK_DOC_CHAR_LIMIT,
    RRF_POOL_SIZE,
)
from rse.state import EchoState

logger = logging.getLogger(__name__)

# Normalisation ceilings for effort signals. Values at or above the ceiling
# count as full-effort (1.0); everything scales linearly below it.
_DWELL_CEILING_SECONDS = 600     # 10 minutes of active reading
_REVISIT_CEILING = 5             # returned 5+ times
_INTERACTION_CEILING = 10        # 10+ clicks / text selections


@lru_cache(maxsize=1)
def get_cross_encoder():
    """Load the cross-encoder once per process (~80 MB, CPU-friendly)."""
    try:
        from sentence_transformers import CrossEncoder
    except ImportError as exc:
        raise RuntimeError(
            "sentence-transformers is required for cross-encoder re-ranking. "
            "Install RSE dependencies before running retrieval."
        ) from exc
    logger.info("Loading cross-encoder model %s", CROSS_ENCODER_MODEL)
    return CrossEncoder(CROSS_ENCODER_MODEL)


def _sigmoid(logit: float) -> float:
    """Map an ms-marco relevance logit to (0, 1)."""
    return 1.0 / (1.0 + math.exp(-logit))


def compute_effort_score(item: dict[str, Any]) -> float:
    """
    Cognitive effort score in [0, 1] — how much attention the user invested.

    Averages whichever signals exist for the item's source type:
      dwell/watch time, scroll depth, revisits, interactions, watch completion.
    Items never opened score 0.0. No recency term by design.
    """
    signals: list[float] = []

    dwell = (item.get("dwell_time_seconds") or 0) + (item.get("watch_time_seconds") or 0)
    signals.append(min(dwell / _DWELL_CEILING_SECONDS, 1.0))

    scroll_depth = item.get("scroll_depth")
    if scroll_depth is not None:
        signals.append(min(max(float(scroll_depth), 0.0), 1.0))

    revisit_count = item.get("revisit_count")
    if revisit_count is not None:
        signals.append(min(revisit_count / _REVISIT_CEILING, 1.0))

    interaction_count = item.get("interaction_count")
    if interaction_count is not None:
        signals.append(min(interaction_count / _INTERACTION_CEILING, 1.0))

    completion_rate = item.get("completion_rate")
    if completion_rate is not None:
        signals.append(min(max(float(completion_rate), 0.0), 1.0))

    return sum(signals) / len(signals) if signals else 0.0


def _document_text(item: dict[str, Any]) -> str:
    """Build the text side of a (query, document) cross-encoder pair."""
    parts = [
        item.get("title") or "",
        item.get("subject") or "",
        item.get("raw_snippet") or "",
    ]
    text = " ".join(p for p in parts if p).strip()
    return text[:RERANK_DOC_CHAR_LIMIT] if text else "(untitled item)"


def rerank_cross_encoder(state: EchoState) -> dict:
    """
    Re-rank the fused candidate pool with the cross-encoder + effort score.

    Degrades gracefully: if the model cannot load or scoring fails, ranking
    falls back to RRF order with the effort component only, so retrieval never
    dies on a model issue.

    Args:
        state: EchoState carrying merged_candidates and user_query.

    Returns:
        Partial state dict with ranked_results — candidate dicts annotated
        with ce_score, effort_score, final_score; sorted by final_score desc.
    """
    candidates: list[dict[str, Any]] = state.get("merged_candidates", [])[:RRF_POOL_SIZE]
    original_query: str = state.get("user_query", "")

    if not candidates:
        return {"ranked_results": []}

    ce_probs: list[float] | None = None
    try:
        model = get_cross_encoder()
        pairs = [(original_query, _document_text(item)) for item in candidates]
        logits = model.predict(pairs)
        ce_probs = [_sigmoid(float(logit)) for logit in logits]
    except Exception as exc:
        logger.error("rerank_cross_encoder: scoring failed, falling back to RRF order — %s", exc)

    ranked: list[dict[str, Any]] = []
    for position, item in enumerate(candidates):
        entry = dict(item)
        effort = compute_effort_score(item)
        entry["effort_score"] = effort
        if ce_probs is not None:
            entry["ce_score"] = ce_probs[position]
            entry["final_score"] = (
                CROSS_ENCODER_WEIGHT * ce_probs[position] + EFFORT_WEIGHT * effort
            )
        else:
            entry["ce_score"] = None
            # Fallback: preserve RRF ordering via rank position, effort as tiebreak.
            entry["final_score"] = (
                CROSS_ENCODER_WEIGHT * (1.0 - position / len(candidates))
                + EFFORT_WEIGHT * effort
            )
        ranked.append(entry)

    ranked.sort(key=lambda r: r["final_score"], reverse=True)

    logger.info(
        "rerank_cross_encoder: ranked %d candidates (cross-encoder=%s), top=%r",
        len(ranked),
        "ok" if ce_probs is not None else "fallback",
        ranked[0].get("title") if ranked else None,
    )
    return {"ranked_results": ranked}
