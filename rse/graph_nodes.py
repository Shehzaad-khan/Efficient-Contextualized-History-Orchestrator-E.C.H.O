"""
All LangGraph node implementations for the hybrid RSE pipeline.

This module is the single import point for the graph assembly in
retrieval_engine.py. Node inventory:

  parse_intent            LLM Call 1 — query_parser.py (query expansion + anchor fields)
  resolve_time_anchor     deterministic anchor lookup — time_anchor.py
  postgres_keyword_search FTS/ILIKE branch — search_coordinator.py   (parallel A)
  faiss_semantic_search   multi-variant vector branch — search_coordinator.py (parallel B)
  merge_and_rrf           Reciprocal Rank Fusion — fusion.py
  evaluate_quality        deterministic 5-check gate (no LLM)
  widen_scope             self-correcting loop, 3 attempts max
  rerank_cross_encoder    local cross-encoder + effort score — reranker.py
  extend_neighborhood     thread/session context — neighborhood.py
  check_attachments       routing point for on-demand attachment fetch
  fetch_attachment        stub (Phase: Gmail API + Redis cache)
  synthesize              LLM Call 2 — llm_synthesizer.py
  no_results_found        structured no-results message

Total LLM calls per query: exactly 2 (parse_intent + synthesize), regardless
of widen_scope loop iterations.
"""
import logging
from datetime import datetime, timedelta

from rse.config import MIN_RESULT_COUNT, MIN_TOP_SIMILARITY
from rse.state import EchoState
from rse.query_parser import parse_intent
from rse.search_coordinator import postgres_keyword_search, faiss_semantic_search
from rse.fusion import merge_and_rrf
from rse.time_anchor import resolve_time_anchor
from rse.reranker import rerank_cross_encoder
from rse.neighborhood import extend_neighborhood
from rse.llm_synthesizer import synthesize

logger = logging.getLogger(__name__)

_ALL_SOURCES = ["gmail", "chrome", "youtube"]


def node_parse_intent(state: EchoState) -> dict:
    """Node 1 — parse_intent (LLM Call 1). See query_parser.parse_intent."""
    logger.info("NODE: parse_intent")
    return parse_intent(state)


def node_resolve_time_anchor(state: EchoState) -> dict:
    """Node 1b — resolve_time_anchor. No-op when the intent has no anchor."""
    logger.info("NODE: resolve_time_anchor")
    return resolve_time_anchor(state)


def node_postgres_keyword_search(state: EchoState) -> dict:
    """Node 2a — keyword branch (runs in parallel with 2b)."""
    logger.info("NODE: postgres_keyword_search")
    return postgres_keyword_search(state)


def node_faiss_semantic_search(state: EchoState) -> dict:
    """Node 2b — semantic branch with query expansion (parallel with 2a)."""
    logger.info("NODE: faiss_semantic_search")
    return faiss_semantic_search(state)


def node_merge_and_rrf(state: EchoState) -> dict:
    """Node 3 — Reciprocal Rank Fusion of both branches."""
    logger.info("NODE: merge_and_rrf")
    return merge_and_rrf(state)


def node_evaluate_quality(state: EchoState) -> dict:
    """
    Node 4 — evaluate_quality. Deterministic, five sequential checks, no LLM.

    Operates on the fused candidate pool:
      1. Empty pool                              → 'empty'
      2. Requested source absent from pool       → 'empty'
      3. No candidate inside the time window     → 'weak'
      4. Fewer than MIN_RESULT_COUNT candidates  → 'weak'
      5. Best cosine similarity in the pool below
         MIN_TOP_SIMILARITY (skipped when the semantic
         branch returned nothing to measure)     → 'weak'
      Otherwise                                  → 'strong'
    """
    logger.info("NODE: evaluate_quality")
    candidates = state.get("merged_candidates", [])
    intent = state.get("parsed_intent", {})

    # Check 1 — any results at all?
    if not candidates:
        logger.info("evaluate_quality: empty pool")
        return {"result_quality": "empty"}

    # Check 2 — source match when a specific source was requested.
    sources = intent.get("sources", _ALL_SOURCES)
    if sources and set(sources) != set(_ALL_SOURCES) and "all" not in sources:
        if not any(c.get("source_type") in sources for c in candidates):
            logger.info("evaluate_quality: no candidate from requested sources %s", sources)
            return {"result_quality": "empty"}

    # Check 3 — time window match when a time filter is set.
    time_filter = intent.get("time_filter")
    if time_filter:
        try:
            filter_dt = datetime.fromisoformat(time_filter)
            in_window = [
                c for c in candidates
                if isinstance(c.get("created_at"), datetime) and c["created_at"] >= filter_dt
            ]
            if not in_window:
                logger.info("evaluate_quality: nothing within time window %s", time_filter)
                return {"result_quality": "weak"}
        except ValueError:
            logger.warning("evaluate_quality: unparseable time_filter %r — check skipped", time_filter)

    # Check 4 — minimum candidate count.
    if len(candidates) < MIN_RESULT_COUNT:
        logger.info("evaluate_quality: only %d candidate(s)", len(candidates))
        return {"result_quality": "weak"}

    # Check 5 — semantic strength of the pool (best cosine among candidates).
    similarities = [
        c["similarity_score"] for c in candidates if c.get("similarity_score") is not None
    ]
    if similarities and max(similarities) < MIN_TOP_SIMILARITY:
        logger.info("evaluate_quality: best similarity %.3f < %.2f",
                    max(similarities), MIN_TOP_SIMILARITY)
        return {"result_quality": "weak"}

    logger.info("evaluate_quality: strong (%d candidates)", len(candidates))
    return {"result_quality": "strong"}


def node_widen_scope(state: EchoState) -> dict:
    """
    Node 5 — widen_scope. Called on weak/empty quality while attempts remain.
    Each attempt widens one dimension, then the graph loops back to BOTH
    search branches.

      Attempt 1: extend time window by 4 days, or open all sources,
                 or trim query to its first keyword.
      Attempt 2: drop time filter AND time-anchor constraint, all sources,
                 core keyword only (variants collapse to it too).
      Attempt 3: skip the Postgres dynamic filters entirely and widen the
                 FAISS k per variant (full_faiss_scan).
    """
    logger.info("NODE: widen_scope")
    attempt = state.get("attempt_count", 0)
    intent = dict(state.get("parsed_intent", {}))
    updates: dict = {}

    if attempt == 0:
        if intent.get("time_filter"):
            try:
                original_dt = datetime.fromisoformat(intent["time_filter"])
                intent["time_filter"] = (original_dt - timedelta(days=4)).isoformat()
                logger.info("widen_scope attempt 1: time window extended by 4 days")
            except ValueError:
                intent["time_filter"] = None
                logger.info("widen_scope attempt 1: invalid time_filter cleared")
        elif intent.get("sources") not in [_ALL_SOURCES, ["all"]]:
            intent["sources"] = list(_ALL_SOURCES)
            logger.info("widen_scope attempt 1: sources opened to all")
        else:
            query_clean = intent.get("query_clean", "")
            core = query_clean.split()[0] if query_clean else query_clean
            intent["query_clean"] = core
            logger.info("widen_scope attempt 1: query trimmed to first keyword")

    elif attempt == 1:
        intent["time_filter"] = None
        intent["sources"] = list(_ALL_SOURCES)
        query_clean = intent.get("query_clean", "")
        core = query_clean.split()[0] if query_clean else query_clean
        intent["query_clean"] = core
        if core:
            intent["query_variants"] = [core]
        # Drop the anchor constraint — it may be the reason nothing matched.
        intent["time_anchor_query"] = None
        intent["time_relation"] = None
        updates["anchor_time"] = None
        updates["anchor_item"] = None
        logger.info("widen_scope attempt 2: all filters and anchor removed, core keyword only")

    elif attempt >= 2:
        intent["skip_postgres_filter"] = True
        intent["full_faiss_scan"] = True
        logger.info("widen_scope attempt 3: skip_postgres_filter and full_faiss_scan enabled")

    updates["parsed_intent"] = intent
    updates["attempt_count"] = attempt + 1
    return updates


def node_rerank_cross_encoder(state: EchoState) -> dict:
    """Node 6 — cross-encoder re-ranking. See reranker.rerank_cross_encoder."""
    logger.info("NODE: rerank_cross_encoder")
    return rerank_cross_encoder(state)


def node_extend_neighborhood(state: EchoState) -> dict:
    """Node 7 — neighborhood extension. See neighborhood.extend_neighborhood."""
    logger.info("NODE: extend_neighborhood")
    return extend_neighborhood(state)


def node_check_attachments(state: EchoState) -> dict:
    """
    Node 8 — check_attachments.

    Pure pass-through routing point; route_after_check_attachments in
    graph_routing.py inspects the top-3 ranked results for attachments.
    """
    logger.info("NODE: check_attachments")
    return {}


def node_fetch_attachment(state: EchoState) -> dict:
    """
    Node 9 — fetch_attachment.

    STUB — the on-demand Gmail attachment pipeline (fetch binary, extract text
    with PyPDF2/pdfplumber, cache in Redis with 1-hour TTL) is integrated in a
    later session together with the GMC module rework.
    """
    logger.info("NODE: fetch_attachment (stub)")
    return {"attachment_content": None}


def node_synthesize(state: EchoState) -> dict:
    """Node 10 — synthesize (LLM Call 2). See llm_synthesizer.synthesize."""
    logger.info("NODE: synthesize")
    return synthesize(state)


def node_no_results_found(state: EchoState) -> dict:
    """
    Node 11 — no_results_found.

    Triggered after 3 failed widen_scope attempts. Returns a structured
    descriptive message explaining what was searched and suggesting alternatives.
    """
    logger.info("NODE: no_results_found")
    intent = state.get("parsed_intent", {})
    query = intent.get("original_query", state.get("user_query", ""))
    sources = intent.get("sources", [])
    time_filter = intent.get("time_filter")
    anchor = state.get("anchor_item")

    sources_str = ", ".join(sources) if sources else "all sources"
    time_str = f" from around {time_filter}" if time_filter else ""
    anchor_str = ""
    if anchor:
        anchor_str = (
            f" (anchored {intent.get('time_relation', '')} "
            f"'{anchor.get('title', 'the anchor item')}')"
        )

    message = (
        f"No results found for '{query}'{time_str}{anchor_str} across {sources_str}. "
        "Echo searched progressively broader filters across 3 attempts, combining "
        "keyword and semantic retrieval. Suggestions: try a shorter keyword, remove "
        "the time constraint, or check that the relevant content has been ingested "
        "and processed."
    )
    return {"final_answer": message, "no_results": True}
