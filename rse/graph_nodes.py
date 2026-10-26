"""
The search-stage nodes of the RSE (A26 agent loop).

Since A26 these are not LangGraph nodes any more: rse/search_tool.py run_search calls them in order
for each search Luna asks for, and the graph itself is only agent ⇄ search → finish
(rse/agent_nodes.py, rse/retrieval_engine.py). The functions are unchanged and keep their node_ names
and log lines. Inventory:

  resolve_time_anchor     deterministic anchor lookup — time_anchor.py
  postgres_keyword_search FTS/ILIKE branch — search_coordinator.py   (parallel A)
  faiss_semantic_search   multi-variant vector branch — search_coordinator.py (parallel B)
  merge_and_rrf           Reciprocal Rank Fusion — fusion.py
  evaluate_quality        deterministic 5-check gate (no LLM)
  widen_scope             self-correcting loop, 3 attempts max (no LLM)
  rerank_cross_encoder    local cross-encoder + effort score — reranker.py
  extend_neighborhood     thread/session context — neighborhood.py
  fetch_attachment        on-demand Tier-2 extraction — enp/attachment_processor.py
  no_results_found        friendly no-results chat message (used by rse/agent_nodes.py)

Removed with A26: parse_intent (the agent's first call now supplies the search arguments), the
check_attachments routing point (the read_attachment argument decides), fetch_api (A45 dropped the
live poll; fetch_recent_items stays for E1) and synthesize (the agent's last reply is the answer).
Logs carry node names, counts, flags and dates only — never query text or titles.
"""
import logging
from datetime import datetime, timedelta
from typing import Any

from rse.config import MIN_RESULT_COUNT, MIN_TOP_SIMILARITY
from rse.state import EchoState
from rse.search_coordinator import (
    faiss_semantic_search,
    filter_time_of,
    local_to_utc_naive,
    postgres_keyword_search,
)
from rse.fusion import merge_and_rrf
from rse.time_anchor import resolve_time_anchor
from rse.reranker import rerank_cross_encoder
from rse.neighborhood import extend_neighborhood

logger = logging.getLogger(__name__)

_ALL_SOURCES: list[str] = ["gmail", "chrome", "youtube"]

# When the agent call fails, the fallback search's query_clean is the verbatim
# user question — so "trim to the first keyword" must skip question/filler
# words or the whole widen loop ends up searching for "What".
_TRIM_STOPWORDS = frozenset({
    "a", "an", "the", "any", "all", "some", "my", "me", "i", "you", "it",
    "what", "which", "who", "whom", "whose", "when", "where", "why", "how",
    "did", "do", "does", "was", "were", "is", "are", "am", "be", "been",
    "have", "has", "had", "get", "got", "find", "show", "search", "give",
    "about", "for", "from", "to", "of", "on", "in", "at", "with", "and",
    "or", "that", "this", "those", "these", "there",
})


def _core_keyword(query_clean: str) -> str:
    """First informative token of query_clean (first token if all are filler)."""
    tokens = query_clean.split()
    for token in tokens:
        if token.strip("?!.,\"'").lower() not in _TRIM_STOPWORDS:
            return token.strip("?!.,\"'")
    return tokens[0] if tokens else query_clean


def _is_all_sources(sources: Any) -> bool:
    """True when the intent covers every source (order-independent)."""
    return not sources or "all" in sources or set(sources) == set(_ALL_SOURCES)


def node_resolve_time_anchor(state: EchoState) -> dict[str, Any]:
    """Node 1b — resolve_time_anchor. No-op when the intent has no anchor."""
    logger.info("NODE: resolve_time_anchor")
    return resolve_time_anchor(state)


def node_postgres_keyword_search(state: EchoState) -> dict[str, Any]:
    """Node 2a — keyword branch (runs in parallel with 2b)."""
    logger.info("NODE: postgres_keyword_search")
    return postgres_keyword_search(state)


def node_faiss_semantic_search(state: EchoState) -> dict[str, Any]:
    """Node 2b — semantic branch with query expansion (parallel with 2a)."""
    logger.info("NODE: faiss_semantic_search")
    return faiss_semantic_search(state)


def node_merge_and_rrf(state: EchoState) -> dict[str, Any]:
    """Node 3 — Reciprocal Rank Fusion of both branches."""
    logger.info("NODE: merge_and_rrf")
    return merge_and_rrf(state)


def node_evaluate_quality(state: EchoState) -> dict[str, Any]:
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
    candidates: list[dict[str, Any]] = state.get("merged_candidates", [])
    intent = state.get("parsed_intent", {})

    # Check 1 — any results at all?
    if not candidates:
        logger.info("evaluate_quality: empty pool")
        return {"result_quality": "empty"}

    # Check 2 — source match when a specific source was requested.
    sources = intent.get("sources", _ALL_SOURCES)
    if not _is_all_sources(sources):
        if not any(c.get("source_type") in sources for c in candidates):
            logger.info("evaluate_quality: no candidate from requested sources %s", sources)
            return {"result_quality": "empty"}

    # Check 3 — time window match when a time filter is set. The filter is the
    # user's local date; created_at and visited_at are naive UTC, so convert before
    # comparing. Gmail is compared on arrival, Chrome/YouTube on their latest visit (A1.1).
    time_filter = intent.get("time_filter")
    if time_filter:
        try:
            filter_utc = local_to_utc_naive(time_filter)
        except ValueError:
            logger.warning("evaluate_quality: unparseable time_filter — check skipped")
        else:
            in_window = [
                c for c in candidates
                if (filter_time := filter_time_of(c)) is not None and filter_time >= filter_utc
            ]
            if not in_window:
                logger.info("evaluate_quality: nothing within time window (since %s local)", time_filter)
                return {"result_quality": "weak"}

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


def node_widen_scope(state: EchoState) -> dict[str, Any]:
    """
    Node 5 — widen_scope. Called on weak/empty quality while attempts remain.
    Each attempt widens one dimension, then the graph loops back to BOTH
    search branches. The intent is copied, never changed in place.

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
    updates: dict[str, Any] = {}

    if attempt == 0:
        if intent.get("time_filter"):
            try:
                original_dt = datetime.fromisoformat(intent["time_filter"])
                intent["time_filter"] = (original_dt - timedelta(days=4)).isoformat()
                logger.info("widen_scope attempt 1: time window extended by 4 days")
            except ValueError:
                intent["time_filter"] = None
                logger.info("widen_scope attempt 1: invalid time_filter cleared")
        elif not _is_all_sources(intent.get("sources")):
            intent["sources"] = list(_ALL_SOURCES)
            logger.info("widen_scope attempt 1: sources opened to all")
        else:
            query_clean = intent.get("query_clean", "")
            core = _core_keyword(query_clean) if query_clean else query_clean
            intent["query_clean"] = core
            logger.info("widen_scope attempt 1: query trimmed to its core keyword (%d → %d words)",
                        len(query_clean.split()), len(core.split()))

    elif attempt == 1:
        intent["time_filter"] = None
        intent["sources"] = list(_ALL_SOURCES)
        query_clean = intent.get("query_clean", "")
        core = _core_keyword(query_clean) if query_clean else query_clean
        intent["query_clean"] = core
        if core:
            intent["query_variants"] = [core]
        # Drop the anchor constraint — it may be the reason nothing matched.
        intent["time_anchor_query"] = None
        intent["time_relation"] = None
        updates["anchor_time"] = None
        updates["anchor_item"] = None
        logger.info("widen_scope attempt 2: all filters and anchor removed, core keyword only")

    else:
        intent["skip_postgres_filter"] = True
        intent["full_faiss_scan"] = True
        logger.info("widen_scope attempt 3: skip_postgres_filter and full_faiss_scan enabled")

    updates["parsed_intent"] = intent
    updates["attempt_count"] = attempt + 1
    return updates


def node_rerank_cross_encoder(state: EchoState) -> dict[str, Any]:
    """Node 6 — cross-encoder re-ranking. See reranker.rerank_cross_encoder."""
    logger.info("NODE: rerank_cross_encoder")
    return rerank_cross_encoder(state)


def node_extend_neighborhood(state: EchoState) -> dict[str, Any]:
    """Node 7 — neighborhood extension. See neighborhood.extend_neighborhood."""
    logger.info("NODE: extend_neighborhood")
    return extend_neighborhood(state)


def node_fetch_attachment(state: EchoState) -> dict[str, Any]:
    """
    fetch_attachment — run when Luna set read_attachment on the search.

    On-demand Tier-2 extraction (architecture §8.3): for the highest-ranked
    Gmail result in the top 3 that carries attachments, fetch the binary from
    the Gmail API, extract text (PDF/DOCX/TXT/OCR), and cache it in Redis for
    1 hour. Returns the text and the memory_id it belongs to, so the result
    text can name the item's number.
    """
    logger.info("NODE: fetch_attachment")
    from enp.attachment_processor import fetch_attachment_text_for_memory

    query_clean = state.get("parsed_intent", {}).get("query_clean", "")
    for result in state.get("ranked_results", [])[:3]:
        if result.get("source_type") == "gmail" and result.get("has_attachments"):
            content = fetch_attachment_text_for_memory(
                str(result["memory_id"]), query_clean=query_clean
            )
            if content:
                logger.info(
                    "NODE: fetch_attachment — extracted %d chars for %s",
                    len(content), result["memory_id"],
                )
                return {"attachment_content": content, "attachment_item_id": str(result["memory_id"])}

    logger.info("NODE: fetch_attachment — no extractable attachment in top 3")
    return {"attachment_content": None, "attachment_item_id": None}


def node_no_results_found(state: EchoState) -> dict[str, Any]:
    """
    no_results_found — the canned reply when the searches found nothing and Luna wrote no answer.

    Returns a friendly chat message (A27 style): final_answer, answer_markdown,
    answer_sources (empty), no_results=True.
    By now the intent has been widened (filters and anchor dropped), so the
    message speaks about the original question, not the widened filters.
    """
    logger.info("NODE: no_results_found")
    query = state.get("user_query", "")
    message = (
        f"I couldn't find anything about \"{query}\" in your emails, pages or videos — "
        "even after widening the search (longer time range, all sources, no time anchor).\n\n"
        "A few things that might help:\n"
        "- try a shorter or different keyword (a name, a site, a subject word)\n"
        "- leave out the date if you're not sure when it happened\n"
        "- if it's very recent, it may still be processing — try again in a minute"
    )
    return {"final_answer": message, "answer_markdown": message, "answer_sources": [], "no_results": True}
