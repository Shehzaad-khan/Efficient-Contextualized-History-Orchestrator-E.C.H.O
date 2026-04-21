"""
graph_nodes.py — RSE Module
Echo Personal Memory System

All 9 LangGraph node implementations.

Fully implemented:
    parse_intent       — LLM Call 1, structured JSON output
    postgres_search    — dynamic SQL, live Neon data
    widen_scope        — 3-level progressive filter relaxation
    check_attachments  — routing signal, no DB work
    no_results_found   — structured dead-end response

Partially implemented (real logic, stub output):
    evaluate_quality   — returns "strong" unconditionally for Phase 2
                         (real threshold needs FAISS cosine scores)

Stubs (Phase 3):
    faiss_search       — returns empty list until ENP fully indexed
    fetch_attachment   — Redis-cached attachment extraction
    synthesize         — LLM Call 2, formats results for demo readably
"""

import logging
from datetime import datetime
from typing import Any, Dict

from .conversation_memory import load_conversation_history
from .query_parser import parse_user_intent
from .search_coordinator import run_faiss_search, run_postgres_search
from .state import EchoState

logger = logging.getLogger(__name__)


# ── Node 1: parse_intent ──────────────────────────────────────────────────────

def parse_intent(state: EchoState) -> Dict[str, Any]:
    """
    LLM Call 1 — parse the user query into structured ParsedIntent JSON.

    Sends query + conversation history to the configured LLM.
    Returns parsed_intent dict that all downstream nodes depend on.
    Falls back to is_ambiguous=True on any failure.
    """
    query = state.get("user_query", "")
    history = state.get("conversation_history", [])

    logger.info(f"parse_intent: query='{query}'")

    intent = parse_user_intent(query, history)

    return {"parsed_intent": intent}


# ── Node 2: postgres_search ───────────────────────────────────────────────────

def postgres_search(state: EchoState) -> Dict[str, Any]:
    """
    Execute dynamic Postgres SQL from parsed_intent.

    Joins memory_items + memory_engagement + all three source metadata tables.
    Applies source filter, time filter, and keyword conditions.
    Returns up to 1000 candidate rows.
    """
    parsed_intent = state.get("parsed_intent")
    if not parsed_intent:
        logger.warning("postgres_search: no parsed_intent in state — returning empty")
        return {"postgres_results": []}

    results = run_postgres_search(parsed_intent)
    logger.info(f"postgres_search: {len(results)} rows returned")
    return {"postgres_results": results}


# ── Node 3: faiss_search ──────────────────────────────────────────────────────

def faiss_search(state: EchoState) -> Dict[str, Any]:
    """
    FAISS semantic search over embeddings — STUB for Phase 2.

    Will be wired to FAISSManager.search() once ENP has fully indexed
    all existing items. The filtered-search pattern:
      1. postgres_search returns candidate IDs (structural filter)
      2. FAISS re-ranks by 384-dim cosine similarity (semantic filter)

    Current: returns empty list.
    """
    parsed_intent = state.get("parsed_intent", {})
    query_clean = parsed_intent.get("query_clean", "")
    postgres_results = state.get("postgres_results", [])
    full_scan = parsed_intent.get("full_faiss_scan", False)

    candidate_ids = [str(r["memory_id"]) for r in postgres_results if r.get("memory_id")]

    results = run_faiss_search(query_clean, candidate_ids, full_scan=full_scan)
    return {"faiss_results": results}


# ── Node 4: evaluate_quality ──────────────────────────────────────────────────

def evaluate_quality(state: EchoState) -> Dict[str, Any]:
    """
    Evaluate whether the retrieval results are good enough to synthesise.

    Phase 2: Returns "strong" unconditionally when postgres_results is non-empty.
    Returns "empty" when no results at all.

    Phase 3 will compute:
      - Source match ratio (did we find what the user asked for?)
      - FAISS cosine similarity distribution
      - Result count vs. expected count
    """
    postgres_results = state.get("postgres_results", [])
    faiss_results = state.get("faiss_results", [])

    total = len(postgres_results) + len(faiss_results)

    if total == 0:
        logger.info("evaluate_quality: empty — no results from any search")
        return {"result_quality": "empty"}

    # Phase 2: if we have any results, treat as strong
    logger.info(f"evaluate_quality: strong — {total} total results")
    return {"result_quality": "strong"}


# ── Node 5: widen_scope ───────────────────────────────────────────────────────

def widen_scope(state: EchoState) -> Dict[str, Any]:
    """
    Progressive filter relaxation when quality is weak or empty.

    Level 0 (initial): tight — source + time + keyword filters all applied
    Level 1: widen time window (double the range, or add 30 days)
    Level 2: drop time filter entirely — any date
    Level 3: skip all Postgres filters + full FAISS scan

    Max 3 attempts before routing to no_results_found.
    """
    parsed_intent = state.get("parsed_intent", {})
    attempt_count = state.get("attempt_count", 0) + 1
    current_level = parsed_intent.get("scope_level", 0)
    new_level = min(current_level + 1, 3)

    updated_intent = dict(parsed_intent)
    updated_intent["scope_level"] = new_level

    if new_level >= 2:
        updated_intent["time_filter"] = None
        logger.info(f"widen_scope: level {new_level} — dropping time filter")

    if new_level >= 3:
        updated_intent["skip_postgres_filter"] = True
        updated_intent["full_faiss_scan"] = True
        logger.info("widen_scope: level 3 — full scan mode")

    logger.info(f"widen_scope: attempt={attempt_count} scope_level={new_level}")
    return {
        "parsed_intent": updated_intent,
        "attempt_count": attempt_count,
    }


# ── Node 6: check_attachments ─────────────────────────────────────────────────

def check_attachments(state: EchoState) -> Dict[str, Any]:
    """
    Routing signal — does NOT perform any DB work.

    Checks:
      1. Does the user intent request an attachment (fetch_attachment=True)?
      2. Do any of the top-3 postgres results have has_attachments=True?

    If both True: graph routes to fetch_attachment node.
    Otherwise: graph routes directly to synthesize.

    This node leaves state unchanged — routing happens via graph_routing.py.
    """
    parsed_intent = state.get("parsed_intent", {})
    fetch_requested = parsed_intent.get("fetch_attachment", False)

    if not fetch_requested:
        logger.debug("check_attachments: fetch_attachment=False — routing to synthesize")
        return {}

    postgres_results = state.get("postgres_results", [])
    top3 = postgres_results[:3]
    has_attachments_in_top = any(r.get("has_attachments") for r in top3)

    if has_attachments_in_top:
        logger.info("check_attachments: attachment requested + found in top-3 — routing to fetch_attachment")
    else:
        logger.info("check_attachments: attachment requested but none in top-3 — routing to synthesize")

    return {}


# ── Node 7: fetch_attachment ──────────────────────────────────────────────────

def fetch_attachment(state: EchoState) -> Dict[str, Any]:
    """
    Full attachment text extraction — STUB for Phase 3.

    Phase 3 will:
      1. Check Redis cache (1h TTL) for memory_id + attachment_id key
      2. On cache miss: call Gmail API to download attachment bytes
      3. Extract text from PDF/DOCX/TXT using appropriate parser
      4. Cache result in Redis for 1 hour
      5. Return full text as attachment_content in state

    Current: returns None (attachment_content not set).
    """
    logger.debug("fetch_attachment STUB — returning None")
    return {"attachment_content": None}


# ── Node 8: synthesize ────────────────────────────────────────────────────────

def synthesize(state: EchoState) -> Dict[str, Any]:
    """
    LLM Call 2 — synthesise a readable answer from retrieved results.

    Phase 2: Returns a structured text summary of postgres_results.
    Phase 3: Will use LLM to generate a natural language answer with
             source citations, engagement context, and conversation continuity.
    """
    postgres_results = state.get("postgres_results", [])
    parsed_intent = state.get("parsed_intent", {})
    query = state.get("user_query", "")

    if not postgres_results:
        return {
            "final_answer": "No results found matching your query.",
            "no_results": True,
        }

    # Phase 2: Format top results into a readable summary
    sources_label = ", ".join(parsed_intent.get("sources", ["all"]))
    lines = [
        f"Found {len(postgres_results)} result(s) across [{sources_label}] for: \"{query}\"\n"
    ]

    for i, row in enumerate(postgres_results[:10], 1):
        source = row.get("source_type", "unknown")
        title = row.get("title") or row.get("gmail_subject") or "(no title)"
        created = row.get("created_at")
        date_str = created.strftime("%Y-%m-%d") if isinstance(created, datetime) else str(created or "")

        if source == "youtube":
            channel = row.get("channel_name") or ""
            watch = row.get("watch_time_seconds") or 0
            duration = row.get("duration_seconds") or 0
            completion = f"{int(row['completion_rate'] * 100)}%" if row.get("completion_rate") else "?"
            detail = f"Channel: {channel} | Watched: {watch}s / {duration}s ({completion} completion)"
        elif source == "gmail":
            sender = row.get("sender") or ""
            has_attach = "📎" if row.get("has_attachments") else ""
            detail = f"From: {sender} {has_attach}"
        elif source == "chrome":
            domain = row.get("domain") or ""
            scroll = row.get("scroll_depth") or 0
            dwell = row.get("dwell_time_seconds") or 0
            detail = f"Domain: {domain} | Dwell: {dwell}s | Scroll: {int(scroll * 100)}%"
        else:
            detail = ""

        lines.append(f"{i}. [{source.upper()}] {title}\n   {date_str} | {detail}")

    if len(postgres_results) > 10:
        lines.append(f"\n... and {len(postgres_results) - 10} more results.")

    answer = "\n".join(lines)
    logger.info(f"synthesize: formatted {min(len(postgres_results), 10)} results")
    return {"final_answer": answer}


# ── Node 9: no_results_found ──────────────────────────────────────────────────

def no_results_found(state: EchoState) -> Dict[str, Any]:
    """
    Terminal node — all widen attempts exhausted with no results.

    Returns a structured no-results response with the original query
    and suggestions for the user.
    """
    query = state.get("user_query", "")
    parsed_intent = state.get("parsed_intent", {})
    sources = parsed_intent.get("sources", ["all"])
    attempts = state.get("attempt_count", 0)

    answer = (
        f"No results found for: \"{query}\"\n\n"
        f"Searched across: {', '.join(sources)}\n"
        f"Scope widening attempts: {attempts}\n\n"
        "Suggestions:\n"
        "  • Try different keywords\n"
        "  • Broaden the time range\n"
        "  • Check that the Echo extension was active when you browsed\n"
        "  • Verify the backend was running when the content was accessed"
    )

    logger.info(f"no_results_found: all attempts exhausted for query='{query}'")
    return {
        "final_answer": answer,
        "no_results": True,
    }
