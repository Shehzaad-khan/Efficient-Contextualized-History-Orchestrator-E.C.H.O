"""
<<<<<<< HEAD
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
=======
All 9 LangGraph node implementations for the RSE pipeline.

This module is the single import point for the graph assembly in
retrieval_engine.py. Nodes that are fully implemented this phase:
  - parse_intent    (LLM Call 1 — query_parser.py)
  - postgres_search (SQL — search_coordinator.py)
  - faiss_search    (stub — search_coordinator.py)
  - evaluate_quality (unconditionally returns 'strong' this phase)
  - widen_scope     (full logic from architecture Section 10.3 Node 5)
  - check_attachments (routing signal logic)
  - fetch_attachment (stub)
  - synthesize      (stub)
  - no_results_found (structured message)
"""
import logging
from datetime import datetime, timedelta

from rse.state import EchoState
from rse.query_parser import parse_intent
from rse.search_coordinator import postgres_search, faiss_search
>>>>>>> 4842d1a3060d3dea11ed107e06e4212e96c74fb4

logger = logging.getLogger(__name__)


<<<<<<< HEAD
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
=======
# ── Re-export real implementations ──────────────────────────────────────────
# parse_intent and the two search nodes live in their own modules and are
# imported here so retrieval_engine.py only needs to import from graph_nodes.

def node_parse_intent(state: EchoState) -> dict:
    """
    Node 1 — parse_intent.

    LLM Call 1: parses the user query and conversation history into a
    structured ParsedIntent JSON object. Calls Gemini 2.5 Flash (or the
    configured provider). Falls back gracefully on parse failure.

    Args:
        state: EchoState carrying user_query and conversation_history.

    Returns:
        Partial state dict with parsed_intent.
    """
    logger.info("NODE: parse_intent")
    return parse_intent(state)


def node_postgres_search(state: EchoState) -> dict:
    """
    Node 2 — postgres_search.

    Executes parameterised SQL against Neon PostgreSQL. Dynamically applies
    source, time, and keyword filters from parsed_intent.

    Args:
        state: EchoState carrying parsed_intent.

    Returns:
        Partial state dict with postgres_results.
    """
    logger.info("NODE: postgres_search")
    return postgres_search(state)


def node_faiss_search(state: EchoState) -> dict:
    """
    Node 3 — faiss_search.

    Semantic similarity search stub. Returns empty faiss_results this phase.
    Real implementation requires Mir's faiss_manager and ENP embeddings.

    Args:
        state: EchoState carrying parsed_intent and postgres_results.

    Returns:
        Partial state dict with faiss_results (empty list this phase).
    """
    logger.info("NODE: faiss_search")
    return faiss_search(state)


def node_evaluate_quality(state: EchoState) -> dict:
    """
    Node 4 — evaluate_quality.

    Deterministic quality check with five sequential checks. This phase stub
    unconditionally sets result_quality='strong' so the graph can be tested
    end-to-end before real embeddings exist.

    Real logic (Phase 3):
      1. Empty check: len(merged_results) == 0 → 'empty'
      2. Source match: requested source not in results → 'empty'
      3. Time window: no results within the requested window → 'weak'
      4. Minimum count: fewer than 2 results → 'weak'
      5. Top result similarity: results[0].similarity_score < 0.35 → 'weak'
      Otherwise → 'strong'

    Args:
        state: EchoState carrying postgres_results and faiss_results.

    Returns:
        Partial state dict with result_quality='strong'.
    """
    logger.info("NODE: evaluate_quality (stub — unconditionally strong)")
    return {"result_quality": "strong"}


def node_widen_scope(state: EchoState) -> dict:
    """
    Node 5 — widen_scope.

    Called when evaluate_quality returns weak/empty and attempt_count < 3.
    Each attempt widens one parameter and the graph loops back to postgres_search.

    Attempt 1: widen time window by 4 days, or open all sources if no time filter.
    Attempt 2: remove time filter, open all sources, keep only first keyword.
    Attempt 3: set skip_postgres_filter=True and full_faiss_scan=True.

    Args:
        state: EchoState carrying parsed_intent and attempt_count.

    Returns:
        Partial state dict with updated parsed_intent and incremented attempt_count.
    """
    logger.info("NODE: widen_scope")
    attempt = state.get("attempt_count", 0)
    intent = dict(state.get("parsed_intent", {}))

    if attempt == 0:
        # Attempt 1: widen time window or open sources
        if intent.get("time_filter"):
            try:
                original_dt = datetime.fromisoformat(intent["time_filter"])
                intent["time_filter"] = (original_dt - timedelta(days=4)).isoformat()
                logger.info("widen_scope attempt 1: time window extended by 4 days")
            except ValueError:
                intent["time_filter"] = None
                logger.info("widen_scope attempt 1: invalid time_filter cleared")
        elif intent.get("sources") not in [["gmail", "chrome", "youtube"], ["all"]]:
            intent["sources"] = ["gmail", "chrome", "youtube"]
            logger.info("widen_scope attempt 1: sources opened to all")
        else:
            # Sources already open, trim query to first keyword
            query_clean = intent.get("query_clean", "")
            intent["query_clean"] = query_clean.split()[0] if query_clean else query_clean
            logger.info("widen_scope attempt 1: query trimmed to first keyword")

    elif attempt == 1:
        # Attempt 2: remove all filters, core keyword only
        intent["time_filter"] = None
        intent["sources"] = ["gmail", "chrome", "youtube"]
        query_clean = intent.get("query_clean", "")
        intent["query_clean"] = query_clean.split()[0] if query_clean else query_clean
        logger.info("widen_scope attempt 2: all filters removed, core keyword only")

    elif attempt >= 2:
        # Attempt 3: bypass postgres filter, full FAISS scan
        intent["skip_postgres_filter"] = True
        intent["full_faiss_scan"] = True
        logger.info("widen_scope attempt 3: skip_postgres_filter and full_faiss_scan enabled")

    return {
        "parsed_intent": intent,
        "attempt_count": attempt + 1,
    }


def node_check_attachments(state: EchoState) -> dict:
    """
    Node 6 — check_attachments.

    Inspects top-3 results for attachment presence. Routing is handled by
    route_after_check_attachments in graph_routing.py. This node itself is a
    pure pass-through — its only role is to be a named routing point.

    Args:
        state: EchoState carrying postgres_results and parsed_intent.

    Returns:
        Unchanged state (empty dict — no fields to update).
    """
    logger.info("NODE: check_attachments")
    return {}


def node_fetch_attachment(state: EchoState) -> dict:
    """
    Node 7 — fetch_attachment.

    STUB for this phase.

    Real implementation (Phase 3): calls Gmail API to fetch attachment binary,
    extracts text using PyPDF2/pdfplumber, caches result in Redis (1-hour TTL).
    Binary file is never stored permanently.

    Args:
        state: EchoState carrying postgres_results and parsed_intent.

    Returns:
        Partial state dict with attachment_content=None (stub).
    """
    logger.info("NODE: fetch_attachment (stub)")
    return {"attachment_content": None}


def node_synthesize(state: EchoState) -> dict:
    """
    Node 8 — synthesize.

    STUB for this phase.

    Real implementation (Phase 3, LLM Call 2): assembles context from top-10
    re-ranked results, attachment_content, and conversation history. Calls the
    configured synthesizer LLM to generate a readable answer with source
    citations and temporal context.

    Args:
        state: EchoState carrying postgres_results, faiss_results,
               attachment_content, and conversation_history.

    Returns:
        Partial state dict with final_answer placeholder.
    """
    logger.info("NODE: synthesize (stub)")
    postgres_results = state.get("postgres_results", [])
    count = len(postgres_results)
    query = state.get("user_query", "")
    final_answer = (
        f"[STUB] Retrieved {count} candidate(s) for query: '{query}'. "
        "Synthesis will be implemented in Phase 3."
    )
    return {"final_answer": final_answer, "no_results": False}


def node_no_results_found(state: EchoState) -> dict:
    """
    Node 9 — no_results_found.

    Triggered after 3 failed widen_scope attempts. Returns a structured
    descriptive message explaining what was searched and suggesting alternatives.

    Args:
        state: EchoState carrying parsed_intent and attempt_count.

    Returns:
        Partial state dict with final_answer and no_results=True.
    """
    logger.info("NODE: no_results_found")
    intent = state.get("parsed_intent", {})
    query = intent.get("original_query", state.get("user_query", ""))
    sources = intent.get("sources", [])
    time_filter = intent.get("time_filter")

    sources_str = ", ".join(sources) if sources else "all sources"
    time_str = f" from around {time_filter}" if time_filter else ""

    message = (
        f"No results found for '{query}'{time_str} across {sources_str}. "
        "Echo searched progressively broader filters across 3 attempts. "
        "Suggestions: try a shorter keyword, remove the time constraint, "
        "or check that the relevant content has been ingested and processed."
    )
    return {"final_answer": message, "no_results": True}
>>>>>>> 4842d1a3060d3dea11ed107e06e4212e96c74fb4
