"""
Search-argument checker — turns one search_memories tool call into a ParsedIntent.

Until A26 this module held LLM Call 1 (parse_intent): a prompt, few-shot examples and
a JSON reply parsed out of free text. With the agent loop Luna passes the same fields
as the arguments of a tool call (query, alternative_phrasings, sources, since,
anchor_item, anchor_relation, read_attachment), so that separate call is gone. What
stays is the checking: the arguments are still model output and can be wrong (an
unknown source, a broken date, the string "false", duplicate variants, an empty query),
so every set of arguments passes through intent_from_tool_args before any search runs.

The ParsedIntent it returns has exactly the shape the search stages already read
(rse/search_coordinator.py, rse/fusion.py, rse/time_anchor.py, widen_scope), so none
of them changed. Fields that code owns — original_query and the widen flags
skip_postgres_filter / full_faiss_scan — are set here, never asked of the model.

Error contract: intent_from_tool_args raises ValueError when the call is unusable
(no query); the caller reports that to Luna as the tool result. fallback_intent is the
safe intent for when the agent call itself failed. Logs carry flags and counts only —
never the query.
"""
import logging
from datetime import datetime
from typing import Any, Optional

from rse.state import ParsedIntent

logger = logging.getLogger(__name__)

_VALID_SOURCES: tuple[str, ...] = ("gmail", "chrome", "youtube")

# The query is a few keywords; anything longer is a pasted question and is cut.
_MAX_QUERY_CHARS = 300
# query_clean plus two rephrasings (the same three variants the parser used to make).
_MAX_VARIANTS = 3


def _intent(
    original_query: str,
    *,
    sources: list[str],
    time_filter: Optional[str],
    fetch_attachment: bool,
    fetch_api: bool,
    query_clean: str,
    query_variants: list[str],
    time_anchor_query: Optional[str],
    time_relation: Optional[str],
    is_ambiguous: bool,
) -> ParsedIntent:
    """Build a ParsedIntent; code-owned fields are always set here."""
    return ParsedIntent(
        sources=sources,
        time_filter=time_filter,
        fetch_attachment=fetch_attachment,
        fetch_api=fetch_api,
        query_clean=query_clean,
        query_variants=query_variants,
        time_anchor_query=time_anchor_query,
        time_relation=time_relation,
        is_ambiguous=is_ambiguous,
        original_query=original_query,
        skip_postgres_filter=False,
        full_faiss_scan=False,
    )


def fallback_intent(original_query: str) -> ParsedIntent:
    """Safe intent when the agent call failed: all sources, the raw question, no filters."""
    return _intent(
        original_query,
        sources=list(_VALID_SOURCES),
        time_filter=None,
        fetch_attachment=False,
        fetch_api=False,
        query_clean=original_query,
        query_variants=[original_query],
        time_anchor_query=None,
        time_relation=None,
        is_ambiguous=True,
    )


def _as_bool(value: Any) -> bool:
    """True only for a real true; the string "false" must not become True."""
    if isinstance(value, str):
        return value.strip().lower() == "true"
    return value is True


def _valid_time_filter(value: Any) -> Optional[str]:
    """Keep the time filter only if it is a real ISO date/datetime; else drop it."""
    if not value:
        return None
    text = str(value).strip()
    try:
        datetime.fromisoformat(text)
    except ValueError:
        logger.warning("intent_from_tool_args: dropped invalid since (len=%d)", len(text))
        return None
    return text


def intent_from_tool_args(args: Any, original_query: str) -> ParsedIntent:
    """
    Validate and repair one search_memories call so no search stage ever sees a
    malformed field.

    Args:
        args: The tool call's arguments as Luna sent them.
        original_query: The user's verbatim question (kept on the intent for logs).

    Returns:
        A ParsedIntent for run_search.

    Raises:
        ValueError: args is not a mapping or has no usable query.
    """
    if not isinstance(args, dict):
        raise ValueError("the arguments must be an object with at least a query")

    query_clean = " ".join(str(args.get("query") or "").split())[:_MAX_QUERY_CHARS]
    if not query_clean:
        raise ValueError("query is required: give a few keywords for what to look for")

    # query_variants: unique strings, query first, at most three in all.
    raw_phrasings = args.get("alternative_phrasings") or []
    if not isinstance(raw_phrasings, list):
        raw_phrasings = [raw_phrasings]
    variants: list[str] = []
    for candidate in [query_clean, *raw_phrasings]:
        text = " ".join(str(candidate or "").split())[:_MAX_QUERY_CHARS]
        if text and text.lower() not in {v.lower() for v in variants}:
            variants.append(text)

    # sources: only known names, in a fixed order; anything else (or nothing) → all three.
    raw_sources = args.get("sources") or []
    if not isinstance(raw_sources, list):
        raw_sources = [raw_sources]
    wanted = {str(source).strip().lower() for source in raw_sources}
    sources = [source for source in _VALID_SOURCES if source in wanted]

    # An anchor needs both its item phrase and a direction; one without the other is dropped.
    anchor_query = " ".join(str(args.get("anchor_item") or "").split()) or None
    relation = str(args.get("anchor_relation") or "").strip().lower() or None
    if relation not in ("after", "before") or not anchor_query:
        relation = None
        anchor_query = None

    return _intent(
        original_query,
        sources=sources or list(_VALID_SOURCES),
        time_filter=_valid_time_filter(args.get("since")),
        fetch_attachment=_as_bool(args.get("read_attachment", False)),
        fetch_api=False,
        query_clean=query_clean,
        query_variants=variants[:_MAX_VARIANTS],
        time_anchor_query=anchor_query,
        time_relation=relation,
        is_ambiguous=False,
    )
