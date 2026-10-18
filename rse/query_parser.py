"""
parse_intent node — LLM Call 1.

Receives the user query and recent conversation history, builds a prompt with
few-shot examples, calls the configured LLM, and turns the JSON reply into a
ParsedIntent dict that the deterministic search nodes run on.

The LLM fills only the fields it can actually judge (sources, time filter,
attachment/API flags, cleaned query + 2 variants, time anchor, ambiguity).
Fields that code owns — original_query and the widen flags
skip_postgres_filter / full_faiss_scan — are set here, not asked of the model,
so they cost no output tokens and cannot be got wrong.

Extended schema (hybrid retrieval redesign):
  - query_variants: query_clean plus two semantic variations/synonyms, produced
    in the SAME LLM call — query expansion costs zero extra API calls.
  - time_anchor_query / time_relation: deterministic time-anchor resolution for
    queries like "OS material after the interview email" (rse/time_anchor.py).

Error contract: on any LLM or parse failure this function returns a safe
fallback ParsedIntent with is_ambiguous=True. The graph never crashes here.
Logs carry flags, counts and timings only — never the query or model output.
"""
import json
import logging
import time
from datetime import date, datetime, timedelta
from typing import Any, Optional

from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage

from rse.llm_factory import build_chat_model
from rse.state import EchoState, ParsedIntent

logger = logging.getLogger(__name__)

_VALID_SOURCES: tuple[str, ...] = ("gmail", "chrome", "youtube")

# History sent to the parser: it only needs the topic of earlier turns to
# resolve "that topic", so long answers are cut.
_HISTORY_MESSAGES = 10
_HISTORY_CHARS_PER_MESSAGE = 500

# ── Few-shot examples ────────────────────────────────────────────────────────
# Date tokens (<TODAY>, <YESTERDAY>, <WEEK_START>) are replaced with real dates
# on every call. (They used to be "{yesterday}"-style placeholders that were
# never filled in, so the model was shown literal "{yesterday}" as a correct
# time_filter — which then reached SQL as a timestamp and broke the search.)
_FEW_SHOT_EXAMPLES = """
EXAMPLES — study these carefully before answering:

Query: "find my TechCorp interview email"
Output: {"sources":["gmail"],"time_filter":null,"fetch_attachment":false,"fetch_api":false,"query_clean":"TechCorp interview","query_variants":["TechCorp interview","TechCorp job interview invitation","interview schedule TechCorp"],"time_anchor_query":null,"time_relation":null,"is_ambiguous":false}

Query: "Chrome pages about operating systems I read yesterday"
Output: {"sources":["chrome"],"time_filter":"<YESTERDAY>","fetch_attachment":false,"fetch_api":false,"query_clean":"operating systems","query_variants":["operating systems","OS concepts process scheduling","computer operating system fundamentals"],"time_anchor_query":null,"time_relation":null,"is_ambiguous":false}

Query: "OS material I looked at after the interview email"
Output: {"sources":["gmail","chrome","youtube"],"time_filter":null,"fetch_attachment":false,"fetch_api":false,"query_clean":"operating systems","query_variants":["operating systems","OS concepts scheduling memory management","operating system tutorial"],"time_anchor_query":"interview email","time_relation":"after","is_ambiguous":false}

Query: "get the full PDF from the offer letter email"
Output: {"sources":["gmail"],"time_filter":null,"fetch_attachment":true,"fetch_api":false,"query_clean":"offer letter","query_variants":["offer letter","job offer document","employment offer email"],"time_anchor_query":null,"time_relation":null,"is_ambiguous":false}

Query: "everything I studied about machine learning this week including emails videos and articles"
Output: {"sources":["gmail","chrome","youtube"],"time_filter":"<WEEK_START>","fetch_attachment":false,"fetch_api":false,"query_clean":"machine learning","query_variants":["machine learning","ML models neural networks","machine learning tutorial course"],"time_anchor_query":null,"time_relation":null,"is_ambiguous":false}

Query: "what did I browse before the flight booking confirmation"
Output: {"sources":["chrome"],"time_filter":null,"fetch_attachment":false,"fetch_api":false,"query_clean":"browsing","query_variants":["browsing","web pages articles","websites visited"],"time_anchor_query":"flight booking confirmation","time_relation":"before","is_ambiguous":false}

Query: "did I get any emails from Google today"
Output: {"sources":["gmail"],"time_filter":"<TODAY>","fetch_attachment":false,"fetch_api":true,"query_clean":"Google","query_variants":["Google","Google email notification","message from Google"],"time_anchor_query":null,"time_relation":null,"is_ambiguous":false}

Query: "xkq8z"
Output: {"sources":["gmail","chrome","youtube"],"time_filter":null,"fetch_attachment":false,"fetch_api":false,"query_clean":"xkq8z","query_variants":["xkq8z"],"time_anchor_query":null,"time_relation":null,"is_ambiguous":true}
"""

# ── System prompt (instructions; the history + query go in the user message) ─
_SYSTEM_PROMPT = """You are Echo's query parser. Your ONLY job is to convert a user query into a valid JSON object.

OUTPUT RULES — read carefully:
- Output ONLY a single JSON object. No preamble, no explanation, no markdown fences, no trailing text.
- Every field listed in the schema below must be present in your output.
- Do not add any fields not in the schema.
- Today's date is <TODAY>.

PARSED INTENT SCHEMA:
{
  "sources":           ["gmail" | "chrome" | "youtube"],  // array — include all relevant, or all three if query is broad
  "time_filter":       "YYYY-MM-DD" | null,               // START date: keep items from this day onward; null when the query has no time reference
  "fetch_attachment":  true | false,                      // true ONLY if user says: PDF, document, file, attachment, open fully, read the file
  "fetch_api":         true | false,                      // true ONLY if user says: latest, today, just received, right now
  "query_clean":       "string",                          // core topic stripped of meta-language (remove: find, show, search for, etc.)
  "query_variants":    ["string", "string", "string"],    // EXACTLY: query_clean first, then TWO semantic variations/synonyms of the topic
  "time_anchor_query": "string" | null,                   // when the time reference is RELATIVE TO ANOTHER SAVED ITEM ("after the interview email"), the phrase describing that anchor item
  "time_relation":     "after" | "before" | null,         // temporal direction relative to the anchor item; null when time_anchor_query is null
  "is_ambiguous":      true | false                       // true if query is unintelligible, single character, or unresolvable even with history
}

CLASSIFICATION RULES:
- sources: Use ["gmail"] for email-specific queries. Use ["chrome"] for web page / article queries. Use ["youtube"] for video queries. Use ["gmail","chrome","youtube"] for broad or unspecified queries.
- time_filter: Absolute dates as given; convert relative dates using today=<TODAY>. "yesterday" → <YESTERDAY>. "this week" → Monday of the current week (<WEEK_START>). "last month" → first day of last month. null for queries with no time reference.
- time_anchor_query: Use ONLY when the user positions the search relative to another remembered item ("after the interview email", "before that offer letter", "since the flight confirmation"). Extract the shortest phrase identifying the anchor item. This is DIFFERENT from time_filter — never set both from the same phrase.
- query_variants: The first element is query_clean verbatim. The second and third are short semantic rephrasings using synonyms or closely related terms — they widen semantic recall. For ambiguous queries output query_clean alone.
- fetch_attachment: Only for explicit file-content requests. "find the email about the offer" → false. "open the PDF in the offer email" → true.
- fetch_api: Only when the user explicitly needs live/fresh data they expect to have arrived recently.
- query_clean: Extract the semantic core. "find YouTube videos about neural networks from last week" → "neural networks". "OS material after the interview email" → "operating systems".
- is_ambiguous: Set true only when the query is completely unintelligible. Short queries like "OS email" are NOT ambiguous.
- Use the conversation history to resolve references like "that topic" or "the same sender".
"""

_USER_PROMPT = """CONVERSATION HISTORY (oldest first):
{history}

Now parse the following query:
Query: "{user_query}"
Output:"""


def _build_provider_llm() -> Any:
    """The parser chat model (built once; see rse/llm_factory.py for the provider rule)."""
    return build_chat_model("parser_model", "parser_temperature")


def _example_dates(today: date) -> dict[str, str]:
    """Real values for the date tokens used in the prompt and examples."""
    return {
        "<TODAY>": today.isoformat(),
        "<YESTERDAY>": (today - timedelta(days=1)).isoformat(),
        "<WEEK_START>": (today - timedelta(days=today.weekday())).isoformat(),
    }


def _fill_dates(text: str, dates: dict[str, str]) -> str:
    for token, value in dates.items():
        text = text.replace(token, value)
    return text


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


def _fallback_intent(original_query: str) -> ParsedIntent:
    """Safe intent when parsing fails: all sources, raw query, flagged ambiguous."""
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
        logger.warning("parse_intent: dropped invalid time_filter (len=%d)", len(text))
        return None
    return text


def _normalise_intent(parsed: dict[str, Any], original_query: str) -> ParsedIntent:
    """
    Validate and repair the LLM's JSON so downstream nodes never see malformed
    intent fields. Raises ValueError only when core keys are missing.
    """
    required_keys = {"sources", "time_filter", "fetch_attachment", "fetch_api", "query_clean", "is_ambiguous"}
    missing = required_keys - parsed.keys()
    if missing:
        raise ValueError(f"LLM response missing keys: {sorted(missing)}")

    query_clean = str(parsed.get("query_clean") or original_query).strip()

    # query_variants: guarantee non-empty list of unique strings, query_clean first.
    raw_variants = parsed.get("query_variants") or []
    if not isinstance(raw_variants, list):
        raw_variants = [raw_variants]
    variants: list[str] = []
    for candidate in [query_clean, *raw_variants]:
        text = str(candidate or "").strip()
        if text and text.lower() not in {v.lower() for v in variants}:
            variants.append(text)
    if not variants:
        variants = [original_query]

    # sources: only known names; anything else (e.g. "all") → all three.
    raw_sources = parsed.get("sources") or []
    if not isinstance(raw_sources, list):
        raw_sources = [raw_sources]
    sources = [s for s in _VALID_SOURCES if s in {str(x).strip().lower() for x in raw_sources}]

    # time_relation only makes sense alongside an anchor query.
    anchor_query = str(parsed.get("time_anchor_query") or "").strip() or None
    relation = parsed.get("time_relation") or None
    if relation not in ("after", "before") or not anchor_query:
        relation = None
        anchor_query = None

    return _intent(
        original_query,
        sources=sources or list(_VALID_SOURCES),
        time_filter=_valid_time_filter(parsed.get("time_filter")),
        fetch_attachment=_as_bool(parsed.get("fetch_attachment", False)),
        fetch_api=_as_bool(parsed.get("fetch_api", False)),
        query_clean=query_clean,
        query_variants=variants[:3],
        time_anchor_query=anchor_query,
        time_relation=relation,
        is_ambiguous=_as_bool(parsed.get("is_ambiguous", False)),
    )


def _format_history(messages: list[BaseMessage]) -> str:
    """Render the last few turns, oldest first, each cut to a short excerpt."""
    if not messages:
        return "(no prior conversation)"
    lines = []
    for msg in messages[-_HISTORY_MESSAGES:]:
        role = "User" if isinstance(msg, HumanMessage) else "Assistant"
        text = msg.text if isinstance(msg.content, list) else str(msg.content)
        lines.append(f"{role}: {text[:_HISTORY_CHARS_PER_MESSAGE]}")
    return "\n".join(lines)


def _extract_json_object(raw_text: str) -> dict[str, Any]:
    """Take the outermost {...} from the reply (tolerates fences or stray words)."""
    start, end = raw_text.find("{"), raw_text.rfind("}")
    if start == -1 or end <= start:
        raise ValueError("no JSON object in LLM reply")
    parsed = json.loads(raw_text[start:end + 1])
    if not isinstance(parsed, dict):
        raise ValueError("LLM reply JSON is not an object")
    return parsed


def parse_intent(state: EchoState) -> dict[str, Any]:
    """
    LLM Call 1: parse the user query into a structured ParsedIntent.

    Args:
        state: Current EchoState. Reads user_query and conversation_history.

    Returns:
        Partial state dict with the parsed_intent key. Falls back to
        _fallback_intent on any error.
    """
    user_query: str = state.get("user_query", "")
    conversation_history: list[BaseMessage] = state.get("conversation_history", [])

    dates = _example_dates(date.today())
    messages = [
        SystemMessage(content=_fill_dates(_SYSTEM_PROMPT + _FEW_SHOT_EXAMPLES, dates)),
        HumanMessage(content=_USER_PROMPT.format(
            history=_format_history(conversation_history),
            user_query=user_query,
        )),
    ]

    started = time.perf_counter()
    try:
        response = _build_provider_llm().invoke(messages)
        # .text joins the text blocks; Responses API replies are a list of
        # blocks (reasoning + text), so .content is not always a string.
        raw_text = response.text.strip()
    except Exception as exc:
        logger.error("parse_intent: LLM call failed after %.0f ms — %s: %s",
                     (time.perf_counter() - started) * 1000, type(exc).__name__, exc)
        return {"parsed_intent": _fallback_intent(user_query)}

    elapsed_ms = (time.perf_counter() - started) * 1000
    try:
        intent = _normalise_intent(_extract_json_object(raw_text), user_query)
    except (json.JSONDecodeError, ValueError) as exc:
        logger.error("parse_intent: bad JSON from LLM (%s; reply len=%d)", exc, len(raw_text))
        return {"parsed_intent": _fallback_intent(user_query)}

    logger.info(
        "parse_intent: ok in %.0f ms — sources=%s time_filter=%s variants=%d anchor=%s "
        "attachment=%s api=%s ambiguous=%s",
        elapsed_ms,
        intent["sources"],
        intent["time_filter"] is not None,
        len(intent["query_variants"]),
        intent["time_relation"],
        intent["fetch_attachment"],
        intent["fetch_api"],
        intent["is_ambiguous"],
    )
    return {"parsed_intent": intent}
