"""
parse_intent node — LLM Call 1.

Receives the user query and full conversation history, constructs a structured
prompt with few-shot examples, calls the configured LLM provider, and parses
the JSON response into a ParsedIntent dict.

Extended schema (hybrid retrieval redesign):
  - query_variants: query_clean plus two semantic variations/synonyms, produced
    in the SAME LLM call — query expansion costs zero extra API calls.
  - time_anchor_query / time_relation: deterministic time-anchor resolution for
    queries like "OS material after the interview email".

Error contract: on any LLM or parse failure this function returns a safe
fallback ParsedIntent with is_ambiguous=True. The graph never crashes here.
"""
import json
import logging
from datetime import date
from typing import Any

from langchain_core.messages import BaseMessage, HumanMessage

from rse.config import LLM_CONFIG
from rse.state import EchoState, ParsedIntent

logger = logging.getLogger(__name__)

# ── Few-shot examples ────────────────────────────────────────────────────────
_FEW_SHOT_EXAMPLES = """
EXAMPLES — study these carefully before answering:

Query: "find my TechCorp interview email"
Output: {"sources":["gmail"],"time_filter":null,"fetch_attachment":false,"fetch_api":false,"query_clean":"TechCorp interview","query_variants":["TechCorp interview","TechCorp job interview invitation","interview schedule TechCorp"],"time_anchor_query":null,"time_relation":null,"scope_level":0,"is_ambiguous":false,"original_query":"find my TechCorp interview email","skip_postgres_filter":false,"full_faiss_scan":false}

Query: "Chrome pages about operating systems I read yesterday"
Output: {"sources":["chrome"],"time_filter":"{yesterday}","fetch_attachment":false,"fetch_api":false,"query_clean":"operating systems","query_variants":["operating systems","OS concepts process scheduling","computer operating system fundamentals"],"time_anchor_query":null,"time_relation":null,"scope_level":0,"is_ambiguous":false,"original_query":"Chrome pages about operating systems I read yesterday","skip_postgres_filter":false,"full_faiss_scan":false}

Query: "OS material I looked at after the interview email"
Output: {"sources":["gmail","chrome","youtube"],"time_filter":null,"fetch_attachment":false,"fetch_api":false,"query_clean":"operating systems","query_variants":["operating systems","OS concepts scheduling memory management","operating system tutorial"],"time_anchor_query":"interview email","time_relation":"after","scope_level":0,"is_ambiguous":false,"original_query":"OS material I looked at after the interview email","skip_postgres_filter":false,"full_faiss_scan":false}

Query: "get the full PDF from the offer letter email"
Output: {"sources":["gmail"],"time_filter":null,"fetch_attachment":true,"fetch_api":false,"query_clean":"offer letter","query_variants":["offer letter","job offer document","employment offer email"],"time_anchor_query":null,"time_relation":null,"scope_level":0,"is_ambiguous":false,"original_query":"get the full PDF from the offer letter email","skip_postgres_filter":false,"full_faiss_scan":false}

Query: "everything I studied about machine learning this week including emails videos and articles"
Output: {"sources":["gmail","chrome","youtube"],"time_filter":"{week_start}","fetch_attachment":false,"fetch_api":false,"query_clean":"machine learning","query_variants":["machine learning","ML models neural networks","machine learning tutorial course"],"time_anchor_query":null,"time_relation":null,"scope_level":0,"is_ambiguous":false,"original_query":"everything I studied about machine learning this week including emails videos and articles","skip_postgres_filter":false,"full_faiss_scan":false}

Query: "what did I browse before the flight booking confirmation"
Output: {"sources":["chrome"],"time_filter":null,"fetch_attachment":false,"fetch_api":false,"query_clean":"browsing","query_variants":["browsing","web pages articles","websites visited"],"time_anchor_query":"flight booking confirmation","time_relation":"before","scope_level":0,"is_ambiguous":false,"original_query":"what did I browse before the flight booking confirmation","skip_postgres_filter":false,"full_faiss_scan":false}

Query: "did I get any emails from Google today"
Output: {"sources":["gmail"],"time_filter":"{today}","fetch_attachment":false,"fetch_api":true,"query_clean":"Google","query_variants":["Google","Google email notification","message from Google"],"time_anchor_query":null,"time_relation":null,"scope_level":0,"is_ambiguous":false,"original_query":"did I get any emails from Google today","skip_postgres_filter":false,"full_faiss_scan":false}

Query: "xkq8z"
Output: {"sources":["gmail","chrome","youtube"],"time_filter":null,"fetch_attachment":false,"fetch_api":false,"query_clean":"xkq8z","query_variants":["xkq8z"],"time_anchor_query":null,"time_relation":null,"scope_level":0,"is_ambiguous":true,"original_query":"xkq8z","skip_postgres_filter":false,"full_faiss_scan":false}
"""

# ── System prompt ────────────────────────────────────────────────────────────
_SYSTEM_PROMPT = """You are Echo's query parser. Your ONLY job is to convert a user query into a valid JSON object.

OUTPUT RULES — read carefully:
- Output ONLY a single JSON object. No preamble, no explanation, no markdown fences, no trailing text.
- Every field listed in the schema below must be present in your output.
- Do not add any fields not in the schema.
- Today's date is {today}.

PARSED INTENT SCHEMA:
{{
  "sources":             ["gmail" | "chrome" | "youtube"],  // array — include all relevant, or all three if query is broad
  "time_filter":         "ISO-8601 date string" | null,      // e.g. "2026-04-18" — only for explicit ABSOLUTE time references
  "fetch_attachment":    true | false,                       // true ONLY if user says: PDF, document, file, attachment, open fully, read the file
  "fetch_api":           true | false,                       // true ONLY if user says: latest, today, just received, right now
  "query_clean":         "string",                           // core topic stripped of meta-language (remove: find, show, search for, etc.)
  "query_variants":      ["string", "string", "string"],     // EXACTLY: query_clean first, then TWO semantic variations/synonyms of the topic
  "time_anchor_query":   "string" | null,                    // when the time reference is RELATIVE TO ANOTHER SAVED ITEM ("after the interview email"), the phrase describing that anchor item
  "time_relation":       "after" | "before" | null,          // temporal direction relative to the anchor item; null when time_anchor_query is null
  "scope_level":         0,                                  // ALWAYS set to 0 — widen_scope node manages this
  "is_ambiguous":        true | false,                       // true if query is unintelligible, single character, or unresolvable even with history
  "original_query":      "string",                           // verbatim copy of the user query
  "skip_postgres_filter": false,                             // ALWAYS set to false — widen_scope node manages this
  "full_faiss_scan":     false                               // ALWAYS set to false — widen_scope node manages this
}}

CLASSIFICATION RULES:
- sources: Use ["gmail"] for email-specific queries. Use ["chrome"] for web page / article queries. Use ["youtube"] for video queries. Use ["gmail","chrome","youtube"] for broad or unspecified queries.
- time_filter: Convert relative dates to ISO-8601 using today={today}. "yesterday" → one day before today. "this week" → Monday of current week. "last month" → first day of last month. Set null for queries with no time reference.
- time_anchor_query: Use ONLY when the user positions the search relative to another remembered item ("after the interview email", "before that offer letter", "since the flight confirmation"). Extract the shortest phrase identifying the anchor item. This is DIFFERENT from time_filter — never set both from the same phrase.
- query_variants: The first element is query_clean verbatim. The second and third are short semantic rephrasings using synonyms or closely related terms — they widen semantic recall. For ambiguous queries output query_clean alone.
- fetch_attachment: Only for explicit file-content requests. "find the email about the offer" → false. "open the PDF in the offer email" → true.
- fetch_api: Only when the user explicitly needs live/fresh data they expect to have arrived recently.
- query_clean: Extract the semantic core. "find YouTube videos about neural networks from last week" → "neural networks". "OS material after the interview email" → "operating systems".
- is_ambiguous: Set true only when the query is completely unintelligible. Short queries like "OS email" are NOT ambiguous.

CONVERSATION HISTORY (most recent turns listed first):
{history}

{few_shot_examples}

Now parse the following query:
Query: "{user_query}"
Output:"""


def _build_provider_llm() -> Any:
    """
    Instantiate the parser LLM via LangChain's universal init_chat_model factory.

    Reads provider + parser_model from LLM_CONFIG. API keys are read from
    environment variables by the integration package (e.g. GOOGLE_API_KEY,
    OPENAI_API_KEY, ANTHROPIC_API_KEY). To add a new provider, install its
    langchain integration package, set the right env var, and update LLM_CONFIG.

    Returns:
        A LangChain chat model instance ready to invoke.
    """
    from langchain.chat_models import init_chat_model
    return init_chat_model(
        model=LLM_CONFIG["parser_model"],
        model_provider=LLM_CONFIG["provider"],
        temperature=LLM_CONFIG.get("parser_temperature", 0.0),
    )


def _fallback_intent(original_query: str) -> ParsedIntent:
    """Return a safe fallback ParsedIntent when parsing fails."""
    return ParsedIntent(
        sources=["gmail", "chrome", "youtube"],
        time_filter=None,
        fetch_attachment=False,
        fetch_api=False,
        query_clean=original_query,
        query_variants=[original_query],
        time_anchor_query=None,
        time_relation=None,
        scope_level=0,
        is_ambiguous=True,
        original_query=original_query,
        skip_postgres_filter=False,
        full_faiss_scan=False,
    )


def _normalise_intent(parsed: dict, original_query: str) -> ParsedIntent:
    """
    Validate and repair the LLM's JSON so downstream nodes never see malformed
    intent fields. Raises ValueError only when core keys are missing.
    """
    required_keys = {
        "sources", "time_filter", "fetch_attachment", "fetch_api",
        "query_clean", "is_ambiguous", "original_query",
    }
    missing = required_keys - parsed.keys()
    if missing:
        raise ValueError(f"LLM response missing keys: {missing}")

    query_clean = str(parsed.get("query_clean") or original_query).strip()

    # query_variants: guarantee non-empty list of unique strings, query_clean first.
    raw_variants = parsed.get("query_variants") or []
    variants: list[str] = []
    for candidate in [query_clean, *raw_variants]:
        text = str(candidate or "").strip()
        if text and text.lower() not in {v.lower() for v in variants}:
            variants.append(text)
    if not variants:
        variants = [query_clean or original_query]

    # time_relation only makes sense alongside an anchor query.
    anchor_query = parsed.get("time_anchor_query") or None
    relation = parsed.get("time_relation") or None
    if relation not in ("after", "before"):
        relation = None
    if not anchor_query:
        relation = None
    if not relation:
        anchor_query = None

    return ParsedIntent(
        sources=list(parsed.get("sources") or ["gmail", "chrome", "youtube"]),
        time_filter=parsed.get("time_filter") or None,
        fetch_attachment=bool(parsed.get("fetch_attachment", False)),
        fetch_api=bool(parsed.get("fetch_api", False)),
        query_clean=query_clean,
        query_variants=variants[:3],
        time_anchor_query=anchor_query,
        time_relation=relation,
        scope_level=0,
        is_ambiguous=bool(parsed.get("is_ambiguous", False)),
        original_query=original_query,
        skip_postgres_filter=False,
        full_faiss_scan=False,
    )


def _format_history(messages: list[BaseMessage]) -> str:
    """Render conversation history as readable text for the prompt."""
    if not messages:
        return "(no prior conversation)"
    lines = []
    for msg in messages[-10:]:  # last 10 messages to keep prompt bounded
        role = "User" if isinstance(msg, HumanMessage) else "Assistant"
        lines.append(f"{role}: {msg.content}")
    return "\n".join(lines)


def parse_intent(state: EchoState) -> dict:
    """
    LLM Call 1: parse the user query into a structured ParsedIntent.

    Loads conversation history, builds a few-shot prompt, calls the configured
    LLM, and parses the JSON response. Falls back gracefully on any error.

    Args:
        state: Current EchoState. Reads user_query and conversation_history.

    Returns:
        Partial state dict with updated parsed_intent key.
    """
    user_query: str = state.get("user_query", "")
    conversation_history: list = state.get("conversation_history", [])

    today_str = date.today().isoformat()
    history_text = _format_history(conversation_history)

    prompt_text = _SYSTEM_PROMPT.format(
        today=today_str,
        history=history_text,
        few_shot_examples=_FEW_SHOT_EXAMPLES,
        user_query=user_query,
    )

    logger.info("parse_intent: calling LLM for query=%r", user_query)

    try:
        llm = _build_provider_llm()
        response = llm.invoke([HumanMessage(content=prompt_text)])
        raw_text: str = response.content.strip()

        # Strip accidental markdown fences if the model adds them
        if raw_text.startswith("```"):
            raw_text = raw_text.split("```")[1]
            if raw_text.startswith("json"):
                raw_text = raw_text[4:]
            raw_text = raw_text.strip()

        parsed: dict = json.loads(raw_text)
        intent = _normalise_intent(parsed, user_query)

        logger.info(
            "parse_intent: success — sources=%s query_clean=%r variants=%d anchor=%r",
            intent.get("sources"),
            intent.get("query_clean"),
            len(intent.get("query_variants", [])),
            intent.get("time_anchor_query"),
        )
        return {"parsed_intent": intent}

    except (json.JSONDecodeError, ValueError, KeyError) as exc:
        logger.error("parse_intent: JSON parse error — %s. Raw: %r", exc, locals().get("raw_text", "N/A"))
        return {"parsed_intent": _fallback_intent(user_query)}

    except Exception as exc:
        logger.error("parse_intent: LLM call failed — %s", exc)
        return {"parsed_intent": _fallback_intent(user_query)}
