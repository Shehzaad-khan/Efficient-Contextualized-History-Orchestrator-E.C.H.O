"""
parse_intent node — LLM Call 1.

Receives the user query and full conversation history, constructs a structured
prompt with few-shot examples, calls Gemini 2.5 Flash (or the configured LLM
provider), and parses the JSON response into a ParsedIntent dict.

Error contract: on any LLM or parse failure this function returns a safe
fallback ParsedIntent with is_ambiguous=True. The graph never crashes here.
"""
import json
import logging
from datetime import date
from typing import Any

from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage

from rse.config import LLM_CONFIG
from rse.state import EchoState, ParsedIntent

logger = logging.getLogger(__name__)

# ── Few-shot examples ────────────────────────────────────────────────────────
_FEW_SHOT_EXAMPLES = """
EXAMPLES — study these carefully before answering:

Query: "find my TechCorp interview email"
Output: {"sources":["gmail"],"time_filter":null,"fetch_attachment":false,"fetch_api":false,"query_clean":"TechCorp interview","scope_level":0,"is_ambiguous":false,"original_query":"find my TechCorp interview email","skip_postgres_filter":false,"full_faiss_scan":false}

Query: "Chrome pages about operating systems I read yesterday"
Output: {"sources":["chrome"],"time_filter":"{yesterday}","fetch_attachment":false,"fetch_api":false,"query_clean":"operating systems","scope_level":0,"is_ambiguous":false,"original_query":"Chrome pages about operating systems I read yesterday","skip_postgres_filter":false,"full_faiss_scan":false}

Query: "get the full PDF from the offer letter email"
Output: {"sources":["gmail"],"time_filter":null,"fetch_attachment":true,"fetch_api":false,"query_clean":"offer letter","scope_level":0,"is_ambiguous":false,"original_query":"get the full PDF from the offer letter email","skip_postgres_filter":false,"full_faiss_scan":false}

Query: "everything I studied about machine learning this week including emails videos and articles"
Output: {"sources":["gmail","chrome","youtube"],"time_filter":"{week_start}","fetch_attachment":false,"fetch_api":false,"query_clean":"machine learning","scope_level":0,"is_ambiguous":false,"original_query":"everything I studied about machine learning this week including emails videos and articles","skip_postgres_filter":false,"full_faiss_scan":false}

Query: "did I get any emails from Google today"
Output: {"sources":["gmail"],"time_filter":"{today}","fetch_attachment":false,"fetch_api":true,"query_clean":"Google","scope_level":0,"is_ambiguous":false,"original_query":"did I get any emails from Google today","skip_postgres_filter":false,"full_faiss_scan":false}

Query: "xkq8z"
Output: {"sources":["gmail","chrome","youtube"],"time_filter":null,"fetch_attachment":false,"fetch_api":false,"query_clean":"xkq8z","scope_level":0,"is_ambiguous":true,"original_query":"xkq8z","skip_postgres_filter":false,"full_faiss_scan":false}
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
  "time_filter":         "ISO-8601 date string" | null,      // e.g. "2026-04-18" — only for explicit time references
  "fetch_attachment":    true | false,                       // true ONLY if user says: PDF, document, file, attachment, open fully, read the file
  "fetch_api":           true | false,                       // true ONLY if user says: latest, today, just received, right now
  "query_clean":         "string",                           // core topic stripped of meta-language (remove: find, show, search for, etc.)
  "scope_level":         0,                                  // ALWAYS set to 0 — widen_scope node manages this
  "is_ambiguous":        true | false,                       // true if query is unintelligible, single character, or unresolvable even with history
  "original_query":      "string",                           // verbatim copy of the user query
  "skip_postgres_filter": false,                             // ALWAYS set to false — widen_scope node manages this
  "full_faiss_scan":     false                               // ALWAYS set to false — widen_scope node manages this
}}

CLASSIFICATION RULES:
- sources: Use ["gmail"] for email-specific queries. Use ["chrome"] for web page / article queries. Use ["youtube"] for video queries. Use ["gmail","chrome","youtube"] for broad or unspecified queries.
- time_filter: Convert relative dates to ISO-8601 using today={today}. "yesterday" → one day before today. "this week" → Monday of current week. "last month" → first day of last month. Set null for queries with no time reference.
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
        scope_level=0,
        is_ambiguous=True,
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

        # Validate required keys are present
        required_keys = {
            "sources", "time_filter", "fetch_attachment", "fetch_api",
            "query_clean", "scope_level", "is_ambiguous", "original_query",
            "skip_postgres_filter", "full_faiss_scan",
        }
        missing = required_keys - parsed.keys()
        if missing:
            raise ValueError(f"LLM response missing keys: {missing}")

        intent = ParsedIntent(**parsed)
        logger.info("parse_intent: success — sources=%s query_clean=%r", intent.get("sources"), intent.get("query_clean"))
        return {"parsed_intent": intent}

    except (json.JSONDecodeError, ValueError, KeyError) as exc:
        logger.error("parse_intent: JSON parse error — %s. Raw: %r", exc, locals().get("raw_text", "N/A"))
        return {"parsed_intent": _fallback_intent(user_query)}

    except Exception as exc:
        logger.error("parse_intent: LLM call failed — %s", exc)
        return {"parsed_intent": _fallback_intent(user_query)}
