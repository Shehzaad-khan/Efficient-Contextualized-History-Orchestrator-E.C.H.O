"""
<<<<<<< HEAD
query_parser.py — RSE Module
Echo Personal Memory System

parse_intent node — LLM Call 1.

Responsibilities:
    - Send user query + conversation history to the configured LLM
    - Return structured ParsedIntent JSON reliably using few-shot prompting
    - Fall back gracefully on any LLM failure (is_ambiguous=True, no crash)

Design:
    - 6 few-shot examples cover the main intent patterns.
    - Temperature=0 ensures deterministic, repeatable parsing.
    - Output is validated against required fields before being accepted.
"""

import json
import logging
import os
from typing import Any, Dict, List

from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, SystemMessage

load_dotenv()
logger = logging.getLogger(__name__)

# ── Few-shot examples embedded in the system prompt ──────────────────────────
FEW_SHOT_EXAMPLES = """
EXAMPLES — return exactly this JSON structure based on the query:

Example 1:
Query: "what YouTube videos did I watch this week about operating systems?"
Output:
{
  "sources": ["youtube"],
  "time_filter": "7_days",
  "fetch_attachment": false,
  "fetch_api": false,
  "query_clean": "operating systems",
  "scope_level": 0,
  "is_ambiguous": false,
  "original_query": "what YouTube videos did I watch this week about operating systems?",
  "skip_postgres_filter": false,
  "full_faiss_scan": false
}

Example 2:
Query: "show me emails about the capstone project from last month"
Output:
{
  "sources": ["gmail"],
  "time_filter": "30_days",
  "fetch_attachment": false,
  "fetch_api": false,
  "query_clean": "capstone project",
  "scope_level": 0,
  "is_ambiguous": false,
  "original_query": "show me emails about the capstone project from last month",
  "skip_postgres_filter": false,
  "full_faiss_scan": false
}

Example 3:
Query: "what was that news article I read yesterday?"
Output:
{
  "sources": ["chrome"],
  "time_filter": "1_day",
  "fetch_attachment": false,
  "fetch_api": false,
  "query_clean": "news article",
  "scope_level": 0,
  "is_ambiguous": false,
  "original_query": "what was that news article I read yesterday?",
  "skip_postgres_filter": false,
  "full_faiss_scan": false
}

Example 4:
Query: "show me the attachment from the internship offer email"
Output:
{
  "sources": ["gmail"],
  "time_filter": null,
  "fetch_attachment": true,
  "fetch_api": false,
  "query_clean": "internship offer",
  "scope_level": 0,
  "is_ambiguous": false,
  "original_query": "show me the attachment from the internship offer email",
  "skip_postgres_filter": false,
  "full_faiss_scan": false
}

Example 5:
Query: "what OS tutorial did I watch before I got the interview email?"
Output:
{
  "sources": ["all"],
  "time_filter": null,
  "fetch_attachment": false,
  "fetch_api": false,
  "query_clean": "operating systems tutorial interview",
  "scope_level": 0,
  "is_ambiguous": false,
  "original_query": "what OS tutorial did I watch before I got the interview email?",
  "skip_postgres_filter": false,
  "full_faiss_scan": false
}

Example 6:
Query: "what have I been doing?"
Output:
{
  "sources": ["all"],
  "time_filter": "7_days",
  "fetch_attachment": false,
  "fetch_api": false,
  "query_clean": "recent activity",
  "scope_level": 0,
  "is_ambiguous": true,
  "original_query": "what have I been doing?",
  "skip_postgres_filter": false,
  "full_faiss_scan": false
}
"""

SYSTEM_PROMPT = f"""You are the intent parser for E.C.H.O, a personal memory system.
Your ONLY job is to parse a natural language query into a structured JSON object.
Return ONLY the JSON object — no explanation, no markdown, no code fences.

Rules:
- "sources" must be a list containing one or more of: "gmail", "chrome", "youtube", "all"
- "time_filter" must be one of: "1_day", "7_days", "30_days", "90_days", an ISO-8601 date string, or null
- "query_clean" is the semantic core — remove filler words like "show me", "find", "what was", etc.
- "is_ambiguous" = true only if the query is genuinely unclear or has no searchable content
- "fetch_attachment" = true if the user explicitly asks about file attachments or PDFs
- All other boolean fields default to false
- "scope_level" always starts at 0 — the retrieval engine handles widening

{FEW_SHOT_EXAMPLES}

If the conversation history contains prior turns, use them to resolve pronouns and implicit references.
For example: if the prior query was "show me OS videos" and the new query is "now show me emails about that",
then sources=["gmail"] and query_clean="operating systems".
"""


def _get_llm():
    """Build the LangChain chat model from environment config."""
    provider = os.getenv("LLM_PROVIDER", "google_genai")
    model = os.getenv("PARSER_MODEL", os.getenv("LLM_MODEL", "gemini-1.5-flash"))

    if provider == "google_genai":
        from langchain_google_genai import ChatGoogleGenerativeAI
        return ChatGoogleGenerativeAI(
            model=model,
            temperature=0,
            google_api_key=os.getenv("GOOGLE_API_KEY"),
        )
    elif provider == "anthropic":
        from langchain_anthropic import ChatAnthropic
        return ChatAnthropic(
            model=model,
            temperature=0,
            api_key=os.getenv("ANTHROPIC_API_KEY"),
        )
    elif provider == "openai":
        from langchain_openai import ChatOpenAI
        return ChatOpenAI(
            model=model,
            temperature=0,
            api_key=os.getenv("OPENAI_API_KEY"),
        )
    else:
        raise ValueError(f"Unsupported LLM_PROVIDER: {provider}. Use google_genai, anthropic, or openai.")


def _invoke_parser_with_google_fallback(messages):
    """
    Invoke Google GenAI parser model with fallback candidates when a model name is unavailable.
    """
    from langchain_google_genai import ChatGoogleGenerativeAI

    preferred = os.getenv("PARSER_MODEL", os.getenv("LLM_MODEL", "gemini-1.5-flash"))
    candidates = [preferred, "gemini-2.0-flash", "gemini-1.5-flash-latest"]
    seen = set()

    for model in candidates:
        if model in seen:
            continue
        seen.add(model)
        try:
            llm = ChatGoogleGenerativeAI(
                model=model,
                temperature=0,
                google_api_key=os.getenv("GOOGLE_API_KEY"),
            )
            response = llm.invoke(messages)
            return response
        except Exception as e:
            err = str(e)
            if "not found" in err.lower() or "not supported" in err.lower():
                continue
            raise
    raise RuntimeError("No available Google parser model from configured fallback list.")


def _build_user_message(query: str, conversation_history: List[Dict[str, str]]) -> str:
    """Format the user message with conversation history context."""
    if not conversation_history:
        return f"Query: {query}"

    history_text = "\n".join(
        f"{turn['role'].upper()}: {turn['content']}"
        for turn in conversation_history[-6:]  # Last 3 turns (6 messages)
    )
    return f"Conversation history:\n{history_text}\n\nCurrent query: {query}"


def _validate_intent(intent: Dict[str, Any]) -> bool:
    """Validate that the parsed intent has all required fields with correct types."""
    required = {
        "sources": list,
        "fetch_attachment": bool,
        "fetch_api": bool,
        "query_clean": str,
        "scope_level": int,
        "is_ambiguous": bool,
        "original_query": str,
        "skip_postgres_filter": bool,
        "full_faiss_scan": bool,
    }
    for field, expected_type in required.items():
        if field not in intent:
            logger.warning(f"ParsedIntent missing field: {field}")
            return False
        if not isinstance(intent[field], expected_type):
            logger.warning(f"ParsedIntent field {field} wrong type: {type(intent[field])}")
            return False
    return True


def _fallback_intent(query: str) -> Dict[str, Any]:
    """Safe fallback when LLM call fails — marks as ambiguous so no bad retrieval runs."""
    logger.warning("parse_intent falling back to ambiguous intent")
    return {
        "sources": ["all"],
        "time_filter": None,
        "fetch_attachment": False,
        "fetch_api": False,
        "query_clean": query,
        "scope_level": 0,
        "is_ambiguous": True,
        "original_query": query,
        "skip_postgres_filter": False,
        "full_faiss_scan": False,
    }


def _heuristic_intent(query: str) -> Dict[str, Any]:
    """
    Deterministic parser fallback when LLM is unavailable/quota-limited.
    """
    q = (query or "").strip()
    ql = q.lower()

    sources = ["all"]
    if "youtube" in ql or "video" in ql or "watch" in ql:
        sources = ["youtube"]
    elif "email" in ql or "gmail" in ql or "inbox" in ql:
        sources = ["gmail"]
    elif "chrome" in ql or "article" in ql or "website" in ql or "page" in ql:
        sources = ["chrome"]

    time_filter = None
    if "today" in ql:
        time_filter = "1_day"
    elif "yesterday" in ql:
        time_filter = "1_day"
    elif "this week" in ql or "recent" in ql or "recently" in ql:
        time_filter = "7_days"
    elif "last month" in ql or "this month" in ql:
        time_filter = "30_days"
    elif "last 3 months" in ql or "past 3 months" in ql:
        time_filter = "90_days"

    cleaned = q
    for phrase in [
        "what youtube videos did i watch recently",
        "what youtube videos did i watch",
        "show me",
        "find",
        "what did i",
        "can you",
        "?",
    ]:
        cleaned = cleaned.replace(phrase, "").strip()
        cleaned = cleaned.replace(phrase.title(), "").strip()
    if not cleaned:
        cleaned = "recent activity"

    return {
        "sources": sources,
        "time_filter": time_filter,
        "fetch_attachment": ("attachment" in ql or "pdf" in ql),
        "fetch_api": False,
        "query_clean": cleaned,
        "scope_level": 0,
        "is_ambiguous": False,
        "original_query": q,
        "skip_postgres_filter": False,
        "full_faiss_scan": False,
    }


def parse_user_intent(
    query: str,
    conversation_history: List[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Parse a natural language query into structured ParsedIntent JSON.

    Args:
        query: The raw user query string.
        conversation_history: List of prior turn dicts from conversation_memory.

    Returns:
        ParsedIntent dict. If LLM fails, returns is_ambiguous=True fallback.
    """
    if not query or not query.strip():
        return _fallback_intent(query or "")

    history = conversation_history or []
    try:
        provider = os.getenv("LLM_PROVIDER", "google_genai")
        messages = [
            SystemMessage(content=SYSTEM_PROMPT),
            HumanMessage(content=_build_user_message(query, history)),
        ]
        if provider == "google_genai":
            response = _invoke_parser_with_google_fallback(messages)
        else:
            llm = _get_llm()
            response = llm.invoke(messages)
        raw = response.content.strip()

        # Strip markdown code fences if the LLM wrapped the JSON
        if raw.startswith("```"):
            lines = raw.split("\n")
            raw = "\n".join(
                line for line in lines
                if not line.strip().startswith("```")
            )

        intent = json.loads(raw)

        # Ensure missing optional fields are set to defaults
        intent.setdefault("time_filter", None)
        intent.setdefault("scope_level", 0)
        intent.setdefault("skip_postgres_filter", False)
        intent.setdefault("full_faiss_scan", False)
        intent["original_query"] = query

        if not _validate_intent(intent):
            logger.warning("ParsedIntent failed validation — using fallback")
            return _fallback_intent(query)

        logger.info(
            f"parse_intent: sources={intent['sources']} "
            f"time_filter={intent['time_filter']} "
            f"query_clean='{intent['query_clean']}' "
            f"is_ambiguous={intent['is_ambiguous']}"
        )
        return intent

    except json.JSONDecodeError as e:
        logger.error(f"parse_intent JSON decode error: {e} — raw: {raw[:200]}")
        heuristic = _heuristic_intent(query)
        return heuristic
    except Exception as e:
        logger.error(f"parse_intent LLM error: {e}")
        heuristic = _heuristic_intent(query)
        return heuristic
=======
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
>>>>>>> 4842d1a3060d3dea11ed107e06e4212e96c74fb4
