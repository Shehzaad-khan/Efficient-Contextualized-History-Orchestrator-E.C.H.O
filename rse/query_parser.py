"""
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
from langchain.schema import HumanMessage, SystemMessage

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
        llm = _get_llm()
        messages = [
            SystemMessage(content=SYSTEM_PROMPT),
            HumanMessage(content=_build_user_message(query, history)),
        ]

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
        return _fallback_intent(query)
    except Exception as e:
        logger.error(f"parse_intent LLM error: {e}")
        return _fallback_intent(query)
