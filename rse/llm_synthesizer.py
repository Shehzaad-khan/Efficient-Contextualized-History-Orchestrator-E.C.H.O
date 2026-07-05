"""
synthesize node — LLM Call 2.

Assembles context from the top-ranked results, their neighborhood items,
attachment content, and conversation history, then calls the configured
synthesizer LLM to produce a readable answer with source citations and
temporal context.

Privacy contract (architecture Section 13.2): the LLM receives ONLY the query,
formatted snippets (title, short excerpt, timestamp, source), and session
history — never full raw_text, full transcripts, or database rows. Snippets
are plain leading excerpts (SNIPPET_CHAR_LIMIT); no semantic filtering or
compression is applied by design.

The provider factory mirrors query_parser._build_provider_llm but uses
LLM_CONFIG['synthesizer_model'] so parser and synthesizer can use different
model tiers.
"""
import logging
from typing import Any

from langchain_core.messages import BaseMessage, HumanMessage

from rse.config import LLM_CONFIG, SNIPPET_CHAR_LIMIT, SYNTHESIS_TOP_RESULTS
from rse.state import EchoState

logger = logging.getLogger(__name__)

_SYNTHESIS_PROMPT = """You are Echo, a personal memory assistant. The user searched their own captured digital activity (emails, web pages, YouTube videos) and the retrieval engine found the items below.

Answer the user's query using ONLY these retrieved items. Rules:
- Cite items inline by their bracketed number, e.g. [1], [2].
- Mention WHEN things happened (dates/times from the metadata) — temporal context matters to the user.
- Mention the source type naturally (email, article, video).
- "Related context" entries under an item are surrounding activity (same email thread or same browsing session) — use them to enrich the answer, but the numbered items are the primary evidence.
- If the items only partially answer the query, say what was found and what wasn't.
- Be concise and factual. Never invent items, dates, or contents not present below.

USER QUERY: {query}

CONVERSATION SO FAR:
{history}

RETRIEVED ITEMS:
{results}
{attachment_block}
Answer:"""


def build_synthesizer_llm() -> Any:
    """
    Instantiate the synthesis LLM via LangChain's universal init_chat_model factory.

    Reads provider + synthesizer_model from LLM_CONFIG. API keys are read from
    environment variables by the integration package.

    Returns:
        A LangChain chat model instance.
    """
    from langchain.chat_models import init_chat_model
    return init_chat_model(
        model=LLM_CONFIG["synthesizer_model"],
        model_provider=LLM_CONFIG["provider"],
        temperature=LLM_CONFIG.get("synthesizer_temperature", 0.3),
    )


def _format_timestamp(value: Any) -> str:
    """Render a created_at value for the prompt."""
    if value is None:
        return "unknown time"
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d %H:%M")
    return str(value)


def _item_location(item: dict[str, Any]) -> str:
    """Source-specific locator line: sender, domain, or channel."""
    source = item.get("source_type")
    if source == "gmail":
        return f"from {item.get('sender') or 'unknown sender'}"
    if source == "chrome":
        return f"on {item.get('domain') or item.get('url') or 'unknown site'}"
    if source == "youtube":
        return f"channel {item.get('channel_name') or 'unknown channel'}"
    return ""


def _format_item(index: int, item: dict[str, Any], neighbors: list[dict[str, Any]]) -> str:
    """One numbered context block: metadata line, snippet, neighbor lines."""
    title = item.get("title") or item.get("subject") or "(untitled)"
    lines = [
        f"[{index}] ({item.get('source_type')}) {title}",
        f"    {_item_location(item)} — {_format_timestamp(item.get('created_at'))}",
    ]
    snippet = (item.get("raw_snippet") or "").strip()
    if snippet:
        lines.append(f"    Excerpt: {snippet[:SNIPPET_CHAR_LIMIT]}")

    for neighbor in neighbors:
        n_title = neighbor.get("title") or neighbor.get("subject") or "(untitled)"
        lines.append(
            f"    Related context ({neighbor.get('source_type')}, "
            f"{_format_timestamp(neighbor.get('created_at'))}): {n_title}"
        )
    return "\n".join(lines)


def _format_history(messages: list[BaseMessage]) -> str:
    """Render conversation history for the synthesis prompt."""
    if not messages:
        return "(first query in this session)"
    lines = []
    for msg in messages[-6:]:
        role = "User" if isinstance(msg, HumanMessage) else "Echo"
        lines.append(f"{role}: {msg.content}")
    return "\n".join(lines)


def _deterministic_fallback(query: str, results: list[dict[str, Any]]) -> str:
    """Readable answer built without the LLM when the synthesis call fails."""
    lines = [
        f"Found {len(results)} item(s) for '{query}' "
        "(answer synthesis is temporarily unavailable — showing raw results):"
    ]
    for index, item in enumerate(results, start=1):
        title = item.get("title") or item.get("subject") or "(untitled)"
        lines.append(
            f"{index}. [{item.get('source_type')}] {title} — "
            f"{_format_timestamp(item.get('created_at'))}"
        )
    return "\n".join(lines)


def synthesize(state: EchoState) -> dict:
    """
    LLM Call 2: generate the final answer from the ranked retrieval payload.

    Args:
        state: EchoState carrying user_query, ranked_results, neighbor_items,
               attachment_content, and conversation_history.

    Returns:
        Partial state dict with final_answer and no_results=False. On LLM
        failure, returns a deterministic listing of the top results instead
        of an error — the retrieval work is never thrown away.
    """
    query: str = state.get("user_query", "")
    ranked: list[dict[str, Any]] = state.get("ranked_results", [])[:SYNTHESIS_TOP_RESULTS]
    neighbor_items: dict[str, list[dict[str, Any]]] = state.get("neighbor_items", {}) or {}
    attachment_content = state.get("attachment_content")
    history: list = state.get("conversation_history", [])

    if not ranked:
        # evaluate_quality should have routed away before this, but stay safe.
        return {
            "final_answer": f"No matching items were found for '{query}'.",
            "no_results": True,
        }

    result_blocks = [
        _format_item(index, item, neighbor_items.get(item["memory_id"], []))
        for index, item in enumerate(ranked, start=1)
    ]

    attachment_block = ""
    if attachment_content:
        attachment_block = f"\nATTACHMENT CONTENT (top result):\n{attachment_content}\n"

    prompt = _SYNTHESIS_PROMPT.format(
        query=query,
        history=_format_history(history),
        results="\n\n".join(result_blocks),
        attachment_block=attachment_block,
    )

    logger.info("synthesize: calling LLM with %d results, %d neighbor groups",
                len(ranked), len(neighbor_items))

    try:
        llm = build_synthesizer_llm()
        response = llm.invoke([HumanMessage(content=prompt)])
        answer = str(response.content).strip()
        if not answer:
            raise ValueError("LLM returned an empty answer")
        return {"final_answer": answer, "no_results": False}
    except Exception as exc:
        logger.error("synthesize: LLM call failed — %s", exc)
        return {"final_answer": _deterministic_fallback(query, ranked), "no_results": False}
