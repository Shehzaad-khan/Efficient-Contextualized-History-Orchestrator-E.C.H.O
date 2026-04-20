"""
Conversation memory management for the LangGraph RSE.

Uses LangChain's PostgresChatMessageHistory backed by the Neon PostgreSQL
message_store table. History is loaded before every LLM call so multi-turn
references (e.g. 'find Chrome pages about that topic') resolve correctly.
"""
import logging
from datetime import datetime, timezone
from typing import Any

import psycopg2
from langchain_community.chat_message_histories import PostgresChatMessageHistory

from rse.config import DATABASE_URL, CONVERSATION_HISTORY_DAYS

logger = logging.getLogger(__name__)


def get_session_history(session_id: str) -> PostgresChatMessageHistory:
    """
    Return a PostgresChatMessageHistory instance for the given session.

    LangChain will auto-create the message_store table on first use if it does
    not already exist.

    Args:
        session_id: Unique identifier for the conversation session.

    Returns:
        PostgresChatMessageHistory bound to the session.
    """
    return PostgresChatMessageHistory(
        connection_string=DATABASE_URL,
        session_id=session_id,
    )


def load_conversation_history(session_id: str) -> list[Any]:
    """
    Load all messages for a session as a list of LangChain BaseMessage objects.

    Called by parse_intent before each LLM call so the model has full context
    to resolve cross-turn references.

    Args:
        session_id: Conversation session identifier.

    Returns:
        Ordered list of BaseMessage objects (HumanMessage / AIMessage).
        Returns an empty list if no history exists or on any error.
    """
    try:
        history = get_session_history(session_id)
        messages = history.messages
        logger.info("Loaded %d history messages for session %s", len(messages), session_id)
        return messages
    except Exception as exc:
        logger.error("Failed to load conversation history for session %s: %s", session_id, exc)
        return []


def save_turn(session_id: str, user_query: str, assistant_answer: str) -> None:
    """
    Persist one complete query/response turn to the message_store table.

    Called after the RSE graph finishes processing to make the turn available
    for subsequent queries in the same session.

    Args:
        session_id: Conversation session identifier.
        user_query: The raw user query text.
        assistant_answer: The synthesized answer returned to the user.
    """
    try:
        history = get_session_history(session_id)
        history.add_user_message(user_query)
        history.add_ai_message(assistant_answer)
        logger.info("Saved turn to session %s", session_id)
    except Exception as exc:
        logger.error("Failed to save turn for session %s: %s", session_id, exc)


def cleanup_old_sessions() -> int:
    """
    Delete message_store rows older than CONVERSATION_HISTORY_DAYS days.

    Should be called periodically (e.g. on application startup or daily via a
    scheduler) to prevent unbounded growth of the conversation table.

    Returns:
        Number of rows deleted, or -1 on error.
    """
    sql = """
        DELETE FROM message_store
        WHERE created_at < NOW() - INTERVAL '%s days'
    """
    try:
        conn = psycopg2.connect(DATABASE_URL)
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, (CONVERSATION_HISTORY_DAYS,))
                deleted = cur.rowcount
        conn.close()
        logger.info("Cleaned up %d old message_store rows", deleted)
        return deleted
    except Exception as exc:
        logger.error("Session cleanup failed: %s", exc)
        return -1
