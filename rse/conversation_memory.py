"""
Conversation memory for the LangGraph RSE — multi-turn history in message_store.

run_query (rse/retrieval_engine.py) loads a session's history once before the
graph starts, so both LLM calls can resolve cross-turn references ("find Chrome
pages about that topic"), and saves the finished turn afterwards.

Storage: table 16 `message_store` (id, session_id, message JSONB, created_at),
created by scripts/setup_db.py. Each row holds one LangChain message in
LangChain's own dict format (`message_to_dict`), so rows written earlier by
LangChain's PostgresChatMessageHistory still load, and vice versa.

Why not PostgresChatMessageHistory any more (2026-10-18 review): the
langchain_community class is deprecated, opened a brand-new psycopg connection
per call (load + save = 2 handshakes per query, ~700 ms each to Neon) that was
only closed by garbage collection, and ran CREATE TABLE IF NOT EXISTS on every
call. This module does the same reads/writes over the RSE connection pool.
"""
import logging

from langchain_core.messages import BaseMessage, AIMessage, HumanMessage, message_to_dict, messages_from_dict
from psycopg2.extras import Json

from rse.config import CONVERSATION_HISTORY_DAYS
from rse.search_coordinator import get_connection

logger = logging.getLogger(__name__)


def load_conversation_history(session_id: str) -> list[BaseMessage]:
    """
    Load all messages of a session, oldest first.

    Args:
        session_id: Conversation session identifier.

    Returns:
        Ordered list of HumanMessage / AIMessage objects. Returns an empty list
        when the session is new or on any error — the query is then answered
        without history instead of failing.
    """
    try:
        conn = get_connection()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT message FROM message_store WHERE session_id = %s ORDER BY id",
                        (session_id,),
                    )
                    rows = cur.fetchall()
        finally:
            conn.close()
        # psycopg2 already decodes JSONB into dicts.
        messages = messages_from_dict([row[0] for row in rows])
        logger.info("Loaded %d history messages for session %s", len(messages), session_id)
        return messages
    except Exception as exc:
        logger.error("Failed to load conversation history for session %s: %s", session_id, exc)
        return []


def save_turn(session_id: str, user_query: str, assistant_answer: str) -> None:
    """
    Persist one query/answer turn to message_store.

    Both rows are written in one transaction, so a failure never leaves a
    question without its answer in the history.

    Args:
        session_id: Conversation session identifier.
        user_query: The raw user query text.
        assistant_answer: The answer returned to the user.
    """
    turn = [HumanMessage(content=user_query), AIMessage(content=assistant_answer)]
    try:
        conn = get_connection()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO message_store (session_id, message) VALUES (%s, %s), (%s, %s)",
                        (
                            session_id, Json(message_to_dict(turn[0])),
                            session_id, Json(message_to_dict(turn[1])),
                        ),
                    )
        finally:
            conn.close()
        logger.info("Saved turn to session %s", session_id)
    except Exception as exc:
        logger.error("Failed to save turn for session %s: %s", session_id, exc)


def cleanup_old_sessions() -> int:
    """
    Delete message_store rows older than CONVERSATION_HISTORY_DAYS days.

    Not called anywhere yet — meant to run periodically (backend startup or a
    daily job) so the table does not grow forever; wiring tracked in the
    backend/main.py review (C7).

    Returns:
        Number of rows deleted, or -1 on error.
    """
    try:
        conn = get_connection()
        try:
            with conn:
                with conn.cursor() as cur:
                    # created_at is a TIMESTAMP without zone (DEFAULT NOW()), so compare
                    # against LOCALTIMESTAMP; the day count is a real bind parameter.
                    cur.execute(
                        "DELETE FROM message_store WHERE created_at < LOCALTIMESTAMP - %s * INTERVAL '1 day'",
                        (CONVERSATION_HISTORY_DAYS,),
                    )
                    deleted = cur.rowcount
        finally:
            conn.close()
        logger.info("Cleaned up %d old message_store rows", deleted)
        return deleted
    except Exception as exc:
        logger.error("Session cleanup failed: %s", exc)
        return -1
