"""
conversation_memory.py — RSE Module
Echo Personal Memory System

Multi-turn conversation context via LangChain PostgresChatMessageHistory.
One session_id per user conversation. History is loaded before every LLM call
so that "show me Chrome pages about that topic" resolves correctly because
the prior turn referencing "that topic" is in context.

Storage: message_store table on Neon (16-table schema, Table 16).
Schema: id SERIAL, session_id TEXT, message JSONB, created_at TIMESTAMP.
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import psycopg2
from dotenv import load_dotenv

from .config import CONVERSATION_HISTORY_DAYS, DATABASE_URL, MESSAGE_STORE_TABLE

load_dotenv()
logger = logging.getLogger(__name__)


def _get_db_url() -> str:
    return DATABASE_URL or os.getenv("DATABASE_URL", "")


# ── Load history ──────────────────────────────────────────────────────────────

def load_conversation_history(session_id: str) -> List[Dict[str, str]]:
    """
    Load all messages for a session from message_store.

    Returns list of dicts: [{"role": "human"|"ai", "content": "..."}]
    Most recent turns last (chronological order).
    Returns empty list if session not found or on DB error.
    """
    if not session_id:
        return []

    db_url = _get_db_url()
    if not db_url:
        logger.warning("DATABASE_URL not set — conversation history unavailable")
        return []

    sql = f"""
        SELECT message
        FROM {MESSAGE_STORE_TABLE}
        WHERE session_id = %s
        ORDER BY created_at ASC
    """
    conn = None
    try:
        conn = psycopg2.connect(db_url)
        with conn.cursor() as cur:
            cur.execute(sql, (session_id,))
            rows = cur.fetchall()

        messages = []
        for (msg_json,) in rows:
            # message column is JSONB — psycopg2 returns it as a dict already
            if isinstance(msg_json, dict):
                msg_type = msg_json.get("type", "")
                content = msg_json.get("data", {}).get("content", "")
                if msg_type == "human":
                    messages.append({"role": "human", "content": content})
                elif msg_type == "ai":
                    messages.append({"role": "ai", "content": content})
            else:
                logger.warning(f"Unexpected message format in message_store: {type(msg_json)}")

        logger.info(f"conversation_memory: loaded {len(messages)} messages for session={session_id}")
        return messages

    except Exception as e:
        logger.error(f"load_conversation_history error: {e}")
        return []
    finally:
        if conn:
            conn.close()


# ── Save turn ─────────────────────────────────────────────────────────────────

def save_turn(session_id: str, user_query: str, assistant_answer: str) -> bool:
    """
    Persist one completed query-answer turn to message_store.

    Each turn is two rows: one HUMAN message, one AI message.
    Uses the LangChain-compatible JSONB format so history is
    readable by LangChain's PostgresChatMessageHistory if needed.

    Returns True on success, False on failure.
    """
    if not session_id:
        return False

    db_url = _get_db_url()
    if not db_url:
        logger.warning("DATABASE_URL not set — turn not saved")
        return False

    import json
    human_msg = json.dumps({
        "type": "human",
        "data": {"content": user_query, "additional_kwargs": {}, "type": "human"},
    })
    ai_msg = json.dumps({
        "type": "ai",
        "data": {"content": assistant_answer, "additional_kwargs": {}, "type": "ai"},
    })

    sql = f"""
        INSERT INTO {MESSAGE_STORE_TABLE} (session_id, message, created_at)
        VALUES (%s, %s::jsonb, NOW()), (%s, %s::jsonb, NOW())
    """
    conn = None
    try:
        conn = psycopg2.connect(db_url)
        with conn.cursor() as cur:
            cur.execute(sql, (session_id, human_msg, session_id, ai_msg))
        conn.commit()
        logger.info(f"conversation_memory: saved turn for session={session_id}")
        return True

    except Exception as e:
        logger.error(f"save_turn error: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


# ── Cleanup ───────────────────────────────────────────────────────────────────

def cleanup_old_sessions(days: int = CONVERSATION_HISTORY_DAYS) -> int:
    """
    Delete message_store rows older than `days` days.
    Returns the number of rows deleted, or -1 on error.
    Call periodically (e.g. on app startup or daily cron).
    """
    db_url = _get_db_url()
    if not db_url:
        return -1

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    sql = f"""
        DELETE FROM {MESSAGE_STORE_TABLE}
        WHERE created_at < %s
    """
    conn = None
    try:
        conn = psycopg2.connect(db_url)
        with conn.cursor() as cur:
            cur.execute(sql, (cutoff,))
            deleted = cur.rowcount
        conn.commit()
        logger.info(f"conversation_memory: purged {deleted} rows older than {days} days")
        return deleted

    except Exception as e:
        logger.error(f"cleanup_old_sessions error: {e}")
        if conn:
            conn.rollback()
        return -1
    finally:
        if conn:
            conn.close()


# ── Session ID helpers ────────────────────────────────────────────────────────

def generate_session_id() -> str:
    """Generate a new unique session ID."""
    import uuid
    return str(uuid.uuid4())


def get_session_messages_count(session_id: str) -> int:
    """Return the number of messages in a session. Useful for diagnostics."""
    db_url = _get_db_url()
    if not db_url or not session_id:
        return 0

    conn = None
    try:
        conn = psycopg2.connect(db_url)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {MESSAGE_STORE_TABLE} WHERE session_id = %s",
                (session_id,)
            )
            row = cur.fetchone()
        return row[0] if row else 0
    except Exception as e:
        logger.error(f"get_session_messages_count error: {e}")
        return 0
    finally:
        if conn:
            conn.close()
