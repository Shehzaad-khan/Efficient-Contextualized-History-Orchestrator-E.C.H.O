"""
retrieval.py — Backend Module
Echo Personal Memory System

FastAPI router that exposes the RSE (Retrieval & Synthesis Engine) as HTTP endpoints.

Endpoints:
    POST /retrieval/query          — main query endpoint
    GET  /retrieval/session/{id}   — session diagnostic
    DELETE /retrieval/session/{id} — clear session history
"""

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/retrieval", tags=["Retrieval & Synthesis Engine"])


class QueryRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=2000, description="Natural language query")
    session_id: Optional[str] = Field(None, description="Session ID for multi-turn context. Leave null for new session.")


class QueryResponse(BaseModel):
    final_answer: str
    session_id: str
    no_results: bool
    result_count: int
    parsed_intent: Optional[dict] = None


@router.post("/query", response_model=QueryResponse)
def query(request: QueryRequest):
    """
    Execute a natural language query against Echo memory.

    Runs the full 9-node LangGraph RSE:
      parse_intent → postgres_search → faiss_search → evaluate_quality
      → synthesize (or widen_scope up to 3x → no_results_found)

    Multi-turn: pass the session_id from a previous response to continue
    the conversation with full context ("find Chrome pages about that topic").
    """
    from rse.retrieval_engine import run_query

    try:
        result = run_query(
            user_query=request.query,
            session_id=request.session_id,
        )
        return QueryResponse(**result)
    except Exception as e:
        logger.error(f"retrieval /query error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/session/{session_id}")
def get_session_info(session_id: str):
    """
    Return diagnostic info about a conversation session.
    Useful for debugging multi-turn context issues.
    """
    from rse.conversation_memory import get_session_messages_count, load_conversation_history

    history = load_conversation_history(session_id)
    return {
        "session_id": session_id,
        "message_count": len(history),
        "history": history,
    }


@router.delete("/session/{session_id}")
def clear_session(session_id: str):
    """
    Delete all messages for a session from message_store.
    Use this to start a fresh conversation context.
    """
    import psycopg2
    import os

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise HTTPException(status_code=500, detail="DATABASE_URL not configured")

    conn = None
    try:
        conn = psycopg2.connect(db_url)
        with conn.cursor() as cur:
            cur.execute("DELETE FROM message_store WHERE session_id = %s", (session_id,))
            deleted = cur.rowcount
        conn.commit()
        return {"session_id": session_id, "messages_deleted": deleted}
    except Exception as e:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()
