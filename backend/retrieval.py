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
import os
import re
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Security
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field

from backend.local_store import list_records, search_records

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/retrieval", tags=["Retrieval & Synthesis Engine"])

_SESSION_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{1,128}$")

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def _require_api_key(api_key: str = Security(_api_key_header)) -> None:
    expected = os.getenv("ECHO_API_KEY")
    if not expected:
        raise HTTPException(status_code=503, detail="Session management unavailable")
    if api_key != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")


class QueryRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=2000, description="Natural language query")
    session_id: Optional[str] = Field(None, max_length=128, description="Session ID for multi-turn context. Leave null for new session.")


class QueryResponse(BaseModel):
    final_answer: str
    session_id: str
    no_results: bool
    result_count: int
    parsed_intent: Optional[dict] = None


def local_query_fallback(user_query: str, *, session_id: Optional[str] = None, db_path: str | None = None) -> dict:
    """Fallback retrieval using the persisted local ingestion store."""
    matches = search_records(user_query, db_path=db_path)
    if not matches:
        return {
            "final_answer": f"I couldn't find anything matching '{user_query}' in the local memory store.",
            "session_id": session_id or "local-session",
            "no_results": True,
            "result_count": 0,
            "parsed_intent": {"mode": "local-fallback", "query": user_query},
        }

    item_lines = []
    for item in matches[:5]:
        payload = item.get("payload", {})
        title = payload.get("title") or payload.get("subject") or payload.get("url") or "Untitled"
        url = payload.get("url")
        if url:
            item_lines.append(f"- {title}: {url}")
        else:
            item_lines.append(f"- {title}")

    answer = "Here are the closest local matches I found:\n" + "\n".join(item_lines)
    return {
        "final_answer": answer,
        "session_id": session_id or "local-session",
        "no_results": False,
        "result_count": len(matches),
        "parsed_intent": {"mode": "local-fallback", "query": user_query},
    }


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
    try:
        from rse.retrieval_engine import run_query
        result = run_query(
            user_query=request.query,
            session_id=request.session_id,
        )
        return QueryResponse(**result)
    except Exception as e:
        logger.warning("RSE query failed; falling back to local ingestion store: %s", e)
        return QueryResponse(**local_query_fallback(request.query, session_id=request.session_id))


@router.get("/session/{session_id}", dependencies=[Depends(_require_api_key)])
def get_session_info(session_id: str):
    """
    Return diagnostic info about a conversation session.
    Useful for debugging multi-turn context issues.
    """
    if not _SESSION_ID_RE.match(session_id):
        raise HTTPException(status_code=400, detail="Invalid session_id")
    from rse.conversation_memory import load_conversation_history

    history = load_conversation_history(session_id)
    return {
        "session_id": session_id,
        "message_count": len(history),
        "history": history,
    }


@router.delete("/session/{session_id}", dependencies=[Depends(_require_api_key)])
def clear_session(session_id: str):
    """
    Delete all messages for a session from message_store.
    Use this to start a fresh conversation context.
    """
    if not _SESSION_ID_RE.match(session_id):
        raise HTTPException(status_code=400, detail="Invalid session_id")
    import psycopg2

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise HTTPException(status_code=500, detail="Database not configured")

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
        logger.error(f"clear_session error for session {session_id!r}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if conn:
            conn.close()
