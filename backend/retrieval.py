"""
retrieval.py — Backend Module
Echo Personal Memory System

FastAPI router that exposes the RSE (Retrieval & Synthesis Engine) as HTTP endpoints.

Endpoints:
    POST /retrieval/query          — main query endpoint (no API key: the local UI calls it)
    GET  /retrieval/session/{id}   — session diagnostic          (X-API-Key required)
    DELETE /retrieval/session/{id} — clear session history       (X-API-Key required)

The session routes return or delete chat history, so they need ECHO_API_KEY;
without it set they answer 503.

Threading: every route is a plain `def`, so FastAPI runs it in its threadpool.
The RSE graph and the DB calls block, and must never run on the event loop.

Logging: run_query already logs each query's timing and counts. This file only
logs failures and session-route results — never the query text or the answer.
"""

import logging
import os
import re
import secrets
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Security
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field

from rse.conversation_memory import load_conversation_history
from rse.search_coordinator import get_connection

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/retrieval", tags=["Retrieval & Synthesis Engine"])

# One rule for session ids everywhere: run_query makes UUIDs, which match it.
_SESSION_ID_PATTERN = r"^[a-zA-Z0-9_-]{1,128}$"
_SESSION_ID_RE = re.compile(_SESSION_ID_PATTERN)

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def _require_api_key(api_key: Optional[str] = Security(_api_key_header)) -> None:
    expected = os.getenv("ECHO_API_KEY")
    if not expected:
        raise HTTPException(status_code=503, detail="Session management unavailable")
    # compare_digest takes the same time whatever the input, so response
    # timing can't reveal how much of a guessed key was right.
    if api_key is None or not secrets.compare_digest(api_key.encode(), expected.encode()):
        raise HTTPException(status_code=401, detail="Unauthorized")


def _check_session_id(session_id: str) -> None:
    if not _SESSION_ID_RE.match(session_id):
        raise HTTPException(status_code=400, detail="Invalid session_id")


class QueryRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=2000, description="Natural language query")
    session_id: Optional[str] = Field(
        None,
        pattern=_SESSION_ID_PATTERN,
        description="Session ID for multi-turn context. Leave null for new session.",
    )


class QueryResponse(BaseModel):
    # run_query returns the whole graph state; pydantic drops the fields not
    # listed here (raw rows, candidates), so they never reach the browser.
    final_answer: str
    session_id: str
    no_results: bool
    result_count: int
    parsed_intent: Optional[dict] = None


@router.post("/query", response_model=QueryResponse)
def query(request: QueryRequest):
    """
    Execute a natural language query against Echo memory.

    Runs the 14-node LangGraph RSE (rse/retrieval_engine.py):
      parse_intent → resolve_time_anchor → keyword + semantic search → RRF merge
      → evaluate_quality (widen_scope up to 3x) → cross-encoder rerank
      → neighbours / attachments / live Gmail → synthesize (or no_results_found)

    Multi-turn: pass the session_id from a previous response to continue
    the conversation with full context ("find Chrome pages about that topic").
    """
    # Imported on first use: building the graph loads the RSE modules, which
    # the rest of the app doesn't need at startup.
    from rse.retrieval_engine import run_query

    try:
        result = run_query(
            user_query=request.query,
            session_id=request.session_id,
        )
        return QueryResponse(**result)
    except Exception as exc:
        # run_query already turns graph failures into a polite answer, so this
        # is rarer (e.g. a bad response shape). Only the type is logged.
        logger.exception("retrieval /query failed — %s", type(exc).__name__)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/session/{session_id}", dependencies=[Depends(_require_api_key)])
def get_session_info(session_id: str):
    """
    Return diagnostic info about a conversation session.
    Useful for debugging multi-turn context issues.
    """
    _check_session_id(session_id)
    # Returns [] on a DB error (it logs the error itself).
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
    _check_session_id(session_id)
    try:
        # Same pooled connection as rse.conversation_memory, so no fresh
        # TCP+TLS handshake per call. `with conn` commits, or rolls back on error.
        conn = get_connection()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM message_store WHERE session_id = %s", (session_id,))
                    deleted = cur.rowcount
        finally:
            conn.close()
    except Exception as exc:
        logger.exception("clear_session failed for session %s — %s", session_id, type(exc).__name__)
        raise HTTPException(status_code=500, detail="Internal server error")

    logger.info("Cleared session %s (%d messages)", session_id, deleted)
    return {"session_id": session_id, "messages_deleted": deleted}
