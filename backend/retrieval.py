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
import json
from pathlib import Path
import time

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional

logger = logging.getLogger(__name__)
_DEBUG_LOG_PATH = Path(__file__).resolve().parents[1] / "debug-20c712.log"


def _debug_log(run_id: str, hypothesis_id: str, location: str, message: str, data: dict):
    payload = {
        "sessionId": "20c712",
        "runId": run_id,
        "hypothesisId": hypothesis_id,
        "location": location,
        "message": message,
        "data": data,
        "timestamp": int(time.time() * 1000),
    }
    try:
        with _DEBUG_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=True) + "\n")
    except Exception:
        pass

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
    run_id = f"pre-fix-{int(time.time() * 1000)}"
    # #region agent log
    _debug_log(
        run_id=run_id,
        hypothesis_id="H0",
        location="backend/retrieval.py:query",
        message="received retrieval query request",
        data={
            "has_session_id": bool(request.session_id),
            "query_len": len(request.query or ""),
        },
    )
    # #endregion

    try:
        # #region agent log
        _debug_log(
            run_id=run_id,
            hypothesis_id="H1",
            location="backend/retrieval.py:query",
            message="importing run_query",
            data={},
        )
        # #endregion
        from rse.retrieval_engine import run_query
        # #region agent log
        _debug_log(
            run_id=run_id,
            hypothesis_id="H1",
            location="backend/retrieval.py:query",
            message="imported run_query successfully",
            data={},
        )
        # #endregion
        result = run_query(
            user_query=request.query,
            session_id=request.session_id,
        )
        # #region agent log
        _debug_log(
            run_id=run_id,
            hypothesis_id="H4",
            location="backend/retrieval.py:query",
            message="run_query returned",
            data={
                "result_type": type(result).__name__,
                "has_final_answer": isinstance(result, dict) and ("final_answer" in result),
                "has_no_results": isinstance(result, dict) and ("no_results" in result),
                "has_result_count": isinstance(result, dict) and ("result_count" in result),
            },
        )
        # #endregion
        return QueryResponse(**result)
    except Exception as e:
        # #region agent log
        _debug_log(
            run_id=run_id,
            hypothesis_id="H5",
            location="backend/retrieval.py:query",
            message="retrieval endpoint exception",
            data={"error_type": type(e).__name__, "error": str(e)},
        )
        # #endregion
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
