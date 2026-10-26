"""
retrieval.py — Backend Module
Echo Personal Memory System

FastAPI router that exposes the RSE (Retrieval & Synthesis Engine) as HTTP endpoints.

Endpoints:
    POST /retrieval/query          — main query endpoint (no API key: the local UI calls it)
    POST /retrieval/query/stream   — same query as server-sent events with live steps (no API key)
    GET  /retrieval/sessions       — past chats, newest first    (no API key: the local UI calls it)
    GET  /retrieval/sessions/{id}  — one past chat's messages    (no API key: the local UI calls it)
    GET  /retrieval/session/{id}   — session diagnostic          (X-API-Key required)
    DELETE /retrieval/session/{id} — clear session history       (X-API-Key required)

The diagnostic and delete routes (/session/{id}) need ECHO_API_KEY; without it
set they answer 503. The chat list and one chat's messages (/sessions, /sessions/{id})
are open like /query: they show only the user's own questions and Echo's replies, on
a local port.

Threading: every route is a plain `def`, so FastAPI runs it in its threadpool.
The RSE graph and the DB calls block, and must never run on the event loop.

Logging: run_query already logs each query's timing and counts. This file only
logs failures and session-route results — never the query text or the answer.
"""

import json
import logging
import os
import re
import secrets
from datetime import datetime, timezone
from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Security
from fastapi.responses import StreamingResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field

from langchain_core.messages import AIMessage, HumanMessage

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


# LangChain stores each message as {"type": "human"|"ai", "data": {"content": ...}}.
_LIST_SESSIONS_SQL = """
SELECT
    session_id,
    (ARRAY_AGG(message->'data'->>'content' ORDER BY id)
        FILTER (WHERE message->>'type' = 'human'))[1] AS first_question,
    MAX(created_at) AS last_active,
    COUNT(*) FILTER (WHERE message->>'type' = 'human') AS turns
FROM message_store
GROUP BY session_id
ORDER BY MAX(created_at) DESC
LIMIT %s
"""

_SESSION_TITLE_MAX_CHARS = 60


def _session_title(first_question: Optional[str]) -> str:
    """The first question on one line, cut at a word boundary with an ellipsis."""
    text = " ".join((first_question or "").split())
    if not text:
        return "Untitled chat"
    if len(text) <= _SESSION_TITLE_MAX_CHARS:
        return text
    return text[: _SESSION_TITLE_MAX_CHARS].rsplit(" ", 1)[0] + "…"


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
    # A27: the answer with validated ⟦n⟧ markers, and the cited items they point
    # at (n, memory_id, source_type, anchor, date_label, created_at_local, title,
    # link, role). `final_answer` stays the readable text for clients that draw
    # no pills. Defaults keep the fields present when a graph path sets neither.
    answer_markdown: str = ""
    answer_sources: list[dict] = Field(default_factory=list)


def _as_utc(moment: datetime) -> datetime:
    """message_store.created_at is a naive UTC time; mark it so the browser shows local time."""
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)


class SessionSummary(BaseModel):
    session_id: str
    title: str
    last_active: datetime
    turns: int


class SessionMessage(BaseModel):
    role: Literal["user", "echo"]
    text: str


class SessionMessages(BaseModel):
    session_id: str
    messages: list[SessionMessage]


@router.post("/query", response_model=QueryResponse)
def query(request: QueryRequest):
    """
    Execute a natural language query against Echo memory.

    Runs the LangGraph agent loop (rse/retrieval_engine.py, A26): Luna calls the
    search_memories tool (up to 5 searches) and writes the answer. Each search is
      time anchor → keyword + semantic search → RRF merge → quality check
      (widen_scope up to 3x) → cross-encoder rerank → neighbours / attachment text.
    Usually 2 LLM calls, at most 6.

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


def _sse(event: str, payload: dict) -> str:
    """One server-sent event. json.dumps puts the payload on a single line."""
    return f"event: {event}\ndata: {json.dumps(payload, default=str)}\n\n"


@router.post("/query/stream")
def query_stream(request: QueryRequest):
    """
    The same query as POST /query, but as a stream of server-sent events (A28).

    Events: `session` (the session id, first), `step` (label + optional count after
    each graph node), `done` (the full QueryResponse, last), or `error` instead of
    `done` when something broke. POST /query stays as the plain fallback.
    A sync generator: Starlette runs it in the threadpool, so the blocking graph
    never runs on the event loop.
    """
    from rse.retrieval_engine import run_query_stream

    def events():
        try:
            for event in run_query_stream(user_query=request.query, session_id=request.session_id):
                if event["event"] == "done":
                    payload = QueryResponse(**event["result"]).model_dump(mode="json")
                else:
                    payload = {key: value for key, value in event.items() if key != "event"}
                yield _sse(event["event"], payload)
        except Exception as exc:
            # Same rule as /query: only the type is logged; the browser gets no detail.
            logger.exception("retrieval /query/stream failed — %s", type(exc).__name__)
            yield _sse("error", {"detail": "Internal server error"})

    return StreamingResponse(
        events(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/sessions", response_model=list[SessionSummary])
def list_sessions(limit: int = Query(50, ge=1, le=200)):
    """
    The past chats, most recently active first (A27 chat list).

    One entry per session_id in message_store: the title is the first question
    (cut short), `turns` counts the questions. No new table; message_store's
    created_at gives the times. Old sessions disappear with the 30-day cleanup.
    """
    try:
        conn = get_connection()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(_LIST_SESSIONS_SQL, (limit,))
                    rows = cur.fetchall()
        finally:
            conn.close()
    except Exception as exc:
        logger.exception("list_sessions failed — %s", type(exc).__name__)
        raise HTTPException(status_code=500, detail="Internal server error")

    logger.info("Listed %d chat sessions", len(rows))
    return [
        SessionSummary(
            session_id=session_id,
            title=_session_title(first_question),
            last_active=_as_utc(last_active),
            turns=turns,
        )
        for session_id, first_question, last_active, turns in rows
    ]


@router.get("/sessions/{session_id}", response_model=SessionMessages)
def get_session_messages(session_id: str):
    """
    One past chat's messages, oldest first, for the chat screen (A27 resume).

    The stored reply is the readable answer text (message_store keeps no pills),
    so a resumed chat shows plain text; new questions in it get pills as usual.
    404 when the session has no messages (never existed, or the 30-day cleanup
    removed it); a DB error is also logged and answers 404 because
    load_conversation_history returns [] on failure.
    """
    _check_session_id(session_id)
    history = load_conversation_history(session_id)
    messages = [
        SessionMessage(role="user" if isinstance(message, HumanMessage) else "echo", text=str(message.content))
        for message in history
        if isinstance(message, (HumanMessage, AIMessage))
    ]
    if not messages:
        raise HTTPException(status_code=404, detail="Chat not found")
    logger.info("Loaded %d messages of a past chat", len(messages))
    return SessionMessages(session_id=session_id, messages=messages)


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
