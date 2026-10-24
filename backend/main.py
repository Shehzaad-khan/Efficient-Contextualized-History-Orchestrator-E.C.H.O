"""
main.py — Echo Backend Entry Point v0.2.0
Mounts all module routers and starts the FastAPI app.

Routers mounted:
    /ytc        — YouTube Connector (YTC)
    /chrome     — Chrome Connector (CHC)
    /gmail      — Gmail Connector (GMC)
    /retrieval  — Retrieval & Synthesis Engine (RSE)
    /wellbeing  — Digital Wellbeing (WBA), only when ECHO_WBA_ENABLED=1
    (no prefix) — DELETE /items/{id}, GET /export, GET|POST /settings (backend/items.py)

Background work started by the lifespan (one process):
    gmail-poller     asyncio task on the event loop; each poll runs in a worker thread
    enp-worker       the enrichment pipeline, a blocking loop in its own thread;
                     stopped with a threading.Event, between batches
    message-cleanup  one-off at startup: deletes chat history older than 30 days
    youtube-sign-in  one-off daemon thread at startup: opens the browser sign-in
                     only when there is no usable YouTube token
    gmail-sign-in    one-off daemon thread at startup: opens the browser sign-in
                     only when there is no usable Gmail token

Shutdown order: stop the Gmail poller, ask ENP to stop and wait for its current
batch, then close the shared async Redis client.
"""

import asyncio
import importlib.util
import logging
import os
import sys
import threading
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Load .env before importing any project module, so modules that read
# DATABASE_URL / REDIS_URL at import time see them. The explicit path makes it
# work whatever directory uvicorn was started from.
load_dotenv(PROJECT_ROOT / ".env")

# Ingested email/page text is arbitrary Unicode (currency signs, CJK, emoji).
# On Windows stdout defaults to cp1252, so printing such text raises
# UnicodeEncodeError mid-ingestion and aborts the whole batch. Force UTF-8 and
# replace anything unmappable so logging can never take the pipeline down.
for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        _stream.reconfigure(encoding="utf-8", errors="replace")

# Configure logging BEFORE importing the modules below, so log lines they emit
# while importing already reach the console and logs/echo.log.
from backend.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

from ingestion.youtube.youtube_api_client import sign_in_youtube
from ingestion.gmail.gmail_api import sign_in_gmail
from ingestion.youtube.youtube_connector import router as ytc_router
from ingestion.chrome.chrome_connector import router as chc_router
from ingestion.gmail.router import poll_forever as gmail_poll_forever
from ingestion.gmail.router import router as gmail_router
from backend.retrieval import router as retrieval_router
from backend.items import router as items_router
from rse.conversation_memory import cleanup_old_sessions
from ste.redis_manager import get_async_client
from ste import postgresql_manager

ENP_THREAD_NAME = "enp-worker"
YOUTUBE_SIGN_IN_THREAD_NAME = "youtube-sign-in"
GMAIL_SIGN_IN_THREAD_NAME = "gmail-sign-in"

# How long shutdown waits for ENP to finish its current batch (10 items by
# default). If it takes longer, shutdown carries on; Python's exit still waits
# for the thread, which ends as soon as the batch is done.
ENP_STOP_TIMEOUT_SECONDS = 60


def _run_enp_in_named_thread(stop_event: threading.Event) -> None:
    """Run the ENP loop with a readable thread name, so its log lines show
    [enp-worker] instead of the default executor name [asyncio_0]."""
    thread = threading.current_thread()
    original_name = thread.name
    thread.name = ENP_THREAD_NAME
    try:
        from enp.enrichment_pipeline import run_pipeline

        run_pipeline(stop_event=stop_event)
    finally:
        # The thread belongs to asyncio's default executor; give its name back
        # when run_pipeline returns, so later executor work isn't mislabelled.
        thread.name = original_name


def start_enp_worker(stop_event: threading.Event) -> asyncio.Task:
    async def runner() -> None:
        await asyncio.to_thread(_run_enp_in_named_thread, stop_event)

    return asyncio.create_task(runner(), name="enp-worker")


async def _stop_enp_worker(enp_task: asyncio.Task, stop_event: threading.Event) -> None:
    """Ask ENP to stop and wait for its current batch to finish.

    Cancelling the asyncio task would not stop the thread (a running thread
    can't be cancelled), so the stop goes through the Event. shield() keeps a
    timeout from cancelling the task itself.
    """
    stop_event.set()
    try:
        await asyncio.wait_for(asyncio.shield(enp_task), timeout=ENP_STOP_TIMEOUT_SECONDS)
        logger.info("ENP background worker stopped")
    except asyncio.TimeoutError:
        logger.warning(
            "ENP still finishing its batch after %d s; exit will wait for it",
            ENP_STOP_TIMEOUT_SECONDS,
        )
    except Exception:
        logger.exception("ENP background worker ended with an error")


async def _connect_redis() -> bool:
    """Create the shared async Redis client and check it answers.

    Redis is optional at startup: every Redis caller already handles a failure
    (YouTube revisit -> first visit, settings -> defaults, reminders -> none).
    """
    try:
        await get_async_client().ping()
        logger.info("Redis connected")
        return True
    except Exception as exc:
        # The error text carries host:port at most, never the password.
        logger.warning("Redis not reachable at startup; Redis features fall back until it is: %s", exc)
        return False


async def _close_redis() -> None:
    try:
        await get_async_client().aclose()
        logger.info("Redis async client closed")
    except Exception:
        logger.exception("Closing the Redis async client failed")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    redis_connected = await _connect_redis()

    gmail_task = asyncio.create_task(gmail_poll_forever(), name="gmail-poller")
    logger.info("Gmail background polling started")

    enp_stop_event = threading.Event()
    enp_task = start_enp_worker(enp_stop_event)
    logger.info("ENP background worker started")

    # One DB round trip; run it in a worker thread so startup doesn't wait for it.
    # cleanup_old_sessions logs its own result and never raises.
    cleanup_task = asyncio.create_task(asyncio.to_thread(cleanup_old_sessions), name="message-cleanup")

    # Plain daemon threads, not asyncio.to_thread: either sign-in can wait in
    # the browser for as long as the user takes, and Python's exit drops daemon
    # threads instead of waiting for them. The sign-in functions log their own
    # result and never raise.
    threading.Thread(target=sign_in_youtube, name=YOUTUBE_SIGN_IN_THREAD_NAME, daemon=True).start()
    threading.Thread(target=sign_in_gmail, name=GMAIL_SIGN_IN_THREAD_NAME, daemon=True).start()

    try:
        yield
    finally:
        # Shutdown
        for task in (gmail_task, cleanup_task):
            task.cancel()
        await asyncio.gather(gmail_task, cleanup_task, return_exceptions=True)

        await _stop_enp_worker(enp_task, enp_stop_event)
        postgresql_manager.dispose_engine()

        if redis_connected:
            await _close_redis()


app = FastAPI(
    title="Echo Backend",
    version="0.2.0",
    description="E.C.H.O — Efficient Contextualized History Orchestrator",
    lifespan=lifespan,
)

_ALLOWED_ORIGINS = [
    o.strip()
    for o in os.getenv(
        "ECHO_ALLOWED_ORIGINS",
        "http://localhost:3000,http://localhost:8000,http://127.0.0.1:3000,http://127.0.0.1:8000",
    ).split(",")
    if o.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["*"],
)

# Mount routers
app.include_router(ytc_router)
app.include_router(chc_router)
app.include_router(gmail_router)
app.include_router(retrieval_router)
# Wellbeing (WBA) is an optional feature module. It mounts only when the wba
# package is present AND ECHO_WBA_ENABLED=1, so the app boots and serves every
# other route when wba/ is absent. Re-enable with ECHO_WBA_ENABLED=1.
if os.getenv("ECHO_WBA_ENABLED", "0") == "1" and importlib.util.find_spec("wba") is not None:
    from backend.wellbeing import router as wellbeing_router

    app.include_router(wellbeing_router)
    logger.info("Wellbeing module enabled")
else:
    logger.info("Wellbeing module disabled (set ECHO_WBA_ENABLED=1 to enable)")
app.include_router(items_router)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "echo-backend", "version": "0.2.0"}
