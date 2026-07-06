"""
main.py — Echo Backend Entry Point v0.2.0
Mounts all module routers and starts the FastAPI app.

Routers mounted:
    /ytc        — YouTube Connector (YTC)
    /chrome     — Chrome Connector (CHC)
    /gmail      — Gmail Connector (GMC)
    /retrieval  — Retrieval & Synthesis Engine (RSE)

Background workers:
    Gmail polling and the enrichment pipeline start with the API process.
"""

import asyncio
import logging
import os

import redis.asyncio as aioredis
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from ste.security import validate_redis_tls_url
from ingestion.youtube.youtube_connector import router as ytc_router
from ingestion.chrome.chrome_connector import router as chc_router
from ingestion.gmail.router import poll_forever as gmail_poll_forever
from ingestion.gmail.router import router as gmail_router
from backend.retrieval import router as retrieval_router
from backend.wellbeing import router as wellbeing_router
from backend.auth_routes import router as auth_router

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL") or os.getenv("UPSTASH_REDIS_URL") or "redis://localhost:6379"


def start_enp_worker() -> asyncio.Task:
    async def runner() -> None:
        from enp.enrichment_pipeline import run_pipeline

        await asyncio.to_thread(run_pipeline)

    return asyncio.create_task(runner(), name="enp-worker")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup — connect Redis
    import ingestion.youtube.youtube_connector as ytc_module
    background_tasks: list[asyncio.Task] = []
    try:
        ytc_module.redis_client = aioredis.from_url(validate_redis_tls_url(REDIS_URL), decode_responses=True)
        await ytc_module.redis_client.ping()
        logger.info("Redis connected")
    except Exception as e:
        logger.warning(f"Redis not available — revisit detection disabled: {e}")
        ytc_module.redis_client = None

    background_tasks.append(asyncio.create_task(gmail_poll_forever(), name="gmail-poller"))
    logger.info("Gmail background polling started")

    background_tasks.append(start_enp_worker())
    logger.info("ENP background worker started")

    try:
        yield
    finally:
        for task in background_tasks:
            task.cancel()
        if background_tasks:
            await asyncio.gather(*background_tasks, return_exceptions=True)

        # Shutdown — close Redis
        if ytc_module.redis_client:
            await ytc_module.redis_client.close()


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
app.include_router(auth_router)
app.include_router(ytc_router)
app.include_router(chc_router)
app.include_router(gmail_router)
app.include_router(retrieval_router)
app.include_router(wellbeing_router)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "echo-backend", "version": "0.2.0"}

