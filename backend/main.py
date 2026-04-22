"""
main.py — Echo Backend Entry Point v0.2.0
Mounts all module routers and starts the FastAPI app.

Routers mounted:
    /ytc        — YouTube Connector (YTC)
    /chrome     — Chrome Connector (CHC)
    /retrieval  — Retrieval & Synthesis Engine (RSE)

TODO (next implementation task):
    - Mount Gmail router: from ingestion.gmail.router import router as gmail_router
    - Start gmail_poll_forever() as background task in lifespan
    - Start enp/enrichment_pipeline.run_pipeline() as background task (NOT
      backend/enrichment_worker — that one has no embeddings)
    - Fix Redis startup: redis_manager now owns the client, not ytc_module.redis_client
"""

import logging
import os

import redis.asyncio as aioredis
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from fastapi import FastAPI

from ingestion.youtube.youtube_connector import router as ytc_router
from ingestion.chrome.chrome_connector import router as chc_router
from backend.retrieval import router as retrieval_router

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL") or os.getenv("UPSTASH_REDIS_URL") or "redis://localhost:6379"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup — connect Redis
    import ingestion.youtube.youtube_connector as ytc_module
    try:
        ytc_module.redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
        await ytc_module.redis_client.ping()
        logger.info("Redis connected")
    except Exception as e:
        logger.warning(f"Redis not available — revisit detection disabled: {e}")
        ytc_module.redis_client = None

    yield

    # Shutdown — close Redis
    if ytc_module.redis_client:
        await ytc_module.redis_client.close()


app = FastAPI(
    title="Echo Backend",
    version="0.2.0",
    description="E.C.H.O — Efficient Contextualized History Orchestrator",
    lifespan=lifespan,
)

# Mount routers
app.include_router(ytc_router)
app.include_router(chc_router)
app.include_router(retrieval_router)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "echo-backend", "version": "0.2.0"}

