"""
main.py - Echo Backend Entry Point
Mounts all module routers and starts the FastAPI app.
"""

import asyncio
import logging
import os
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from dotenv import load_dotenv
from fastapi import FastAPI

from backend.enrichment_worker import process_pending_items
from backend import retrieval
from ingestion.chrome.chrome_connector import router as chrome_router
from ingestion.gmail.router import poll_forever as gmail_poll_forever
from ingestion.gmail.router import router as gmail_router
from ingestion.youtube.youtube_connector import router as youtube_router

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL") or os.getenv("UPSTASH_REDIS_URL") or "redis://localhost:6379"


async def enrichment_poll_forever() -> None:
    while True:
        try:
            await asyncio.to_thread(process_pending_items)
        except Exception as exc:
            logger.exception("Enrichment poll failed: %s", exc)
        await asyncio.sleep(15)


@asynccontextmanager
async def lifespan(app: FastAPI):
    import ingestion.youtube.youtube_connector as ytc_module

    background_tasks: list[asyncio.Task] = []
    try:
        ytc_module.redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
        await ytc_module.redis_client.ping()
        logger.info("Redis connected")
    except Exception as exc:
        logger.warning("Redis not available - revisit detection disabled: %s", exc)
        ytc_module.redis_client = None

    background_tasks.append(asyncio.create_task(gmail_poll_forever()))
    background_tasks.append(asyncio.create_task(enrichment_poll_forever()))

    yield

    for task in background_tasks:
        task.cancel()
    if background_tasks:
        await asyncio.gather(*background_tasks, return_exceptions=True)
    if ytc_module.redis_client:
        await ytc_module.redis_client.close()


app = FastAPI(
    title="Echo Backend",
    version="0.2.0",
    lifespan=lifespan,
)

app.include_router(youtube_router)
app.include_router(chrome_router)
app.include_router(gmail_router)
app.include_router(retrieval.router)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "echo-backend",
        "modules": ["youtube", "chrome", "gmail", "retrieval"],
    }
