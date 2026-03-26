"""
main.py — Echo Backend Entry Point
Mounts all module routers and starts the FastAPI app.
"""

import logging
import os

import redis.asyncio as aioredis
from dotenv import load_dotenv
from fastapi import FastAPI
from contextlib import asynccontextmanager

from ytc.youtube_connector import router as ytc_router, redis_client as ytc_redis

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup — connect Redis
    import ytc.youtube_connector as ytc_module
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
    version="0.1.0",
    lifespan=lifespan,
)

# Mount routers
app.include_router(ytc_router)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "echo-backend"}
