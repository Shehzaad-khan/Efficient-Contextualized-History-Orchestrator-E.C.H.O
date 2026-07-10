"""
Configuration module - Handles all settings, constants, and environment variables
"""

import logging
import os
import redis
from pathlib import Path
from dotenv import load_dotenv

from ste.security import validate_redis_tls_url

logger = logging.getLogger(__name__)

# ==============================
# LOAD ENVIRONMENT VARIABLES
# ==============================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

DATABASE_URL = os.getenv('DATABASE_URL')
REDIS_URL = os.getenv('REDIS_URL') or os.getenv('UPSTASH_REDIS_URL')

# Constants
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
CHECK_INTERVAL = 30  # seconds

# ==============================
# REDIS CLIENT (LAZY INITIALIZATION)
# ==============================

redis_client = None

def get_redis_client():
    """Get Redis client with lazy initialization and error handling"""
    global redis_client
    if redis_client is None:
        try:
            redis_client = redis.from_url(
                validate_redis_tls_url(REDIS_URL),
                decode_responses=True,
                socket_connect_timeout=5,
                socket_keepalive=True,
            )
            redis_client.ping()  # Test connection
            logger.info("Redis connected")
        except Exception as e:
            logger.warning("Redis connection failed: %s. Continuing without Redis caching.", e)
            redis_client = None
    return redis_client
