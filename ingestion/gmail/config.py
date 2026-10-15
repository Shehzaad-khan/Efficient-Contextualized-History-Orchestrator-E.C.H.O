"""
config.py — Gmail ingestion settings
Echo Personal Memory System

Holds the Gmail connector's constants.

    SCOPES          OAuth scope (read-only Gmail), used by gmail_api.authenticate_gmail
    CHECK_INTERVAL  seconds between polls (router.poll_forever)

Redis: the Gmail connector no longer uses Redis directly. The token lives in
the encrypted file token_gmail.enc, and capture settings are read through
ste.settings_store.
"""

from pathlib import Path

from dotenv import load_dotenv

# Load .env as soon as the Gmail package is imported. backend/main.py imports
# this package before its own load_dotenv() call runs (tracked as C7), so
# without this, DATABASE_URL / REDIS_URL could be unset at import time.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
CHECK_INTERVAL = 30  # seconds between Gmail polls
