"""
config.py — Gmail ingestion settings
Echo Personal Memory System

Holds the Gmail connector's constants.

    SCOPES          OAuth scope (read-only Gmail), used by gmail_api.authenticate_gmail
    CHECK_INTERVAL  seconds between polls (router.poll_forever)

Redis: the Gmail connector no longer uses Redis directly. The token lives in
the local file token_gmail.json, and capture settings are read through
ste.settings_store.
"""

from pathlib import Path

from dotenv import load_dotenv

# Load .env when the Gmail package is imported so its configuration is available
# even when the connector is used outside the backend entry point.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
CHECK_INTERVAL = 30  # seconds between Gmail polls
