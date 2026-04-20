"""
RSE module configuration.
To switch LLM provider, change 'provider' and update the model strings below.
Supported providers: 'google' | 'anthropic' | 'ollama'
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# ── Single change point to swap LLM provider ─────────────────────────────────
LLM_CONFIG: dict = {
    "provider": "google",
    # gemini-2.5-flash-preview-04-17 is specified in the architecture doc but not
    # yet available via the v1beta API as of April 2026. Using the GA release.
    "parser_model": "gemini-2.5-flash",
    "synthesizer_model": "gemini-2.5-flash",
    # temperature overrides (optional — init_chat_model accepts these as kwargs)
    "parser_temperature": 0.0,
    "synthesizer_temperature": 0.3,
}
# ─────────────────────────────────────────────────────────────────────────────

DATABASE_URL: str = os.getenv("DATABASE_URL", "")
REDIS_URL: str = os.getenv("REDIS_URL", "")

# Retrieval tuning constants
POSTGRES_RESULT_LIMIT = 1000
FAISS_TOP_K = 20
MAX_WIDEN_ATTEMPTS = 3
CONVERSATION_HISTORY_DAYS = 30
