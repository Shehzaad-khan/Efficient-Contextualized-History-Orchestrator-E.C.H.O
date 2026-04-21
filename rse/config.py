"""
config.py — RSE Module
Echo Personal Memory System

LLM provider config, DB URL, and tuning constants.
Switching LLM provider is a one-line change in LLM_CONFIG.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── Database ──────────────────────────────────────────────────────────────────
DATABASE_URL: str = os.getenv("DATABASE_URL", "")

# ── LLM Config ────────────────────────────────────────────────────────────────
# Uses LangChain init_chat_model factory — swap provider by changing "model".
# Supported values (set via LLM_MODEL env var or change default here):
#   Gemini:  "gemini/gemini-1.5-flash", "gemini/gemini-1.5-pro"
#   Claude:  "anthropic/claude-3-haiku-20240307"
#   OpenAI:  "openai/gpt-4o-mini"
LLM_MODEL: str = os.getenv("LLM_MODEL", "gemini-1.5-flash")
LLM_PROVIDER: str = os.getenv("LLM_PROVIDER", "google_genai")

# Two separate model tiers: parser is cheap/fast, synthesizer is higher quality
PARSER_MODEL: str = os.getenv("PARSER_MODEL", LLM_MODEL)
SYNTHESIZER_MODEL: str = os.getenv("SYNTHESIZER_MODEL", LLM_MODEL)

LLM_TEMPERATURE: float = 0.0  # Deterministic — same query, same intent parse

# ── Retrieval Tuning ──────────────────────────────────────────────────────────
POSTGRES_SEARCH_LIMIT: int = 1000   # Max candidate rows from Postgres
FAISS_TOP_K: int = 20               # Top-K from FAISS search

# Quality thresholds
STRONG_RESULT_MIN_COUNT: int = 3    # >= 3 results with source match = strong
WEAK_RESULT_MIN_COUNT: int = 1      # 1-2 results = weak, trigger widen

# Scope widening
MAX_WIDEN_ATTEMPTS: int = 3

# ── Conversation Memory ────────────────────────────────────────────────────────
CONVERSATION_HISTORY_DAYS: int = 30   # Purge sessions older than this
MESSAGE_STORE_TABLE: str = "message_store"
