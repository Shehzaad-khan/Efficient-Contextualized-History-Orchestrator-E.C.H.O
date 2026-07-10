"""
RSE module configuration.
To switch LLM provider, change 'provider' and update the model strings below.
Supported providers: 'google_genai' | 'anthropic' | 'ollama'
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# ── Single change point to swap LLM provider ─────────────────────────────────
LLM_CONFIG: dict = {
    # 2026-07-09: switched google_genai → openai. The Google key's free tier
    # allows only 20 requests/day PER MODEL (each query = 2 calls, each ENP
    # item classification = 1 more), which made the system unusable within
    # minutes of real use. The OpenAI key is a funded paid account;
    # gpt-4.1-mini costs ~$0.003/query at Echo's token sizes, handles strict
    # JSON (Call 1) and grounded prose (Call 2) well, and accepts the
    # temperature overrides below (reasoning models don't).
    # GOOGLE_API_KEY stays in .env — switch back by flipping these 3 strings.
    "provider": "openai",
    "parser_model": "gpt-4.1-mini",
    "synthesizer_model": "gpt-4.1-mini",
    # temperature overrides (optional — init_chat_model accepts these as kwargs)
    "parser_temperature": 0.0,
    "synthesizer_temperature": 0.3,
    # Quota errors (429 RESOURCE_EXHAUSTED on a DAILY limit) are not transient:
    # langchain's default ~6 exponential-backoff retries burned ~70s per LLM
    # call before failing. Fail fast instead — the graph has deterministic
    # fallbacks for both calls.
    "max_retries": 2,
}
# ─────────────────────────────────────────────────────────────────────────────

DATABASE_URL: str = os.getenv("DATABASE_URL", "")
REDIS_URL: str = os.getenv("REDIS_URL", "")

# ── Hybrid retrieval ──────────────────────────────────────────────────────────
# Keyword branch: candidates returned by postgres_keyword_search, FTS-rank ordered.
PG_KEYWORD_LIMIT = 200
# Semantic branch: FAISS hits fetched per query variant before pooling.
FAISS_TOP_K_PER_VARIANT = 30
# Multiplier applied to FAISS k when widen_scope sets full_faiss_scan (attempt 3).
FULL_SCAN_K_MULTIPLIER = 3
# Reciprocal Rank Fusion constant: score = Σ 1 / (RRF_K + rank).
RRF_K = 60
# Fused candidates kept after merge_and_rrf — this pool feeds the cross-encoder.
RRF_POOL_SIZE = 50

# ── Cross-encoder re-ranking ──────────────────────────────────────────────────
CROSS_ENCODER_MODEL = "cross-encoder/ms-marco-MiniLM-L-6-v2"
# final_score = CROSS_ENCODER_WEIGHT * sigmoid(ce_logit) + EFFORT_WEIGHT * effort_score
CROSS_ENCODER_WEIGHT = 0.8
EFFORT_WEIGHT = 0.2
# Characters of item text (title + snippet) fed to the cross-encoder per pair.
RERANK_DOC_CHAR_LIMIT = 512

# ── Quality evaluation ────────────────────────────────────────────────────────
# Top candidate cosine similarity below this → result_quality 'weak'.
MIN_TOP_SIMILARITY = 0.35
MIN_RESULT_COUNT = 2

# ── Neighborhood extension ────────────────────────────────────────────────────
NEIGHBOR_TOP_ITEMS = 3        # ranked items that get neighborhood context
NEIGHBOR_WINDOW_MINUTES = 5   # ± window for Chrome/YouTube temporal neighbors
NEIGHBORS_PER_ITEM = 5        # max neighbors fetched per top item

# ── Time anchor resolution ────────────────────────────────────────────────────
ANCHOR_CANDIDATE_LIMIT = 5    # keyword candidates inspected when resolving anchor

# ── Synthesis ─────────────────────────────────────────────────────────────────
SYNTHESIS_TOP_RESULTS = 10    # ranked results included in LLM Call 2 context
SNIPPET_CHAR_LIMIT = 500      # raw-text excerpt length per result (plain truncation)

# ── Legacy / shared constants ─────────────────────────────────────────────────
POSTGRES_RESULT_LIMIT = 1000  # used only by skip_postgres_filter fallback path
FAISS_TOP_K = 20
MAX_WIDEN_ATTEMPTS = 3
CONVERSATION_HISTORY_DAYS = 30
