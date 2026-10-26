# Echo — Personal Memory System

> *Your digital life, unified. Search across Gmail, Chrome, and YouTube in one natural language query.*

**E.C.H.O — Efficient Contextualized History Orchestrator** · Final-year capstone project · Local-first · Single-user

---

## What is Echo?

You read articles about OS concepts after an interview email. You watched YouTube tutorials the same week. A month later you can't find any of it — scattered across three platforms with no way to search them together.

**Echo fixes this.** It captures your emails, browsing, and watch history locally on your laptop, unifies them into one searchable memory, and lets you query it conversationally:

> *"OS material I was reading after the TechCorp interview email"*

Echo finds the interview email, resolves its timestamp as a hard time anchor, retrieves Chrome pages and YouTube videos about operating systems after that date, and synthesizes one answer citing every source.

---

## Core Features

| Feature | What it does |
|---|---|
| **Hybrid Unified Search** | Keyword (PostgreSQL FTS) + semantic (FAISS) retrieval in parallel, fused by Reciprocal Rank Fusion, re-ranked by a local cross-encoder |
| **Conversational Memory** | Multi-turn sessions — *"find Chrome pages about that topic after that email"* resolves references from earlier turns |
| **Intent Filtering** | Only saves content you genuinely engaged with — a 3-second misclick is discarded, a 2-minute read is kept |
| **On-Demand Attachments** | Email PDFs/DOCX extracted in full only when a search needs them; binaries never stored |
| **Local-First Privacy** | All personal data stays in PostgreSQL on your laptop. The LLM sees only your query and bounded text excerpts of the top results. |

---

## How It Works

```
 Gmail (OAuth API)   Chrome (MV3 extension)   YouTube (extension + Data API)
        │                     │                        │
        └──────── intent gates (per-source rules) ─────┘
                              ▼
        PostgreSQL (permanent) · FAISS (semantic) · Redis (24h cache)
                              ▲
              Enrichment Pipeline (background):
              clean → classify (4-stage) → embed → index
                              │
                              ▼
              LangGraph Retrieval Engine (agent loop; usually 2 LLM calls, max 6):
              Luna calls search_memories → time anchor → [keyword ∥ semantic] → RRF
              → quality gate → (widen ≤3×) → cross-encoder re-rank
              → neighborhood context → attachment text → Luna answers (cited)
                              │
                              ▼
        React dashboard ("Midnight Study")
```

---

## Repository Map

| Directory | Module | What lives there |
|---|---|---|
| `ingestion/gmail/` | **GMC** | Gmail OAuth, polling, email + attachment-metadata ingestion, engagement updates |
| `ingestion/chrome/` | **CHC** | Two-phase intent gate, revisit tracking, page ingestion endpoint |
| `ingestion/youtube/` | **YTC** | Video detection, watch-time heartbeats, Data API metadata, Shorts classification |
| `extension/` | — | MV3 extension: per-site trackers, service worker |
| `ste/` | **STE** | PostgreSQL / FAISS / Redis managers, token-file helpers, capture-settings store |
| `enp/` | **ENP** | Background enrichment: cleaning, 4-stage classifier, embeddings, Tier-2 attachment extraction |
| `rse/` | **RSE** | LangGraph hybrid retrieval graph, conversation memory, LLM synthesis |
| `backend/` | **UIL** | FastAPI gateway (`main.py`) — all routers, data export/deletion/settings |
| `frontend/` | **UIL** | React + TypeScript dashboard: Recall |
| `scripts/` | — | DB setup (`setup_db.py`), ENP queue status (`enp_status.py`) |
| `docs/Timeless_docs/` | — | **`Echo_System_Design.md` — architecture + DB design, the source of truth** |

---

## Running Echo

```bash
# 1. Backend (starts Gmail poller + enrichment worker with it)
pip install -r requirements.txt
python scripts/setup_db.py          # one-time: creates the 17-table schema
uvicorn backend.main:app --port 8000

# 2. Frontend — http://localhost:3000
cd frontend && npm install && npm run dev

# 3. Extension: chrome://extensions → Developer mode → Load unpacked → extension/
```

Configuration via `.env`: `DATABASE_URL` (local PostgreSQL), `REDIS_URL` (local Redis), plus the key for the LLM provider set in `rse/config.py` `LLM_CONFIG` (OpenAI today; plug-and-play — one place to switch). `credentials.json` supplies Google OAuth; the tokens are plain local files (`token_gmail.json`, `token_youtube.json`, decision A35).

The frontend runs without the backend too — it flips into a clearly-badged **demo mode** with seeded data, useful for demos.

---

## Design Decisions Worth Knowing

- **Usually 2 LLM calls per query, max 6** — Luna calls a `search_memories` tool for up to 5 searches, then must answer (decision A26, built 2026-10-26). The search itself is deterministic, inspectable Python.
- **Rank fusion, not score fusion** — `ts_rank` and cosine similarity live on incomparable scales; RRF (k=60) merges by rank.
- **Everything rebuildable but PostgreSQL** — FAISS and Redis can be regenerated from it at any time.

---

## Tech Stack

**Backend:** Python 3.10+ · FastAPI · SQLAlchemy · LangGraph / LangChain · sentence-transformers (snowflake-arctic-embed-xs, 384-dim) · cross-encoder (ms-marco-MiniLM-L-6-v2) · pypdf / pdfplumber / python-docx

**Storage:** PostgreSQL 16 (local Windows service) · FAISS (local) · Redis (local, in WSL Ubuntu) — fully local since decision A31; Neon and Upstash are no longer used

**Frontend:** React 18 + TypeScript + Vite · Tailwind (custom token system) · Framer Motion + GSAP + Lenis · Chrome Extension (Manifest V3)

**LLM:** OpenAI GPT-6 Luna on the Responses API, via LangChain (decision A25) — plug-and-play via `rse/config.py` `LLM_CONFIG`

---

## Privacy First

- All personal data lives on your laptop — never permanently in the cloud
- Incognito is never tracked; typed text is never captured; app content (Slack, Notion, Jira) is never read
- The LLM API receives only your query and bounded text excerpts of the top results (up to 8,000 characters each for the best 3, 2,000 for the rest) and attachment text (up to 40,000 characters per file) — never the database or your whole store
- Built-in controls: per-source capture toggles, domain/sender exclusions, soft deletion, full JSON/CSV export
