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
| **Local-First Privacy** | All personal data stays in PostgreSQL on your laptop. The LLM sees only your query and short snippets. |

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
              LangGraph Retrieval Engine (exactly 2 LLM calls):
              parse_intent → time anchor → [keyword ∥ semantic] → RRF
              → quality gate → (widen ≤3×) → cross-encoder re-rank
              → neighborhood context → attachments / live poll → synthesize
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
| `ste/` | **STE** | PostgreSQL / FAISS / Redis managers, encryption, capture-settings store |
| `enp/` | **ENP** | Background enrichment: cleaning, 4-stage classifier, embeddings, Tier-2 attachment extraction |
| `rse/` | **RSE** | LangGraph hybrid retrieval graph, conversation memory, LLM synthesis |
| `backend/` | **UIL** | FastAPI gateway (`main.py`) — all routers, data export/deletion/settings |
| `frontend/` | **UIL** | React + TypeScript dashboard: Recall |
| `scripts/` | — | DB setup, ENP maintenance (rebuild FAISS, recompute centroids) |
| `docs/Timeless_docs/` | — | **Architecture & DB design documents — the source of truth** |

---

## Running Echo

```bash
# 1. Backend (starts Gmail poller + enrichment worker with it)
pip install -r requirements.txt
python scripts/setup_db.py          # one-time: creates the 16-table schema
uvicorn backend.main:app --port 8000

# 2. Frontend — http://localhost:3000
cd frontend && npm install && npm run dev

# 3. Extension: chrome://extensions → Developer mode → Load unpacked → extension/
```

Configuration via `.env`: `DATABASE_URL` (PostgreSQL), `REDIS_URL`/`UPSTASH_REDIS_URL`, plus the LLM provider key set in `rse/config.py` (plug-and-play: Anthropic / Google / Ollama — one line to switch). `credentials.json` supplies Google OAuth; tokens are stored encrypted.

The frontend runs without the backend too — it flips into a clearly-badged **demo mode** with seeded data, useful for demos.

---

## Design Decisions Worth Knowing

- **Exactly 2 LLM calls per query** — intent parsing and answer synthesis. Every retrieval step between them is deterministic, inspectable Python.
- **Rank fusion, not score fusion** — `ts_rank` and cosine similarity live on incomparable scales; RRF (k=60) merges by rank.
- **Everything rebuildable but PostgreSQL** — FAISS and Redis can be regenerated from it at any time.

---

## Tech Stack

**Backend:** Python 3.10+ · FastAPI · SQLAlchemy · LangGraph / LangChain · sentence-transformers (all-MiniLM-L6-v2, 384-dim) · cross-encoder (ms-marco-MiniLM-L-6-v2) · pypdf / pdfplumber / python-docx

**Storage:** PostgreSQL 15+ (Neon in dev) · FAISS (local) · Redis 7 (Upstash in dev)

**Frontend:** React 18 + TypeScript + Vite · Tailwind (custom token system) · Framer Motion + GSAP + Lenis · Chrome Extension (Manifest V3)

**LLM:** Claude Haiku / Gemini Flash / Ollama — plug-and-play via one config change

---

## Privacy First

- All personal data lives on your laptop — never permanently in the cloud
- Incognito is never tracked; typed text is never captured; app content (Slack, Notion, Jira) is never read
- The LLM API receives only your query and short result snippets — never raw bodies, transcripts, or the database
- Built-in controls: per-source capture toggles, domain/sender exclusions, soft deletion, full JSON/CSV export
