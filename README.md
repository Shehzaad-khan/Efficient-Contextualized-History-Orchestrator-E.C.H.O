# Echo — Personal Memory System

> *Your digital life, unified. Search across Gmail, Chrome, and YouTube in one natural language query.*

---

## What is Echo?

You read articles about OS concepts after an interview email. You watched YouTube tutorials the same week. A month later you can't find any of it — scattered across three platforms with no way to search them together.

**Echo fixes this.** It captures your emails, browsing, and watch history locally on your laptop, unifies them into one searchable memory, and lets you query it conversationally.

---

## Core Features

| Feature | What it does |
|---|---|
| **Unified Search** | One natural language query across Gmail, Chrome, and YouTube |
| **Intent Filtering** | Only saves content you genuinely engaged with — no noise |
| **Conversational Memory** | Multi-turn queries that remember context across turns |
| **Digital Wellbeing** | Time patterns and regret-based reflection — no scoring, no blocking |
| **Local-First Privacy** | All personal data stays on your laptop. Always. |

---

## How It Works

```
Your Gmail + Chrome + YouTube
         ↓
   Intent Filtering
   (only saves what you actually read/watched)
         ↓
   PostgreSQL + FAISS + Redis
   (local storage + semantic search)
         ↓
   LangGraph Retrieval Pipeline
   (LLM parses query → deterministic search → LLM synthesizes answer)
         ↓
   You get a coherent answer citing sources with dates
```

**Example query:** *"OS material I was reading after the TechCorp interview email"*

Echo finds the interview email, gets its timestamp, locates Chrome pages and YouTube videos about OS after that date, and synthesizes a single answer — citing each source.

---

## Architecture at a Glance

- **3 data sources** — Gmail (OAuth API), Chrome (custom MV3 extension), YouTube (Data API)
- **3 storage layers** — PostgreSQL (permanent), FAISS (semantic search), Redis (temp cache)
- **8 backend modules** — connectors, enrichment pipeline, LangGraph RSE, wellbeing analytics
- **15-table schema** — single-user, locked, no cloud sync
- **2 LLM calls per query** — parse intent + synthesize answer. Everything else is deterministic Python.

---

## Tech Stack

**Backend:** Python 3.10+, FastAPI, SQLAlchemy, LangGraph, LangChain, sentence-transformers (all-MiniLM-L6-v2)

**Storage:** PostgreSQL 15, FAISS, Redis 7

**Frontend:** React, TailwindCSS, Recharts, Chrome Extension (Manifest V3)

**LLM:** Claude Haiku / Gemini Flash / Ollama — plug-and-play via config
---

## Privacy First

- All personal data lives on your laptop — never in the cloud
- Incognito mode never tracked
- No keylogging, no form inputs, no app content (Slack, Jira)
- LLM API receives only your query and retrieved snippets — never your full database
- Complete data deletion controls built in

---

