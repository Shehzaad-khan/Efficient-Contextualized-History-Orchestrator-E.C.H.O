---
name: verify
description: Run E.C.H.O end to end in a cloud/Linux container (throwaway PostgreSQL 16 + Redis, real backend, Vite frontend, Playwright) to verify a change at runtime.
---

# Verifying E.C.H.O at runtime (cloud container, no live database)

Everything below ran in a Linux container with no access to the owner's Windows database. Use throwaway services; never point at Neon/Upstash/the owner's `echo_db`.

## Services
- PostgreSQL 16 binaries are at `/usr/lib/postgresql/16/bin` (psql, initdb, pg_ctl); `redis-server` is on PATH. Postgres refuses to run as root:
  `mkdir /tmp/pgverify && chown postgres /tmp/pgverify`, then
  `su postgres -c "initdb -D /tmp/pgverify/data -A trust && pg_ctl -D /tmp/pgverify/data -o '-p 55432 -k /tmp/pgverify' -w start"`,
  then create role `echo` (SUPERUSER LOGIN) and a database.
- Redis: `redis-server --port 56379 --save "" --daemonize yes`.
- Schema: `DATABASE_URL=... python scripts/setup_db.py` (fresh install). To test a migration, build the OLD schema first from the previous commit (`git show <rev>:scripts/setup_db.py`).
- Env for the backend: `DATABASE_URL=postgresql://echo@/<db>?host=/tmp/pgverify&port=55432`, `REDIS_URL=redis://127.0.0.1:56379/0`, `ECHO_FAISS_INDEX_PATH=<tmp>/echo.index`, `ECHO_WBA_ENABLED=1` (needed for `/wellbeing/*`, e.g. the Timeline page).

## Python env
`requirements.txt` cannot be installed as written (see `docs/review/ci_cd_guide.md`) and Python 3.13 rejects the old pins. Make a venv in the scratchpad and install unpinned deps (fastapi, uvicorn, sqlalchemy, psycopg2-binary, redis, faiss-cpu, numpy, langgraph, langchain-*, mcp, etc.). **Pin `SQLAlchemy==2.0.49`**: SQLAlchemy 2.1 maps `postgresql://` to psycopg3 and the app breaks.

## Run
- Backend: `python -m uvicorn backend.main:app --port 8000` (start it with a pid file and kill that pid; `pkill -f uvicorn` also kills your own shell if the command line contains the string). Expect two harmless sign-in errors (no Google `credentials.json`). The ENP worker and retrieval need the embedding model and an LLM key, which the container lacks, so `/retrieval/query` cannot be driven; seed vectors yourself with `ste.faiss_manager.get_manager().add(id, unit_vector_384d, text)`.
- Frontend: `cd frontend && npm ci && npm i --no-save @rollup/rollup-linux-x64-gnu@<rollup version> && npx vite --port 3000 --strictPort` (port must stay 3000 for CORS).
- Drive the UI with Playwright (`require('playwright')` works globally; set `NODE_PATH=$(npm root -g)`). The API base is `http://localhost:8000`.

## Flows worth driving
- Timeline (`/timeline`): expand an entry, Delete, Keep, Delete, confirm; check Postgres child tables, the FAISS file (reload with `FAISSManager(path).load_index()`) and `redis-cli --scan --pattern 'echo:*'`.
- Retention: `POST /settings {"retention":"1y"}`, insert an item with `created_at` 800 days ago, restart the backend, read the log line `Retention 1y: deleted ...`.
- Chrome capture: `POST /chrome/ingest` needs `content_extract` of roughly 200+ words and `word_count`; re-posting a deleted URL should save a new item.
- Failure probes: stop Redis then `DELETE /items/{id}` (still 200, warning logged); six parallel deletes (one 200, rest 404).
