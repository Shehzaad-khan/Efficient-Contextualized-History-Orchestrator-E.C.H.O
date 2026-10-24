"""
ingestion.chrome — Chrome page capture (architecture §6, CHC module)

    chrome_connector.py   FastAPI router: POST /chrome/ingest, POST /chrome/revisit-check, GET /chrome/rules
    intent_filter.py      two-phase intent gate + application-domain check
    revisit_tracker.py    24 h revisit flag in Redis

Import the modules directly, e.g. `from ingestion.chrome.chrome_connector import router`
(backend/main.py) — this file re-exports nothing.
"""
