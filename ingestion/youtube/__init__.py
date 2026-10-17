"""
ingestion.youtube — YouTube capture (architecture §7, YTC module)

    youtube_connector.py    FastAPI router: POST /ytc/video-detected, /ytc/heartbeat, /ytc/video-closed
    youtube_api_client.py   Data API metadata + best-effort transcript; encrypted token handling
    video_classifier.py     Shorts vs long-form (§7.4)

Import the modules directly, e.g. `from ingestion.youtube.youtube_connector import router`
(backend/main.py) — this file re-exports nothing.
"""
