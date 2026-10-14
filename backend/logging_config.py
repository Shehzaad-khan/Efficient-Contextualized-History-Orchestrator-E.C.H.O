"""
logging_config.py — the one place E.C.H.O configures logging.

Call setup_logging() once, at process start:
    - backend/main.py calls it before importing any module, so every log line
      from the backend process goes through it: routers, ingestion connectors,
      the Gmail poller, the ENP worker thread, every LangGraph node in rse/,
      wba/, and every ste/ helper.
    - a standalone script calls it at the start of its main().

Every other file only does:
    logger = logging.getLogger(__name__)
Those per-module loggers have no handlers of their own. Their records travel
up to the root logger, which owns the two handlers set here. `__name__` only
adds the module path (e.g. `ste.postgresql_manager`) to each line.

Where logs go:
    - the console (stderr), as before
    - logs/echo.log in the project root (gitignored). At midnight the file is
      renamed to echo.log.YYYY-MM-DD and a new one starts. Old files are NEVER
      deleted automatically (backupCount=0); clean logs/ by hand when needed.

Thread safety: logging handlers carry their own lock, so the ENP thread, the
YouTube executor threads and request threads can all log at the same time.
"""

from __future__ import annotations

import logging
import sys
import threading
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE = LOG_DIR / "echo.log"

# INFO while we develop: every important event, not only errors.
LOG_LEVEL = logging.INFO

# Example: 2026-10-14 10:32:01 INFO    [enp-worker] enp.enrichment_pipeline: enriched 12 items
LOG_FORMAT = "%(asctime)s %(levelname)-7s [%(threadName)s] %(name)s: %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Libraries that log every HTTP request or model load at INFO. Raised to
# WARNING so our own INFO lines are not buried. Their warnings/errors still show.
_NOISY_LIBRARIES = (
    "httpx",
    "httpcore",
    "urllib3",
    "openai",
    "sentence_transformers",
    "transformers",
    "huggingface_hub",
    "faiss",
    "googleapiclient.discovery_cache",
)

# uvicorn installs its own handlers on these loggers (and stops them from
# propagating) before it imports our app. We remove those handlers and let
# them propagate, so startup and access lines also reach logs/echo.log.
_UVICORN_LOGGERS = ("uvicorn", "uvicorn.error", "uvicorn.access")

_configured = False
_setup_lock = threading.Lock()


def setup_logging() -> None:
    """Configure the root logger once. Later calls do nothing."""
    global _configured
    with _setup_lock:
        if _configured:
            return

        LOG_DIR.mkdir(parents=True, exist_ok=True)
        formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)

        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setFormatter(formatter)

        file_handler = TimedRotatingFileHandler(
            LOG_FILE,
            when="midnight",
            backupCount=0,  # 0 = keep every old file; nothing is auto-deleted
            encoding="utf-8",  # email/page text is arbitrary Unicode
        )
        file_handler.setFormatter(formatter)

        # Replace any handler a stray logging.basicConfig() installed before us,
        # so there is exactly one configuration and no duplicated lines.
        root = logging.getLogger()
        for handler in list(root.handlers):
            root.removeHandler(handler)
            handler.close()
        root.setLevel(LOG_LEVEL)
        root.addHandler(console_handler)
        root.addHandler(file_handler)

        for name in _UVICORN_LOGGERS:
            uvicorn_logger = logging.getLogger(name)
            uvicorn_logger.handlers.clear()
            uvicorn_logger.propagate = True

        for name in _NOISY_LIBRARIES:
            logging.getLogger(name).setLevel(logging.WARNING)

        # Route warnings.warn(...) (e.g. library deprecation notices) into the log too.
        logging.captureWarnings(True)

        _configured = True

    logging.getLogger(__name__).info(
        "Logging configured: level=%s, file=%s", logging.getLevelName(LOG_LEVEL), LOG_FILE
    )
