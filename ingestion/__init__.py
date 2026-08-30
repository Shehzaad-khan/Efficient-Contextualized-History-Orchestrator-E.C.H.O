"""Bootstrap ingestion package for Echo.

This project's architecture documents define a full Gmail/Chrome/YouTube
capture pipeline, but the repository was missing the ingestion package itself.
This stub keeps the app importable and provides minimal health endpoints while
the real source connectors are being implemented.
"""

__all__ = ["gmail", "chrome", "youtube"]
