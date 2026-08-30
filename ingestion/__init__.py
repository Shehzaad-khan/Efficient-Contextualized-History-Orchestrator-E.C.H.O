"""E.C.H.O. ingestion package.

This package exposes the Gmail, Chrome, and YouTube source adapters used by the
main application. The adapters write through the canonical intake path and keep a
local JSON backup for offline/local testing.
"""

__all__ = ["gmail", "chrome", "youtube"]
