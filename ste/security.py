"""
security.py — local OAuth token files and the Redis TLS rule.

Gmail and YouTube OAuth tokens are stored as JSON files in the project root.
The token helpers serialize access within this process and use atomic writes so
readers never see a partially written file.

Redis TLS rule:
    validate_redis_tls_url() refuses any non-TLS (redis://) URL that points at
    a remote host. Plain redis:// is allowed only for localhost development.

Logging: never logs a token, a key, or a URL with a password — only file
names, hosts and what happened.
"""

from __future__ import annotations

import logging
import os
import threading
from pathlib import Path
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

LOCAL_REDIS_HOSTS = {"localhost", "127.0.0.1", "::1"}

# Suffix for the temp file used by atomic writes (gitignored via token_*.json.tmp).
_TEMP_SUFFIX = ".tmp"
_token_file_lock = threading.RLock()


class SecurityConfigError(RuntimeError):
    pass


def read_token_text(path: Path) -> str | None:
    """Return a token file's text, or None when the file does not exist."""
    with _token_file_lock:
        try:
            # utf-8-sig: a token file saved by Notepad starts with a BOM, which json rejects.
            return path.read_text(encoding="utf-8-sig")
        except FileNotFoundError:
            return None


def write_token_text(path: Path, value: str) -> None:
    """Write token text through a same-directory temp file and atomic replace."""
    temp_path = path.with_name(path.name + _TEMP_SUFFIX)
    with _token_file_lock:
        temp_path.write_text(value, encoding="utf-8")
        os.replace(temp_path, path)
    logger.info("Token written to %s", path.name)


def validate_redis_tls_url(redis_url: str) -> str:
    """Return the URL unchanged if it is allowed; raise SecurityConfigError if not.

    Allowed: any rediss:// (TLS) URL, or redis:// to localhost / 127.0.0.1 / ::1.
    """
    parsed = urlparse(redis_url)
    host = (parsed.hostname or "").lower()

    if parsed.scheme == "rediss":
        return redis_url

    if parsed.scheme == "redis" and host in LOCAL_REDIS_HOSTS:
        return redis_url

    # Only scheme and host in the message — the URL itself may carry a password.
    raise SecurityConfigError(
        f"Remote Redis connections must use TLS (got {parsed.scheme}:// to host '{host}'). "
        "Use a rediss:// URL, or redis:// only for localhost/127.0.0.1 development."
    )
