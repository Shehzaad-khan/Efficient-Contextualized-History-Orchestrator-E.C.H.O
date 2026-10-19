"""
security.py — OAuth token encryption at rest, and the Redis TLS rule.

Token encryption (Fernet = AES-128-CBC + HMAC-SHA256, from `cryptography`):
    The Gmail and YouTube OAuth tokens are stored on disk only in encrypted form
    (token_gmail.enc, token_youtube.enc). The key comes from the env variable
    ECHO_TOKEN_ENCRYPTION_KEY. Callers: ingestion/gmail/gmail_api.py,
    ingestion/youtube/youtube_api_client.py.
    Older installs stored plaintext token_*.json; migrate_plaintext_file()
    converts them once and deletes the plaintext copy.

Redis TLS rule:
    validate_redis_tls_url() refuses any non-TLS (redis://) URL that points at
    a remote host, so Redis traffic (which includes the cached Gmail token) is
    never sent unencrypted over the network. Plain redis:// is allowed only for
    localhost development.

Thread safety:
    Token files are read and written from several threads at once (the
    YouTube connector's 4-thread executor, request threads, the Gmail poller).
    `_token_file_lock` serialises all token-file access in this process, and
    writes go to a temp file that is then atomically renamed over the real
    one, so a reader never sees a half-written token and a crash mid-write
    never corrupts the existing token.

Logging: never logs a token, a key, or a URL with a password — only file
names, hosts and what happened.
"""

from __future__ import annotations

import logging
import os
import threading
from pathlib import Path
from urllib.parse import urlparse

from cryptography.fernet import Fernet, InvalidToken

logger = logging.getLogger(__name__)

TOKEN_KEY_ENV = "ECHO_TOKEN_ENCRYPTION_KEY"
LOCAL_REDIS_HOSTS = {"localhost", "127.0.0.1", "::1"}

# Suffix for the temp file used by atomic writes (gitignored via *.enc.tmp).
_TEMP_SUFFIX = ".tmp"

# RLock, not Lock: migrate_plaintext_file holds it and then calls
# read_encrypted_text / write_encrypted_text, which take it again.
_token_file_lock = threading.RLock()


class SecurityConfigError(RuntimeError):
    pass


def get_token_cipher() -> Fernet:
    """Build a Fernet cipher from ECHO_TOKEN_ENCRYPTION_KEY (re-read each call; cheap)."""
    key = os.getenv(TOKEN_KEY_ENV)
    if not key:
        raise SecurityConfigError(
            f"{TOKEN_KEY_ENV} is required for encrypted OAuth token storage. "
            "Generate one with: python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
        )
    try:
        return Fernet(key.encode("utf-8"))
    except ValueError as exc:
        raise SecurityConfigError(f"{TOKEN_KEY_ENV} is not a valid Fernet key") from exc


def encrypt_text(value: str) -> str:
    return get_token_cipher().encrypt(value.encode("utf-8")).decode("utf-8")


def decrypt_text(value: str) -> str:
    """Decrypt a Fernet token. Raises SecurityConfigError if the key doesn't match
    (key rotated, or the data was corrupted)."""
    try:
        return get_token_cipher().decrypt(value.encode("utf-8")).decode("utf-8")
    except InvalidToken as exc:
        logger.error("Token decryption failed: wrong %s or corrupted data", TOKEN_KEY_ENV)
        raise SecurityConfigError("Encrypted token could not be decrypted with the configured key") from exc


def read_encrypted_text(path: Path) -> str:
    with _token_file_lock:
        return decrypt_text(path.read_text(encoding="utf-8"))


def write_encrypted_text(path: Path, value: str) -> None:
    """Encrypt `value` and write it to `path` atomically.

    Write to <path>.tmp first, then os.replace() it over <path>. os.replace is
    atomic on the same volume (Windows and POSIX), so <path> always holds either
    the old token or the new one — never a partial file.
    """
    encrypted = encrypt_text(value)
    temp_path = path.with_name(path.name + _TEMP_SUFFIX)
    with _token_file_lock:
        temp_path.write_text(encrypted, encoding="utf-8")
        os.replace(temp_path, path)
    logger.info("Encrypted token written to %s", path.name)


def migrate_plaintext_file(plaintext_path: Path, encrypted_path: Path) -> str | None:
    """Return the stored token, converting a legacy plaintext file on first use.

    - encrypted file exists  -> decrypt and return it
    - only plaintext exists  -> encrypt it, delete the plaintext, return it
    - neither exists         -> None (caller must run the auth script)

    Runs under the lock so two threads can't both migrate (the second unlink
    would raise FileNotFoundError).
    """
    with _token_file_lock:
        if encrypted_path.exists():
            if plaintext_path.exists():
                # Not deleted automatically: if the key changed, the plaintext
                # copy may be the only readable one. The owner removes it by hand.
                logger.warning(
                    "Plaintext token %s still exists next to %s — delete it once the encrypted token works",
                    plaintext_path.name,
                    encrypted_path.name,
                )
            return read_encrypted_text(encrypted_path)

        if not plaintext_path.exists():
            logger.info("No stored token found (%s / %s)", encrypted_path.name, plaintext_path.name)
            return None

        value = plaintext_path.read_text(encoding="utf-8")
        write_encrypted_text(encrypted_path, value)
        plaintext_path.unlink()
        logger.info("Migrated plaintext token %s to encrypted %s", plaintext_path.name, encrypted_path.name)
        return value


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
