from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse

from cryptography.fernet import Fernet, InvalidToken


TOKEN_KEY_ENV = "ECHO_TOKEN_ENCRYPTION_KEY"
LOCAL_REDIS_HOSTS = {"localhost", "127.0.0.1", "::1"}


class SecurityConfigError(RuntimeError):
    pass


def get_token_cipher() -> Fernet:
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
    try:
        return get_token_cipher().decrypt(value.encode("utf-8")).decode("utf-8")
    except InvalidToken as exc:
        raise SecurityConfigError("Encrypted token could not be decrypted with the configured key") from exc


def read_encrypted_text(path: Path) -> str:
    return decrypt_text(path.read_text(encoding="utf-8"))


def write_encrypted_text(path: Path, value: str) -> None:
    path.write_text(encrypt_text(value), encoding="utf-8")


def migrate_plaintext_file(plaintext_path: Path, encrypted_path: Path) -> str | None:
    if encrypted_path.exists():
        return read_encrypted_text(encrypted_path)

    if not plaintext_path.exists():
        return None

    value = plaintext_path.read_text(encoding="utf-8")
    write_encrypted_text(encrypted_path, value)
    plaintext_path.unlink()
    return value


def validate_redis_tls_url(redis_url: str) -> str:
    parsed = urlparse(redis_url)
    host = (parsed.hostname or "").lower()

    if parsed.scheme == "rediss":
        return redis_url

    if parsed.scheme == "redis" and host in LOCAL_REDIS_HOSTS:
        return redis_url

    raise SecurityConfigError(
        "Remote Redis connections must use TLS. Use a rediss:// URL, "
        "or redis:// only for localhost/127.0.0.1 development."
    )
