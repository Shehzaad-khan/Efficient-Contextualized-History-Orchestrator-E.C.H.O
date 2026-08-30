"""Gmail API integration bootstrap.

This module exists so the rest of the project can import the expected symbol
without crashing during bootstrapping.
"""

from __future__ import annotations

from typing import Any


def authenticate_gmail(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return {"status": "stub", "message": "Gmail API integration not implemented yet."}
