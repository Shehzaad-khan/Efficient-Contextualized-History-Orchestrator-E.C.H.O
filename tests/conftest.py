"""
Shared test setup: make the repository importable and keep tests off real services.

The tests never need PostgreSQL, Redis, FAISS, Gmail or an OpenAI key. Where a driver is not installed
(psycopg2 on a bare machine) a stand-in module is registered so the RSE modules can still be imported;
where it is installed it is left alone and the tests patch the functions that would touch a database.
"""
import sys
from pathlib import Path
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    import psycopg2  # noqa: F401
except ImportError:
    for _name in ("psycopg2", "psycopg2.pool", "psycopg2.extras", "psycopg2.extensions"):
        sys.modules[_name] = MagicMock()
