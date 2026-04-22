"""
Main module - Email Listener orchestrator
Integrates Gmail API, Database, and Config modules
"""

import time
from datetime import datetime
from pathlib import Path
import sys

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from ingestion.gmail.config import CHECK_INTERVAL, get_redis_client
    from ingestion.gmail.database import initialize_database
    from ingestion.gmail.gmail_api import authenticate_gmail, fetch_and_store_new_emails
else:
    from .config import CHECK_INTERVAL, get_redis_client
    from .database import initialize_database
    from .gmail_api import authenticate_gmail, fetch_and_store_new_emails


def run_listener():
    db_ok = initialize_database()
    if not db_ok:
        print("PostgreSQL unavailable - running in offline mode (Excel only)")

    service = authenticate_gmail()
    print("Gmail Service Ready")

    print("\n" + "=" * 50)
    print("Email Listener Started")
    print("=" * 50)
    if db_ok:
        print("PostgreSQL: Connected")
    print(f"Redis: {'Connected' if get_redis_client() else 'Unavailable (continuing without cache)'}")
    print("=" * 50 + "\n")

    while True:
        try:
            if db_ok:
                fetch_and_store_new_emails(service)
            else:
                print("Skipping fetch (PostgreSQL unavailable)")
            print(f"Checked at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception as exc:
            print(f"Error: {exc}")

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    run_listener()
