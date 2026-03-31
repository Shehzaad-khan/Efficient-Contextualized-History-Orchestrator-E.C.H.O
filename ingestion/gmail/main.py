"""
Main module - Email Listener orchestrator
Integrates Gmail API, Database, and Config modules

Run this file to start the email listener
"""

import time
import psycopg2
from pathlib import Path
import sys

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from ingestion.gmail.config import DATABASE_URL, CHECK_INTERVAL, get_redis_client
    from ingestion.gmail.database import initialize_database
    from ingestion.gmail.gmail_api import authenticate_gmail, fetch_and_store_new_emails
else:
    from .config import DATABASE_URL, CHECK_INTERVAL, get_redis_client
    from .database import initialize_database
    from .gmail_api import authenticate_gmail, fetch_and_store_new_emails
from datetime import datetime

# ==============================
# RUN AS BACKGROUND LISTENER
# ==============================

def run_listener():
    # Initialize database first
    db_ok = initialize_database()
    if not db_ok:
        print("⚠️  PostgreSQL unavailable - running in offline mode (Excel only)")
    
    service = authenticate_gmail()
    print("Gmail Service Ready ✅")
    
    print("\n" + "="*50)
    print("Email Listener Started 🚀")
    print("="*50)
    if db_ok:
        print(f"PostgreSQL: Connected")
    print(f"Redis: {'Connected' if get_redis_client() else 'Unavailable (continuing without cache)'}")
    print("="*50 + "\n")

    while True:
        try:
            if db_ok:
                # Create connection for fetch_and_store_new_emails
                conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
                cursor = conn.cursor()
                
                fetch_and_store_new_emails(service, conn, cursor)
                
                cursor.close()
                conn.close()
            else:
                print("⚠️  Skipping fetch (PostgreSQL unavailable)")
            print(f"✓ Checked at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception as e:
            print(f"❌ Error: {e}")

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    run_listener()
