import psycopg2
import logging
import time
from typing import List, Dict

from system_group_classifier import classify_system_group, initialize_centroids
from embedding_generator import generate_embedding
from text_cleaner import clean_text

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# -------------------------
# DB CONNECTION
# -------------------------
DB_CONFIG = {
    "host": "ep-blue-mountain-amvfgcpj-pooler.c-5.us-east-1.aws.neon.tech",
    "database": "echodb",
    "user": "neondb_owner",
    "password": "npg_WgQBrxHUh85t",
    "sslmode": "require"
}

def get_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

# -------------------------
# FETCH DATA
# -------------------------
def fetch_unprocessed_items(conn):
    cursor = conn.cursor()

    cursor.execute("""
        SELECT memory_id, source_type, raw_text, title, preprocessed FROM memory_items
        WHERE preprocessed = FALSE
        LIMIT 10
    """)

    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    cursor.close()

    return [dict(zip(columns, row)) for row in rows]

# -------------------------
# PREPARE ITEM
# -------------------------
def prepare_item(conn, row):
    try:
        cursor = conn.cursor()
        source_type = row.get("source_type", "").lower()
        memory_id = row.get("memory_id")
        item = {"source": source_type, "memory_id": memory_id}

        if source_type == "gmail":
            cursor.execute(
                "SELECT gmail_labels, sender FROM gmail_metadata WHERE memory_id=%s",
                (memory_id,)
            )
            data = cursor.fetchone()
            if data:
                item["gmail_labels"] = data[0] or []
                item["sender_domain"] = data[1] or ""

        elif source_type == "youtube":
            cursor.execute(
                "SELECT youtube_category_id, is_short FROM youtube_metadata WHERE memory_id=%s",
                (memory_id,)
            )
            data = cursor.fetchone()
            if data:
                item["youtube_category_id"] = data[0]
                item["is_short"] = data[1] or False

        elif source_type == "chrome":
            cursor.execute(
                "SELECT domain FROM chrome_metadata WHERE memory_id=%s",
                (memory_id,)
            )
            data = cursor.fetchone()
            if data:
                item["domain"] = data[0] or ""

        cursor.close()
        return item
    except Exception as e:
        logger.error(f"Error preparing item {memory_id}: {e}")
        return None

# -------------------------
# UPDATE DB
# -------------------------
def update_classification(conn, memory_id, category, method, confidence):
    try:
        cursor = conn.cursor()
        
        # Map category name to system_group_id (assuming 1-5 mapping)
        category_map = {
            "work": 1,
            "study": 2,
            "entertainment": 3,
            "personal": 4,
            "misc": 5
        }
        system_group_id = category_map.get(category, 5)  # Default to misc
        
        cursor.execute("""
            UPDATE memory_items
            SET system_group_id = %s,
                classified_by = %s,
                classification_confidence = %s,
                preprocessed = TRUE,
                last_updated_at = NOW()
            WHERE memory_id = %s
        """, (system_group_id, method, confidence, memory_id))
        
        cursor.close()
        conn.commit()
        logger.info(f"Updated memory_id {memory_id}: {category} (id={system_group_id}, {method}, confidence={confidence})")
        return True
    except Exception as e:
        logger.error(f"Error updating classification for {memory_id}: {e}")
        conn.rollback()
        return False


# -------------------------
# MAIN PIPELINE
# -------------------------
def run_pipeline(batch_size: int = 10, poll_interval: int = 10):
    """Run the enrichment pipeline continuously.
    
    Args:
        batch_size: Number of items to process per batch
        poll_interval: Seconds between polls for new unprocessed items
    """
    
    try:
        conn = get_connection()
        logger.info("Database connection established")
        
        # Initialize centroids once at startup
        logger.info("Initializing centroids...")
        initialize_centroids(generate_embedding)
        logger.info("Centroids initialized")
        
        iteration = 0
        while True:
            iteration += 1
            logger.info(f"\n--- Iteration {iteration} ---")
            
            rows = fetch_unprocessed_items(conn)
            
            if not rows:
                logger.info(f"No unprocessed items. Waiting {poll_interval}s before next poll...")
                time.sleep(poll_interval)
                continue
            
            logger.info(f"Found {len(rows)} unprocessed items")
            
            processed = 0
            failed = 0
            
            for row in rows:
                try:
                    memory_id = row["memory_id"]
                    logger.info(f"\nProcessing memory_id={memory_id}")
                    
                    item = prepare_item(conn, row)
                    if item is None:
                        logger.warning(f"Failed to prepare item {memory_id}")
                        failed += 1
                        continue
                    
                    # Clean text and generate embedding
                    text = clean_text(row)
                    if not text:
                        logger.warning(f"No text to embed for {memory_id}")
                        failed += 1
                        continue
                    
                    embedding = generate_embedding(text)
                    
                    # Classify
                    category, method, confidence = classify_system_group(item, embedding)
                    
                    # Update DB
                    if update_classification(conn, memory_id, category, method, confidence):
                        processed += 1
                    else:
                        failed += 1
                        
                except Exception as e:
                    logger.error(f"Unexpected error processing {row.get('memory_id')}: {e}")
                    failed += 1
                    continue
            
            logger.info(f"Iteration {iteration} complete: {processed} processed, {failed} failed")
            logger.info(f"Waiting {poll_interval}s before next poll...")
            time.sleep(poll_interval)
            
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error in pipeline: {e}")
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")


# -------------------------
# RUN
# -------------------------
if __name__ == "__main__":
    run_pipeline(batch_size=10, poll_interval=10)