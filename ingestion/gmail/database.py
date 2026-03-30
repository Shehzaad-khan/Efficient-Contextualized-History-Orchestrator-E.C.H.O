"""
Database module - Handles all PostgreSQL and Excel storage operations
"""

import json
import uuid
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
import os

from config import DATABASE_URL, get_redis_client

# ==============================
# DATABASE INITIALIZATION
# ==============================

def initialize_database():
    """Create the emails table if it doesn't exist and fix foreign key constraints"""
    try:
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
        cursor = conn.cursor()
        
        # Create table for storing emails
        create_table_query = """
        CREATE TABLE IF NOT EXISTS gmail_memory (
            id SERIAL PRIMARY KEY,
            memory_id VARCHAR(36) UNIQUE NOT NULL,
            source_type VARCHAR(50) NOT NULL,
            source_item_id VARCHAR(255) UNIQUE NOT NULL,
            title TEXT,
            content_primary_text TEXT,
            content_attachments JSONB,
            content_summary TEXT,
            event_timestamp TIMESTAMP,
            ingested_at TIMESTAMP,
            semantic JSONB,
            classification JSONB,
            interaction JSONB,
            analytics JSONB,
            is_regret BOOLEAN DEFAULT FALSE,
            email_from VARCHAR(255),
            email_to JSONB,
            email_labels JSONB,
            email_thread_id VARCHAR(255),
            email_has_attachments BOOLEAN,
            source_link TEXT,
            message_history JSONB DEFAULT '[]'::jsonb,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_source_item_id ON gmail_memory(source_item_id);
        CREATE INDEX IF NOT EXISTS idx_email_from ON gmail_memory(email_from);
        CREATE INDEX IF NOT EXISTS idx_ingested_at ON gmail_memory(ingested_at);
        """
        
        cursor.execute(create_table_query)
        conn.commit()
        
        # Fix foreign key constraint and column type issues
        try:
            print("\n🔧 === FIXING GMAIL_ATTACHMENTS TABLE ===")
            
            # First, check and fix the memory_id column type
            print("🔧 Checking memory_id column types...")
            cursor.execute("""
                SELECT data_type FROM information_schema.columns
                WHERE table_name = 'gmail_attachments' AND column_name = 'memory_id'
            """)
            att_type = cursor.fetchone()
            
            if att_type:
                att_col_type = att_type[0]
                print(f"   gmail_attachments.memory_id type: {att_col_type}")
                
                if att_col_type == 'uuid':
                    print(f"   ⚠️  Column is UUID, should be VARCHAR(36)")
                    print(f"   🔧 Converting to VARCHAR(36)...")
                    try:
                        # Drop foreign key if it exists first
                        cursor.execute("""
                            SELECT constraint_name FROM information_schema.table_constraints
                            WHERE table_name = 'gmail_attachments' 
                            AND constraint_type = 'FOREIGN KEY'
                        """)
                        constraints = cursor.fetchall()
                        for c in constraints:
                            cursor.execute(f"DROP CONSTRAINT IF EXISTS {c[0]}")
                        
                        # Convert column type
                        cursor.execute("""
                            ALTER TABLE gmail_attachments 
                            ALTER COLUMN memory_id TYPE VARCHAR(36) USING memory_id::text
                        """)
                        conn.commit()
                        print(f"      ✅ Column type converted to VARCHAR(36)")
                    except Exception as e:
                        print(f"      ❌ Error converting column: {e}")
                        conn.rollback()
                elif att_col_type == 'character varying':
                    print(f"   ✅ Column is already VARCHAR")
            
            # Now create or fix the foreign key
            print("\n🔧 Checking foreign key constraint...")
            cursor.execute("""
                SELECT constraint_name FROM information_schema.table_constraints
                WHERE table_name = 'gmail_attachments' 
                AND constraint_type = 'FOREIGN KEY'
            """)
            constraints = cursor.fetchall()
            
            if not constraints:
                print("🔧 No foreign key found, creating one...")
                try:
                    cursor.execute("""
                        ALTER TABLE gmail_attachments 
                        ADD CONSTRAINT gmail_attachments_memory_id_fkey 
                        FOREIGN KEY (memory_id) REFERENCES gmail_memory(memory_id) ON DELETE CASCADE
                    """)
                    conn.commit()
                    print("   ✅ Foreign key created successfully!")
                except Exception as e:
                    print(f"   ❌ Error creating foreign key: {e}")
                    conn.rollback()
            else:
                print(f"🔧 Foreign key already exists: {constraints[0][0]}")
                # Verify it points to gmail_memory
                cursor.execute("""
                    SELECT ccu.table_name FROM information_schema.table_constraints tc
                    JOIN information_schema.constraint_column_usage ccu
                    ON tc.constraint_name = ccu.constraint_name
                    WHERE tc.table_name = 'gmail_attachments'
                    AND tc.constraint_type = 'FOREIGN KEY'
                """)
                ref_table = cursor.fetchone()
                if ref_table and ref_table[0] == 'gmail_memory':
                    print(f"   ✅ Constraint correctly references gmail_memory")
                else:
                    print(f"   ⚠️  Constraint references {ref_table[0] if ref_table else 'unknown'}")
            
            print("🔧 === TABLE FIXES COMPLETE ===\n")
        except Exception as e:
            print(f"🔧 ❌ Error during table fixes: {e}\n")
            try:
                conn.rollback()
            except:
                pass
        
        cursor.close()
        conn.close()
        print("PostgreSQL Database initialized ✅")
    except psycopg2.OperationalError as e:
        print(f"⚠️  PostgreSQL connection error: {e}")
        print("    Check your internet connection and database URL in .env")
        return False
    except Exception as e:
        print(f"⚠️  Database initialization error: {e}")
        return False
    return True


# ==============================
# GET MESSAGE HISTORY FROM DB
# ==============================

def get_thread_history(thread_id):
    """Retrieve all messages in the same thread from the database with complete data"""
    try:
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute("""
            SELECT 
                memory_id, title, email_from, email_to, content_primary_text, 
                content_attachments, email_labels,
                event_timestamp, ingested_at, email_has_attachments
            FROM gmail_memory 
            WHERE email_thread_id = %s
            ORDER BY event_timestamp ASC
        """, (thread_id,))
        
        history = cursor.fetchall()
        cursor.close()
        conn.close()
        
        # Convert datetime objects to ISO format strings for JSON serialization
        result = []
        for row in history:
            converted_row = dict(row)
            # Convert timestamps
            if converted_row.get('event_timestamp'):
                converted_row['event_timestamp'] = converted_row['event_timestamp'].isoformat() if hasattr(converted_row['event_timestamp'], 'isoformat') else str(converted_row['event_timestamp'])
            if converted_row.get('ingested_at'):
                converted_row['ingested_at'] = converted_row['ingested_at'].isoformat() if hasattr(converted_row['ingested_at'], 'isoformat') else str(converted_row['ingested_at'])
            
            # Parse JSON fields if they're strings
            if isinstance(converted_row.get('content_attachments'), str):
                converted_row['content_attachments'] = json.loads(converted_row['content_attachments'])
            if isinstance(converted_row.get('email_to'), str):
                converted_row['email_to'] = json.loads(converted_row['email_to'])
            if isinstance(converted_row.get('email_labels'), str):
                converted_row['email_labels'] = json.loads(converted_row['email_labels'])
            
            result.append(converted_row)
        
        return result
    
    except Exception as e:
        print(f"⚠️  Failed to fetch thread history: {e}")
        return []


# ==============================
# STORE ATTACHMENTS IN DATABASE
# ==============================

def store_attachments_metadata(attachments, memory_id):
    """Store attachment metadata in gmail_attachments table"""
    print(f"   🔍 ATTACHMENT STORAGE STARTED")
    print(f"   🔍 Attachments count: {len(attachments)}")
    print(f"   🔍 Memory ID: {memory_id[:12]}...")
    
    if not attachments:
        print("   ℹ️  No attachments to store")
        return True
    
    stored_count = 0
    failed_count = 0
    
    for idx, attachment in enumerate(attachments, 1):
        conn = None
        filename = attachment.get("filename", "unknown")
        print(f"\n   📌 [{idx}] Processing attachment: {filename}")
        
        try:
            conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
            print(f"      ✅ Database connected")
            cursor = conn.cursor()
            
            mime_type = attachment.get("mime_type", "application/octet-stream")
            file_size = int(attachment.get("size", 0))
            
            print(f"      📊 Size: {file_size} bytes | Type: {mime_type}")
            
            # Check if attachment already exists
            print(f"      🔍 Checking for duplicates...")
            cursor.execute(
                "SELECT 1 FROM gmail_attachments WHERE memory_id = %s AND filename = %s LIMIT 1",
                (memory_id, filename)
            )
            print(f"      ✅ Duplicate check query executed")
            
            if cursor.fetchone():
                print(f"      ⏭️  Duplicate found - skipping insert")
            else:
                print(f"      ➕ Not a duplicate - preparing INSERT")
                generated_attachment_id = str(uuid.uuid4())
                print(f"      📝 Generated UUID: {generated_attachment_id[:12]}...")
                
                try:
                    print(f"      ⏳ Executing INSERT statement...")
                    cursor.execute("""
                        INSERT INTO gmail_attachments (
                            memory_id, attachment_id, filename, mime_type, file_size
                        ) VALUES (%s, %s, %s, %s, %s)
                    """, (
                        memory_id,
                        generated_attachment_id,
                        filename,
                        mime_type,
                        file_size
                    ))
                    print(f"      ⏳ Committing transaction...")
                    conn.commit()
                    stored_count += 1
                    print(f"      ✅ Successfully inserted into gmail_attachments")
                except Exception as e:
                    conn.rollback()
                    print(f"      ❌ INSERT ERROR: {str(e)}")
                    failed_count += 1
            
            cursor.close()
        
        except Exception as e:
            print(f"      ❌ CONNECTION ERROR: {str(e)}")
            failed_count += 1
        
        finally:
            if conn:
                try:
                    conn.close()
                    print(f"      ✅ Connection closed")
                except Exception as e:
                    print(f"      ⚠️  Error closing connection: {e}")
    
    print(f"\n   🔍 ATTACHMENT STORAGE COMPLETED")
    print(f"   📊 Summary: {stored_count} stored, {failed_count} failed")
    return failed_count == 0


# ==============================
# STORE IN POSTGRESQL
# ==============================

def store_in_postgresql(data):
    try:
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
        cursor = conn.cursor()

        # Check if already exists
        cursor.execute(
            "SELECT id FROM gmail_memory WHERE source_item_id = %s",
            (data["source_item_id"],)
        )
        
        if cursor.fetchone():
            cursor.close()
            conn.close()
            print("Already stored → Skipping")
            return False

        # Fetch thread history from database (all previous messages in this thread)
        thread_history = get_thread_history(data["source_metadata"]["email"]["thread_id"])
        
        # Insert new record with message history
        insert_query = """
        INSERT INTO gmail_memory (
            memory_id, source_type, source_item_id, title,
            content_primary_text, content_attachments, content_summary,
            event_timestamp, ingested_at, semantic, classification,
            interaction, analytics, is_regret,
            email_from, email_to, email_labels, email_thread_id,
            email_has_attachments, source_link, message_history
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        cursor.execute(insert_query, (
            data["memory_id"],
            data["source_type"],
            data["source_item_id"],
            data["title"],
            data["content"]["primary_text"],
            json.dumps(data["content"]["attachments"]),
            data["content"]["summary"],
            data["time"]["event_timestamp"],
            data["time"]["ingested_at"],
            json.dumps(data["semantic"]),
            json.dumps(data["classification"]),
            json.dumps(data["interaction"]),
            json.dumps(data["analytics"]),
            data["regret"]["is_regret"],
            data["source_metadata"]["email"]["from"],
            json.dumps(data["source_metadata"]["email"]["to"]),
            json.dumps(data["source_metadata"]["email"]["labels"]),
            data["source_metadata"]["email"]["thread_id"],
            data["source_metadata"]["email"]["has_attachments"],
            data["source_link"],
            json.dumps(thread_history)  # Store the message history
        ))

        conn.commit()
        cursor.close()
        conn.close()
        
        # Cache in Redis with 30 minute expiry
        rc = get_redis_client()
        if rc:
            try:
                rc.setex(f"email:{data['source_item_id']}", 1800, json.dumps(data))
            except Exception as e:
                print(f"⚠️  Failed to cache email in Redis: {e}")
        
        print(f"   ✅ Stored in PostgreSQL (with {len(thread_history)} previous messages)")
        return True

    except psycopg2.OperationalError as e:
        print(f"⚠️  PostgreSQL unavailable: {e}")
        return False
    except Exception as e:
        print(f"PostgreSQL storage error: {e}")
        return False


# ==============================
# STORE IN EXCEL (BACKUP)
# ==============================

def store_in_excel(data):
    try:
        row = {
            "memory_id": data["memory_id"],
            "subject": data["title"],
            "sender": data["source_metadata"]["email"]["from"],
            "received_time": data["time"]["event_timestamp"],
            "labels": ",".join(data["source_metadata"]["email"]["labels"]),
            "body": data["content"]["primary_text"][:500]  # First 500 chars
        }

        df = pd.DataFrame([row])

        if os.path.exists("emails.xlsx"):
            existing = pd.read_excel("emails.xlsx")
            df = pd.concat([existing, df], ignore_index=True)

        df.to_excel("emails.xlsx", index=False)
        print("Backed up in Excel ✅")
    except Exception as e:
        print(f"Excel backup error: {e}")
