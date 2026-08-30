E.C.H.O
Episodic Cache for Human Observation

DATABASE DESIGN SPECIFICATION
Version 5.0 — Final Schema
March 2026


1. Overview
Echo uses a three-layer storage architecture. PostgreSQL 16 (Neon, migrating to local at submission) is the permanent truth store — the only layer that cannot be rebuilt.
FAISS is a local in-memory vector index rebuilt on demand from PostgreSQL.
Redis is an ephemeral signal cache with short TTLs.

This document covers PostgreSQL exclusively. All 16 tables are defined here with every attribute, constraint, relationship, and design rationale.

Storage Layers

The following table:
Layer,Technology,Rebuildable?,Role
PostgreSQL 16,Neon → Local,NO,"Permanent truth — all memory, schema, history"
FAISS,faiss-cpu (local),YES,"Vector similarity search, 384-dim embeddings"
Redis,Upstash / local,YES,"Revisit TTL (24hr), attachment cache (1hr)"


Entity Classification

The following table:
Table,Category,PK Style,Role
system_groups,Strong Entity,SERIAL,Fixed 5-row reference. Never modified at runtime.
memory_items,Strong Entity (Central),UUID,Every captured item. Single source of truth.
user_groups,Strong Entity,UUID,User-defined custom categories.
sessions,Strong Entity,UUID,Contiguous activity blocks (5-min gap rule).
gmail_metadata,Weak Subtype (1:1),UUID = FK,Gmail-specific fields. PK shared with memory_items.
chrome_metadata,Weak Subtype (1:1),UUID = FK,Chrome-specific fields. PK shared with memory_items.
youtube_metadata,Weak Subtype (1:1),UUID = FK,YouTube-specific fields. PK shared with memory_items.
memory_engagement,Weak Subtype (1:1),UUID = FK,Engagement metrics. Separated to prevent write amplification.
embedding_index,Weak Subtype (1:0..1),UUID = FK,FAISS tracking. Created only after enrichment completes.
gmail_attachments,Weak Multi-valued (1:N),Own UUID,One row per attachment per email.
regret_events,Weak Multi-valued (1:N),Own UUID,Each mark/unmark is a new row. Toggle history preserved.
memory_user_groups,Associative (M:N),Composite,Items ↔ User groups bridge. Human-approval-only writes.
session_memory_map,Associative (M:N),Composite,Sessions ↔ Items bridge.
group_rules,Workflow,UUID,Persistent classifier rules. Survive restarts.
group_suggestions,Workflow (Audit Trail),UUID,Human-in-the-loop queue. All classifications pass through here.
message_store,LangChain-managed,SERIAL,Multi-turn conversation history for LangGraph RSE.



2. Core Design Principles
Five non-negotiable principles govern every schema decision:

1.  system_group_id is NEVER NULL. Every memory_items row must have a system group assigned before INSERT.
Fallback is always misc (ID 5). Enforced at the database level with NOT NULL, not just in application code.
2.  UNIQUE(source_type, source_id) on memory_items is the restart-safety guarantee.
Re-ingesting the same item on restart silently does nothing via ON CONFLICT DO NOTHING.
3.  No item enters memory_user_groups without human approval through group_suggestions. Architecturally enforced — no exceptions, no shortcuts.
4.  Every ingestion is a single atomic transaction: INSERT memory_items + INSERT source_metadata + INSERT memory_engagement in one BEGIN/COMMIT block.
Partial ingestion must never be committed.
5.  Soft delete only. is_deleted = TRUE on memory_items cascades visibility to all subtables.
Hard deletes only via explicit user data export/deletion flow.


3. Table Definitions
TABLE 1 — system_groups
Fixed reference table. Pre-populated with exactly 5 rows at schema creation. Never inserted to, updated, or deleted at runtime.
Exists so that memory_items.system_group_id has a named FK target and analytics queries can JOIN against a named entity rather than checking raw strings.


The following table:
Attribute,Type,Default,Description
system_group_id,SERIAL,Auto-increment,Primary key. Values 1–5 fixed at seed time.
group_name,VARCHAR(20),—,One of: work | study | entertainment | personal | misc. CHECK constraint enforces exactly these five values. No others accepted.


CRITICAL:  CHECK (group_name IN ('work','study','entertainment','personal','misc')) must be present. These five categories cover every human digital activity at the top level.
All finer granularity lives in user_groups.

TABLE 2 — memory_items
The central abstraction of the entire system. Every email, webpage, and YouTube video captured by Echo gets exactly one row here first.
All other tables reference this table. This is the Canonical Memory Module from scope Module 5.


The following table:
Attribute,Type,Default,Description
memory_id,UUID,gen_random_uuid(),Primary key. UUID chosen over SERIAL to support distributed inserts without coordination.
system_group_id,INTEGER,NOT NULL,FK → system_groups. MUST NEVER be NULL. Assigned before INSERT. Fallback is misc (ID=5). NOT NULL enforced at DB level.
source_type,VARCHAR(20),—,"Source of this item. CHECK (source_type IN ('gmail','chrome','youtube'))."
source_id,TEXT,NOT NULL,"Original identifier from the source system. Gmail message ID, full URL, or YouTube video ID."
title,TEXT,NULL,"Human-readable name. Email subject, page title, or video title."
raw_text,TEXT,NULL,"Full content body. Email body, extracted page text, or video transcript. Application code caps at 10,000 characters before INSERT."
auto_keywords,"TEXT[]",'{}',"Array of extracted keywords from enrichment pipeline. e.g. '{python, machine learning, tutorial}'."
preprocessed,BOOLEAN,FALSE,"Set to TRUE only when enrichment pipeline has fully completed: text cleaned, keywords extracted, embedding generated, FAISS indexed. Items remain invisible to FAISS search until TRUE."
classified_by,VARCHAR(20),'pending',How system_group was determined. Values: structural | domain | centroid | llm | user_override | pending.
classification_confidence,FLOAT,NULL,"Confidence of classification. 1.0 for structural/domain, 0.55–1.0 for centroid, 0.90 for llm, NULL while pending."
created_at,TIMESTAMP,NOT NULL,"When the original content was created — NOT when Echo captured it. Email received_at, page publication date, video upload date."
first_ingested_at,TIMESTAMP,NOW(),When Echo first captured this item.
last_updated_at,TIMESTAMP,NOW(),Last time any field on this row changed.
is_deleted,BOOLEAN,FALSE,Soft delete flag. TRUE hides item from all UI queries. Cascades through 6 subtables. Never hard-delete.


CRITICAL:  UNIQUE(source_type, source_id) is the restart-safety constraint. Without it, every connector restart duplicates the entire database silently.
Use ON CONFLICT DO NOTHING on all INSERTs.

TABLE 3 — gmail_metadata
Weak subtype extending memory_items for Gmail-specific fields. Shares its PK with the parent row — same UUID, one-to-one.
Created in the same transaction as the parent memory_items row. Cannot exist without a parent.


The following table:
Attribute,Type,Default,Description
memory_id,UUID,PK + FK CASCADE,Shared primary key. Same UUID as parent memory_items. CASCADE ensures deletion propagates.
email_id,TEXT,NOT NULL,Gmail's native message ID (e.g. 18e4f2a3b1c). Unique within Gmail.
thread_id,TEXT,NULL,Conversation thread this email belongs to. Used by thread_analyzer.py to enrich reply emails with context from prior messages in the same thread.
sender,TEXT,NULL,Full sender address. e.g. hr@techcorp.com. Indexed for sender-based search queries.
recipients,"TEXT[]",NULL,Array of all recipient addresses including CC.
subject,TEXT,NULL,Email subject line. Primary text signal for Stage 1 structural classification.
received_at,TIMESTAMP,NULL,When the email arrived in the inbox.
has_attachments,BOOLEAN,FALSE,Quick flag for attachment presence. Avoids a JOIN to gmail_attachments on every search result.
gmail_labels,"TEXT[]",'{}',"Gmail labels at time of ingestion. e.g. '{INBOX,CATEGORY_PERSONAL,UNREAD}'. Critical for Stage 1 structural classifier — CATEGORY_PROMOTIONS maps to personal, CATEGORY_UPDATES maps to work. Must be fetched from Gmail API and stored; cannot be reconstructed later."
is_sent,BOOLEAN,FALSE,"TRUE if this email was sent by the user (from Sent folder). FALSE if received. Needed for wellbeing analytics to distinguish writing time from reading time, and for queries like find emails I sent to TechCorp."


TABLE 4 — gmail_attachments
One row per attachment per email. An email with 3 PDFs produces 3 rows.
Weak multi-valued entity — owns its own UUID, not shared with memory_items.
The two-tier extraction design keeps storage lean: lightweight extract always, full extract on demand via Redis cache.


The following table:
Attribute,Type,Default,Description
attachment_id,UUID,gen_random_uuid(),Own primary key. Not shared with memory_items.
memory_id,UUID,FK CASCADE,FK → gmail_metadata.memory_id. CASCADE ensures deletion propagates.
filename,TEXT,NULL,Original filename. e.g. project_report.pdf
mime_type,TEXT,NULL,"MIME type. e.g. application/pdf, image/png"
file_size,INTEGER,NULL,Size in bytes. Used to decide Tier 2 extraction eligibility (size < 500 KB threshold).
lightweight_extract,TEXT,NULL,First ~500 characters extracted from the file. Always populated after processing. Merged into parent email's embeddable text for FAISS indexing.
last_extracted_at,TIMESTAMP,NULL,When lightweight extraction last ran.
is_processed,BOOLEAN,FALSE,TRUE when this attachment's lightweight_extract has been successfully extracted and merged into the parent email's embeddable text. Enrichment pipeline uses this flag to find and retry failed extractions.
full_extract_cached,BOOLEAN,FALSE,TRUE if a full text extraction has been successfully generated at least once. Full text is not stored in PostgreSQL — it lives in Redis (1-hour TTL). This flag tells the system whether re-extraction is needed or whether to wait for Redis to warm up.
full_extract_generated_at,TIMESTAMP,NULL,Timestamp of last successful full extraction. Used to decide whether cached version is stale.


CRITICAL:  Full attachment text is NOT stored permanently in PostgreSQL to avoid storage bloat.
Redis caches it for 1 hour on demand. full_extract_cached and full_extract_generated_at provide operational visibility without the storage cost.

TABLE 5 — chrome_metadata
Chrome-specific fields for every webpage that passed the two-phase intent gate. Shares PK with memory_items.
The canonical_url field is the deduplication key for revisit detection — it is the URL with all UTM parameters and tracking query strings stripped.


The following table:
Attribute,Type,Default,Description
memory_id,UUID,PK + FK CASCADE,Shared primary key with parent memory_items.
url,TEXT,NOT NULL,Full URL including all query parameters. Preserved for display.
canonical_url,TEXT,NOT NULL,"UTM-stripped, cleaned URL used for revisit detection via Redis. example.com?utm_source=newsletter becomes example.com. Revisit tracker must compare against this field, not url."
domain,TEXT,NULL,"Domain only. e.g. github.com, medium.com. Used by domain-type group_rules for classification."
referrer,TEXT,NULL,URL of the page the user came from.
scroll_depth,FLOAT,0.0,"How far down the page the user scrolled. 0.0 = top, 1.0 = bottom. Captured by content.js injected into the page. Used in intent gate Phase 2 (>=0.25 threshold) and engagement re-ranking."
interaction_count,INTEGER,0,Number of clicks and text selections on the page. Captured by content.js. Intent gate Phase 2 signal.
revisit_count,INTEGER,0,How many times the user returned to this canonical URL. Incremented on each revisit signal from Redis.
word_count,INTEGER,NULL,"Word count of extracted page content. Set by enrichment pipeline. Used to compute estimated reading completion (dwell_time / word_count ratio) for smarter effort scoring in re-ranking. A 3-minute dwell on a 200-word page means different intent than 3 minutes on a 3,000-word article."


TABLE 6 — youtube_metadata
YouTube-specific fields for every video that passed the intent gate. Shares PK with memory_items.
Detection is entirely extension-driven from install date forward — no YouTube watch history API exists.
The transcript_text field is the richest semantic source for FAISS embedding quality.


The following table:
Attribute,Type,Default,Description
memory_id,UUID,PK + FK CASCADE,Shared primary key with parent memory_items.
video_id,TEXT,NOT NULL,YouTube's 11-character video identifier. e.g. dQw4w9WgXcQ
channel_name,TEXT,NULL,Channel display name at time of capture. Used for display and channel-type group_rules.
channel_id,TEXT,NULL,YouTube's permanent channel identifier. e.g. UC_x5XG1OV2P6uZZ5FSM9Ttw. Channel names can change — channel_id is stable forever. Group rules should prefer matching on this field over channel_name.
duration_seconds,INTEGER,NULL,Total video length from YouTube Data API. Used to compute completion_rate in memory_engagement. Also used as Shorts fallback classifier (duration < 60s when URL pattern is ambiguous).
is_short,BOOLEAN,FALSE,"TRUE if this is a YouTube Short. Primary classification by URL pattern (/shorts/), fallback by duration < 60s. TRUE automatically maps to system group entertainment. Shorts sessions are regret-marked at session level, not per-video."
transcript_text,TEXT,NULL,"Full auto-generated transcript from YouTube. Best-effort — not all videos have transcripts. The single richest embedding source for YouTube videos. When present, it produces far more semantically meaningful vectors than title alone."
youtube_category_id,INTEGER,NULL,"YouTube Data API categoryId field. e.g. 27=Education, 10=Music, 20=Gaming, 28=Science & Technology. Most powerful Stage 1 structural classifier for YouTube — correctly classifies ~70% of videos at zero ML cost. Fetched in the same API call as other metadata at no extra quota cost."


CRITICAL:  youtube_category_id is the most important Stage 1 signal for YouTube classification.
Not storing it means relying on ML for items that could be classified for free.
Always fetch and store this from the videos.list API response.

TABLE 7 — memory_engagement
Tracks depth of user engagement per memory item. Deliberately separated from memory_items to prevent write amplification — engagement fields update frequently on every revisit while the parent row stays stable.
Every memory_items row must have exactly one corresponding memory_engagement row (total participation). Created in the same ingestion transaction.


The following table:
Attribute,Type,Default,Description
memory_id,UUID,PK + FK CASCADE,Shared primary key with parent memory_items.
dwell_time_seconds,INTEGER,0,Total cumulative foreground time for Gmail and Chrome items. Foreground = tab active + browser focused + user not idle > 30 seconds. All three conditions must hold simultaneously. Incremented on each revisit.
watch_time_seconds,INTEGER,0,Total cumulative foreground playback time for YouTube items. Incremented per viewing session.
first_opened_at,TIMESTAMP,NULL,When the user first accessed this item through the Echo interface. Different from memory_items.first_ingested_at — an item may be ingested automatically but never manually opened.
last_accessed_at,TIMESTAMP,NULL,Most recent access timestamp. Primary driver of the 0.20 recency weight in the re-ranking formula. Indexed DESC for fast recency-sorted queries.
play_sessions_count,INTEGER,0,Number of separate viewing sessions for YouTube items. Distinct from watch_time_seconds — a user who watches a video in 3 separate sittings has play_sessions_count=3.
completion_rate,FLOAT,NULL,watch_time_seconds / duration_seconds. Populated for YouTube items only. Stored to avoid a cross-table JOIN on every retrieval query. Updated every time watch_time_seconds is updated. Values capped at 1.0 for cases where watch time exceeds official duration.


TABLE 8 — regret_events
Records every regret mark and unmark as a separate row. Never updated, never deleted.
Current regret status = COUNT(*) WHERE memory_id = X is odd.
This design preserves full toggle history required for pattern detection (e.g. user consistently regrets YouTube Shorts on Sunday evenings).


The following table:
Attribute,Type,Default,Description
regret_id,UUID,gen_random_uuid(),Own primary key for this specific event.
memory_id,UUID,FK CASCADE,Which memory item is being marked or unmarked.
marked_at,TIMESTAMP,NOW(),Exact timestamp of the mark or unmark action.
regret_note,TEXT,NULL,Optional user note. e.g. wasted 2 hours on this.
regret_hour,SMALLINT,NULL,Hour of day when regret was marked (0–23). Pre-computed from marked_at for fast pattern queries. Reminder system uses GROUP BY regret_hour to detect habitual patterns without EXTRACT() on every row.
regret_day_of_week,SMALLINT,NULL,"Day of week (0=Monday, 6=Sunday). Pre-computed from marked_at. Enables queries like: does user consistently regret Shorts on Sunday evenings?"


CRITICAL:  Odd row count = currently regretted. Even count (including 0) = not regretted. Never DELETE or UPDATE rows.
Each mark and unmark is a new INSERT.

TABLE 9 — user_groups
User-created custom categories. Different from system_groups which are fixed. Examples: Capstone Project, Job Hunt 2026, Tamil Learning.
Classification into these groups is handled by the hybrid KNN + rule engine and requires human approval via group_suggestions.


The following table:
Attribute,Type,Default,Description
group_id,UUID,gen_random_uuid(),Primary key.
group_name,TEXT,NOT NULL,User-chosen name. e.g. Capstone Project
description,TEXT,NULL,Optional description the user writes when creating the group. e.g. All content related to my final year capstone submission. Shown as subtitle in dashboard.
is_active,BOOLEAN,TRUE,Soft delete. FALSE hides group from UI without destroying historical associations in memory_user_groups or invalidating group_rules. Never hard-delete a user group.
created_at,TIMESTAMP,NOW(),
updated_at,TIMESTAMP,NOW(),


TABLE 10 — memory_user_groups
M:N bridge table connecting memory items to user groups. One item can belong to multiple user groups.
The only valid write path is via group_suggestions — no direct INSERTs. Both FKs cascade on delete.


The following table:
Attribute,Type,Default,Description
memory_id,UUID,FK CASCADE,References memory_items.memory_id.
group_id,UUID,FK CASCADE,References user_groups.group_id.
assigned_at,TIMESTAMP,NOW(),When this assignment was approved and created. Enables queries like what did I add to Capstone Project this week.


CRITICAL:  (memory_id, group_id) is the composite primary key. No item enters this table without a human setting decision='accepted' in group_suggestions first.
Any direct INSERT bypasses the human-in-the-loop guarantee and must never be written.

TABLE 11 — embedding_index
Tracks the FAISS embedding state for each memory item. A row is created here only after enrichment completes and the embedding has been successfully indexed in FAISS.
The is_active flag drives the rebuild queue on model upgrades — set all rows to FALSE, background worker re-embeds all inactive rows.


The following table:
Attribute,Type,Default,Description
memory_id,UUID,PK + FK CASCADE,Shared primary key with parent memory_items. 1:0..1 relationship — only exists after preprocessed=TRUE.
embedding_version,TEXT,NOT NULL,Model identifier. e.g. all-MiniLM-L6-v2-v1. Enables safe model upgrades and version tracking.
vector_dimension,INTEGER,NOT NULL,"Dimensionality of the vector. 384 for all-MiniLM-L6-v2, 1536 for OpenAI models."
indexed_at,TIMESTAMP,NOW(),When this embedding was created and indexed.
is_active,BOOLEAN,TRUE,"FALSE means this item needs re-embedding. On model upgrade: UPDATE embedding_index SET is_active=FALSE. Partial index WHERE is_active=FALSE makes the rebuild queue fast even with 50,000+ rows."
embeddable_text,TEXT,NULL,"The exact concatenated string that was fed to the embedding model. Stores: title + extracted headings + domain hints + snippet + category label. Critical for debugging unexpected retrieval results — when item X appears for query Y, this field shows exactly why. Without it, the embedding pipeline is a black box with no audit trail."


TABLE 12 — sessions
Represents a contiguous block of user activity. Items within 5 minutes of each other belong to the same session.
Sessions are closed when a gap of more than 5 minutes is detected. Computed by the Wellbeing Analytics module.


The following table:
Attribute,Type,Default,Description
session_id,UUID,gen_random_uuid(),Primary key.
session_start,TIMESTAMP,NOT NULL,Timestamp of the first item in this session.
session_end,TIMESTAMP,NOT NULL,Timestamp of the last item in this session.
total_duration_seconds,INTEGER,NULL,Pre-computed sum of all member item durations. Accepted denormalization — avoids SUM aggregation on session_memory_map for every dashboard chart render. Consistent with established data warehouse patterns.
dominant_group_id,INTEGER,NULL,FK → system_groups. Whichever system group accumulated the most time in this session. Computed and stored when the session is closed. Drives the session-level category label on the wellbeing dashboard.
source_switch_count,INTEGER,0,"Number of times the source type changed within the session. Chrome→YouTube counts as 1, YouTube→Gmail counts as 1. Raw descriptive count shown to user — not evaluated or judged."


TABLE 13 — session_memory_map
M:N bridge connecting sessions to the memory items they contain. Both FKs cascade.
No additional fields needed — membership is the only information.


The following table:
Attribute,Type,Default,Description
session_id,UUID,FK CASCADE,References sessions.session_id.
memory_id,UUID,FK CASCADE,References memory_items.memory_id.


TABLE 14 — group_rules
Persistent rules for the user group classifier. Without this table, all rules evaporate on restart.
Five rule types cover every classification signal: keyword matching on title, domain matching on chrome pages, channel matching on YouTube, sender matching on Gmail, and time-window matching for temporal patterns.


The following table:
Attribute,Type,Default,Description
rule_id,UUID,gen_random_uuid(),Primary key.
group_id,UUID,FK CASCADE,Which user group this rule belongs to.
rule_type,VARCHAR(30),NOT NULL,One of: keyword | domain | channel | sender | time_window. Determines which field is matched and how.
rule_value,TEXT,NOT NULL,"The match value stored lowercase. e.g. techcorp.com for domain type, hr@techcorp.com for sender type."
is_active,BOOLEAN,TRUE,FALSE disables the rule without deleting it. Allows temporary suspension without losing match history.
created_at,TIMESTAMP,NOW(),
match_count,INTEGER,0,Incremented every time this rule fires and matches a new item. A rule with match_count=0 after 30 days is a dead rule that should be surfaced for review. A rule with match_count=847 is a high-value classifier. Makes the rule engine observable and maintainable over time.


Rule Type Evaluation Logic

The following table:
Rule Type,Matches Against,Example
keyword,memory_items.title ILIKE '%{value}%',value='pytorch' matches any title containing pytorch
domain,chrome_metadata.domain = '{value}',value='github.com' matches all GitHub pages
channel,youtube_metadata.channel_name = '{value}',value='3Blue1Brown' matches all videos from that channel
sender,gmail_metadata.sender ILIKE '%@{value}',value='techcorp.com' matches all emails from that domain
time_window,memory_items.created_at BETWEEN start AND end,"value='2026-01-01,2026-03-31' matches items in Q1 2026"


TABLE 15 — group_suggestions
The human-in-the-loop audit trail. Every potential group assignment must pass through this table before entering memory_user_groups. No exceptions.
The 9-step hybrid KNN + rule classification flow uses this table as its decision checkpoint.


The following table:
Attribute,Type,Default,Description
suggestion_id,UUID,gen_random_uuid(),Primary key.
memory_id,UUID,FK CASCADE,The item being suggested for group assignment.
group_id,UUID,FK CASCADE,The user group it is being suggested for.
rule_score,FLOAT,NULL,0.0–1.0. Proportion of active group_rules that matched this item.
knn_score,FLOAT,NULL,0.0–1.0. Cosine similarity to the centroid of existing group members in FAISS space. Requires >= 6 confirmed items in the group before KNN activates.
suggested_at,TIMESTAMP,NOW(),When the classifier generated this suggestion.
decision,VARCHAR(20),'pending',Current status. Values: pending | accepted | rejected. Default is pending — items must be explicitly accepted. accepted triggers INSERT into memory_user_groups. rejected means no assignment.
reviewed,BOOLEAN,FALSE,FALSE = must appear in next weekly review screen. Set to TRUE after item is shown to user regardless of their decision. Drives the UI notification badge showing count of pending reviews.
decided_at,TIMESTAMP,NULL,When the user accepted or rejected this suggestion.


CRITICAL:  decision DEFAULT must be 'pending' — NOT 'auto_accepted'.
If set to 'auto_accepted', new suggestions bypass human review entirely and the architectural guarantee of human-in-the-loop is silently broken.

TABLE 16 — message_store
Stores multi-turn conversation history for the LangGraph RSE. Managed by LangChain's PostgresChatMessageHistory.
When a user asks find the email about OS and then follows up with now show Chrome pages about the same topic, the second query uses session history to understand the reference.
This table makes cross-turn context possible.


The following table:
Attribute,Type,Default,Description
id,SERIAL,Auto-increment,Primary key. SERIAL not UUID because LangChain manages row ordering by ID internally.
session_id,TEXT,NOT NULL,Groups all messages in one conversation. LangChain generates this identifier. Indexed for fast session retrieval.
message,JSONB,NOT NULL,"Full LangChain message object. Includes role (user/assistant/system), content string, and metadata. JSONB used because message structure varies by type and includes nested fields."
created_at,TIMESTAMP,NOW(),Message timestamp. Used for ordering within a session (ORDER BY created_at ASC).


CRITICAL:  This is Table 16 — outside the 15 core tables.
It is auto-initialized by LangChain on first RSE query if pre-created by setup_db.py (IF NOT EXISTS).
Old sessions should be purged periodically: DELETE WHERE created_at < NOW() - INTERVAL '30 days'.


4. Indexes
All indexes are created by setup_db.py. Partial indexes on boolean columns are critical for performance — they index only the matching subset, keeping the index small even as the table grows.


The following table:
Index,Table.Column(s),Purpose
"UNIQUE(source_type, source_id)",memory_items,Restart-safety deduplication constraint
idx_memory_items_group,memory_items(system_group_id),Wellbeing analytics GROUP BY queries
idx_memory_items_created,memory_items(created_at DESC),Time-range queries and recency ordering
idx_memory_items_unprocessed,memory_items(preprocessed) WHERE FALSE,Enrichment pipeline pickup queue
idx_memory_items_active,memory_items(is_deleted) WHERE FALSE,All UI queries filter deleted items
idx_gmail_email_id,gmail_metadata(email_id),Gmail deduplication lookup
idx_gmail_thread_id,gmail_metadata(thread_id),Thread enrichment queries
idx_gmail_sender,gmail_metadata(sender),Sender-based search and group rules
idx_chrome_canonical_url,chrome_metadata(canonical_url),Revisit detection lookup
idx_chrome_domain,chrome_metadata(domain),Domain-type group rule matching
idx_youtube_video_id,youtube_metadata(video_id),Video deduplication lookup
idx_youtube_channel,youtube_metadata(channel_name),Channel-type group rule matching
idx_engagement_last_accessed,memory_engagement(last_accessed_at DESC),Recency ranking in search results
idx_embedding_stale,embedding_index(is_active) WHERE FALSE,FAISS rebuild queue — partial index
idx_sessions_start,sessions(session_start DESC),Wellbeing timeline queries
idx_group_rules_active,"group_rules(group_id, is_active)",Active rule lookup per group
idx_suggestions_item,"group_suggestions(memory_id, decision)",Pending suggestion lookup per item
idx_suggestions_group,"group_suggestions(group_id, decision)",Pending review count per group
idx_message_store_session,message_store(session_id),Conversation history fetch by session



5. Complete Relationship Map


The following table:
From,Cardinality,To,Notes
system_groups,1:N,memory_items,Every item has one system group. NOT NULL enforced.
system_groups,1:N,sessions,Session dominant category references system group.
memory_items,1:1,gmail_metadata,Total from subtype — must exist for gmail source_type.
memory_items,1:1,chrome_metadata,Total from subtype — must exist for chrome source_type.
memory_items,1:1,youtube_metadata,Total from subtype — must exist for youtube source_type.
memory_items,1:1,memory_engagement,Total participation — every item gets an engagement row.
memory_items,1:0..1,embedding_index,Partial — only after preprocessed=TRUE.
memory_items,1:N,regret_events,Partial — zero or more regret events per item.
memory_items,1:N,group_suggestions,Partial — suggestions created by classifier.
memory_items,M:N,user_groups,Via memory_user_groups. Human approval required.
memory_items,M:N,sessions,Via session_memory_map.
gmail_metadata,1:N,gmail_attachments,Partial — zero or more attachments per email.
user_groups,1:N,group_rules,Partial — zero or more rules per group.
user_groups,1:N,group_suggestions,Partial — suggestions targeting this group.



6. Mandatory Transaction Patterns
All writes to the database must follow one of two atomic transaction patterns. No partial commits.
No single-table INSERTs outside these patterns.

Pattern A — New Memory Item Ingestion (All Three Connectors)
Used by: gmail_connector.py, chrome_connector.py, youtube_connector.py


The following table:
Step,Operation,Failure Behaviour
1,BEGIN,—
2,INSERT INTO memory_items (...) → get memory_id,ROLLBACK entire transaction
3,"INSERT INTO [source]_metadata (memory_id, ...)",ROLLBACK entire transaction
4,"INSERT INTO memory_engagement (memory_id, ...)",ROLLBACK entire transaction
5,COMMIT,—


Pattern B — User Group Assignment (group_suggestions → memory_user_groups)
This is the ONLY valid path to memory_user_groups. No direct INSERTs to memory_user_groups are permitted under any circumstance.


The following table:
Step,Operation,Failure Behaviour
1,BEGIN,—
2,"UPDATE group_suggestions SET decision='accepted', decided_at=NOW()",ROLLBACK
3,"INSERT INTO memory_user_groups (memory_id, group_id, assigned_at)",ROLLBACK
4,COMMIT,—



7. Storage Estimates


The following table:
Component,Per Item,"50,000 Items",Notes
PostgreSQL (all tables),~25 KB avg,~1.25 GB,"Raw text capped at 10,000 chars"
FAISS index,~1.5 KB,~75 MB,384-dim float32 vectors + IDs
Redis revisit cache,~200 bytes,~10 MB,"24hr TTL, evicted automatically"
Redis attachment cache,~200 KB avg,On-demand,"1hr TTL, only top-3 search results"
Typical student usage,~1.25 MB/day,~450 MB/yr,"Gmail ~20/day, Chrome ~30/day, YT ~10/day"



8. Key Design Decisions & Viva Rationale
These are the questions most likely to be asked during viva examination, with the correct defensible answers.

Q1: Why three separate databases instead of just PostgreSQL?
FAISS handles vector similarity search which PostgreSQL cannot do efficiently at scale without extensions.
Redis handles time-windowed lookups (24hr TTL) which would require polling queries on PostgreSQL.
Each database does what it is architecturally best at. PostgreSQL is the truth store, FAISS is the similarity engine, Redis is the ephemeral signal layer.
Q2: Why not use pgvector instead of FAISS?
pgvector is a valid alternative and would simplify infrastructure. FAISS was chosen for better raw similarity search performance at scale and to maintain a clean separation of concerns — vectors belong to the search layer, not the persistence layer.
The architecture's three-layer design is a deliberate principle, not an accident.
Q3: How do you guarantee no duplicate ingestion on restart?
The UNIQUE(source_type, source_id) constraint on memory_items with ON CONFLICT DO NOTHING makes deduplication atomic and database-enforced.
It does not depend on application-level checks that could fail or be bypassed.
Q4: Why is regret_events designed as 1:N with one row per toggle?
A simple boolean flag on memory_items would only capture current state.
The regret reminder system requires temporal history — when was it marked, how long was it regretted, was it re-marked after unmarking.
The 1:N toggle design preserves this history completely. Current state is derived as: odd row count = currently regretted.
Q6: Why is memory_engagement a separate table?
Engagement fields update frequently on every revisit. If they were columns on memory_items, every engagement update would create a write conflict on the central table row.
Separation means engagement writes only touch this lightweight subtable while memory_items remains stable.
Q6: Why is system_group_id NOT NULL with a fallback to misc?
The wellbeing dashboard must always be able to categorize every item.
A NULL system group would cause items to disappear from analytics charts silently.
By enforcing NOT NULL at the database level and always assigning misc as fallback, every item is always visible in at least one category.
Q7: Why is total_duration_seconds stored denormalized in sessions?
Dashboard analytics compute SUM of session durations constantly. If not stored, this would require joining session_memory_map and summing engagement for every chart render.
Storing it redundantly makes analytics queries O(1). This is an accepted and standard data warehouse pattern.