# Review 3 deck: slide text (draft 2, written as the finished system)

> Note (2026-10-22, later): the built deck is the source of truth where it differs from this draft. Differences: the 'revisit check' arrow and the 'No boosts' tile were removed; privacy wording says 'never full emails'; all dates before 28 Oct read Done/Completed; the team table uses full names; the thank-you slide has no footer text.

Status: TEXT ONLY. The deck, the build script and the backgrounds are unchanged.
Basis: every decided item in `docs/Timeless_docs/Echo_System_Design.md` (v3.0) is treated as built by the review on Wed 28 Oct 2026 (owner's instruction, 2026-10-22).
Slide numbers = the current deck (20 slides). Deleted: 7 and 15. The final count is reported at build time.
Written from: the Review 3 template, the design document, and a read of the code (to take names, technologies and pseudocode).
Stretch items (automatic retry of failed items, "fresh" Gmail poll, history import) are NOT claimed as built.

## What the template requires, per section

| Section | Limit | Must contain |
|---|---|---|
| Abstract and Scope | 1 slide | Introduction, overview of scope, context |
| Suggestions from Review 1 and 2 | 1 slide | Earlier suggestions; feasibility shown with progress |
| Pipeline / Architecture | max 2 | Diagram with all modules, dependencies and integration |
| Implementation (for each module) | max 4 (per module or in total? this draft uses 6 slides, 2 per module; merge pairs if your guide reads it as 4 in total) | Module name, technology used, code/algorithm explanation; pseudocode where applicable |
| Project Demonstration | — | Complete working project; demo video ready |
| Test Plan and Strategy | max 2 | Activities with timeline; methods and why; environment and each member's role; benefits and drawbacks; tools |
| Results and Discussion | max 2 | Results vs expected, reasons for deviation, comparison with other methods |
| Timeline | 1 | Update on tasks; project complete in all aspects |
| Conclusion and Future Work | 1 | Key points; glimpse of future work |
| References | 1 | IEEE format |

---

## Slide 1: Title (no text change)
UE23CS441A · Project Progress Review #3 · E.C.H.O. – Efficient Contextualized History Orchestrator · ID 92 · Dr. M. Shalima Sulthana ·
Mrigank Jain, Mohammed Mir Fazlai Ali, Mohammed Shehzaad Khan, Arjun Sai N. Tagline (decoration): "Your private, searchable memory."

## Slide 2: Guide Approval Mail
Empty on purpose: the screenshot goes here. Plain light background, nothing under the image area.

## Slide 3: Outline (template text, unchanged)

## Slide 4: Abstract and Scope
**The problem.** A student gets an interview email, reads articles about operating systems and watches tutorials on the same topic. A week later the material is spread over Gmail, Chrome and YouTube. No tool searches all three, and none knows the articles came after the email.

**What E.C.H.O. is.** A local-first, single-user personal memory. It remembers what you engaged with across Gmail, Chrome and YouTube, lets you ask for it in plain language, and reflects on how you spend your time without judging.

**Three things it does**
1. **Capture** only what you paid attention to. Every email is kept. A page or video is kept only if you read, scrolled, clicked or watched.
2. **Recall** in a chat. One question searches mail, pages and videos together. Answers link back to the original, and follow-up questions continue the same chat.
3. **Reflect** on your time: time per category, sessions, regret you mark yourself, and goal groups that change only with your yes.

**Scope.** One user, one laptop. Desktop Chrome, the Gmail API and YouTube. Not a cloud service, not a productivity score, not a blocker, not a mobile app.

**Local-first.** Storage and processing run on your laptop. Only the question and short snippets go to the language model, never whole mails, pages or transcripts. Gmail and YouTube APIs fetch your own data.

## Slide 5: Suggestions from Review 1 and 2 (two rows, one neutral accent colour)
| Review | Suggestion | Our response | Status |
|---|---|---|---|
| Review 1 | Data retention | A setting: keep forever, 1 year or 6 months. Items older than the window are deleted from every store: database, vector index and cache. | Done |
| Review 1 | Spam filtering | Gmail's own spam filter is our filter. Spam and Trash mail is never read. Drafts are skipped too. | Done |

## Slide 6: Complete Pipeline / Architecture
Redrawn from design doc §3.1: same boxes and arrows, native shapes, one style, no pause popup, no decision IDs.

**Boxes (grouped as in the document)**
1. **User activity:** Gmail mailbox · Chrome pages · YouTube videos.
2. **Chrome extension (Manifest V3):** background script (the gate; the only way data leaves the browser) · Chrome tracker · Gmail tracker · YouTube tracker.
3. **FastAPI backend (one process, port 8000):** Gmail, Chrome and YouTube connectors → **Storage engine** (one transaction per item) → **Enrichment worker** (background thread) · **Recall engine** (LangGraph) · **Wellbeing module** · API routers.
4. **Local stores:** PostgreSQL 16 (the permanent truth) · FAISS (vectors, 384-d) · Redis (revisit flags, settings).
5. **Outside the laptop:** Gmail API and YouTube Data API (your own data) · OpenAI language model (question + short snippets; unclear items; weekly numbers).
6. **Interfaces:** React dashboard (port 3000) · MCP server (lets Claude Desktop query your memory, read-only).

**Arrows (from the document's diagram)**
Gmail → Gmail API → Gmail connector · Chrome page → tracker → background script → Chrome connector · Gmail web (reading time) → Gmail tracker → background script ·
YouTube → tracker → background script → YouTube connector → YouTube Data API (title, channel) · all connectors → storage engine → PostgreSQL ·
PostgreSQL → enrichment worker → FAISS and PostgreSQL · enrichment worker → language model (only unclear items) ·
Dashboard → routers → recall engine → PostgreSQL + FAISS · recall engine ⇄ language model · routers → wellbeing → PostgreSQL · wellbeing → language model (numbers only) ·
MCP server → recall engine and wellbeing (read-only) · Redis ⇄ Chrome and YouTube connectors (revisit check).

**Caption:** Capture writes through one storage engine. Enrichment runs in the background. Recall and wellbeing only read.

## Slide 7: DELETED

## Slide 8: Implementation, Module 1 (part A): Capture
**Title:** Module 1: Memory Capture and Enrichment, Part A: Capture
**Technology:** Chrome extension (Manifest V3, JavaScript) · FastAPI 0.115 on Python 3.11 · Gmail API and YouTube Data API v3 (read-only OAuth) · PostgreSQL 16 via SQLAlchemy 2 · Redis (revisit flags).

**One gate per source**
- **Chrome page:** the extension counts only active time: tab in front, window focused, not idle for 30 s. The server repeats the same checks, because the server never trusts the client.
- **YouTube video:** kept if you watched long enough, watched a good part of it, came back to it within a day, or paused or seeked it yourself.
- **Gmail:** every email is kept. New mail is fetched every 30 seconds from the last sync point. Spam, Trash and drafts are skipped. Reading time is recorded when you open a mail.

**The numbers**
| Rule | Value |
|---|---|
| Page seen at all | 5 s in front |
| Keep a page if ANY of | 10 s active · scrolled 15 % · a click or text selection (link clicks do not count) · opened again within 24 h |
| Keep a Short | 10 s watched |
| Keep a regular video | 20 s watched |
| Keep any video | ≥ 10 % watched with at least 5 s · or opened again within 24 h at 5 s of watch |
| Gmail | every mail · checked every 30 s · only new mail |

**Pseudocode (from `extension/background.js`)**
```
if seconds_in_front >= 5:                       # phase 1: exposure
    keep = active_seconds >= 10
        or scroll_depth  >= 15%
        or clicked_or_selected_text
        or opened_again_within_24h              # phase 2: any one is enough
```

**Never captured:** incognito windows · typed text, passwords and form fields · sensitive pages (login, payment, settings …) · the text of app pages (Slack, Notion, Jira, Figma …; title only) · secret keys in URLs (token, code, key … removed before storing).

**Stored safely:** one atomic transaction per item (item + source details + engagement + visit row). The same item is never stored twice (unique source + id). A return visit adds time to the same item.

**Example**
- Open a page, close it after 3 s → thrown away.
- Open 8 s, scroll 20 % → saved.
- Read 12 s, no scroll → saved.
- Open a login page → never stored.

## Slide 9: Implementation, Module 1 (part B): Enrichment
**Title:** Module 1: Memory Capture and Enrichment, Part B: Enrichment
**Technology:** Python background thread (checks every 10 s, 10 items per batch) · sentence-transformers 5.4.1 with `snowflake-arctic-embed-xs` (384-d, reads 512 tokens) · FAISS (faiss-cpu 1.8, flat index) · PostgreSQL · OpenAI model for unclear items only.
**Embedding model:** chosen over `all-MiniLM-L6-v2` after a 39-question comparison on our own hand-made test set (notes: right item first in 37 of 39 vs 31 of 39). Full re-embed done once.

**Steps (one item at a time through the worker)**
1. **Fetch** unprocessed rows, oldest first.
2. **Clean:** mail HTML to text and quoted replies cut (signatures are kept on purpose: removing them cut real text in 6 of 8 test mails); page text arrives already extracted, with its headings. Attachment text is extracted here (PDF, Word, Excel, PowerPoint, text, mail files; OCR for images).
3. **Build the text to embed:** short hints first (subject, keywords, sender domain / title, headings, channel), long text last, cut at the model's own 512-token window. What is stored is exactly what was embedded.
4. **Embed** the batch.
5. **Classify** into one of five groups (below).
6. **Index** into FAISS and save the file **before** marking the row done (crash-safe).
7. **Mark processed.** If a step fails the row is set aside as failed and the others continue.

**Classification: the first stage that is sure wins**
1. **Structural signals:** Gmail labels, sender domain (`.edu` → study), YouTube category, Shorts → entertainment.
2. **Curated domain list:** Chrome only (geeksforgeeks.org → study).
3. **Nearest centroid:** compare the item's meaning with an example vector per group; accept only a clear winner. Centroids start from real example sentences and refresh from confirmed items when the worker starts and the saved file is older than 30 days.
4. **Language model:** title (200 characters) + 300-character snippet, constrained to the five group names. No model, an error or a bad reply → `misc`.
Groups: work · study · entertainment · personal · misc. A row never has an empty group.

**Example:** the interview email → Stage 1 (sender domain on the work list) → work. A GeeksforGeeks article → Stage 2 (domain list) → study. A blog nobody listed → Stage 3, or Stage 4 if the match is unclear.

## Slide 10: Implementation, Module 2 (part A): Recall
**Title:** Module 2: Retrieval and Synthesis Engine (RSE): how a question is answered
**Technology:** LangGraph + LangChain · OpenAI `gpt-5.6-luna` (Responses API) · PostgreSQL full-text search · FAISS · cross-encoder `ms-marco-MiniLM-L-6-v2` (runs locally) · FastAPI · React + TypeScript · MCP Python SDK.

**The idea:** the model decides **what** to search for. **How** the search runs is fixed Python. The model never touches the database.

**Per question**
1. **Agent:** the model calls one read-only tool, `search_memories`, with a query, optional sources, a time phrase ("after the interview email") and an optional sender. Up to 5 search rounds; after round 5 it must answer. Usually 2 model calls, at most 6. The cap is enforced in code.
2. **Anchor:** plain Python turns the time phrase into a hard filter (the anchor item's time).
3. **Search two ways at once:** keyword search (PostgreSQL full text) and meaning search (FAISS, three rephrasings).
4. **Merge by rank** (Reciprocal Rank Fusion): each list gives an item `1 / (60 + rank)`; scores add up.
5. **Check quality** (strong, weak or empty). If weak, **widen** (wider dates, all sources, core keyword), up to 3 times.
6. **Re-rank:** a cross-encoder reads the question and each item together. Final score = 0.8 × cross-encoder + 0.2 × effort (time spent, scroll, revisits).
7. **Add context:** the rest of the mail thread, or neighbouring items within 5 minutes.
8. **Answer:** the model writes a short chat answer from at most 10 snippets of 500 characters, citing items by number. Citations become **memory pills** (source logo, sender or site, date); click one to open the original. Live progress ("Searching Gmail…") streams while it works.
Not used, by decision: no recency boost and no diversity re-ranking.

**Pseudocode**
```
rrf_score(item)   = Σ over {keyword list, meaning list}  1 / (60 + rank)
final_score(item) = 0.8 × cross_encoder(question, item) + 0.2 × effort(item)
```
**What the model sees:** your question, recent chat turns, and short numbered snippets (plus the best three matching attachment passages, about 1,500 characters, when the question asks for file content). Never whole mails or pages.
**Follow-ups:** each chat has its own session; the next question continues from the earlier turns.
**MCP server:** Claude Desktop (or any MCP client) can call the same read-only tools (`search_memories`, `get_memory`, `recent_activity`) over stdio; Echo stays the engine.

## Slide 10b (new, optional): Recall, worked example
**Example question:** "OS material I read after the interview email."
1. The model calls `search_memories(query="operating systems scheduling", time_phrase="after the interview email")`.
2. Python finds the interview email (keyword "interview", Gmail) and takes its arrival time.
3. Keyword and meaning search run only on items after that time.
4. Rank fusion merges the two lists; the cross-encoder re-orders them.
5. The model answers with pills for the two best items. Clicking a pill opens the original page.
Label: "Example question" (traced from the design; not a recorded run).

## Slide 11: Implementation, Module 3: Wellbeing
**Title:** Module 3: Digital Wellbeing and Analytics (WBA)
**Technology:** FastAPI router · PostgreSQL via SQLAlchemy · Redis (reminder limits) · FAISS vectors for similarity · OpenAI model for the weekly insight (numbers only) · React 18 + TypeScript + Vite + Tailwind dashboard.

**What it measures**
1. **Time:** per category and per source. Mails count only if you opened them; pages count when stored; videos count watch time. Added up per visit, so a return visit counts on the day it happened.
2. **Sessions:** items less than 20 minutes apart belong to one sitting.
3. **Regret:** you mark or unmark an item yourself. Each mark is a new event (odd count = currently regretted). Optional reminders (at most 2 a day, only for a habit) quote your own earlier note. Nothing is scored or judged.
4. **Weekly insight:** the model receives only totals, counts and hours, never titles or URLs. A reply with judging words ("you should", "wasted", "too much") is rejected and replaced by a plain summary.

**Groups you control**
- You create a goal group ("Capstone project"), add a few items and write rules (keyword, domain, channel, sender, date range).
- A new item is scored by **rules** (do any match?) and **similarity** (mean of its 3 nearest confirmed members). A group starts suggesting after 5 confirmed items.
- **Confident:** a rule matches **and** similarity is above the bar → the item is added at once, marked "added automatically", and one click undoes it in the Sunday review.
- **Unsure:** it waits for your yes or no.
- Only items you confirmed teach the similarity. Echo never decides alone when it is unsure.

**Pseudocode (`wba/wellbeing_analytics.py`)**
```
for item in items_in_time_order:
    if gap_since_previous_item > 20 minutes:  start a new session
    else:                                     add item to the current session
```
**Example (illustrative; the real function was run on made-up data):** six items on one morning (09:00, 09:12, 09:25, 09:40, 10:30, 10:45) → two sessions: 09:00–09:40 (4 items, 3 sources) and 10:30–10:45 (2 items). The 50-minute gap starts the second.

## Slide 12: Project Demonstration
**Title:** Project Demonstration: how the demo runs. One left-to-right pipeline, one short label per stage. Footer: "Recorded demo video ready as backup."
1. **Ingestion:** Gmail, Chrome and YouTube activity is captured through the intent gates.
2. **Enrichment:** the item is cleaned, embedded, classified into a group and indexed.
3. **Recall:** a question is answered in chat with memory pills; a follow-up continues the same chat.
4. **Wellbeing:** time and patterns, a regret marked, a group suggestion reviewed.

## Slide 13: Test Plan and Strategy (1/2): Activities, environment, team
Everything below is a plan unless marked Done.

| Dates | Activity | Status |
|---|---|---|
| 6–19 Oct | File-by-file review of every module with offline simulation tests: 30 backend and script checks and 56 extension checks (recorded in commit messages) | Done |
| 22–25 Oct | Re-measure thresholds on the re-embedded data; unit and integration tests for the 25 planned scenarios | In progress |
| 26–27 Oct | End-to-end run, evaluation (slide 16), rehearsal | Planned |

**Test environment:** Windows 11 laptop · local PostgreSQL 16 and Redis (WSL) · Chrome with the extension · a test Gmail and YouTube account.

**Team roles: four equal shares.** Each member owns one area and cross-checks one area of another member. (Proposal; swap names as you like.)
| Member | Owns | Cross-checks |
|---|---|---|
| Mrigank Jain | Capture gates (Chrome, YouTube) and privacy rules | Enrichment |
| Mohammed Mir Fazlai Ali | Enrichment and classification | Recall |
| Mohammed Shehzaad Khan | Recall: retrieval quality and citations | Wellbeing |
| Arjun Sai N | Wellbeing, groups and the dashboard | Capture |

## Slide 14: Test Plan and Strategy (2/2): Methods and tools
**Functional**
- *Unit:* gate boundaries (3 s, 6 s, 8 s + 20 % scroll, a login page); session gap (exactly 20 min vs 20 min 1 s); rank fusion on a small fixed list.
- *Integration:* each API route through the FastAPI test client; capture → stored row → enriched → searchable.
- *End to end:* send a mail, browse, watch, then ask a question and check the answer cites the right item.
**Non-functional**
- *Performance:* time per question, model calls per question, enrichment speed.
- *Privacy and security:* inspect every payload sent to the model; check that sensitive pages, incognito, Spam and Trash never produce a row.
- *Reliability:* behaviour when Redis is down, the model is down, or a worker step fails.
**Test cases:** the 25 scenarios in design doc §19: Gmail 5, Chrome 7, YouTube 4, Recall 4, Groups 2, Enrichment 1, Retention 1, Privacy 1 = 25.
**Tools:** Python test scripts (pytest), FastAPI test client, a Node harness with a fake browser for the extension, TypeScript compiler + Vite build, Python compile check, browser DevTools.
**Why:** fast, repeatable, and never touches personal data. **Drawback:** simulated services can miss live API changes, so the end-to-end run uses the real services.

## Slide 15: DELETED

## Slide 16: Results and Discussion: Evaluation (to be measured)
**Statement (top of slide):** The evaluation runs after the new embedding model is switched in and all items are re-embedded. Every field the template asks for will be measured on that same data.
**Fields (all "to be measured"; no values on the slide)**
- Retrieval quality on a fixed question set (Recall@5, MRR)
- Hybrid search vs keyword-only vs meaning-only
- Embedding model comparison (previous vs new)
- Time per question (median, 95th percentile)
- Model calls and cost per question
- Enrichment speed (items per minute) and how many items each classification stage resolves

## Slide 17: Timeline: Update on Tasks
Month-level, taken from `git log` (the Phase 2 review date is not stored in the repo; the ESA files are dated 21–22 Apr 2026).
| When | Milestone | Status |
|---|---|---|
| Apr 2026 | Capture, enrichment and retrieval foundations merged into one codebase | Completed |
| Jun 2026 | Storage engine (PostgreSQL, FAISS, Redis), security fixes, Gmail reading time, intent gates aligned with the design | Completed |
| Jul 2026 | Wellbeing module, finished retrieval engine, first dashboard | Completed |
| Aug 2026 | Review 1: scope focused on the core recall path | Completed |
| Sep 2026 | Wellbeing and dashboard restored; Review 2 deck and design document | Completed |
| 1–21 Oct 2026 | File-by-file review (reliability, thread safety, privacy); every design decision closed; system design document v3.0 | Completed |
| 22–25 Oct 2026 | Final build: local stack, new embedding model, agent recall, memory pills, MCP server, retention, auto-grouping; freeze on 25 Oct | Completed |
| 26–27 Oct 2026 | Evaluation and rehearsal | In progress |
| 28 Oct 2026 | Review 3 | — |

## Slide 18: Conclusion and Future Work
**Conclusion**
1. **One private memory.** It keeps what you engaged with across Gmail, Chrome and YouTube, stores it on your laptop and sorts it into five groups in the background.
2. **Ask, don't search.** Keyword and meaning search are merged and re-ranked; the answer cites the original items, and follow-up questions continue the same chat.
3. **Reflect without judging.** Time per group, sessions, regrets you mark yourself, and a weekly summary built from numbers only. Groups change only with your yes.
4. **Privacy by design.** Sensitive pages, typed text and incognito are never captured; the language model sees only the question and short snippets.
**Future work**
- **Import and export your data:** bring in past history from Gmail, Chrome and YouTube (Google Takeout, Chrome History file) and export your memories as JSON or CSV.
- **Publish the Chrome extension** to the Chrome Web Store.
- **More apps:** Google Calendar, Google Drive and others.

## Slide 19: References (IEEE)
Keep [1] Sentence-BERT, [2] FAISS, [3] Reciprocal rank fusion.
Candidates to add. **Likely (from memory, verify each in full before use):**
[4] R. Nogueira and K. Cho, "Passage Re-ranking with BERT," arXiv:1901.04085, 2019.
[5] L. Merrick et al., "Arctic-Embed: Scalable, Efficient, and Accurate Text Embedding Models," arXiv:2405.05374, 2024.

## Slide 20: Thank You
**Thank you.** We would be glad to take your questions and feedback.
Team and guide names as on slide 1. Restrained background, one accent colour.

---

## What must be true on 28 Oct for every line above to be true (your build checklist)
Decided items the text now treats as built, with the document id:
- **Capture:** gate numbers (A39: scroll 15 %, Shorts 10 s, completion 10 % + 5 s, link clicks excluded) · YouTube revisit check at 5 s (A34) · Gmail history sync with Spam/Trash dropped (A5, A38) · sensitive-page rules and URL key stripping (A7) · app pages title-only and the shared rules route (A8, A9) · per-visit time rows (A1.1b).
- **Storage:** local PostgreSQL 16 and Redis, UTC per connection (A31) · retention setting and delete job in every store (A37).
- **Enrichment:** `snowflake-arctic-embed-xs`, token budget, stored = embedded (A16, A40) · seed sentences, balanced recompute at worker start (A18, A20, A36) · attachment text at ingestion, matching passages only (A4, A22, A23) · classifier constrained to five names (A21).
- **Recall:** agent loop with `search_memories`, 5-round cap (A26) · memory pills, sessions list and past chats (A27) · live progress (A28) · MCP server (E1) · best-matching 500-character window for snippets and the cross-encoder (A24, A40).
- **Wellbeing:** auto-add path with Sunday undo and accepted-only learning (A42) · KNN after 5 items (A39).
- psql
- **Kept as plans, not results:** test outcomes, the measurements on slide 16, and the evaluation row on slide 17.
