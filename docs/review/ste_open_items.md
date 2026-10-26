# STE Review — Open Items

Source: the `ste/` file review, committed as `f555d9a` on branch `phase3-review2` (2026-10-14).
Working file — committed to the repo from 2026-10-22 so Claude web can read it from GitHub. Tick a box when an item is done, and write the date + commit next to it.

**How to work through it**
1. Section A is ON HOLD until the first review pass of every file is finished (his decision, 2026-10-17). Then decide A first — it changes what B, C and D look like.
2. Section C items are done only when we review that file (one-file-at-a-time rule).
3. Section B needs a backup before any statement runs: a Neon snapshot or branch for Neon (B1–B3, done), a `pg_dump -Fc` of `echo_db` into `C:\Users\mdshe\echo_backups\` for the local database (B5).
4. Section D is built after the whole review is finished.

**Audit snapshot — 2026-10-26 (top-level IDs; supersedes the 2026-10-25 one):** 86 entries = **85 complete/resolved + 1 not fully complete** (E1 built later on 2026-10-26). Counting rule: A1–A45 are owner-decision entries, counted complete when the decision is recorded; their implementation follow-ups are audited separately below. B4 is informational/no action. Breakdown: A 45/45 decisions recorded; B 5/5 resolved (B5 ran 2026-10-25); C 29/29 resolved; D 5 complete (D1, D2, D3, D4, D5; D3 and D5 built 2026-10-25, D2 closed 2026-10-26 with old `.doc` dropped), D6 open; E1 built 2026-10-26. E2 (import personal history + export button) was removed from scope on 2026-10-25: the presentation uses the test account only. The one not-fully-complete top-level ID is D6 (four frontend gaps, found 2026-10-26, nothing built); E1 was the other until it was built on 2026-10-26. D6 was added 2026-10-26, so the total went from 85 to 86. A26 was built later on 2026-10-26; the top-level count is unchanged because A entries count as complete when the decision is recorded (its build status is in the A26 entry). This top-level count is not a claim that all decided features are implemented. The 2026-10-25 build work (A16/A40 + B5, A27, A28, A37, OCR, D3, D5, A42, A18 part 1, review fixes) is committed and merged to `main` (PRs #12, #13, #14); the two branches were checked after the PR #14 merge and nothing was lost.

**Decision 2026-10-25 (his): the presentation runs on the TEST ACCOUNT only.** There is no time to move to his personal account. So E2 (import personal history + export button) is out of scope, and the Mon 26 / Tue 27 Oct retest and the threshold recompute (A18, A40) use the test account's data.

**Implementation follow-ups inside decided A entries (not additional top-level IDs):** 3 decision IDs still have an implementation step open or partial. They wait on the Mon 26 / Tue 27 Oct retest on the test account's live data: A18 part 2 (seed-centroid threshold), A40 (threshold retune) and A42 (the KNN bar is built, its value must be re-measured). A26 (tool loop) was built 2026-10-26; its first live run against OpenAI is still to be done by him. `AGENTS.md` was brought in line with `CLAUDE.md` on 2026-10-25, so none of them is blocked by it. A16 closed with B5; A17 closed by his decision (no further build); A23 and A24 closed 2026-10-26 in a simplified form (leading text, bigger fixed caps, no passage selection); A27 complete (past-chats screen built, touch "peek" dropped); A28 closed as it is (progress lines only); A37 closed; A42's build is done (only the bar value is open). Their status is checked after A45 below.

Original inventory: 45 decisions · 5 live-DB entries · 29 cross-file fixes · 5 build items · 2 later items = **86, minus E2 (removed 2026-10-25) = 85**. C29 (delete pending-answers file) was added 2026-10-22; A44 retry + A45 fresh Gmail poll were added 2026-10-21 as stretch (A45 closed with no build on 2026-10-24); A42 auto-accept + A43 user-group nesting, A41 Gmail open tracking, A40 model choice, E2 personal-data import, and A37–A39 (retention, spam, timings) were added 2026-10-21; A5/A6 were added 2026-10-15; A7–A24 were added 2026-10-17; A25–A28 were added 2026-10-18; E1 MCP was added 2026-10-18.

---

## A. Decisions waiting on you

### A1. Time filters ignore when the user opened things — [x] decided 2026-10-21
- **Problem:** retrieval time filters use `memory_items.created_at` only.
  Gmail = received date, Chrome/YouTube = first capture. `first_opened_at` / `last_accessed_at` are used
  only for sorting. Opens between the first and the last are not stored at all (no visit-history table).
- **Where:** `rse/search_coordinator.py` `_dynamic_filters` (`m.created_at >= time_filter`, start date only,
  no end date) and `_anchor_constraint` (`m.created_at > / < anchor`).
- **Example:** email received 5 Oct, opened 12 Nov and 20 Dec. "The mail I saw in November" → filter starts
  1 Nov → the email is dropped.
- **Options:**
  1. Visit-history table (`memory_id`, `opened_at`). Fully correct. Breaks the 16-table lock.
  2. Filter on `created_at` OR `first_opened_at` OR `last_accessed_at`, and add an end date.
     No schema change. Still misses a month where only a middle open happened. **(Recommended.)**
  3. Accept as a known limitation.
- **A1.1 decided 2026-10-21 (his proposal):** the filter depends on the source.
  - Gmail: filter on when the mail **arrived** (`created_at`), as today.
  - Chrome + YouTube: a new **visit table**, one row per gated visit (`memory_id`, `visited_at`). The item
    matches if ANY visit falls in the range.
  - Write points: `ste/storage_engine.py` `store_chrome_page` and `store_youtube_detection` (both run once per
    gated visit). Backfill old rows with one visit at `first_opened_at`/`created_at` and one at `last_accessed_at`.
  - Filter changes: `rse/search_coordinator.py` (EXISTS on the visit table), `rse/fusion.py`,
    `rse/graph_nodes.py` evaluate_quality.
  - Known gap: "the mail I read yesterday" matches by arrival date, not open date.
  - **A1.1b decided 2026-10-21: option b.** Chrome + YouTube visit rows also store `dwell_seconds`; wellbeing
    sums visits by day. YouTube heartbeats (`update_youtube_watch_time`) update the latest visit row (ties to A2).
    Gmail keeps today's rule (time lands on the last-opened day) — and see A41, which may remove Gmail opens.
    This settles A30.
- **A1.2 decided 2026-10-21: option a, keep start-only (his pick).** Claude flagged: "yesterday" also returns today's
  items and "last month" also returns this month's; the cheap fix if it shows in testing is option b (parser returns
  an exclusive `time_filter_end`, null for open-ended asks).
  - **His addition (2026-10-21):** code returns everything from the start date on; the answering LLM keeps only
    items inside the asked period (prompt change). Build needs: (1) a `TODAY: <date> (<weekday>)` line + the rule
    "if the question names a period, use only items inside it" in `rse/llm_synthesizer.py`; (2) Chrome/YouTube
    item lines also show the visit times inside the range (the line shows `created_at` = first capture today,
    line 185). Known risk: only the top 10 (`SYNTHESIS_TOP_RESULTS`, `rse/config.py:102`) reach the LLM — on a
    busy day today's items can fill all 10 and "yesterday" comes back empty → then switch to option b.
    Check again after A26 (Luna tool rounds may change the prompt layout).
- **Decision:** A1 fully decided 2026-10-21 (A1.1 source-based filter + visit table, A1.1b dwell per visit, A1.2 start-only).

### A2. Meaning of YouTube watch time — [x] decided 2026-10-21
- **Today:** `watch_time_seconds = GREATEST(stored, session_total)` → the longest single session.
  10 min today + 10 min tomorrow = 10 min.
- **Options:** "total time spent" (adds across re-watches) or "how far into the video" (keep as is).
- **Decides:** C6.
- **Decision (2026-10-21): keep "how far" (longest session).** Total time comes from the per-visit rows (A1.1b).
  Keeps `completion_rate` honest (5 × first 2 min of a 10-min video stays 20%, not 100%). C6 → no change.

### A3. Re-embed a Chrome page when a revisit brings new text? — [x] decided 2026-10-21
- **Today:** a revisit with new page text updates `raw_text`, but the FAISS vector stays the old one.
- **If yes:** goes to Section D.
- **Decision (2026-10-21): yes, re-embed when text changes.** Build: in `ste/storage_engine.py` `_upsert_memory_item`
  update branch, Chrome only, set `preprocessed = FALSE` when `raw_text IS DISTINCT FROM` the new text. ENP re-embeds;
  `FAISSManager.add` already replaces in place. Accepted side effects: re-classification (group may change, Stage 4
  may call the LLM once); always-changing pages re-embed on every revisit (small CPU cost).
- **Built 2026-10-22:** `_upsert_memory_item` update branch sets `preprocessed = FALSE` for Chrome when the new `raw_text` is not NULL and differs from the stored one (`IS DISTINCT FROM`; a SET expression reads the old row). SQL NOT run against Postgres (DB rule). Live check: revisit a Chrome page whose text changed, confirm `preprocessed` flips to FALSE and ENP re-embeds it.

### A4. Should attachment text be embedded? — [x] decided 2026-10-21
- **Today:** Tier-2 extracts attachment text only at query time (`enp/attachment_processor.py`); it never
  reaches FAISS. Schema: `gmail_attachments.is_processed = TRUE` only once merged into embeddable text.
- **Decides:** part of D2.
- **Decision (2026-10-21, his idea, revised): extract at ingestion, merge into the email's embedded text.**
  - Every A22 type (PDF, DOCX, TXT/CSV/MD/JSON/ICS, HTML, XLSX, PPTX, EML, images via Tesseract OCR); inline
    logos skipped (A22). Extraction runs in the ENP background worker.
  - Text stored in a new column `gmail_attachments.extracted_text` (NOT inside `raw_text` — that stays the real
    body). `is_processed = TRUE` once merged.
  - Semantic: embedded text uses a shared 800-char budget (A24): body takes up to ~500, attachments fill the rest
    up to 800, each attachment's start in turn. A "PFA" mail gives nearly all 800 to its PDF.
  - Keyword: the stored tsvector (A24) includes the FULL extracted text.
  - Answers: A23 query-matched passages read the stored text; no Gmail download at query time.
  - Limit: semantic sees only each attachment's start; deep content is found by keyword only.
  - Build: B5 (Thursday re-embed) must also extract + backfill attachments for the test mailbox.

### A5. Replace Gmail polling with something incremental — [x] decided 2026-10-21, built 2026-10-24 (found 2026-10-15, `gmail_api.py`) — DISCUSS AT THE END
- **Today:** every 30 s, `fetch_and_store_new_emails` pages through ALL message ids in the mailbox (100 per
  Gmail call). The DB check is now batched, but a 10,000-email mailbox is still 100 Gmail list calls per poll,
  forever, even when nothing arrived.
- **Your direction:** move away from "pull 100 every 30 s" to something more optimal; a webhook is the front
  runner, not decided yet.
- **Options:**
  1. **Webhook / push** — Gmail `users.watch` + Google Cloud Pub/Sub. Gmail notifies us when the mailbox
     changes, with a `historyId`; we then fetch only the changes via `history.list`. Near-instant, zero idle
     calls. Needs: a GCP Pub/Sub topic, Gmail granted publish rights on it, and either a public HTTPS endpoint
     (push subscription — hard for a local backend; needs a tunnel) or a pull subscription (the backend asks
     Pub/Sub for messages; no public URL). `watch` expires every 7 days and must be renewed.
  2. **Incremental polling** — keep a timer, but call `history.list` from the last saved `historyId` instead
     of listing the mailbox. One cheap call per poll; no GCP setup. Needs a full re-sync fallback when the
     `historyId` is too old (Gmail returns 404).
  3. **Stop paging early** — stop once a whole page is already stored. Smallest change; can miss an older
     email whose store failed earlier.
  4. Keep it as is (only fine for a small mailbox).
- **Note:** 1 and 2 both need somewhere to keep the last `historyId` (no new table allowed — Redis or a
  file, like the settings store).
- **Decision (2026-10-21): option 2, incremental polling with `history.list`.** Keep the 30 s timer; ask only for
  changes since the saved `historyId` (Redis key `echo:gmail:history_id`). Fallback: on 404 (id too old) or a
  missing key, one full listing, then save the new id. Dedup (`UNIQUE(source_type, source_id)`) makes the fallback
  safe. Why not the webhook: needs GCP Pub/Sub + a tunnel, and puts a cloud piece in the local privacy story.
- **Built 2026-10-24 (cloud session):** `ingestion/gmail/gmail_api.py` (`_list_new_ids_from_history`, `_current_history_id`,
  `fetch_and_store_new_emails`) + `ste/redis_manager.py` (`get_gmail_history_id` / `set_gmail_history_id`, key
  `echo:gmail:history_id`, no TTL; Redis down = full listing, one WARNING per outage).
  - History call: `historyTypes=["messageAdded"]`, 500 per page, no retry on 404 (`_call_with_backoff(..., no_retry_statuses)`).
  - Full listing (no key, Redis down, or 404) reads `getProfile().historyId` BEFORE listing.
  - The id moves forward only when every new email of the poll is in the DB (re-queried; `store_in_postgresql` cannot tell a
    duplicate from an error). A failed store keeps the old id, so the next poll sees the same window again.
  - Excluded-sender mail: ids kept in `_excluded_ids` are re-checked each poll, so un-excluding still captures them. After a
    backend restart that memory is gone, and mail from before the saved id is no longer re-listed (the old full listing
    re-fetched it): un-excluding a sender does not bring back its OLD mail. Accepted.
  - Not covered: mail moved OUT of Spam later (no `messageAdded` for it); a label-only change. Accepted.
  - Checked on a mock Gmail service (9 scenarios: first full sync, idle poll = one call, new mail with spam/trash/sent, store
    failure then retry, 404 fallback without a sleep, Redis down, excluded then un-excluded sender, mail trashed before the
    fetch, draft) and on the Redis helpers with a fake client. NOT checked against the real Gmail API: the field names
    (`history[].messagesAdded[].message.labelIds`, response `historyId`) are from the API docs/memory, so run one live poll
    and look for "Gmail poll (history)" in `logs/echo.log`.

### A6. Gmail HTML/body cleaning gaps — [x] decided 2026-10-21, A6.1 + A6.2 built (found 2026-10-15, `gmail_api.py`) — DISCUSS AT THE END
Fixed already: HTML keeps line breaks (ENP can now strip quotes/signatures), script/style/head/comments/
tracking pixels removed, entities and zero-width characters cleaned, charset honoured, drafts skipped.
Still open — decide each:
- [x] **Hidden preview text.** Marketing mail hides a preview line in a `display:none` span; it survives into
      `raw_text` and the embedding. Fix: drop hidden elements with BeautifulSoup (as ENP's pixel remover does).
- [x] **Body split into several text parts.** Some mail apps split one body around an inline image; only the
      first text part is kept. Fix: join all non-attachment text parts inside `multipart/mixed`.
- [x] **Signature logos counted as attachments.** Inline images (`logo.png`, `Content-ID`) are stored in
      `gmail_attachments`. Fix: skip inline images under a size limit, or mark them. Affects D2.
- **Decision (2026-10-21):**
  1. Hidden preview text: **drop hidden elements** — BeautifulSoup in `_html_to_text` removes elements whose inline
     style is display:none / visibility:hidden / max-height:0 / font-size:0 / opacity:0. Class-based hiding not caught.
  2. Split body: **join all body text parts in order**; inside multipart/alternative pick one (plain, else HTML);
     parts with a filename stay attachments; test a forwarded (nested message) mail so nothing is joined twice.
  3. Signature logos: already settled by A22 (skip inline logos).
- **Built (his, verified by him; light re-check 2026-10-24, cloud session):** `ingestion/gmail/gmail_api.py`.
  - A6.1 `_html_to_text` drops elements whose inline style hides them (`_element_is_hidden`): display:none and opacity:0
    always; max-height:0 only with overflow:hidden; visibility:hidden and font-size:0 only when nothing inside can
    still show (block-level child or a child that re-declares the property), because Outlook wraps real text in
    font-size:0 cells. A mail with no hiding property in its text skips BeautifulSoup. Class-based hiding not caught.
  - A6.2 `_collect_body_parts` / `extract_body` join all body text parts in order; multipart/alternative picks plain,
    else the first HTML; parts with a filename stay attachments; the same part is never walked twice. An attached
    message (`message/rfc822`) is skipped when the mail has other text (so a bounce does not repeat the original it
    quotes) and used only when it is the mail's only text. Consequence: a "forward as attachment" WITH a note keeps the
    note and not the forwarded text.
  - Re-check: synthetic MIME trees with the BeautifulSoup path (preview span, Outlook cell, opacity, visibility),
    split text parts, alternative, HTML-only, forward with and without a note, a repeated part object: all as above.

### A7. How to detect sensitive Chrome pages — [x] decided 2026-10-21 (found 2026-10-17, `chrome_connector.py`) — DISCUSS AT THE END
- **Today:** a fixed list of 14 words (`login`, `auth`, `payment`, ...) matched as plain substrings of the URL,
  in 3 copies that must stay equal: `chrome_connector.py`, `extension/background.js`,
  `extension/content/chrome_tracker.js`.
- **Offline probe (2026-10-17):**
  - Wrongly dropped: `/authentication-explained`, `/oauth-2-in-plain-english`, `/accounting-basics`,
    `stripe.com/docs/payments`, `/author/jane` (`/auth` is a prefix of `/author`).
  - Wrongly kept: `/reset?token=abc`, `/verify-email?code=999`, `bank.com/netbanking/home`. The token or code
    is then stored in `source_id` and `url`.
  - GitHub skip is broken too: `"github.com/issues"` never matches a real repo issue
    (`github.com/owner/repo/issues/12`) or PR (`/pull/3`).
- **Options:**
  1. Keep the list (known false positives and misses).
  2. Layered check **(recommended):** (a) the page's own form fields decide first — the content script's
     `hasPrivateFormSurface` (password / card / email inputs) already exists; send it as an `is_sensitive_page`
     flag; (b) URL words matched as whole tokens only (split the path on `/ - _ .`), plus `token`, `reset`,
     `verify`, `2fa`, `mfa`, `sso`; (c) strip secret-looking query keys (`token`, `code`, `state`, `key`, `sig`,
     `session`) before the URL is stored; (d) the user's own `excluded_domains` for banks.
  3. An LLM classifies each page. Rejected by the rules: the LLM may only see query text and snippets (§2.6),
     and it would add a paid call to every page view.
- **Touches:** the 3 files above → do together, in one change.
- **Owner note (2026-10-19, background.js review): the list is too short — decide the full list.** 14 words cover
  login and payment only. Candidate additions to discuss (not decided):
  - Account recovery / one-time codes: `token`, `reset`, `verify`, `otp`, `2fa`, `mfa`, `sso`, `session`.
  - Money: `bank`, `banking`, `netbanking`, `wallet`, `upi`, `invoice`, `order`, `orders`, `cart`, `tax`.
  - Personal records: `admin`, `dashboard`, `inbox`, `messages`, `health`, `patient`, `medical`, `insurance`.
  - Watch-outs: whole-token matching is needed first (see the probe above); otherwise `order` drops
    `/ordering-pizza-guide` and `admin` drops `/administration-history`.
- **Form check is too eager too — likely, read from code (2026-10-19, chrome_tracker.js review):**
  `hasPrivateFormSurface` drops a page's text when it has ANY `input[type=email]` / `tel` / `number`. Most blogs
  and news sites have a newsletter box ("Your email"), so their text is never captured — the same class of bug
  as the Wikipedia checkbox fix. Option for 2(a): count only VISIBLE fields inside the main content, or require
  a password field / 2+ personal fields in one `<form>`.
- **A7.1 decided 2026-10-21: whole-token matching.** Split host + path on `/ - _ .`; match words against the pieces
  only. The 3 copies (`chrome_connector.py`, `background.js`, `chrome_tracker.js`) change together. Known gap:
  glued words (`/userlogin`) — covered by the form check (A7.4).
- **A7.2 decided 2026-10-21: the 38-word list, written by hand once** (no learning).
  - Keep 14: account, auth, billing, callback, checkout, login, logout, oauth, password, payment, profile, settings,
    signin, signup. Plurals (2): accounts, payments.
  - Recovery / codes (9): reset, verify, verification, otp, 2fa, mfa, sso, recover, recovery.
  - Money (9): bank, banking, netbanking, wallet, upi, invoice, invoices, orders, cart.
  - Personal areas (4): admin, dashboard, inbox, messages.
  - Left out on purpose (common in article URLs): health, medical, insurance, tax, order, token, session — query
    secrets go to A7.3; health / bank / tax portals go in `excluded_domains` (`ste/settings_store.py:59`).
  - Build: also check each path segment with separators removed (`sign-in` → `signin`); bank hostnames
    (`hdfcbank.com`) need the domain list.
- **A7.3 decided 2026-10-21: strip secret query keys on the server** from both `url` and `canonical_url` before
  storing (one function in `chrome_connector.py`). Keys: token, access_token, id_token, code, state, key, sig,
  signature, session, sid, pwd, otp, rlkey. Accepted: two URLs differing only by one of these keys merge into one
  item. Also keep the existing query-KEY check in `is_sensitive_page` (`?password=`) in the whole-token rewrite.
  Build order: run `is_sensitive_page` on the URL BEFORE stripping the keys (`chrome_connector.py:157` today),
  or the query-key check never sees them.
- **A7.3 built 2026-10-22:** `SECRET_QUERY_PARAMS` (the 13 keys above) + `strip_secret_query_params` in `chrome_connector.py`; `is_sensitive_page` runs on `payload.url` AND `payload.canonical_url` first, then both are stripped. Checked on 10 inputs (token / TOKEN / only-secret keys / no change / fragment kept / empty / None / `keyword` kept while `key` goes / `password=` still caught by the sensitive check). Gap found and fixed 2026-10-22: `/chrome/revisit-check` got the extension's raw `canonical_url`, so for a URL with a secret key its Redis key (a sha256 hash, so no raw URL was ever stored) differed from the stripped one `record_visit` writes and the revisit was missed. `revisit_check` now strips first (checked: same key). A7.4 was built 2026-10-24 (below).
- **A7.1 + A7.2 built 2026-10-23:** `chrome_connector.py` `is_sensitive_page` rewritten to whole-token matching: splits
  host+path on `[/_\-.]`, also collapses per-segment separators (`sign-in` → `signin`), and checks query parameter keys.
  Same logic applied in `background.js` `isSensitiveChromeUrl` and `chrome_tracker.js` `detectSensitivePage`.
  `SENSITIVE_PATH_TERMS` expanded to the full 38-word list (A7.2) in all three copies.
  Probe URLs all pass (Python + JS tested): `/author/jane` kept, `/authentication-explained` kept,
  `/reset?token=abc` dropped, `/verify-email?code=999` dropped ("verify" token), `netbanking/home` dropped.
- **A7.4 decided 2026-10-21: option a, with the count raised (his ask) to 3.** In `chrome_tracker.js`
  `hasPrivateFormSurface`: (1) a VISIBLE password box → private; (2) 3+ visible email/tel boxes in the same
  `<form>` → private; (3) `type=number` ignored; (4) the 5-field rule counts visible fields only. Hidden sign-in
  pop-ups no longer count. Checkout forms (usually 2 email/tel) are caught by rule 4 (name, address, pincode…)
  and the URL words (A7.2), not by rule 2.
- **A7.4 built 2026-10-24:** `chrome_tracker.js` `hasPrivateFormSurface` + new `isVisible` helper and
  `PRIVATE_EMAIL_TEL_PER_FORM = 3`. Visible = `getClientRects().length > 0` and not `visibility:hidden`. Rules as decided:
  visible password → private; 3+ visible email/tel in the same `<form>` (grouped by `input.form`, so form-less inputs are
  not grouped) → private; `type=number` no longer triggers the early exit (it still counts as a data-entry field in the
  5-field rule, as before); the 5-field rule counts visible fields only. Tested in headless Chromium with the real script
  and a stubbed `chrome` API, 13 pages (newsletter box, number/tel, hidden and `visibility:hidden` password, 2 vs 3
  email/tel in one form, 3 across 3 forms, 3 with 2 in a hidden form, 5 vs 4 visible fields, 5 with 2 hidden): all pass;
  the old code fails 8 of them.
- **Decision:** A7 fully decided 2026-10-21 (A7.1–A7.4); all four parts built.

### A8. App pages (Slack, Notion, Jira) are stored as memory items — [x] decided 2026-10-21 (found 2026-10-17) — DISCUSS AT THE END
- **Doc §6.5:** session duration only, "not in canonical memory_items, not semantically indexed".
- **Code:** `chrome_connector.py` stores them in `memory_items` with `raw_text = NULL`, skipping the intent gate.
  The title is stored and may carry private text (a Notion page name, a Jira ticket title).
  Not listed as an approved deviation in CLAUDE.md §3.
- **Options:** keep (and record it as a deviation), or store only dwell for wellbeing and skip the title.
- **Also:** the extension sends an app page after 5 s or 1 interaction; the server skips the gate for app pages
  entirely (accepts 0 s). Harmless today, but the two rules differ.
- **A8.1 decided 2026-10-21: option a, keep app pages as per-page items with their title; recorded as a deviation
  (CLAUDE.md §3).** Claude flagged: ENP embeds the title alone (`enrichment_pipeline.py:313`), so a Slack DM / Notion
  page title is searchable and can reach the LLM as a snippet. Cheap safer add-on if wanted later: in
  `rse/llm_synthesizer.py` send app-page items as "<app> page" instead of their title.
- **A8.2 decided 2026-10-21: the server enforces it too** — in `chrome_connector.py`, discard an app page with
  `dwell_seconds < 5` and `interaction_count == 0`. Add this second copy of the 5 s value to A39's timing list.
- **Built 2026-10-22:** `chrome_connector.py` discards an app page with `dwell_seconds < 5` and `interaction_count == 0` (reason `app_page_not_engaged`). The 5 s test now calls `intent_filter.phase1_passes` instead of a literal 5 (2026-10-22). Still add the extension's copy to A39's list.
- **Decision:** A8 fully decided 2026-10-21 (A8.1 keep items + titles as a deviation; A8.2 server minimum).

### A9. Which domains count as "application pages" — [x] decided 2026-10-21 (found 2026-10-17, `intent_filter.py`) — DISCUSS AT THE END
- **Today:** 11 domains, whole domain + all subdomains, in 3 copies (`intent_filter.py`, `background.js`,
  `chrome_tracker.js`).
- **Too broad:** the public help/blog/docs pages of these companies count as app pages, so their text is dropped
  (`slack.com/help/...`, `figma.com/blog/...`, `asana.com/guide/...`).
- **Missing private apps** (their text IS captured today, unless the content script's form check happens to
  catch them): `docs.google.com`, `drive.google.com`, `calendar.google.com`, `outlook.office.com` /
  `outlook.live.com`, `teams.microsoft.com`, `web.whatsapp.com`, `discord.com`, `chatgpt.com`, `claude.ai`.
- **Gmail web is captured twice — likely (read from the code, not run live):** `mail.google.com` is an app
  domain, but `background.js` does not skip it, so Gmail web is also stored as a *Chrome* row, titled with the
  tab title (the open email's subject, or "Inbox (3) - <address> - Gmail"). Correction 2026-10-19: Gmail's URLs
  differ only after `#` (`/mail/u/0/#inbox/<id>`), and canonicalisation drops the `#` part, so it is likely ONE
  row per account, re-sent with whatever email was open. Gmail engagement already comes from
  `gmail_tracker.js`. Option: skip `mail.google.com` in Chrome capture entirely.
- **Self-capture (found 2026-10-19, background.js review):** Echo's own pages are captured as memories:
  the dashboard (`localhost:3000`) and the API docs (`localhost:8000/docs`) pass the gate like any page.
  Option: skip `localhost` / `127.0.0.1` in `shouldSkipChromeUrl` (and the server check).
- **Owner note (2026-10-19, background.js review): the list is too short — decide the full list.** 11 domains
  (2 redundant) cover work chat and trackers only. Candidate additions to discuss (not decided), on top of the
  "Missing private apps" above:
  - Mail and calendars: `mail.yahoo.com`, `outlook.office365.com`, `proton.me`.
  - Chat and calls: `web.telegram.org`, `meet.google.com`, `zoom.us`, `messenger.com`.
  - Social inboxes (path, not domain): `linkedin.com/messaging`, `instagram.com/direct`, `x.com/messages`.
  - Docs and notes: `docs.google.com`, `onedrive.live.com`, `dropbox.com`, `evernote.com`.
  - Path-level entries need option (3) above (match app paths, not whole domains).
- **Redundant entries:** `app.slack.com`, `confluence.atlassian.net` (covered by `slack.com`, `atlassian.net`).
  `jira.com` only redirects; Jira Cloud lives on `atlassian.net`.
- **Options:** (1) keep; (2) extend the list + skip Gmail web; (3) match app paths, not whole domains
  (e.g. `app.asana.com`, `www.figma.com/file/...`); (4) move the list into the Redis settings so the user can
  edit it, and have the extension fetch it (one copy instead of three).
- **A9.1 decided 2026-10-21: skip Gmail web** — `mail.google.com` skipped in Chrome capture (`background.js`
  `shouldSkipChromeUrl` + the same server check); remove it from the app-domain lists. Gmail comes from its API.
  - **Built 2026-10-22:** removed from the 3 app-domain lists (`background.js`, `chrome_tracker.js`, `intent_filter.py`); `shouldSkipChromeUrl` (host or `.mail.google.com`; a URL that fails to parse is skipped) and the server route (`gmail_web_excluded`) skip it. Read from the diff, JS not run in a browser.
- **A9.2 decided 2026-10-21: c, keep as is** (his words: "if it causes issues we will see that time"). Claude flagged
  the demo risk (the dashboard page with Luna's answer gets stored and can outrank the real source). Zero-code
  fallback if it shows up: add `localhost` in Settings → excluded domains (`settings_store.is_domain_excluded`,
  checked at `chrome_connector.py:151`; likely works — the server compares the hostname without the port; not tested).
- **A9.3 decided 2026-10-21: whole domain minus public sections.** App domains still match whole-domain + subdomains,
  but pages under `/help`, `/blog`, `/guide`, `/docs`, `/learn`, `/resources` are normal pages (text captured).
  Fails closed: an unknown path stays private.
  - **Extended 2026-10-21 (his ask): 38 public paths**, matched as the FIRST path piece, exact:
    help, support, guide, guides, docs, learn, resources, articles, tutorials, academy, templates (11);
    features, pricing, plans, product, solutions, enterprise, integrations, apps, customers, download, about,
    security (12); blog, news, press, changelog, events, webinars, community, forum, developers (9);
    careers, jobs, legal, privacy, terms, api (6).
  - Known gap (fails closed): language prefixes (`slack.com/intl/en-in/help`) stay text-less.
  - Public SUBDOMAINS (help., support., blog., docs., developers., developer., community., academy., learn.,
    status.; exception atlassian.net): **yes, added (his pick, 2026-10-21)** — match the first hostname label.
- **A9.4 decided 2026-10-21: lenient — no additions (his words: keep the same or decrease, no need to increase).**
  The list becomes 7: slack.com, atlassian.net, notion.so, figma.com, linear.app, trello.com, asana.com
  (removed: mail.google.com per A9.1; app.slack.com, confluence.atlassian.net redundant; jira.com redirect only).
  - **Built 2026-10-23 (working tree):** all three Chrome app-domain lists now contain exactly these seven domains. The ENP classifier's separate work-context list was left unchanged.
  Claude flagged: ChatGPT / claude.ai / Google Docs / Outlook / Teams / WhatsApp Web text stays captured as normal
  pages. Zero-code fallback per site: Settings → excluded domains (blocks the site fully, time included).
- **A9.5 decided 2026-10-21: b, one copy on the server; the extension fetches it.** New `GET /chrome/rules`
  (app domains, public paths + subdomains, sensitive words, secret query keys); `background.js` fetches at startup
  into `chrome.storage`; `chrome_tracker.js` reads it from there. Build detail: until the rules have loaded, the
  content script extracts no text (fail closed); no built-in fallback list (backend down = nothing stored anyway).
- **A9.3 + A9.5 built 2026-10-24 (cloud session):**
  - Server (`ingestion/chrome/intent_filter.py`): `PUBLIC_PATHS` (the 38), `PUBLIC_SUBDOMAINS` (the 10), `PUBLIC_SUBDOMAIN_EXCEPTIONS`
    (`atlassian.net`) and `is_application_page(domain, path="")`; `chrome_connector.py` passes the URL path and serves
    `GET /chrome/rules` = `app_domains`, `public_paths`, `public_subdomains`, `public_subdomain_exceptions`, `sensitive_terms`,
    `secret_query_params` (static, sorted, no DB / Redis).
  - Extension: the literal app-domain and sensitive-word lists are DELETED from `background.js` and `chrome_tracker.js`
    (the A9.4 "three lists" note above is superseded). `background.js` fetches the rules through `postToBackend(..., "GET")`
    (so the pause flag still gates it) on browser start / install, and also when storage holds none (at most every 30 s), keeps
    them in `chrome.storage.local.echo_chrome_rules`; `chrome_tracker.js` reads them from there and watches `storage.onChanged`.
    Fail closed: with no rules `shouldSkipChromeUrl` skips every page and the content script extracts no text.
    `secret_query_params` is served but the extension does not use it (the server strips them).
  - Still duplicated by hand (not in the A9.5 list): `CHROME_APPLICATION_PATH_SNIPPETS` (GitHub skip) and
    `CHROME_IGNORED_QUERY_PARAMS` in `background.js` vs `chrome_connector.py`.
  - Checked: `is_application_page` on 21 cases (private app page, `/help` `/blog` `/pricing` public, `help.` `blog.` `status.`
    public subdomain, `docs.atlassian.net` stays private, `notslack.com`, empty); `GET /chrome/rules` and `POST /chrome/ingest`
    through FastAPI's test client with DB / Redis stubbed. End to end in headless Chromium with the REAL unpacked extension
    against the real router (stubbed storage): rules arrive in `chrome.storage.local`; a normal article is saved with text and
    `## ` headings (A13); `slack.com/help/articles/1` is saved as a normal page with text; `app.slack.com/...` and
    `acme.atlassian.net/wiki/...` are saved engagement-only; `/login` is never sent. With `/chrome/rules` answering 503 nothing
    is saved (fail closed). Not run: the live Postgres / Redis, a long-lived worker restart, Settings > pause while fetching.
  - Behaviour to know: a SPA like Notion/Slack keeps one URL per canonical page, so the app-page flag is decided per page;
    a Chrome restart with the backend down means no capture until the backend is up (the next signal or tab event retries).
- **Decision:** A9 fully decided 2026-10-21 (A9.1–A9.5).

### A10. A brief (< 5 s) visit never counts as a revisit — doc conflict — [x] decided 2026-10-21 (found 2026-10-17) — DISCUSS AT THE END
- **Doc §6.2 contradicts itself.** The Phase 1 rule says "if NOT met → nothing written to Redis". The example
  table says "open page 4 s → Redis notes brief visit → SAVED on revisit".
- **Code follows the rule:** the revisit flag is written only after 5 s (extension `checkChromeRevisitSignal`,
  server `record_visit`). So the table's 4-s example can never happen.
- **Options:** keep the rule (stricter, less Redis writes), or write the flag on any visit (matches the table).
  Touches `background.js` and `chrome_connector.py`, not `intent_filter.py`.
- **Decision (2026-10-21): keep the rule as the code does today** — the revisit flag is written only after 5 s.
  The doc's §6.2 example table (4 s → saved on revisit) is wrong; fixing the doc is his (doc is his to change).

### A11. YouTube transcripts are never fetched — pick a new source — [x] decided 2026-10-21 (found 2026-10-17) — DISCUSS AT THE END
- **Checked live 2026-10-17 (read-only GETs, no DB):** `youtube.com/api/timedtext?v=…&lang=en` returns an empty
  `200` for every video. Even the signed caption URL taken from the watch page returns empty: YouTube now wants a
  proof-of-origin (PO) token. So `transcript_text` is always empty; videos embed on title + description + tags.
- Doc §7 calls the transcript "best-effort", so nothing crashes — but the "richest embedding source" is gone.
- **Options:**
  1. `youtube-transcript-api` library (new dependency; uses YouTube's internal player API; breaks when YouTube
     changes; can get IP-blocked).
  2. Extension reads the caption track inside the page (the browser already has the PO token) and sends the text
     with `/ytc/video-detected`. Public content only, so privacy rule §2.6 holds. Most reliable; extension work.
  3. Data API `captions.download` — Likely (API docs, not checked): only works for videos you own. Not usable.
  4. Accept: no transcripts. Drop the dead fetch code.
- **Also:** today only `lang=en` is tried, so non-English videos would get nothing even if fetching worked.
- **Decision (2026-10-21): option 1 (`youtube-transcript-api`), tested first, fallback = option 4.** Thursday
  morning: try the library on 3 videos from his laptop. Works → wire it into the server (English first, then
  auto-generated captions). Fails → no transcripts: delete the dead `timedtext` fetch code and say "title +
  description + tags" in the paper. Final by Thursday noon. Note: under A24 the transcript reaches the vector only
  when the description is short; its full text always goes to keyword search + answer passages.

### A12. Email signature removal destroys real content — remove it or replace it — [x] decided 2026-10-21 (found 2026-10-17) — DISCUSS AT THE END
- Doc §5.2 / §9.1 require it. Code: `enp/text_cleaner.py` `remove_email_signature` (left unchanged in review).
- **How it works:** in the last 6 lines (never line 0), cut at the first line that (a) starts with a fixed word
  (thanks, regards, cheers, sent from my…), (b) is an email address alone, or (c) is any ≤ 4-word line of letters
  ("looks like a name"). (c) and the "starts with" rule match ordinary sentences.
- **Simulation (2026-10-17, 8 realistic mails):** correct on 2 ("Thanks,\nPriya" and "Sent from my iPhone");
  destroyed content on 6. Words kept: "Hi team,\nDeploy is done\nSee you tomorrow" → 2/8 · "Hi,\nThanks for
  sending the contract, signed and returned." → 1/9 · "Hi Ana,\n…Regards to your family, we must meet again in
  June." → 2/16 · "Status update\nServer is down again\n…" → 2/9 · "Hello,\nReport attached.\n--\nPriya Rao…" → 1/12
  ("Report attached" read as a name) · a 5-line mail ending "And the phone." → 16/22.
- **Options:**
  1. **Remove the step** (your first thought). A leftover signature adds a name and a phone number to the
     embedding: small noise. Losing the body loses the memory. Doc deviation → add to CLAUDE.md §3.
  2. **Keep only near-certain markers:** cut at the RFC 3676 delimiter line "-- " and at "Sent from my …"; drop
     the name rule and the "starts with thanks/regards" rule. Lines that are exactly "Thanks," / "Regards," (with
     nothing after) could stay as markers.
  3. A signature-detection library: new dependency, heavier, still imperfect.
- **Decision (2026-10-21): option 1 — remove the step.** Signatures stay in the embedded text (small noise, uses
  part of the ~500-char body budget, A4); no body is ever cut. Doc deviation (§5.2 / §9.1) → added to CLAUDE.md §3.
  - Build: in `enp/text_cleaner.py` delete `remove_email_signature`, `_looks_like_signature_line`,
    `SIGNATURE_MARKERS`, `EMAIL_ONLY_RE`, `NAME_ONLY_RE` (no other users — grep 2026-10-21); `clean_gmail_text`
    goes quotes → normalize; update the pipeline comment at line 21.
  - B5 re-embed (Thursday) picks up the change for old mails.
  - **Built 2026-10-22:** `remove_email_signature`, `_looks_like_signature_line`, `SIGNATURE_MARKERS`, `EMAIL_ONLY_RE`,
    `NAME_ONLY_RE` deleted; `clean_gmail_text` is now quotes -> normalize; pipeline docstring updated. Kept the
    normalize-before-quotes step for non-HTML mail (a zero-width character in front of ">" or "On ... wrote:" hid the
    marker without it). Checked on 7 inputs: same output as before minus the signature cut.

### A13. Chrome headings never reach ENP; ENP's Readability runs on nothing — [x] decided 2026-10-21 (found 2026-10-17) — DISCUSS AT THE END
- Doc §6.4: "Readability … Headings are preserved", and embeddable text = title + **headings** + snippet + domain.
- Reality: `extension/content/chrome_tracker.js` `visibleTextFrom` sends the innerText of `<article>`/`<main>`/
  `<body>` with ALL whitespace collapsed to single spaces (max 12,000 chars). So raw_text is plain text: the
  extension already does a simple "main content" pick, ENP's Readability branch never runs, and `headings` is
  always [] for every Chrome row.
- **Options:**
  1. Extension also sends the page's h1–h3 texts (a short list) and keeps line breaks → headings feed the embedding
     as the doc says. Small extension + connector + storage change (no schema change needed).
  2. Extension sends the article HTML; ENP runs real Readability. Bigger payloads; more page content leaves the
     page than is needed.
  3. Accept: document that main-content extraction happens in the extension; delete ENP's Readability branch.
- **Decision (2026-10-21): option 1 — send the headings.** Headings feed the embedding as §6.4 says, so a later
  section ("BRIN for time-series data") is findable by meaning even when the exact word is forgotten.
  - Build (~1–2 h): `chrome_tracker.js` collects visible h1–h3 texts from the chosen content node (cap ~10
    headings / ~200 chars) and sends them as the first lines of `raw_text`, each prefixed `## `; ENP
    `clean_chrome_text` lifts `## ` lines into `headings` and keeps the body unchanged. No schema change.
    `build_embeddable_text` already joins title + headings + domain + snippet (snippet 800 per A24) — check the
    total still fits MiniLM's ~256 tokens.
  - Old pages: no headings (their text is already flattened); B5 re-embed cannot recover them.
  - **Built 2026-10-24 (cloud session):** `chrome_tracker.js` `headingLinesFrom` collects visible h1-h3 of the chosen content
    node (skips nav / aside / footer / form / hidden / duplicates; max 10, 200 chars each, whitespace collapsed) and puts them
    first as `## <text>` lines; the 12,000-char cap covers headings + body together; the body stays one line.
    `enp/text_cleaner.py` `lift_heading_lines` lifts the leading `## ` lines (consecutive, <= 10, <= 200 chars, so an old flat
    row that starts with `## ` is not mistaken) into `CleanedContent.headings`; `topic_extractor` already joins them.
    No schema change. Checked: 8 Python inputs (headings + body, old flat row, flat row starting `## `, 11 headings, CRLF,
    empty heading, empty) and the extension script in headless Chromium on 4 pages (menu / footer / hidden / duplicate
    headings skipped, 300-char heading cut to 200, 14 sections -> 10 headings, 5,000-word page -> exactly 12,000 chars total).
    Not run: the full extension in Chrome, ENP against the live DB.
  - **Side effects to know:** (1) a Chrome page revisited after this change sends different `raw_text`, so A3 re-embeds it
    (intended; this is how old pages gain headings). (2) The stored `raw_text` now begins with `## ` lines, so keyword search
    and the 1000-char answer snippet (`raw_snippet`) see them. (3) Worst case 10 x 200 chars of headings sit before the
    snippet in the embedded text; the token-window fit is A16's job (`fit_to_window`), not built yet.
  - Fallback if the build runs short: option 3.

### A14. Quoted replies: Outlook headers are not cut, inline replies are lost — [x] decided 2026-10-21 (found 2026-10-17) — DISCUSS AT THE END
- Fixed in review: Gmail's 2-line "On … <addr>\nwrote:" header (was missed). Not changed, needs a decision:
  1. Outlook replies quote with "-----Original Message-----" or "From: … Sent: …", which are not cut, so the old
     thread is embedded again. Older Outlook uses the same marker for forwards, whose content exists nowhere else;
     cutting there could lose a forward. Option: cut only when the subject starts with "RE:".
  2. Everything after "On … wrote:" is dropped. A reply written inline (between "> " lines, below the header)
     loses its own lines. Option: drop only "> " lines and the header itself.
- **A14.1 decided 2026-10-21: option c — leave Outlook replies as they are** (quoted old thread stays). Known
  cost: repeated results on Outlook threads. Fallback if it shows in the demo data: cut at "-----Original
  Message-----" / "From: … Sent: …" only when the subject starts with "RE:" (~30 min).
- **A14.2 decided 2026-10-21: option a — keep inline answers.** In `remove_quoted_replies`
  (`enp/text_cleaner.py`): below the "On … wrote:" header, drop the header and "> " lines and keep unquoted lines,
  ONLY when that part has at least one "> " line; with none (old thread unmarked, e.g. HTML-converted) cut as
  today. ~30 min. Cases: inline answers kept; normal "> " reply cut; unmarked old thread cut.
  - **Built 2026-10-22:** `remove_quoted_replies` rewritten (header + `> ` lines dropped, unquoted lines kept when a `> ` line follows; none follows -> cut from the header). Checked on 9 inputs (inline answers, normal reply, unmarked thread, two-line header, forward, empty, no header, header as last line, two headers). Gap found and fixed 2026-10-22: the `> ` test looked at ALL lines after the header; it now looks only at that header's block (up to the next reply header). "Hi / header / old1 / header / > older" returned "Hi / old1", now returns "Hi" (10 inputs checked).

### A15. YouTube descriptions are embedded uncleaned — [x] decided 2026-10-21 (found 2026-10-17) — DISCUSS AT THE END
- `clean_youtube_text` only joins and normalizes. Descriptions often carry links, sponsor blocks, "subscribe"
  lines, social handles, chapter timestamps and hashtags; all of it goes into the embedding. The doc asks for no
  cleaning here, so this is a quality choice, not a doc gap.
- **Options:** 1. strip URLs and lines that are mostly links/handles; keep chapter titles without timestamps.
  2. keep only the first N lines of the description. 3. accept as is.
- **Decision (2026-10-21): option 3 — accept as is.** Descriptions embedded uncleaned (first 600 chars). Known
  cost: link-heavy descriptions give weak fingerprints; matters more if A11 yields no transcripts. Fallback
  (~30 min): strip URLs/@handles/#hashtags, drop chapter timestamps, drop emptied lines (exact patterns only).

### A16. Confirm: embeddable text puts short hints before long text — [x] decided 2026-10-21 (found 2026-10-17)
- Already done in `enp/topic_extractor.py` (a fix, but it changes the doc's part ORDER, so it needs your OK for
  CLAUDE.md §3). Doc §5.2: subject + body + thread keywords + sender. Now: subject + keywords + sender + body
  (Chrome: domain before snippet; YouTube: channel + category before description).
- **Why:** MiniLM reads only the first 256 tokens. Measured with the real tokenizer: a 1,346-char email was 261
  tokens and its sender hint was cut; dense technical text (IDs, code) runs ~1.4 chars/token, so no char cap is
  safe. Order otherwise does not matter (mean pooling).
- **Options:** 1. keep (add to CLAUDE.md §3). 2. revert to doc order and rely on char caps only.
- **2026-10-21, his direction:** "we need to embed all that we provide the model" — nothing passed to the model
  may be silently cut. Proposed: keep today's order + build the text with the model's own tokenizer so the total
  fits `max_seq_length` (read from the model, not hard-coded); short parts whole, body/attachments trimmed at a word
  boundary; char caps of A4/A13/A24 become token budgets. He then asked why only 256 tokens (verified from the local
  model files: 512 position slots, `max_seq_length` 256, trained on 128) → **he will change the model; close A16
  together with A40 later today.**
- **Decision (his, 2026-10-21; with A40, he accepted every pick of the A16/A40/A42/A43 brief; its build notes and measurements are kept in A40/A42 below):**
  - **A16.1 keep the order: short hints first, long text last** (hints measured 16–50 tokens). Add to CLAUDE.md §3.
  - **A16.2 fill the model's 512-token window, cut with its own tokenizer at a word boundary; the stored
    `embeddable_text` is exactly what was embedded.** Shares: Gmail = hints whole, body up to 300 tokens, then
    attachments fill the rest; Chrome = title + headings + domain, then page text; YouTube = title + channel +
    category, description up to 200 tokens, then transcript; fallback/app pages = title, then text. Cutting lives
    in `embedding_generator.fit_to_window(parts)`; `topic_extractor` returns ordered parts. Replaces the char caps of
    A4 / A13 / A24.1 (same intent: top-only, one vector). A11's "only when the description is short" note changes.
  - **A16 build notes (from the brief; line numbers as of 2026-10-21):**
    - Budget = 510 content tokens (512 minus the 2 special tokens). If the hints alone pass 510 (very rare) they are
      cut at a word too. No attachment → body takes all; no transcript → description takes all. Chunking (several
      vectors per long item) stays paper future work (A24.1).
    - Unit is tokens, not chars: prose runs 3.3–5.4 chars/token, code-like text 1.6 (arctic-xs tokenizer, test set).
    - `topic_extractor.build_embeddable_text` returns an ordered list of (part, max_tokens or None) — that file stays
      pure functions with no model. `embedding_generator.fit_to_window(parts)` reads `get_model().max_seq_length` and
      the tokenizer, keeps hints whole, cuts long parts at a word boundary (tokenizer offsets), returns the final text.
    - Remove `GMAIL_BODY_CHARS`, `CHROME_SNIPPET_CHARS`, `YOUTUBE_DESCRIPTION_CHARS`, `YOUTUBE_TRANSCRIPT_CHARS`,
      `FALLBACK_BODY_CHARS` (`topic_extractor.py:57-61`); add `GMAIL_BODY_MAX_TOKENS = 300`,
      `YOUTUBE_DESCRIPTION_MAX_TOKENS = 200`.
    - Test to add: one long dense item (code / IDs) → stored text has ≤ 512 tokens and ends on a whole word.
    - Cost: worst case 0.22 s per item (every item fills the window). Trade-off: one long text → one blurred vector;
      measured, adding unrelated text lowered the right-question match by 0.03 on average, up to 0.24 (bge-small).
    - ~1 h. Must land before B5. CLAUDE.md §3 bullet: "embeddable text = hints first, long parts last (A16)".
    - A4 (800-char split), A13 (snippet 800) and A24.1 (Chrome 500 → 800) lines should point to A16.
  - **Built 2026-10-25 (commit c719cdc on branch `b5-arctic-embed-reembed`):** `enp/embedding_generator.py` `fit_to_window(parts)` (window read from the model: 512 - 2 special = 510;
    cuts on whitespace with the tokenizer's offsets; a 40,000-character pre-trim keeps a multi-MB attachment fast and gives the same output as full
    tokenization, checked on 25 random long inputs). `enp/topic_extractor.py` `build_embeddable_text` now returns `(list[TextPart], keywords)`;
    constants `GMAIL_BODY_MAX_TOKENS = 300`, `GMAIL_ATTACHMENTS_POOL_TOKENS = 210` (split evenly over the attachments, so each attachment's start gets in),
    `YOUTUBE_DESCRIPTION_MAX_TOKENS = 200`; the five char-cap constants are gone. `enp/enrichment_pipeline.py` calls `fit_to_window`; the stored
    `embeddable_text` is exactly what was embedded. **One generalization of the brief:** a cap holds only while later parts need the room; room left over
    flows back to the cut parts in order, so a short attachment does not leave the body cut at 300 (no attachment -> the body takes all, as decided).
    Checked: keywords identical to the committed code on 7 items (all sources); dense 6,000-char item -> 508 tokens, ends on a whole source word; hostile
    inputs (empty, emoji, CJK, one 10,000-char word, 100k chars, None fields) and 300 fuzz cases all <= 512 tokens incl. special; a tokenizer maps an emoji-only
    or 100+-char single word to one [UNK] token, so such text costs 1 token. Attachment text in the embedding is tested on synthetic input only: the 4
    attachments in the local data are 0-byte JPEG images (no OCR yet, D2).

### A17. Domain map covers too little — how to grow it — [x] decided 2026-10-21
- Stage 1 lists (32 work + 31 entertainment) + Stage 2 map (136 entries after removing 68 unreachable ones:
  63 duplicates of the Stage-1 lists, 4 "site.com/path" keys that can never match because only the host is
  looked at, and "ember.js", not a host). No domain maps to **personal** at all.
- **Simulation (2026-10-17):** 60 typical domains of an Indian CS student → only **19 covered**. Missed: chatgpt.com,
  claude.ai, linkedin.com, naukri.com, internshala.com, amazon.in, flipkart.com, swiggy.com, zomato.com,
  irctc.co.in, hotstar.com, jiocinema.com, kaggle.com, huggingface.co, docs.google.com, javatpoint.com,
  tutorialspoint.com, programiz.com, cricbuzz.com, thehindu.com, zerodha.com, paytm.com, bookmyshow.com, … (41).
  Every miss goes to Stage 3 (rarely fires, A18) → Stage 4 LLM (cost + snippet leaves the machine, A19).
- **Options (can combine):**
  1. **Learn from the user's own history (recommended):** a domain whose pages were classified the same way
     N+ times (by llm / user_override, e.g. ≥ 3 with ≥ 80% agreement) becomes a domain rule. One SQL query over
     memory_items + chrome_metadata at ENP start / monthly recompute; no schema change; matches the doc's
     "improves over time" idea. Gets better exactly on the sites YOU use.
  2. **Offline category dataset.** Likely (memory, not checked): UT1 blacklists (Université Toulouse Capitole,
     CC BY-SA, millions of domains in categories like social_networks, games, press, shopping, audio-video) or the
     Curlie (ex-DMOZ) dump. Needs a mapping from their categories to our 5; adds a data file + licence note.
  3. **Hand-extend** the map with the 41 misses above (+ personal: swiggy, zomato, irctc, banking, travel). Cheap,
     immediate, never complete.
  4. Online categorization APIs (e.g. Cloudflare Radar) — reject: sends every browsed domain off the machine.
  - Also: path rules (linkedin.com/learning → study) would need a URL-prefix match on the item's url.
- **Decision (his, 2026-10-21): grow it by hand, once.** Thursday, on the restored local data: list the top
  50-100 Chrome domains that reached Stage 4 (classified_by = 'llm'/'fallback'), add them to DOMAIN_CATEGORY_MAP
  (~30 min). Paper: "curated domain map"; a self-learning map (LLM agrees N times -> rule) may go in future work.
- **Built 2026-10-24 (cloud session, first half):** 29 domains added to `DOMAIN_CATEGORY_MAP` (136 -> 165 entries: 96 study,
  38 entertainment, 20 work, 11 personal; the first `personal` entries) from the simulation's misses above. Left out on
  purpose, like medium.com: chatgpt.com, claude.ai, docs.google.com, news sites (content too varied for a domain rule).
  **Closed 2026-10-25 by his decision, no further build:** the pass over the real top 50-100 Stage-4 domains is dropped. The
  test account has hardly any browsing, so its domains cannot predict real use and there is nothing worth adding. The 29
  domains added above stay as the final map for this demo.

### A18. Improve the initial seed centroids — Stage 3 almost never fires with them — [x] decided 2026-10-21 (found 2026-10-17; his ask 2026-10-21: improve the seeds)
- Seeds are one 7–9-word bag of words per category. Real items score cosine **0.10–0.55** against them, so the
  doc's `> 0.55` threshold is almost never met. Measured on 20 realistic items (4 per category, real MiniLM):
  | threshold (score / margin) | fires | correct |
  |---|---|---|
  | 0.55 / 0.08 (doc, today) | 1 / 20 | 1 |
  | 0.35 / 0.08 | 5 / 20 | 5 |
  | 0.25 / 0.08 | 8 / 20 | 8 |
  | 0.20 / 0.10 | 9 / 20 | 8 |
  The best centroid was right on 14/20 — the direction is good, only the threshold is off for seeds.
- Until recompute_centroids has ≥ 10 confirmed items per category, ~95% of non-structural items go to the LLM.
- **Options:** 1. richer seeds: 5–10 real example sentences per category, centroid = their mean.
  2. lower threshold while a category still uses its SEED (e.g. 0.25 / 0.08), doc value 0.55 once recomputed.
  3. both. 4. keep doc values (accept the LLM load until recompute kicks in). 20 items is a small sample: re-measure on
  your real rows before picking numbers.
- **Decision (his, 2026-10-21), part by part:**
  1. **Option 3 — both:** richer seeds (5–10 real example sentences per category, centroid = their mean) AND a
     separate, lower Stage-3 threshold while a category still uses its seed.
  2. **Threshold: measure, then fix.** After writing the new seeds, test on restored local rows with a known
     category (Stage-1 'structural' labels): pick the lowest score/lead threshold with ZERO wrong (start guess
     0.25 / 0.08). A category with recomputed centroids (A36) goes back to the doc's 0.55 / 0.08. Thursday, after
     the restore (~30 min, local DB read under the standing permission).
- **Build status 2026-10-25 — part 1 DONE, part 2 STILL TO DO (needs the local DB; run it in the local session):**
  - **Part 1 built (cloud session, commit dbaebee):** `SEED_TEXTS` in `enp/system_group_classifier.py` is now
    8 example sentences per category (was one 7–9-word bag), and `initialize_centroids` embeds each sentence and takes
    the mean as the seed centroid. Not run or measured: the cloud session has no PostgreSQL and no embedding model.
  - **Part 2 NOT built — the seed-specific Stage-3 threshold. To do in the local session:**
    1. Read local `echo_db` (standing permission, read-only, no backup needed): pick rows with `classified_by =
       'structural'` (a known correct category), per source (gmail / chrome / youtube).
    2. Get each row's stored arctic-embed-xs vector (`get_manager().get_vector()`), score it by `cosine` against the
       NEW seed centroids only (build them with `initialize_centroids`, ignore `centroids.npz`).
    3. Sweep score x lead (start guess 0.25 / 0.08; arctic-xs scores unrelated pairs ~0.35 higher than MiniLM, so expect
       the score part to land higher, the 0.08 lead is the portable part). Pick the **lowest pair with ZERO wrong**
       answers. Report fires / correct per source, and the best-centroid accuracy without a threshold.
    4. Add the result as its own constants (e.g. `SEED_MIN_SCORE` / `SEED_MIN_MARGIN`) in `enp/system_group_classifier.py`
       and change `stage3_centroid` so a category still on its seed uses them, while a category in
       `_persisted_categories` (recomputed, A36) keeps `CENTROID_MIN_SCORE` 0.55 / `CENTROID_MIN_MARGIN` 0.08.
       Today `stage3_centroid` still applies 0.55 / 0.08 to the seeds too, so the richer seeds alone may not make
       Stage 3 fire much more often until this is done. A `TODO(A18 part 2)` marks the constant in code.
    5. Write the measured numbers here, in design doc §9.3, and remove the TODO comment. This is the same Thursday
       re-tune as in A40 (and the "Thursday task" line in the A32 area).

### A19. Stage 4 LLM: one call per item, and the doc contradicts itself on privacy — [x] decided 2026-10-21
- Doc §9.4: "Items are batched (every few minutes) to minimise API calls — not one call per item." Code: one
  call per item, inside the ENP batch.
- Doc §1/§2 (lines 34, 57): the only data sent to the LLM is "query text and retrieved snippets" for synthesis.
  Stage 4 also sends source + 200-char title + 300-char snippet of every ambiguous item — emails included — at
  ingestion time, whether or not you ever search. With A17/A18 as they are, that is most non-structural items.
- `classified_by = 'fallback'` (no LLM / LLM failed) is not in the DB doc's value list (structural | domain |
  centroid | llm | user_override | pending). The column is VARCHAR(20) with no CHECK, so it works.
- **Options:** 1. batch: one LLM call per ENP batch with a numbered list, reply = numbered labels (doc's intent;
  ~10× fewer calls). 2. keep per-item. 3. privacy: exclude Gmail from Stage 4 (→ 'misc'), or send title only.
  4. add 'fallback' to the documented values (CLAUDE.md §3).
- **Decision (his, 2026-10-21), part by part:**
  1. **Keep one Stage-4 call per item** (deviation from doc §9.4, record in CLAUDE.md §3 + paper): A21's typed
     classifier gives per-item probabilities that a batched call cannot; cost < 1 cent/month; A18 + A36 cut the
     Stage-4 load further.
  2. **Privacy: keep title + 300-char snippet for every source, and say it plainly** (slide/paper wording: "only the
     query and short snippets go to the LLM, for classifying and for answering — never full mails, pages or
     transcripts"; matches A31.5). Housekeeping (option 4): add 'fallback' to the documented classified_by values.

- **Checked 2026-10-22:** CLAUDE.md §3 records the per-item Stage-4 call and snippet boundary. The design doc
  already lists `fallback` in §9.6, Table 2, and the glossary, and explains the fallback result in §9.4.

### A20. Centroids may learn the SOURCE instead of the category — [x] decided 2026-10-21
- Your concern: a centroid could end up meaning "Gmail-like" / "YouTube-like" instead of "work" / "study".
- **Measured offline (2026-10-17, real MiniLM, same 4 topics written in each source's embeddable-text format):**
  same topic + different source → mean cosine 0.90; same source + different topic → 0.15; neither → 0.12. So the
  source FORMAT adds only ~0.03. Even centroids built from one source each put all 8 cross-source items in the right
  category. Caveat: the test reused the same text per topic, so it is kind to us.
- **The real risk is the training data, not the format.** recompute_centroids learns from 'structural' items (+
  user_override, llm), and each structural rule belongs to one source: misc ≈ Gmail promotions, personal ≈ Gmail
  CATEGORY_PERSONAL, entertainment ≈ YouTube Shorts / music, study ≈ YouTube education + .edu mail. So "misc" learns
  what promotional EMAILS look like, and a misc Chrome page (news, weather) may not be near it.
- **Options:**
  1. Balanced recompute: centroid = mean of the per-source means, so each source counts equally.
  2. Per-source centroids (15 = 5 categories × 3 sources); an item is compared only with its own source's. Needs
     ≥ 10 confirmed items per cell — many cells would stay on seeds.
  3. Verify on real data first (needs your OK for a read-only DB query): confirmed items per category × source,
     and Stage-3 accuracy per source on held-out structural items.
- **Decision (his, 2026-10-21): option 1, balanced recompute** — in `recompute_centroids()`, average each source's
  confirmed vectors first, then average those per-source means (each source gets an equal vote). Check on
  Thursday's local data: confirmed items per category x source, and Stage-3 accuracy per source. Note: Stage-4
  LLM labels come from all sources and also soften the imbalance; the count shows how much.
- **Implemented 2026-10-22:** recomputation now groups confirmed vectors by category and `source_type`, computes
  each source mean, then averages those means equally. The existing 10-item minimum and returned counts still use
  the total number of indexed confirmed items in that category.

### A21. Replace the Stage-4 text LLM call with a typed classifier — [x] decided 2026-10-21 (revised from 2026-10-18)
- Today: a chat model is asked for "exactly one word" and the reply is parsed; confidence is a fixed 0.90.
- A typed classifier returns one of the given options plus real probabilities — no parsing, real
  classification_confidence.
- **Options:**
  1. **OpenAI Decisions API (recommended).** `client.decisions.create(model="gpt-6-luna", input=..., questions=[{type:
     "choice", choices:[{value, description} × 5]}])` → `choice`, `probabilities` per option, `confidence`. Public beta
     since 2026-10-06; $0.10 / 1M input tokens, no output cost; ZDR available, abuse logs up to 30 days by default.
     Same vendor + key you already use. Needs an `openai` SDK upgrade (installed 2.32.0 has no `decisions`); check
     `langchain-openai` still works with the new SDK, else call the REST endpoint with httpx.
  2. **TypeSafe Jev.** Same idea (choice / score / yes-no with calibrated probabilities), $0.04 / 1M input, 70–500 ms.
     Early access with a waitlist; a second outside vendor + key; no data-retention policy found (2026-10-17).
  3. Structured Outputs with an enum (LangChain `with_structured_output`): valid label guaranteed on OpenAI, works
     with other providers to varying degrees; no real probabilities.
  4. Keep the current text call.
- Options 1–2 are provider-specific → break CLAUDE.md's "provider only via LLM_CONFIG" rule unless the current
  LangChain call stays as the fallback. Cost is negligible at Echo's volume (~100 items/day ≈ under 1 cent/month).
- Privacy unchanged: still title + 300-char snippet leave the machine (see A19).
- **Decision (2026-10-18, follows A25's rule):** provider = openai → `langchain_openai.decisions.OpenAIDecisions`
  (LangChain's own wrapper, beta, needs `langchain-openai>=1.7.0` — see A25 upgrade); any other provider →
  option 3 (`with_structured_output` enum via `init_chat_model`). Real probabilities only on OpenAI; fixed
  confidence elsewhere. Jev dropped.
- **FINAL decision (his, 2026-10-21) — replaces the 2026-10-18 text above:**
  - provider = openai -> **Decisions API** (`OpenAIDecisions`), choices = the 5 category names. Its probabilities
    are NOT stored.
  - any other provider -> the current text call with a **strict single-word reply = the category name**
    (prompt says so; `_parse_category` accepts only the 5 names, anything else -> 'misc' / 'fallback').
  - Guard (beta API, not yet called for real): if the Decisions call raises, fall back to the single-word path
    for that item, so a beta failure never stops classification.
  - `classification_confidence` is no longer stored: drop the column on Thursday (schema open, A31.3) —
    `setup_db.py` + ALTER on the restored dump + remove the writes at `enp/enrichment_pipeline.py:366` and `:404`.
    Paper/deck: no confidence claims.
- **Implemented 2026-10-24** (`enp/system_group_classifier.py` `_stage4_decisions`, `rse/config.py` `decisions_model`),
  written from the docs and the published `langchain-openai` 1.7.2 / `openai` 3.37.0 source; **not yet called against
  the real API** (no key in the build session). Checked offline against a local fake `/v1/decisions` through the real
  SDK path. What it does:
  - One `choice` question named `category`, choices = the 5 names with a one-line description each; `input` is
    `Source / Title (200) / Snippet (300)`, the same data as the one-word path.
  - Model comes from `LLM_CONFIG["decisions_model"]` = `gpt-6-luna` (the only model the endpoint accepts; since 2026-10-24
    the two chat models use `gpt-6-luna` too). Empty string, or a provider other than openai, skips the Decisions path.
  - Falls back to the one-word call for that item on: an exception, a `refusal` answer, or a value outside the 5 names.
    After 3 failed calls in a row the Decisions API is skipped for 10 minutes (so a persistent 401/404 does not cost
    every item a failed call plus retries). The API's `confidence` is returned for the debug log only; nothing is stored.
  - First real run: check `Stage 4 Decisions API ready` in the ENP log, then that `classified_by = 'llm'` rows appear.
    Any `Stage 4 Decisions call failed (<ErrorName>)` line means the item went through the one-word path instead.

### A22. Attachment types that still give no text — which to add — [x] decided 2026-10-21
- Doc §5.4 requires: PDF, DOC, DOCX, TXT, images (Tesseract OCR). Audio/video out of scope.
- **Simulation (2026-10-17, real file bytes):** PDF, DOCX (now incl. tables + header/footer), TXT/CSV/MD/JSON/ICS
  (UTF-8, UTF-8 BOM, UTF-16, Latin-1) and HTML work. Everything below returns no text → the email shows only
  "(metadata only) filename|mime|size".
  | Type | Doc requires? | Cheapest way | Cost |
  |---|---|---|---|
  | Images (PNG/JPG…) | **yes** | install Tesseract binary + `pytesseract` (code already there) | system install |
  | Legacy `.doc` | **yes** | LibreOffice headless convert, or `antiword` | system binary |
  | Scanned PDF (no text layer) | no (common!) | render pages (PyMuPDF; Likely AGPL licence) → Tesseract | needs OCR first |
  | XLSX | no | `openpyxl` (already installed) | none |
  | PPTX | no | `python-pptx` | 1 pip package |
  | `.eml` (forwarded mail as attachment) | no | stdlib `email` | none |
  | RTF | no | `striprtf` | 1 pip package |
  | ODT | no | `odfpy` | 1 pip package |
  | ZIP | no | stdlib `zipfile` + size/zip-bomb limits | none |
  | HEIC (iPhone photos) | no | `pillow-heif` + OCR | 1 pip package |
- Related gaps: (1) Tier 1 (`gmail_api.extract_attachments`) also stores INLINE images (signature logos) as
  attachments → `has_attachments` is TRUE for mails with only a logo, and Tier 2 then returns logo metadata. Option:
  skip inline parts / tiny images at ingestion. (2) Two attachments with the same filename: Tier 2 always downloads
  the first (no stable Gmail part id is stored).
- **Recommended:** OCR (doc-required; also unlocks scanned PDFs) + XLSX + PPTX + EML (cheap); .doc only if you get
  such files.
- **Decision (his, 2026-10-21), part by part:**
  1. **Add XLSX (`openpyxl`, installed) + PPTX (`python-pptx`, pip) + `.eml` (stdlib) + OCR for images and
     scanned PDFs** (install the Tesseract Windows program + `pytesseract`; checked 2026-10-21: neither is
     installed). Skip .doc / RTF / ODT / ZIP / HEIC. Add the new packages to requirements.txt; README notes the
     Tesseract install. ~2-3 h.
  2. **Skip signature logos at ingestion:** in `gmail_api.extract_attachments`, drop parts Gmail marks inline
     (Content-Disposition inline / Content-ID) and images under ~10 KB. Real attachments unaffected.
  3. **Same filename: match on filename + size** in `enp/attachment_processor._download_attachment_data` (size is
     already stored in `gmail_attachments.file_size`). No schema change. Gmail's attachmentId stays unused (not
     stable between fetches).
- **Status 2026-10-24:**
  - Part 2 (skip signature logos) BUILT by him, light re-check done: `gmail_api.extract_attachments` drops parts with
    Content-Disposition inline, parts with a Content-ID unless the disposition is `attachment`, and images with a known
    size under 10 KB (`_MIN_ATTACHMENT_IMAGE_BYTES`); an unknown size (0) is kept. A PDF or DOCX under 10 KB stays.
  - Part 1 (types): XLSX, PPTX and EML built in the cloud session (D2, standard library instead of openpyxl/python-pptx).
    RTF/ODF were built by mistake and removed. OCR for images and scanned PDFs was built later, 2026-10-25 (see D2).
  - Part 3 (same filename: filename + size) BUILT 2026-10-24 (cloud session): `_download_attachment_data(service, email_id,
    filename, size)` keeps the same-named parts whose Gmail body size equals the stored `file_size` and takes the first of
    those; size 0 (unknown) or no size match falls back to the first same-named part. Both callers (`extract_full_text`,
    the Tier-2 loop) pass the size. `_walk_parts` now lists parts in message order (it listed siblings last-first, so
    "the first" was the last). Two parts with the same name AND size are still taken as one file. Checked on a mock Gmail
    message (three same-named PDFs: nested, attachmentId, inline data). NOT checked against the real Gmail API: that
    `body.size` equals the byte size stored at ingestion (it should; both come from the same field).

### A23. Attachment text is sent to the LLM — the doc says it never is — [x] decided 2026-10-21; [x] closed 2026-10-26 (simplified: leading text, 40,000 / 80,000 chars)
- Doc line 702: "The LLM API never receives: raw email bodies, full page content, video transcripts, **attachment
  text**, …". But doc §8.3 / §11 put attachment content into the synthesizer's context, and the code sends up to
  6,000 chars per attachment, 12,000 total, to the synthesis LLM (`MAX_CHARS_*` in enp/attachment_processor.py).
- **Options:** 1. keep (it is the whole point of fetch_attachment) and note the exception in CLAUDE.md §3.
  2. send only the passages that match the query (e.g. top-3 paragraphs by keyword/embedding), not the first
  6,000 chars. 3. lower the caps.
- **Decision (his, 2026-10-21): option 2 — send only the passages that match the question.** In
  `enp/attachment_processor.py`: extract the full text, split into paragraphs, score each against the query
  (keyword overlap and/or MiniLM cosine), send the top 3 (~1,500 chars total) instead of the first 6,000 per file.
  Keeps the A31.5 "short snippets" privacy line true and finds content deep in the file. ~1-2 h. Record the
  remaining doc deviation (attachment snippets do reach the synthesis LLM) in CLAUDE.md §3.
- **2026-10-21 follow-up:** option 2 is "contextual compression", which CLAUDE.md §2.3 prohibited. His decision:
  drop that prohibition completely (CLAUDE.md line removed 2026-10-21; MMR and recency boosting stay prohibited).
  At build time also fix the `rse/llm_synthesizer.py:29` docstring ("no semantic filtering or compression ... by
  design") and any paper/deck line that says compression is excluded.
- **Simplified and closed 2026-10-26 (his decision, after the Sun 25 Oct freeze: "cut the difficult part"):** the passage selection
  is NOT built. The query-matched top-3 passages are replaced by the leading text of each attachment, with larger fixed caps:
  `MAX_CHARS_PER_ATTACHMENT` 6,000 -> **40,000** and `MAX_CHARS_TOTAL` 12,000 -> **80,000** (`enp/attachment_processor.py`; the
  Redis cache stores the same longer text). Why these numbers: web research 2026-10-26 (AWS Bedrock model card, llm-stats, UF
  NaviGator docs) gives gpt-6-luna a 1,050,000-token window, 922K max input, 128K max output, and a double price rate above 272K
  input tokens ($0.10 per million input tokens). So nothing here comes near a limit; the caps are about answer quality and cost.
  40,000 chars is ~10k tokens (about 12-15 pages), 80,000 total ~20k tokens: enough for most real PDFs and DOCX, and one 300-page
  file cannot flood the prompt. No long-context accuracy test for Luna exists in public sources (checked), so the sizes follow the
  general finding that models answer best from a few thousand relevant tokens, not from everything. Privacy: attachment text
  already reached the LLM (exception recorded under A23 option 1); only the size grew. CLAUDE.md constraint 6, `AGENTS.md`, README
  and the design doc now say "bounded text excerpts". **Not tested against a real long attachment and the live LLM yet.**

### A24. How much text we take, and from WHERE (top only vs top + middle + end) — [x] decided 2026-10-21; [x] A24.2 closed 2026-10-26 (simplified: leading excerpts, 8,000 / 2,000 chars)
- **Today every cut takes the TOP only.** Checked in code 2026-10-17:
  | Where | Text | Cap (chars) |
  |---|---|---|
  | Extension → stored raw_text | Chrome page (article/main/body) | 12,000 |
  | ENP embedding (MiniLM sees ~256 tokens ≈ 1,000–1,300 chars total) | Gmail body / Chrome snippet / YT description + transcript | 800 / 500 / 600 + 400 |
  | ENP auto_keywords | full text | no cap |
  | ENP Stage-4 LLM | title + clean text | 200 + 300 |
  | RSE keyword search (FTS) | raw_text | **5,000** (`search_coordinator.py:97`) |
  | RSE cross-encoder | title + raw snippet | 512 |
  | RSE synthesizer, per result × 10 | raw_text (uncleaned: quoted replies included) | 500 |
  | Tier-2 attachments → synthesizer | extracted text | 6,000 each, 12,000 total |
- **Claude's view:**
  1. **Embedding: top only is right; current caps are sufficient.** One vector holds ~256 tokens; mixing top +
     middle + end into it blurs the topic, and the top carries the subject and intent. Small tweak: Chrome
     snippet 500 → 800 like Gmail (still fits the window). Real fix for long documents is chunking (several
     vectors per item) — a schema change, after the capstone.
  2. **What the LLM reads (snippets, attachments): neither top-only nor blind top/middle/end — pick by the
     query.** Split into ~800-char chunks, always keep chunk 1 (context), then add the chunks that best match the
     query (keyword overlap or MiniLM cosine — local, no extra LLM call, so the 2-call rule holds). Blind
     top/middle/end wastes ~2/3 of the budget unless the question is "summarize it".
     Numbers: attachments ≈ 1,000 lead + best chunks up to 4,000 per attachment, 8,000 total (less than today's
     12,000, so less leaves the machine); synthesizer snippets 500 per result but query-matched and from the
     CLEANED text, not raw_text.
  3. **Keyword search: raise the 5,000 cap to the full text** (Chrome stores up to 12,000, so a word in the
     second half of a long page is unsearchable today).
  4. **Is today's setup sufficient?** For finding items: mostly (fix 3). For answering about long PDFs/articles:
     no — if the answer is on page 7, top-only never sends it.
- **Options:** 1. keep top-only everywhere. 2. query-aware chunks for LLM inputs (recommended) + top-only
  embedding. 3. fixed top + middle + end sampling. Plus independently: FTS cap → full text; Chrome snippet → 800.
- **Decision (his, 2026-10-21), part by part:**
  1. **Embedding: keep top-only; raise the Chrome snippet 500 -> 800 chars** (same as Gmail; fits MiniLM's
     window). Chunking (several vectors per item) = paper future work.
  2. **(Simplified 2026-10-26, see the closing note below.) Result snippets to the LLM: the best-matching 500 chars of the CLEANED text** (quotes removed — signatures
     stay since A12 — with ENP's cleaner, then the 500-char window that best matches the query; same method as A23). Replaces the
     raw first 500 chars in `rse/llm_synthesizer.py:189`. ~1 h.
  3. **Keyword search over the FULL text, with a stored index:** add a stored `tsvector` column + GIN index (schema
     open, A31.3) built at write time; drop the 5,000-char cap in `rse/search_coordinator.py` `_FTS_DOCUMENT`
     (line ~107). setup_db.py + ALTER on the dump. ~1 h. He wants a low-level walk-through of GIN at build time.

- **A24.2 simplified and closed 2026-10-26 (his decision, same reason as A23):** no cleaned-text windows, no query matching. Each
  result gets the leading text of `raw_text`: **8,000 chars for the 3 best results, 2,000 for the other 7**
  (`rse/config.py`: `SYNTHESIS_EXCERPT_CHARS_TOP`, `SYNTHESIS_EXCERPT_CHARS_REST`, `SYNTHESIS_FULL_EXCERPT_RESULTS`; replaces
  `SNIPPET_CHAR_LIMIT = 500`). `RESULT_COLUMNS` in `rse/search_coordinator.py` now cuts `raw_snippet` at 8,000 in SQL (was 1,000); the
  cross-encoder still clips it to `RERANK_DOC_CHAR_LIMIT` (512), so ranking is unchanged. Worst case for ten results ~38,000 chars,
  ~10k tokens. Top results get more because the answer is usually there and the lower ones are mostly noise. A24.1 (embedding)
  and A24.3 (full-text keyword search) are unchanged and were already built. Checked offline: rank 1-3 blocks 8,063 chars, rank 4-10
  2,063 (stubbed database and LLM); NOT run against the live backend yet. Known small leftover: raw Gmail text still carries quoted
  replies (the cleaned-text part of the decision is dropped with the windows).

### A25. LLM = GPT-5.6 Luna on the Responses API, via LangChain — [x] DECIDED 2026-10-18 (build in Step 2)
- **Rule ("plug-in for all, specialised for OpenAI"):** every LLM call stays behind LangChain, so any provider key
  works by changing `LLM_CONFIG`. When `provider == "openai"`, OpenAI-only extras switch on (Responses API,
  reasoning effort, Decisions API). Other providers get the generic path; their own extras can be added later
  the same way.
- Model `gpt-5.6-luna` everywhere `LLM_CONFIG` is used (RSE both calls, ENP Stage 4 fallback, WBA insights).
  **Changed 2026-10-24 (owner): `gpt-6-luna` everywhere** (`parser_model`, `synthesizer_model`, `decisions_model` in `rse/config.py`; no `.env` change).
  $0.20 / $1.20 per 1M in/out (gpt-4.1-mini was $0.40 / $1.60). Cutoff Feb 2026, 1.05M context.
- **Reasoning effort: `medium` for both calls** (his call: better answers beat faster answers for the demo).
- Drop `parser_temperature` / `synthesizer_temperature` for Luna (installed `langchain-openai` already silently
  removes them for gpt-5* unless effort is `none`, `base.py:971-984`). Keep them for other providers.
- Endpoint: `ChatOpenAI(use_responses_api=True, reasoning={"effort": "medium"}, use_previous_response_id=True)`,
  so reasoning carries across tool rounds.
- **Dependency upgrade — [x] DONE 2026-10-18 (his go).** Minimal upgrade only: `langchain-openai` 1.1.16 → 1.7.2,
  `langchain-core` 1.3.0 → 1.6.12, `openai` 2.32.0 → 3.35.1, `langsmith` 0.7.33 → 0.14.11 (+ small deps).
  `langchain` 1.2.15, `langgraph` 1.1.9, anthropic/google packages and the pinned `google-auth` left as they are
  (a full-family upgrade would have bumped `google-auth` and `pydantic`). `pip check` clean. Offline checks: all
  project modules import, `compileall` silent, the RSE graph compiles (16 nodes), both current LLM clients build,
  a Luna `ChatOpenAI(use_responses_api=True, reasoning=medium)` builds a Responses payload (`input` + `reasoning`,
  temperature dropped), `OpenAIDecisions` imports (beta warning). Not yet tested: a real API call (needs his go).
  `requirements.txt` floors raised (`langchain-core>=1.6.10`, `langchain-openai>=1.7.2`). Rollback list:
  scratchpad `freeze_before_upgrade.txt`.
- Cost: assume ~1,000 reasoning tokens per call → ~$0.004 per 2-call query. Measure real tokens + latency.

### A26. Agentic retrieval: Luna calls a `search_memories` tool, up to 5 rounds — [x] DECIDED 2026-10-18; [x] BUILT 2026-10-26 (first live run still to do)
- Replaces the one-way parse → search → synthesize flow. LangGraph agent node ⇄ tool node; round counter in
  `EchoState`.
- Tool `search_memories(query, sources?[gmail|chrome|youtube], time_phrase?, sender_or_domain?)` runs the
  EXISTING deterministic pipeline (FTS + FAISS → RRF → cross-encoder → neighbourhood). `time_phrase` ("last
  Tuesday") is resolved by `time_anchor.py`, not by the model. Returns short numbered snippets only (privacy rule
  unchanged). Numbering is global across rounds, so [n] citations stay stable.
- **Cap: 5 search rounds** (his call). After round 5 the next call is forced to answer (no tools). LLM calls per
  query: usually 2, max 6. Cap enforced in code, never only in the prompt. Parallel tool calls allowed.
- **Hard constraint #5 changes** from "exactly 2" to "usually 2, max 6" → record as an approved deviation in
  CLAUDE.md §3 (architecture doc untouched).
- Fallback: LLM down → today's deterministic pipeline on the raw query.
- Risks: prompt injection from captured emails/pages (tool is read-only; system prompt says snippets are data,
  not instructions); non-deterministic searches → bring back an eval set (`git checkout 16bde99 --
  rsr/quality_evaluator.py rsr/query_quality_eval_set.py`, drop its diversity metric); latency per round.
- **Conversation chaining (agreed 2026-10-18, build with this item):** with one agent conversation per chat,
  use `previous_response_id` instead of resending history. Save the turn's last `response_id` inside the AI
  message JSON in message_store (no new table); next question on OpenAI sends only the new message +
  `previous_response_id`. Fall back to history from message_store when: provider is not OpenAI, the id has
  expired (~30 days), or the provider changed mid-chat. Not cheaper (the chain is billed as input, mostly at the
  cached rate) — the gain is carried-over reasoning/tool results and simpler code. Needs his OK on
  `store=true` (OpenAI keeps the conversation; doc §10.7 says history never goes to a cloud service).
- **store=true DECIDED (his, 2026-10-21): option a — `store=true` + `previous_response_id`.** Claude's pick was
  store=false; he chose a. Consequence to write everywhere the privacy line appears (design doc, paper, deck
  privacy slide, README): "what is sent is unchanged (question + short snippets); OpenAI keeps each chat for ~30
  days so a follow-up continues from it". The message_store history path stays as the fallback (non-OpenAI
  provider, expired id, provider changed mid-chat). Likely (OpenAI docs, may have changed): stored responses
  are kept ~30 days — verify at build.
- Open sub-question: a `read_more(n)` tool (longer capped excerpt / attachment text of one item) touches the
  privacy rule; ties to A23/A24. Decide at build time.
- **Built 2026-10-26 (owner approved the plan; decisions below were made in the 2026-10-26 discussion):**
  - **Graph:** `agent ⇄ search → finish` (`rse/agent_nodes.py`, `rse/retrieval_engine.py`). One search is `rse/search_tool.py` `run_search`, the
    same stages as before in the same order (anchor → keyword ∥ semantic → RRF → quality → widen_scope → cross-encoder → neighbourhood →
    optional attachment text), no LLM call inside. Common case 2 LLM calls (Luna asks for one search, then answers); at most 6.
  - **Cap in code:** `MAX_SEARCHES = 5`, `MAX_SEARCHES_PER_ROUND = 3` (my number: parallel calls are allowed up to 3 per step); calls 1-5 carry the
    tool, call 6 has none, a tool call on a call without the tool is ignored; every tool call still gets a reply (extra ones "not run").
  - **First question of a new chat forces a search** (`tool_choice="required"`, `FORCE_FIRST_SEARCH` in `rse/config.py`); follow-ups use auto.
  - **Tool arguments:** `query`, `alternative_phrasings` (≤ 2), `sources`, `since` (YYYY-MM-DD, copied from a calendar in the first message),
    `anchor_item` / `anchor_relation`, `read_attachment`. **Not built, by decision:** `time_phrase` and `sender_or_domain` (no resolver or sender
    filter exists; the sender goes into `query`, it is already searchable text), `read_more` (A23/A24 already give 8,000 chars per top result), the
    live Gmail poll (A45). `fetch_recent_items` stays in `rse/search_coordinator.py` (E1's `recent_activity` tool was dropped).
  - **`parse_intent` is gone, its checking is not:** `rse/query_parser.py` now holds `intent_from_tool_args` (repairs unknown sources, broken dates,
    the string "false", duplicate variants, half an anchor, empty query) and `fallback_intent`. Removed: `node_parse_intent`, `node_check_attachments`,
    `node_fetch_api`, the separate `synthesize` node.
  - **Numbering:** global across the searches of one question ([n] = `found_items[n-1]`); an item seen before comes back as "(already shown above)".
  - **Luna is told when Echo widened a search** (what was loosened), because `widen_scope` runs inside the tool without an LLM call.
  - **History the agent reads (his decision):** 12 messages, 1,000 chars per question, 3,000 per answer (`HISTORY_MESSAGES`, `HISTORY_CHARS_QUESTION`,
    `HISTORY_CHARS_ANSWER`); was 10 / 6 messages at 500 chars in two places.
  - **Failures:** agent call fails on the first call → one search on the raw question and a deterministic list; later → list of what was found;
    a search that raises → tool result "this search failed" (no error text). No fallback switch (his decision): git is the rollback.
  - **Conversation chaining (`previous_response_id`, `store=true`) DEFERRED** (his decision, 2026-10-26): item numbers restart each question, so a
    remembered turn would hold a different "[2]" than this turn's list and a pill could point at the wrong item; the fix is to store the found-items
    list per chat and continue numbering across turns. The message_store history path is what runs.
  - **Tests (`python -m pytest tests`, 22 pass, no database/index/key needed):** the real graph with a scripted Luna and a fake search. They show: 2 calls
    for a simple question; first question forces a search, a follow-up does not; never more than 6 calls / 5 searches (and a model that ignores the cap
    is stopped); per-step cap and a reply for every tool call; stable numbering across searches; unknown citation dropped; empty query reported back;
    nothing-found answer flagged; agent failure first / later; a raising search is not a crash and leaks no error text; one progress line per step;
    first message has date, calendar, history, question; history limits; excerpt 8,000 / 2,000 by rank; widening noted to Luna; tool schema valid;
    argument repair; stage order, automatic widening and give-up inside one search. Three deliberate breakages (cap removed, citations unchecked,
    numbering reset) each failed the matching test.
  - **Checked with the real `langchain-openai` 1.7.2 client against a local fake Responses endpoint:** request 1 = `/v1/responses`, reasoning medium, no
    temperature, `tool_choice=required`, the tool as a function; request 2 = the reasoning item + `function_call` + `function_call_output` with the same
    call id; tool call parsed back; 2 calls. **Not checked against the live OpenAI service.**
  - **Known risk:** round 2 refers to Luna's reasoning item by id, which needs the stored response. The code does not set `store` (API default stores
    for ~30 days, to verify); an account with zero data retention would need `store=false` and encrypted reasoning content.
  - **His first live run:** `python -m rse.retrieval_engine "find my TechCorp interview email"`; the log must show `llm_calls=2 searches=1` and the
    answer must cite. If the API rejects the forced first tool choice, set `FORCE_FIRST_SEARCH = False`.

### A27. Answer style = chat (chatbot reply), NOT a fixed structured format — [x] DECIDED 2026-10-18
- **His call:** no fixed JSON response type. Echo answers like a chatbot: a natural, conversational reply in
  markdown. (The structured-schema idea of earlier the same day was rejected.)
- **What a good reply looks like (guidance for the system prompt, not a schema):**
  - Lead with the direct answer in the first sentence, in a friendly voice ("Yes — Priya emailed you on Tue 14
    Oct…"). No "Based on the retrieved items…" preamble.
  - Short paragraphs; bold the key facts (dates, deadlines, amounts, names); a bullet list only when there are
    really several things; a small time-ordered list only when the answer is a sequence of events.
  - Sources are cited right after the facts they support, as memory pills (see "Citation format" below) —
    no visible numbers. Mention when and where naturally.
  - Honest about gaps in plain words ("I didn't find a reply from you").
  - If the query is ambiguous, ask back in one line instead of guessing.
  - End with at most one natural follow-up offer ("Want me to pull up the onboarding checklist too?").
  - Tone: warm and personal (it is the user's own memory), never judgemental; length matches the question.
- **Citation format — "memory pills" + "moment strip" (decided 2026-10-18, Claude's design by his delegation):**
  - *Market base:* ChatGPT search / Perplexity / Gemini all moved from bare [1] numbers to inline source chips
    (favicon + short name, hover preview, "+2" when several sources back one claim). Echo uses the same pattern.
  - *Echo's twist — the pill names the human anchor, not the website:* in a personal memory the useful cue is
    who / where / when. Pill = small source logo + anchor + short time:
    Gmail → sender (`✉ Priya · 14 Oct`), Chrome → site (`◎ arxiv.org · Tue`), YouTube → channel
    (`▶ 3Blue1Brown · last week`). Several sources for one claim → stacked logos `Priya +1`.
  - *Honest evidence styling:* solid pill = the item itself says it; dashed outline = related context (thread /
    same-session neighbour). Label text, not colour alone, carries the difference (accessible).
  - *Click a pill = open the original directly (his call 2026-10-18):* Gmail → the mail in Gmail, Chrome → the
    page, YouTube → the video, in a new tab. Hover (desktop) shows a peek card first: title, 2-line snippet,
    exact date-time, "What was I doing around then?" (the ±5-min neighbourhood RSE already computes). On touch,
    first tap = peek, second tap = open.
    Note: ingestion stores the Gmail link as `#inbox/{message_id}` (`ingestion/gmail/gmail_api.py:372`), which
    breaks once a mail is archived — build the link as `#all/{message_id}` instead (fix in ingestion or in RSE
    when building `sources`).
  - *Moment strip (innovation, fits Recall's "thread of time"):* a thin horizontal time ribbon under each reply
    with one dot per cited item placed by date; hovering a pill lights up its dot and vice versa, so the user
    sees WHEN the answer comes from at a glance. Hidden when all sources are from the same day.
  - Logos: recognisable Gmail / Chrome / YouTube marks rendered muted to match the Midnight tokens, full colour on
    hover; respects `prefers-reduced-motion`; pills have aria-labels ("Email from Priya, 14 October").
  - *How it works:* the model writes hidden markers (e.g. `⟦m3⟧`) after facts; the backend checks every marker is
    an item it actually returned (drops the rest) and sends a `sources` list; the frontend swaps markers for
    pills. The user never sees numbers; the model never invents a source.
- **UI otherwise:** chat bubbles with streamed text (A28); the follow-up offer works as a quick-reply button.
- **Chat sessions (added 2026-10-18, his logic):** every new chat gets a new session_id; opening an old chat
  resumes its session_id, so its history (message_store, keyed by session_id) continues.
  - Today: history is keyed by session_id ✅; `run_query` makes a UUID when none is sent ✅; the frontend keeps
    the id only in memory (`useRef` in RecallPage) so a page reload silently starts a new chat; no "New chat"
    button; no list of past chats; no endpoint to list sessions.
  - Build: backend `GET /sessions` → [{session_id, title = first question (cut short), last_active, turns}]
    from message_store (no new table; created_at gives the times); `GET /session/{id}` already returns the
    messages; `DELETE /session/{id}` already exists (delete chat). Frontend: "New chat" button, past-chats list
    (sidebar or drawer), click to resume (loads the messages, keeps sending that session_id), remember the
    open chat id across reloads (localStorage, per-viewer convenience only).
- Backend: `final_answer` stays a string (markdown with markers) + a `sources` list the backend fills (id,
  source_type, anchor, created_at, title, snippet, link, direct|related); drop markers not in the context. `QueryResponse`, `types.ts`, `RecallPage.tsx`, demo data get the `sources` list.

- **Progress 2026-10-18 (his request, done early during the rse/ review):** `rse/llm_synthesizer.py` now
  writes the chat-style answer with ⟦n⟧ markers, validates them, and returns `final_answer` (readable, markers
  rendered as text pills "(✉ Priya · 14 Oct)" so today's UI works), `answer_markdown` (validated markers) and
  `answer_sources` (n, memory_id, source_type, anchor, date_label, created_at_local, title, link, role). State
  fields added in `rse/state.py`. Prompt/pill times are local (created_at is UTC). Done too: `retrieval_engine.py`
  defaults + return of the two new fields, `no_results_found` node shape (graph_nodes.py). Left: backend
  `QueryResponse.answer_sources` / `answer_markdown`, and the frontend pill UI + moment strip (Step 2).
- **Backend half built 2026-10-25 (committed):** `QueryResponse` in `backend/retrieval.py` now
  returns `answer_markdown` and `answer_sources` (types in `frontend/src/lib/types.ts`: `AnswerSource`,
  `SessionSummary`). New `GET /retrieval/sessions?limit=50` -> `[{session_id, title, last_active, turns}]` from
  `message_store` (title = first question cut at 60 chars; limit 1-200, 422 outside). Checked on the real DB through
  FastAPI's TestClient with real LLM calls: a query returned both fields (markers in `answer_markdown`, none in
  `final_answer`; 3 sources, each with a link and role `direct`); a second turn on the same `session_id` gave
  `turns` = 2; the list is newest first; no key and a wrong key give 401. My two test chats were deleted from
  `message_store` afterwards. The Gmail link already uses `#all/<thread_id>` (`rse/llm_synthesizer.py:166`), so that
  note is done. **Settled 2026-10-25 (his decision): `/retrieval/sessions` no longer needs `X-API-Key`** (the frontend must not ship a key in
  the browser bundle). Only this route was opened: `GET /retrieval/session/{id}` (the messages) and `DELETE` stay locked.
  Checked through FastAPI's TestClient on the real DB: list without a key 200 (5 chats), `limit=0` 422, detail and delete
  without a key 401, detail with a wrong key 401. **Past-chats screen built 2026-10-25 (his go; commit 9472a09, merged in PR #13).** His wish was to open `GET /retrieval/session/{id}`; instead a new
  open route `GET /retrieval/sessions/{id}` returns clean `{role, text}` messages (404 when the chat has no messages, 400 on a bad id),
  and the diagnostic `/session/{id}` and `DELETE` stay locked. `frontend/src/components/PastChats.tsx`: a "Past chats" button next to
  "New chat" opens a panel (newest first, title, time, question count, "open now" mark; Escape or an outside click closes it; loaded on
  every open). Picking a chat loads its messages and keeps its session id, so the next question continues it. `RecallPage.tsx` also
  remembers the open chat id in `localStorage` (`echo.recall.session`) and goes back to it after a reload; a chat that no longer
  exists starts fresh. `last_active` is returned as UTC so the browser shows local time. Stored replies carry no pills (message_store
  keeps text only), so an old chat shows plain text, with bold and lists drawn. Checked: route test on the real DB with throwaway chats
  (messages, unknown id 404, bad id 400, diagnostic and delete still 401, list time and title correct); `tsc` clean; in Chrome: panel
  lists the chats (an HTML `<script>` title is shown as text), opening one shows its messages, a reload returns to it, a question asked
  in the opened chat made it grow from 2 to 3 turns (same session id), New chat clears the stored id and a reload then starts empty,
  a deleted chat starts fresh, no console errors. All throwaway chats deleted. **The moment strip was seen in a real browser in the same
  run (dots from 30 Mar to 21 May).** Not built: deleting a chat from the screen, and a stored chat's pills.
  **Bug found and fixed in the same run:** one real answer showed a stray `⟦8⚠` after the pills (a garbled citation marker from the model).
  `rse/llm_synthesizer.py` now removes broken markers (`_BROKEN_MARKER_RE`); checked on 6 strings (garbled, cut-off, `[2]` form,
  out-of-range, none, lone `⟦`).
  **Also 2026-10-25 (his decision): laptop only, so the touch "first tap = peek, second tap = open" is dropped.**
- **Pills, moment strip and New chat built 2026-10-25 (committed):** `frontend/src/components/AnswerBody.tsx`
  draws `answer_markdown` as chat text (paragraphs, bullet / numbered lists, **bold**; built as React elements, no HTML
  injection) with a pill for each ⟦n⟧: source icon (Mail / Globe / Play), the anchor (sender, site, channel) and the date;
  a click opens the original in a new tab; hover or keyboard focus shows a peek card (source, title, exact date and time);
  a dashed outline plus the word "related" marks related context (the backend sends only `direct` today). Under the text,
  the moment strip places each cited memory on a time ribbon, hovering a pill lights its dot and the reverse, and it is
  hidden when everything cited is from one day. `RecallPage.tsx` uses it when the reply has sources (else the plain text as
  before) and has a "New chat" button (drops the session id and clears the screen). Checked: `tsc` clean; in Chrome against
  the live backend, real answers showed bold text, bullets, pills with the right icons and a working peek card (the two
  test answers cited only 1 Sep items, so the strip correctly stayed hidden); the strip itself on synthetic sources
  rendered to HTML in Node: 3 sources over 3 days -> 3 dots at 1.0 %, 31.8 %, 99.0 % (31.8 % hand-checked from the
  times), same-day sources -> no strip, a `<script>` title is escaped, an unknown marker is dropped, a source with no link
  is a focusable span, null and invalid dates do not crash. NOT built: "What was I doing around then?" in the peek card
  (needs the ±5 min neighbourhood in `answer_sources`), the past-chats list
  and resume screen (built later the same day, see above), remembering the open chat across a reload, a visual check of the strip in a browser.

### A28. Live "what Echo is doing" progress in the UI — [x] DECIDED 2026-10-18 (his go: "really good one"); CLOSED as it is 2026-10-25 (progress lines only; word-by-word reply text is future work)
- With up to 5 search rounds a query can take several seconds. Stream the steps ("Searching Gmail for 'offer
  letter', last 2 weeks…", "Reading 8 results…", "Writing the answer…") over SSE instead of one silent POST,
  then stream the reply text itself into the chat bubble (A27). Backend: a streaming variant of `/query`
  (LangGraph `stream` events); frontend: a step indicator above the typing bubble. Keep the plain POST as
  fallback.
- **Built 2026-10-25 (committed): the progress lines.** `rse/retrieval_engine.py` `run_query_stream`
  (shares its setup and finish code with `run_query`; uses `graph.stream(stream_mode="updates")`), route
  `POST /retrieval/query/stream` in `backend/retrieval.py` (server-sent events: `session`, then one `step` per finished
  graph node with a fixed label and an optional count, then `done` with the same body as `/query`, or `error`); the plain
  `POST /retrieval/query` is unchanged and is the fallback (`askEchoStreaming` in `frontend/src/lib/api.ts` uses it when
  the stream route is missing). The Recall page lists the finished steps under "searching your memory". Labels are
  fixed texts and counts, never the question or a title. Checked on the real DB with real LLM calls: 19 checks (event
  order, labels, counts, no question words in any step, `done` equals the `/query` shape, two turns in one session saved
  once each, a graph failure gives a plain `done` and saves no turn, empty / 2001-char / bad-session-id inputs give
  422); the real frontend reader (`api.ts` bundled and run in Node) against the live backend (10 steps arriving over
  4 s, same session on the second turn); and in Chrome (steps appear live). NOT built: streaming the reply text token
  by token (needs `llm_synthesizer` to stream; the answer arrives whole in `done`), and the step lines are about the
  fixed graph, not the "Searching Gmail for 'offer letter'…" wording, which belongs to the A26 tool loop (not wired).

### A29. Effort score is unfair across sources — [x] decided 2026-10-21 (found 2026-10-18, rse/reranker.py review) — DISCUSS AT THE END
- `final_score = 0.8 × cross-encoder + 0.2 × effort`; effort = average of the signals an item has.
- Chrome items average up to 4 signals (dwell, scroll, revisits, interactions); YouTube 2 (watch time,
  completion); Gmail only dwell — and dwell exists only for mail read in Gmail on the web with the extension.
  Mail read on the phone, or never opened in the browser, always has effort 0 → up to −0.2 vs. a page of equal
  relevance. Dwell is also always included, so a page with no dwell data is dragged down by a 0.
- Options: 1. keep. 2. per-source effort: only average signals that source CAN have, and give items with no
  measurable signal a neutral value (e.g. the source's median) instead of 0. 3. drop effort for sources without
  signals and rank them on the cross-encoder alone (renormalise to 1.0 × ce).
- Claude's view: 2 — it keeps "attention the user gave" as a signal without punishing email for being unmeasurable.
- **Decision (2026-10-21): option 2 — neutral value instead of 0** (first picked 3, switched after the flagged
  risk: under 3 an unopened mail beat a studied page). A Gmail item with 0 s reading time gets the MEDIAN effort of
  the Gmail items in the same result list (computed per query in `rse/reranker.py`, no storage); mails with real
  reading time, Chrome and YouTube score exactly as today. "Read on phone" and "never opened" both land in the
  middle (cannot be told apart). ~30 min.
- **Built 2026-10-22:** `_query_effort_scores` in `rse/reranker.py`. The median is taken over Gmail items WITH real reading time (> 0 s) in the same list; with none, 0.5. Reason: a median that counts the zero-dwell mails would be 0 when most mails are unopened, which is the problem this decision fixes. Checked on 7 inputs (zero and None dwell, real dwell unchanged, Chrome unchanged, no opened Gmail -> 0.5, empty list, YouTube unchanged, input not mutated). Reviewed again 2026-10-22, no change: a literal median over all Gmail items is 0.0 when most mails are unopened (checked: 3 unopened + 1 opened), which would undo the decision, so this rule stays.

### A30. Wellbeing time lands on the LAST-opened day — [x] decided 2026-10-21 via A1.1b (his pick: b) (found 2026-10-18, wba/time_aggregator.py review) — DISCUSS AT THE END
- `memory_engagement` keeps one running total per item (not one row per visit). §11.2 attributes that whole
  total to `DATE(last_accessed_at)`.
- Effect: read an email for 10 min on Monday, reopen it for 5 s on Wednesday → Monday loses 10 min, Wednesday
  shows 10 min 5 s. Past days and weekly reports change after the fact; the heatmap moves the time too.
- Options: 1. keep (doc rule; document it as an approximation). 2. a per-visit table — breaks the 16-table
  lock. 3. no schema change: split nothing, but show "time from items last opened in this range" as the label.
- Claude's view: 1 + the honest label (3) for Review; 2 only if per-day accuracy becomes a real need.
- 2026-10-18: option 2 becomes possible if the schema is unlocked in A31 (go local).
- **Decision (2026-10-21, asked and answered under A1.1b):** per-visit rows with `dwell_seconds` for Chrome +
  YouTube; `time_aggregator.py` sums visits by the visit's day. Gmail unchanged (or removed — A41).

### A31. Go fully local: own PostgreSQL + Redis, real personal data, schema may grow — [x] decided 2026-10-21 (all 6 parts)
- Why (his words, summarised): the final review (28 Oct) is the last one; the panel's main question is "how is
  privacy maintained?". Development used cloud Neon + Redis; for the final system everything should run
  locally, and he moves from the test data to his actual personal accounts.
- Scope he named: replace Neon with a local PostgreSQL, replace cloud Redis with a local Redis, switch to his
  personal data. While doing it he may add new columns and new tables.
- Points to settle when we discuss:
  1. Schema lock: CLAUDE.md §2.2 locks the 16 tables. Unlocking is his call → update CLAUDE.md, `scripts/setup_db.py`
     and a deviation note at the same time. First candidate: a per-visit table (fixes A30).
  2. Fresh start vs migrate: with a fresh personal DB, the Neon repairs B1–B5 become unnecessary, and
     `data_faiss/` must be emptied / rebuilt so FAISS ids match the new rows.
  3. What still leaves the machine even when the DB is local: the LLM calls (query text + short snippets to
     OpenAI, A25) and the Gmail / YouTube APIs (needed to fetch the data). The privacy answer should say this
     plainly, or a local-LLM option is discussed.
  4. Config only, no code: `DATABASE_URL` / `REDIS_URL` in `.env` point to localhost; check sslmode and the
     Neon-specific stale-connection handling still behave on a local server.
  5. Time zone: the code assumes `NOW()` = UTC (Neon's default). A local PostgreSQL takes the machine's zone
     (IST) at install, which would silently shift every timestamp by 5:30. Set `timezone = 'UTC'` in
     postgresql.conf (or per connection) — then C22's local `marked_at` stays the only local column.
  6. **Demo risk — college Wi-Fi (raised 2026-10-19).** He reports "port 8000 doesn't work" on college Wi-Fi.
     Likely (inference, not verified): `localhost:8000` is loopback and never touches the Wi-Fi; what the campus
     firewall blocks is the backend's OUTGOING connections on non-web ports — Neon PostgreSQL (5432) and Upstash
     Redis (6379/6380). OpenAI / Gmail / YouTube APIs use 443 and pass. The review on 28 Oct is at college, so a
     Neon-backed demo would fail in the room.
     - 1-minute check on campus: `curl http://localhost:8000/docs` (page = port fine), then
       `Test-NetConnection <neon-host> -Port 5432` (`TcpTestSucceeded : False` = blocked).
     - Rarer cause: a college proxy pushed into Windows/Chrome routes `localhost` through it (curl works, browser
       fails) → add `localhost;127.0.0.1` to the proxy bypass list. No code change.
     - Going local removes the problem entirely: only 443 traffic leaves the laptop. Another reason to do A31 first.
     - If port 8000 itself is ever the issue: it is hard-coded in 5 places (`background.js`, `manifest.json`,
       `popup.js`, frontend `VITE_API_BASE` default, backend CORS list) → would need a small config change.
- **Decision (his, 2026-10-21), part by part:**
  1. Go local: **YES** — local PostgreSQL + local Redis, Neon/Upstash dropped for the final system.
  2. Data: **keep the test account's Neon data.** `pg_dump` Neon (at home) -> restore into local PostgreSQL ->
     schema alterations as later decisions need them (e.g. A30 visit table) -> clean up the rows that need it
     (B1-B3) -> full re-embed (B5; old FAISS index discarded). For data a new column/table cannot get from Neon,
     three ways, case by case at build time: (a) look the real value up from the TEST account itself — Gmail API,
     the test account's YouTube Takeout, the test Chrome profile's History file (real visit times, last 90 days);
     (b) approximate; (c) NULL where honest. Personal-data import
     (Google Takeout + browser History file) is NOT part of the build: E2 (personal-history import) was REMOVED from scope 2026-10-25
     (his decision: the presentation uses the test account only).
  3. Schema: **fully open** (add, change or drop) — the 16-table lock is lifted. Guardrails (his ask: "keep me
     within limits so that I don't creep scope"): every schema change must belong to a decided item; each one is
     written twice — in `scripts/setup_db.py` (fresh installs) AND as an ALTER/migration SQL run on the restored
     Neon dump; all schema changes happen Thursday, before the data cleanup and re-embed; dedup
     UNIQUE(source_type, source_id) and non-NULL `system_group_id` stay unless a decision says otherwise.
     Update CLAUDE.md §2.2 + paper DB section to match.
     Owner's decision 2026-10-23: Neon is left as is; known drift is fixed once, on the local restore.
     Migration SQL list so far: (a) `ALTER TABLE gmail_attachments ALTER COLUMN memory_id TYPE uuid USING memory_id::uuid`,
     then add the FK `memory_id REFERENCES gmail_metadata(memory_id) ON DELETE CASCADE` (Neon has varchar and no FK
     since B1 dropped `gmail_memory`; all 4 ids exist in `gmail_metadata`).
  4. Time zone: **store UTC, show IST** ("the user end should always be in IST, backend anything is fine").
     Force UTC per connection in code: SQLAlchemy engine in `ste/postgresql_manager.py` + psycopg2 connect in
     `rse/search_coordinator.py` (and ENP's `get_connection`). UI already converts to IST everywhere — verify
     during the frontend pass that every displayed time is IST. C22's local `marked_at` stays documented.
  5. Privacy statement: **say it plainly** — "storage and processing are local; only the query and short snippets
     go to the LLM, never full mails, pages or transcripts; Gmail/YouTube APIs fetch the user's own data". NO local
     LLM, and it is NOT listed as future work (his decision: college laptops cannot run one).
  6. Redis: **WSL Ubuntu** (`sudo apt install redis-server`; start before use with
     `wsl -d Ubuntu -u root service redis-server start`). PostgreSQL 16 is already installed natively as a Windows
     service. Check at home before the dump: if Neon runs PG 17, pg_dump 16 refuses -> install PG 17 client tools.
- **Built 2026-10-24 (restore + UTC + Redis; schema items, B1–B5 and the arctic-xs switch are NOT part of this run):**
  - Neon was PostgreSQL 16.15 (not 17), so the local 16.12 client dumped it directly. Dump taken from the direct host
    (not the `-pooler` host) with `pg_dump -Fc --no-owner --no-privileges`; file and baseline are in
    `C:\Users\mdshe\echo_backups\` (outside the repo, together with `.env` and `data_faiss/` copies).
  - Local database `echo_db` (it already existed, empty), owner role `echo` (no CREATEDB), `ALTER DATABASE echo_db SET
    timezone TO 'UTC'`. Restore: `pg_restore --no-owner --no-privileges --exit-on-error`, 0 errors.
  - Check: all 16 tables have the same row count AND the same md5 of their rows (`md5(string_agg(t::text ... order by
    t::text))`) as Neon; 36 indexes and 35 constraints on both sides (35 counted before the migration below added the foreign key; 36 now).
  - Migration SQL run (A31.3 item a), in one transaction: `ALTER TABLE gmail_attachments ALTER COLUMN memory_id TYPE uuid
    USING memory_id::uuid;` then `ALTER TABLE gmail_attachments ADD CONSTRAINT gmail_attachments_memory_id_fkey FOREIGN
    KEY (memory_id) REFERENCES gmail_metadata(memory_id) ON DELETE CASCADE;` (4 rows, 0 orphans). `scripts/setup_db.py`
    already declared this, so no change there.
  - Schema diff, restored vs fresh `setup_db.py` on a scratch database (dropped afterwards): the only differences are
    (1) a leftover enum type `userrole` (ADMIN/FACULTY/STUDENT) from the removed auth layer, which nothing used;
    (2) the two CHECK constraints print in a different but equivalent form (same accept/reject result on 7 test values).
    (1) was dropped on 2026-10-24 with `DROP TYPE userrole` (no CASCADE; 0 columns and 0 dependents; `pg_dump -Fc` taken first:
    `echo_db_before_drop_userrole_2026-10-24.dump`). (2) was left as is.
  - UTC per connection (A31.4) built in `ste/postgresql_manager.py`, `rse/search_coordinator.py` (this pool also serves
    `conversation_memory.py`) and `enp/enrichment_pipeline.py`. Shown on a database WITHOUT the ALTER DATABASE setting:
    plain connection = Asia/Calcutta, with the option = UTC. `scripts/setup_db.py` and `scripts/enp_status.py` open plain
    connections and rely on the database-level setting.
  - Redis: `REDIS_URL=redis://127.0.0.1:6379/0` (WSL Ubuntu, Redis 7.0.15). Upstash held only a test key `foo` and no
    `echo:*` key, so nothing was copied. WSL stops when no process runs in it and Redis stops with it; hold it with
    `wsl -d Ubuntu -u root -- sleep infinity`. Open choice: `vmIdleTimeout` in `.wslconfig` (not set).
  - Smoke test on the local stores: `/health` ok; `POST /retrieval/query` returned 10 results; `/export` 109 items;
    `enp_status.py`: 0 pending, 0 failed. Found, not caused by the move: FAISS has 110 vectors vs 109 `embedding_index`
    rows (the B5 re-embed rebuilds the index), and the LLM synthesis step got HTTP 401 from OpenAI (the `OPENAI_API_KEY`
    in `.env` is rejected).
  - Neon `echo-dev` and Upstash are untouched. Rollback = put the two lines from `env.neon-upstash.bak` back in `.env`.
- **Built 2026-10-24, schema batch (his picks for all four small decisions; schema only, the code that uses the new structures is a later stage):**
  - Migration file `scripts/migrations/2026-10-24_schema_batch.sql`, one transaction, run as role `echo`. Order: `pg_dump -Fc` backup
    (`echo_db_before_schema_batch_2026-10-24.dump`) and a count + md5 baseline of all 16 tables; restore into scratch database `echo_scratch`
    (identical); run the migration on scratch and pass 20 checks; build `echo_fresh` with `scripts/setup_db.py`; run on `echo_db`; same 20 checks.
  - A21: `ALTER TABLE memory_items DROP COLUMN classification_confidence` (109 of 109 rows had a value; nothing read it). Required code edit in
    `enp/enrichment_pipeline.py`: `mark_item_processed` lost its `confidence` parameter and both `UPDATE`s stopped writing the column
    (the debug log line still prints the confidence). Run against the migrated scratch copy: `mark_item_processed` and `mark_item_failed`
    (600-char reason cut to 500) both work.
  - A4: `ALTER TABLE gmail_attachments ADD COLUMN extracted_text TEXT` (4 rows, all NULL).
  - A1.1/A1.1b: new table `visits (visit_id UUID PK, memory_id UUID FK ON DELETE CASCADE, visited_at TIMESTAMP, dwell_seconds INTEGER)`
    with `idx_visits_memory (memory_id, visited_at DESC)` and `idx_visits_visited_at`. Backfill: 66 rows over 39 items (27 items with 2 visits, 12 with 1;
    Gmail has none). The stored total (Chrome `dwell_time_seconds`, YouTube `watch_time_seconds`) sits on the last visit, the first has 0: sum 2,449 s
    = 1,520 + 929, and every item's visit sum equals its stored total. FK rejects an unknown `memory_id`; deleting an item deletes its visits.
  - A24.3/A4: `memory_items.search_vector TSVECTOR` + `idx_memory_items_search` (GIN), filled for all 109 rows by one `UPDATE`. A generated
    column was ruled out (it cannot read `gmail_metadata`, `chrome_metadata`, `youtube_metadata` or `gmail_attachments`); ENP writes it from now on
    (not built yet). Check: for the 104 rows with `raw_text` ≤ 5,000 chars the vector equals today's `_FTS_DOCUMENT` (101 of 104 exactly; the other 3
    differ only in the very last word, because the new text ends with a space and `to_tsvector` tokenizes the final word differently at the end
    of a string; 104 of 104 when the old expression gets the same trailing space). All 5 rows over 5,000 chars now carry their tail (a word past
    character 5,000 is found by the new vector and missed by the old); the planner uses the GIN index when sequential scans are off (the table has
    109 rows, so by default it will not).
  - Result: 17 tables. The other 14 tables are identical to the baseline (count + md5); in `memory_items` and `gmail_attachments` the untouched columns are
    identical. Schema diff `echo_db` vs fresh `setup_db.py`: 114 columns and 40 indexes identical, constraints differ only in the two known CHECK
    spellings (left as is). `compileall` silent, `npm run build` ok.
  - Left for the code stage on purpose (done the same day, see the next "Built" block): `store_chrome_page` / `store_youtube_detection` writing `visits`;
    time filters (`search_coordinator`, `fusion`, `evaluate_quality`) and `wba/time_aggregator.py` reading them; ENP writing `search_vector` and
    `extracted_text`; `search_coordinator` reading `search_vector`.
  - Risk to check at that stage: if YouTube metadata (title, channel) arrives after ENP ran and does not reset `preprocessed`, the stored vector goes stale.
- **Built 2026-10-24, code wiring for the schema batch (tested on a scratch copy of `echo_db`; the live DB, real FAISS index and Gmail were not touched by tests):**
  - A1.1 writers, `ste/storage_engine.py`: `_store_visit`; `store_chrome_page` inserts a visit with `payload.dwell_seconds`; `store_youtube_detection` inserts a visit at the
    detection time with dwell 0; `update_youtube_watch_time` sets the video's NEWEST visit to `GREATEST(its dwell, session watch time)`. Gmail writes none.
  - A1.1 readers: `rse/search_coordinator.py` — `last_visit_at` (latest visit) is a result column; the keyword time filter is `(gmail AND created_at >= start) OR
    (not gmail AND EXISTS visit >= start)`; `filter_time_of(item)` is the one Python rule (Gmail arrival, Chrome/YouTube latest visit, no visit -> `created_at`), used by
    `rse/fusion.py` and `evaluate_quality`. Anchors, neighbours and `fetch_recent_items` still use `created_at` (not part of A1.1).
  - A1.1b wellbeing: `wba/time_aggregator.py` `TIME_UNION_SQL` (public): Gmail unchanged; Chrome/YouTube rows come from `visits` (YouTube only dwell > 0); item counts are
    `COUNT(DISTINCT)`; shorts and heatmap follow. `wba/regret_system.regret_rate` and `wba/pattern_detector.peak_activity_hours` reuse the union so regretted seconds stay
    inside the total. Sessions (`wellbeing_analytics._fetch_day_activity`) are built from the same union (review finding: otherwise one dashboard payload showed totals
    and session durations attributed to different days), so a visit is a session point and session durations add up to `total_seconds`.
  - A1.2: `rse/llm_synthesizer.py` — `TODAY: <date> (<weekday>)` line, the rule "use ONLY items inside the asked period", and a "Visits in the asked period" line for
    Chrome/YouTube items (`search_coordinator.fetch_visit_times`, at most the latest 5). Re-check after A26 (the Luna tool loop may change the prompt layout).
  - A24.3: `enp/enrichment_pipeline.mark_item_processed` writes `search_vector` in the same `UPDATE` as `preprocessed = TRUE` (raw_text and attachment text each cut at 300,000
    characters); `search_coordinator._FTS_DOCUMENT = "m.search_vector"` (the 5,000-character cap is gone).
  - A4: ENP step 2 (`_load_attachment_texts`) reads each Gmail attachment not yet extracted through `enp/attachment_processor.extract_full_text` (types without an extractor are
    not downloaded; cut at `MAX_EXTRACTED_TEXT_CHARS` = 200,000; PDF reading stops at the same cap), stores `extracted_text`, and a failure on one attachment (no Gmail, API error,
    unreadable file) is logged and skipped. `LazyGmailService` builds one Gmail service per batch and remembers a failed sign-in for that batch. `item["attachment_texts"]` is ready for A16.
  - Independent review (`opus5-reviewer`) found 8 points; fixed: (1) the SQL time filter and `filter_time_of` now agree on an item with NO visit row (fall back to
    `created_at`); (2) `update_youtube_metadata` sends a processed video back to ENP when its title or text changes, so a late transcript reaches the vector and the
    embedding (same idea as A3); (4) `extract_full_text` skips files over `MAX_INGEST_ATTACHMENT_BYTES` = 10 MB (my choice, change if you want another limit; size 0 = unknown is
    tried); (5) sessions from the same rows as totals; (6) a heartbeat for a video with no visit row logs a WARNING and ties on `visited_at` break by `visit_id`; (7) stale comments;
    (8) `attachment_texts` is documented as unused until A16. Not fixed on purpose: (3) an attachment that gives no text is read again whenever its mail is processed again
    (B5, A3, a retry): there is no column to record "tried" and the schema is closed for this change, and a mail is processed once otherwise.
    Left as a named risk: the end of a period ("yesterday") is enforced by the prompt, not in code (the filter has always been start-only, A1.2).
  - Tests (72 checks, all pass: 50 + 10 + 12): visits per capture/revisit/re-watch/heartbeat/unknown video; day totals with a page reopened on a later day (time stays on each visit's own day);
    regret rate 31.0 % and 100.0 % (never above); empty day 0; a page captured in August and reopened on 20 Oct IS found for "since 10 Oct" while a page captured today but last
    opened in September is NOT; a word past character 5,000 and a sender address found through the stored vector; a 600,000-character body is cut and still saves; attachment text
    stored once and reused; failed download leaves NULL and the mail still loads; prompt gets the TODAY line and the visits line only where it applies.
  - Not done: merging attachment text into the embedded text (A16 token budget, part of the model switch); the retry-duplicates-a-visit gap (needs a visit time from the extension).

### A32. KNN alone can never create a group suggestion — [x] decided 2026-10-21 (found 2026-10-18, wba/group_manager.py review) — DISCUSS AT THE END
- Hybrid score = 0.5 × rule_score + 0.5 × knn_score; a suggestion needs score > 0.70 OR a rule match.
- With no rule matching, the score is at most 0.5 × 1.0 = 0.5, so the 0.70 threshold is unreachable: KNN never
  suggests anything by itself — only rules do. Same formula in the architecture doc (§11.3 pseudo-code).
- Measured earlier (A18): MiniLM cosines between related items are often 0.3–0.6, so even a reweighting needs care.
- Options: 1. keep (rules drive suggestions; KNN only informs the score shown in review). 2. when no rule
  matches, use knn_score alone against its own threshold (e.g. top-3 mean ≥ 0.60, tune on his data).
  3. renormalise: rules present but unmatched → 0.5/0.5; no rules in the group → 1.0 × knn.
- Claude's view: 2 — it is what Phase 4 ("KNN improves each cycle") assumes; threshold tuned on real groups.
- **Decision (2026-10-21): option 2 — KNN gets its own threshold.** Suggest when any rule matches, OR (no rule
  match) knn_score (mean cosine of the 3 most similar confirmed members) >= KNN-only threshold. Threshold value set
  on Thursday's data AFTER the A40 model is chosen (cosines change with the model); start point ~0.60. Every
  suggestion stays pending until he accepts (human-in-the-loop unchanged) — EXCEPT, since A42 (2026-10-21), rule match
  AND KNN >= this bar, which is auto-accepted and shown in the Sunday review for undo. Build: `wba/group_manager.py:479`
  condition + a constant; update the docstring Phase 2 line and doc/paper wording.
  - **Built 2026-10-23 (working tree):** `KNN_ONLY_THRESHOLD = 0.60` now allows KNN-only suggestions. The active code still uses MiniLM; retune after A40's arctic-xs model is activated and vectors are rebuilt.

### A33. The pause switch has no UI — restore the extension popup? — [x] decided 2026-10-21 (found 2026-10-19, extension/background.js review) — DISCUSS AT THE END
- `background.js` honours `echo_tracking_paused` before every send, but the only thing that sets it is the popup
  (`popup.html` / `popup.js`), staged out on 2026-08-31 (git-ignored, `default_popup` removed from the manifest;
  see RESTORE.md §1 + §3). So today the user cannot pause capture at all.
- The popup also calls `/retrieval/query` and `/wellbeing/recent` directly with `fetch` (needs its own review pass).
- **Options:** (1) restore the popup as it was (pause + search + recent 5) and review `popup.js`; (2) restore
  only the pause toggle; (3) put the pause switch in the dashboard Settings page instead (backend flag in Redis,
  extension polls it — more moving parts); (4) leave it out of the demo.
- **Decision (2026-10-21): option 2 — restore only the pause toggle.** Build (~20 min): un-ignore `popup.html` /
  `popup.js`, strip the search box and recent-5 (and their direct `fetch` calls), keep the pause switch writing
  `chrome.storage.local.echo_tracking_paused`; add `default_popup` back to `manifest.json` `action`; update
  RESTORE.md. Later add-on if time: dashboard switches returned with `GET /chrome/rules` (A9.5) so the extension
  stops sending (today the server discards).
  - **Built 2026-10-23 (working tree):** popup and manifest now expose only the pause toggle; popup fetches were removed and the popup files are no longer gitignored. `RESTORE.md` was updated.

### A34. YouTube "revisit" (Option C) can never decide anything — [x] decided 2026-10-21 (found 2026-10-19, extension/content/youtube_tracker.js review) — DISCUSS AT THE END
- The server checks the revisit flag only inside `/ytc/video-detected`, and the extension sends that only AFTER
  Options A / B / manual interaction already passed. A short first visit sends nothing, so it leaves no flag; a
  second short visit sends nothing either. So Option C never lets a video through on its own.
- Chrome solves this with an early call at 5 s (`/chrome/revisit-check`), which records the visit.
- **Options:** (1) add a `/ytc/revisit-check` call at ~5 s of watch time (like Chrome; then a second short watch
  passes); (2) drop Option C from the doc's gate and the code (say so in the paper); (3) keep as is.
- **Decision (2026-10-21): option 1 — add a 5 s `/ytc/revisit-check` call** (same as Chrome's 5 s rule, A10).
  Build (~30 min): `youtube_tracker.js` sends it once per video at 5 s of active watch time through
  `postToBackend` (pause respected); the route calls `check_and_record_revisit_async` (Redis, 24 h TTL) and, on a
  revisit, lets the video through Option C. Add the 5 s value to the A39 timings list.
  - **Built 2026-10-23 (working tree):** one `YTC_REVISIT_CHECK` is sent at 5 s through the background relay; a true response fires the normal video-detected path as `revisit`. The route reuses `check_revisit`, which calls `check_and_record_revisit_async` and respects the YouTube capture setting.
  - **Gate fix 2026-10-23:** `/video-detected` no longer reads the Redis key for Option C (the 5 s call writes that key on every first visit, so the server saw a "revisit" for any video past 5 s and its own gate always passed). Now `/revisit-check` remembers a confirmed revisit in memory for 60 s (`REVISIT_CONFIRM_SECONDS`), and `/video-detected` accepts `triggered_by="revisit"` only against that entry, once. The visit is still recorded in Redis after the gate passes. Checked through the real handlers on real Redis (Postgres write patched out, test keys deleted): first visit + 2 s event refused; real revisit saved once, replay refused; forged revisit refused; 25 s watch and manual pause still saved; confirmation expired after 60 s refused; invalid/empty id false. Browser flow not run.

### A35. Store the OAuth tokens unencrypted, since everything is local? — [x] decided 2026-10-21 — PLAIN TOKENS
- Today `token_gmail.enc` / `token_youtube.enc` are Fernet-encrypted (`ste/security.py`); the key is
  `ECHO_TOKEN_ENCRYPTION_KEY` in `.env`. His idea: once A31 makes everything local, plain `token_*.json` is enough.
- **For plain:** one less env key to lose; simpler `security.py`; the key sits in `.env` on the same disk anyway,
  so encryption only stops someone who copies the token file but not `.env`.
- **Against plain:** the Gmail token is a long-lived refresh token with `gmail.readonly` = read access to the whole
  mailbox. A plain file leaks via backups, OneDrive/Desktop sync, a zip of the repo, or an accidental `git add -f`.
  Encryption at rest is also a point the paper/deck can claim.
- **Touches:** `ste/security.py`, `gmail_api.py`, `youtube_api_client.py`, `scripts/test_youtube_auth.py`, `.gitignore`.
- Note: `scripts/test_gmail_auth.py` deleted 2026-10-19 (`gmail_api.authenticate_gmail` opens the browser sign-in
  itself). `scripts/test_youtube_auth.py` also deleted 2026-10-19 (his decision): `youtube_api_client.sign_in_youtube()`
  now opens the browser sign-in once at backend startup, on a daemon thread started by `backend/main.py`.
  `ste/security.py` callers line fixed.
- **Decision (his, 2026-10-21): plain `token_gmail.json` / `token_youtube.json`, no encryption.** Reason: in the
  demo he shows the real token; an encrypted blob looks "fake" to the panel.
- **Build:** `gmail_api.py` + `youtube_api_client.py` read/write plain JSON; remove the token encrypt/decrypt/
  migrate helpers from `ste/security.py` but KEEP `validate_redis_tls_url` (redis_manager uses it; it already
  allows redis:// to localhost, so local Redis works); drop `ECHO_TOKEN_ENCRYPTION_KEY` from .env / README;
  existing `.enc` tokens: one-time decrypt to .json, or just sign in again. `.gitignore` already covers
  `token_*.json`.
- **Built (his, verified by him; light re-check 2026-10-24):** `gmail_api.py` and `youtube_api_client.py` read and write
  plain `token_gmail.json` / `token_youtube.json` through `ste.security.read_token_text` / `write_token_text` (UTF-8 with
  BOM tolerated, same-folder temp file + atomic replace, one lock); the Fernet helpers are gone, `validate_redis_tls_url`
  stays; `.gitignore` covers `token_*.json`, `token_*.json.tmp` and the old `.enc` names; no `ECHO_TOKEN_ENCRYPTION_KEY`
  or `cryptography` left in `requirements.txt`, README or the code. Old `.enc` files are not migrated: the first start
  signs in again. Re-check: missing file -> None, write leaves no temp file, BOM file reads clean. C28 (sign-in at startup
  on a daemon thread, poller never opens a browser) came with it.
- **Guards:** paper/deck must NOT claim "tokens encrypted at rest". A token shown on the projector is a live key
  to the test mailbox — revoke it after the demo (Google Account -> Security -> third-party access) and sign in
  again.

### A36. Two maintenance jobs lost their only trigger (scripts deleted 2026-10-19) — [x] decided 2026-10-21
- **Monthly centroid recompute (architecture 9.3/9.4).** `enp/system_group_classifier.recompute_centroids()` still
  exists, but nothing calls it now. Note: it had never run — `data_faiss/centroids.npz` does not exist, so the
  classifier has always used the seed-text centroids.
  Options: (1) call it from the backend (e.g. at startup when `centroids.npz` is missing or > 30 days old, in a
  background thread); (2) drop the feature: delete the function + the centroid loader, say "seed centroids
  only" in the paper/deck; (3) leave the dead function.
- **FAISS rebuild.** `ste/faiss_manager.py` tells the user to "rebuild" when the index file is missing, corrupt or
  of the wrong dimension/version, but the rebuild tool is gone. Relevant to A31: a fresh local DB from `pg_dump`
  keeps the same `memory_id`s, so the existing index stays valid; a re-embed from scratch needs either a rebuild
  or resetting `preprocessed = FALSE` (ENP then re-embeds every row — the old `enp_reprocess.py` job).
  Options: (1) ENP worker rebuilds automatically when the index fails to load; (2) messages say "reset
  preprocessed and let ENP re-embed" (one SQL line, written in the README); (3) restore the script from git.
- **Claude's recommendation (NOT decided — he wants to discuss it):** centroids -> option 1 (startup, background
  thread, when `centroids.npz` is missing or > 30 days old); FAISS rebuild -> option 2 (fix the messages + README SQL;
  clears C27's faiss_manager bullets). Paper/deck must not claim "monthly centroid recompute" unless option 1 is built.
- **Decision (his, 2026-10-21):** centroids -> **option 1, run at startup** when `centroids.npz` is missing or
  > 30 days old. FAISS rebuild -> **moot**: A31.2 re-embeds everything into a fresh index; C27 still fixes the
  stale "rebuild" messages in `ste/faiss_manager.py`.
- **Build (implemented 2026-10-22):** after loading FAISS and initializing seeds, the ENP worker checks the
  centroid file age and calls `recompute_centroids()` at startup when the file is missing or older than 30 days.
  A stale file is not loaded, so categories below the 10-item minimum retain their seeds; recomputed values update
  the current ENP process and are persisted for later starts. If no category reaches the threshold, no file is
  written and the trigger retries on the next startup. Run after the B5 re-embed has filled FAISS for useful
  results. This is startup-triggered maintenance, not a calendar scheduler.

### A37. Data retention: auto-delete old data after a fixed period (PANEL, Phase 3 Review 1) — [x] decided 2026-10-21 (added 2026-10-21, from the Review 2 deck/LLD)
- Panel: "keep captured data for a fixed period (6 months or 1 year), then delete it automatically to save storage,
  as production systems do". Our Review 2 answer (deck slide + LLD): "planned as future work — a configurable
  auto-delete window (6 or 12 months)". Today nothing deletes memory items by age; only chat history is cleaned
  (`rse/conversation_memory.cleanup_old_sessions`, 30 days, once at startup).
- It is the final review, so the panel may ask "did you do it?".
- A full delete must reach every store: `memory_items` + child rows (embedding_index, group links, wellbeing
  rows), the FAISS vectors (`remove` by memory_id), and Redis keys. `DELETE /items/{id}` already does one item —
  reuse that path. Soft delete (`is_deleted`) vs hard delete is part of the decision.
- Correction 2026-10-21: `DELETE /items/{id}` is SOFT only (`backend/items.py:98`, sets `is_deleted`); text stays in
  Postgres and the vector stays in FAISS. No hard-delete path exists yet.
- **Decision (2026-10-21): option a — retention setting, default "keep forever".** Settings → Retention:
  forever / 1 year / 6 months (stored with the capture settings in Redis, `echo:settings:capture`). A job at backend
  startup HARD-deletes items older than the window from every store: `memory_items` + child rows (CASCADE),
  FAISS vectors (`remove` by memory_id), Redis keys. ~2 h. Panel answer: "user-controlled; default forever because
  recalling old things is the product". Storage estimate (assumption, not measured): ~100 items/day ≈ 180 MB/yr
  text + ~55 MB/yr vectors.
- **Built 2026-10-25 (commit b354647):** `ste/retention.py` (`purge_expired_items`), run once at startup
  by `backend/main.py` (`_run_retention`, never raises); setting `retention` = `forever` | `1y` | `6m` in
  `ste/settings_store.py` (an unknown value counts as `forever`) and `POST /settings` (`backend/items.py`, other
  values give 422); `FAISSManager.remove_many` (new); Settings page picker (`SettingsPage.tsx`, `api.ts`).
  Age = `memory_items.created_at`; 1 year = 365 days, 6 months = 183. Deletes Postgres first (all 11 child tables
  are ON DELETE CASCADE, checked in `pg_constraint`), then the FAISS vectors + `save_index`, then Redis
  (`echo:attachment:<id>:*`, `echo:enp:retry:<id>`). Two guards: default `forever`, and the job refuses to run when
  EVERY item is past the cutoff (clock set ahead, or nothing new for the whole window).
  Checked on a scratch copy (`echo_db_retention`, restored from the rehearsal DB, temp index copy; real `echo_db` and
  `data_faiss/` untouched): 111 items; forever and 1y delete 0; clock 2030 refused; 6m deleted exactly the 63 older
  than 2026-04-25, leaving 48; 0 rows left for the deleted ids in all 11 child tables (4 gmail_attachments rows went
  with them); FAISS memory and disk ids both equal the 48 DB ids; survivors' vectors unchanged, still unit length,
  self-search score 1.0; second run deletes 0; `remove_many` with `[]`, an unknown id and every id all behave;
  Redis down does not raise. NOT checked: the Redis key deletion against a live Redis (none was running); the Settings
  picker in a browser (only `tsc` passes); the job racing ENP on the same item (rare: only items past the cutoff).
  The change applies at the NEXT backend start, not on save (the page says so).
- **Branch review of c719cdc, 2026-10-25 (Opus 5 reviewer, ~4,250 fuzzed window cases, 0 over 512 tokens): window math and
  the query/document paths correct.** Fixed in the working tree the same day, each tested (20 checks, all pass): (1) the prompt guard
  tested key presence and could never fail (a prompt-less MiniLM passed it); it now checks the values: query prompt
  non-empty, document prompt empty; (2) the "identity of last resort" text in `prepare_item_for_embedding` skipped
  `fit_to_window` (a 31k-char url was stored whole; not shown to occur in real data); (3) `centroids.npz` had no model
  stamp, so a file from another 384-d model would load silently; it now stores `_embedding_version` + `_dimension`, an
  unstamped / other-model / wrong-shape / corrupt file is ignored, and `centroids_need_recompute()` returns True for it.
  **Side effect: the current real `centroids.npz` (written before the stamp) is ignored at the next backend start and
  `misc` is recomputed from its 66 confirmed items.** Docs fixed: `Echo_System_Design.md` (stale char-cap row, 0.35 vs
  0.55, the claim that the backend does not start on an old index: it does start, ENP stops, search falls to keyword-only),
  `build_lld.py`, `cls_m2_retrieval.mmd`, `approved_content.md`. Left as is: `Research Paper/Draft 1` `main.tex:214` (his
  paper text names MiniLM) and dead `text_cleaner.truncate()` (no callers; cleanup only).

### A38. Spam filtering for Gmail (PANEL, Phase 3 Review 1) — [x] decided 2026-10-21 (added 2026-10-21, from the Review 2 deck/LLD)
- Panel: "add a model that filters out spam emails before they are captured". Our Review 2 answer: Gmail's own
  Spam folder is never read; "a dedicated spam model is planned as future work".
- Checked 2026-10-21: `gmail_api.py:296` lists mail with spam and trash excluded (Gmail API default
  `includeSpamTrash=false`), so the Review 2 answer is true. The real gap: Promotions / Social / Updates mail
  (newsletters, offers) IS captured and fills memory.
- **Decision (2026-10-21, his): use Gmail's own spam marking only — skip mail Gmail puts in Spam (and Trash).**
  Already true for the full listing. Promotions / Social stay captured (his choice). Panel answer: "Gmail's spam
  filter is our spam model; spam is never read."
  - Build: the new `history.list` path (A5) must skip messages whose `labelIds` contain SPAM or TRASH — Likely
    (from memory of the API) history.list has no includeSpamTrash switch, so the list call's default does not
    cover it. Verify when building A5.
  - **Built 2026-10-24 with A5 (cloud session):** the history ids drop SPAM/TRASH from the `labelIds` the history record
    carries, and `fetch_and_store_new_emails` checks the labels of the fetched message again (mail trashed after it
    arrived). Checked on a mock service only; whether a real history record carries `labelIds` is for the live poll to show
    (if it did not, the second check still catches it, at the cost of one `messages.get`).
  - Fallback if Promotions/Social crowd results in the demo data: skip those two categories too (~30 min).

### A39. Review every timing and threshold before the final review (his ask, 2026-10-21) — [x] decided 2026-10-21
- He wants to go through each number once and change any that feel too short or too long. Current values:
- **Chrome gate** (`extension/background.js` 42–46 + `ingestion/chrome/intent_filter.py` 26–29, must match):
  Phase 1 foreground >= 5 s (sets the revisit flag); Phase 2 passes on ANY of: >= 10 s, scroll >= 25 %,
  >= 1 interaction, or a revisit. App pages (Slack/Notion/Jira) >= 5 s, server copy too (A8.2). Signals sent every 5 s
  (`chrome_tracker.js` 37). Page text: 200 min / 12,000 max chars. Failed send retried after 30 s.
- **Revisit:** flag lives 24 h in Redis (`ste/redis_manager.py` 42), Chrome and YouTube. A visit < 5 s never
  counts (A10).
- **YouTube gate** (`youtube_tracker.js` 87–96 + `youtube_connector.py` 85–87, must match): Shorts >= 15 s
  watched, regular >= 20 s, OR completion >= 50 %, OR a revisit (A34: revisit check sent at 5 s of watch time).
  A Short = `/shorts/` URL (duration <= 60 s fallback is unused)
  (`video_classifier.py` 30). Heartbeat every 5 s; idle after 30 s of no input.
- **Gmail:** polled every 30 s (`ingestion/gmail/config.py` 26); no gate, every mail is stored. Read time
  (extension): an open < 3 s is not a visit; idle after 30 s; in-focus reading credited per 60 s
  (`gmail_tracker.js` 59–61).
- **Wellbeing:** session gap 20 min (`wba/wellbeing_analytics.py` 50); regret reminder only after a >= 15 min
  session, max 1 per type and 2 in total per day, 2 h cooldown (`wba/regret_system.py` 47–50); a habit =
  >= 3 times in 30 days (`wba/pattern_detector.py` 34–35); insights cover the last 7 days.
- **Groups:** KNN needs >= 6 confirmed items; suggest when any rule matches OR KNN-only >= ~0.60 (A32, set after A40);
  rule suggestion after >= 5 shared matches (`wba/group_manager.py` 59–65).
- **Background jobs:** ENP checks for new rows every 10 s, waits up to 600 s for YouTube metadata
  (`enp/enrichment_pipeline.py` 60, 71); chat history kept 30 days (`rse/config.py` 106); attachment text
  cached 1 h.
- **Retrieval:** quality gate top similarity >= 0.35 and >= 2 results, widen up to 3 times; neighbours ± 5 min
  (`rse/config.py` 78–96).
- **Part 1 decided 2026-10-21 (his values; he let Claude fine-tune "around" them):**
  - Chrome scroll: 25 % → **15 %** ("reduce a bit"; Claude picked 15).
  - YouTube Shorts watch time: 15 s → **10 s**. Regular videos stay 20 s.
  - YouTube completion: 50 % → **10 %** (his value). Claude flagged once: completion uses watch time / length, so a
    20 s Short passes at 2 s and a Shorts-feed swipe would save almost every Short. Safer add-on proposed: count
    completion only after >= 5 s of watch time. **Floor: YES — 10 % + 5 s minimum (his choice 2026-10-21;
    he first said "let it be as it is", then switched to option a).** Completion passes only when watch time >= 5 s
    AND watch / length >= 10 %. Build: a `INTENT_COMPLETION_MIN_SECONDS = 5` constant in `youtube_tracker.js` and
    `COMPLETION_MIN_WATCH_SECONDS = 5` in `youtube_connector.py` (must match). Side effect: any Short up to 50 s
    saves at 5 s, so the 10 s Shorts rule only decides for Shorts over 100 s.
  - Groups: KNN starts at **5** confirmed items (was 6; his "change it to 5" read as this, since rule suggestions
    are already 5).
  - Everything else in the list above kept as is.
  - Build (~30 min, both copies must match): `extension/background.js` PHASE2_MIN_SCROLL +
    `ingestion/chrome/intent_filter.py` PHASE2_MIN_SCROLL = 0.15; `youtube_tracker.js` INTENT_WATCH_SHORT = 10,
    INTENT_COMPLETION_THRESHOLD = 0.1 + `youtube_connector.py` SHORT_MIN_WATCH_SECONDS = 10,
    COMPLETION_RATE_THRESHOLD = 0.1 (+ header comments "0.5"); `wba/group_manager.py` MIN_CONFIRMED_FOR_KNN = 5.
    Doc §6.2 / §7.3 numbers are his to change; paper + deck numbers change too (tell the deck agent).
  - Thursday task (not a vote): every cosine cutoff was tuned on MiniLM — 0.35 search quality gate, Stage-3
    centroid cutoff (A18), KNN-only ~0.60 (A32). Re-measure all three on Thursday's data after A40.
- **Part 2 decided 2026-10-21: option c — clicks on links do not count as an interaction.** Only matters for
  pages left between 5 and 10 s (10 s passes on time alone). Today a click on a headline counts, and the
  `pagehide` signal still carries it, so a homepage clicked through after 6 s is saved. New: copy / expand /
  "show more" clicks and text selections still count; link clicks don't.
  - Build (~15 min, extension only; the server just compares the count): in `chrome_tracker.js` the click
    listener skips events whose target is inside an `a[href]` (`event.target.closest("a[href]")`). Known cost:
    an in-page link (table-of-contents jump) no longer counts; such pages still pass by scroll (15 %) or 10 s.
- **A39 built 2026-10-24 (cloud session; read from the diff and checked offline, extension NOT run in Chrome):**
  - Chrome scroll 0.25 -> 0.15 in `extension/background.js` and `ingestion/chrome/intent_filter.py` (+ comments).
  - YouTube: `INTENT_WATCH_SHORT` 15 -> 10, `INTENT_COMPLETION_THRESHOLD` 0.5 -> 0.1, new `INTENT_COMPLETION_MIN_SECONDS = 5`
    in `youtube_tracker.js`; `SHORT_MIN_WATCH_SECONDS` 10, `COMPLETION_RATE_THRESHOLD` 0.1, new `COMPLETION_MIN_WATCH_SECONDS = 5`
    in `youtube_connector.py`. Gate checked on 8 cases (short 10 s passes; 9 s of 100 s fails; 4 s of 20 s fails the floor;
    5 s of 20 s passes; regular 19 s of 600 s fails; 60 s of 600 s passes; 20 s passes; revisit passes).
  - `wba/group_manager.py` `MIN_CONFIRMED_FOR_KNN` 6 -> 5 (docstring too).
  - `chrome_tracker.js` click listener skips targets inside `a[href]` (stand-in DOM check: link click not counted, others counted).
  - `node --check` clean on the three JS files; `compileall` silent. Doc §6.2 / §7.3 numbers and the paper/deck numbers are still to be changed.
  - Not changed: the dead-doc line in A39's first list (values above) and the other "kept as is" numbers.

### A40. Embedding model and cross-encoder model — keep or upgrade? — [x] decided 2026-10-21 (his ask, 2026-10-21)
- Today: embeddings = `all-MiniLM-L6-v2` (`enp/embedding_generator.py:56`; 384-d, reads ~256 tokens; used by ENP,
  RSE query embedding, Stage-3 centroids, A23/A24 passage matching). Re-ranker = cross-encoder
  `ms-marco-MiniLM-L-6-v2` (`rse/reranker.py`). Both run on CPU on his laptop.
- Timing: B5 re-embeds every row on Thursday anyway, so switching the embedding model now costs no extra re-embed.
  After Thursday, any switch = another full re-embed.
- Candidates (Likely — from memory of the model cards / MTEB, verify sizes + licences before picking):
  - Embedding: keep MiniLM-L6 (fastest, weakest) | `BAAI/bge-small-en-v1.5` (same 384-d so FAISS dim unchanged,
    ~512-token window, better retrieval, ~1.5x slower; queries want an instruction prefix) | larger 768-d models
    (e.g. nomic-embed-text v1.5, long window) — slower on CPU, FAISS dim + VECTOR_DIMENSION change.
  - Cross-encoder: keep MiniLM-L-6 | `ms-marco-MiniLM-L-12-v2` (~2x slower, a bit better) | `BAAI/bge-reranker-base`
    (much better, several x slower on CPU — risky for demo latency).
- Touches: model names, `EMBEDDING_VERSION`, maybe VECTOR_DIMENSION, A18 threshold (measure with the chosen
  model), A24.1 snippet size (window), paper/deck model names. Measure on his laptop: embed speed, query latency.
- **2026-10-21, his direction: "We will be changing the model"** (embedding model; which one = this decision).
  Close A16 (token budget, order) at the same time.
- **Research 2026-10-21 (model cards / web, not measured on his laptop):**
  - Local MiniLM files: `max_position_embeddings` 512, `max_seq_length` 256, model card "sequence length was
    limited to 128 tokens" in training.
  - Retrieval NDCG@10 (MTEB/BEIR, from model cards): all-MiniLM-L6-v2 41.95 · snowflake-arctic-embed-xs 50.15
    (22M, 384-d, 512 tok, query prefix needed) · bge-small-en-v1.5 51.68 (33M, 384-d, 512 tok, query prefix) ·
    ibm-granite/granite-embedding-small-english-r2 BEIR 50.9 (47M, 384-d, **8192 tok**, no prefix, Apache 2.0,
    ModernBERT → needs transformers ≥ 4.48; installed 4.57.6, sentence-transformers 5.4.1, torch 2.2.2+cpu) ·
    nomic-embed-text-v1.5 (137M, 768-d, 8192 tok, prefixes, trust_remote_code).
  - All 384-d options keep the FAISS dimension. Long windows cost CPU time (attention grows with length²) —
    pick a cap (e.g. 512–1024) and measure embed speed on his laptop before Thursday's B5.
- **Decision (his, 2026-10-21; he accepted every pick of the A16/A40/A42/A43 brief; build notes + measurements are below):**
  - **A40.1 embedding model = `Snowflake/snowflake-arctic-embed-xs`, 512-token window** (384-d, FAISS dim unchanged,
    Apache-2.0; query prefix per its model card). Measured on his laptop, 39 questions: right item first 37/39
    (MiniLM 31, bge-small 36, granite-r2 @1,024 35), top-5 39/39; ~221 ms/item at 512 tokens. Fallback if Thursday's
    data shows long pages missed by meaning: granite-r2 at 1,024 tokens.
  - **A40.2 cross-encoder = keep `ms-marco-MiniLM-L-6-v2`** (L-12 same hits at 2x time; bge-reranker-base worse, 12x slower).
  - **A40.3 (new): the cross-encoder reads title + the best-matching 500-char window** (same function as A24.2),
    not the first 512 chars: 25/27 -> 27/27 at no measured cost.
  - Thursday re-tune (cutoffs were tuned on MiniLM): search gate start 0.55, KNN bar (A32 + A42.1) start 0.65,
    A18 seed cutoff re-measured (lead 0.08 portable); ALSO re-measure the 0.55 / 0.08 cutoff for RECOMPUTED centroids
    (set on MiniLM; flagged by the design-doc sync 2026-10-22). Passage picking (A23/A24.2): prefer keyword overlap.
  - Update model names: paper `\embmodel`, deck `EMBEDDING_MODEL`, design doc, README, EMBEDDING_VERSION; B5 re-embeds.
- **A40 build notes (from the brief; line numbers as of 2026-10-21):**
  - Model facts (read from the downloaded files): arctic-xs 22.6M params, 384-d, `max_seq_length` 512, CLS pooling,
    ends with a Normalize layer (unit vectors, like MiniLM today, so C14 holds), config prompt
    `query = "Represent this sentence for searching relevant passages: "`, no document prompt. bge-small: 33.4M, 384-d,
    512, CLS, Normalize, MIT. granite-r2: 47.7M, 384-d, 8,192, CLS, **no Normalize** (code must normalise if it is
    ever the fallback), Apache-2.0.
  - `enp/embedding_generator.py:56-57` `MODEL_NAME` / `MODEL_REPO_ID` → `Snowflake/snowflake-arctic-embed-xs`; today's
    repo id is built as `sentence-transformers/{MODEL_NAME}` — that pattern must go; line 130 loads by `MODEL_NAME`,
    load by the repo id instead. Offline loading (`_prefer_local_models`) checks the cache by repo id, so it keeps
    working once the id is right.
  - Add a query path `generate_query_embeddings(texts)` that prepends `model.prompts["query"]` (verified to exist in
    the loaded config). Callers that embed QUESTIONS switch to it: `rse/search_coordinator.py:415-419`,
    `rse/time_anchor.py:149-152`. Items, centroid seeds (`enp/system_group_classifier.py:325`) and group KNN keep
    the document path (no prefix).
  - `ste/faiss_manager.py:68` `EMBEDDING_VERSION = "snowflake-arctic-embed-xs-v1"`; `VECTOR_DIMENSION` stays 384.
  - Docstrings naming MiniLM / 256 tokens: `embedding_generator.py:2,22-29`, `topic_extractor.py:8-19,54-61`,
    `enrichment_pipeline.py:13`, `system_group_classifier.py:13`, `search_coordinator.py:17`,
    `faiss_manager.py:6,122` (+ the stale "UNNORMALIZED" wording, C14). CLAUDE.md §5 FAISS bullet. Paper, deck and LLD
    model names are other agents' files — tell them.
  - Scores sit higher with arctic-xs (unrelated pairs ~0.40 instead of ~0.06), so all cutoffs need re-tuning (below).
  - Query cost: one short question 22 ms (arctic-xs) vs 12.5 ms (MiniLM). If A23/A24.2 passage matching scored
    windows with the embedding model, 10 results × ~5 windows per round would add ~1 s per round under load
    (Likely, from per-item speed) — so use keyword overlap there, or measure first.
  - B5 re-embed time, worst case (every item fills the window): ~110 test items (Likely: `data_faiss/echo_faiss.index`
    was 169,005 bytes on 16 Oct = ~110 vectors of 1,536 bytes; Neon may hold more now) → arctic-xs ~24 s. If a
    personal import (E2, removed from scope 2026-10-25) had brought 3,000 items: MiniLM 4 min, arctic-xs 11 min, bge 17 min, granite@512 36 min, @1,024 100 min.
    Daily load at ~100 items/day (A37's assumption): arctic-xs ~22 s of CPU.
  - Build: ~45 min code + the B5 re-embed already planned + the cutoff re-tune.
  - Risk: no test item was longer than ~940 tokens, but Chrome pages reach 12,000 chars (~3,000 tokens). Notice it on
    Thursday's data as long pages found by keyword but missed by meaning → fallback granite-r2 at 1,024 tokens (same
    384-d, ~9× the embed time, normalise its vectors).
  - Reading past 512 tokens bought little: on the 4 questions with the answer past token 512, granite@1,024 and
    arctic-xs@512 both got 2 first / 4 in top 5 (a long text becomes one averaged vector). Granite@1,024 costs ~9×
    arctic-xs per item. Granite alone separated "in group" from "not in group" in both test groups (A42), but its
    scores are squeezed into 0.72–0.95.
  - nomic-embed-text-v1.5 was not measured (768-d changes the FAISS dimension, needs `trust_remote_code`, 137M params).
  - **A40.2/A40.3 notes:** no code change for A40.2 (CLAUDE.md §3 already names L-6). Today the re-ranker scores
    `RRF_POOL_SIZE = 50` candidates (`rse/config.py:76`), once per search round, up to 5 rounds (A26): 2.4 s × 5 = 12 s
    worst case under load. Optional, not measured on real data: pool 50 → 30 cuts re-rank time ~40 % (1.4 s vs 2.4 s)
    — only if Thursday's demo feels slow. All three cross-encoders take 512 tokens for question + item; today a pair
    is 127 tokens on average, 152 at most (item cut at `RERANK_DOC_CHAR_LIMIT = 512`, `rse/config.py:86`).
    A40.3 build: `rse/reranker.py:114-128` `_document_text` → title + best window (reuse the A24.2 function);
    `rse/search_coordinator.py:64` must select enough text (cleaned text, or `raw_text` with no 1,000-char cut).
    ~30 min on top of A24.2. Late-answer cases: first-512-chars 14/16 → window 16/16.
  - **Thursday re-tune** — cosine ranges on the 25-item set, not comparable across models:

    | Model | Right item (median / lowest) | Wrong items (median / 95th pct) | No-answer questions, best score |
    |---|---|---|---|
    | MiniLM (today) | 0.532 / 0.026 | 0.055 / 0.223 | 0.11 – 0.43 |
    | bge-small | 0.750 / 0.573 | 0.475 / 0.594 | 0.51 – 0.66 |
    | arctic-xs | 0.709 / 0.530 | 0.405 / 0.530 | 0.48 – 0.64 |
    | granite-r2 | 0.872 / 0.795 | 0.722 / 0.783 | 0.74 – 0.83 |

    - Search gate (`rse/config.py:90` `MIN_TOP_SIMILARITY = 0.35`, also used by `rse/time_anchor.py:155`): arctic-xs
      start **0.55** (just above the wrong-item 95th percentile 0.53). For every model, no-answer questions overlap
      answered ones (arctic-xs: best no-answer 0.641 vs lowest right answer 0.530), so the gate can only trigger
      widening, never prove "nothing there".
    - A18 seed cutoff (0.25 / 0.08, set on MiniLM): unrelated pairs sit ~0.35 higher with arctic-xs, so the absolute
      value must move up; the lead (0.08) is the more portable part.
    - A32 KNN-only bar (~0.60 on MiniLM): arctic-xs test groups suggest ~**0.65** (Placement: lowest "in" 0.656,
      highest "out" 0.632; Capstone overlaps). The same constant feeds A42.1.
  - **Measurements (test set: 38 invented items, 39 questions with known answers + 6 with none, 2 test user groups —
    hand-made by Claude, small; one question is noise, only big gaps count).** Laptop: Ryzen 5 5600H, 6 cores, 7.3 GB
    RAM, measured on battery with another Python job running → absolute times pessimistic (a same-moment re-run was
    1.4–3× slower), the ratios are the useful part. sentence-transformers 5.4.1, transformers 4.57.6, torch 2.2.2+cpu.
    The bench scripts were in a session temp folder and are likely gone; nothing was installed.
    - Right item first / in top 5 (39 questions): MiniLM today's caps 31/35 · bge-small@512 36/37 · arctic-xs@512
      **37/39** · granite@512 34/38 · granite@1,024 35/39. Answers past token 512 (4 questions): MiniLM 0 first /
      2 top-5 · bge 2/3 · arctic 2/4 · granite@1,024 2/4.
    - Embed ms per item @512 tokens: arctic-xs 221 · bge-small 342 · granite 725 (1,990 @1,024; 10.4 s @2,048; batch of
      4 at 4,096 ran out of RAM, asked for 3.2 GB). MiniLM @256 79.
    - Cross-encoder (27 questions, pool of 20): L-6 25/27 first with first-512-chars, 27/27 with the best window,
      30 pairs 1.4 s / 50 pairs 2.4 s · L-12 same hits, 2.8 s / 4.7 s · bge-reranker-base (278M) 23/27 (26/27 window),
      16.7 s / 25.7 s.
    - Group KNN (mean cosine of the 3 nearest of 5 confirmed members), arctic-xs: Placement prep "in" lowest 0.656 vs
      "out" highest 0.632 (separates); Capstone "in" lowest 0.600 vs "out" highest 0.614 (overlaps). MiniLM: 0.289 vs
      0.254 / 0.252 vs 0.264; bge: overlaps in both; granite separates both. Per item (arctic-xs) — Placement in:
      Zentrix 0.659, Northwind 0.656, podcast 0.739, system-design guide 0.708; out: biryani 0.472, song 0.531, fee
      circular 0.552, BRIN article 0.547, OS lecture 0.632, Swiggy UPI 0.556. Capstone in: RRF article 0.600,
      review-schedule mail 0.603, GIN article 0.652; out: LeetCode 0.585, OS-project mail 0.614, laptop review 0.467,
      metro news 0.438, BCNF video 0.580, internship offer 0.570.
    - Not measured: nomic; granite@4,096; any real-data test (Thursday).
  - **Flags against decided items:** A11 (a 200-token description share means a transcript now reaches the vector for
    most videos; A11's "only when the description is short" changes); A23 / A24.2 say "keyword overlap and/or MiniLM
    cosine" for passage picking → prefer keyword overlap with the new model.

---

### A41. Stop tracking when emails are opened? — [x] decided 2026-10-21 (his ask, 2026-10-21)
- His view: opening time of a mail gains nothing. Nobody asks "when did I open this mail"; they ask about mail
  that arrived (A1.1 already filters mail by arrival).
- What it is today: `extension/content/gmail_tracker.js` reports each opened email + reading time →
  `POST /gmail/engagement` (`ingestion/gmail/router.py:106`) → `update_gmail_engagement`
  (`ste/storage_engine.py:662`) adds dwell, stamps first_opened_at / last_accessed_at.
- What else uses it (the cost of removing):
  - Wellbeing: `wba/time_aggregator.py` counts only OPENED emails, and `wba/wellbeing_analytics.py:75` builds
    sessions from opened emails → Gmail disappears from time charts, heatmap and sessions.
  - Reranker effort score (`rse/reranker.py:92`): Gmail dwell becomes 0 for every mail → read mails no longer
    rank above unread ones.
  - A39 timings: Gmail read minimum 3 s / idle 30 s / credit 60 s go away.
- Gain: less capture (one content script fewer on mail.google.com — a clean privacy line), less code to build
  and defend.
- Options (to brief): 1. remove fully. 2. keep for wellbeing + ranking only, drop from retrieval wording.
  3. keep as is.
- **Decision (2026-10-21): option b — keep it.** It sends only the email ID + seconds (`gmail_tracker.js:126`).
  After A1 it feeds only wellbeing (the only record of email time, since A9.1 skips mail.google.com in the Chrome
  tracker) and the effort score (A29). Open times are not used to filter retrieval (A1.1: Gmail by arrival).

### A42. Auto-accept confident group suggestions; ask only below a threshold — [x] decided 2026-10-21 (his ask, 2026-10-21)
- His idea: instead of the user accepting every suggestion, an item whose score is above a HIGH threshold is put
  into the user group automatically; below it (but above the suggest bar from A32) it stays `pending` for him.
- Today: every suggestion is `pending` (`group_suggestions.decision DEFAULT 'pending'`, `scripts/setup_db.py:338`);
  only `decide_suggestion` / `manual_assign` write `memory_user_groups` (`wba/group_manager.py`).
- Doc history: the architecture doc §15.3 step 5 originally did exactly this for ALL suggestions
  (`decision='auto_accepted'`, `reviewed=FALSE`, reviewed weekly; doc lines ~917/971/1129). DB design v5 changed
  the default to 'pending' (DB doc line 336: auto_accepted "silently breaks" human-in-the-loop). CLAUDE.md §2.4
  (hard constraint) and §3 (deviation) state the strict version — both must be updated if this is decided.
- To decide at the end: (1) which score auto-accepts (rule match? KNN ≥ a high bar? both?) and the value — set on
  Thursday's data after A40; (2) do auto-accepted items still show in the weekly review so he can undo them
  (`reviewed = FALSE`, as the doc had)? (3) wording for the panel: "human-in-the-loop for uncertain items,
  human-on-the-loop (weekly undo) for confident ones".
- Ties to A32 (suggest bar) and A43 (groups under system groups).
- **Decision (his, 2026-10-21; he accepted every pick of the A16/A40/A42/A43 brief):**
  - **A42.1 auto-accept only when a rule matches AND the KNN score >= the A32 KNN bar** (one shared constant, start
    0.65 with arctic-xs, set Thursday). Anything less certain stays `pending`.
  - **A42.2 auto-added items still show in the Sunday review** with Keep / Remove (UI + delete path already exist:
    `PatternsPage.tsx:199-214`, `group_manager.py:545-554`); row written `decision='auto_accepted'`, `reviewed=FALSE`.
  - **A42.3 only items he confirmed count as KNN examples** (`_load_group_context` keeps `decision='accepted'`).
  - **A42.4 panel wording:** "Echo never decides alone when it is unsure. If a rule you wrote AND the similarity to
    items you confirmed both agree, the item is added and shown in your Sunday review, where one click undoes it —
    human-on-the-loop. Anything less certain waits for your yes — human-in-the-loop. Only items you confirmed ever
    teach the system."
  - `setup_db.py` keeps `DEFAULT 'pending'`; only the confident path writes 'auto_accepted'. Entry is still only via
    `group_suggestions` (wba/group_manager.py stays the only write path). CLAUDE.md §2.4 + §3 updated 2026-10-21.
    A32's "every suggestion stays pending" now reads: except the A42.1 confident path. ~1.5 h.
  - **A42 build notes (from the brief; line numbers as of 2026-10-21):**
    - Why rule AND KNN: rule alone auto-adds everything a broad rule touches (e.g. the "Semester fee circular", KNN
      0.552, under a `pes.edu` rule); KNN alone needs ~0.70 to be safe and then catches only 2 of 7 real members
      (Capstone "in" and "out" overlapped). A group without rules works exactly as today (all pending). Auto-accept
      fires only for KNN-ready groups (≥ 5 confirmed members, A39), so a new group starts fully manual.
    - Why only confirmed items teach KNN: a wrong auto-add (an OS-project mail in "Capstone E.C.H.O") would become a
      "member", the next OS mail would match it, score high and be auto-added too — one mistake teaching the next.
    - `wba/group_manager.py`: new constant `AUTO_ACCEPT_KNN_THRESHOLD` (or reuse the A32 constant) near lines 59-65;
      at line 479, rule match AND knn ≥ bar → new `_insert_auto_accepted()` writing `group_suggestions`
      (`decision='auto_accepted'`, `reviewed=FALSE`, `decided_at=NOW()`) and `memory_user_groups` in one transaction;
      docstrings at lines 8-23 and 425 ("nothing is auto-accepted"). A42.3 filter: `_load_group_context` (lines 319-339)
      keeps `decision = 'accepted'` (manual adds already write `'accepted'`, line 600); same filter for the readiness
      count in `list_groups` (line 141) and `manual_assign` (line 619). `review_queue` (lines 488-508) already returns
      `decision`.
    - `scripts/setup_db.py` (~lines 322-341): keep `DEFAULT 'pending'`; update the comments at ~325-326 and 338; the
      setup check at ~526-535 stays valid. No schema change (`decision` is VARCHAR(20), no CHECK).
    - `backend/wellbeing.py`: docstring lines 21-22; `POST /groups/auto-assign` can return an `auto_accepted` count.
    - `frontend/src/pages/PatternsPage.tsx:170-222`: small "added automatically" label when
      `decision === 'auto_accepted'` (~15 min, frontend only).
    - Docs that stay yours to change: DB design doc (lines 331, 336-337) and architecture doc §15.3 step 5.
    - Threshold check on Thursday's data (arctic-xs start ~0.65).

- **Built 2026-10-25 (all four parts; tested on a throwaway PostgreSQL 16 with fake vectors, NOT on the live `echo_db`):**
  - `wba/group_manager.py`: in `_run_auto_assignment`, a rule match AND `knn_score >= KNN_ONLY_THRESHOLD` calls the new
    `_insert_auto_accepted` (one transaction: `group_suggestions` with `decision='auto_accepted'`, `reviewed=FALSE`,
    `decided_at=NOW()` + `memory_user_groups` + rule `match_count`). Anything else that was a suggestion before is still
    `pending`. `KNN_ONLY_THRESHOLD` is the one shared bar (A32 + A42.1). The run now returns `auto_accepted` too.
  - A42.3: `_CONFIRMED_MEMBER_SQL` leaves out members still marked `auto_accepted`. It is used by `_load_group_context`
    (KNN examples), `list_groups` (new `confirmed_count`; `auto_assignment_active` = `confirmed_count >= 5`) and
    `manual_assign`. `member_count` still counts every member. Keep (accept) makes an item confirmed; Remove (reject)
    deletes the membership (existing path). `manual_assign` on an auto-added item confirms it.
  - Frontend (`PatternsPage.tsx`): "added automatically" tag on `auto_accepted` rows, new panel wording (A42.4), "N more to
    auto-sort" counts confirmed members.
  - `AGENTS.md` constraint 4 and the `decision` default line now carry the A42 exception (they said strict-only before and
    would have made an agent refuse this build); `setup_db.py` comments updated, no schema change.
  - Checked: rule+KNN item auto-added and listed in the review; rule-only and KNN-only items stay `pending`; a group
    with fewer than 5 confirmed members never auto-adds; auto-added items are not KNN examples and do not count toward
    readiness; Keep raises both; Remove removes the member; a second run adds nothing twice.
  - **Still to do locally:** re-measure the KNN bar on the real rows (A40 start 0.65; also the "Thursday re-tune" in A32/A40),
    then set `KNN_ONLY_THRESHOLD`. Until then auto-adds use 0.65 on unmeasured data.
  - **Found while testing (fixed, same file):** `create_group` and `add_rule` ran `INSERT ... RETURNING` through
    `postgresql_manager.fetchone`, which never commits under SQLAlchemy 2.x, so the group or rule was reported as created
    but rolled back. Both now use `transaction()`. If creating a group or rule in the app ever seemed to vanish after a
    reload, this was why. Not checked on the live DB.

### A43. User groups as fine-grained children of system groups? — [x] decided 2026-10-21 (his ask, 2026-10-21)
- His view: user groups should be finer grains of the 5 system groups (work, study, entertainment, personal, misc).
- Today: NOT like that. `user_groups` has no link to `system_groups` (`scripts/setup_db.py:224`); group_manager
  never reads `system_group_id`. A user group can hold items of any system group.
- What it would take: a `system_group_id` column on `user_groups` (schema open since A31), the group form picks a
  parent, and auto-assignment only scores items whose system group = the parent (fewer wrong suggestions).
- Cost to weigh: a goal that spans two system groups ("Placement prep" = study pages + work mails) needs one
  parent; and an item misclassified at system level (ENP stage 1–4) can never reach the right user group.
  Possible middle: parent is optional (NULL = any system group).
- **Decision (his, 2026-10-21; he accepted every pick of the A16/A40/A42/A43 brief): keep user groups unlinked** from system
  groups. Measured: the hard wrong suggestions sit inside the same system group, so a parent filter buys little
  and can lose items ENP misclassified. No schema change.
  - Evidence: in "Capstone E.C.H.O" the near misses (OS-project mail 0.614, LeetCode page 0.585) are study items
    too, so a "study" parent would still suggest them; the items a parent would remove (metro news 0.438, laptop
    review 0.467) were already rejected by KNN. Assumption in the test: ENP puts the OS mail and LeetCode in study.
  - If revisited (optional parent or display label, ~2 h; required parent ~2.5 h): `ALTER TABLE user_groups ADD COLUMN
    system_group_id INTEGER NULL REFERENCES system_groups(system_group_id)` in `scripts/setup_db.py` (~line 224) AND
    as migration SQL (A31.3); `create_group` (`group_manager.py:105`), the items query (lines 454-467),
    `backend/wellbeing.py:107-109` `GroupCreateRequest`, `frontend/src/lib/types.ts:51`, `api.ts:155`,
    `PatternsPage.tsx:~254`. `manual_assign` must never check the parent.

### A44. Automatic retry of failed ENP items — [x] decided 2026-10-21 (found by the paper agent) — [x] built 2026-10-23
- Today: a row that fails any ENP step is marked `classified_by = 'failed'` and skipped by every later batch; it is
  retried only with `ENP_RETRY_FAILED=true` + a backend restart (`enp/enrichment_pipeline.py` 62, 102).
  `scripts/enp_run_once.py`, named in the module docstring, was deleted 2026-10-19 (A36) — fix that docstring.
- Draft 1/2/3 of the paper say failed items are "retried automatically" (Draft 2 line ~278).
- **Decision (his, 2026-10-21): build automatic retry, if time permits before the Sun 25 freeze.** Sketch: retry
  failed rows on worker start and then periodically, max 3 attempts per row (attempt counter: a column — schema open
  under A31.3 — or a Redis key), ~45 min.
- **Done 2026-10-23:** attempt counter in Redis (`echo:enp:retry:{memory_id}`, TTL 7d); helpers in `ste/redis_manager.py`
  (`get_enp_retry_count`, `increment_enp_retry_count`, `clear_enp_retry_count`, `MAX_ENP_RETRIES=3`).
  `retry_failed_items(conn)` in `enp/enrichment_pipeline.py` resets rows with `classified_by = NULL` when counter < 3;
  called at worker startup and every `DEFAULT_RETRY_INTERVAL_SECONDS=300` s during normal polling. On success,
  `mark_item_processed` clears the counter. No schema change (counter in Redis, not a new column).
  - **Fix 2026-10-23 (review):** Redis down no longer lifts the cap. `get_enp_retry_count` returns `MAX_ENP_RETRIES` on a Redis error (fail closed), `increment_enp_retry_count` raises, and `retry_failed_items` skips a row whose attempt it could not record. Checked on Neon with a throwaway failed row: read failure and increment failure each reset 0 rows; normal cycles reset on 1, 2, 3 and refuse the 4th. Left: the query takes the 50 oldest failed rows, so 50 or more capped rows would hide newer failed ones.

### A45. Fresh Gmail poll for "just now" questions in the A26 tool loop — [x] closed 2026-10-24; no build needed
- Today: `parse_intent` sets `fetch_api=True` for "latest / today / just received"; `node_fetch_api`
  (`rse/graph_nodes.py:281`) runs a live Gmail `poll_once` and loads today's items. A26 replaces the fixed graph with
  Luna's `search_memories` tool loop, and no decision placed this live poll in it. Without it a new mail appears
  after the next background poll (every 30 s, A5).
- **Decision updated 2026-10-24:** drop the `fresh` option. The existing 30-second poll delay is acceptable; this
  is a rare scenario and does not justify a separate query-time path. No implementation or paper change is needed.

### Build-status cross-check for decided A entries — 2026-10-24 (rows updated 2026-10-26)

The `[x] decided` marks above mean the owner's decision is recorded. They do not mean the feature was built. Static review of the current workspace found 11 decision IDs with implementation work (A7's prior open row was stale; A7.1–A7.4 are built as recorded above). Rows were refreshed on 2026-10-26: the ones still open are A18 (part 2), A40 and A42 (bar value only); A26 was built later that day; the rest are closed:

| ID | Current implementation check |
|----|-------------------------------|
| A16 | **Built and applied by B5 2026-10-25 (commit c719cdc):** `embedding_generator.fit_to_window` + `topic_extractor` ordered `TextPart`s (see the A16 entry). Closed once B5 has re-embedded with it. |
| A17 | **Closed 2026-10-25 (his decision):** the 29 hand-added domains stay; the review of real top domains is dropped (the test account has hardly any browsing). |
| A18 | **Part 1 built 2026-10-25 (commit dbaebee):** `SEED_TEXTS` are now 8 example sentences per category, averaged in `initialize_centroids`. **Part 2 still open:** the seed-specific Stage-3 threshold must be measured on the local `echo_db` (lowest score/lead with zero wrong) and wired into `stage3_centroid`; `CENTROID_MIN_SCORE` is still 0.55 for seeds. Steps are in the A18 entry. |
| A23 | **Closed 2026-10-26 (simplified by his decision):** no passage selection; leading attachment text, 40,000 chars per file and 80,000 total. See the A23 entry. |
| A24 | **Closed 2026-10-26 (simplified by his decision):** A24.3 full-text indexing was already built; A24.2 became leading excerpts, 8,000 chars for the top 3 results and 2,000 for the rest. See the A24 entry. |
| A26 | **Built 2026-10-26** (see the "Built 2026-10-26" block in the A26 entry): graph `agent ⇄ search → finish`, 22 offline tests, request/response checked with the real OpenAI client against a local fake endpoint. Not yet run against the live service. |
| A27 | **Built 2026-10-25, complete:** backend (`answer_markdown` / `answer_sources`), memory pills, moment strip, New chat, and the past-chats screen (`PastChats.tsx`, `GET /retrieval/sessions`, `GET /retrieval/sessions/{id}`). His decisions 2026-10-25: laptop only, so the touch "first tap = peek" is dropped; `GET /retrieval/sessions` needs no `X-API-Key`. See the A27 entry. |
| A28 | **Built 2026-10-25 and closed as it is (his decision):** the progress lines are built (`run_query_stream`); streaming the reply text word by word is not built and is future work. See the A28 entry. |
| A37 | **Built 2026-10-25 (commit b354647):** see the A37 entry. Closed. |
| A40 | **Model switch built 2026-10-25 (commit c719cdc):** arctic-xs, query/document paths, `EMBEDDING_VERSION = "snowflake-arctic-embed-xs-v1"`, refuse-to-load on a version mismatch. Still open: the threshold retune (Stage 9, needs an evaluation question set) and B5.  **2026-10-25 his choice C (no question set yet): the design's start values were set, not measured:** `MIN_TOP_SIMILARITY` 0.35 -> 0.55 (`rse/config.py`), `KNN_ONLY_THRESHOLD` 0.60 -> 0.65 (`wba/group_manager.py`); recomputed-centroid 0.55 / 0.08 left as designed; the seed-centroid threshold needs A18, not built. Sanity check on the real index (105 title questions, 10 made-up unrelated ones): right answers scored 0.641-0.84 (median 0.777), unrelated up to 0.607 (median 0.518); 0.35 blocked 0 of 10 unrelated, 0.55 blocked 8 of 10. The sample suggests ~0.62 would separate it, but it is small and the right-answer questions are title-based (optimistic): measure on a real question set (Stage 9). **Deferred by him 2026-10-25: the recompute of these values (Stage 9 measuring, and the A18 seed threshold) waits until after Sun 25 Oct. He retests everything on live data Mon 26 + Tue 27 Oct, and the values are recomputed from that data before the demo. Until then the option C start values stay. Open item.** |
| A42 | **Built 2026-10-25:** the confident path, accepted-only KNN examples and readiness count, and the review label are in. **Still open:** the KNN bar `KNN_ONLY_THRESHOLD` (0.65) is a start value and must be re-measured on the local data (shared with A32); see the A42 entry. |

## B. Live database actions — your explicit go + backup first (B1–B3 ran on Neon; B5 runs on local `echo_db`)

| ID | Action | Status |
|----|--------|--------|
| B1 | `DROP TABLE gmail_memory` (archive `legacy_gmail_threads.json` stays local) | [x] 2026-10-23 on Neon: `DROP TABLE gmail_memory CASCADE` (30 rows). Checked first: the `semantic`/`classification`/`interaction`/`analytics`/`content_summary` columns were empty in all 30 rows; title, body, thread, sender, time and has-attachments match `memory_items`/`gmail_metadata` for all 30 (row 28 sits under a different `memory_id`, matched by `source_id`); `source_link` = `https://mail.google.com/mail/u/0/#inbox/<source_item_id>` in all 30; `message_history` was filled in 10 rows = the 10 in `legacy_gmail_threads.json`; `content_attachments` held only filename/mime/size 0 (+ stale Gmail attachment ids in 3). After: table gone, `gmail_attachments` still 4, `memory_items` gmail 70, `gmail_metadata` 70. Backup: Neon branch `backup-pre-b1-gmail-memory` (`br-shy-unit-am2pkoub`; the snapshot limit was reached); its cleanup was not checked. Neon is frozen by current instructions, so no remote cleanup was attempted. The `varchar`/no-FK drift in `gmail_attachments.memory_id` was fixed on the local restore by A31.3; local read-only check 2026-10-24 confirms UUID + FK. Local `echo_db` rechecked read-only 2026-10-24: table absent. |
| B2 | Remove duplicate `gmail_attachments` rows — keep one per `(memory_id, filename)` | [x] 2026-10-23 no-op: Neon has 4 rows, 0 duplicate groups; local `echo_db` rechecked read-only 2026-10-24: 0 duplicate groups |
| B3 | Gmail rows wrongly marked opened: set `first_opened_at` and `last_accessed_at` to NULL where `play_sessions_count = 0` | [x] 2026-10-23 on Neon: 34 rows cleared, 0 left wrong, the 3 real opens untouched (snapshot `snap-small-bonus-aml6iy9e` taken first); local `echo_db` rechecked read-only 2026-10-24: 0 Gmail rows with `play_sessions_count = 0` and either open timestamp set |
| B4 | Shorts stored as `is_short = FALSE` — cannot be fixed from the DB; each fixes itself on a re-watch | [x] informational; no action required |
| B5 | Re-embed every row after the ENP review (cleaner, keywords and embeddable text changed; old vectors and
       auto_keywords — full of "the"/"and"/"for" — stay until re-processed). Set `preprocessed = FALSE` on all rows,
       run ENP. Do it ONCE, after all of enp/ is reviewed, A12–A15 are decided, and the A16/A40 implementations are complete. Rebuild FAISS from a fresh index so stale vector IDs are removed. | [x] 2026-10-25 on local `echo_db` (details below) |

- **B5 run 2026-10-25 on the real `echo_db`:** backups first (`echo_backups/echo_db_pre_B5_2026-10-25.dump`, old `data_faiss/` moved to `echo_backups/data_faiss_moved_aside_B5_2026-10-25/`). 111 live rows, 0 `user_override`, 0 `failed`. The first ENP run was stopped by Claude Code at 10 of 111 rows (low system memory), then resumed; because 10 finished rows were exactly the minimum for a `misc` centroid recompute, an empty `centroids.npz` stub was placed for the resumed run so it used seed centroids like the rehearsal, then deleted before the one planned restart. Results identical to the rehearsal: checks 1-6 pass (111 processed; FAISS = embedding_index = processed = 111; norms 1.0; version `snowflake-arctic-embed-xs-v1`; every text <= 512 tokens; no null `search_vector`; stored vectors = re-embed); groups moved for 17 of 111 items (same counts as below); full search chain: the three test items at rank 1, 68 of 76 unique-title items at rank 1; vector-only the YouTube item is rank 5 (bar changed to the full chain by his decision). Restart recomputed only `misc` (66 confirmed items); `centroids.npz` has that 1 centroid. Redis was not running (retry-counter warnings only). The 29 Gmail items without a `memory_engagement` row stay invisible to search (see below).
- **Rehearsal 2026-10-25 (scratch DB `echo_db_rehearsal` restored from `echo_backups/echo_db_pre_arctic_2026-10-25.dump`, temporary index folder; the real `echo_db` and `data_faiss/` were not touched, hashes equal the backup):** live items are **111** (not 109); embedding_index 111 active; the saved FAISS index has 112 ids (1 stale id). ENP re-embedded all 111 in 293 s (0 errors, 0 failed rows, OpenAI classifier calls worked). Checks: all rows processed; FAISS = embedding_index = processed = 111 with equal id sets; norms 1.0; versions all `snowflake-arctic-embed-xs-v1`; every stored text <= 512 tokens (max 512, mean 246); search_vector non-null; stored vectors equal a re-embed of the stored text. Groups moved for 17 of 111 items (misc 80 -> 78, entertainment 3 -> 7, study 8 -> 11, personal 10 -> 7, work 10 -> 8); `centroid` now decides 12 items (0 before, because arctic scores sit higher), `fallback` 6 -> 0. A title question found its item at rank 1 for 73 of 76 items with a unique title (top 3: 75); the one miss is a crowded topic (5 items about one product). A second ENP start recomputed only the `misc` centroid (66 confirmed items); the other four have < 10 and keep their seeds. **Full search chain (keyword + FAISS -> RRF -> cross-encoder, hand-made intent, no LLM; one variant, so conservative):** the three test items (Chrome, Gmail, YouTube) all rank 1; the vector search alone put the YouTube one at rank 5 (five items on one product). Across the 76 unique-title items: 68 rank 1 (89%), 69 in the top 3. **Found, not caused by this work (identical in the real `echo_db`): 29 of 72 Gmail items have no `memory_engagement` row**, and `search_coordinator.RESULT_JOINS` inner-joins that table, so those mails never reach the keyword branch or hydration (the 7 unique-title ones in the test were FAISS rank 1 and still absent from the final list). `ste/storage_engine.py:10` says one row per item. Decide separately: backfill the missing rows or LEFT JOIN.
  - **Backfilled 2026-10-25 on the real `echo_db` (his go):** after `echo_backups/echo_db_pre_engagement_backfill_2026-10-25.dump`, one transaction inserted `memory_id` only for the 29 live Gmail items without a row (all created 2026-02-12 to 2026-04-01); defaults gave dwell 0, sessions 0, no open times, the same as the 40 other never-opened mails. After: 0 missing, 111 rows for 111 live items; Gmail 72 rows (69 never opened, 3 opened, as before). Full-chain test after: the three test items at rank 1; 74 of 76 unique-title items at rank 1 (97%), all 76 in the top 3 (was 68 and 69). Cause of the gap: not verified; the dates point at the older ingestion code. `RESULT_JOINS` still inner-joins the table, so a future gap would hide items again.
  - **Stage 7 closed 2026-10-25 on his decision ("full chain"):** the rehearsal pass bar for "one test question per source finds its item at rank 1" is the real search chain, not the vector search alone. Result 3 of 3 at rank 1. The vector-only run is kept on record: the YouTube item ranked 5th behind four items on the same product (cosine 0.756 vs 0.785-0.814).
- **Read-only check 2026-10-24:** local `echo_db` has 109 live items, 0 unprocessed, and 109 active `embedding_index` rows; the FAISS metadata lists 110 IDs. B5 is still open: no full re-embed was run after the new embedding-text/schema work, and the FAISS/index counts differ. Take the required `pg_dump -Fc` first, preserve the current FAISS backup, then rebuild the index from the 109 live rows; simply re-embedding into the current index could leave its extra ID behind.

- Do B2 only after C1 is done, or new duplicates keep appearing.
- For each: count the affected rows first, show the count, then run.

---

## C. Fixes in other files (do when that file is reviewed)

### C1. `ingestion/gmail/database.py` — [x] 2026-10-15 (718fe9a) — B2 is now unblocked
- Delete `store_attachments_metadata` and its call. Until then every new email with attachments gets
  **2 rows** (one from `ste/storage_engine.py`, one from here).
- Delete the stubs `store_in_memory_items` and `store_in_gmail_metadata`.
- Replace `print()` with `logger`.

### C2. `ingestion/gmail/config.py` — [x] 2026-10-15 (718fe9a)
- [x] 2026-10-15 Follow-up after deleting `ingestion/gmail/main.py`: `get_redis_client()` has no callers left → delete it.
      Also remove the "main.run_listener" lines from the docstrings of `config.py`, `database.py`, `gmail_api.py`.
- It builds its own Redis client. Use `ste.redis_manager.get_sync_client` instead.

### C3. `is_source_enabled` on the event loop — gmail [x] 2026-10-15 · youtube [x] 2026-10-17
- `ingestion/youtube/youtube_connector.py` calls `settings_store.is_source_enabled` directly on the event
  loop. On a cache miss that is a sync Redis GET — up to 5 s blocked when Redis is down. Wrap it in
  `asyncio.to_thread`.
- `ingestion/gmail/router.py`: **was a false alarm** — the call sits inside `poll_once`, which already runs
  in `asyncio.to_thread`. Done there instead: `poll_once` now skips the fetch when `initialize_database()`
  returns False (it used to ignore the result).

### C4. FAISS staleness — RSE never sees new vectors until a restart — [x] 2026-10-23
- **Cause:** ENP builds its own `FAISSManager` instead of the shared `get_manager()`.
- **Order matters** (otherwise ENP's `add()` races unlocked reads → crash risk):
  1. [x] `rse/search_coordinator.py` `_search_full_index` → `manager.search_all`; drop `_cosine_similarity`
         (2026-10-18; parity-tested on the real 110-vector index: identical ids and scores).
  2. [x] `rse/time_anchor.py` (uses `_search_full_index`) → `get_manager().search_all` (2026-10-18).
  3. [x] `wba/group_manager.py` `_item_embedding` / `_knn_score` → `get_vector` + `cosine` (2026-10-18).
  4. [x] `enp/system_group_classifier.py` → `get_vector` (2026-10-17; also imports `ste.faiss_manager` directly).
  5. [x] 2026-10-23 `enp/enrichment_pipeline.py` → `get_manager()` (the `load_index()` call went with it; `get_manager()` loads once). Module imports; `compileall` silent. Live end-to-end run 2026-10-23 on Neon (real `run_pipeline` in a thread named `enp-worker`, real embedding model, FAISS index + `centroids.npz` in a temp copy, one synthetic chrome row, removed afterwards): 13/13 checks passed - the reader's `get_vector()` / `search_all()` saw the new vector right after ENP finished, with no reload; it ranked first for its own vector and in the top 3 for a natural-language query; `embedding_index` row written; the saved index file held it on a cold load; the worker stopped cleanly. Stage 4 got `401 Unauthorized` from OpenAI and fell back to `misc` (see chat).
  6. [x] 2026-10-23 `ste/faiss_manager.py`: `index` → `_fresh_index`, `memory_ids` → `_memory_ids`, `memory_id_to_offset` → `_memory_id_to_offset`, public `vectors` property deleted. A grep over all `.py` files finds no outside use. A public-API round trip (add, re-embed, save, reload, search, get_vector, empty search, wrong dimension) gave identical output before and after the rename.
  7. [x] 2026-10-22 shim `enp/faiss_manager.py` deleted; module-level `add/search/save_index/load_index` and `FAISSManager.search(candidate_ids)` removed (grep: no caller; `search_all` untouched; compileall clean). (2026-10-17: shim now exports only `get_manager`, its last use.) Delete the `enp/faiss_manager.py` shim and the dead module-level `add/search/save_index/load_index`
         + `FAISSManager.search(candidate_ids)`. Point `system_group_classifier` at `ste.faiss_manager`.
         (2026-10-19: `scripts/enp_recompute_centroids.py`, the shim's last caller, was deleted -> the shim has
         ZERO callers now; delete it.)

### C5. Chrome dwell is undercounted — `extension/background.js` — [x] 2026-10-23
- The page is sent once, when the intent gate passes (~5–10 s), then `sentToBackend = true`. Time after that
  is never sent. A 20-minute read is stored as ~10 s → wellbeing Chrome time is far too low.
- **Fix:** on tab close, send the extra dwell since the first send. `ste/storage_engine.py` already adds
  dwell on conflict, so the backend needs no change.
- **Done 2026-10-23:** `background.js`: added `sentDwellSeconds` to page state; set on first send; `sendExtraDwellToBackend` sends
  `dwellSeconds - sentDwellSeconds` to `/chrome/ingest` (which adds it via the ON CONFLICT clause);
  `finalizeChromePage` now branches: if already sent → call `sendExtraDwellToBackend`, else evaluate+send normally.
  Backend unchanged (ON CONFLICT already accumulates `dwell_time_seconds`).
  - **Fix 2026-10-23 (review):** the extra send now carries `revisit_count: 0`. The server ADDS `revisit_count` on conflict, so repeating the first send's value turned 1 into 2. Replayed through `/chrome/ingest` on Neon (first send 12 s, extra 600 s): dwell 612, `revisit_count` stays 1. An extra under 10 s with no scroll or interaction is still refused by the gate (at most ~10 s lost, accepted). Browser flow not run.

### C6. YouTube re-watch time not added — `ingestion/youtube/youtube_connector.py` — [x] no change (A2 = "how far", 2026-10-21)
- Heartbeats send the session's running total, so plain `+=` would double count.
- **Fix if A2 = "total":** take the stored value at detection as a baseline; store baseline + session total.

### C7. `backend/main.py` — [x] (main.py side 2026-10-19; leftovers 2026-10-22: dead `redis_client` in youtube_connector.py deleted, gmail/config.py comment and conversation_memory.py docstring fixed; D1 engine dispose is tracked separately)
- **Done in main.py:** all six bullets below. Startup now pings `redis_manager.get_async_client()` and closes it
  (`aclose`) on shutdown; ENP stops via `threading.Event` with a 60 s wait for its batch; cleanup runs once at startup.
  - **Original leftovers (fixed 2026-10-22):** the dead `redis_client = None` and its comment were deleted from
    `ingestion/youtube/youtube_connector.py`; the comments in `ingestion/gmail/config.py` and
    `rse/conversation_memory.py` were corrected.
- D1 (engine dispose): done 2026-10-22 — see D1.
- Remove the dead `ytc_module.redis_client`.
- Close `redis_manager`'s async client on shutdown.
- Replace the f-string inside `logger.warning` with `%s` args.
- `load_dotenv()` runs after the imports — move it before.
- ENP clean stop (added 2026-10-17): `run_pipeline` now takes `stop_event`. Create a `threading.Event`, pass it
  in `_run_enp_in_named_thread`, and `set()` it in the lifespan shutdown. Today the ENP thread never ends, so
  shutdown waits on asyncio's default executor (up to 300 s on 3.12+, and the interpreter's exit join).
- Conversation cleanup (added 2026-10-18): call `rse.conversation_memory.cleanup_old_sessions()` once at
  startup (in a background thread — it is a DB round trip), so message_store keeps only 30 days. Nothing calls
  it today.

### C8. `wba/regret_system.py` — [x] 2026-10-22 (verified: `_redis()` returns the shared client without PING)
- Verified: `_redis()` returns `get_sync_client()` directly; Redis availability is detected by each caller's first command.

### C9. Remove `gmail_memory` leftovers (standing goal) — [x] 2026-10-22 (no code mentions it; the Neon table is dropped separately in B1)
- [x] Legacy fallback in `enp/enrichment_pipeline.py` (2026-10-17).
- [x] `parse_message_history` in `enp/topic_extractor.py` + its now-unused imports (2026-10-17).
- [x] Delete `scripts/export_legacy_gmail_threads.py` (2026-10-19; the archive `legacy_gmail_threads.json`, 25 KB,
      31 Aug, already exists in the project root).
- **Done when:** `git grep -i gmail_memory -- '*.py' '*.ts' '*.tsx' '*.js' '*.jsx' '*.sql' '*.json' '*.toml' '*.txt' '*.yml' '*.yaml'`
  returns nothing (rewritten 2026-10-22: the old test, "only the `.gitignore` line", could never pass — there is no such line,
  and the design doc and this file must keep naming the table until B1 drops it).
- **Verified 2026-10-22:** that command returns no hits (exit 1), and no untracked file contains the name. The remaining
  mentions are docs only (`Echo_System_Design.md`, this file) and go with B1.

### C11. Small follow-ups from `enp/enrichment_pipeline.py` (2026-10-17) — [x] 2026-10-22
- [x] 2026-10-22 `ste/storage_engine.py` `store_youtube_detection` docstring names `enrichment_pipeline._awaiting_hydration`;
      that check is now SQL inside `fetch_unprocessed_items`. Fix the comment.
- [x] ~~`scripts/enp_run_once.py`: call `setup_logging()`~~ — moot: script deleted 2026-10-19.
- [x] 2026-10-22 (`snippet` field and every `snippet=` call removed from `text_cleaner.py`; `truncate()` stays, topic_extractor uses it) `CleanedContent.snippet` -> `item["content_snippet"]` in `enrichment_pipeline.py` is never read (topic_extractor
      cuts the text itself, now with `truncate`). Decided at topic_extractor's review: topic_extractor keeps the
      caps (one place for the token budget) -> drop `snippet` from text_cleaner + `content_snippet` from the pipeline.
- [x] 2026-10-22 (SQL not run against Postgres) `enp/enrichment_pipeline.py` `_fetch_thread_history`: add `AND mi.preprocessed = TRUE` — a failed earlier
      message has its ERROR TEXT in auto_keywords, which would become thread keywords of the reply.

### C12. `import sentence_transformers` takes 16–23 s on this machine — [x] investigated 2026-10-22
- Profiled a fresh process with `.venv\Scripts\python.exe -X importtime -c "import sentence_transformers"`:
  44.29 s wall time; the `sentence_transformers` cumulative import entry was 43.30 s. The largest self-time
  entry was `transformers` at 20.36 s; `torch` was 1.91 s self / 5.79 s cumulative. This points primarily to
  Transformers' import work, not PyTorch alone.
- The installed `sentence_transformers` 5.4.1 top-level `__init__.py` eagerly imports backend utilities,
  cross-encoder/trainer APIs, sparse-encoder APIs and utilities alongside `SentenceTransformer`. No safe narrow
  import path was identified: Python executes the package `__init__.py` even when importing its submodules.
  A lower-level Transformers rewrite would change embedding/model behavior and is not a cheap safe optimization.
- Re-measured 2026-10-22 (same command, second run): 18.7 s cumulative for `sentence_transformers` — back inside the
  16–23 s range of the heading. `transformers` is again the largest self-time entry (2.4 s self / 8.7 s cumulative);
  `torch` 0.9 s self / 3.7 s cumulative. So the 44 s run above was a slow (likely cold-cache) outlier; the
  "mostly Transformers, not PyTorch" reading holds in both runs.
- Defender exclusion test **not run**: `Get-MpPreference` requires Administrator on this machine and the current
  shell is not elevated. No security exclusions were changed. To test later, compare the same fresh-process profile
  with a temporary `.venv` exclusion from an elevated shell, then remove it. Backend warm before the demo remains
  the no-change workaround; model load itself is still about 0.5 s offline.

### C13. LLM calls have no timeout — `rse/config.py` (+ synthesizer, parse_intent) — [x] 2026-10-22 (`LLM_CONFIG["timeout"] = 60`, passed by `rse/llm_factory.py`, which all 4 callers use; checked on ChatOpenAI: timeout 60.0. ChatAnthropic and ChatGoogleGenerativeAI accept `timeout` (fields checked); ChatOllama is not installed, so unchecked)
- Found 2026-10-17 in `enp/system_group_classifier.py`. Stage 4 now uses LLM_CONFIG's `max_retries` (fail fast),
  but no call sets a timeout. Likely (openai client default, not checked): 600 s per attempt, so a hung request
  can stall the ENP thread for many minutes, and a /query for the same.
- Fix: add `"timeout": <seconds>` to LLM_CONFIG and pass it in every `init_chat_model` (Stage 4, parse_intent,
  synthesizer). Check each provider class accepts `timeout` (openai / anthropic / google_genai / ollama).

### C14. MiniLM vectors ARE unit-length — the "UNNORMALIZED" notes are wrong — [x] 2026-10-22
- Found 2026-10-17. The loaded model is Transformer -> Pooling -> **Normalize**; all 110 vectors in the saved FAISS
  index have norm 1.0. (`normalize_embeddings=False` only skips a SECOND normalization.)
- Nothing is broken: code that computes true cosine is still correct; for unit vectors L2 order == cosine order,
  so that extra cosine work is simply unnecessary.
- **Also fixed 2026-10-22:** `AGENTS.md` (FAISS bullet) still said "unnormalized"; now matches CLAUDE.md §5 (unit-length).
- Fix the wording in: project `CLAUDE.md` §5 ("FAISS is IndexFlatL2 over UNNORMALIZED MiniLM vectors" — needs your
  OK, it is the project instructions file), `ste/faiss_manager.py` docstrings, `rse/search_coordinator.py` comments.
  `enp/embedding_generator.py` already corrected.
- 2026-10-21: CLAUDE.md §5 wording fixed (his go: "fix it"). Still open: `ste/faiss_manager.py` docstrings + `rse/search_coordinator.py` comments.
- 2026-10-22: done — `ste/faiss_manager.py` (module + class docstring, `search_all`) and `enp/system_group_classifier.py` `stage3_centroid` now say "unit-length". `rse/search_coordinator.py` needed no change (a search for "unnormali" in ste/, enp/, rse/, wba/, backend/ finds nothing).

### C15. Tier-1 attachment info never reaches search — `enp/enrichment_pipeline.py` + `enp/topic_extractor.py` — [x] 2026-10-23
- Found 2026-10-17 while reviewing `enp/attachment_processor.py`. Doc §5.4 Tier 1: "Metadata in gmail_attachments
  table, lightweight_extract text, **embedding in FAISS**". Nothing outside storage reads `gmail_attachments`:
  filenames are in neither the embedding nor the keyword search, and `is_processed` is never set TRUE.
- Effect: "the email with the offer letter PDF" is found only if the subject/body mention it.
- Fix: in `_fetch_gmail_context` also read the attachment filenames; in `build_embeddable_text` (gmail) add them as a
  short hint (before the body, per A16); set `gmail_attachments.is_processed = TRUE` when marked processed. Then
  re-embed (B5). Optional: add filenames to the RSE keyword search.
- **Done 2026-10-23:** `_fetch_gmail_context` now queries `gmail_attachments` in the same cursor block and stores
  filenames in `item["attachment_filenames"]`; `build_embeddable_text` (gmail) inserts the filenames as a short hint
  between sender_domain and body; `gmail_keywords` includes attachment filenames when computing auto-keywords;
  `mark_item_processed` now runs `UPDATE gmail_attachments SET is_processed = TRUE WHERE memory_id = %s` for gmail
  items and calls `clear_enp_retry_count`. B5 re-embed picks up the change for existing emails.
  - **Fix 2026-10-23 (review):** keywords now use the filename words (`_filename_words` in `enp/topic_extractor.py`): extension dropped, `_ - .` turned into spaces, so `Offer_Letter_2026.pdf` gives `offer`, `letter` (was `offerletter2026`, `pdf`). The embedded hint keeps the raw filename. Checked on 8 name shapes and on the real mail with `thor.jpeg`/`thor1.jpeg`.

### C16. `enp/centroids.npz` is not git-ignored — `.gitignore` — [x] (2026-10-17)
- Found 2026-10-17 reviewing `enp/faiss_manager.py`. recompute_centroids saves `enp/centroids.npz` next to the
  FAISS index. `.gitignore` covers `*.index` and `*.index.meta.json` only, so a recompute followed by `git add -A`
  would commit vectors derived from your personal data.
- Fix: add `enp/centroids.npz` (or `*.npz`) to `.gitignore`. Longer term: keep all runtime data out of the source
  folder (e.g. a `data/` dir), both index and centroids.
- **Done 2026-10-17 (his request):** index, meta.json and centroids now live in `data_faiss/` at the repo root
  (`ste.faiss_manager.DEFAULT_INDEX_PATH`), and `data_faiss/` is git-ignored. Existing files moved byte-identical
  (110 vectors load). `scripts/enp_status.py` now imports the path; `scripts/enp_rebuild_faiss.py` docstring fixed.
- Left (small): `enp/system_group_classifier.py` `_centroids_path()` uses the DEFAULT folder even when
  `ECHO_FAISS_INDEX_PATH` points elsewhere — use the manager's `index_path.parent` instead.
- **Left item done 2026-10-22:** `_centroids_path()` is now `get_manager().index_path.parent / "centroids.npz"`; the unused `DEFAULT_INDEX_PATH` import is gone. Note: the path call now loads the FAISS index first, so a broken index raises there.

### C10. YouTube `seeked` may save every video at 0 s — `extension/content/youtube_tracker.js` — [x] 2026-10-23 closed by code reading; live check moved to the testing phase (no code change)
- Found 2026-10-17 while reviewing `youtube_connector.py`. Not verified live.
- The tracker treats a `seeked` event as a manual interaction, but only after a real click or key press (see the code check below).
- Likely (not tested): YouTube seeks by itself when it resumes a half-watched video or opens a `&t=` link.
  If so, every such video is saved at 0 s, without real intent.
- **Check:** set `DEBUG = true`, open a resumed video, see if "intent fired seek" appears with no click.
- **Fix if true:** count a seek only after a user input event (click/keydown on the player).
- **Code check 2026-10-23:** the handler already returns unless `pointerdown` or `keydown` occurred within 1.5 s, so an automatic seek without recent input cannot fire intent. The requested live reproduction remains unverified; the browser session would not allow reloading the unpacked extension for DEBUG logging.
- **Decision (his, 2026-10-23): no live test, leave as is.** One edge case is accepted: a resume-seek within 1.5 s of the user's own click on play would count as a user seek and save the video at about 0 s. Not seen, harmless for this project.

### C17. `scope_level` is a dead intent field — `rse/query_parser.py` — [x] (found and done 2026-10-18; removed from prompt, examples, builders and `ParsedIntent`)
- The parser prompt, examples, `_fallback_intent` and `_normalise_intent` always set `scope_level = 0`; nothing
  reads it and widen_scope never changes it (widening is tracked by `attempt_count`).
- Fix when reviewing query_parser.py: drop it from the prompt/examples/builders, then delete the field from
  `ParsedIntent` in `rse/state.py` (state.py now documents it as unused).

### C18. Unused LLM-memory packages — `requirements.txt` + CLAUDE.md §3 — [x] 2026-10-22 (found 2026-10-18, conversation_memory review)
- `rse/conversation_memory.py` no longer uses LangChain's deprecated `PostgresChatMessageHistory`; it reads and
  writes `message_store` over the RSE psycopg2 pool (same table, same LangChain message JSON).
- Nothing else imports `langchain_community` or `psycopg` (v3) → remove `langchain-community==0.4.1` and
  `psycopg[binary]==3.3.3` from requirements.txt (check `pip check` after uninstalling).
- Add to CLAUDE.md §3 (deviations): doc §10.7 names PostgresChatMessageHistory; we keep its table format but
  not the class.
- **Done 2026-10-22:** both pins removed from `requirements.txt`; no `.py` file imports `langchain_community` or psycopg v3 (grep). CLAUDE.md §3 bullet added. Left: `pip uninstall` of the two in `.venv` + `pip check` (installed copies remain).

### C19. `POSTGRES_RESULT_LIMIT` is now unused — `rse/config.py` — [x] 2026-10-22 (constant and comment deleted; no `.py` file references it) (found 2026-10-18, search_coordinator review)
- The keyword branch no longer has a "most recent 1000 items" fallback (it fed RRF a recency-ranked list —
  recency boosting, a prohibited feature). Delete the constant and its comment from config.py.

### C20. Local-date vs UTC `created_at` in the other time checks — `rse/fusion.py`, `rse/graph_nodes.py` — [x] (found 2026-10-18)
- `created_at` is naive UTC (`ste/storage_engine._safe_datetime`); parser dates are the user's local dates.
  search_coordinator now converts with `local_to_utc_naive()`. Do the same where `time_filter` is compared in
  Python: `fusion.py` (semantic hits vs time_filter, ~line 57) and `graph_nodes.py` evaluate_quality (~line 127).
  Without it, in IST, items from local midnight to 05:30 are treated as the previous day.
- [x] `fusion.py` done 2026-10-18. [x] `graph_nodes.py` evaluate_quality done 2026-10-18.

### C21. One shared LLM factory instead of four copies — `rse/llm_factory.py` — [x] 2026-10-22 (his OK 2026-10-18; used by query_parser + llm_synthesizer + wba/insight_generator (2026-10-18); ENP Stage 4 switched 2026-10-22, with a fix: Stage 4 read `.content`, which is a block list under the Responses API, so every reply would have fallen back to 'misc' — it now reads `.text`; factory docstring caller list updated)
- The "OpenAI + reasoning_effort → reasoning + Responses API, else temperature" builder now exists in
  `rse/query_parser.py` and `rse/llm_synthesizer.py`; `enp/system_group_classifier.py` and
  `wba/insight_generator.py` still build their own (temperature only, no reasoning/Responses wiring).
- Proposal: `rse/llm_factory.py` with `build_chat_model(model_key, temperature_key) -> chat model` (cached per
  key), used by all four. New file → needs his OK (structure rule). Otherwise copy the same block into the
  ENP/WBA callers when they are reviewed.

### C22. `regret_events.marked_at` is LOCAL time, everything else naive UTC — `wba/regret_system.py` — [x] closed 2026-10-24 under A31.4 (keep `marked_at` local; UTC for other database timestamps)
- `toggle_regret` inserts `marked_at = datetime.now()` (local, IST); the column default and every other
  timestamp is `NOW()` = UTC on Neon. `wba/time_aggregator.activity_heatmap` therefore compares `marked_at`
  with local days directly (commented there).
- **Resolution under A31.4:** keep `marked_at`, `regret_hour` and `regret_day_of_week` local and document them as
  the local-time exception. Other database timestamps are UTC; the connection timezone is forced to UTC. No row
  repair is needed for this choice.

### C23. Shared WBA helpers are imported by their private names — `wba/time_aggregator.py`, `wba/regret_system.py` — [x] 2026-10-22 (4 names now public: `utc_offset_seconds`, `CURRENTLY_REGRETTED_CTE`, `MARK_EVENTS_CTE`, `standing_marks_sql`; the first three renamed by him, `standing_marks_sql` + `insight_generator.py` import by Claude; checked in 3 import orders) (found 2026-10-18, wba/pattern_detector.py review)
- `pattern_detector` imports `time_aggregator._utc_offset_seconds` and `regret_system._CURRENTLY_REGRETTED_CTE` /
  `_MARK_EVENTS_CTE` (one source of truth instead of copies). Rename them public (`utc_offset_seconds`,
  `CURRENTLY_REGRETTED_CTE`, `MARK_EVENTS_CTE`) and update the imports. Pure rename, no behaviour change.
- Also `pattern_detector._standing_marks_sql` (used by `wba/insight_generator.py` for the weekly regret counts).

### C24. Record the session-gap deviation in CLAUDE.md §3 — [x] (2026-10-18; CLAUDE.md is git-ignored, so the bullet is local only) (his decision 2026-10-18, wba/wellbeing_analytics.py review)
- `SESSION_GAP_MINUTES` 5 → 20 (doc §11.5 says 5). Reason: an item is one point in time (last access), so a
  15-minute read before the next page looked like a 15-minute gap and split one sitting into two sessions.
- Add one bullet to CLAUDE.md §3 when the wba/ commit goes in (architecture doc stays untouched).
- Not affected: `rse/neighborhood.py`'s ±5 min window (a different idea: what else was open around a result).

### C25. DB error messages carry bound values into the logs — `ste/postgresql_manager.py` — [x] 2026-10-22 (`hide_parameters=True` added to `create_engine`) (found 2026-10-19, backend/wellbeing.py review)
- SQLAlchemy puts the statement's parameters in the exception text ("[parameters: {'note': ...}]"). Every route's
  `logger.exception` prints that text, so a failed regret toggle would log the user's note; a failed insert, a title.
- Fix: `create_engine(..., hide_parameters=True)` in `get_engine()`. One line; errors then show the SQL only.
- psycopg2 (rse/) errors have no parameter dump; their DETAIL line can show key values (UUIDs) — fine.

### C26. Capture settings are logged in full — `ste/settings_store.py` — [x] 2026-10-22 (both log lines now show the three toggles + list lengths) (found 2026-10-19, backend/items.py review)
- `save_settings` ("Capture settings saved ... : %s", merged) and `_current_settings` ("Capture settings loaded: %s")
  log the whole dict at INFO, including `excluded_senders` — other people's email addresses — and excluded domains.
- Fix: log the three toggles plus the list LENGTHS only (e.g. `excluded_senders=3`).

---

### C27. Stale pointers to the scripts deleted on 2026-10-19 — [x] 2026-10-22: all code pointers fixed, plus CLAUDE.md §4 and `AGENTS.md:74` reworded (see "Done 2026-10-22" below); README and the LLD generator fixed too (found 2026-10-19, scripts/ review)
- Deleted by his decision: `redis-py.py`, `test_gmail_auth.py`, `test_youtube_auth.py`, `enp_run_once.py`,
  `enp_reprocess.py`, `enp_rebuild_faiss.py`, `enp_recompute_centroids.py`, `export_legacy_gmail_threads.py`.
- Text that still names them (fix in the same pass as A36, since A36 decides what the messages should say):
  - `ste/faiss_manager.py` line 20 (docstring) and the 4 error/log messages at ~399, 417, 430, 443:
    "Rebuild with scripts/enp_rebuild_faiss.py".
  - `enp/system_group_classifier.py` lines 23 and 530 (docstrings: "run monthly via scripts/enp_recompute_centroids.py").
  - `enp/faiss_manager.py` line 8 ("One caller is left: scripts/enp_recompute_centroids.py") -> see C4 step 7.
  - `enp/enrichment_pipeline.py` line 20 and `enp/embedding_generator.py` line 9.
  - [x] `scripts/enp_status.py` hints fixed 2026-10-19 (his go): drift hint prints the reset SQL (A36 option 2),
    retry hint says "restart the backend with ENP_RETRY_FAILED=true" (both work today, whatever A36 decides); also compares FAISS vs embedding_index
    counts and shows "n/a" when there is no index file. Keeps `print` (standalone script).
  - [x] `ste/security.py` callers line (fixed 2026-10-19 with the YouTube sign-in change).
- CLAUDE.md section 4 (local only) lists "monthly centroid recomputation (scripts/enp_recompute_centroids.py)" as built.
- **Done 2026-10-22:** `ste/faiss_manager.py` — the 4 "rebuild" messages now share one `_REBUILD_HINT` (delete `data_faiss/`
  files, `UPDATE memory_items SET preprocessed = FALSE`, restart; failed rows also need `ENP_RETRY_FAILED=true`) and the
  dead script row is gone from the callers list; `enp/enrichment_pipeline.py` retry hint; `enp/embedding_generator.py`
  callers list; `enp/system_group_classifier.py` (2 places; reworded again once A36 was built: the docstrings now say
  the ENP worker runs the recompute at start); `enp/faiss_manager.py` (no caller left, delete per C4 step 7).
- **Also done 2026-10-22:** CLAUDE.md §4 and `AGENTS.md:74` first said `recompute_centroids()` had no caller yet; reworded
  after A36 was built (they now say ENP runs it at startup when `centroids.npz` is missing or older than 30 days).
- **Also done 2026-10-22:** `README.md:70` no longer lists the deleted "rebuild FAISS, recompute centroids" scripts (now `setup_db.py`, `enp_status.py`, the two files left in `scripts/`).
- **Also done 2026-10-22:** `docs/Phase III/build/lld/build_lld.py:589` (the LLD generator) now names `recompute_centroids()` run at ENP startup instead of the deleted script. The already-built `.docx` is unchanged until the generator is re-run.

### C28. Gmail sign-in can hang backend shutdown — `ingestion/gmail/router.py` + `backend/main.py` — [x] built with A35, found already done 2026-10-24 (found 2026-10-19, YouTube sign-in change)
- `authenticate_gmail()` opens the browser sign-in inside an `asyncio.to_thread` worker of the gmail-poller.
  Cancelling the poller task does not stop that thread. Likely (Python's executor joins its workers at
  interpreter exit): stopping the backend while the Gmail sign-in tab is still open hangs until the sign-in
  finishes or the process is killed. Not tested live.
- **Fix (same as YouTube):** do the Gmail sign-in once at startup on a daemon thread; the poller then only uses
  the token, never opens the browser. Touches `gmail_api.py`, `router.py`, `main.py`.
- **Done (checked 2026-10-24):** `gmail_api.sign_in_gmail()` runs once on a daemon thread started in the `backend/main.py`
  lifespan (`gmail-sign-in`); `authenticate_gmail()` never opens a browser and raises `GmailAuthError`, which
  `router.poll_once` turns into a skipped poll. The tracker entry was simply never ticked.

### C29. Delete `docs/review/pending_answers_A16_A40_A42_A43.md` — [x] 2026-10-22 (his ask; not in git, so not recoverable)
- Done: the four decision lines (A16, A40, A42, A43) were reworded without the path, and the brief's build notes,
  measurements, re-tune table and flags were copied into A16, A40, A42 and A43 before the file was deleted.
- 2026-10-22: `docs/Timeless_docs/Echo_System_Design.md` line 12 reworded to "measured briefs for A16/A40/A42/A43 are
  folded into their entries in `ste_open_items.md`" (his standing permission to edit the architecture doc).

## D. Missing-10% — build after the review

### D1. Dispose the DB pool on shutdown — [x] 2026-10-22 (`postgresql_manager.dispose_engine()`, called in the `backend/main.py` lifespan after ENP stops; idempotent, engine is re-created on next use; the RSE psycopg2 pool in `rse/search_coordinator.py` is NOT closed — not part of this item)
- Backend shutdown never calls `engine.dispose()`; pooled connections wait for Neon's idle timeout.

### D2. Full attachment support — [x] 2026-10-26 complete (OCR built 2026-10-25, A4 merge found built, old `.doc` dropped by his decision)
- **Works today:** PDF (pypdf → pdfplumber), DOCX (python-docx), `.txt` / `.md` / `.csv`.
- **To build:**
  - [x] OCR for images (PNG/JPG/…): **built and checked with the real program 2026-10-25 (committed)**.
        `pytesseract==0.3.13` in `.venv` and `requirements.txt`; `_ocr_ready()` in `enp/attachment_processor.py` checks the
        Python package AND the Tesseract program once per process (finds it from `TESSERACT_CMD`, the PATH, the default
        Windows folders, then `~\tools\tesseract\tesseract.exe`). Tesseract 5.4.0 (UB-Mannheim build, English + osd) was
        installed WITHOUT administrator rights: `winget install` waited on a Windows permission prompt (UAC) and was then
        killed for low memory, so the setup exe winget had already downloaded was unpacked with 7-Zip into
        `C:\Users\mdshe\tools\tesseract` (the installer was never run; `tesseract --version` and `--list-langs` work).
        Real OCR checked: a rendered PNG (`HELLO OCR 12345`), a scanned 1-page PDF, a scanned 2-page A4 invoice at 10 pt
        (invoice number, amount `1,250.00` and an email address all read exactly) and a blank scanned page (returns None).
        The backend must be restarted once so it notices the program (the check is cached). Only English is installed.
        The 4 real attachments are 0-byte JPEGs, so they give no text either way.
  - [x] Scanned PDFs (no text layer): **built and checked 2026-10-25**. A PDF with no text layer is rendered with
        `pypdfium2` at 200 dpi and read page by page, at most `OCR_PDF_MAX_PAGES` = 10 pages, stopping at the char cap,
        60 s per page. 20 checks pass (17 with a stand-in OCR call: page order, A4 render size 1654 x 2339, the 10-page
        limit, the char cap, no readable text, an OCR crash, a 0-byte PDF, garbage bytes, a 0-byte JPEG, garbage as PNG,
        a text PDF still read by pypdf; plus the 3 real-program checks above).
  - [x] Old `.doc` files (python-docx cannot read them): **dropped 2026-10-26 by his decision, will not be handled.** The same goes for old `.xls` / `.ppt`.
  - [x] PPTX, XLSX, EML built 2026-10-24 (cloud session) in `enp/attachment_processor.py`; HTML was already there.
        (ODT/RTF were built first, then REMOVED the same day on his word: A22 says skip them. `.rtf` / `text/rtf` is now
        refused explicitly so it is not read as plain text.)
        Standard library only (zip + regex, per-member 16 MB and per-file 64 MB read caps, no XML parser), so
        `requirements.txt` does not change. Checked on generated files (slide order, notes, shared strings, inline
        strings, sheet names, EML plain vs HTML),
        on corrupt input (returns None) and on an oversized member (skipped). NOT checked on real Office files from
        a real mailbox; do that locally. Open then: old `.doc`/`.xls`/`.ppt` (dropped 2026-10-26), OCR, scanned PDFs (OCR built 2026-10-25, see D2).
  - [x] A4 decided 2026-10-21: extract at ingestion into `extracted_text`, merge into embedded text (shared
        800-char budget) + full text into the tsvector, set `is_processed = TRUE`. **Found built on 2026-10-26 (the box was stale):**
        `_load_attachment_texts` in `enp/enrichment_pipeline.py` reads `gmail_attachments.extracted_text` or extracts and stores it
        (`extract_full_text`); `topic_extractor._attachment_parts` puts the attachment text after the body in the embedded text
        (A16's 210-token pool split evenly replaced the 800-char budget); `_SEARCH_VECTOR_EXPRESSION` adds it to `search_vector`;
        `mark_item_processed` sets `gmail_attachments.is_processed = TRUE`. Not checked on a real attachment with text: the 4 real
        attachments are 0-byte JPEGs, so B5 re-embedded the mails without any attachment text.

### D4. Sessions are never built — [x] 2026-10-23 (found 2026-10-18, wba/wellbeing_analytics.py review)
- **Built 2026-10-23:** `ensure_sessions_for_range` / `ensure_sessions_for_day` in `wba/wellbeing_analytics.py`. Today is rebuilt
  on every call; a past day once per process (`_past_days_built`), future days skipped. Called by `daily_summary`,
  `GET /wellbeing/sessions` (`backend/wellbeing.py`) and `collect_weekly_aggregates` (`wba/insight_generator.py`).
  `POST /sessions/rebuild` stays as a manual rebuild. `DELETE FROM sessions` was a no-op: Neon had 0 `sessions` and 0
  `session_memory_map` rows (counted first, so no snapshot). Live check on Neon, 14 days: stored sessions and items per
  day equal an independent SQL window-function count (2026-10-16: 10 items, 1 session; other days 0); second call
  identical; future day, reversed range and empty day are no-ops. Gap rule checked on synthetic times (0/15/40 -> 2+1;
  exactly 20 min stays together; 21 min splits). Left: `docs/Phase III/build/lld/build_lld.py:1007` still says sessions
  appear only after a rebuild (generator text, `.docx` unchanged).
- Only `POST /wellbeing/sessions/rebuild` builds sessions, and nothing calls it (no frontend call, no scheduler).
  So `sessions` stays empty: fragmentation, focus score and session patterns (insights) show nothing.
- Options: rebuild today's sessions inside `GET /sessions` / `daily_summary` (cheap: one day of items), and past
  days once when first asked; or a background task in backend/main.py (C7). Before first use, clear any rows
  built with the old UTC-day logic (`DELETE FROM sessions`, his go).

### D5. No route to pause/resume a group rule — [x] 2026-10-25 route + rules screen built (found 2026-10-18, wba/group_manager.py review)
- `group_manager.set_rule_active` exists (DB Table 14: is_active pauses a rule, keeps match history) but no
  backend route or UI calls it. Also not surfaced: rules with match_count = 0 after 30 days ("dead rules").
- Build: `PATCH /wellbeing/rules/{rule_id}` {active} + a toggle in the group rules list; or drop from scope.
- **Built 2026-10-22:** `PATCH /wellbeing/groups/{group_id}/rules/{rule_id}` body `{"active": bool}`; 404 when the rule is not in that group. `set_rule_active(group_id, rule_id, active)` now also checks the group (its only caller is this route). Route is registered (router routes listed). SQL NOT run against Postgres. Left: the toggle in the UI; surfacing dead rules (match_count = 0 after 30 days).
- **UI built 2026-10-25:** a rules screen at `/groups/:groupId/rules` (`frontend/src/pages/GroupRulesPage.tsx`), reached from the
  "rules →" link on each goal group in Patterns. It lists the group's rules, paused ones included
  (`GET /wellbeing/groups/{id}/rules?include_inactive=true`), with a switch per rule that calls the PATCH route. The switch
  changes at once and goes back, with a short message, if the save fails (`setRuleActive` in `lib/api.ts` does not fall back to
  demo data, so a failed save never looks saved). Dead rules: an active rule with `match_count = 0` and older than 30 days shows
  "no matches in 30+ days" (worked out in the page from `created_at`; no backend change). Pausing matches the backend: the matcher
  loads active rules only. Checked in a browser with demo data and with a mocked backend (toggle, failed save, revert);
  NOT yet tried against the live backend and database. Not built: adding a rule from the UI (the POST route exists) and showing
  the step-9 rule suggestions.

### D3. Regret reminders are never shown — [x] 2026-10-25 built in the dashboard (found 2026-10-18, wba/regret_system.py review)
- `GET /wellbeing/regret/reminder` works (rate limits, cooldown, user's own note) but NO client calls it: the
  extension and frontend only use the enable/disable setting. Nothing reports `session_minutes` either.
- The payload offers "Got it / Snooze for today / Disable permanently"; only Disable has a backend route
  (`POST /regret/reminders/settings`). Snooze-for-today needs a route (e.g. set the per-type day key).
- Build: extension polls the route during a long Chrome/YouTube session and shows a non-blocking notice; or
  drop reminders from the final scope and say so (§11.6 marks them optional).
- **Built 2026-10-25, in the dashboard (not the extension):**
  - Backend: `POST /wellbeing/regret/reminders/snooze` (`snooze_reminders_today` in `wba/regret_system.py`) fills today's total
    counter to its cap, the key `maybe_get_reminder` already checks, so nothing else changed. It stops all types for the day,
    because the per-type cap is 1 and the reminder just shown already used it. 503 when Redis is down. Checked against a real
    Redis: after a snooze both a new type and the same type return nothing.
  - Frontend: `components/RegretReminderNotice.tsx`, mounted once in `AppShell`. While the dashboard is open it checks every 2
    minutes: is today's latest stored session still going (last activity within the 20-minute gap) and at least 15 minutes
    long? If so it takes the source type of the newest item and asks `GET /wellbeing/regret/reminder`; the backend still applies
    every other rule (habit, cooldown, limits, disabled). A returned reminder shows as a card in the corner with the user's own
    note: "Got it", "Snooze for today", "Turn off reminders" (the last also switches the Settings toggle off). It never blocks or
    takes focus, and shows nothing in demo mode (a reminder is always the user's own note, never a made-up one).
  - **Limit, on purpose:** it only appears while an Echo dashboard tab is open. The extension was not changed, so a reminder
    does not pop up on the YouTube/Chrome page itself. That would need a notification or an in-page banner from the extension
    (`background.js` / `postToBackend`, with the pause flag respected).
  - The session length comes from stored sessions (dwell sums), not a live timer, so it can trail real time by a few minutes.
  - Checked in a browser with a mocked backend (card appears, snooze sends the call and closes it); NOT yet tried against the live
    backend and database with a real regret habit.

### D6. Frontend gaps found after the build — [ ] open (found 2026-10-26; all four are frontend-only changes)
Found while reviewing what the screens can actually do. Each one was checked on 2026-10-26: the backend route exists, and nothing in
`frontend/src` calls it (also searched every remote branch for `addRule`, `assignToGroup`, `/assign` and `deleteSession`: no hits).
- [ ] **D6.1 Old chats show plain text, no memory pills.** `save_turn` (`rse/conversation_memory.py`) stores only `final_answer` in
  `message_store`, and `GET /retrieval/sessions/{id}` returns `{role, text}`, so `toChatMessages` in `frontend/src/pages/RecallPage.tsx`
  rebuilds an old reply with `sources: []`. Bold text and lists still render (the text is markdown); only the pills and the moment strip
  are missing. To fix: also save `answer_markdown` + `answer_sources` with the reply (the `message` column is JSONB, so no schema change)
  and return them from `/sessions/{id}`. This one needs a small backend change as well as the frontend.
- [ ] **D6.2 Deleting a chat from the Past chats panel.** The route `DELETE /retrieval/session/{id}` exists but needs `X-API-Key`, and
  the key must not ship in the browser bundle (same reasoning as the A27 decision that opened `GET /retrieval/sessions`). So: open a
  delete route the way `/sessions/{id}` was opened, then add the button and a confirm step to `PastChats.tsx` and a `deleteSession`
  in `lib/api.ts`. Decide first whether to open the route (his call, it deletes data).
- [ ] **D6.3 Adding a rule to a group.** `POST /wellbeing/groups/{id}/rules` exists (types keyword, domain, channel, sender,
  time_window; 400 on a bad value) but no frontend code calls it. Add a form on the rules screen (`GroupRulesPage.tsx`) and an
  `addRule` in `lib/api.ts` that does not fall back to demo data (same rule as `setRuleActive`). Also still not shown on that screen:
  the step-9 rule suggestions (`GET /wellbeing/groups/{id}/rule-suggestions`).
- [ ] **D6.4 Choosing items for a group by hand.** `POST /wellbeing/groups/{id}/assign` (`manual_assign`) and `GET .../items` exist; no
  screen lets him pick items and put them in a group. Needs an item picker (recent items or search) and an `assignToGroup` in
  `lib/api.ts`. This is a write path into `memory_user_groups` through `manual_assign`, which hard constraint 4 allows.

---

## E. Later — after the review AND Step 2 are complete

### E1. Echo as an MCP server (Claude Desktop or any MCP client) — [x] decided 2026-10-21; [x] BUILT 2026-10-26 (first run against Claude Desktop still to do)
- **Decision (his, 2026-10-21): build it this week** (moved from "later" into the Thu 22 – Sun 25 build).
  Scope for the build: tools `search_memories` (same contract as A26), `get_memory`, `recent_activity`; stdio
  transport; Claude Desktop config snippet + an MCP README with the privacy note below. The wellbeing tool set
  stays optional (only if time permits). Estimate: Likely ~2–3 h.
- New dependency: the official MCP Python SDK (`pip install mcp`) — not installed in `.venv` (checked
  2026-10-21); add to requirements.txt at build.
- Update: deck + paper + design doc move MCP from "future work" into the implementation; CLAUDE.md §4/§1 module
  list gains the MCP entry point.
- Expose Echo's deterministic tools over MCP so an outside assistant (Claude Desktop first) does the tool
  calling itself; its own model replaces Luna's agent loop. Echo stays the engine, the client is the brain.
- Candidate tools: `search_memories` (same contract as A26), `get_memory` (one item's details / peek card),
  `recent_activity`; later a second set for digital wellbeing (time patterns, sessions, regret, weekly review —
  read-only summaries; group decisions stay human-in-the-loop through the existing review flow).
- Privacy boundary unchanged: tools return snippets and aggregates only, never raw bodies. The client's LLM
  provider then sees those snippets — call this out in the MCP README.
- Likely stack: official MCP Python SDK, stdio transport for Claude Desktop, reusing `rse/` and `wba/`
  functions directly (no HTTP hop). Decide when we get here.
- **Status check 2026-10-24:** no MCP server entry point/config/README was found in the workspace, and `mcp` is not in `requirements.txt`. The decision is complete; the implementation is not.
- **Built 2026-10-26 (scope cut by his decision the same day):**
  - **Tools:** `search_memories` (the same arguments and the same `run_search` as Luna's tool; up to 10 numbered items with their `memory_id`;
    every result starts with today's date so the client can work out `since`) and `get_memory` (one item by `memory_id`: details, link, first
    8,000 characters, attachment names; `read_attachment` adds attachment text, up to 40,000 characters). **Dropped by decision:**
    `recent_activity` (search with `since` = today covers a question with a topic; a question with no topic, like "what did I do today",
    is weak under a topic search) and the optional wellbeing tools. No tool writes.
  - **No Luna call:** the client's model (Claude, in Claude Desktop) reasons and writes the answer; what goes to the cloud is the tool results,
    to Anthropic instead of OpenAI. Same bounded excerpts as the app. The README says so.
  - **Files:** `mcp_server/server.py` (registers the tools with the official SDK; `mcp>=2.3,<3`, v2 where `FastMCP` became `MCPServer`; stdio),
    `mcp_server/tools.py` (the logic), `mcp_server/README.md` (install, Windows config snippet, privacy, troubleshooting), `mcp>=2.3,<3` in
    `requirements.txt`. Small shared changes in `rse/llm_synthesizer.py` (`format_result_body`, `format_memory_detail`, `quality_words`, optional
    `memory_id` line) and `rse/search_tool.py` (`argument_description`); Luna's result text is unchanged.
  - **Design points:** the server sets the working directory itself (the client config has none); logs to stderr only (the backend's `logs/echo.log`
    is not shared); the models load in a thread started by the server's lifespan so the start-up handshake is immediate; before each search the
    FAISS index is reloaded if the backend saved a newer file (retry once on a half-written pair, else keep the old copy); `memory_id` is validated
    as a UUID before any query; errors are short text with no connection details.
  - **Tests (`tests/test_mcp_server.py`, 16; all 38 tests pass offline):** tool logic (numbered items with ids and today's date, excerpt limits
    8,000 / 2,000, bad arguments, a failing search leaks nothing, widening and attachment text reported, no matches), `get_memory` (detail,
    attachment names and link, text only on request, ids that are not ids never reach the database, unknown id, no attachment query for pages),
    index reload (once per change; a failed reload keeps the old index), the protocol with the SDK's own client (two read-only tools, Luna's
    argument descriptions, a search through the protocol, an unknown source refused) and a real child process over stdio pipes. Deliberate
    breakages (id check removed, ids hidden, reload on every search, a stray `print` in the warm-up) each failed the matching test or, for the
    print, did not break the protocol.
  - **His first run (not done here: no database or Claude Desktop in the cloud container):** `pip install "mcp>=2.3,<3"`, add the snippet from
    `mcp_server/README.md` to `%APPDATA%\Claude\claude_desktop_config.json`, quit and restart Claude Desktop, ask it to search Echo for the
    TechCorp interview email, then to open the first result. Logs: `%APPDATA%\Claude\logs\mcp-server-echo.log`.
