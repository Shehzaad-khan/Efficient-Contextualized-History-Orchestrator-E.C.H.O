# Draft 2 vs Draft 1: what changed

Draft 2 (21 Oct 2026) describes the system as designed and decided on 21 Oct 2026. Decision ids (A1 to A43) refer to
`docs/review/ste_open_items.md`. Draft 1 is untouched in `../Draft 1 Research Paper of This Project/`.
Build: `latexmk -pdf main.tex` (8 pages, compiles with no errors). Search the PDF for "TBD" to find every open value.

## Section by section

| Section | What changed | Decision |
|---|---|---|
| Header comment | New list of beyond-build claims (everything decided for the Thu 22 to Sun 25 Oct build) and the open items. | all |
| Preamble | One-line swap macros: `\embmodel`, `\embdim`, `\rerankmodel`, `\llmname`; `\tbd{}` placeholder macro. | A40, A25 |
| Abstract | Rewritten: local store, spam/trash skipped, attachment text, tool loop (usually 2, max 6 calls), local storage and search, wellbeing + human approved groups. "Exactly two calls" removed. | A26, A31, A38, A4, A25 |
| Keywords | Added tool calling agents, digital wellbeing. | A26 |
| I. Introduction | Contributions rewritten: capture privacy filters, local storage + attachment search, bounded agentic loop, wellbeing module (was "out of scope" in Draft 1). Section roadmap updated. | A7, A9, A31, A4, A26 |
| II. Related Work | RAG subsection now covers tool use (new cite ReAct). New subsection Digital Wellbeing Tools (new cite Lyngs et al.). Local first text: everything but the LLM is local. Embedding model no longer named here. | A26, A31, A40 |
| III. System Overview | Gmail asks for changes only; Redis holds rate limits too; all stores local; three modules (wellbeing now in scope); popup = pause only. | A5, A31, A33 |
| Fig. 1 | Redrawn: laptop boundary, wellbeing box, single dotted path to the hosted LLM. | A31 |
| IV.A Capture | Gmail: history.list polling, 30 s, expired position fallback, Spam/Trash never read, extension sends only id + seconds. Chrome: revisit flag only after 5 s, link clicks no longer interactions, headings sent. YouTube: 10 % completion with 5 s floor, 5 s revisit check. | A5, A38, A41, A10, A39, A13, A34 |
| Table I | Scroll 25 % to 15 %; Shorts 15 s to 10 s; completion 50 % to 10 % + 5 s floor; app pages 5 s / 1 interaction, title only; Gmail 3 s open minimum; Spam/Trash. | A39, A8, A38 |
| IV.B Storage | Visit table (one row per visit with dwell); 16 to 17 tables; misc fallback; re-embed on changed page text; UTC stored / local shown; retention setting with hard delete across PostgreSQL, FAISS, Redis. | A1.1, A1.1b, A31.3, A3, A31.4, A37 |
| IV.C Cleaning | Hidden preview text dropped, split body parts joined, inline replies kept, signatures NOT removed (Draft 1 said removed), headings from the extension, transcript via a library. | A6, A14.2, A12, A13, A11 |
| IV.C Attachments | Was "extract on demand at query time, cache 1 h". Now extracted at ingestion, new types (XLSX, PPTX, EML, OCR), inline logos skipped, stored separately, start embedded, full text keyword indexed. | A4, A22 |
| IV.C Embedding | Model via macro + TBD; hint-first order; trimming only cuts the long part; token budget TBD. | A40, A16 |
| IV.C Classification | Curated domain map; richer seed centers + separate seed threshold (TBD); balanced per-source recompute; 0.55 / 0.08 for recomputed centers; recompute at worker start if missing or > 30 days; Stage 4 one call per item, Decisions API on OpenAI, strict single word elsewhere, fallback path, no confidence stored. | A17, A18, A20, A36, A19, A21 |
| IV.D Retrieval | "Exactly two calls, fixed graph" replaced by the agent + `search_memories` tool loop (max 5 rounds, 2 to 6 calls, cap in code). Parse-intent step removed. | A25, A26 |
| Fig. 2 | Redrawn as agent loop around the deterministic tool. | A26 |
| Time phrase | Code resolves time words; mail by arrival, pages/videos by any visit; open-ended filter + "today" line and "keep items inside the period" rule for the answer. | A26, A1.1, A1.2 |
| Parallel search | Keyword search over a stored full-text index (full text + attachment text), no 5,000 char cap. | A24.3, A4 |
| Quality / widening | Unchanged rules, widening steps made concrete (4 days). Tool now reports "nothing found" to the model. | A39 (kept), A26 |
| Reranking | Cross encoder via macro; median effort for unmeasured email; "no diversity reranking" stated. | A29, A40 |
| Nearby context and passages | Snippet = best-matching 500 char window of the CLEANED text (was first 500 chars of raw text); attachment = top 3 matching paragraphs, about 1,500 chars (was up to a few thousand chars); stable numbering across rounds. | A24.2, A23, A26 |
| Writing the answer | Chat style answer, validated hidden markers, memory pills, time strip, streamed progress. | A27, A28 |
| Conversation memory | Resume old chats; 30-day cleanup. previous_response_id NOT claimed (store=true not approved). | A27, A26 |
| V. Wellbeing and User Groups (new) | Time from per-visit rows; 20 min sessions; regret event log; habit = 3 in 30 days; weekly reflection from aggregates only, judging-phrase check, fallback; groups: KNN from 5 items, KNN-only threshold (TBD), rule suggestions at 5 shared values, every suggestion pending, user groups independent of system groups. | A1.1b, A30, C24, A32, A39, A42 (TODO), A43 (TODO) |
| VI. Privacy | New: 38-word whole-token sensitive list, form check, secret query keys stripped, 7 app domains title-only + public sections, Gmail web skipped, rules served by the server, fail closed. What leaves the laptop: Google APIs + three LLM callers (retrieval, Stage 4, weekly reflection). Prompt-injection note. Removed "OAuth tokens are stored encrypted" (no longer true). | A7, A8, A9, A31.5, A19, A23, A35 |
| VII. Implementation + Table II | Agent/tool LangGraph, LangChain with OpenAI extras (Responses API, medium effort, typed choice), local PostgreSQL 16 + Redis under WSL, SSE, new extractors + Tesseract, GPT-5.6 Luna. | A25, A21, A31, A22, A28 |
| VIII. Evaluation | Was a 4-bullet plan. Now a protocol with data, question types, 5 configurations (Table III, all TBD), cost/speed, capture, privacy audit, threshold calibration. | A26, A39 |
| IX. Limitations / Future work | Added: one vector per item (chunking = future), Outlook quoted threads, open-ended date filter + top 10, private apps outside the 7, plain token files. Removed: manual periodic jobs, "wellbeing not validated". Future: chunking, self-learning domain map, A42, A43, Takeout/History import, MCP. No local LLM (owner's decision). | A24.1, A14.1, A1.2, A9.4, A35, A36, A17, A42, A43, E1, E2, A31.5 |
| X. Conclusion | Rewritten for the bounded loop and the wellbeing module. | A26 |
| References | Added [6] ReAct (verified), [14] Lyngs et al. CHI 2019 (verified title/venue; `% VERIFY` author order + article no.), [15] Smith, Tesseract, ICDAR 2007 (verified). Reordered by first citation. LangGraph access date updated. | A26, A22 |

## Figures
- Both figures are TikZ in main.tex (no image files needed).
- Not reused: `docs/Phase III/Arch diagram/echo_architecture.png` and `docs/Phase III/build/diagrams/*.png`.
  They are outdated: "exactly 2 LLM calls", "gpt-4.1-mini" (seq_recall), "16 tables", popup "quick search, recent",
  YouTube "metadata + transcript" via the Data API. They are owned by another agent and were not edited.

## TBD placeholders (visible in the PDF)
1. Final embedding model (A40), Section IV.C.
2. Embedding token budget (A16), Section IV.C.
3. Stage 3 seed acceptance threshold (A18), Section IV.C.
4. KNN-only suggestion threshold (A32), Section V.
5. Number of items per source in the test data, Section VIII.A.
6. Number of evaluation questions, Section VIII.A.
7. Table III: Recall@10 and MRR for 5 configurations (10 cells).
8. Mean LLM calls per question.
9. Share of questions hitting the 5-round cap.
10. Median and 95th percentile latency.
11. Cost per question.
12. Capture gate agreement with scripted labels.
13. Largest LLM request in characters (privacy audit).

## Unverified or to recheck (LaTeX comments in main.tex)
- `\embmodel` is set to the leading candidate (granite-embedding-small-english-r2), not a decision. Swap in one line.
- 0.35 quality floor and 0.55 / 0.08 centroid values are from code, tuned on MiniLM; re-measure after A40 (A39).
- A11 transcript library is tested Thu 22 Oct; if it fails, change one sentence in IV.C (comment marks it).
- Automatic retry of failed enrichment items is carried over from Draft 1; no decision schedules it (today manual).
- Live Gmail check for "just arrived" questions (fetch_api) has no decided place in the tool loop; not described.
- Lyngs et al. [14]: author order and CHI article number not verified.
- 8 pages (Draft 1 was 6). If the venue limits to 6, trim Sections II and VIII first.
