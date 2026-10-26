# Draft 5 (final): notes for whoever rewrites the wording

Source: Draft 4 (`main.tex`), corrected against the final build on 2026-10-26 and filled with the measured results.
Compiles with pdflatex (9 pages, no undefined references). Results come from `docs/review/eval/RESULTS_2026-10-26.md`
and `docs/review/eval/capture_sessions.md`.

## Rule for the wording pass
Change words and sentence shape only. Do NOT change any number, name, citation, equation, table value, threshold, or claim.
Keep: US spelling, no hyphens or dashes in prose, plain wording. Keep all `\cite{...}`, `\ref{...}`, `\label{...}` and the TikZ figures.

## What changed from Draft 4 (facts, not style)
- Search tool arguments now match the build: query, up to two alternative phrasings, sources, start date, reference item (before or after), attachment flag. No `fresh` option, no sender argument.
- Text sent to the model: bounded excerpts (8,000 characters for the top three results, 2,000 for the rest, 40,000 for one attachment), NOT 500 character snippets and NOT "never a whole email".
- Reranker reads the title plus the first 512 characters (no best matching window). Chat history is re-sent (last 12 messages); no response chaining.
- MCP server has two tools (`search_memories`, `get_memory`). Failed enrichment items are retried by a configuration switch, not automatically.
- Video text: title, channel, category, tags, description. Captions are best effort and returned nothing for all 14 videos.
- LLM name is GPT-6 Luna. Development history (earlier models, hosted services, "first proposed") removed.
- Values measured and written in: seed lead 0.05, quality floor 0.55 (kept), KNN bar 0.85. New reference [16] Arctic-Embed (verified online) and [17] MCP.
- Section "Evaluation Protocol" replaced by "Evaluation" with five subsections of measured results.

## The owner must confirm before submission
1. The 38 questions and the relevant items were marked by the assistant, not yet checked by the owner; the paper says "the authors". Check them (`python scripts/eval_retrieval.py labels`) or reword.
2. The cost line assumes a price of 0.20 and 1.20 US dollars per million tokens; the real price of GPT-6 Luna was not checked.
3. The three user groups behind the KNN bar and the 12 unanswerable queries were also written by the assistant.
4. The wellbeing module, group suggestions, and weekly reflection are described but not evaluated (the paper says so in the Limitations section).
5. Page count is 9; check the venue's limit.
