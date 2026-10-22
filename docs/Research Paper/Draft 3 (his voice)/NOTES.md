# Draft 3 (his voice): notes

Draft 3 is Draft 2 with the prose reworded in Shehzaad's own voice at publication register. Nothing else changed.
Build: `latexmk -pdf main.tex` gives **8 pages, 0 errors** (same page count as Draft 2). I checked all 8 pages
visually. Both figures are TikZ inside `main.tex`, so there are no asset files to copy. `STYLE_PROFILE.md` has the
voice analysis. The scratch scripts and raw outputs are in the session scratchpad (`style_agent/`), not in the repo.

## 1. Which app stages ran

All of these ran against the app's code and `data/` exactly as they are, and read the existing artifacts. Nothing
was written into the style project. `.env` was never opened or loaded. Stage 5 used the `claude_agent_sdk`
provider, which runs on the Claude login, not on a key.

| Stage | Ran? | What it gave |
|---|---|---|
| Stage 1 parse/score | Reused, not rerun | `parsed_corpus.json` (2,017 messages with formality, Wegmann and rule scores) and `corpus_stats.json` (Wegmann centroid, formality IQR 0.535 to 0.637). |
| Stage 1 v2 features (`stage1_features.extract_v2_features`) | **Ran** on his-only text (1,725 messages) and the explanatory subset (601) | Mean sentence 13.9 words (15.0 explanatory), FK grade 6.9 (7.4), MTLD 57.8 (61.0), function-word ratio 0.54, 0.88 commas per sentence, POS nouns 18% / verbs 13% / adjectives 7%. |
| Stage 2 embeddings | Reused (`embeddings.npy`, 2,017 x 384 MiniLM; `wegmann_vectors.npy`) | Used by Stage 3, and by my own cosine search over the explanatory subset. |
| Stage 3 retrieval (`stage3_retriever.retrieve`, `use_cache=False`) | **Ran** once for each of the 64 Draft 2 prose paragraphs | 5-signal fusion (BM25, Wegmann, MiniLM, voice rules, MS MARCO cross encoder). Median top hybrid score 0.740. Fallback used 0 times. Style blend: push_toward_median 36, match_input 26, bring_toward_median 2. |
| Stage 3.5 style agent | Ran inside Stage 3 | Input formality per paragraph. Draft 2 sits mostly below his median of 0.587, so the agent says "push toward median". |
| Stage 4 prompt builder | **Ran** for one paragraph | 10 retrieved examples, blend push_toward_median. Notice: `user_style_notes.txt` was not found, so personal notes were not in the prompt. |
| Stage 5 LLM rewriter (`rewrite(..., save=False)`) | **Ran** for one paragraph (claude_agent_sdk / sonnet, about 59 s) | Attempt 1 formality 0.572 against a target of 0.587, composite style match 0.539. The refinement attempt scored lower (0.431), so attempt 1 was kept. The output is in sample 1 below. It is chat register, so I did the Draft 3 rewrite myself, as briefed. |
| Stage 6 evaluator / AI detection | **Not run, on purpose** | The brief says not to target an AI detector. |

**Authorship filter.** The corpus is marked as his per account, not per message. The Wispr export header reads
"Welcome back, Shehzaad" and the footer has his account email. The Wispr rows come from his own `flow.sqlite`. I
kept messages of 9 or more words with no pasted code and no em or en dash. That kept 1,725 and dropped 292, all
of them for being too short. Details are in STYLE_PROFILE.md.

**A descriptive check, not a target.** I measured each prose paragraph's Wegmann style vector against his
corpus centroid, once, after the rewrite. Draft 2 has mean cosine 0.407 (median 0.420). Draft 3 has 0.433
(median 0.446). 36 of 66 paragraphs moved closer. Sentence length went from a mean of 19.7 to 17.1 words.
Semicolons went from 19 to 0.

## 2. Three before/after samples

### IV.A Intent Aware Capture, Gmail paragraph

*Retrieval:* Stage 3 top: msg 1055 (hybrid 0.830, bm25 38.0) and msg 683, his review speech (hybrid 0.707, sem 0.489; also the top plain-semantic match, cos 0.489).

**Draft 2**

> Gmail is treated differently from the other two sources. Mail arrives on its own rather than being chosen, daily volume is small, and even an unread message may matter later, so every message is stored when it arrives. The connector polls every 30 seconds, but each poll asks Gmail only for the changes since a saved history position instead of listing the whole mailbox. If that position has expired, the connector lists the mailbox once and the uniqueness rule on stored items absorbs the overlap. Mail that Gmail has filed as spam or trash is never read; Gmail's own spam filter acts as E.C.H.O's spam filter. When the user opens a message in Gmail on the web, the extension reports the message id and the seconds spent reading, and nothing else.

**Draft 3**

> Gmail is treated differently from the other two sources. Mail arrives on its own instead of being chosen by the user, the daily volume is small, and even an unread message may matter later. So every message is stored when it arrives. The connector polls every 30 seconds, but each poll asks Gmail only for the changes since a saved history position, instead of listing the entire mailbox. If that position has expired, the connector lists the mailbox once, and the uniqueness rule on stored items absorbs the overlap. Mail that Gmail has filed as spam or trash is never read, so Gmail's own spam filter acts as the spam filter of E.C.H.O. When the user opens a message in Gmail on the web, the extension reports the message id and the seconds spent reading, and nothing else.

**For comparison, the app's own Stage 5 output for the same paragraph** (formality 0.572, style match 0.539):

> Gmail module is treated differently from the other two sources. Mail arrives on its own, it isn't chosen by the user, and the daily volume is small, and even an unread message may matter later, so basically every message is stored the moment it arrives. The connector polls every 30 seconds, but each poll asks Gmail only for the changes since a saved history position instead of listing the whole mailbox. If that position has expired, the connector lists the mailbox once, and the uniqueness rule on the stored items takes care of the overlap. Mail that Gmail has filed as spam or trash is never read, so Gmail's own spam filter acts as E.C.H.O's spam filter too. When the user opens a message in Gmail on the web, the extension just reports the message id and the seconds spent reading it, and nothing else.

It keeps the facts but adds chat markers ("so basically", "just", "too", contractions). That is fine for a message, not for a paper. Draft 3 keeps his structure (the reason first, then "So every message is stored…") without those markers.

### IV.D Retrieval Loop, opening paragraph

*Retrieval:* Stage 3 top: msg 859 (hybrid 0.684, voice 3.79) and msg 849 (0.629). Habit applied: the problem, then question, then answer opener from msg 683 ("How do we filter it?").

**Draft 2**

> Questions are answered by the loop in Fig.~\ref{fig:graph}. The language model, \llmname{}, receives the conversation so far, today's date, the question, and one tool called \texttt{search\_memories}. The tool takes a query, an optional list of sources, an optional time phrase, and an optional sender or domain. The model decides how to phrase the search and may issue several searches in parallel in one round, for instance one per source. After reading the results it either answers or searches again. A usual question needs one call that searches and one call that answers. The code allows at most five search rounds; after the fifth, the next call is made without the tool, which forces an answer. A question therefore costs between two and six model calls, and the bound is enforced by the program, not by the prompt.

**Draft 3**

> How is a question answered? It passes through the loop in Fig.~\ref{fig:graph}. The language model, \llmname{}, receives the conversation so far, today's date, the question, and one tool called \texttt{search\_memories}. The tool takes a query, an optional list of sources, an optional time phrase, and an optional sender or domain. The model decides how to phrase the search, and it may issue several searches in parallel in one round, for instance one per source. After reading the results, it either answers or searches again. A usual question needs one call that searches and one call that answers. The code allows at most five search rounds. After the fifth round, the next call is made without the tool, which forces an answer. So a question costs between two and six model calls, and this bound is enforced by the program, not by the prompt.

### II.C Hybrid Search and Reranking

*Retrieval:* Stage 3 top: msg 847 (hybrid 0.823) and msg 1002 (0.812). Plain-semantic top: msg 309 (cos 0.463). Habit applied: name the problem plainly ("The problem is that…"), split the long sentence at the semicolon.

**Draft 2**

> Sentence embeddings such as Sentence BERT make it possible to compare texts by meaning with cosine similarity \cite{b6}. E.C.H.O embeds every item with a small local sentence model and searches the vectors with FAISS \cite{b8}. Keyword search uses the full text search built into PostgreSQL. Scores from the two methods live on unrelated scales, so adding them is not meaningful. Reciprocal rank fusion sidesteps the problem by using only each item's position in each list \cite{b9}. After fusion, a cross encoder reads the question and a candidate together and scores how well they match; Nogueira and Cho showed that this works well for passage reranking \cite{b10}. Our cross encoder is a compact MiniLM \cite{b7} model trained on the MS MARCO passage ranking data \cite{b11}.

**Draft 3**

> Sentence embeddings such as Sentence BERT make it possible to compare texts by meaning, using cosine similarity \cite{b6}. E.C.H.O embeds every item with a small local sentence model and searches the vectors with FAISS \cite{b8}. Keyword search uses the full text search built into PostgreSQL. The problem is that the two methods score on unrelated scales, so simply adding their scores is not meaningful. Reciprocal rank fusion avoids this by using only the position of each item in each list \cite{b9}. After fusion, a cross encoder reads the question and a candidate together and scores how well they match. Nogueira and Cho showed that this works well for passage reranking \cite{b10}. Our cross encoder is a compact MiniLM \cite{b7} model trained on the MS MARCO passage ranking data \cite{b11}.

## 3. Invariant check (Draft 2 against Draft 3)

`check_invariants.py` (scratchpad) compares the two files mechanically. Output:

```
lines changed: D2 64, D3 67 (expected: 63 prose lines + 1 layout line \IEEEtriggeratref, + 3 added header comment lines)
\cite keys                                 D2=  23 D3=  23  SAME
\ref targets                               D2=  16 D3=  16  SAME
\label names                               D2=  16 D3=  16  SAME
\tbd{...} placeholders                     D2=  13 D3=  13  SAME
[TBD] cells                                D2=  10 D3=  10  SAME
\embmodel uses                             D2=   2 D3=   2  SAME
\embdim uses                               D2=   1 D3=   1  SAME
\rerankmodel uses                          D2=   2 D3=   2  SAME
\llmname uses                              D2=   2 D3=   2  SAME
inline math $...$                          D2=  16 D3=  16  SAME
equation blocks                            D2=   2 D3=   2  SAME
figure blocks                              D2=   2 D3=   2  SAME
table blocks                               D2=   3 D3=   3  SAME
bibliography block                         D2=   1 D3=   1  SAME
preamble (to \begin{document})             D2=   1 D3=   1  SAME
comment lines (incl. % TODO / % VERIFY)    D2= 107 D3= 110  removed=0 added=3
    added: %  Draft 3 (his voice), 21 Oct 2026: same content as Draft 2, prose reworded
    added: %  in Mohammed Shehzaad Khan's own voice (see STYLE_PROFILE.md, NOTES.md).
    added: %  Every fact, number, citation, label, TBD and comment below is unchanged.
% TODO / % VERIFY lines                    D2=   7 D3=   7  SAME
section/subsection headings                D2=  27 D3=  27  SAME
numerals in prose                          D2=  41 D3=  41  SAME
number words in prose                      D2= 168 D3= 174  DIFF (soft)
    only in D2: {'two': 1}
    only in D3: {'single': 1, 'first': 2, 'second': 2, 'both': 1, 'once': 1}
quoted phrases ``...''                     D2=   9 D3=   9  SAME
\texttt{...}                               D2=   1 D3=   1  SAME
named systems/tools in prose               D2= 116 D3= 116  SAME
hyphens/dashes in prose (style rule)       D2=   0 D3=   0
RESULT: PASS
```

What the output shows:

- **Identical sets.** Citations, refs, labels, the 13 `\tbd{}` placeholders, the 10 `[TBD]` table cells, every
  macro use (`\embmodel`, `\embdim`, `\rerankmodel`, `\llmname`), inline math, both equations, both figures, all
  three tables, the bibliography, the preamble, the section headings, the 41 numerals in prose, quoted phrases,
  and the 7 `% TODO` / `% VERIFY` lines. All 107 original comment lines are still present and unchanged.
- **The 3 added comment lines** are a header note after line 1 that marks this as Draft 3.
- **One non-prose line changed:** `\IEEEtriggeratref{10}` became `\IEEEtriggeratref{7}`.
  - Draft 3's prose is about 5% longer, so a break at [10] left the last page lopsided.
  - The comment above that line says to adjust it. It is a layout setting, not content.
- **The "soft" number-word difference** is wording only:
  - The enumerations "First, … Second, …" add the words *first* and *second*.
  - "The two branches run" became "Both branches run".
  - "during one day" became "In a single day".
  - "After five seconds of watching" became "Once a video has been watched for five seconds".
- **Hyphens or dashes added to prose:** 0, which keeps the paper's own style rule.

The mechanical check can't catch a change in meaning, so I also reread every rewritten paragraph against its
Draft 2 sentence by sentence. Three places needed care, and I resolved each toward Draft 2's meaning:

1. **Privacy, "What leaves the laptop".** Draft 2's "…excerpt, plus at most three short passages of attachment
   text…" became its own sentence: "Retrieval also adds at most three short passages…". Same claim.
2. **No widened claims.** I did not add "only" anywhere it would widen a claim. For example, the abstract still
   says the model receives "the question and short excerpts", because chat history is also sent.
3. **Evaluation.** The question type keeps its exact quote and parentheses: (``after the interview email'').

## 4. Risks and open points

- **Voice is judged against chat messages.** His corpus is mostly messages to AI assistants, with no academic
  writing in it.
  - His closest formal register is his Phase 2 review speech (msgs 683 and 684), so that is the main anchor.
  - If the result reads too plain for a venue, the fix is a pass on Sections II and VIII only. Those are the
    most "academic" sections.
- **Stage 3's voice signal is biased toward questions.** Its `style_rules` signal rewards questions and
  instructions ("…, right?", "Can you…"). For paragraphs that explain, those messages rank high even though they
  are the wrong register for a paper.
  - That is why I also ran a plain semantic search restricted to his explanatory messages, and leaned on it.
- **Three rhetorical questions were added** ("What should E.C.H.O keep?", "How is a question answered?", and
  the existing "where did my time go?").
  - They are his signature move, but some reviewers dislike questions in a paper.
  - Each one is a single line to revert in `main.tex`, in IV.A and IV.D.
- **Things carried over from Draft 2 unchanged.** All TBDs, the `% VERIFY` on Lyngs et al., and the open
  decisions (A11, A16, A40, A42, A43) are still open. See Draft 2 `CHANGES.md`.
