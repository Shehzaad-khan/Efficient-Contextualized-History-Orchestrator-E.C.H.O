# Tests, security checks and CI/CD

Written 2026-10-26 (plan: Asr until Maghrib, "Test cases" and "CI/CD pipeline"). Short version for the review:
**563 tests, 94% line coverage, 0 high-severity security findings, and every push runs all of it on GitHub.**

## The numbers (for the PPT and the paper)

| What | Result |
|---|---|
| Tests | **563** pytest tests: **333 unit**, **220 integration**, **10 system** (the two older files, `test_agent_loop` and `test_mcp_server`, are part of the integration count) |
| Runtime | about 15 seconds, no PostgreSQL, Redis, Gmail account, GPU or API key needed |
| Line coverage | **94.3 %** (5,704 of 6,050 statements) over `backend enp ingestion mcp_server rse ste wba`; the gate in CI is 80 % |
| By package | backend 97.0 % · ingestion 96.4 % · rse 96.1 % · wba 94.7 % · enp 92.1 % · ste 92.1 % · mcp_server 84.3 % |
| Bandit (security scan) | **HIGH 0** · MEDIUM 36 · LOW 1 (details below) |
| Lint (ruff) | 0 findings (rules: syntax errors and undefined names only) |
| Frontend | `npm ci`, `tsc --noEmit` and `vite build` run in CI |

Before this work the 38 existing tests covered 16 % of the code.

## What the three layers are

| Layer | Folder | Idea | Example |
|---|---|---|---|
| Unit | `tests/unit/` | one function or class, everything else faked | the 20-minute session gap, RRF fusion arithmetic, "a login page is never captured" |
| Integration | `tests/integration/` | several real modules together; only PostgreSQL / Redis / Gmail / models / LLM are faked | the real SQL of the storage engine against an in-memory database, every HTTP route through FastAPI's test client, the ENP worker with a real FAISS index |
| System | `tests/system/` | one whole user journey | Chrome visit → enrichment → index → question → cited answer; the group-review journey (suggest → accept / remove) |

`docs/review/test_guide.md` lists, per file, the *Shows* sentence of the tests that are worth saying out loud
(regenerate it with `python scripts/make_test_guide.py`).

### Hard constraints that are proven by a test (CLAUDE.md section 2)

| Constraint | Test |
|---|---|
| UNIQUE(source_type, source_id) dedup | `tests/integration/test_storage_engine.py` (same page / same mail twice = one row; the database refuses a raw duplicate) |
| `system_group_id` never NULL | same file (every source gets a group; the schema rejects NULL) and `test_enrichment_pipeline.py` (unknown category is stored as `misc`) |
| `memory_user_groups` only through `group_suggestions` | `tests/integration/test_group_manager.py` (scans the whole code base: only `wba/group_manager.py` writes it) and the system journey |
| A42 confident path is undoable | `test_group_manager.py`, `tests/system/test_group_review_journey.py` |
| No recency boost / MMR | `tests/unit/test_retrieval_logic.py` (RRF order and effort score ignore dates) |
| At most 6 LLM calls per query | `tests/integration/test_agent_loop.py` and `tests/system/test_capture_to_answer.py` (over HTTP) |
| Privacy: bounded text, no rows, no private pages | `test_capture_to_answer.py`, `test_system_group_classifier.py` (Stage 4 sends 200 + 300 characters), `test_wellbeing_logic.py` (weekly insight gets aggregates only) |
| Wellbeing language is never judging | `test_wellbeing_logic.py` |
| Embedding version / centroid mismatch is refused | `test_faiss_manager.py`, `test_system_group_classifier.py` |

## How the tests avoid real services

* **PostgreSQL**: an in-memory SQLite database with the same table shapes runs the *same SQL strings* the app
  executes (`sqlite_db` fixture in `tests/conftest.py`). PostgreSQL-only code (full-text search, advisory locks, the
  psycopg2 pool) is covered with scripted fake connections instead.
* **Redis**: `FakeRedis`, a dictionary with TTLs, counters and a MULTI pipeline.
* **Embedding model and cross-encoder**: a word-hashing stand-in that returns unit-length 384-d vectors, so the
  real FAISS index, the real token-window cutting and the real classifier run. This is why CI does not need torch.
* **LLM**: a scripted model that returns prepared replies and records what it was sent.

## Run it yourself

```bash
pip install -r requirements-dev.txt        # CI uses requirements-ci.txt, see the note below
pytest                                     # all 563 tests
pytest -m unit                             # or: -m integration, -m system
pytest --cov                               # + coverage table (settings in .coveragerc)
pytest --cov --cov-report=html             # open htmlcov/index.html for the PPT screenshot
bandit -r backend enp ingestion mcp_server rse ste wba scripts -lll    # fails on HIGH
ruff check .
```

## The pipeline (`.github/workflows/ci.yml`)

Runs on **every push to any branch**. A merged pull request is a push to `main`, so it runs too. (There is
deliberately no separate `pull_request` trigger: it would run twice for every commit on a PR branch.)

| Job | Steps | Fails the build when |
|---|---|---|
| `backend` | install (Python 3.11) → `compileall` → `ruff` → `bandit -lll` → `pytest --cov` → run summary → upload reports → `pip-audit` | compile error, undefined name, a HIGH bandit finding, any test failing, or coverage under 80 % |
| `frontend` | `npm ci` → add rollup's Linux binary → `tsc --noEmit` → `vite build` → `npm audit` | type error or build error |

Reports (HTML coverage, `coverage.xml`, `junit.xml`, bandit JSON/TXT) are attached to each run as the
`backend-reports` artifact for 30 days; the coverage table and bandit counts are also written into the run summary
page. `pip-audit` and `npm audit` are report-only (a new advisory should not turn an unrelated commit red).

### Three things found while building this

1. **`requirements.txt` cannot be installed as written.** `mcp>=2.3` needs `pydantic>=2.12` and
   `uvicorn>=0.31.1`, but the file pins `pydantic==2.8.2` and `uvicorn[standard]==0.30.6`, so
   `pip install -r requirements.txt` ends in `ResolutionImpossible` (checked with `uv pip compile`). CI therefore uses
   `requirements-ci.txt`, which is `requirements.txt` minus torch / torchvision / sentence-transformers with those two
   pins loosened to `pydantic>=2.12` and `uvicorn[standard]>=0.31.1`. The same two lines should be changed in
   `requirements.txt`; that file was not touched.
2. **`frontend/package-lock.json` lists only the Windows build of rollup's native part**, so `npm ci` on Linux
   leaves `vite build` with "Cannot find module @rollup/rollup-linux-x64-gnu" (reproduced here). The workflow installs the matching
   Linux package for that run only (`--no-save`); the lock file is unchanged. Regenerating the lock on a machine with
   `npm install --os=linux --cpu=x64` would remove the need for the extra step.
3. **A small oddity, not changed:** `ste/storage_engine.py update_youtube_watch_time` keeps the highest watch time
   but recomputes `completion_rate` from the value just sent, so a lower, late heartbeat lowers the completion rate.

### Bandit findings (0 HIGH, so the build passes)

* **2 HIGH (B613)**, both false positives: the regular expressions in `enp/text_cleaner.py` and
  `ingestion/gmail/gmail_api.py` contain invisible Unicode characters *on purpose* (they are the characters the
  expression strips from e-mail text). Each line now ends with `# nosec B613`; behaviour is unchanged.
* **35 MEDIUM (B608)**, "SQL built from strings": the f-strings only join fixed SQL fragments and column lists
  that are module constants (for example `TIME_UNION_SQL`); every value goes through bind parameters. Reviewed, not changed.
* **1 MEDIUM + 1 LOW (B314, B405)**: `ingestion/youtube/youtube_api_client.py` parses a caption file from YouTube
  with `xml.etree`. A hostile XML file could in theory expand to a lot of memory. Suggested later fix: `defusedxml`.

## Replaying every old commit

GitHub only runs a workflow for a commit that already contains the workflow file, so the 82 commits made before
this work cannot trigger anything by themselves. The workflow therefore has a manual trigger (`workflow_dispatch`)
with an input `ref`. When `ref` is a commit hash it:

1. checks out the old commit into `target/`,
2. lays today's tests and settings over it (`tests/`, `pytest.ini`, `.coveragerc`, `ruff.toml`, requirement files),
3. runs the same compile / lint / bandit / pytest / frontend checks,
4. is titled `CI replay · <short hash> · <commit message>` in the Actions list.

Old commits are **expected to fail** the test step (they do not contain most of today's code). They will
usually pass compile, lint and bandit; commits before the frontend existed simply skip the frontend job.
The newest commit and everything after it must stay green.

To (re)start the replay of the 82 commits made before this work (needs the GitHub CLI, run once the workflow is on `main`).
The list ends at `950e6ef`, the last commit before the tests and the pipeline: the three commits after it
(`aa25b1e`, `a047f4d` and the PR #17 merge) already have their own push runs, so using `main` here would start duplicates.
This replay was done on 2026-10-26 (82 runs); only repeat it if you need to.

```bash
git log --reverse --format='%H%x09%h%x09%s' 950e6ef | while IFS=$'\t' read -r full short subject; do
  gh workflow run ci.yml --ref main -f ref="$full" -f sha_short="$short" -f label="$subject"
  sleep 3
done
```

PowerShell:

```powershell
git log --reverse --format="%H|%h|%s" 950e6ef | ForEach-Object {
  $f,$s,$m = $_ -split '\|',3
  gh workflow run ci.yml --ref main -f ref=$f -f sha_short=$s -f label="$m"
  Start-Sleep -Seconds 3
}
```

From then on, **one commit = one run**: every new push or merge creates its own entry.
