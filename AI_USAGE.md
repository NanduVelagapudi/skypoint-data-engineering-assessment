# AI usage

## Tools and how I used them
- **Claude Code (VS Code extension, Opus 5.5)**: wrote the Stage 1 code and tests, and the Task 2 parsers (D0/D1). I ran it in plan mode first, then in manual-approval mode with no auto mode. Stage 1 was built in four reviewed groups. I read each diff before accepting it and committed each group myself.
- **Claude chat and ChatGPT**: used for planning, prompt drafting, command help and as a second opinion on Claude Code's output. I treated their advice as suggestions and checked it against the brief and the data.

## Where it saved time
- Checked the data pack before planning: manifest hashes and row counts, BOM and CRLF/LF per file, the Athena schema change in batch_003, and the truncated batch_004 Epic file (18 records against 22 in the manifest).
- Drafted the Stage 1 plan, including the all-or-nothing batch design (validate in memory, then one transaction).
- Wrote the config, error types, JSON logging, schema contract and 90 tests quickly.
- Probed its own path-guard (raw DB inside output/) with tricky paths such as `work/../output/...` and reported what the guard cannot catch.

## What it got wrong or did unasked
- **Wrong Python:** it first ran the tests with the global Anaconda 3.9, then tried the global 3.11, instead of the project `.venv` with the pinned packages. I stopped it and told it to use the venv only.
- **Wrote outside the repo:** it tried to save notes to `.claude/.../memory`, which breaks my "no files outside the repository" rule. I declined.
- **Unplanned file:** it added `tests/test_logging_setup.py`. I reviewed it and kept it.
- **Unintended change:** its `.gitignore` edit changed one line ending (CRLF to LF). It reported this itself, and I left it as is.
- **Log leak risk:** the first logger passed through any `extra` key. I asked for a key allow-list so PHI protection doesn't depend on every caller remembering the rule. The allow-list filters keys, not values, so I also check the log calls in later groups.
- **Wrong test:** one of its own tests assumed the Epic entry came first, but entries are sorted by file name. It fixed the test, not the code.
- **Robustness gap:** it found and fixed a manifest `file_name` that is not a string, which would have raised a TypeError instead of rejecting the batch.

## Stage 1 issues found during the build
- **Slow inserts, measured before choosing a design.** Before writing the raw store, it timed the obvious insert methods against an in-memory DuckDB:
  - `executemany` took 86 s for the 3,636-row pack, and a columnar `unnest` took 59 s.
  - It traced the cost to the Python driver converting each value one at a time, about 1 ms per value, because numpy and pandas are not installed.
  - It switched to sending one JSON document per file and expanding it with DuckDB's `from_json`. That took 0.08 s, needed no new dependency, and was checked to be lossless for NULL vs empty strings, quotes, CRLF, NUL and non-ASCII text.
  - The same probe showed that reading DuckDB `TIMESTAMPTZ` needs `pytz` and that DuckDB drops a datetime's offset instead of converting it. That is why the store keeps UTC in `TIMESTAMP` columns and converts times itself.
- **Catalog and schema name clash.** Its first raw store opened the database file directly. Every DuckDB test then failed: DuckDB names the catalog after the file, so `raw.duckdb` made `raw.encounters` ambiguous (catalog `raw` or schema `raw`). It fixed this by attaching the file under a fixed catalog name, `raw_store`, and added a test for file names that could clash.
- **Logger lost under `python -m`.** Its first `main.py` used `logging.getLogger(__name__)`. Under `python -m pipeline.main`, `__name__` is `"__main__"`, which is outside the configured `pipeline` logger, so `pipeline_started`, `pipeline_finished` and `pipeline_failed` would never have reached the JSON log. The in-process tests could not see this; its subprocess test caught it before I reviewed the group. The logger is now named `pipeline.main` explicitly.

## Where I overrode it
- **Container user:** its plan ran the container as non-root UID 1000. I switched to root, because a bind-mounted `./output` owned by another user on a Linux host could fail the run and break the required exit code 0. The trade-off is recorded in DECISIONS.md; Docker itself is not built yet.
- **Missing columns:** I required NULL, not an empty string, for a canonical column a schema version lacks (for example `encounter_source` before Athena v2). An empty string is stored only when the field was delivered empty.
- **Auto mode:** I declined the "auto mode" and "allow all edits" options on every prompt.
- **Docs after code:** I scheduled README and DECISIONS to be written after the code they describe. DECISIONS.md now covers Stage 1 and the Task 2 parsers; README.md and ARCHITECTURE.md are still to come.

## What I verified myself
- Compared the manifest hash of every file in my `data/` copy and again in a fresh clone. 11 files match, and only the batch_004 Epic file mismatches, as intended.
- Set `.gitattributes` to `data/** -text` and `core.autocrlf` to false so Git never changes the data bytes.
- Checked that its summary of the data matched what I found (row counts of 2,863 / 585 / 188 for batches 001-003).
- Cross-checked advice from chat assistants:
  - An AI-generated profile had counts that included batch_004. I re-profiled.
  - One assistant said `git ls-files --eol` would show `i/-text w/-text`. The real output is `attr/-text`, with the index and working-tree line endings equal, which is the correct result.
  - One assistant claimed a contradiction in the schema-contract plan. There wasn't one, because Athena v2 is an exact configured version.

## Decisions I made without asking the company
- I made documented assumptions instead of emailing questions, as the brief allows.

## Task 2 parsers (D0/D1)

### Where it saved time
- Profiled the five Task 2 fields across batches 001-003 as masked shapes (digits shown as `9`, unknown words hidden), so no patient values were printed. This found:
  - every format the parsers must support;
  - that the two-digit-year rule exists only in Athena's free-text notes;
  - that no amount is negative or in brackets;
  - that all 59 Meditech decimal amounts end in `.0`.
- Ran the finished parsers over every accepted row and reported reason-code counts only. No observed format fell through. It also checked that the 12 admit dates rejected as invalid really are impossible dates, without printing any of them.
- Wrote the parsers and 147 new tests. The full suite (292 tests) passed on the first run.
- Checked that `tzdata` was already pinned, so `America/Chicago` needed no new dependency.

### Decisions I made, not the AI
- Fractional cents become NULL and are never rounded; a K suffix is invalid on Meditech cents; a trailing `.0` is valid on Meditech.
- DST: an ambiguous time takes the earlier instant with a warning flag, and a nonexistent time becomes NULL.
- Profile before deciding on negative or bracketed amounts.
- Never persist a parsed DOB.
- Reason codes stay separate from severity.
- `CANCELLED` maps to `VOID` (for D2).

### AI choices I reviewed
- Reading the two-digit-year century from Athena's free-text notes. Other sources get NULL with `DATE_CENTURY_UNKNOWN`. In a later review pass I had it check this against the reference file and the brief: only Athena's notes state a century, so the behaviour was kept unchanged, and I approved it.
- No D0/D1 choices remain open.

### Corrections
- Its first `parse_date` would have quietly read a naive `delivered_at` as machine-local time. It caught this in its own review before running tests, and the function now raises.