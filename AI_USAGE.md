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

## Task 2 parsers (D2): categorical fields, ICD-10, NPI

### Where it saved time
- Checked my D2 prompt against the brief. The prompt said the assessment lists source spellings such as `ED / ER / Emergency Room`. Claude Code searched the PDF text and confirmed the PDF contains no images, then reported that the brief lists only the target categories. The spelling mappings are therefore recorded as my decisions in DECISIONS.md.
- Profiled batches 001–003 before writing code. Categorical labels were listed with exact counts; diagnosis codes and NPIs only as masked shapes and aggregate counts. This found:
  - 13 encounter-type spellings (about 2,000 rows) that my prompt's mapping list did not cover;
  - that the data writes `Closed - Paid`, not `Closed-Paid`;
  - 14 numeric ICD-9 codes, all in Meditech;
  - 80 Athena NPIs with a trailing `.0`.
- Ran the finished parsers over all 3,636 accepted rows and reported counts only. Every count matched the profile, and no value was unmapped.
- Checked the NPI check digit against the CMS worked example, and in the tests against an independently written Luhn function.

### Decisions I made, not the AI
- Claude Code asked four questions before writing code, and I chose its recommended option each time:
  - map the 13 extra encounter-type spellings;
  - use the proposed payer table, including BadgerCare Plus → MEDICAID and Uninsured → SELF_PAY;
  - make unmapped values NULL with a reason, and blank values UNKNOWN with a warning flag;
  - keep a valid ICD-10 code missing from the reference, with a warning flag, and check roster presence for NPIs separately.
- CANCELLED → VOID was decided before D2.

### Corrections
- No D2 test failed during the build.

## Task 2 facility resolution (D3)

### Where it saved time
- Profiled every `facility_name` value per source system in batches 001–003 (36 distinct values; organisation names, not PHI). Each value turned out to be one of:
  - an exact master name;
  - a master name that differs only in case or whitespace;
  - an abbreviation or spelling variant with a single candidate in its own system;
  - one of two names not in the master.
- Checked the three points in my prompt against the files rather than assuming them:
  - the master has 8 facilities;
  - `Saint Brendan Medic` occurs only in rejected batch_004;
  - `Westfield Surgical Center` and `TEST FACILITY - DO NOT USE` are not in the master.
- Ran the resolver over all 3,636 accepted rows and reported counts only. The per-facility totals matched the profile: 3,604 resolved and 32 unresolved.

### Decisions
- Mine, set in my prompt: resolve within the source system, be conservative, no fuzzy matching unless the data proves it necessary, and no aliases built from rejected data.
- Claude Code's, which I then reviewed:
  - **The 22 aliases**, each a single-candidate variant within its own system.
    - I reviewed and approved the four shortened aliases: `Lakeshore General` → FAC001, `Riverbend CH` → FAC004, `Harbor Point BH` → FAC005 and `Cedar Valley Clinic` → FAC006. I checked that each is an exact value observed in accepted batches 001–003, is scoped to its own source system, has exactly one candidate facility there, and resolves by deterministic exact matching without fuzzy matching.
    - The remaining 18 aliases are still open for my review.
  - **Keeping the aliases in `config/facility_aliases.json`**, checked against the facility master at load time. This is still open for my review.

### Corrections (D3)
- Its first batch_004 check skipped records with the wrong field count. That skipped the truncated record 18, the only place `Saint Brendan Medic` occurs, so the check came back empty. It noticed the empty result and re-checked that record's facility field alone, without printing any other field.
## Task 3: protecting patient data

### Where it saved time
- Before writing any code, it ran a read-only profile of batches 001–003 that printed only counts and patterns. It covered MRN shapes, DOB validity, name punctuation and middle initials, sex labels, linkage under the minimum rule, near-miss categories, free-text identifier patterns, age bands and ZIP shapes. This gave the expected counts that the real-data tests now pin.
- It checked the finished stage against the real data through an in-memory DuckDB, so no files were written, and reported counts only. Every expected figure matched: 1,056 identities, 1,044 linked, 895 person groups, 142 cross-system groups, 0 same-system collisions, and the five age-band counts.
- It wrote the HMAC, linkage and cleaning code with 108 new tests. These cover name, sex, age-band and ZIP rules; HMAC payloads checked against an independent `hmac` call; source-scoped unlinked keys; and a missing secret. They also check that no PHI value or secret reaches the cleaned table, the logs or an error.

### Decisions I made, not the AI
- The Task 3 rules in my prompt:
  - the four-field linkage, with no fuzzy, nickname or fallback rule;
  - the LINKED and UNLINKED namespaces, with source-scoped unlinked keys;
  - the secret only from `PATIENT_KEY_HMAC_SECRET`, failing before any cleaned output;
  - `chief_complaint` left out, not redacted;
  - DOB never persisted;
  - documenting that a key can change once an identity becomes linkable.
- **My removal of `sex_raw`.** Claude Code's first cleaned table stored the raw sex label (`Female`, `M`, …) as `sex_raw` next to the normalised `sex`. I had it removed on the minimum-necessary principle: nothing downstream needs the source spelling, and it only duplicates `sex`. The raw value is still read and normalised in memory, and stays only in the restricted raw layer.
- **My override on single-letter first names.** In its inspection, Claude Code proposed dropping every single-letter token from the first name and taking the first remaining token, which would discard a legitimate one-letter first name. My Task 3 rule instead takes the first token as the given name and ignores only the tokens after it.

### Corrections
- **Wrong source of `source_system`.** Its first cleaning stage read the row's own delivered `source_system` column instead of the file's validated source system in `raw.ingested_files`. Real data happens to agree, but synthetic test rows do not, so 9 existing tests failed. It now uses the validated value.
- **Over-broad PHI test.** Its first PHI-leak test treated every synthetic patient field as PHI, including sex. Sex is not PHI, and at that point the raw label was stored as `sex_raw`, so the test would have failed wrongly. It noticed this before running the test and limited the check to the PHI columns.
- **Shell quoting during profiling.** Two of its read-only profiling commands failed in the bash tool because the Python scripts held an unpaired single quote. Nothing ran. It reran the same scripts through PowerShell.

### Choices still open for my review
- The cleaned table: its name, grain and columns; rebuilding it in full on each run; and keeping it in the same DuckDB file under a separate `clean` schema.
- `PATIENT_UNLINKED_CONFLICT` for an identity whose rows disagree, no key for a blank MRN, ZIP+4 accepted for ZIP3, and the new `REFERENCE_DIR` setting.

## Task 4: incremental processing and history

### How I ran it
- First a read-only design review: no repository changes, profiling allowed only in a scratch DuckDB outside the repository. Then implementation in groups, in manual-approval mode, with a pause after each group. Group (a) is the classifier, the history tables, the current-state view and their tests.

### Where it saved time
- Profiled batches 001–003 by loading copies through the pipeline's own batch processor and parsers into a scratch database. It printed counts and `source_record_id` values only. Findings:
  - **Volumes:** 3,274 encounters and 3,541 distinct versions.
  - **Duplicates:** 30 within batch_001, and 40 in batch_002 duplicating the then-current version.
  - **Stale rows:** 25 in batch_003, all replays of a batch_001 version that batch_002 had superseded.
  - **Two in-batch patterns:**
    - Nine Athena batch_003 replays differ from the held version only in timestamp format and the new `encounter_source` column, so comparing raw text would miss them.
    - Three batch_002 encounters have their new version before a re-sent copy of the current version in the file, so processing rows in file order would wrongly mark the copy as stale.
  - **Ordering by text would be wrong:** 20 Meditech encounters sort differently as day-first text, and ignoring Athena v2's offset would change 8 encounters' current version.
- Simulated the two possible classification orders before I chose one. Checking duplicate first gives 86 duplicates and 9 stale rows; checking stale first gives 70 and 25. The modelled state is the same either way; only the audit labels differ. The same simulation showed that history built batch by batch equals history built from the whole set of rows at once.
- Ran the finished classifier on a scratch copy of the real batches. Every per-file count matched the approved design table: 3,541 versions and 3,274 current encounters. The test that proves incremental and rebuild runs give identical results comes in group (c).

### Decisions I made, not the AI
- Stale is checked before duplicate. A version is `(source_system, source_record_id, UTC last_updated_ts)`, with a fingerprint guard and the absent-column rule.
- A never-held older version goes to history but never becomes current. The newest version stays current even when it is quarantined; it is flagged and kept out of analytics.
- A same-timestamp conflict is quarantined and the first arrival kept. The current version's lineage points to its first arrival.
- `accepted_count` means rows that created a version. There is a rebuild-from-raw path. A duplicate file or a redelivered `batch_id` is flagged only in the audit reason and the logs. There is no late-arrival class, history holds only the version spine, and `encounter_patients` stays a full rebuild.
- Task 4 quarantine covers only rows that cannot be placed in version order. Task 6 data-quality failures will be a flag on the version, not an outcome.

### AI choices I reviewed
- **Python classifier instead of SQL.** Its design review proposed classifying in SQL. When implementing, it wrote the classifier as a pure Python function instead and flagged the change itself. Its reasons: the function can be unit-tested and permutation-tested, and timestamp parsing is already in Python. SQL still loads only the touched encounters' history and writes the results. I accepted this and had the trade-off and the production design (set-based classification and an insert-only `MERGE` on `version_key`) written up in ARCHITECTURE.md.

### Corrections
- **Two wrong test expectations.** One of its storage tests expected `STALE` for a re-sent copy of the version that was current before the batch. Under the approved rule (compare with the state before the batch), that row is a `DUPLICATE`. Another assumed the wrong file row for an encounter's first arrival. Both were test errors, and it fixed the tests, not the code.
- **A profiling script crashed** (`TypeError`) on its first run because of a convoluted branch in the duplicate-first simulation. It simplified the branch and reran it. Nothing was written to the repository.

### Choices resolved after review
Claude Code raised these three points after group (a). I approved its proposal for each:
- Same-timestamp conflicts (`VERSION_CONFLICT_SAME_TS`) count in `quarantined_count`, so that received = accepted + duplicate + stale + quarantined still holds.
- `accepted_count` excludes stale rows that create a history version. They are counted as stale, and `history_rows_written` reconciles the difference.
- A stale replay whose values differ from the held version stays `STALE`, with `match_type = CONFLICT`, because the stale check runs first.
