# AI usage

## Tools and how I used them
- **Claude Code (VS Code extension, Opus 5.5)**: wrote the Stage 1 code and tests, and the Task 2 parsers (D0/D1); its work on later tasks and on the documentation is recorded in the sections below. I ran it in plan mode first, then in manual-approval mode with no auto mode. Stage 1 was built in four reviewed groups. I read each diff before accepting it and committed each group myself.
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
- **Container user:** its plan ran the container as non-root UID 1000. I switched to root, because a bind-mounted `./output` owned by another user on a Linux host could fail the run and break the required exit code 0. The trade-off is recorded in DECISIONS.md (Task 8: Docker), and the Dockerfile has no `USER` directive.
- **Missing columns:** I required NULL, not an empty string, for a canonical column a schema version lacks (for example `encounter_source` before Athena v2). An empty string is stored only when the field was delivered empty.
- **Auto mode:** I declined the "auto mode" and "allow all edits" options on every prompt.
- **Docs after code:** I scheduled README and DECISIONS to be written after the code they describe. README.md, ARCHITECTURE.md and DECISIONS.md now cover the work through Task 8 (see "Documentation" below).

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

## Task 6: data quality and observability

### What it did
- Claude Code implemented the DQ check catalogue and its severity mapping (`dq_rules.py`), the publish gate, the version-level issues, the quarantine and the DQ report, with their tests (commits `be244cf` and `bedac3b`).
- When I later had it document Task 6 in DECISIONS.md, it wrote the section from the code and tests: the severity of every check, the gate rule, the quarantine levels and the DQ report, with counts read from the generated outputs.

### Decisions I made, not the AI
- **The 5% publish-gate threshold is my design judgment.** It is not a requirement of the brief, which only asks for a threshold that batches 001-003 pass, and not something the AI derived. My rationale: roughly three times the highest error share of an accepted batch (batch_001, 1.54%).
- **Requirements and judgments are kept apart.** I required DECISIONS.md to say which reasons come from the brief and which are our design judgments: the admit-date ERROR, the rationale for WARNINGs, and rejecting a batch for an undecodable, unparseable or malformed file.

### Corrections
- **An unrecorded rationale.** No document or commit gave the reason for treating an unusable admit date as an ERROR. It wrote one from dependencies in the code and flagged that it had done so. I had it marked as a design judgment, and its "every analytical use depends on it" narrowed to "the main analytical uses".
- **Its consistency review of its own section** found three reasons written as if the brief required them (the admit date, the warnings, the file-level rejections) and two gaps (`SCHEMA_VERSION_CHANGED` was not defined; the DQ report's special cases were missing). All were corrected.
- **A claim I removed.** At my request it added "Status: approved in design review" to match Tasks 4 and 5, and noted that it could not verify such a review. I removed the line.
- **One of my instructions was wrong.** I asked it to document that a batch rejected at file validation gets only batch-scope DQ report rows. It checked the code and the outputs (batch_004 has 153 file-scope rows) and documented the actual behaviour: only a batch rejected at the manifest gets batch-scope rows only.

## Task 7: chronic_acute_encounters.csv

### What it did
- Claude Code implemented the export (`chronic_acute_export.py`) and its tests (commit `9039e06`), including the check that the file is byte-identical across incremental, one-shot, rerun and rebuild runs.

### Decisions
- The rules are recorded in DECISIONS.md, Task 7: the brief's six conditions, the Task 6 ERROR exclusion as in queries 1 and 5, a NULL claim status kept, the 30-day readmission search over all current encounters, point-in-time provider attributes and first-arrival lineage. DECISIONS.md does not mark them as approved.
- Two implementation choices are still open for my review: what "valid dates" means for the follow-up encounter, and using a row's own `discharge_date` as stored when it is before its admission.

## Task 8: Docker

### How I ran it
- Three steps, each ending with a stop for my approval: a read-only inspection and design, a second read-only review of that design against the assessment PDF, then the implementation. Claude Code did not build the image, run the containers or commit anything; I ran the verification.

### Where it saved time
- Found what `docker compose up --build` was missing: `Dockerfile`, `docker-compose.yml` and `README.md` were empty, there was no `.dockerignore`, and a fresh clone with no `.env` had no source for the required `PATIENT_KEY_HMAC_SECRET`, so the run would exit 1.
- Confirmed the application needed no Docker-specific change: every path is an environment variable, the batch_004 rejection already exits 0, and the tests only read the data pack and write to temp folders, so a read-only mount works.
- Checked which tests depend on the repository layout (`REPO_ROOT / "data"`, `sql/`, `.env.example`) and laid the image out at `/app` to match, so the tests run unchanged in the container.
- Looked up the newest published `python:3.11.x-slim-bookworm` tag (3.11.17) with read-only manifest checks, to pin the base image.

### Corrections: its first design
I had it re-check its first design against the assessment PDF before implementing anything. That review found that the first design:
- had no tests container and excluded `tests/` from the image, although Task 8 requires the tests to run in a container with one documented command;
- kept the raw DuckDB database in a persistent named volume. A repeated `docker compose up --build` would then skip all four batches as already processed, the volume would outlive code changes (a pre-Task-4 database there makes the run exit 1), and raw PHI would stay in Docker storage after the run;
- set the development-only HMAC key as a default in `docker-compose.yml`, a second committed copy that the brief does not allow for (it allows a labelled one in `.env.example`);
- mounted the data pack at `/data`, which would break the tests' `REPO_ROOT / "data"` path.

### Decisions I made, not the AI
- The dev-only key lives only in `.env.example`. Compose loads that file, then an optional, gitignored `.env` that overrides it; the key is never in `docker-compose.yml`.
- The raw DuckDB database is on a tmpfs at `/work`, empty on every pipeline run, so no raw-data state persists locally between runs. No persistent volume.
- No new dependencies: the existing pinned set stays, and the only change is pinning pytest's own dependencies at the versions already installed.
- Two Compose services, `pipeline` and `tests`; `./data` mounted read-only, `./output` read-write.
- Scope of the Task 8 step: README gets only the Task 8 sections, ARCHITECTURE.md is unchanged, and the generated outputs are committed separately after verification. README and ARCHITECTURE.md were completed later (see "Documentation").
- The container runs as root. Its plan used non-root UID 1000; I changed it because a bind-mounted `./output` owned by another user could make the run fail and break the required exit code 0 (see "Where I overrode it").

### Unplanned but necessary change
- It added `.env.example text eol=lf` to `.gitattributes`, outside the file list I approved, and said so in its report. Compose now reads `.env.example` directly, and a CRLF checkout on Windows could otherwise put a carriage return into the HMAC key and change every `patient_key`. The edit also removed a stray carriage return from the existing `Dockerfile text eol=lf` line.

### Its own errors, found in review
At my request it reviewed its Task 8 files against the brief. It found three inaccuracies in its own work, and I had them corrected:
- the README clone command still had a placeholder URL;
- the `config.py` docstring said Docker sets every value in `docker-compose.yml`, but three come from `.env.example`;
- DECISIONS.md said "every installed package is pinned", which overstated it: the packages in `requirements.txt` and the base image tag are pinned.

### What I verified myself
- `docker compose up --build` exited with code 0. batch_001-003 were accepted and batch_004 was rejected, and the rejection did not change the exit code.
- A second `docker compose up` also exited with code 0.
- `./output` held all 17 CSV files.
- `docker compose run --rm --build tests` passed 1038 of 1038 tests inside the container.

## Documentation: README, ARCHITECTURE and DECISIONS

### What it did
- **README.md:** drafted the Task 8 sections, then the rest: overview, an architecture diagram with the PHI boundary, the repository layout, the six example queries, assumptions and known limitations. It copied the queries from `sql/` and checked them byte for byte against the files.
- **ARCHITECTURE.md:** compared the old file with the brief. It covered only Task 4 and had six problems: a future-tense statement about implemented behaviour, an incomplete description of the batch transaction, a scale claim that ignored the per-run rebuilds, a hypothetical parity test that could be confused with the existing one, an absolute claim about Databricks transactions, and a Task 4-only scope. It then rewrote the document, and I had it trimmed from 526 to 415 lines.
- **DECISIONS.md:** added the Task 6 section and reviewed it for consistency (see Task 6).

### Decisions I made, not the AI
- I had every document checked against the assessment PDF before it was changed.
- **Current vs proposed.** The current implementation (Python and DuckDB in Docker) and the proposed production design are kept clearly apart, and every production section is labelled as proposed.
- **Platform.** Azure Databricks is the primary platform, with Snowflake in one sentence, and Databricks Workflows with a file-arrival trigger, with Azure Data Factory as the alternative.
- **No unsupported production claims.** No production control (access control, masking, encryption, CI/CD) is presented as implemented, and SLA numbers are labelled as proposed targets.
- **README.** No rationale for the 5% threshold in the README, and an explicit PHI boundary in its diagram: `raw.encounters` is the only persisted PHI.

### AI choices I reviewed
- Drafts it wrote for my review, not decisions: the proposed SLA targets, the six-week plan for three engineers and the pull-request checklist.
- From four candidate risks I gave it, it chose three for the top-three list and covered `patient_key` changes during key rotation in the PHI governance section instead.

### Corrections
- **Unverified claims flagged.** Its README review marked a claim it could not yet support: that a repeat run changes only the run timings. The claim stayed only after it found the evidence in `test_incremental_parity.py`, which compares the CSVs excluding only `start_time` and `end_time`.
- **A mismatch in my own request.** The SQL file names in my README request did not match the repository. It used the real files.
