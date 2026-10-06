# Northwind Health encounter pipeline

## Overview

A local, containerised pipeline that loads the weekly encounter batches in
`data/landing` into a DuckDB warehouse model and exports it as CSV files.
One command processes every pending batch in `batch_id` order:

- **Batch and manifest validation.** Before anything is written, each file is
  checked against its manifest (SHA-256 over the raw bytes, record count), read
  as strict UTF-8 CSV (byte-order mark, CRLF/LF and quoted fields handled), and
  matched against a configured schema contract (`config/schema_contracts.json`).
  Athena's batch_003 layout change is a configuration entry; an unknown change
  rejects the batch. Each batch is all-or-nothing, and every outcome is
  recorded in `ops.batch_audit`.
- **Raw layer (PHI).** Every accepted row is stored exactly as delivered in
  `raw.encounters`, with its lineage: `batch_id`, `file_name`,
  `source_row_number`, file SHA-256 and `ingested_at`.
- **Cleaning and PHI protection.** One unit-tested parser per field keeps the
  source value next to the cleaned value, and every NULL has a reason code.
  Patient identifiers are never written outside the raw layer: `patient_key`
  is an HMAC-SHA256 keyed hash whose key comes from `PATIENT_KEY_HMAC_SECRET`,
  date of birth becomes `age_band`, ZIP becomes `zip3`, and `chief_complaint`
  is left out. Logs are JSON and contain no PHI.
- **Incremental history and current state.** A version is
  `(source_system, source_record_id, last_updated_ts)`, with the timestamp
  compared as a UTC instant. Every raw row gets exactly one outcome: new
  encounter, new version, duplicate, stale or quarantined. History is
  insert-only, the version with the latest timestamp is current, and each
  batch's history step touches only the encounters in that batch.
- **Warehouse model.** Date, facility, diagnosis, payer and patient
  dimensions; a Type 2 provider dimension built from the roster snapshots and
  joined point-in-time on `admit_date`; and encounter facts at version grain
  and at current-state grain.
- **Data quality, quarantine and publish gate.** Every check has a severity:
  ERROR, WARNING or INFO. `ops.quarantine` lists every rejected file,
  quarantined row and ERROR version with reason codes and lineage, and
  `ops.dq_report` gives per-batch counts by check, with row counts reconciled
  across layers. A batch is not published when more than the configured
  threshold of its rows (`DQ_GATE_MAX_ERROR_SHARE`, 5% by default) fail
  error-level checks.
- **Required export.** `chronic_acute_encounters.csv`: one row per qualifying
  current encounter, with `readmit_30d_flag` and lineage to the source batch,
  file and row.

Design decisions and their reasons are in [DECISIONS.md](DECISIONS.md), the
incremental-processing design in [ARCHITECTURE.md](ARCHITECTURE.md), and the
use of AI coding tools in [AI_USAGE.md](AI_USAGE.md).

## Architecture

```
 data/landing/batch_NNN/  manifest.json + encounters_<system>.csv
   (delivered source files, contain PHI; mounted read-only;
    batches in batch_id order, each all-or-nothing)
        |
        v
 1. Validate in memory: manifest, SHA-256, strict CSV, schema
    contract, record counts
        |-- fail --> ops.batch_audit: REJECTED + reason codes (batch_004)
        | pass
        v
 2. Raw layer        raw.encounters                   PHI (persisted)
                     every accepted row as delivered, with lineage
                     raw.ingested_files: file metadata, no PHI
        |
 ==================== PHI boundary ====================================
  raw.encounters is the only table that persists PHI. Patient
  identifiers (MRN, names, date of birth, phone, ZIP) are read from it
  in memory only, to link identities and derive patient_key, age_band
  and zip3. No clean, mart or ops table, CSV or log below holds patient
  names, MRNs, phone numbers, full dates of birth or full ZIPs.
 ======================================================================
        |
        v
 3. History          clean.encounter_row_outcomes,
                     clean.encounter_versions, clean.encounter_current
    Cleaning         clean.encounter_version_fields: encounter fields
                     only (facility, dates, type, claim, payer,
                     diagnosis, NPI, amount), each with its source
                     value, cleaned value and reason
 4. Publish gate     more than DQ_GATE_MAX_ERROR_SHARE (default 5%) of
                     the batch's rows fail ERROR checks: roll back
                     steps 2-4; REJECTED with DQ_GATE_FAILED; failing
                     rows (lineage and codes only) in
                     ops.gate_rejected_issues
        |  steps 2-4 run in one DuckDB transaction per batch
        |  commit: ACCEPTED, reconciled counts in ops.batch_audit
        v
 Rebuilt on every run, after all batches:
 5. Patient privacy  clean.encounter_patients: patient_key
                     (HMAC-SHA256), age_band, zip3, sex
 6. Warehouse        mart.dim_date, dim_facility, dim_diagnosis,
                     dim_payer, dim_provider (SCD2), dim_patient,
                     fact_encounter_version, fact_encounter_current
 7. Data quality     clean.version_dq_issues, ops.quarantine,
                     ops.dq_report
        |
        v
 8. ./output: 17 CSVs, including batch_audit.csv and
    chronic_acute_encounters.csv
```

**Where PHI is:** apart from the delivered landing files, PHI is persisted
only in `raw.encounters`. Patient identifiers are read from it in memory, to
link identities and derive `patient_key`, `age_band` and `zip3`, and are never
written to the `clean`, `mart` or `ops` schemas, the CSVs or the logs. Each
export is checked against the PHI column names before it is written, and a
test scans every exported CSV on the real data pack for MRNs, phone numbers
and patient names.

All schemas are in one DuckDB file. Under Docker that file is on a tmpfs
inside the container and is discarded when the run ends.

## Repository layout

```
src/pipeline/          the pipeline package; entry point src/pipeline/main.py
src/pipeline/parsers/  one parser per Task 2 field
config/                schema contracts and facility aliases
sql/                   the six example queries (q1-q6)
tests/                 unit and integration tests (pytest)
data/                  the data pack: landing/ batches and reference/ files
output/                generated CSVs
Dockerfile, docker-compose.yml, .env.example, requirements.txt
ARCHITECTURE.md, DECISIONS.md, AI_USAGE.md
```

## Prerequisites

- Docker with Docker Compose **v2.24 or later** (`docker compose version`).
  Docker Desktop on Windows or macOS, or Docker Engine with the Compose plugin
  on Linux.
- Git.

Nothing else is needed: no local Python, no `.env` file, no cloud account.
The data pack is part of the repository under `data/`.

## Run

```sh
git clone https://github.com/NanduVelagapudi/skypoint-data-engineering-assessment.git
cd skypoint-data-engineering-assessment
docker compose up --build
```

This builds the image and runs the pipeline once. It:

1. processes `batch_001` to `batch_004` in order, each batch all-or-nothing;
2. rejects `batch_004`. Its Epic file fails the manifest SHA-256 and row count
   checks and has a truncated record. The rejection is a data outcome, not a
   failure: it is reported in `output/batch_audit.csv`, `output/dq_report.csv`
   and `output/quarantine.csv`, and nothing from the batch is published;
3. writes every output CSV to `./output`;
4. exits with code 0. Compose prints `pipeline-1 exited with code 0`.

Exit code 1 means a configuration or system failure, such as a missing
`PATIENT_KEY_HMAC_SECRET` or a missing landing folder. To make the
`docker compose` command itself return the pipeline's exit code, for example in
a script, run `docker compose up --build --exit-code-from pipeline`.

Every run starts from an empty raw database: the raw DuckDB layer lives on a
tmpfs inside the container and is discarded when the container stops. Running
the command again therefore reprocesses all four batches and rewrites the same
outputs; only the run timings in `batch_audit.csv` change. Incremental,
batch-by-batch loading is proven by the test suite (`tests/test_incremental_parity.py`).

Logs are JSON lines on stdout, with batch and step context and no PHI.

## Tests

```sh
docker compose run --rm --build tests
```

This runs the whole pytest suite (parser unit tests, pipeline integration
tests and the incremental-vs-rebuild idempotency test) in the same image as
the pipeline. The data pack is mounted read-only, and tests write only to
temporary folders inside the container, never to `./output`. The `tests`
service is not started by `docker compose up`.

To run part of the suite, pass a pytest command, for example:

```sh
docker compose run --rm --build tests python -m pytest tests/test_manifest.py
```

## Outputs

Every run writes these files to `./output` (bind-mounted from the host), each
written through a temp file and a rename:

| Group | Files |
| --- | --- |
| Required export | `chronic_acute_encounters.csv` |
| Batch audit | `batch_audit.csv` |
| Data quality | `quarantine.csv`, `dq_report.csv`, `version_dq_issues.csv` |
| History (cleaned layer) | `encounter_versions.csv`, `encounter_row_outcomes.csv`, `encounter_version_fields.csv`, `encounter_patients.csv` |
| Facts | `fact_encounter_version.csv`, `fact_encounter_current.csv` |
| Dimensions | `dim_date.csv`, `dim_facility.csv`, `dim_diagnosis.csv`, `dim_payer.csv`, `dim_provider.csv`, `dim_patient.csv` |

None of them contain PHI. The raw DuckDB database, which does, is never written
to the host. On a Linux host the files are owned by root, because the
container runs as root (see DECISIONS.md, Docker).

## Configuration

All configuration comes from environment variables. Compose loads
`.env.example` first, then `.env` if it exists:

- `.env.example` is committed and holds working defaults, including a
  clearly labelled **development-only** HMAC key, so a fresh clone runs with
  no setup.
- `.env` is optional and gitignored. To override a value, copy `.env.example`
  to `.env` and edit it; its values win. Variables exported in your shell do
  not override these files.

| Variable | Default | Purpose |
| --- | --- | --- |
| `PATIENT_KEY_HMAC_SECRET` | dev-only placeholder | Key for the HMAC-SHA256 `patient_key`. Required; changing it changes every `patient_key`. Use a real key only via `.env`, never commit one. |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING` or `ERROR`. |
| `DQ_GATE_MAX_ERROR_SHARE` | `0.05` | Publish gate: a batch is not published when more than this share of its rows fail error-level checks. |

The container paths are fixed in `docker-compose.yml`: the data pack is
mounted read-only at `/app/data`, `./output` at `/app/output`, and the raw
database is `/work/raw.duckdb` on a tmpfs.

## Example queries

The six query patterns from the brief are in `sql/`, one file each, and are
shown below exactly as in those files. They read only the PHI-free `clean`,
`mart` and `ops` schemas, never `raw` (a test checks this). Parameters use
DuckDB's `$name` syntax.

**How they are exercised.** The Docker pipeline's database lives on a tmpfs
and is discarded when the container stops, so nothing is left to query after
`docker compose up`. The test suite runs the queries instead
(`docker compose run --rm --build tests`):

- `tests/test_real_data_exports.py` runs all six on the real data pack
  (batches 001-004 in one run). It checks query 1, and query 5 as of
  batch_002, against independently computed monthly totals, and checks that
  query 5 as of the latest batch equals query 1.
- `tests/test_exports.py` runs them on synthetic batches covering VOID, a NULL
  claim status and versions with Task 6 errors.

### 1. Monthly volume and billed amount, current state, excluding VOID

`sql/q1_monthly_volume_current.sql`. This is the most important use case.
Month is the admit month. `IS DISTINCT FROM` keeps a NULL claim status. An
encounter whose current version has a Task 6 ERROR is left out, with no
fallback to an older version.

```sql
SELECT d.year,
       d.month,
       f.facility_id,
       fac.facility_name,
       f.encounter_type,
       count(*)                AS encounters,
       sum(f.billed_amount_usd) AS billed_amount_usd
FROM mart.fact_encounter_current AS f
JOIN mart.dim_date AS d ON d.date_key = f.admit_date
LEFT JOIN mart.dim_facility AS fac ON fac.facility_id = f.facility_id
WHERE f.claim_status IS DISTINCT FROM 'VOID'
  AND NOT EXISTS (
      SELECT 1 FROM clean.version_dq_issues AS i
      WHERE i.version_key = f.version_key AND i.severity = 'ERROR'
  )
GROUP BY ALL
ORDER BY ALL;
```

### 2. Full version history of one encounter

`sql/q2_encounter_version_history.sql`. Parameters: `$source_system`,
`$source_record_id`.

```sql
SELECT v.version_key,
       v.last_updated_ts_utc,
       v.first_seen_batch_id         AS arrived_in_batch,
       v.arrival_outcome,
       v.claim_status,
       v.billed_amount_usd,
       v.version_key = c.version_key AS is_current
FROM mart.fact_encounter_version AS v
JOIN mart.fact_encounter_current AS c ON c.encounter_key = v.encounter_key
WHERE v.source_system = $source_system
  AND v.source_record_id = $source_record_id
ORDER BY v.last_updated_ts_utc;
```

### 3. Source batch, file and row for a row of the export

`sql/q3_export_row_lineage.sql`. Parameter: `$encounter_key`. Returns the row
that supplied the current version, with the batch audit of its file. A user
with access to the restricted raw layer can continue to `raw.encounters` on
`(source_batch_id, source_file_name, source_row_number)`.

```sql
SELECT f.encounter_key,
       f.source_system,
       f.source_record_id,
       f.source_batch_id,
       f.source_file_name,
       f.source_row_number,
       a.status         AS batch_status,
       a.received_count AS file_received_count
FROM mart.fact_encounter_current AS f
JOIN ops.batch_audit AS a ON a.batch_id = f.source_batch_id AND a.file_name = f.source_file_name
WHERE f.encounter_key = $encounter_key;
```

### 4. Attending provider's specialty and employment status at the encounter

`sql/q4_provider_at_encounter.sql`. `provider_sk` was resolved point-in-time
on `admit_date` (`valid_from <= admit_date < valid_to`); when there is no
provider, `provider_sk_reason` says why.

```sql
SELECT f.encounter_key,
       f.admit_date,
       f.attending_npi,
       p.specialty,
       p.employment_status,
       p.valid_from,
       p.valid_to,
       f.provider_sk_reason
FROM mart.fact_encounter_current AS f
LEFT JOIN mart.dim_provider AS p ON p.provider_sk = f.provider_sk
ORDER BY f.encounter_key;
```

### 5. Monthly totals as known at the end of a batch

`sql/q5_monthly_volume_as_of.sql`. Parameter: `$as_of_batch`, for example
`'batch_002'`. The current version as of that batch is the latest version
first seen in it or earlier. The VOID and Task 6 ERROR rules are as in
query 1.

```sql
WITH known AS (
    SELECT *
    FROM mart.fact_encounter_version
    WHERE first_seen_batch_id <= $as_of_batch
    QUALIFY row_number() OVER (PARTITION BY encounter_key ORDER BY last_updated_ts_utc DESC) = 1
)
SELECT d.year,
       d.month,
       k.facility_id,
       fac.facility_name,
       k.encounter_type,
       count(*)                AS encounters,
       sum(k.billed_amount_usd) AS billed_amount_usd
FROM known AS k
JOIN mart.dim_date AS d ON d.date_key = k.admit_date
LEFT JOIN mart.dim_facility AS fac ON fac.facility_id = k.facility_id
WHERE k.claim_status IS DISTINCT FROM 'VOID'
  AND NOT EXISTS (
      SELECT 1 FROM clean.version_dq_issues AS i
      WHERE i.version_key = k.version_key AND i.severity = 'ERROR'
  )
GROUP BY ALL
ORDER BY ALL;
```

### 6. Everything quarantined or rejected, with reason codes

`sql/q6_quarantined_and_rejected.sql`. Levels:

- `FILE`: a file of a rejected batch, rejected by Task 1 validation or by the
  publish gate.
- `ROW`: a row that could not be placed in version order, or a failing row of
  a gate-rejected batch.
- `VERSION`: a version with a Task 6 ERROR. It stays in the history but is
  excluded from analytics.

```sql
SELECT quarantine_level  AS level,
       quarantine_source AS source,
       batch_id,
       file_name,
       source_row_number,
       source_record_id,
       version_key,
       is_current_version,
       reason_codes,
       reason_detail
FROM ops.quarantine
ORDER BY batch_id, file_name, source_row_number NULLS FIRST, level;
```

## Assumptions

The main calls this implementation makes where the brief is silent. The full
list, with reasons, is in [DECISIONS.md](DECISIONS.md).

- **batch_004 is rejected as a whole.** Its Epic file fails SHA-256, has a
  3-field record 18 and holds 18 records against 22 in the manifest. It is not
  repaired, and the valid Meditech and Athena files in the same batch are not
  loaded. A rejection is a data outcome: the run still exits 0. A processed
  batch, accepted or rejected, is final; a redelivery needs a new batch id.
- **Publish gate.** A batch is not published when more than
  `DQ_GATE_MAX_ERROR_SHARE` of its received rows fail error-level checks: a
  quarantined row, an unresolved facility or an unusable admit date. Warnings
  never count, and a batch exactly at the threshold is published. The
  configured threshold is 5% (`0.05` in `.env.example`, which is also the
  code default). In `output/dq_report.csv`, batches 001-003 have error shares
  of 1.54%, 0.85% and 0.53% and are published.
- **Patient linkage uses only the brief's minimum rule:** normalised last
  name, first given name, date of birth and sex, all four required. There is
  no fuzzy or nickname matching. An identity is `(source_system, MRN)`; an
  identity with no date of birth is never linked across systems. A false
  merge is treated as clinically worse than a false split.
- **Versions, duplicates and stale rows.** Timestamps are compared as UTC
  instants. A row older than the held current version is stale, and this is
  checked before duplicates. Rows of one version that differ only in
  timestamp text or in columns their schema version lacks count as the same
  version. Two different rows with the same timestamp are quarantined, and the
  first arrival is kept. A stale row carrying a version never seen before is
  kept in history but never becomes current. A newest version with a Task 6
  ERROR stays current, flagged and excluded from analytics; there is no
  fallback to an older version. Lineage points to a version's first arrival.
- **Facilities resolve within the row's source system,** by normalisation
  (case, whitespace, hyphen spacing) and 22 explicit aliases in
  `config/facility_aliases.json`, checked against the facility master at load.
  There is no fuzzy or prefix matching. `TEST FACILITY - DO NOT USE` and
  `Westfield Surgical Center` are not in the master and stay unresolved.
  Values seen only in rejected batch_004 add no aliases.
- **NULL with a reason code, never a guess.** An unparseable amount is never
  zero, and fractions of a cent are not rounded. Impossible dates are not
  repaired. Two-digit years are read as 20xx only for Athena, whose reference
  notes state it. ICD-9 codes are not mapped to ICD-10. A source spelling with
  no mapping is not put in a category. A provider not on the roster at the
  admit date gets no fallback to the latest snapshot. A diagnosis missing from
  the reference has an unknown chronic flag, not "N".
- **Other mappings:** `CANCELLED` maps to VOID; 13 encounter-type spellings
  the brief does not list are mapped; the earliest roster snapshot applies to
  all earlier dates; `chief_complaint` is left out of the analytical tables
  rather than redacted.

## Known limitations

- **Local, row-oriented processing.** Version classification runs row by row
  in one Python process. That suits this data pack (about 3,600 rows) but not
  production volumes; ARCHITECTURE.md describes the set-based production
  design.
- **Some tables are rebuilt in full on every run.**
  `clean.encounter_patients`, the mart and the DQ tables are rebuilt from the
  clean layer, because a new batch can re-link a patient or change which
  version is current. This departs from "each batch updates only the records
  it touches"; the history tables do follow that rule.
- **`patient_key` can change** when an identity gains a date of birth or a
  corrected name, since no persistent patient crosswalk is kept. Re-runs on
  the same data always give the same keys.
- **The Docker run starts from an empty database every time.** The raw
  database is on a tmpfs, so the Docker run does not show incremental loading
  across runs, and no database remains to query afterwards. Incremental
  loading and idempotency are proven by the tests.
- **Run timings differ between runs.** `start_time` and `end_time` in
  `batch_audit.csv` change on every run; all other output values are
  identical.
- **Redelivery.** A changed redelivery of an already processed batch is only
  logged, and a changed redelivery of a rejected batch cannot be detected,
  because a rejected batch stores no file hashes.
- **One DuckDB file for every layer.** Raw and PHI-free tables are separated
  by schema, not by access control; production would use separately
  permissioned catalogs.
- **Development-only HMAC key.** `.env.example` holds a labelled placeholder
  so the project runs as cloned. A real key belongs in `.env` locally, or in a
  secret store in production.
- **Output files are owned by root on Linux hosts,** because the container
  runs as root.
- **Linkage is conservative,** so some likely matches stay split: in batches
  001-003, 2 clusters differ only in sex, 3 in first name, 1 in last name, and
  4 date-of-birth pairs look like typos.
- **Only numeric ICD-9 codes are recognised.** ICD-9 V and E codes have the
  same shape as valid ICD-10 codes and are treated as ICD-10.
- **Choices still open for review** are marked in DECISIONS.md, including 18
  of the 22 facility aliases.
- **The optional Bonus** (semantic search over chief complaints) is not
  implemented.
