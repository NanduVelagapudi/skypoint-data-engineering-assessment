# Decisions

Design decisions and their reasons, in build order. Stage 1 decisions were
approved in plan review and are implemented in commits `a7a9d11` to `d8be834`.
In the Task 2 sections, each entry says whether the reviewer approved it or
whether it is an implementation choice still open for review.

## Stage 1: ingestion and raw layer (Groups A–C)

### Stack and structure
- **Python 3.11, DuckDB and pytest, pinned** (`duckdb==1.3.2`,
  `pytest==8.4.1`, `tzdata==2025.2`). DuckDB gives in-process SQL storage with
  no server to run, which keeps the pipeline small and deterministic. CSV
  parsing uses the standard `csv` module. No pandas, Polars, PySpark or dbt.
- **One package, one entry point:** `src/pipeline`, run with
  `python -m pipeline.main`. Small single-purpose modules: config, errors,
  logging, manifest, CSV reader, schema contract, raw store, batch audit,
  batch processor and main.
- **Settings come from environment variables** (`config.py`), with defaults
  for a local run. `RAW_DB_PATH` must resolve outside `OUTPUT_DIR`, because
  the raw database holds PHI and `output/` is shared and committed.
  `.env.example` has a clearly labelled development-only HMAC placeholder for
  Task 3, which Stage 1 does not read.
- **The data pack's bytes are protected in Git** with `data/** -text` in
  `.gitattributes`, so Git never rewrites line endings and the manifest
  SHA-256 values stay valid.

### Validation before loading
- **Manifest first.** `batch_id` must equal the folder name, every entry must
  be well formed, and each file name must equal the schema contract's name for
  its source system, so a manifest cannot point outside the batch folder. A
  listed file that is missing, or an unlisted `*.csv` in the folder, rejects
  the batch.
- **Each file is read once.** SHA-256 is computed over those raw bytes before
  parsing, and the same bytes are then parsed, so the checked bytes are the
  loaded bytes.
- **Row count means parsed CSV records**, excluding the header, counted with
  the `csv` module rather than by counting lines, so quoted commas and quoted
  newlines are handled.
- **Strict CSV reading.** Strict UTF-8, with a BOM stripped if present; the
  encoding is configured per source and never guessed. CRLF and LF both work,
  and every field stays text, exactly as delivered.
- **A record with the wrong number of fields rejects the batch.** It is
  structural file damage, not a row-level quality issue, and quarantine
  belongs to a later stage.

### Schema contracts
- **Known header versions live in `config/schema_contracts.json`**, one list
  of exact, ordered headers per source system. A delivered header is accepted
  only if it exactly matches a configured version, so a known change, such as
  Athena's batch_003 layout (two renames, `encounter_source` added, columns
  reordered), needs a config entry, not code.
- **A header that matches no configured version** is
  `UNKNOWN_SCHEMA_CHANGE` and rejects the batch. That includes a pure reorder
  of an existing version.
- **Fields are mapped to canonical columns by header name**, never by
  position. A canonical column the matched version lacks is NULL; an empty
  string is stored only when the field was delivered empty.
- **Mismatch details name only contract column names, counts and column
  positions.** Delivered header values are never echoed, because a file sent
  without a header would otherwise put its first data row (PHI) into the audit
  and the logs.

### All-or-nothing batches
- **Validate everything in memory, then write once.** Every check runs before
  anything is written, and the batch is then written in one DuckDB
  transaction. Any exception rolls the whole batch back.
- **A rejected batch writes only its `batch_audit` rows.** Failed files carry
  their own reason codes; files that passed carry `SIBLING_FILE_REJECTED` with
  `accepted_count = 0`. A manifest-level failure writes one row with
  `file_name = 'manifest.json'`.
- **batch_004 is rejected as a whole.** Its Epic file fails SHA-256, has a
  3-field record 18, and holds 18 records against 22 in the manifest. It is
  not repaired, and the valid Meditech and Athena files in the same batch are
  not loaded. Tests prove this on the real data pack.
- **Rejection is a data outcome, not a failure.** `python -m pipeline.main`
  exits 0 when the run completes, rejected batches included. It exits 1 only
  for configuration or system failures, such as a missing landing folder, bad
  settings or an unexpected exception.
- **Re-runs.** A batch is pending only if it has no audit row. ACCEPTED and
  REJECTED are both final, so a re-run skips them, and an old batch can never
  be loaded after newer ones. A redelivery needs a new batch id or a manual
  reset. Primary keys on the raw and audit tables also block duplicate loads at
  the database level.
- **Ordering never depends on the filesystem.** Batch folders must be named
  `batch_NNN` and are processed in sorted order; manifest entries are sorted by
  file name.

### Raw layer and lineage
- **`raw.encounters`** holds the five lineage columns (`batch_id`,
  `file_name`, `source_row_number`, `file_sha256`, `ingested_at`) plus one
  VARCHAR per canonical column, with values byte-for-byte as delivered.
  `source_row_number` is the 1-based data-record number. Canonical columns are
  added from the schema contract with `ADD COLUMN IF NOT EXISTS`.
- **`raw.ingested_files`** keeps, per file, the matched schema version, the
  manifest row count, `delivered_at` as delivered, and the delivered header
  before renames, so the original column names are not lost.
- **Rows are loaded as one JSON document per file**, expanded in SQL with
  `from_json`. Measured on the 3,636-row pack, `executemany` took 86 s and a
  columnar `unnest` 59 s, because the Python driver converts each value one at
  a time when numpy and pandas are not installed. The JSON route took 0.08 s
  and was checked to be lossless for NULL vs `""`, quotes, CRLF, NUL and
  non-ASCII text. DuckDB's JSON support is built in, so no dependency was
  added. After each insert, the stored row count is checked against the
  validated file.
- **The database file is attached under a fixed catalog name, `raw_store`.**
  DuckDB names a database after its file, so `raw.duckdb` made
  `raw.encounters` ambiguous (catalog `raw` or schema `raw`).
- **Timestamps are `TIMESTAMP` holding UTC, not `TIMESTAMPTZ`.** Reading
  `TIMESTAMPTZ` back into Python needs `pytz`, which is not a dependency.
  DuckDB also drops a datetime's offset instead of converting it, so the store
  converts to UTC itself and refuses naive datetimes.

### Batch audit
- **One `ops.batch_audit` row per (batch, file)**, with counts, status and
  reason codes, and no row values.
- **`duplicate_count`, `stale_count` and `quarantined_count` are NULL in
  Stage 1,** meaning "not evaluated yet", rather than a misleading zero.
- **`output/batch_audit.csv`** is sorted by `batch_id` and `file_name`, and
  written through a temp file and a rename, so a failed export never leaves a
  partial file.

### Logging and PHI
- **JSON lines on stdout.** Only an allow-list of context keys is emitted
  (step, batch, file, source system, counts, status, reason code, error type,
  duration); any other key is dropped.
- **Exceptions log their type only,** never the message or traceback, because
  a third-party message could quote source data.
- **Rows are referred to by batch, file and record number,** never by value.
  A sentinel test (`tests/test_no_phi_leak.py`) puts a PHI-like value into
  each failure path that sees delivered content, then checks the logs, the
  audit rows and the CSV.
- **The entry point's logger is named `pipeline.main` explicitly.** Under
  `python -m`, `__name__` is `__main__`, which is outside the configured
  logger.

## Task 2 parsers: common structure (D0)

- **One result type for every field parser.** Each parser returns a
  `ParseResult` (`src/pipeline/parsers/result.py`) holding `raw_value`,
  `cleaned_value`, `reason_code` and `warning_flag`. Exactly one of
  `cleaned_value` and `reason_code` is set, so every NULL has a reason, and the
  raw value travels next to the cleaned one, as Task 2 requires.
  `warning_flag` marks something notable about a valid value; today it is only
  used for an ambiguous local time.
- **Reason codes, not severity.** All field-level codes are in one enum,
  `FieldReason`, kept apart from the batch-rejection codes in
  `errors.ReasonCode`. Parsers never decide whether a code is an error or a
  warning; Task 6 will map codes to severity. *(Approved.)*
- **Pure parsers.** Parsers take a raw value and the source's conventions as
  arguments. They do not read files, write to DuckDB, log, or raise on bad
  data. They raise only on a caller bug, such as a naive `delivered_at`.
- **No values in `repr`.** `ParseResult` leaves `raw_value` and
  `cleaned_value` out of its `repr`, so a logged result or a test failure
  message cannot print a patient value.
- **Conventions come from the reference metadata.** Date order, amount unit
  and timestamp time zone are read from
  `data/reference/source_systems_and_facilities.json`
  (`src/pipeline/source_conventions.py`). They are not copied into `config/`
  or hard-coded per system.

## Task 2 parsers: amounts (D1)

- Amounts are `Decimal` with exactly two decimal places. No float is used.
- Nothing is rounded. A value with fractions of a cent is NULL with
  `AMOUNT_FRACTIONAL_CENTS`. *(Approved for Meditech; the same rule is applied
  to USD sources, for example `12.345`.)*
- `LEGACY_MEDITECH` (`USD_CENTS`): the integer number of cents is divided by
  100.
  - A trailing `.0` is valid. *(Approved.)* All 59 Meditech decimal values in
    batches 001–003 are `.0`.
  - A `K` suffix is NULL with `AMOUNT_K_SUFFIX_NOT_ALLOWED`; no conversion is
    invented. *(Approved.)*
  - A `$` or `USD` marker on a Meditech amount is NULL with
    `AMOUNT_UNPARSEABLE`, because it makes it unclear whether the number is
    cents or dollars. The data pack has no such value. *(Approved.)*
- USD sources: `$`, `USD` before or after, stray whitespace and correctly
  grouped thousands separators are accepted, with at most one currency
  marker. A `K` suffix multiplies by 1,000.
- Negative and bracketed amounts: none occur in the accepted batches, and the
  brief gives no rule for them, so they are NULL with `AMOUNT_UNPARSEABLE`
  rather than given an invented meaning.
- Invalid text is NULL with a reason and never zero:
  - blank: `AMOUNT_MISSING`;
  - `N/A`, `PENDING`, `TBD`: `AMOUNT_PLACEHOLDER`;
  - spreadsheet errors such as `#VALUE!`: `AMOUNT_SPREADSHEET_ERROR`;
  - anything else, including free text and `12..50`: `AMOUNT_UNPARSEABLE`.
- Only ASCII digits are accepted, so non-ASCII digits are not silently
  converted.

## Task 2 parsers: dates (D1)

- Supported formats:
  - ISO, with or without a `T` time. The time is checked, then dropped.
  - Numeric with `/` or `-`; both separators must match.
  - `Mon D, YYYY`, `Month D, YYYY` and `D Mon YYYY`.
- Numeric dates use the source's date order (Epic and Athena MDY, Meditech
  DMY). Month-name and ISO dates are unambiguous and ignore it.
- Impossible dates (`02/30/2024`, month 13) are NULL with `DATE_INVALID`.
  They are never swapped or repaired.
- A date after the batch's delivery is NULL with `DATE_AFTER_DELIVERY`. The
  cutoff is the UTC calendar date of the manifest's `delivered_at`, which the
  caller passes in; the delivery day itself is allowed.
- **Two-digit years:** 20xx for Athena only. *(Reviewed against the reference
  and the brief; approved.)*
  - The brief requires two-digit years to be supported and calls the
    reference metadata the only documentation of source conventions.
  - The reference has no structured field for a century. Only Athena's
    `notes` state one ("two-digit years mean 20xx"); the Epic and Meditech
    notes say nothing about years.
  - So the century is read from the notes with one strict pattern. A source
    whose notes state no rule gets NULL with `DATE_CENTURY_UNKNOWN` instead
    of a guessed century.
  - In batches 001–003, two-digit years occur only in Athena admit and
    discharge dates, so no accepted Epic or Meditech value is affected.
- A discharge before admission is not a parsing failure. Neither date is
  nulled. `discharge_before_admit()` reports `DISCHARGE_BEFORE_ADMIT`
  separately.
- Year, quarter and month are derived from the parsed `admit_date`. The date
  dimension is built later.
- **Patient DOB.** `patient_dob` uses the same parser, but only in memory, for
  Task 3's age band and patient linkage. A parsed DOB is never stored in any
  table, column or output. *(Approved.)*

## Task 2 parsers: timestamps (D1)

- `last_updated_ts` becomes a timezone-aware UTC `datetime`, not a string.
- An explicit `Z` or `±HH:MM` offset in the value wins. A value without one is
  wall-clock time in the source's reference zone: UTC for Epic and Athena,
  `America/Chicago` for Meditech.
- Daylight saving in `America/Chicago`:
  - An ambiguous fall-back time takes the earlier valid instant and sets
    `warning_flag = TIMESTAMP_AMBIGUOUS_LOCAL_TIME`. *(Approved.)*
  - A nonexistent spring-forward time is NULL with
    `TIMESTAMP_NONEXISTENT_LOCAL_TIME`. *(Approved.)*
- Time-zone rules come from the standard library's `zoneinfo` and the
  `tzdata==2025.2` package already pinned in `requirements.txt`. No dependency
  was added.

## Task 2 parsers: categorical fields (D2)

- **The brief fixes only the target categories,** not the source spellings,
  so every source-to-category mapping is a decision. The mappings live in one
  table per field in `src/pipeline/parsers/categorical.py`, the code option
  the brief allows, so no new config mechanism was needed. Together the tables
  cover every spelling in batches 001–003. *(Approved.)*
- **Spellings are matched on a normalised key:** upper case, runs of
  whitespace collapsed, and no spaces around hyphens. That is how the data's
  `Closed - Paid` matches `Closed-Paid`. No other fuzzy matching is done.
- **encounter_type.**
  - The specified spellings: `ED`, `ER`, `Emergency Room`, `EMERGENCY DEPT` →
    EMERGENCY; `IP`, `INPT`, `IN-PATIENT`, `inpatient admission` → INPATIENT;
    `OBS`, `Obs Stay` → OBSERVATION; `OTHER` → UNKNOWN. The literal category
    names map to themselves.
  - 13 further spellings in about 2,000 of the 3,636 rows had no mapping in
    the brief, and they are mapped as well: `OP`, `Out Patient`,
    `Office Visit`, `Clinic Visit` → OUTPATIENT; `TELEMED`, `Video Visit`,
    `Virtual` → TELEHEALTH; `UC`, `Walk-in Urgent` → URGENT_CARE. Leaving them
    unmapped would have left over half the rows without an encounter type.
    *(Approved.)*
- **claim_status:**
  - `Paid in Full`, `Closed-Paid`, `Paid` → PAID;
  - `Denied`, `Rejected` → DENIED;
  - `Pending`, `In Process`, `Submitted` → SUBMITTED;
  - `Void`, `VOIDED`, `CANCELLED`, `Cancelled` → VOID.

  *(Approved, including CANCELLED → VOID, which was decided before D2.)*
- **payer_name → payer_category.** The raw payer name is kept alongside the
  category. *(Approved.)*
  - MEDICARE: `Medicare`, `MCR`, `MEDICARE PART A`, `Medicare - Part B`.
  - MEDICAID: `Medicaid`, `IA Medicaid`, `State Medicaid Plan`, and
    `BadgerCare Plus`, which is Wisconsin's Medicaid program.
  - COMMERCIAL: `Aetna`, `AETNA INC`, `Cigna`, `CIGNA HEALTH`,
    `UnitedHealthcare`, `UHC`, `BCBS`, `Blue Cross Blue Shield`.
  - SELF_PAY: `Self Pay`, `self-pay`, `SELFPAY`, `Uninsured`.
  - OTHER: `TRICARE`, `Workers Comp`.
  - UNKNOWN: `N/A`.
- **Blank and unmapped values.** *(Approved.)*
  - A blank encounter_type or payer becomes UNKNOWN with a `*_MISSING`
    warning flag, so the blank is still visible.
  - A blank claim_status is NULL with `CLAIM_STATUS_MISSING`, because the
    brief has no UNKNOWN status.
  - A spelling in no table is NULL with `*_UNMAPPED`. It is never put in
    UNKNOWN or any other category, so a new source spelling shows up instead
    of being silently counted. None occurs in batches 001–003.

## Task 2 parsers: diagnosis codes and NPIs (D2)

- **ICD-10 normalisation:** strip, drop a trailing description after the
  first `-` (ICD-10 codes never contain `-`), upper-case, and insert the dot
  after the third character when it is missing. The result must match the
  ICD-10 shape: a letter, a digit, an alphanumeric, then optionally a dot and
  1–4 alphanumerics.
- **The brief's four outcomes, kept distinct:** *(Approved.)*
  - in the reference: the code;
  - valid format but not in the reference: the code is kept, with warning
    flag `DX_NOT_IN_REFERENCE`;
  - ICD-9: NULL with `DX_ICD9`, and never mapped to ICD-10;
  - anything else: NULL with `DX_UNPARSEABLE`, `DX_MISSING` or
    `DX_PLACEHOLDER`.
- **Only numeric ICD-9 codes are recognised** (three digits, optionally `.d`
  or `.dd`), as seen in Meditech. ICD-9 V and E codes have the same shape as
  valid ICD-10 codes, so telling them apart would be a guess. None occur in
  the data.
- **The ICD reference is loaded outside the parser**
  (`src/pipeline/reference_data.py`) and checked to be in the normalised
  format. The parser receives the codes as an argument, so it does no file
  I/O.
- **NPI cleaning is limited to harmless formatting:** surrounding whitespace
  and a trailing `.0` left by a spreadsheet storing the NPI as a number (80
  Athena values). Prefixes, separators and letters are not stripped; they make
  the value `NPI_INVALID_FORMAT`.
- **NPI validation follows CMS:** exactly 10 digits, with the 10th digit the
  Luhn check digit of `80840` plus the first nine. A failure is
  `NPI_CHECKSUM_FAILED`.
- **Roster presence is a separate check.** `npi_not_in_roster()` reports
  `NPI_NOT_IN_ROSTER` for a valid NPI found in none of the roster snapshots it
  is given, and the NPI stays valid. Loading the rosters and point-in-time
  provider lookup belong to a later step. *(Approved.)*

## Approved for later groups (not implemented yet)

- **The Docker container runs as root**, with no `USER` directive. A
  bind-mounted `./output` owned by a different user on a Linux host could make
  a non-root container fail to write, which would break the required exit
  code 0 on the assessor's machine. The cost is less privilege separation
  inside the container; in production the job would run as a dedicated
  non-root identity with storage permissions set to match. `Dockerfile` and
  `docker-compose.yml` are still empty; this applies when Docker is built.
