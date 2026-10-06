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
  `warning_flag` marks something notable about a valid value; at D0 it was
  only used for an ambiguous local time, and later decisions added more flags
  (for example `*_MISSING` and `DX_NOT_IN_REFERENCE` in D2).
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

## Task 2 facility resolution (D3)

- **Resolution is within the row's source system,** as the brief says. The
  lookup key is `(source_system, normalised name)`, so the same name in two
  systems can never resolve across them. Each facility in the master belongs
  to exactly one source system. *(Required by the reviewer.)*
- **No fuzzy matching.** The brief allows "an alias table, fuzzy matching or
  both". Normalisation plus an explicit alias table resolves every accepted
  value that has a facility in the master, so a fuzzy threshold would add
  false-merge risk and resolve nothing extra. There is no partial or prefix
  matching either.
- **Normalisation** reuses the categorical key: upper case, runs of
  whitespace collapsed, and no spaces around hyphens. Punctuation is not
  removed, so `St.` vs `St` is handled by an alias, not by normalisation.
  This keeps every non-trivial match visible in the alias table.
- **Aliases live in `config/facility_aliases.json`,** grouped by source system
  and facility id. They are curated mappings onto IDs from the external
  facility master, so at load time they are checked against it: the facility
  must exist, belong to that source system, and have the master name written
  next to it. Unlike the categorical tables, which map onto categories fixed
  by the brief, this table is master-data configuration, so it lives in
  `config/`. *(Open for review.)*
- **The 22 aliases** cover every case, abbreviation and spelling variant in
  batches 001–003. Each has exactly one candidate facility with that name in
  its own source system. *(The four shortened aliases are reviewed and
  approved, see below; the other 18 are open for review.)*
  - EPIC_NORTH:
    - FAC001: `Lakeshore Gen Hosp`, `Lakeshore General`
    - FAC002: `St Brendan Medical Center`, `ST BRENDAN MED CTR`,
      `Saint Brendan Medical Center`, `St. Brendan's Medical Center`
    - FAC003: `Maple Grove Pediatric Clinic`, `MAPLE GROVE PEDS`
  - LEGACY_MEDITECH:
    - FAC004: `Riverbend CH`, `Riverbend Community Hosp.`,
      `RIVERBEND COMM HOSP`
    - FAC005: `HARBOR PT BEHAVIORAL HLTH`, `Harborpoint Behavioral Health`,
      `Harbor Point BH`
  - ATHENA_CLINICS:
    - FAC006: `Cedar Vly Family Clinic`, `Cedar Valley Clinic`
    - FAC007: `EASTGATE UC`, `Eastgate Urgent Care Center`,
      `East Gate Urgent Care`
    - FAC008: `Summit Ridge Ortho`, `SUMMIT RIDGE ORTHOPAEDICS`,
      `Summit Ridge Orthopedic Clinic`
- **The four shortened aliases:** `Lakeshore General` → FAC001,
  `Riverbend CH` → FAC004, `Harbor Point BH` → FAC005 and
  `Cedar Valley Clinic` → FAC006. These shorten the master name rather than
  just abbreviating or re-spelling it, so they were reviewed manually. Each one:
  - is an exact value observed in accepted batches 001–003;
  - is scoped to its own source system;
  - has exactly one candidate facility in that source system;
  - resolves by deterministic exact matching, with no fuzzy matching.

  *(Reviewed and approved.)*
- **Ambiguity is a configuration error,** not a runtime outcome. Building the
  index refuses any name or alias that would point at two facilities in the
  same system, so a resolution can never be ambiguous.
- **Unresolved values are NULL with a reason and never guessed:**
  `FACILITY_MISSING` for a blank, `FACILITY_UNRESOLVED` otherwise. The brief
  sends these rows to quarantine; that is Task 6. In batches 001–003 this
  covers `TEST FACILITY - DO NOT USE` (7 Epic rows) and
  `Westfield Surgical Center` (25 Athena rows); neither is in the facility
  master.
- **Rejected data adds no aliases.** `Saint Brendan Medic` occurs only as the
  cut-off facility value of the truncated record in rejected batch_004, so it
  is not an alias and stays unresolved.

## Task 3: protecting patient data

### What reaches the cleaned layer
- **`clean.encounter_patients`** holds one row per accepted raw row, with only
  PHI-free patient attributes:
  - `patient_key`, `patient_link_status` and `patient_link_reason`;
  - `sex`, normalised, with its reason;
  - `age_band` and `zip3`, each with its reason;
  - lineage (`batch_id`, `file_name`, `source_row_number`) and the non-PHI
    source identifiers `source_system` and `source_record_id`.

  *(Table, grain and columns open for review.)*
- **Never stored downstream:** `patient_mrn`, first and last name, DOB, phone,
  full ZIP and `chief_complaint`. These are read from the raw layer and used
  in memory only, for linkage, the age band and the ZIP3. They are never
  logged or put in an error. The table's column list is checked against
  these PHI column names when the module loads, and tests check both the
  columns and the stored values. *(Required by the reviewer.)*
- **`chief_complaint` is left out entirely, not redacted.** Batches 001–003
  have 14 complaints with a phone-number pattern and 4 containing the
  patient's own name. Redaction belongs to the Bonus. *(Required by the
  reviewer.)*
- **Only normalised sex is kept downstream:** `F`, `M` or `UNKNOWN`, with
  `sex_reason` when it is UNKNOWN. The brief does not list sex as PHI, and the
  required export includes it.
  - The source spelling (`Female`, `M`, …) is read and normalised in memory,
    but `sex_raw` is not stored in the clean table. It would only duplicate
    `sex`, and the clean layer keeps the minimum necessary.
  - The original value stays available in the restricted raw layer
    (`raw.encounters.patient_sex`).

  *(Required by the reviewer.)*
- **The table is rebuilt from all accepted raw rows on every run,** in one
  transaction, because linkage looks across every batch and source system.
  The same secret and the same raw rows always give identical output.
  Incremental history is Task 4. *(Open for review.)*
- **It lives in the same DuckDB file** as the raw layer, in a separate `clean`
  schema. DuckDB controls access per file, so the separation here is by
  content. In production, raw and cleaned data would sit in separate,
  separately permissioned catalogs. *(Open for review.)*
- **`source_system` comes from `raw.ingested_files`,** which was validated
  against the manifest and schema contract, not from the row's own delivered
  `source_system` column.

### Patient linkage
- **Only the brief's minimum rule,** with all four fields required:
  normalised last name, first given name, DOB and sex. No fuzzy matching, no
  nickname matching, and no fallback rule. *(Required by the reviewer.)*
  - **Last name:** trim, upper-case, then remove everything that is not a
    letter or digit, so `O'Testa`, `O Testa` and `OTESTA` match.
  - **First given name:** trim, upper-case, take the first whitespace token
    and remove its punctuation. Later tokens, including middle initials, are
    ignored. A single-letter first token is kept, not dropped as an initial.
    In batches 001–003, 25 cross-system groups link only because the middle
    initial is ignored. Known false-split risk: `Mary Ann` gives `MARY`, but
    `Mary-Ann` gives `MARYANN`. No such case occurs in the data.
  - **DOB:** parsed by the D1 date parser with the source's date order and
    the batch's delivery cutoff, and used in memory only.
  - **Sex:** `F`/`Female` → F and `M`/`Male` → M. Anything else is UNKNOWN and
    cannot link.
- **An identity is `(source_system, MRN)`,** never the MRN alone.
  - An identity with a DOB on any of its rows is linkable.
  - An identity with no DOB on any row is never linked across systems.
    That covers 12 identities and 34 rows. *(Required by the reviewer.)*
  - An identity whose rows give two different linkage keys is not linked
    (`PATIENT_UNLINKED_CONFLICT`); none occurs in the data. *(Open for
    review.)*
- **A false merge is clinically worse than a false split.** Merging two people
  puts one patient's diagnoses, medications and allergies into another's
  history, which can lead directly to a wrong treatment decision. A split
  leaves one person's history in two pieces: incomplete, but not wrong. The
  rule therefore stays conservative. The near-misses it deliberately leaves
  unlinked are:
  - 2 clusters that differ only in sex;
  - 3 with a different first name;
  - 1 with a different last name;
  - 4 DOB pairs that are close enough to be typos.

  *(Required by the reviewer.)*
- **Results on batches 001–003:**
  - 1,056 identities, of which 1,044 are linked;
  - 895 person groups, 142 of them across systems (135 span two systems and
    7 span three);
  - 0 groups holding two MRNs from the same system.

### patient_key
- **`patient_key` is HMAC-SHA256** over a canonical JSON payload, hex-encoded,
  using the standard library's `hmac` and `hashlib`. *(Required by the
  reviewer.)*
  - Linked: `["patient_key/v1", "LINKED", last, first, dob ISO, sex]`.
  - Unlinked: `["patient_key/v1", "UNLINKED", source_system, MRN]`.
  - The namespace keeps the two kinds from ever colliding. The unlinked
    payload includes the source system, so an unlinked key can never join
    identities across systems. JSON encoding makes the payload unambiguous,
    and the version string allows a deliberate change later.
- **A blank MRN gets no key** (`PATIENT_MRN_MISSING`), so all blank-MRN rows
  are never merged into one patient. None occurs in the data. *(Open for
  review.)*
- **The key can change.** It is derived from the identity's current
  attributes, so an identity that is unlinked today gets a different, LINKED
  key if a later batch supplies a valid DOB, and a corrected name or DOB also
  changes it. Re-runs on the same data always give the same keys. No
  persistent patient master or crosswalk is built to keep old keys stable;
  that is a production concern. *(Required by the reviewer.)*
- **The secret comes from the `PATIENT_KEY_HMAC_SECRET` environment
  variable.**
  - It is checked before any work starts. A missing or blank value exits 1
    with `MissingSecretError`, a `ConfigError`, so nothing is ingested or
    cleaned.
  - It is never hard-coded, never shown in `Settings`' repr, and never
    logged.
  - `.env.example` holds only the existing, clearly labelled
    development-only placeholder.
  - Changing the secret changes every key. *(Required by the reviewer.)*

### Age band and ZIP3
- **Age is whole years on the admit date:** `0-17`, `18-39`, `40-64`, `65+`.
  UNKNOWN carries a reason:
  - `AGE_BAND_DOB_UNAVAILABLE`: 34 rows;
  - `AGE_BAND_ADMIT_UNAVAILABLE`: 18 rows;
  - `AGE_BAND_DOB_AFTER_ADMIT`: 2 rows.

  The result never carries the DOB.
- **`zip3` is the first three digits of a 5-digit ZIP or a ZIP+4.** Anything
  else is NULL with `ZIP_MISSING` or `ZIP_INVALID`. All 3,636 accepted values
  are valid 5-digit ZIPs. Accepting ZIP+4 is an implementation choice. *(Open
  for review.)* The HIPAA Safe Harbor rule of replacing low-population ZIP3s
  with `000` is not applied, because the brief asks only for the first three
  digits.
- **`REFERENCE_DIR`** is a new path setting, defaulting to
  `DATA_DIR/reference`. The cleaning stage needs the source conventions to
  parse DOB and admit dates. *(Open for review.)*

## Task 4: incremental processing and history

All entries below were approved in design review. Counts are for batches
001–003.

### Identity and classification
- **A version is `(source_system, source_record_id, last_updated_ts)`, with
  the timestamp compared as a UTC instant.**
  - Why the timestamp is compared as an instant: compared as text, 20 Meditech
    encounters (day-first dates) would sort wrongly, and ignoring Athena v2's
    offset would change the current version of 8 encounters.
  - Why `source_system` is part of the key: 26 `source_record_id` values occur
    in more than one system.
  - `encounter_key` and `version_key` are SHA-256 over canonical JSON, so the
    same input always gives the same key, whatever the run or order.
- **A fingerprint guard compares rows of the same version.**
  - EXACT means byte-identical.
  - EQUIVALENT means equal apart from the timestamp text and columns that are
    NULL because one row's schema version lacks them. An empty string was
    delivered, so it is compared.
  - CONFLICT means any other difference.
  - Why: comparing raw text would miss the 9 Athena batch_003 replays, which
    differ only in timestamp format and the new `encounter_source` column. Of
    the 92 version ids with more than one row, 83 are EXACT, 9 EQUIVALENT and
    0 CONFLICT.
- **Stale is checked before duplicate.** The order is: unplaceable
  (QUARANTINED), then older than the held current version (STALE), then
  already known (DUPLICATE, or a conflict), then new.
  - Each row is compared with the state at the end of the previous batch, and
    with earlier rows of its own batch in `(file_name, source_row_number)`
    order.
  - Why: a replay of a superseded version is stale by the brief's own
    wording. Checking duplicate first would count 16 byte-identical replays as
    duplicates but the 9 equivalent Athena replays as stale, an artefact of
    formatting.
  - Result: 70 duplicates and 25 stale, against 86 and 9 with duplicate first.
    The modelled state is the same either way.
- **A stale row with a never-held older version goes to history but is never
  current** (`STALE_NEW_VERSION`).
  - Why: the brief asks to keep every distinct version and forbids an older
    row from overwriting a newer one; this does both.
  - It also keeps history a function of the set of raw rows, independent of
    arrival order. No such row occurs in the data, so it is covered by
    synthetic tests.
- **A stale replay whose values differ stays STALE** with
  `match_type = CONFLICT`. It is not quarantined, because the stale check
  runs first and the row cannot affect the current state.
- **A same-timestamp conflict is quarantined** (`VERSION_CONFLICT_SAME_TS`),
  and the first arrival is kept.
  - Why: two different values for one version cannot both be right, and
    neither may silently replace the other.
  - It counts in `quarantined_count`, alongside rows with no
    `source_record_id` or an unparseable timestamp. None occurs in the data.

### History and current state
- **History is insert-only.** `clean.encounter_versions` holds the version
  spine only: keys, the UTC timestamp, first-seen lineage and the arrival
  outcome. Cleaned attributes are joined in Task 5.
  `clean.encounter_row_outcomes` holds one outcome per raw row.
  `clean.encounter_current` is a view.
  - Why the view: version order and "current" are derived there, so a late,
    older version never forces an update to rows already written.
- **Lineage points to a version's first arrival** (earliest batch, file and
  row).
  - Why: the first arrival is the row that supplied the version; later copies
    are duplicates of it.
  - This choice affects 36 encounters' current lineage, which would point to
    batch_002 instead of batch_001 if the latest arrival were used.
- **A quarantined newest version stays current, flagged, and is excluded from
  analytics.** There is no fallback to an older version.
  - Why: the brief fixes the latest timestamp as current. Falling back would
    report state known to be superseded, for example the old PAID amount of an
    encounter later voided; there are 34 PAID→VOID transitions in the data.
  - Task 4 keeps such a version current. The version-level flag and the
    analytics exclusion come with Task 6. No encounter in the data has a
    failing newest version and a passing older one.
- **Late arrival is not a processing class.**
  - Late encounters and late updates are ordinary versions.
  - Monthly reporting groups the current state by `admit_date`, so past months
    change.
  - As-of reporting picks the latest version with
    `first_seen_batch_id <= N`.
  - Result: 19 of the 24 months known at batch_002 have different current
    totals after batch_003. The as-of-batch_002 totals, read from the final
    history, still equal what batch_002 left as current.

### Audit and reconciliation
- **`accepted_count` = `new_encounter_count + new_version_count`**, the rows
  that created a version that can be current. Stale rows that write a history
  version count as stale.
- **Two reconciliation equations per accepted file,** checked against the
  rows actually written:
  - `received_count = new_encounter + new_version + duplicate + stale + quarantined`
    (every raw row has exactly one outcome);
  - `history_rows_written = new_encounter + new_version + stale_new_version`
    (every history row is accounted for).

  A mismatch raises `PipelineError` and rolls the whole batch back, so an
  inconsistent audit row is never published. Each stored row has
  `reconciliation_status = 'RECONCILED'`.
- **Rejected batches leave every Task 4 column NULL** (not evaluated), as
  Stage 1 did for its unevaluated counts.
- **One transaction per accepted batch** covers raw rows, file records, the
  history step and the audit rows. `raw_store` runs the history step through a
  `before_audit` hook, so it never imports `encounter_history` and there is no
  circular import.

### Redelivery
- **A duplicate file in a new batch is accepted and flagged.** If a file's
  exact bytes were ingested in an earlier batch, its audit reason is
  `DUPLICATE_FILE(first_batch=…)` and a warning is logged. Its rows classify
  as duplicate or stale, so it adds no versions.
  - Why: the bytes are valid, and rejecting the file would reject the whole
    batch.
- **A changed redelivery of a processed `batch_id` is logged only.** It is
  still skipped, its audit rows are not changed, and a warning with
  `BATCH_REDELIVERED_CHANGED` is logged.
  - Limitation: a rejected batch stored no file hashes, so a changed
    redelivery of a rejected batch cannot be detected.

### Rebuild, migration and parity
- **`python -m pipeline.main --rebuild-derived`** rebuilds the history and the
  audit counts from raw, in one transaction:
  1. migrate the audit table if needed;
  2. empty the history tables;
  3. replay every accepted batch in `batch_id` order through
     `_classify_into_history`, the same step an incremental load runs;
  4. overwrite `accepted_count` and the Task 4 audit columns.

  Raw rows and audit status, reason and timings are not touched. The command
  then carries on as a normal run.
- **A database from before Task 4 is refused by normal runs.** Its
  `ops.batch_audit` lacks the Task 4 columns, and `open_store` raises
  `PipelineError`.
  - Why: adding the columns silently would leave old batches with no history.
- **`--rebuild-derived` is the migration path.** Only it opens such a database
  (`allow_outdated_audit`). The audit table is recreated in the current
  layout, keeping every row, inside the rebuild transaction. If the rebuild
  fails, the migration rolls back too: the database stays pre-Task-4, is still
  refused, and the rebuild can be retried.
- **"Identical outputs" excludes only `ingested_at`, `start_time` and
  `end_time`.** They record when a run happened.
  - All other columns are compared across: incremental 001 → 004, a rerun, a
    one-shot run, a rebuild of a migrated pre-Task-4 copy, and an in-place
    rebuild. A test asserts that exactly these three columns are excluded.
  - A rerun on the same database is identical including timings.

### Known conflict and implementation choice
- **`clean.encounter_patients` stays a full rebuild on every run.**
  - Why: patient linkage looks across every batch and source system, so a new
    batch can legitimately change older rows' `patient_key`, for example when
    an identity first gains a DOB.
  - This conflicts with the brief's "each batch updates only the records it
    touches"; the Task 4 history tables do follow that rule. The rebuild is
    deterministic: the same secret and raw rows give identical output.
- **The classifier is pure Python** (`classify_batch`). SQL loads only the
  touched encounters' history and writes the results.
  - Why: it is unit-tested without a database, including a permutation test.
    It reuses the tested timestamp parser and its DST rules, and the
    fingerprint guard is easier to express there.
  - In production the same rules become a set-based query per batch and an
    insert-only `MERGE` on `version_key` (see ARCHITECTURE.md).

## Task 5: warehouse model

All entries below were approved in design review. Counts are for batches
001–003.

### Build strategy
- **`clean.encounter_version_fields` is insert-only.** It holds the cleaned
  Task 2 fields of each new version, with raw value, cleaned value and reason
  side by side. It is written in the batch transaction, inside the same
  per-batch step as the history, so each batch touches only its own new
  versions.
- **The `mart` tables are rebuilt deterministically on every run,** in one
  transaction (`warehouse.rebuild_mart`): the five dimensions, both fact tables
  and `dim_patient`.
  - Why: some inputs change for rows that are already loaded. A new batch can
    re-link a patient and change their `patient_key`, and a late version can
    change which version is current. Rebuilding about 3,500 rows from the
    clean layer is cheap, and the same inputs always give byte-identical
    tables and CSVs.
  - This repeats the documented `encounter_patients` trade-off against "each
    batch updates only the records it touches".
  - In production the mart would be updated with a `MERGE` over the touched
    `encounter_key`s, plus a persistent `patient_key` crosswalk, instead of a
    full rebuild.
- **A change to the reference data or the facility aliases needs
  `python -m pipeline.main --rebuild-derived`.** The insert-only version
  fields were cleaned with the reference data and aliases of their day: a
  changed alias would otherwise apply only to new versions. The rebuild clears
  and refills the fields table with the same step. A normal run refuses a
  database whose versions lack fields.

### Dimensions
- **A provider missing from the roster gets no fallback.** `provider_sk` and
  every provider attribute are NULL, and `provider_sk_reason` says why, checked
  in this order:
  1. the NPI is invalid: the NPI's own reason (20 current versions);
  2. the valid NPI is in no snapshot: `NPI_NOT_IN_ROSTER` (18);
  3. the admit date is unknown: `PROVIDER_ADMIT_DATE_UNKNOWN` (18);
  4. no row covers the admit date: `PROVIDER_NOT_ON_ROSTER_AT_ADMIT` (0).

  The latest snapshot is never used in place of the point-in-time one. For 336
  current encounters, the two give a different employment status.
- **Only the earliest snapshot applies backwards,** read literally. Its rows
  are valid from `0001-01-01`. A provider who first appears in a later snapshot
  has no row before that date, and a provider dropped from a snapshot has none
  after it. `valid_to` is exclusive. Every roster attribute is tracked,
  provider names included, which gives 65 rows for 54 NPIs.
- **`dim_date` covers the full calendar years spanned by the cleaned admit and
  discharge dates:** 2023-01-01 to 2025-12-31, 1,096 days. The range comes from
  the data, so a later batch with 2026 dates extends it rather than falling
  outside it.
- **In `dim_diagnosis`, `is_chronic` NULL means unknown, not "N".** Valid
  ICD-10 codes seen in the data but missing from the reference (3 codes) are
  added with `in_reference = false`, and with NULL description, category and
  `is_chronic`. The reference does not say whether they are chronic, so a
  chronic filter (Task 7) excludes them rather than counting them as acute.

## Task 6: data quality and observability

Implemented in `dq_rules.py` (the check
catalogue), `publish_gate.py`, `version_dq.py`, `quarantine.py` and
`dq_report.py`. Counts are for the real data pack.

### Checks and severity
- **One catalogue of checks** (`dq_rules.CHECKS`). Each check has a code, a
  category, the layer and grain it looks at, a severity and a kind (quality,
  observation, reconciliation or gate).
- **Checks reuse the reason codes Tasks 1–5 already produce;** nothing is
  parsed again. Every code is classified by where it is read (`CODE_MAP`): it
  is counted by a check, or it is a derivative represented by its parent check
  (for example `AGE_BAND_ADMIT_UNAVAILABLE` under `ADMIT_DATE_VALID`) so it is
  not counted twice, or it is a consequence (`SIBLING_FILE_REJECTED`). A code
  with no classification raises, so a new reason code cannot escape the report.
- **ERROR: the batch is rejected, the row is quarantined, or the version is
  flagged and excluded from analytics (for invariants: the run fails).**
  - *File and batch checks (Task 1):* `MANIFEST_VALID`, `FILE_SHA256_MATCH`,
    `FILE_ENCODING_VALID`, `FILE_CSV_PARSEABLE`, `RECORD_SHAPE_VALID`,
    `SCHEMA_CONTRACT_MATCH`, `ROW_COUNT_MATCH`. A failure rejects the whole
    batch. Why:
    - required by the brief: manifest row-count and SHA-256 validation before
      loading, an unknown schema change stops the batch, and a batch is
      all-or-nothing;
    - our Stage 1 decision: an invalid encoding, a CSV parsing failure or a
      malformed record shape also rejects the batch. This is an
      implementation judgment: such a file cannot be safely trusted row by
      row.
  - *Row checks (the Task 4 row quarantine):* `SOURCE_RECORD_ID_PRESENT`,
    `LAST_UPDATED_TS_VALID`, `VERSION_CONFLICT_SAME_TS`. Why: such a row cannot
    be placed in version order, or would give one version two different
    values.
  - *Version checks:* `FACILITY_RESOLVED` and `ADMIT_DATE_VALID`. Why:
    - an unresolved facility: the brief sends rows whose facility cannot be
      resolved to quarantine;
    - an unusable admit date: a design judgment, not a rule stated by the
      brief. The main analytical uses depend on it: the reporting month
      (queries 1 and 5, `dim_date`), the point-in-time provider, the age band
      at admission, the length of stay and the Task 7 year filter. Without it
      the encounter cannot be placed in any period. The age-band, provider and
      length-of-stay reason codes for an unusable admit date are classified
      as derivatives of this check.
  - *The publish gate:* `PUBLISH_GATE`.
- **WARNING: the record is kept and flagged.** The severity rationale below
  is a design judgment; the brief only defines a warning as kept and flagged.
  - *Version checks:* every other version check (discharge date, discharge
    before admit, encounter type, claim status and payer mapping, diagnosis
    valid and in the reference, NPI valid and in the roster, billed amount,
    ambiguous local timestamp, patient key present, patient linked, age band,
    sex, ZIP3, provider on the roster at admit). Why: the value is NULL with
    its reason or flagged, and a version with only WARNINGs is kept and
    flagged, because it can still be counted correctly. For example, query 1
    counts an encounter with an invalid amount, and its sum skips the NULL
    amount.
  - *`STALE_REPLAY_CONFLICT`:* a stale replay whose values differ from the
    held version. Why: a stale replay cannot change the current state
    (Task 4).
  - *`DUPLICATE_FILE`:* an accepted file whose exact bytes were ingested in an
    earlier batch. Why: the duplicate delivery is informational; its rows
    classify as duplicate or stale and change no modeled state (Task 4,
    Redelivery).
  - *`SCHEMA_VERSION_CHANGED`:* an accepted file whose matched schema version
    differs from its source system's previous accepted file. Why: the file
    matched a registered contract version, so it is valid; the change is
    flagged so it stays visible.
- **INFO: a count, not a defect:** `DUPLICATE_ROW`, `STALE_ROW`,
  `RECON_CURRENT_ROWS`.
- **Pipeline invariants** are ERRORs whose failure is a pipeline bug, not a
  data finding: fact foreign keys, current state matching history,
  value/reason consistency, key uniqueness, row counts reconciled across
  layers, and a rejected batch leaving nothing loaded. A failure raises inside
  the mart rebuild transaction, so the mart and DQ tables roll back and the
  run exits 1. Stored invariant rows are therefore always PASS.
- **Result:** 50 versions have an ERROR (32 `FACILITY_RESOLVED`, 18
  `ADMIT_DATE_VALID`), and there are 1,169 WARNING issues on versions. No row
  needed the Task 4 row quarantine.

### Version issues and the analytics exclusion
- **`clean.version_dq_issues`** has one row per (`version_key`, `check_code`)
  that a version fails, with the reason code and severity. Lineage is the
  version's first arrival. It holds keys, codes and severities only, no
  values.
- **A version with an ERROR is not deleted or changed.** It stays in the
  history and the facts, and in the current state when it is the latest.
  Queries 1 and 5 and the Task 7 export exclude it by joining this table, with
  no fallback to an older version (the Task 4 decision).
- **The table is rebuilt on every run,** inside the mart rebuild transaction,
  because `clean.encounter_patients` is rebuilt on every run and a re-link can
  change a version's patient warnings.

### Publish gate
- **The rule.** A batch is published only if its share of error rows is not
  more than the threshold, `DQ_GATE_MAX_ERROR_SHARE`:
  - error rows are received rows that fail an error-level row or version
    check, including every duplicate or stale copy of a version with an ERROR;
  - received rows are every raw row of the batch;
  - the batch fails when `error_rows > threshold × received_rows`, in exact
    `Decimal` arithmetic; a batch exactly at the threshold passes;
  - WARNINGs never count.
- **Where it runs.** Inside the batch transaction, after the history step and
  its reconciliation, before the audit rows are written. It reads version
  ERRORs only from the cleaned fields, using the same classification that
  builds `clean.version_dq_issues`, so the two cannot disagree.
- **On failure** the whole batch transaction rolls back. A second, small
  transaction records every file as REJECTED with
  `DQ_GATE_FAILED(error_rows=..,received=..,threshold_pct=..)` and keeps the
  failing rows in `ops.gate_rejected_issues` (lineage, `source_record_id` and
  codes; insert-only, because it is the only record of those rows). The
  rejection is a data outcome: processing continues with the next batch and
  the run exits 0.
- **The threshold is 5%** (`DQ_GATE_MAX_ERROR_SHARE=0.05`, the code default
  and the value in `.env.example`).
  - It is a design judgment. The threshold itself is not a measured value,
    and the brief does not give one; it asks for a threshold that batches
    001–003 pass.
  - Rationale (design judgment): roughly three times (5% ÷ 1.54% ≈ 3.2) the
    highest error share observed in an accepted batch (batch_001: 1.54%),
    giving headroom for normal variation while still detecting a systemic
    quality failure.
  - Observed shares: batch_001 44 of 2,863 rows (1.54%), batch_002 5 of 585
    (0.85%), batch_003 1 of 188 (0.53%). batch_004 is rejected by Task 1
    validation and never reaches the gate (`NOT_EVALUATED`).
- **The tests prove the gate works** (`tests/test_publish_gate.py`). A bad
  synthetic batch, with 2 of its 10 rows at an unresolved facility (20% > 5%),
  is rejected with `DQ_GATE_FAILED(error_rows=2,received=10,threshold_pct=5.00)`.
  Other tables stay unchanged, the failing rows are kept with lineage and
  codes, the batch appears in the quarantine and the DQ report, and the next
  batch is still processed. Further tests cover the boundary (at a 10%
  threshold, 1 of 10 passes and 2 of 10 fail), that warnings never count, that
  Task 4 quarantined rows count, and that every copy of an error version
  counts.
- **Published data is not withdrawn when the rules change.** If a published
  batch would fail the gate under today's threshold or reference data,
  `--rebuild-derived` fails and rolls back (exit 1), and a normal run keeps
  the batch published but reports `PUBLISH_GATE` as FAIL for it in the DQ
  report.
- **Logging** follows Stage 1, "Logging and PHI". The gate logs
  `publish_gate_evaluated` and, on failure, `batch_rejected` with reason code
  `DQ_GATE_FAILED`; the events carry counts and codes, no PHI or data values.

### Quarantine (`ops.quarantine`)
- **One table of every rejected or unresolved record,** rebuilt on every run
  from what earlier steps store:

  | Level / source | What it is | Lineage |
  | --- | --- | --- |
  | FILE / BATCH_VALIDATION | a file of a batch rejected by Task 1 | batch, file; `row_count` = rows received |
  | FILE / PUBLISH_GATE | a file of a batch the gate rejected | batch, file; `row_count` = rows received |
  | ROW / PUBLISH_GATE | a failing row of a gate-rejected batch | batch, file, row |
  | ROW / HISTORY_ORDERING | a Task 4 row with outcome QUARANTINED | batch, file, row |
  | VERSION / VERSION_DQ | a version with an ERROR | first arrival, plus `version_key` |

- **A rejected file stands for all its rows.** Its rows are not listed one by
  one, because a rejected file's records are not trusted (batch_004's Epic
  file is truncated).
- **A version ERROR is listed once,** at its first arrival, with
  `is_current_version`; its duplicate and stale copies are already counted in
  the audit.
- **No values:** lineage, keys, `source_record_id`, codes and, for a FILE
  entry, the PHI-free audit reason text.
- **Result:** 53 entries: the 3 files of batch_004 and 50 ERROR versions.

### DQ report (`ops.dq_report`)
- **One row per (batch, file, check),** including checks with nothing to
  report, so the report shows that every check ran. Batch-scope rows
  (`MANIFEST_VALID`, `PUBLISH_GATE`, `REJECTED_BATCH_NOT_LOADED`) have no file.
- **Status:** PASS; FAIL for an ERROR check or WARN for a WARNING check with
  failures; NOT_EVALUATED where an earlier failure stopped the check. In a
  rejected batch, the Task 1 checks are evaluated and every other file check
  is NOT_EVALUATED with `BATCH_REJECTED`.
- **Special cases:**
  - a gate-rejected batch: its error-level row and version checks are counted
    on rows from `ops.gate_rejected_issues`, because the batch transaction,
    and with it the versions, was rolled back;
  - a batch rejected at the manifest has no file list, so it gets only the
    batch-scope rows. A batch rejected by file validation, such as batch_004,
    still gets its per-file rows.
- **Counts:** `evaluated_count` (the population checked), `observed_count`,
  `expected_count` (reconciliation checks only), `observed_pct` and, for the
  gate, `threshold_pct`.
- **Row counts are reconciled across layers** for every accepted file: raw
  rows, row outcomes, patient rows, history rows, version-field rows, fact
  rows, the DQ partition and quarantine rows.
- **Deterministic:** rebuilt on every run with no run timestamps, so the same
  state always gives the same rows. Some rows of an earlier batch change
  legitimately when a later batch arrives, for example `RECON_CURRENT_ROWS`
  as versions are superseded.
- **Exported as `dq_report.csv`;** the brief allows CSV or Markdown.

## Task 7: chronic_acute_encounters.csv

Implemented in `chronic_acute_export.py` and written by every run after the
table exports. Counts are for batches 001–003 (batch_004 is rejected and
changes no row).

### Filter
- **One row per encounter, from its current version** in
  `mart.fact_encounter_current`, when all six brief conditions hold:
  1. the facility resolved to the facility master (`facility_id` joins
     `dim_facility`);
  2. `claim_status IS DISTINCT FROM 'VOID'`, so a NULL claim status is kept, as
     in queries 1 and 5;
  3. `encounter_type` is `INPATIENT`, `OBSERVATION` or `EMERGENCY`;
  4. the primary diagnosis is in the ICD-10 reference with `is_chronic = Y`. A
     valid code missing from the reference has `is_chronic` unknown and is left
     out (see Task 5);
  5. `admit_date` is in calendar year 2024;
  6. `billed_amount_usd` is valid (not NULL) and at least 5,000.00.
- **The Task 6 ERROR exclusion of queries 1 and 5 also applies.** A current
  version with an ERROR in `clean.version_dq_issues` is left out, with no
  fallback to an older version. The only version ERRORs are an unresolved
  facility and an unusable admit date, which already fail conditions 1 and 5,
  so the clause excludes 0 extra rows today. It stays so that a future ERROR
  check is honoured without a change here.
- Result: **247 rows** (181 INPATIENT, 34 OBSERVATION, 32 EMERGENCY).

### readmit_30d_flag
- **For INPATIENT rows: 1 when the same `patient_key` has another INPATIENT
  encounter admitted 1 to 30 days after this row's `discharge_date`, at any
  facility; otherwise 0. Blank for OBSERVATION and EMERGENCY.** Day 0 (admitted
  on the discharge day) and day 31 do not count.
- **The other encounter is searched among all current encounters,** not only
  this export's rows: any year, diagnosis or amount. It must be non-VOID
  (`IS DISTINCT FROM`, so a NULL claim status counts), at a resolved facility,
  and have valid dates.
- **"Valid dates" means both `admit_date` and `discharge_date` parsed (not
  NULL).** A discharge before admission is still two valid dates (Task 2 keeps
  both and only warns), so it does not disqualify the other encounter.
  *(Implementation choice; open for review.)* No real follow-up candidate is
  affected: reading it as "admit date only", or also requiring discharge not
  before admit, gives the same 25 flags.
- **An INPATIENT row with a NULL `discharge_date` gets 0,** as there is no
  window to search (none in the export today). A row with a NULL `patient_key`
  (blank MRN) also gets 0: NULL keys never match each other (none today).
- **A row whose own discharge is before its admission uses its
  `discharge_date` as stored.** *(Implementation choice; open for review.)* None
  in the export today.
- Result: 25 of 181 INPATIENT rows are flagged; 2 of them only through
  encounters outside the export and 10 only through another facility.

### Columns and format
- **Columns in the brief's order.** `patient_zip3`, `age_band` and `sex` are the
  current version's values from the facts; `dx_description` and
  `chronic_category` are the reference's description and category.
- **The provider's specialty and employment status come from the
  point-in-time `dim_provider` row** the facts chose for the admit date
  (`provider_sk`), and are blank when there is none (2 rows: an invalid NPI and
  an NPI in no roster). The latest snapshot is never used instead.
- **Lineage columns point to the row that supplied the current version** (its
  first arrival), as in the facts; `version_count` is the encounter's.
- **Sorted by `admit_date`, `source_system`, `source_record_id`** (text order).
  Formats are the table exports': ISO dates, two-decimal amounts, NULL as an
  empty string, UTF-8, LF line endings, written through a temp file and a
  rename. The file is byte-identical across incremental, one-shot, rerun and
  rebuild runs.

## Task 8: Docker

### Image and services
- **One image, two Compose services.** `pipeline` runs `python -m
  pipeline.main` and is what `docker compose up --build` starts. `tests` runs
  pytest in the same image and sits in the `test` profile, so `up` never
  starts it; `docker compose run --rm --build tests` does. One image means the
  tests run against exactly the code and dependency versions that produce the
  outputs.
- **`python:3.11.17-slim-bookworm`, pinned to the patch release.** 3.11
  matches local development (the code needs 3.11+ for `datetime.UTC`).
- **`/app` mirrors the repository layout** (`src/`, `config/`, `sql/`,
  `tests/`, `pytest.ini`, `.env.example`), so `config.REPO_ROOT` and the tests'
  `REPO_ROOT` resolve to `/app` and nothing in the code is Docker-specific.
  Files are copied explicitly; `.dockerignore` also keeps `.env`, `.git`,
  `.venv`, `data/`, `output/` and DuckDB files out of the build context.
- **Exec-form `CMD`.** Python is PID 1, so the container's exit code is the
  pipeline's: 0 for a completed run, rejected batches included; 1 for a
  configuration or system failure. Compose adds no wrapper and no restart
  policy.
- **The container runs as root**, with no `USER` directive. A bind-mounted
  `./output` owned by a different user on a Linux host could make a non-root
  container fail to write, which would break the required exit code 0 on the
  assessor's machine. The cost is less privilege separation inside the
  container, and root-owned output files on Linux hosts; in production the job
  would run as a dedicated non-root identity with storage permissions set to
  match.

### Storage
- **The data pack is a read-only bind mount** (`./data:/app/data:ro`) in both
  services, as the brief requires. It is not copied into the image, so new
  batches dropped into `data/landing` are picked up without a rebuild.
- **Outputs are a bind mount** (`./output:/app/output`), so the CSVs land
  directly in the repository's `output/` folder.
- **The raw DuckDB database is on a tmpfs at `/work`, not a named volume.**
  Every container start begins with an empty database and processes
  `batch_001` to `batch_004` from scratch. A persistent volume was rejected:
  - a repeated `docker compose up --build` would skip all four batches as
    already processed, so the run would no longer process them;
  - the volume outlives code changes (`git pull`, rebuilds, even a re-clone
    into a folder with the same name), so accepted batches would keep derived
    state built by older code, never be re-evaluated against new reference
    data or gate settings, and a pre-Task-4 database would make the run exit 1;
  - raw PHI would stay in Docker storage after the run.

  The container's writable layer and anonymous volumes were also rejected:
  `docker compose up` restarts an unchanged existing container and keeps
  anonymous volumes on recreate, so neither is reliably empty. The cost is that
  the Docker run does not show incremental loading across runs; within a run
  each batch is still its own transaction, and incremental-vs-rebuild
  equivalence is proven by `tests/test_incremental_parity.py`, as the brief
  asks. In production the raw layer is persistent, access-controlled storage.
- **The DuckDB file is not exported.** The brief makes it optional, and the
  raw layer holds PHI; `RAW_DB_PATH` must be outside `OUTPUT_DIR` anyway.

### Configuration and secrets
- **Compose loads `.env.example`, then an optional `.env`** (`env_file` with
  `required: false`, which needs Compose 2.24+). The development-only HMAC key
  therefore exists only in `.env.example`, where the brief allows a clearly
  labelled one, and not as a second committed copy in `docker-compose.yml`. A
  fresh clone runs with no `.env`; a real key goes in the gitignored `.env`,
  whose values win. Shell variables do not override `env_file` values; that is
  accepted, since `.env` is the documented override.
- **Container paths are fixed in `docker-compose.yml`**, not read from the env
  files, so an override cannot move the raw database off the tmpfs or into the
  output folder. Only `PATIENT_KEY_HMAC_SECRET`, `LOG_LEVEL` and
  `DQ_GATE_MAX_ERROR_SHARE` are meant to be changed.
- **The `tests` service gets no pipeline variables.** The tests build their own
  settings; one starts the pipeline as a subprocess with the inherited
  environment, and nothing from the pipeline service can leak into it.

### Dependencies
- **Every package in `requirements.txt` is pinned** to an exact version,
  including pytest's own dependencies (`colorama` only on Windows, by marker),
  and the Python base image uses the pinned `python:3.11.17-slim-bookworm`
  tag. No new packages were added; the pins
  record the versions already used locally. pytest is in the image because the
  brief requires the tests to run in a container.
