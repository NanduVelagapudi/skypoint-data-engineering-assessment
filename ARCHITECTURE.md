# Architecture

## Incremental processing and history (Task 4)

### Model
- **Encounter:** `(source_system, source_record_id)`, keyed by `encounter_key`.
- **Version:** `(source_system, source_record_id, last_updated_ts)`, with the
  timestamp compared as a UTC instant, keyed by `version_key`. Both keys are
  SHA-256 over canonical JSON, so the same input always gives the same key,
  whatever the run or the processing order.
- **`clean.encounter_row_outcomes`:** one row per raw row, with exactly one
  outcome: `NEW_ENCOUNTER`, `NEW_VERSION`, `DUPLICATE`, `STALE` or
  `QUARANTINED`. Every landed row can be accounted for.
- **`clean.encounter_versions`:** one row per distinct version. It is
  insert-only, and its lineage points to the version's first arrival.
- **`clean.encounter_current`:** a view. For each encounter, the version with
  the latest `last_updated_ts` is current. Version order and "is current" are
  derived here, not stored, so a late, older version never forces an update to
  rows already written.

### Classification rules
Each row of a batch is compared with the versions held at the end of the
previous batch, and with the rows before it in the same batch, in lineage
order. The first matching rule wins:

1. Missing `source_record_id`, or a `last_updated_ts` that does not parse:
   `QUARANTINED`.
2. Older than the held current version: `STALE`. If the version is not held
   yet, it is written to history but can never be current.
3. The version is already known: `DUPLICATE`. If its values differ from the
   version's first row, it is `QUARANTINED` (`VERSION_CONFLICT_SAME_TS`), and
   the first arrival is kept.
4. Otherwise it is a new version. For an encounter with no held version, its
   earliest version is the `NEW_ENCOUNTER`.

All of these rules work on sets of rows. None depends on the order rows are
read in, which is what lets the same rules run as SQL in production.

### Local implementation: classify in Python, store in DuckDB
The classifier is a pure Python function,
`encounter_history.classify_batch(rows, held)`. SQL does the two set-based
steps around it:

- load the history of only the encounters the batch touches, joined to each
  version's first-seen raw row;
- write the outcomes and new versions as one JSON document per table (the
  same loading pattern as the raw store).

`apply_batch` runs inside the caller's transaction. The batch processor will
call it in the same transaction as the batch's raw rows and audit rows.

**Why Python rather than SQL here:**
- **Testability.** As a pure function, every rule is unit-tested without a
  database. A permutation test shuffles one mixed batch 20 times and gets an
  identical result each time.
- **Timestamp parsing is already Python.** UTC conversion uses
  `parse_timestamp`, with `zoneinfo` daylight-saving rules for
  `America/Chicago`. Doing the conversion in SQL would mean a second
  implementation that could drift from the tested one.
- **The fingerprint guard is easier to express and test.** Two rows of one
  version are compared column by column. The timestamp text and columns absent
  from one row's schema version (NULL) are skipped, which is how Athena's v2
  replays are recognised as the same version.
- **Scale.** In this data pack a batch is at most about 2,900 rows, already in memory
  after validation. Loading history only for the touched encounters keeps
  each batch's work proportional to the batch, not to all of history.

**Trade-off:** the classification runs row by row in one Python process.
That is fine for this data pack but not for production volumes, which is why
production uses the set-based form below.

### Production: set-based MERGE on Databricks or Snowflake
The rules carry over unchanged; only the execution changes. Per batch:

1. **Stage** the batch in the restricted layer:
   - convert `last_updated_ts` to UTC with the source's time zone;
   - compute `encounter_key` and `version_key`.
2. **Classify in one query.** Join the staged rows to the current state of the
   touched encounters and to their existing versions:
   - `row_number() OVER (PARTITION BY version_key ORDER BY file_name, source_row_number)`
     finds each version's first arrival in the batch;
   - a `CASE` applies the rules in the order above;
   - the fingerprint guard compares each row column by column with the
     version's first-seen row. This stays in the restricted layer, because it
     reads PHI.
3. **Insert new versions with a MERGE.** History is insert-only, so the MERGE
   has a single branch:

   ```sql
   MERGE INTO encounter_versions AS t
   USING batch_new_versions AS s
     ON t.version_key = s.version_key
   WHEN NOT MATCHED THEN INSERT *;
   ```

   `INSERT *` is Databricks syntax; Snowflake needs the column list written
   out. Replaying a batch matches every row and inserts nothing.
4. **Write outcomes and current state.** Row outcomes are merged on the raw
   lineage key, so they are idempotent too. The current state is either a view
   or a table refreshed by a MERGE over the touched `encounter_key`s only.
5. **Layout.** Cluster history on `encounter_key` (Delta liquid clustering or
   Z-order; a Snowflake clustering key). The classification join then reads
   only the files holding the batch's encounters.

**Atomicity:**
- **Snowflake** can wrap a whole batch in one multi-statement transaction.
- **On Databricks**, a Delta transaction covers one table. There, each step is
  an idempotent MERGE, and the batch's audit row is written last as its
  completion marker. A batch without that marker is reprocessed from the
  start, and the reprocessing creates no duplicates.

**Keeping the two implementations aligned:** the local Python classifier is
the reference implementation. A parity test would run both on the same
batches and compare outcomes, versions and current state row for row.
