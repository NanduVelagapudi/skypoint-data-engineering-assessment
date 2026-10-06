"""Task 4 group (b): encounter history wired into batch processing, audit counts and reconciliation.

Synthetic batches only, run through main() into tmp_path. Each scenario row is
(source_record_id, last_updated_ts) plus optional column overrides; every other
value is a fixed placeholder, so rows with the same id and timestamp are exact
duplicates.
"""

import json
from pathlib import Path

import duckdb
import pytest
from conftest import CONTRACT_PATH, FileSpec, contract_header, csv_bytes, write_batch

from pipeline import encounter_history
from pipeline.batch_audit import AUDIT_COLUMNS
from pipeline.encounter_history import FileCounts, encounter_key
from pipeline.errors import PipelineError
from pipeline.main import main
from pipeline.raw_store import open_store
from pipeline.schema_contract import load_contracts

pytestmark = pytest.mark.usefixtures("restore_pipeline_logger")

CONTRACTS = load_contracts(CONTRACT_PATH)
EPIC = "encounters_epic_north.csv"
MEDITECH = "encounters_legacy_meditech.csv"
T1, T2, T3 = "2024-03-01T10:00:00Z", "2024-03-02T10:00:00Z", "2024-03-03T10:00:00Z"
M1_TS = "01/03/2024 10:00:00"  # Meditech: day-first, America/Chicago

TASK4_COLUMNS = AUDIT_COLUMNS[AUDIT_COLUMNS.index("new_encounter_count") : AUDIT_COLUMNS.index("status")]
RUN_TIME_COLUMNS = {"ingested_at", "start_time", "end_time"}


def encounter_file(system, rows):
    header = contract_header(system)
    records = []
    for record_id, ts, *overrides in rows:
        values = {column: f"{column}-zz" for column in header}
        values.update(source_system=system, source_record_id=record_id, last_updated_ts=ts)
        for override in overrides:
            values.update(override)
        records.append([values[column] for column in header])
    file_name = CONTRACTS.source_systems[system].file_name
    return FileSpec(file_name, system, csv_bytes(header, records), len(records))


BATCH_001 = [
    encounter_file("EPIC_NORTH", [("E1", T1), ("E2", T1), ("E2", T1), ("E3", "N/A"), ("", T1)]),
    encounter_file("LEGACY_MEDITECH", [("M1", M1_TS)]),
]
BATCH_002 = [
    encounter_file("EPIC_NORTH", [("E1", T3), ("E1", T1), ("E4", T1)]),
    encounter_file("LEGACY_MEDITECH", [("M1", M1_TS), ("M2", M1_TS)]),
]
BATCH_003 = [
    encounter_file("EPIC_NORTH", [("E1", T2), ("E1", T1), ("E2", T1, {"billed_amount": "1.00"}), ("E5", T1)]),
]
BATCHES = {"batch_001": BATCH_001, "batch_002": BATCH_002, "batch_003": BATCH_003}

# (received, accepted, new_encounter, new_version, duplicate, stale, stale_new_version,
#  quarantined, history_rows_written, current_changed, reconciliation_status)
EXPECTED_COUNTS = {
    # E1, E2 new; E2 repeated; E3 placeholder timestamp and a blank id quarantined
    ("batch_001", EPIC): (5, 2, 2, 0, 1, 0, 0, 2, 2, 2, "RECONCILED"),
    ("batch_001", MEDITECH): (1, 1, 1, 0, 0, 0, 0, 0, 1, 1, "RECONCILED"),
    # E1 newer version; E1 re-sent at its held current time; E4 new
    ("batch_002", EPIC): (3, 2, 1, 1, 1, 0, 0, 0, 2, 2, "RECONCILED"),
    ("batch_002", MEDITECH): (2, 1, 1, 0, 1, 0, 0, 0, 1, 1, "RECONCILED"),
    # E1 never-held older version (history only) and an old replay; E2 same-time conflict; E5 new
    ("batch_003", EPIC): (4, 1, 1, 0, 0, 2, 1, 1, 2, 1, "RECONCILED"),
}


def write_batches(landing_dir, *batch_ids):
    for batch_id in batch_ids:
        write_batch(landing_dir, batch_id, BATCHES[batch_id])


def query(env, sql, params=None):
    con = open_store(Path(env["RAW_DB_PATH"]), CONTRACTS.canonical_columns)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def audit_counts(env):
    rows = query(
        env,
        "SELECT batch_id, file_name, received_count, accepted_count, " + ", ".join(TASK4_COLUMNS) + " "
        "FROM ops.batch_audit WHERE status = 'ACCEPTED' ORDER BY batch_id, file_name",
    )
    return {(r[0], r[1]): tuple(r[2:]) for r in rows}


def snapshot(env, include_run_times=False):
    """Every modelled row the pipeline writes, optionally without the run-time columns."""
    tables = (
        "raw.encounters",
        "raw.ingested_files",
        "ops.batch_audit",
        encounter_history.VERSIONS_TABLE,
        encounter_history.OUTCOMES_TABLE,
        encounter_history.CURRENT_VIEW,
    )
    con = open_store(Path(env["RAW_DB_PATH"]), CONTRACTS.canonical_columns)
    try:
        result = {}
        for table in tables:
            columns = [r[0] for r in con.execute(f"DESCRIBE {table}").fetchall()]
            kept = columns if include_run_times else [c for c in columns if c not in RUN_TIME_COLUMNS]
            result[table] = con.execute(f"SELECT {', '.join(kept)} FROM {table} ORDER BY ALL").fetchall()
        return result
    finally:
        con.close()


def log_events(capsys):
    return [json.loads(line) for line in capsys.readouterr().out.splitlines()]


def spy(monkeypatch, name):
    """Record the calls of an encounter_history function while still running it."""
    calls = []
    original = getattr(encounter_history, name)

    def wrapper(*args, **kwargs):
        calls.append(args)
        return original(*args, **kwargs)

    monkeypatch.setattr(encounter_history, name, wrapper)
    return calls


# --- audit counts ---


def test_accepted_batches_populate_every_task4_count(landing_dir, pipeline_env):
    write_batches(landing_dir, "batch_001", "batch_002", "batch_003")

    assert main(pipeline_env) == 0

    assert audit_counts(pipeline_env) == EXPECTED_COUNTS
    assert query(pipeline_env, "SELECT DISTINCT reason FROM ops.batch_audit") == [(None,)]


def test_duplicate_stale_and_quarantined_counts_are_not_null_for_accepted_batches(landing_dir, pipeline_env):
    write_batches(landing_dir, "batch_001", "batch_002", "batch_003")

    main(pipeline_env)

    nulls = " OR ".join(f"{c} IS NULL" for c in TASK4_COLUMNS)
    assert query(pipeline_env, f"SELECT count(*) FROM ops.batch_audit WHERE status = 'ACCEPTED' AND ({nulls})") == [(0,)]
    totals = query(pipeline_env, "SELECT sum(duplicate_count), sum(stale_count), sum(quarantined_count) FROM ops.batch_audit")
    assert totals == [(3, 2, 3)]


def test_rejected_batch_leaves_task4_counts_null_and_writes_no_history(landing_dir, pipeline_env):
    write_batch(landing_dir, "batch_001", BATCH_001)
    write_batch(landing_dir, "batch_002", BATCH_002, manifest_overrides={EPIC: {"sha256": "0" * 64}})

    assert main(pipeline_env) == 0

    rows = query(pipeline_env, f"SELECT status, {', '.join(TASK4_COLUMNS)} FROM ops.batch_audit WHERE batch_id = 'batch_002'")
    assert len(rows) == 2
    assert all(row == ("REJECTED", *(None,) * len(TASK4_COLUMNS)) for row in rows)
    assert query(pipeline_env, f"SELECT count(*) FROM {encounter_history.OUTCOMES_TABLE} WHERE batch_id = 'batch_002'") == [(0,)]
    assert query(
        pipeline_env, f"SELECT count(*) FROM {encounter_history.VERSIONS_TABLE} WHERE first_seen_batch_id = 'batch_002'"
    ) == [(0,)]


# --- reconciliation ---


def test_reconciliation_equations_hold(landing_dir, pipeline_env):
    write_batches(landing_dir, "batch_001", "batch_002", "batch_003")
    main(pipeline_env)

    for (received, accepted, new_enc, new_ver, dup, stale, stale_new, quarantined, written, _, status) in audit_counts(
        pipeline_env
    ).values():
        assert received == new_enc + new_ver + dup + stale + quarantined  # every raw row has one outcome
        assert written == new_enc + new_ver + stale_new  # every history row is accounted for
        assert accepted == new_enc + new_ver
        assert status == "RECONCILED"
    # And across layers: raw rows = outcome rows, history rows = Σ history_rows_written.
    assert query(pipeline_env, "SELECT count(*) FROM raw.encounters") == query(
        pipeline_env, f"SELECT count(*) FROM {encounter_history.OUTCOMES_TABLE}"
    ) == query(pipeline_env, "SELECT sum(received_count) FROM ops.batch_audit WHERE status = 'ACCEPTED'")
    assert query(pipeline_env, f"SELECT count(*) FROM {encounter_history.VERSIONS_TABLE}") == query(
        pipeline_env, "SELECT sum(history_rows_written) FROM ops.batch_audit"
    ) == [(8,)]  # 2 + 1 + 2 + 1 + 2 from EXPECTED_COUNTS


def test_counts_that_do_not_reconcile_fail_the_batch_without_writing_it(landing_dir, pipeline_env, monkeypatch, capsys):
    write_batches(landing_dir, "batch_001")
    original = encounter_history.file_counts

    def off_by_one(con, batch_id):
        counts = original(con, batch_id)
        return {name: FileCounts(**{**vars(c), "duplicate": c.duplicate + 1}) for name, c in counts.items()}

    monkeypatch.setattr(encounter_history, "file_counts", off_by_one)

    assert main(pipeline_env) == 1

    events = log_events(capsys)
    assert [e["reason_code"] for e in events if e["event"] == "reconciliation_failed"] == ["RECONCILIATION_FAILED"]
    assert [e["error_type"] for e in events if e["event"] == "pipeline_failed"] == ["PipelineError"]
    assert all(rows == [] for rows in snapshot(pipeline_env).values())


# --- atomicity ---


def test_failure_after_history_is_written_rolls_back_the_whole_batch(landing_dir, pipeline_env, tmp_path, monkeypatch):
    write_batches(landing_dir, "batch_001")
    assert main(pipeline_env) == 0
    after_001 = snapshot(pipeline_env, include_run_times=True)
    write_batches(landing_dir, "batch_002")
    original = encounter_history.write_classified_batch

    def write_then_fail(con, classified):
        original(con, classified)  # outcomes and versions are in the transaction ...
        raise RuntimeError("simulated failure after the history write")  # ... and must roll back

    monkeypatch.setattr(encounter_history, "write_classified_batch", write_then_fail)

    assert main(pipeline_env) == 1
    assert snapshot(pipeline_env, include_run_times=True) == after_001  # raw, files, history, audit untouched

    monkeypatch.undo()
    assert main(pipeline_env) == 0  # batch_002 is still pending and now loads cleanly
    fresh_env = {**pipeline_env, "RAW_DB_PATH": str(tmp_path / "fresh" / "raw.duckdb")}
    assert main(fresh_env) == 0
    assert snapshot(pipeline_env) == snapshot(fresh_env)


# --- incremental behaviour ---


def test_incremental_runs_classify_only_the_new_batch(landing_dir, pipeline_env, tmp_path, monkeypatch):
    applied = spy(monkeypatch, "apply_batch")
    read = spy(monkeypatch, "read_batch_rows")
    outcomes_001 = None
    for batch_id in ("batch_001", "batch_002", "batch_003"):
        applied.clear()
        read.clear()
        write_batches(landing_dir, batch_id)

        assert main(pipeline_env) == 0

        assert [args[1] for args in applied] == [batch_id]
        assert [args[1] for args in read] == [batch_id]  # no other batch's rows are re-read
        rows_001 = query(pipeline_env, f"SELECT * FROM {encounter_history.OUTCOMES_TABLE} WHERE batch_id = 'batch_001' ORDER BY ALL")
        outcomes_001 = outcomes_001 or rows_001
        assert rows_001 == outcomes_001  # earlier batches' outcomes are never rewritten

    assert audit_counts(pipeline_env) == EXPECTED_COUNTS
    fresh_env = {**pipeline_env, "RAW_DB_PATH": str(tmp_path / "fresh" / "raw.duckdb")}
    assert main(fresh_env) == 0  # all three batches in one run
    assert snapshot(pipeline_env) == snapshot(fresh_env)


def test_only_encounters_in_the_batch_are_loaded_from_history(landing_dir, pipeline_env, monkeypatch):
    write_batches(landing_dir, "batch_001")
    main(pipeline_env)
    write_batches(landing_dir, "batch_002")
    loaded = spy(monkeypatch, "load_held_versions")

    assert main(pipeline_env) == 0

    [(_, requested)] = loaded
    assert set(requested) == {
        encounter_key("EPIC_NORTH", "E1"),
        encounter_key("EPIC_NORTH", "E4"),
        encounter_key("LEGACY_MEDITECH", "M1"),
        encounter_key("LEGACY_MEDITECH", "M2"),
    }  # E2 is in history but not in batch_002, so it is not read


def test_current_state_after_three_batches(landing_dir, pipeline_env):
    write_batches(landing_dir, "batch_001", "batch_002", "batch_003")
    main(pipeline_env)

    current = {
        r[0]: r[1:]
        for r in query(
            pipeline_env,
            f"SELECT source_record_id, last_updated_ts_utc, version_count, source_batch_id, source_row_number "
            f"FROM {encounter_history.CURRENT_VIEW}",
        )
    }
    assert set(current) == {"E1", "E2", "E4", "E5", "M1", "M2"}  # E3 and the blank id never placed in order
    e1_ts, e1_versions, e1_batch, e1_row = current["E1"]
    assert (str(e1_ts), e1_versions, e1_batch, e1_row) == ("2024-03-03 10:00:00", 3, "batch_002", 1)
    assert current["E2"][1:] == (1, "batch_001", 2)  # the batch_003 conflict did not replace it


# --- re-runs and redeliveries ---


def test_rerun_is_idempotent(landing_dir, pipeline_env, monkeypatch, capsys):
    write_batches(landing_dir, "batch_001", "batch_002", "batch_003")
    assert main(pipeline_env) == 0
    before = snapshot(pipeline_env, include_run_times=True)
    csv_path = Path(pipeline_env["OUTPUT_DIR"]) / "batch_audit.csv"
    csv_before = csv_path.read_bytes()
    applied = spy(monkeypatch, "apply_batch")
    capsys.readouterr()

    assert main(pipeline_env) == 0

    assert applied == []
    assert snapshot(pipeline_env, include_run_times=True) == before
    assert csv_path.read_bytes() == csv_before
    events = log_events(capsys)
    assert [e["batch_id"] for e in events if e["event"] == "batch_already_processed"] == list(BATCHES)
    assert not [e for e in events if e["event"] == "batch_redelivered_with_changes"]


def test_duplicate_file_in_a_new_batch_is_flagged_in_the_audit_reason(landing_dir, pipeline_env, capsys):
    write_batches(landing_dir, "batch_001")
    write_batch(landing_dir, "batch_002", [BATCH_001[0]])  # the same Epic bytes again

    assert main(pipeline_env) == 0

    [(status, reason)] = query(
        pipeline_env, "SELECT status, reason FROM ops.batch_audit WHERE batch_id = 'batch_002'"
    )
    assert (status, reason) == ("ACCEPTED", "DUPLICATE_FILE(first_batch=batch_001)")
    # Nothing new: three re-sent rows are duplicates, the two unplaceable rows are quarantined again.
    assert audit_counts(pipeline_env)[("batch_002", EPIC)] == (5, 0, 0, 0, 3, 0, 0, 2, 0, 0, "RECONCILED")
    [warning] = [e for e in log_events(capsys) if e["event"] == "duplicate_file_delivered"]
    assert (warning["batch_id"], warning["file_name"], warning["reason_code"]) == ("batch_002", EPIC, "DUPLICATE_FILE")


def test_changed_redelivery_of_a_processed_batch_is_logged_not_reloaded(landing_dir, pipeline_env, capsys):
    write_batches(landing_dir, "batch_001")
    assert main(pipeline_env) == 0
    before = snapshot(pipeline_env, include_run_times=True)
    replacement = encounter_file("EPIC_NORTH", [("E9", T1)])
    (landing_dir / "batch_001" / EPIC).write_bytes(replacement.content)
    capsys.readouterr()

    assert main(pipeline_env) == 0

    assert snapshot(pipeline_env, include_run_times=True) == before
    [warning] = [e for e in log_events(capsys) if e["event"] == "batch_redelivered_with_changes"]
    assert (warning["batch_id"], warning["reason_code"]) == ("batch_001", "BATCH_REDELIVERED_CHANGED")


# --- schema guard ---


def test_a_database_from_before_task4_is_refused(tmp_path):
    path = tmp_path / "work" / "raw.duckdb"
    path.parent.mkdir(parents=True)
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA ops")
    con.execute("CREATE TABLE ops.batch_audit (batch_id VARCHAR, file_name VARCHAR, duplicate_count INTEGER)")
    con.close()

    with pytest.raises(PipelineError, match="predates the Task 4 columns"):
        open_store(path, CONTRACTS.canonical_columns)
