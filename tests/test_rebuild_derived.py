"""Task 4 group (c): --rebuild-derived, the rebuild and the migration of a pre-Task-4 database.

Synthetic batches only; every database and output folder lives in tmp_path.
"""

import json
from pathlib import Path

import pytest
from conftest import (
    CONTRACT_PATH,
    FileSpec,
    attach,
    contract_header,
    csv_bytes,
    make_pre_task4,
    state_digest,
    table_columns,
    write_batch,
)

from pipeline import batch_processor, encounter_history
from pipeline.batch_audit import AUDIT_COLUMNS, TASK4_COLUMNS
from pipeline.main import main

pytestmark = pytest.mark.usefixtures("restore_pipeline_logger")

T1, T2, T3 = "2024-03-01T10:00:00Z", "2024-03-02T10:00:00Z", "2024-03-03T10:00:00Z"
EPIC = "encounters_epic_north.csv"


def epic_file(rows):
    header = contract_header("EPIC_NORTH")
    records = []
    for record_id, ts in rows:
        values = {column: f"{column}-zz" for column in header}
        values.update(source_system="EPIC_NORTH", source_record_id=record_id, last_updated_ts=ts)
        records.append([values[column] for column in header])
    return FileSpec(EPIC, "EPIC_NORTH", csv_bytes(header, records), len(records))


BATCHES = {
    "batch_001": [epic_file([("E1", T1), ("E2", T1), ("E2", T1)])],  # E2 repeated in the batch
    "batch_002": [epic_file([("E1", T3), ("E1", T1), ("E3", T1)])],  # E1 newer version; E1 re-sent
    "batch_003": [epic_file([("E1", T2), ("E4", T1)])],  # E1 never-held older version; E4 new
}


def write_batches(landing_dir, *batch_ids):
    for batch_id in batch_ids:
        write_batch(landing_dir, batch_id, BATCHES[batch_id])


def db_path(env):
    return Path(env["RAW_DB_PATH"])


def audit_rows(env, columns=AUDIT_COLUMNS):
    con = attach(db_path(env))
    try:
        return con.execute(f"SELECT {', '.join(columns)} FROM ops.batch_audit ORDER BY batch_id, file_name").fetchall()
    finally:
        con.close()


def has_table(env, schema, name):
    con = attach(db_path(env))
    try:
        return con.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_catalog = current_database() AND table_schema = ? AND table_name = ?",
            [schema, name],
        ).fetchone()[0] == 1
    finally:
        con.close()


def log_events(capsys):
    return [json.loads(line) for line in capsys.readouterr().out.splitlines()]


@pytest.fixture
def pre_task4_env(landing_dir, pipeline_env):
    """A database with batches 001-002 loaded, then taken back to the pre-Task-4 layout."""
    write_batches(landing_dir, "batch_001", "batch_002")
    assert main(pipeline_env) == 0
    make_pre_task4(db_path(pipeline_env))
    return pipeline_env


def test_pre_task4_fixture_has_the_stage1_layout(pre_task4_env):
    con = attach(db_path(pre_task4_env))
    try:
        columns = table_columns(con, "ops.batch_audit")
    finally:
        con.close()

    assert columns == [
        "batch_id", "file_name", "source_system", "expected_count", "received_count", "accepted_count",
        "duplicate_count", "stale_count", "quarantined_count", "status", "reason", "start_time", "end_time",
    ]
    assert not has_table(pre_task4_env, "clean", "encounter_versions")
    assert not has_table(pre_task4_env, "clean", "encounter_row_outcomes")
    assert has_table(pre_task4_env, "clean", "encounter_patients")  # Task 3 predates Task 4


def test_a_normal_run_still_refuses_a_pre_task4_database(pre_task4_env, capsys):
    before = audit_rows(pre_task4_env, ("batch_id", "file_name", "accepted_count"))
    capsys.readouterr()

    assert main(pre_task4_env) == 1

    [failure] = [e for e in log_events(capsys) if e["event"] == "pipeline_failed"]
    assert failure["error_type"] == "PipelineError"
    assert audit_rows(pre_task4_env, ("batch_id", "file_name", "accepted_count")) == before
    assert not has_table(pre_task4_env, "clean", "encounter_versions")


def test_rebuild_migrates_a_pre_task4_database(pre_task4_env, tmp_path, capsys):
    stage1_timings = audit_rows(pre_task4_env, ("batch_id", "file_name", "start_time", "end_time"))
    capsys.readouterr()

    assert main(pre_task4_env, ["--rebuild-derived"]) == 0

    events = [e["event"] for e in log_events(capsys)]
    assert events.index("batch_audit_migrated") < events.index("derived_rebuild_finished")
    con = attach(db_path(pre_task4_env))
    try:
        assert table_columns(con, "ops.batch_audit") == list(AUDIT_COLUMNS)  # same layout as a new table
    finally:
        con.close()
    assert audit_rows(pre_task4_env, ("batch_id", "file_name", "start_time", "end_time")) == stage1_timings
    # Same derived state as a database that was always on Task 4.
    fresh = {**pre_task4_env, "RAW_DB_PATH": str(tmp_path / "fresh" / "raw.duckdb")}
    assert main(fresh) == 0
    assert state_digest(db_path(pre_task4_env)) == state_digest(db_path(fresh))
    # accepted_count now means new encounters + versions (Stage 1 stored rows landed).
    assert audit_rows(pre_task4_env, ("batch_id", "received_count", "accepted_count", "duplicate_count")) == [
        ("batch_001", 3, 2, 1),
        ("batch_002", 3, 2, 1),
    ]
    # After migration a normal run works and loads the next batch incrementally.
    write_batches(Path(pre_task4_env["DATA_DIR"]) / "landing", "batch_003")
    assert main(pre_task4_env) == 0
    assert main(fresh) == 0
    assert state_digest(db_path(pre_task4_env)) == state_digest(db_path(fresh))


def test_a_failed_rebuild_leaves_the_pre_task4_database_unchanged(pre_task4_env, monkeypatch):
    before = state_digest_without_history(pre_task4_env)
    original = encounter_history.apply_batch

    def fail_on_batch_002(con, batch_id, conventions):
        if batch_id == "batch_002":
            raise RuntimeError("simulated failure during the rebuild")
        return original(con, batch_id, conventions)

    monkeypatch.setattr(encounter_history, "apply_batch", fail_on_batch_002)

    assert main(pre_task4_env, ["--rebuild-derived"]) == 1

    assert state_digest_without_history(pre_task4_env) == before  # migration rolled back too
    assert not has_table(pre_task4_env, "clean", "encounter_versions")
    monkeypatch.undo()
    assert main(pre_task4_env) == 1  # still refused
    assert main(pre_task4_env, ["--rebuild-derived"]) == 0  # and the rebuild can be retried


def state_digest_without_history(env):
    con = attach(db_path(env))
    try:
        return {
            table: con.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall()
            for table in ("raw.ingested_files", "ops.batch_audit", "clean.encounter_patients")
        } | {"columns": table_columns(con, "ops.batch_audit")}
    finally:
        con.close()


def test_rebuild_replays_the_incremental_step_in_batch_order(landing_dir, pipeline_env, tmp_path, monkeypatch):
    write_batches(landing_dir, "batch_001", "batch_002")
    assert main(pipeline_env) == 0
    write_batches(landing_dir, "batch_003")  # pending when the rebuild runs
    calls = []
    original = batch_processor._classify_into_history

    def record(con, batch_id, *args):
        calls.append(batch_id)
        return original(con, batch_id, *args)

    monkeypatch.setattr(batch_processor, "_classify_into_history", record)

    assert main(pipeline_env, ["--rebuild-derived"]) == 0

    # Rebuild of the two loaded batches, then the pending batch, all through one step.
    assert calls == ["batch_001", "batch_002", "batch_003"]
    fresh = {**pipeline_env, "RAW_DB_PATH": str(tmp_path / "fresh" / "raw.duckdb")}
    assert main(fresh) == 0
    assert state_digest(db_path(pipeline_env)) == state_digest(db_path(fresh))


def test_rebuild_of_a_current_database_changes_nothing(landing_dir, pipeline_env):
    write_batches(landing_dir, "batch_001", "batch_002", "batch_003")
    write_batch(landing_dir, "batch_004", BATCHES["batch_001"], manifest_overrides={EPIC: {"sha256": "0" * 64}})
    assert main(pipeline_env) == 0
    before = state_digest(db_path(pipeline_env), exclude_run_times=False)
    csv_path = Path(pipeline_env["OUTPUT_DIR"]) / "batch_audit.csv"
    csv_before = csv_path.read_bytes()

    assert main(pipeline_env, ["--rebuild-derived"]) == 0
    assert main(pipeline_env, ["--rebuild-derived"]) == 0

    assert state_digest(db_path(pipeline_env), exclude_run_times=False) == before  # timings included
    assert csv_path.read_bytes() == csv_before
    rejected = audit_rows(pipeline_env, ("status", *TASK4_COLUMNS))[-1]
    assert rejected == ("REJECTED", *(None,) * len(TASK4_COLUMNS))


def test_unknown_argument_exits_2_before_any_work(pipeline_env):
    with pytest.raises(SystemExit) as exit_info:
        main(pipeline_env, ["--rebuild-everything"])

    assert exit_info.value.code == 2
    assert not Path(pipeline_env["RAW_DB_PATH"]).exists()
