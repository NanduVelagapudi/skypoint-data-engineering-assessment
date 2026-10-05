"""End-to-end tests on synthetic batches. DuckDB, landing and output all live in tmp_path."""

import csv
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from conftest import (
    CONTRACT_PATH,
    REPO_ROOT,
    FileSpec,
    contract_header,
    csv_bytes,
    synthetic_file,
    valid_specs,
    write_batch,
)

from pipeline import batch_processor
from pipeline.main import main
from pipeline.raw_store import open_store
from pipeline.schema_contract import load_contracts

pytestmark = pytest.mark.usefixtures("restore_pipeline_logger")

CONTRACTS = load_contracts(CONTRACT_PATH)
EPIC = "encounters_epic_north.csv"
MEDITECH = "encounters_legacy_meditech.csv"
ATHENA = "encounters_athena_clinics.csv"


def query(env, sql):
    con = open_store(Path(env["RAW_DB_PATH"]), CONTRACTS.canonical_columns)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def audit(env):
    rows = query(
        env,
        "SELECT batch_id, file_name, status, reason, expected_count, received_count, accepted_count "
        "FROM ops.batch_audit ORDER BY batch_id, file_name",
    )
    return {(r[0], r[1]): r[2:] for r in rows}


def table_counts(env):
    return tuple(
        query(env, f"SELECT count(*) FROM {t}")[0][0] for t in ("raw.encounters", "raw.ingested_files", "ops.batch_audit")
    )


def log_events(capsys):
    return [json.loads(line) for line in capsys.readouterr().out.splitlines()]


# --- accepted batches ---


def test_valid_batches_are_accepted_in_batch_id_order(landing_dir, pipeline_env, capsys):
    write_batch(landing_dir, "batch_002", valid_specs("b"))  # created first on purpose
    write_batch(landing_dir, "batch_001", valid_specs("a"))
    (landing_dir / "notes").mkdir()  # not a batch folder

    assert main(pipeline_env) == 0

    started = [e["batch_id"] for e in log_events(capsys) if e["event"] == "batch_started"]
    assert started == ["batch_001", "batch_002"]
    assert table_counts(pipeline_env) == (18, 6, 6)
    assert {v[0] for v in audit(pipeline_env).values()} == {"ACCEPTED"}
    assert query(
        pipeline_env, "SELECT min(source_row_number), max(source_row_number) FROM raw.encounters WHERE file_name = 'encounters_epic_north.csv'"
    ) == [(1, 3)]


def test_known_athena_schema_change_is_accepted(landing_dir, pipeline_env):
    specs = [synthetic_file("EPIC_NORTH"), synthetic_file("ATHENA_CLINICS", version="athena_clinics_v2", bom=True)]
    write_batch(landing_dir, "batch_003", specs)

    assert main(pipeline_env) == 0

    assert audit(pipeline_env)[("batch_003", ATHENA)][0] == "ACCEPTED"
    assert query(pipeline_env, "SELECT schema_version FROM raw.ingested_files WHERE file_name = 'encounters_athena_clinics.csv'") == [
        ("athena_clinics_v2",)
    ]


def test_audit_csv_is_written_to_the_output_dir(landing_dir, pipeline_env):
    write_batch(landing_dir, "batch_001", valid_specs())

    main(pipeline_env)

    with (Path(pipeline_env["OUTPUT_DIR"]) / "batch_audit.csv").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert [(r["batch_id"], r["file_name"], r["status"]) for r in rows] == [
        ("batch_001", ATHENA, "ACCEPTED"),
        ("batch_001", EPIC, "ACCEPTED"),
        ("batch_001", MEDITECH, "ACCEPTED"),
    ]


# --- all-or-nothing rejection ---


def test_one_bad_file_rejects_the_whole_batch_and_later_batches_continue(landing_dir, pipeline_env):
    write_batch(landing_dir, "batch_001", valid_specs("a"))
    write_batch(landing_dir, "batch_002", valid_specs("b"), manifest_overrides={EPIC: {"sha256": "0" * 64}})
    write_batch(landing_dir, "batch_003", valid_specs("c"))

    assert main(pipeline_env) == 0

    result = audit(pipeline_env)
    assert result[("batch_002", EPIC)] == ("REJECTED", "SHA256_MISMATCH", 3, 3, 0)
    assert result[("batch_002", MEDITECH)] == ("REJECTED", "SIBLING_FILE_REJECTED", 3, 3, 0)
    assert result[("batch_002", ATHENA)] == ("REJECTED", "SIBLING_FILE_REJECTED", 3, 3, 0)
    assert result[("batch_003", EPIC)][0] == "ACCEPTED"
    assert query(pipeline_env, "SELECT DISTINCT batch_id FROM raw.encounters ORDER BY 1") == [("batch_001",), ("batch_003",)]
    assert query(pipeline_env, "SELECT count(*) FROM raw.ingested_files WHERE batch_id = 'batch_002'") == [(0,)]
    assert table_counts(pipeline_env) == (18, 6, 9)


def test_row_count_mismatch_rejects_the_batch(landing_dir, pipeline_env):
    write_batch(landing_dir, "batch_001", valid_specs(), manifest_overrides={MEDITECH: {"row_count": 4}})

    assert main(pipeline_env) == 0

    assert audit(pipeline_env)[("batch_001", MEDITECH)] == ("REJECTED", "ROW_COUNT_MISMATCH(expected=4,received=3)", 4, 3, 0)
    assert table_counts(pipeline_env) == (0, 0, 3)


def test_hash_valid_but_truncated_file_is_still_rejected(landing_dir, pipeline_env):
    full = synthetic_file("EPIC_NORTH", 5).content
    truncated = FileSpec(EPIC, "EPIC_NORTH", full[: len(full) - 40], 5)  # manifest hash matches the truncated bytes
    write_batch(landing_dir, "batch_001", [truncated, *valid_specs()[1:]])

    assert main(pipeline_env) == 0

    status, reason, *_ = audit(pipeline_env)[("batch_001", EPIC)]
    assert status == "REJECTED" and reason.startswith("MALFORMED_RECORD(record=5,")
    assert table_counts(pipeline_env)[:2] == (0, 0)


def test_unknown_schema_change_rejects_the_batch(landing_dir, pipeline_env):
    header = contract_header("EPIC_NORTH") + ["new_column"]
    epic = FileSpec(EPIC, "EPIC_NORTH", csv_bytes(header, [[f"v{i}" for i in range(len(header))]]), 1)
    write_batch(landing_dir, "batch_001", [epic, *valid_specs()[1:]])

    assert main(pipeline_env) == 0

    status, reason, *_ = audit(pipeline_env)[("batch_001", EPIC)]
    assert status == "REJECTED"
    assert reason == "UNKNOWN_SCHEMA_CHANGE(closest_version=epic_north_v1,columns=22,expected_columns=21,unexpected_positions=22)"
    assert table_counts(pipeline_env)[:2] == (0, 0)


def test_manifest_level_rejection_writes_one_manifest_row(landing_dir, pipeline_env):
    (write_batch(landing_dir, "batch_001", valid_specs()) / "manifest.json").unlink()

    assert main(pipeline_env) == 0

    assert audit(pipeline_env) == {("batch_001", "manifest.json"): ("REJECTED", "MANIFEST_MISSING", None, None, 0)}
    assert table_counts(pipeline_env) == (0, 0, 1)


def test_every_file_is_validated_before_anything_is_written(landing_dir, pipeline_env, monkeypatch):
    write_batch(landing_dir, "batch_001", valid_specs("a"))
    # Meditech sorts last, so it is the final file checked.
    write_batch(landing_dir, "batch_002", valid_specs("b"), manifest_overrides={MEDITECH: {"sha256": "0" * 64}})
    order = []

    def record(name, function):
        def wrapper(*args, **kwargs):
            order.append(name)
            return function(*args, **kwargs)

        return wrapper

    store = batch_processor.raw_store
    monkeypatch.setattr(batch_processor, "_check_file", record("check", batch_processor._check_file))
    monkeypatch.setattr(store, "write_accepted_batch", record("write_accepted", store.write_accepted_batch))
    monkeypatch.setattr(store, "write_rejected_batch", record("write_rejected", store.write_rejected_batch))

    assert main(pipeline_env) == 0

    assert order == ["check"] * 3 + ["write_accepted"] + ["check"] * 3 + ["write_rejected"]
    assert table_counts(pipeline_env) == (9, 3, 6)


# --- re-runs and incremental runs ---


def test_rerun_creates_no_duplicates_and_reexports_the_same_csv(landing_dir, pipeline_env, capsys):
    write_batch(landing_dir, "batch_001", valid_specs("a"))
    write_batch(landing_dir, "batch_002", valid_specs("b"), manifest_overrides={EPIC: {"row_count": 9}})
    assert main(pipeline_env) == 0
    first_counts = table_counts(pipeline_env)
    csv_path = Path(pipeline_env["OUTPUT_DIR"]) / "batch_audit.csv"
    first_csv = csv_path.read_bytes()
    capsys.readouterr()

    assert main(pipeline_env) == 0

    assert table_counts(pipeline_env) == first_counts == (9, 3, 6)
    assert csv_path.read_bytes() == first_csv
    events = log_events(capsys)
    assert [e["batch_id"] for e in events if e["event"] == "batch_already_processed"] == ["batch_001", "batch_002"]
    assert not [e for e in events if e["event"] == "batch_started"]


def test_only_new_batches_are_processed_on_a_later_run(landing_dir, pipeline_env, capsys):
    write_batch(landing_dir, "batch_001", valid_specs("a"))
    assert main(pipeline_env) == 0
    write_batch(landing_dir, "batch_002", valid_specs("b"))
    capsys.readouterr()

    assert main(pipeline_env) == 0

    assert [e["batch_id"] for e in log_events(capsys) if e["event"] == "batch_started"] == ["batch_002"]
    assert table_counts(pipeline_env) == (18, 6, 6)


# --- exit codes ---


def test_missing_landing_folder_is_a_system_failure(tmp_path, pipeline_env, capsys):
    env = {**pipeline_env, "DATA_DIR": str(tmp_path / "nowhere")}

    assert main(env) == 1

    [failure] = [e for e in log_events(capsys) if e["event"] == "pipeline_failed"]
    assert failure["error_type"] == "PipelineError"


def test_invalid_configuration_is_a_system_failure(tmp_path, pipeline_env, capsys):
    env = {**pipeline_env, "RAW_DB_PATH": str(Path(pipeline_env["OUTPUT_DIR"]) / "raw.duckdb")}

    assert main(env) == 1

    [failure] = [e for e in log_events(capsys) if e["event"] == "pipeline_failed"]
    assert (failure["step"], failure["error_type"]) == ("config", "ConfigError")
    assert not (Path(pipeline_env["OUTPUT_DIR"]) / "raw.duckdb").exists()


def test_unexpected_exception_is_a_system_failure(landing_dir, pipeline_env, monkeypatch):
    write_batch(landing_dir, "batch_001", valid_specs())

    def boom(*args, **kwargs):
        raise OSError("disk unavailable")

    monkeypatch.setattr(batch_processor.raw_store, "write_accepted_batch", boom)

    assert main(pipeline_env) == 1


def test_module_entry_point_exits_zero_with_a_rejected_batch(landing_dir, pipeline_env):
    write_batch(landing_dir, "batch_001", valid_specs())
    write_batch(landing_dir, "batch_002", valid_specs(), manifest_overrides={EPIC: {"sha256": "0" * 64}})
    env = {**os.environ, **pipeline_env, "PYTHONPATH": str(REPO_ROOT / "src"), "PYTHONDONTWRITEBYTECODE": "1"}

    completed = subprocess.run(
        [sys.executable, "-m", "pipeline.main"], cwd=REPO_ROOT, env=env, capture_output=True, text=True, timeout=120
    )

    assert completed.returncode == 0
    events = [json.loads(line) for line in completed.stdout.splitlines()]
    assert events[-1]["event"] == "pipeline_finished"
    assert events[-1]["status"] == "COMPLETED_WITH_REJECTIONS"
    assert completed.stderr == ""
