"""Batch audit tests. Each test gets its own DuckDB file and output folder under tmp_path."""

import csv
import os
from datetime import UTC, datetime, timedelta, timezone

import duckdb
import pytest
from conftest import CONTRACT_PATH

from pipeline.batch_audit import (
    AUDIT_COLUMNS,
    AuditRow,
    BatchStatus,
    db_timestamp,
    export_csv,
    processed_batch_ids,
)
from pipeline.raw_store import AcceptedFile, open_store, write_accepted_batch, write_rejected_batch
from pipeline.schema_contract import load_contracts

CONTRACTS = load_contracts(CONTRACT_PATH)
IST = timezone(timedelta(hours=5, minutes=30))
START = datetime(2026, 10, 5, 14, 30, 0, 250000, tzinfo=IST)  # 09:00:00.250 UTC


@pytest.fixture
def store(tmp_path):
    con = open_store(tmp_path / "work" / "raw.duckdb", CONTRACTS.canonical_columns)
    yield con
    con.close()


def audit_row(batch_id, file_name, status=BatchStatus.ACCEPTED, reason=None, count=3):
    accepted = count if status == BatchStatus.ACCEPTED else 0
    return AuditRow(
        batch_id=batch_id,
        file_name=file_name,
        source_system="EPIC_NORTH",
        expected_count=count,
        received_count=count,
        accepted_count=accepted,
        status=status,
        reason=reason,
        start_time=START,
        end_time=START + timedelta(seconds=2),
    )


def read_csv(path):
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.reader(handle))


def test_table_columns_match_audit_columns(store):
    assert [row[0] for row in store.execute("DESCRIBE ops.batch_audit").fetchall()] == list(AUDIT_COLUMNS)


def test_pending_batches_are_those_without_audit_rows(store):
    assert processed_batch_ids(store) == set()

    write_accepted_batch(store, "batch_001", [], [audit_row("batch_001", "a.csv")], START)
    write_rejected_batch(store, "batch_004", [audit_row("batch_004", "b.csv", BatchStatus.REJECTED, "SHA256_MISMATCH")])

    assert processed_batch_ids(store) == {"batch_001", "batch_004"}


def test_stage1_duplicate_stale_and_quarantined_counts_are_null(store):
    write_accepted_batch(store, "batch_001", [], [audit_row("batch_001", "a.csv")], START)

    assert store.execute(
        "SELECT duplicate_count, stale_count, quarantined_count FROM ops.batch_audit"
    ).fetchone() == (None, None, None)


def test_one_audit_row_per_batch_and_file(store):
    write_rejected_batch(store, "batch_004", [audit_row("batch_004", "a.csv", BatchStatus.REJECTED)])

    with pytest.raises(duckdb.ConstraintException):
        write_rejected_batch(store, "batch_004", [audit_row("batch_004", "a.csv", BatchStatus.REJECTED)])

    assert store.execute("SELECT count(*) FROM ops.batch_audit").fetchone() == (1,)


def test_status_must_be_accepted_or_rejected(store):
    with pytest.raises(duckdb.ConstraintException):
        write_rejected_batch(store, "batch_009", [audit_row("batch_009", "a.csv", status="PENDING")])


def test_db_timestamp_converts_to_utc_and_refuses_naive_values():
    assert db_timestamp(START) == datetime(2026, 10, 5, 9, 0, 0, 250000)
    with pytest.raises(ValueError):
        db_timestamp(datetime(2026, 10, 5, 9, 0))


def test_export_is_sorted_with_blank_nulls_and_utc_timestamps(store, tmp_path):
    # Written out of order on purpose.
    write_rejected_batch(
        store,
        "batch_004",
        [
            audit_row("batch_004", "encounters_legacy_meditech.csv", BatchStatus.REJECTED, "SIBLING_FILE_REJECTED"),
            audit_row("batch_004", "encounters_epic_north.csv", BatchStatus.REJECTED, "SHA256_MISMATCH"),
        ],
    )
    write_accepted_batch(store, "batch_001", [], [audit_row("batch_001", "encounters_epic_north.csv")], START)
    path = tmp_path / "output" / "batch_audit.csv"

    export_csv(store, path)

    header, *rows = read_csv(path)
    assert header == list(AUDIT_COLUMNS)
    assert [(r[0], r[1]) for r in rows] == [
        ("batch_001", "encounters_epic_north.csv"),
        ("batch_004", "encounters_epic_north.csv"),
        ("batch_004", "encounters_legacy_meditech.csv"),
    ]
    first = dict(zip(header, rows[0]))
    assert first["status"] == "ACCEPTED" and first["reason"] == ""
    assert (first["duplicate_count"], first["stale_count"], first["quarantined_count"]) == ("", "", "")
    assert (first["start_time"], first["end_time"]) == ("2026-10-05T09:00:00.250Z", "2026-10-05T09:00:02.250Z")
    assert dict(zip(header, rows[1]))["reason"] == "SHA256_MISMATCH"
    assert dict(zip(header, rows[1]))["accepted_count"] == "0"


def test_export_replaces_the_file_and_leaves_no_temp_file(store, tmp_path):
    out = tmp_path / "output"
    out.mkdir()
    (out / "batch_audit.csv").write_text("stale content\n", encoding="utf-8")
    write_accepted_batch(store, "batch_001", [], [audit_row("batch_001", "a.csv")], START)

    export_csv(store, out / "batch_audit.csv")

    assert read_csv(out / "batch_audit.csv")[0] == list(AUDIT_COLUMNS)
    assert sorted(p.name for p in out.iterdir()) == ["batch_audit.csv"]


def test_failed_export_keeps_previous_file_and_removes_temp_file(store, tmp_path, monkeypatch):
    out = tmp_path / "output"
    out.mkdir()
    (out / "batch_audit.csv").write_text("previous export\n", encoding="utf-8")

    def fail(src, dst):
        raise OSError("disk full")

    monkeypatch.setattr(os, "replace", fail)
    with pytest.raises(OSError):
        export_csv(store, out / "batch_audit.csv")

    assert (out / "batch_audit.csv").read_text(encoding="utf-8") == "previous export\n"
    assert sorted(p.name for p in out.iterdir()) == ["batch_audit.csv"]


def test_audit_csv_contains_no_row_values(store, tmp_path):
    sentinel = "ZZPHI"
    columns = CONTRACTS.canonical_columns
    accepted = AcceptedFile(
        batch_id="batch_001",
        file_name="encounters_epic_north.csv",
        source_system="EPIC_NORTH",
        schema_version="epic_north_v1",
        file_sha256="a" * 64,
        manifest_row_count=2,
        delivered_at="2025-01-06T06:00:00Z",
        source_header=tuple(columns),
        rows=tuple({c: f"{sentinel}-{c}-{i}" for c in columns} for i in range(2)),
    )
    write_accepted_batch(store, "batch_001", [accepted], [audit_row("batch_001", accepted.file_name, count=2)], START)
    write_rejected_batch(
        store, "batch_004", [audit_row("batch_004", "encounters_epic_north.csv", BatchStatus.REJECTED, "SHA256_MISMATCH")]
    )
    assert store.execute("SELECT count(*) FROM raw.encounters").fetchone() == (2,)  # sentinel rows are stored

    path = tmp_path / "output" / "batch_audit.csv"
    export_csv(store, path)

    text = path.read_text(encoding="utf-8")
    assert sentinel not in text
    assert read_csv(path)[0] == list(AUDIT_COLUMNS)
