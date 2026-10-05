"""Raw store tests. Each test gets its own DuckDB file under pytest's tmp_path."""

import dataclasses
import logging
from datetime import UTC, datetime, timedelta, timezone

import duckdb
import pytest
from conftest import CONTRACT_PATH, contract_header, csv_bytes, synthetic_file

from pipeline import batch_audit
from pipeline.batch_audit import AuditRow, BatchStatus
from pipeline.csv_reader import parse_csv
from pipeline.errors import ConfigError
from pipeline.manifest import sha256_hex
from pipeline.raw_store import LINEAGE_COLUMNS, AcceptedFile, open_store, write_accepted_batch, write_rejected_batch
from pipeline.schema_contract import load_contracts, match_header

CONTRACTS = load_contracts(CONTRACT_PATH)
CANONICAL = CONTRACTS.canonical_columns
IST = timezone(timedelta(hours=5, minutes=30))
INGESTED_AT = datetime(2026, 10, 5, 14, 30, 0, tzinfo=IST)  # 09:00:00 UTC


@pytest.fixture
def store(tmp_path):
    con = open_store(tmp_path / "work" / "raw.duckdb", CANONICAL)
    yield con
    con.close()


def accepted_file(batch_id, source_system, content, *, file_name=None):
    """Parse and match synthetic bytes the way the pipeline will."""
    parsed = parse_csv(content)
    match = match_header(CONTRACTS, source_system, parsed.header)
    return AcceptedFile(
        batch_id=batch_id,
        file_name=file_name or CONTRACTS.source_systems[source_system].file_name,
        source_system=source_system,
        schema_version=match.version.version,
        file_sha256=sha256_hex(content),
        manifest_row_count=parsed.record_count,
        delivered_at="2025-01-06T06:00:00Z",
        source_header=tuple(parsed.header),
        rows=tuple(match.to_canonical(record) for record in parsed.records),
    )


def audit_rows_for(files, status=BatchStatus.ACCEPTED):
    start = datetime(2026, 10, 5, 9, 0, tzinfo=UTC)
    return [
        AuditRow(
            batch_id=f.batch_id,
            file_name=f.file_name,
            source_system=f.source_system,
            expected_count=f.manifest_row_count,
            received_count=len(f.rows),
            accepted_count=len(f.rows),
            status=status,
            reason=None,
            start_time=start,
            end_time=start + timedelta(seconds=1),
        )
        for f in files
    ]


def counts(con):
    return tuple(
        con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        for table in ("raw.encounters", "raw.ingested_files", "ops.batch_audit")
    )


# --- schema ---


def test_tables_have_lineage_then_canonical_columns(store):
    described = store.execute("DESCRIBE raw.encounters").fetchall()

    assert [row[0] for row in described] == list(LINEAGE_COLUMNS) + list(CANONICAL)
    assert {row[1] for row in described[len(LINEAGE_COLUMNS):]} == {"VARCHAR"}


@pytest.mark.parametrize("file_name", ["raw.duckdb", "ops.duckdb", "raw_store.duckdb", "it's here.duckdb"])
def test_database_file_name_cannot_collide_with_schema_names(tmp_path, file_name):
    con = open_store(tmp_path / file_name, CANONICAL)
    try:
        assert counts(con) == (0, 0, 0)
    finally:
        con.close()
    assert (tmp_path / file_name).is_file()


def test_open_store_is_idempotent_and_keeps_rows(tmp_path):
    db_path = tmp_path / "raw.duckdb"
    epic = accepted_file("batch_001", "EPIC_NORTH", synthetic_file("EPIC_NORTH", 4).content)
    con = open_store(db_path, CANONICAL)
    write_accepted_batch(con, "batch_001", [epic], audit_rows_for([epic]), INGESTED_AT)
    con.close()

    con = open_store(db_path, CANONICAL)
    try:
        assert counts(con) == (4, 1, 1)
    finally:
        con.close()


# --- rows land unchanged ---


def test_rows_land_exactly_as_delivered_with_lineage(store):
    header = contract_header("EPIC_NORTH")
    tricky = [" padded ", "", 'Smith, "J"', "line1\r\nline2", "José", "N/A", "$1,250.00", "\\x"]
    rows = [[f"{tricky[(i + j) % len(tricky)]}" for j in range(len(header))] for i in range(6)]
    content = csv_bytes(header, rows, newline="\r\n", bom=True)
    epic = accepted_file("batch_001", "EPIC_NORTH", content)

    write_accepted_batch(store, "batch_001", [epic], audit_rows_for([epic]), INGESTED_AT)

    stored = store.execute(
        f"SELECT {', '.join(LINEAGE_COLUMNS)}, {', '.join(header)} FROM raw.encounters ORDER BY source_row_number"
    ).fetchall()
    assert [list(row[len(LINEAGE_COLUMNS):]) for row in stored] == rows
    assert [row[:len(LINEAGE_COLUMNS)] for row in stored] == [
        ("batch_001", "encounters_epic_north.csv", n, sha256_hex(content), datetime(2026, 10, 5, 9, 0))
        for n in range(1, 7)
    ]


def test_absent_canonical_column_is_null_and_delivered_empty_stays_empty(store):
    epic = accepted_file("batch_003", "EPIC_NORTH", synthetic_file("EPIC_NORTH", 3).content)
    v2_header = contract_header("ATHENA_CLINICS", "athena_clinics_v2")
    v2_rows = [[f"{c}-{i}" for c in v2_header] for i in range(1, 4)]
    v2_rows[1][v2_header.index("encounter_source")] = ""
    athena = accepted_file("batch_003", "ATHENA_CLINICS", csv_bytes(v2_header, v2_rows))

    write_accepted_batch(store, "batch_003", [epic, athena], audit_rows_for([epic, athena]), INGESTED_AT)

    by_file = dict(
        store.execute(
            "SELECT file_name, list(encounter_source ORDER BY source_row_number) FROM raw.encounters GROUP BY file_name"
        ).fetchall()
    )
    assert by_file["encounters_epic_north.csv"] == [None, None, None]
    assert by_file["encounters_athena_clinics.csv"] == ["encounter_source-1", "", "encounter_source-3"]
    renamed = store.execute(
        "SELECT billed_amount, attending_npi FROM raw.encounters "
        "WHERE file_name = 'encounters_athena_clinics.csv' ORDER BY source_row_number"
    ).fetchall()
    assert renamed[0] == ("total_charge-1", "attending_provider_npi-1")


def test_ingested_files_keeps_version_and_delivered_header(store):
    content = synthetic_file("ATHENA_CLINICS", 2, version="athena_clinics_v2", bom=True).content
    athena = accepted_file("batch_003", "ATHENA_CLINICS", content)

    write_accepted_batch(store, "batch_003", [athena], audit_rows_for([athena]), INGESTED_AT)

    row = store.execute(
        "SELECT source_system, schema_version, file_sha256, manifest_row_count, delivered_at, source_header, ingested_at "
        "FROM raw.ingested_files"
    ).fetchone()
    assert row == (
        "ATHENA_CLINICS",
        "athena_clinics_v2",
        sha256_hex(content),
        2,
        "2025-01-06T06:00:00Z",
        contract_header("ATHENA_CLINICS", "athena_clinics_v2"),
        datetime(2026, 10, 5, 9, 0),
    )


def test_file_with_zero_rows_is_recorded(store):
    empty = accepted_file("batch_001", "EPIC_NORTH", synthetic_file("EPIC_NORTH", 0).content)

    write_accepted_batch(store, "batch_001", [empty], audit_rows_for([empty]), INGESTED_AT)

    assert counts(store) == (0, 1, 1)


# --- duplicates, rollback, rejection ---


def test_primary_key_blocks_a_duplicate_load(store):
    epic = accepted_file("batch_001", "EPIC_NORTH", synthetic_file("EPIC_NORTH", 5).content)
    write_accepted_batch(store, "batch_001", [epic], audit_rows_for([epic]), INGESTED_AT)

    with pytest.raises(duckdb.ConstraintException):
        write_accepted_batch(store, "batch_001", [epic], audit_rows_for([epic]), INGESTED_AT)

    assert counts(store) == (5, 1, 1)


def test_failure_mid_batch_rolls_back_everything(store, monkeypatch, caplog):
    files = [
        accepted_file("batch_002", "EPIC_NORTH", synthetic_file("EPIC_NORTH", 4).content),
        accepted_file("batch_002", "LEGACY_MEDITECH", synthetic_file("LEGACY_MEDITECH", 3).content),
    ]

    def fail_after_raw_rows(con, rows):
        assert counts(con)[:2] == (7, 2)  # raw rows were written inside the transaction
        raise RuntimeError("simulated failure")

    monkeypatch.setattr(batch_audit, "insert_audit_rows", fail_after_raw_rows)
    with caplog.at_level(logging.ERROR, logger="pipeline"):
        with pytest.raises(RuntimeError):
            write_accepted_batch(store, "batch_002", files, audit_rows_for(files), INGESTED_AT)

    assert counts(store) == (0, 0, 0)
    [record] = [r for r in caplog.records if r.getMessage() == "batch_write_rolled_back"]
    assert (record.batch_id, record.error_type) == ("batch_002", "RuntimeError")

    monkeypatch.undo()
    write_accepted_batch(store, "batch_002", files, audit_rows_for(files), INGESTED_AT)
    assert counts(store) == (7, 2, 2)


def test_rejected_batch_writes_only_audit_rows(store):
    epic = accepted_file("batch_004", "EPIC_NORTH", synthetic_file("EPIC_NORTH", 3).content)

    write_rejected_batch(store, "batch_004", audit_rows_for([epic], status=BatchStatus.REJECTED))

    assert counts(store) == (0, 0, 1)
    assert store.execute("SELECT status FROM ops.batch_audit").fetchone() == ("REJECTED",)


def test_naive_ingested_at_is_refused_before_writing(store):
    epic = accepted_file("batch_001", "EPIC_NORTH", synthetic_file("EPIC_NORTH", 2).content)

    with pytest.raises(ValueError, match="timezone-aware"):
        write_accepted_batch(store, "batch_001", [epic], audit_rows_for([epic]), datetime(2026, 10, 5, 9, 0))

    assert counts(store) == (0, 0, 0)


def test_rows_from_another_batch_are_refused(store):
    epic = accepted_file("batch_001", "EPIC_NORTH", synthetic_file("EPIC_NORTH", 2).content)

    with pytest.raises(ValueError, match="belong to the batch"):
        write_accepted_batch(store, "batch_002", [epic], audit_rows_for([epic]), INGESTED_AT)

    assert counts(store) == (0, 0, 0)


def test_unknown_column_in_rows_fails_and_rolls_back(store):
    epic = accepted_file("batch_001", "EPIC_NORTH", synthetic_file("EPIC_NORTH", 2).content)
    bad = dataclasses.replace(epic, rows=tuple({**row, "not_configured": "x"} for row in epic.rows))

    with pytest.raises(duckdb.Error):
        write_accepted_batch(store, "batch_001", [bad], audit_rows_for([bad]), INGESTED_AT)

    assert counts(store) == (0, 0, 0)


def test_lineage_column_name_cannot_be_canonical(tmp_path):
    with pytest.raises(ConfigError, match="lineage column"):
        open_store(tmp_path / "raw.duckdb", [*CANONICAL, "ingested_at"])


def test_unsafe_column_name_is_refused(tmp_path):
    with pytest.raises(ConfigError, match="unsafe column"):
        open_store(tmp_path / "raw.duckdb", ['x" VARCHAR); DROP TABLE raw.encounters; --'])
