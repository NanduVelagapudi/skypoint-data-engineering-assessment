"""Runs the pipeline on the real data pack (read only) into a temp DuckDB file and temp output folder."""

import csv
import hashlib
import io
import json
import logging
from pathlib import Path

import pytest
from conftest import CONTRACT_PATH, REAL_DATA_DIR, REPO_ROOT, TEST_PATIENT_KEY_SECRET

from pipeline.main import main
from pipeline.raw_store import open_store
from pipeline.schema_contract import load_contracts

CONTRACTS = load_contracts(CONTRACT_PATH)
LANDING = REAL_DATA_DIR / "landing"
EPIC = "encounters_epic_north.csv"
MEDITECH = "encounters_legacy_meditech.csv"
ATHENA = "encounters_athena_clinics.csv"


def fingerprint(folder: Path) -> dict[str, str]:
    return {
        str(p.relative_to(folder)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(folder.rglob("*"))
        if p.is_file()
    }


def manifest_counts() -> dict[tuple[str, str], int]:
    counts = {}
    for manifest_path in sorted(LANDING.glob("batch_*/manifest.json")):
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for entry in manifest["files"]:
            counts[(manifest["batch_id"], entry["file_name"])] = entry["row_count"]
    return counts


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("real_pack")
    env = {
        "DATA_DIR": str(REAL_DATA_DIR),
        "OUTPUT_DIR": str(tmp / "output"),
        "RAW_DB_PATH": str(tmp / "work" / "raw.duckdb"),
        "SCHEMA_CONTRACT_PATH": str(CONTRACT_PATH),
        "LOG_LEVEL": "WARNING",
        "PATIENT_KEY_HMAC_SECRET": TEST_PATIENT_KEY_SECRET,
    }
    data_before = fingerprint(REAL_DATA_DIR)
    repo_output_before = fingerprint(REPO_ROOT / "output")
    logger = logging.getLogger("pipeline")
    handlers, level = logger.handlers[:], logger.level

    exit_code = main(env)
    state = {
        "env": env,
        "exit_code": exit_code,
        "con": open_store(Path(env["RAW_DB_PATH"]), CONTRACTS.canonical_columns),
        "data_before": data_before,
        "repo_output_before": repo_output_before,
    }
    try:
        yield state
    finally:
        state["con"].close()  # the second-run test replaces the connection
        logger.handlers[:] = handlers
        logger.setLevel(level)


def audit(con):
    rows = con.execute(
        "SELECT batch_id, file_name, status, reason, expected_count, received_count, accepted_count "
        "FROM ops.batch_audit ORDER BY batch_id, file_name"
    ).fetchall()
    return {(r[0], r[1]): r[2:] for r in rows}


def test_run_exits_zero(run):
    assert run["exit_code"] == 0


def test_batches_001_to_003_accepted_and_batch_004_rejected(run):
    result = audit(run["con"])
    expected = manifest_counts()

    not_versioned = {
        (b, f): n
        for b, f, n in run["con"].execute(
            "SELECT batch_id, file_name, duplicate_count + stale_count + quarantined_count FROM ops.batch_audit"
        ).fetchall()
    }

    assert sorted(result) == sorted(expected)
    for (batch_id, file_name), (status, reason, exp, received, accepted) in result.items():
        if batch_id == "batch_004":
            assert status == "REJECTED"
        else:
            assert (status, reason) == ("ACCEPTED", None)
            assert exp == received == expected[(batch_id, file_name)]
            # accepted_count is rows that created a new encounter or version (Task 4)
            assert accepted == received - not_versioned[(batch_id, file_name)]


def test_batch_004_reasons(run):
    result = audit(run["con"])

    assert result[("batch_004", EPIC)] == (
        "REJECTED",
        "SHA256_MISMATCH; MALFORMED_RECORD(record=18,fields=3,expected=21); ROW_COUNT_MISMATCH(expected=22,received=18)",
        22,
        18,
        0,
    )
    assert result[("batch_004", MEDITECH)] == ("REJECTED", "SIBLING_FILE_REJECTED", 22, 22, 0)
    assert result[("batch_004", ATHENA)] == ("REJECTED", "SIBLING_FILE_REJECTED", 22, 22, 0)


def test_no_rows_from_any_batch_004_file(run):
    con = run["con"]

    assert con.execute("SELECT count(*) FROM raw.encounters WHERE batch_id = 'batch_004'").fetchone() == (0,)
    assert con.execute("SELECT count(*) FROM raw.ingested_files WHERE batch_id = 'batch_004'").fetchone() == (0,)


def test_raw_row_counts_reconcile_with_manifests(run):
    con = run["con"]
    loaded = dict(
        ((b, f), n)
        for b, f, n in con.execute("SELECT batch_id, file_name, count(*) FROM raw.encounters GROUP BY ALL").fetchall()
    )
    expected = {k: v for k, v in manifest_counts().items() if k[0] != "batch_004"}

    assert loaded == expected
    assert con.execute("SELECT count(*) FROM raw.encounters").fetchone() == (3636,)
    gaps = con.execute(
        "SELECT batch_id, file_name FROM raw.encounters GROUP BY ALL "
        "HAVING min(source_row_number) <> 1 OR max(source_row_number) <> count(*)"
    ).fetchall()
    assert gaps == []


def test_encounter_source_is_filled_only_for_athena_v2(run):
    nulls = {
        (b, f): (null_count, total, version)
        for b, f, null_count, total, version in run["con"].execute(
            "SELECT e.batch_id, e.file_name, count(*) FILTER (WHERE e.encounter_source IS NULL), count(*), i.schema_version "
            "FROM raw.encounters e JOIN raw.ingested_files i USING (batch_id, file_name) GROUP BY ALL"
        ).fetchall()
    }

    assert nulls[("batch_003", ATHENA)] == (0, 74, "athena_clinics_v2")
    for (batch_id, file_name), (null_count, total, version) in nulls.items():
        if (batch_id, file_name) != ("batch_003", ATHENA):
            assert null_count == total, (batch_id, file_name)
            assert version.endswith("_v1")


def test_every_raw_value_equals_the_source_file(run):
    """Re-read each accepted file independently and compare every field, mapped by header name."""
    con = run["con"]
    canonical = CONTRACTS.canonical_columns
    files = con.execute("SELECT batch_id, file_name, source_system, schema_version FROM raw.ingested_files").fetchall()
    assert len(files) == 9

    for batch_id, file_name, source_system, schema_version in files:
        version = next(v for v in CONTRACTS.source_systems[source_system].header_versions if v.version == schema_version)
        delivered_name = {version.canonical_name(c): c for c in version.columns}
        text = (LANDING / batch_id / file_name).read_bytes().decode("utf-8-sig")
        header, *records = list(csv.reader(io.StringIO(text, newline="")))
        expected = [
            tuple(r[header.index(delivered_name[c])] if c in delivered_name else None for c in canonical)
            for r in records
        ]

        stored = con.execute(
            f"SELECT {', '.join(canonical)} FROM raw.encounters "
            "WHERE batch_id = ? AND file_name = ? ORDER BY source_row_number",
            [batch_id, file_name],
        ).fetchall()
        assert stored == expected, (batch_id, file_name)


def test_audit_csv_written_to_temp_output_only(run):
    with (Path(run["env"]["OUTPUT_DIR"]) / "batch_audit.csv").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 12
    assert {r["status"] for r in rows if r["batch_id"] == "batch_004"} == {"REJECTED"}
    assert {r["status"] for r in rows if r["batch_id"] != "batch_004"} == {"ACCEPTED"}
    assert fingerprint(REPO_ROOT / "output") == run["repo_output_before"]


def test_data_pack_is_unchanged(run):
    assert fingerprint(REAL_DATA_DIR) == run["data_before"]


def test_second_run_changes_nothing(run):
    con = run["con"]
    before = [con.execute(f"SELECT count(*) FROM {t}").fetchone() for t in ("raw.encounters", "raw.ingested_files", "ops.batch_audit")]
    csv_before = (Path(run["env"]["OUTPUT_DIR"]) / "batch_audit.csv").read_bytes()
    con.close()  # release the file for the second run
    logger = logging.getLogger("pipeline")
    handlers, level = logger.handlers[:], logger.level
    try:
        assert main(run["env"]) == 0
    finally:
        logger.handlers[:] = handlers
        logger.setLevel(level)

    run["con"] = con = open_store(Path(run["env"]["RAW_DB_PATH"]), CONTRACTS.canonical_columns)
    after = [con.execute(f"SELECT count(*) FROM {t}").fetchone() for t in ("raw.encounters", "raw.ingested_files", "ops.batch_audit")]
    assert after == before == [(3636,), (9,), (12,)]
    assert (Path(run["env"]["OUTPUT_DIR"]) / "batch_audit.csv").read_bytes() == csv_before
