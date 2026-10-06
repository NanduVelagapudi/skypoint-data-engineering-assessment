"""Shared fixtures: synthetic batches built in tmp_path.

Synthetic values are obviously fake ("patient_mrn-3" and similar), so tests never
handle PHI and never touch the real data pack. Manifests are written with the
correct SHA-256 and row count unless a test overrides them.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

from pipeline.parsers.amount import parse_amount
from pipeline.parsers.categorical import ClaimStatus, parse_claim_status
from pipeline.parsers.dates import parse_date
from pipeline.source_conventions import load_source_conventions

REPO_ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = REPO_ROOT / "config" / "schema_contracts.json"
REAL_DATA_DIR = REPO_ROOT / "data"

UTF8_BOM = b"\xef\xbb\xbf"

# Test-only HMAC key for patient_key. Not a real secret.
TEST_PATIENT_KEY_SECRET = "test-only-patient-key-secret-ZZSECRET"


@dataclass(frozen=True)
class FileSpec:
    file_name: str
    source_system: str
    content: bytes
    row_count: int


def _contracts() -> dict:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def contract_header(source_system: str, version: str | None = None) -> list[str]:
    """Delivered header for a configured version (the first version by default)."""
    versions = _contracts()["source_systems"][source_system]["header_versions"]
    if version is None:
        return list(versions[0]["columns"])
    for v in versions:
        if v["version"] == version:
            return list(v["columns"])
    raise KeyError(version)


def csv_bytes(header: list[str], rows: list[list[str]], *, newline: str = "\n", bom: bool = False) -> bytes:
    buf = io.StringIO(newline="")
    writer = csv.writer(buf, lineterminator=newline)
    writer.writerow(header)
    writer.writerows(rows)
    data = buf.getvalue().encode("utf-8")
    return UTF8_BOM + data if bom else data


def synthetic_file(
    source_system: str,
    n_rows: int = 3,
    *,
    version: str | None = None,
    newline: str = "\n",
    bom: bool = False,
    tag: str = "x",
) -> FileSpec:
    """A valid file for `source_system` whose values are '<column>-<tag><n>'."""
    contracts = _contracts()["source_systems"]
    header = contract_header(source_system, version)
    rows = [[f"{col}-{tag}{i}" for col in header] for i in range(1, n_rows + 1)]
    return FileSpec(
        file_name=contracts[source_system]["file_name"],
        source_system=source_system,
        content=csv_bytes(header, rows, newline=newline, bom=bom),
        row_count=n_rows,
    )


def write_batch(
    landing_dir: Path,
    batch_id: str,
    specs: list[FileSpec],
    manifest_overrides: dict[str, dict] | None = None,
) -> Path:
    """Write the files and a manifest. `manifest_overrides[file_name]` patches that entry."""
    batch_dir = landing_dir / batch_id
    batch_dir.mkdir(parents=True)
    entries = []
    for spec in specs:
        (batch_dir / spec.file_name).write_bytes(spec.content)
        entry = {
            "file_name": spec.file_name,
            "source_system": spec.source_system,
            "row_count": spec.row_count,
            "sha256": hashlib.sha256(spec.content).hexdigest(),
        }
        entry.update((manifest_overrides or {}).get(spec.file_name, {}))
        entries.append(entry)
    manifest = {"batch_id": batch_id, "delivered_at": "2025-01-06T06:00:00Z", "files": entries}
    (batch_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return batch_dir


def valid_specs(tag: str = "x", n_rows: int = 3) -> list[FileSpec]:
    """One valid file per source system."""
    return [
        synthetic_file("EPIC_NORTH", n_rows, tag=tag),
        synthetic_file("LEGACY_MEDITECH", n_rows, tag=tag, newline="\r\n"),
        synthetic_file("ATHENA_CLINICS", n_rows, tag=tag),
    ]


@pytest.fixture
def landing_dir(tmp_path: Path) -> Path:
    path = tmp_path / "data" / "landing"
    path.mkdir(parents=True)
    return path


@pytest.fixture
def pipeline_env(tmp_path: Path, landing_dir: Path) -> dict[str, str]:
    """Environment for load_settings() pointing at tmp_path, with the real schema contract."""
    return {
        "DATA_DIR": str(landing_dir.parent),
        "OUTPUT_DIR": str(tmp_path / "output"),
        "RAW_DB_PATH": str(tmp_path / "work" / "raw.duckdb"),
        "SCHEMA_CONTRACT_PATH": str(CONTRACT_PATH),
        "REFERENCE_DIR": str(REAL_DATA_DIR / "reference"),
        "LOG_LEVEL": "INFO",
        "PATIENT_KEY_HMAC_SECRET": TEST_PATIENT_KEY_SECRET,
        # Synthetic rows are deliberately invalid ("<column>-x1"), so almost every row fails an
        # error-level check. 1 means the publish gate never blocks: these tests exercise the
        # other stages. Gate tests set their own threshold; real-data tests use the 5% default.
        "DQ_GATE_MAX_ERROR_SHARE": "1",
    }


@pytest.fixture
def restore_pipeline_logger():
    """Undo configure_logging() so tests don't leak handlers into each other."""
    logger = logging.getLogger("pipeline")
    handlers, level = logger.handlers[:], logger.level
    yield
    logger.handlers[:] = handlers
    logger.setLevel(level)


# --- valid synthetic encounter rows (Task 6 DQ tests) ---


@pytest.fixture(scope="session")
def roster_npi() -> str:
    """An NPI of the earliest roster snapshot, which covers every 2024 admit date before July."""
    from pipeline.reference_data import load_warehouse_reference

    earliest = min(load_warehouse_reference(REAL_DATA_DIR / "reference").roster, key=lambda s: s.as_of_date)
    return sorted(entry.npi for entry in earliest.entries)[0]


def epic_file(rows, *, roster_npi: str) -> FileSpec:
    """An Epic file whose rows are valid apart from the given changes. Patient values are fake."""
    header = contract_header("EPIC_NORTH")
    records = []
    for record_id, changes in rows:
        values = {
            "source_system": "EPIC_NORTH", "source_record_id": record_id, "facility_name": "Lakeshore General Hospital",
            "patient_mrn": f"MRN-zz-{record_id or 'none'}", "patient_first_name": "Testfirst", "patient_last_name": "Testlast",
            "patient_dob": "01/02/1980", "patient_sex": "F", "patient_zip": "00000", "patient_phone": "000-0000",
            "admit_date": "03/05/2024", "discharge_date": "03/07/2024", "encounter_type": "IP",
            "attending_npi": roster_npi, "attending_provider_name": "x", "primary_dx_code": "E11.9",
            "chief_complaint": "x", "payer_name": "Medicare", "billed_amount": "$5,000.00", "claim_status": "Paid",
            "last_updated_ts": "2024-03-01T10:00:00Z",
        }  # fmt: skip
        values.update(changes)
        records.append([values.get(column, "") for column in header])
    return FileSpec("encounters_epic_north.csv", "EPIC_NORTH", csv_bytes(header, records), len(records))


# --- database state: pre-Task-4 layout and comparable digests ---

# ops.batch_audit exactly as the last pre-Task-4 commit (94a9a89) created it.
# raw.* and clean.encounter_patients are unchanged since then.
STAGE1_AUDIT_DDL = """
CREATE TABLE ops.batch_audit_stage1 (
    batch_id          VARCHAR   NOT NULL,
    file_name         VARCHAR   NOT NULL,
    source_system     VARCHAR,
    expected_count    INTEGER,
    received_count    INTEGER,
    accepted_count    INTEGER,
    duplicate_count   INTEGER,
    stale_count       INTEGER,
    quarantined_count INTEGER,
    status            VARCHAR   NOT NULL CHECK (status IN ('ACCEPTED', 'REJECTED')),
    reason            VARCHAR,
    start_time        TIMESTAMP NOT NULL,
    end_time          TIMESTAMP NOT NULL,
    PRIMARY KEY (batch_id, file_name)
)
"""

# The operational timestamps that legitimately differ between runs. Nothing else is excluded.
RUN_TIME_COLUMNS = {
    "raw.encounters": ("ingested_at",),
    "raw.ingested_files": ("ingested_at",),
    "ops.batch_audit": ("start_time", "end_time"),
}
STATE_TABLES = (
    "raw.encounters",
    "raw.ingested_files",
    "ops.batch_audit",
    "clean.encounter_versions",
    "clean.encounter_row_outcomes",
    "clean.encounter_current",
    "clean.encounter_patients",
    "clean.encounter_version_fields",
    "mart.dim_provider",
    "mart.dim_facility",
    "mart.dim_diagnosis",
    "mart.dim_payer",
    "mart.dim_date",
    "mart.dim_patient",
    "mart.fact_encounter_version",
    "mart.fact_encounter_current",
    "clean.version_dq_issues",
    "ops.quarantine",
    "ops.gate_rejected_issues",
    "ops.dq_report",
)


def attach(db_path: Path):
    """A plain DuckDB connection to a pipeline database file, attached the way raw_store does."""
    import duckdb

    con = duckdb.connect()
    con.execute(f"ATTACH '{db_path}' AS raw_store")
    con.execute("USE raw_store")
    return con


def make_pre_task4(db_path: Path) -> None:
    """Turn a pipeline database into what a pre-Task-4 build left behind.

    The history tables, the view, the cleaned version fields, the mart schema
    and the DQ tables (added after Task 4) are dropped, and ops.batch_audit goes back to the
    Stage 1 layout with Stage 1 values: accepted_count = rows landed for an
    accepted file, and duplicate, stale and quarantined counts NULL. Raw rows,
    file records, encounter_patients and audit timings are kept as they are.
    """
    con = attach(db_path)
    try:
        con.execute("DROP VIEW IF EXISTS clean.encounter_current")
        con.execute("DROP TABLE IF EXISTS clean.encounter_versions")
        con.execute("DROP TABLE IF EXISTS clean.encounter_row_outcomes")
        con.execute("DROP TABLE IF EXISTS clean.encounter_version_fields")
        con.execute("DROP TABLE IF EXISTS clean.version_dq_issues")
        con.execute("DROP TABLE IF EXISTS ops.dq_report")
        con.execute("DROP TABLE IF EXISTS ops.quarantine")
        con.execute("DROP TABLE IF EXISTS ops.gate_rejected_issues")
        con.execute("DROP SCHEMA IF EXISTS mart CASCADE")
        con.execute(STAGE1_AUDIT_DDL)
        con.execute(
            "INSERT INTO ops.batch_audit_stage1 "
            "SELECT batch_id, file_name, source_system, expected_count, received_count, "
            "       CASE WHEN status = 'ACCEPTED' THEN received_count ELSE 0 END, NULL, NULL, NULL, "
            "       status, reason, start_time, end_time "
            "FROM ops.batch_audit"
        )
        con.execute("DROP TABLE ops.batch_audit")
        con.execute("ALTER TABLE ops.batch_audit_stage1 RENAME TO batch_audit")
    finally:
        con.close()


def table_columns(con, table: str) -> list[str]:
    return [row[0] for row in con.execute(f"DESCRIBE {table}").fetchall()]


def state_digest(db_path: Path, *, exclude_run_times: bool = True) -> dict[str, tuple]:
    """Per table: (compared columns, excluded columns, row count, SHA-256 of the sorted rows).

    Rows are hashed, not returned, so a failing comparison never prints a raw
    value (raw.encounters holds PHI). Only RUN_TIME_COLUMNS can be excluded.
    """
    con = attach(db_path)
    try:
        digest = {}
        for table in STATE_TABLES:
            columns = table_columns(con, table)
            excluded = RUN_TIME_COLUMNS.get(table, ()) if exclude_run_times else ()
            kept = [c for c in columns if c not in excluded]
            rows = con.execute(f"SELECT {', '.join(kept)} FROM {table} ORDER BY ALL").fetchall()
            digest[table] = (
                tuple(kept),
                tuple(c for c in columns if c in excluded),
                len(rows),
                hashlib.sha256(repr(rows).encode("utf-8")).hexdigest(),
            )
        return digest
    finally:
        con.close()


# --- independent monthly totals (checks the mart's reporting queries) ---


def independent_monthly_totals(con, as_of_batch: str | None = None) -> dict[tuple[int, int], tuple[int, Decimal]]:
    """Encounters and billed total per admit month, excluding VOID, computed independently of the mart.

    It re-parses the raw admit date, amount and claim status of each current version with the Task 2
    parsers, so the mart queries (README queries 1 and 5) can be checked against it.

    as_of_batch=None reads clean.encounter_current. Otherwise the current
    version is chosen among the versions known by the end of that batch.
    """
    if as_of_batch is None:
        picked, params = (
            "SELECT source_batch_id AS batch_id, source_file_name AS file_name, source_row_number "
            "FROM clean.encounter_current"
        ), []
    else:
        picked, params = (
            "SELECT first_seen_batch_id AS batch_id, first_seen_file_name AS file_name, "
            "       first_seen_source_row_number AS source_row_number "
            "FROM clean.encounter_versions WHERE first_seen_batch_id <= ? "
            "QUALIFY row_number() OVER (PARTITION BY encounter_key ORDER BY last_updated_ts_utc DESC, "
            "first_seen_batch_id, first_seen_file_name, first_seen_source_row_number) = 1"
        ), [as_of_batch]
    rows = con.execute(
        f"WITH picked AS ({picked}) "
        "SELECT f.source_system, f.delivered_at, e.admit_date, e.billed_amount, e.claim_status "
        "FROM picked p JOIN raw.encounters e USING (batch_id, file_name, source_row_number) "
        "JOIN raw.ingested_files f USING (batch_id, file_name)",
        params,
    ).fetchall()
    conventions = load_source_conventions(REAL_DATA_DIR / "reference" / "source_systems_and_facilities.json")
    totals: dict[tuple[int, int], list] = defaultdict(lambda: [0, Decimal("0.00")])
    for system, delivered_at, admit_text, amount_text, status_text in rows:
        convention = conventions[system]
        admit = parse_date(
            admit_text,
            date_order=convention.date_order,
            delivered_at=datetime.fromisoformat(delivered_at),
            two_digit_year_century=convention.two_digit_year_century,
        ).cleaned_value
        if admit is None or parse_claim_status(status_text).cleaned_value == ClaimStatus.VOID:
            continue
        month = totals[(admit.year, admit.month)]
        month[0] += 1
        month[1] += parse_amount(amount_text, convention.amount_unit).cleaned_value or Decimal("0.00")
    return {month: (count, total) for month, (count, total) in totals.items()}
