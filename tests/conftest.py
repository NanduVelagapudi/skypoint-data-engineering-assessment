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
from dataclasses import dataclass
from pathlib import Path

import pytest

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
    }


@pytest.fixture
def restore_pipeline_logger():
    """Undo configure_logging() so tests don't leak handlers into each other."""
    logger = logging.getLogger("pipeline")
    handlers, level = logger.handlers[:], logger.level
    yield
    logger.handlers[:] = handlers
    logger.setLevel(level)


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

    The history tables and view are dropped, and ops.batch_audit goes back to the
    Stage 1 layout with Stage 1 values: accepted_count = rows landed for an
    accepted file, and duplicate, stale and quarantined counts NULL. Raw rows,
    file records, encounter_patients and audit timings are kept as they are.
    """
    con = attach(db_path)
    try:
        con.execute("DROP VIEW IF EXISTS clean.encounter_current")
        con.execute("DROP TABLE IF EXISTS clean.encounter_versions")
        con.execute("DROP TABLE IF EXISTS clean.encounter_row_outcomes")
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
