"""Restricted raw layer in DuckDB.

raw.encounters holds every accepted source row exactly as delivered (all
VARCHAR) plus the lineage columns. It is the only place raw PHI may exist: the
database file lives outside output/ and is gitignored.

Rows are handed to DuckDB as one JSON document per file, each row an object
keyed by canonical column name, and expanded in SQL with from_json. Binding
values one at a time through the Python driver costs about 1 ms per value when
numpy/pandas are not installed (86 s for the 3,636-row data pack). The JSON
route loads the same rows in under 0.1 s and is lossless for text and NULL.
"""

from __future__ import annotations

import contextlib
import json
import logging
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb

from pipeline import batch_audit
from pipeline.errors import ConfigError, PipelineError
from pipeline.schema_contract import COLUMN_NAME

log = logging.getLogger(__name__)

LINEAGE_COLUMNS = ("batch_id", "file_name", "source_row_number", "file_sha256", "ingested_at")

# DuckDB names a database after its file, so a file called raw.duckdb would make
# "raw.encounters" ambiguous (catalog raw or schema raw). The file is attached
# under this fixed name instead, whatever RAW_DB_PATH is called.
CATALOG = "raw_store"


@dataclass(frozen=True)
class AcceptedFile:
    """One validated file, ready to load. Row i (0-based) is source_row_number i + 1."""

    batch_id: str
    file_name: str
    source_system: str
    schema_version: str
    file_sha256: str
    manifest_row_count: int
    delivered_at: str
    source_header: tuple[str, ...]  # header as delivered, before renames
    rows: tuple[Mapping[str, str | None], ...]  # canonical column -> value as delivered (None if absent)


_CREATE_ENCOUNTERS = """
CREATE TABLE IF NOT EXISTS raw.encounters (
    batch_id          VARCHAR   NOT NULL,
    file_name         VARCHAR   NOT NULL,
    source_row_number INTEGER   NOT NULL,
    file_sha256       VARCHAR   NOT NULL,
    ingested_at       TIMESTAMP NOT NULL,
    PRIMARY KEY (batch_id, file_name, source_row_number)
)
"""

_CREATE_INGESTED_FILES = """
CREATE TABLE IF NOT EXISTS raw.ingested_files (
    batch_id           VARCHAR   NOT NULL,
    file_name          VARCHAR   NOT NULL,
    source_system      VARCHAR   NOT NULL,
    schema_version     VARCHAR   NOT NULL,
    file_sha256        VARCHAR   NOT NULL,
    manifest_row_count INTEGER   NOT NULL,
    delivered_at       VARCHAR   NOT NULL,
    source_header      VARCHAR[] NOT NULL,
    ingested_at        TIMESTAMP NOT NULL,
    PRIMARY KEY (batch_id, file_name)
)
"""


def _ident(name: str) -> str:
    """Quoted SQL identifier for a configured column name."""
    if not COLUMN_NAME.fullmatch(name):
        raise ConfigError(f"unsafe column name {name!r}")
    return f'"{name}"'


def open_store(db_path: Path, canonical_columns: Sequence[str]) -> duckdb.DuckDBPyConnection:
    """Connect to the raw database, creating it and its tables if needed."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()  # in-memory session; the database file is attached below
    try:
        quoted_path = str(db_path).replace("'", "''")
        con.execute(f"ATTACH '{quoted_path}' AS {CATALOG}")
        con.execute(f"USE {CATALOG}")
        ensure_tables(con, canonical_columns)
    except BaseException:
        con.close()
        raise
    return con


def ensure_tables(con: duckdb.DuckDBPyConnection, canonical_columns: Sequence[str]) -> None:
    overlap = set(canonical_columns) & set(LINEAGE_COLUMNS)
    if overlap:
        raise ConfigError("canonical columns must not reuse lineage column names")

    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute(_CREATE_ENCOUNTERS)
    # Columns come from the schema contract, so a known new column needs only a config change.
    for column in canonical_columns:
        con.execute(f"ALTER TABLE raw.encounters ADD COLUMN IF NOT EXISTS {_ident(column)} VARCHAR")
    con.execute(_CREATE_INGESTED_FILES)
    batch_audit.ensure_audit_table(con)


@contextlib.contextmanager
def _transaction(con: duckdb.DuckDBPyConnection, batch_id: str) -> Iterator[None]:
    con.begin()
    try:
        yield
        con.commit()
    except BaseException as exc:
        with contextlib.suppress(duckdb.Error):  # a failed commit may already have rolled back
            con.rollback()
        log.error(
            "batch_write_rolled_back",
            extra={"step": "store", "batch_id": batch_id, "error_type": type(exc).__name__},
        )
        raise


def write_accepted_batch(
    con: duckdb.DuckDBPyConnection,
    batch_id: str,
    files: Sequence[AcceptedFile],
    audit_rows: Sequence[batch_audit.AuditRow],
    ingested_at: datetime,
) -> None:
    """Raw rows, file records and audit rows of one accepted batch, in one transaction."""
    if any(f.batch_id != batch_id for f in files) or any(r.batch_id != batch_id for r in audit_rows):
        raise ValueError("all files and audit rows must belong to the batch being written")
    stamp = batch_audit.db_timestamp(ingested_at)

    with _transaction(con, batch_id):
        for accepted in files:
            _insert_rows(con, accepted, stamp)
            con.execute(
                "INSERT INTO raw.ingested_files VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    accepted.batch_id,
                    accepted.file_name,
                    accepted.source_system,
                    accepted.schema_version,
                    accepted.file_sha256,
                    accepted.manifest_row_count,
                    accepted.delivered_at,
                    list(accepted.source_header),
                    stamp,
                ],
            )
        batch_audit.insert_audit_rows(con, audit_rows)


def write_rejected_batch(
    con: duckdb.DuckDBPyConnection, batch_id: str, audit_rows: Sequence[batch_audit.AuditRow]
) -> None:
    """A rejected batch writes its audit rows and nothing else."""
    if any(r.batch_id != batch_id for r in audit_rows):
        raise ValueError("all audit rows must belong to the batch being written")
    with _transaction(con, batch_id):
        batch_audit.insert_audit_rows(con, audit_rows)


def _insert_rows(con: duckdb.DuckDBPyConnection, accepted: AcceptedFile, ingested_at: datetime) -> None:
    if not accepted.rows:
        return
    columns = list(accepted.rows[0])
    if any(list(row) != columns for row in accepted.rows):
        raise ValueError("every row of a file must have the same canonical columns")

    schema = json.dumps([{"source_row_number": "INTEGER", **{c: "VARCHAR" for c in columns}}])
    document = json.dumps(
        [{"source_row_number": number, **row} for number, row in enumerate(accepted.rows, start=1)],
        ensure_ascii=False,
    )
    targets = ", ".join(_ident(c) for c in columns)
    values = ", ".join(f"r.{_ident(c)}" for c in columns)
    con.execute(
        f"INSERT INTO raw.encounters ({', '.join(LINEAGE_COLUMNS)}, {targets}) "
        f"SELECT ?, ?, r.source_row_number, ?, ?, {values} "
        f"FROM (SELECT unnest(from_json(?, ?)) AS r)",
        [accepted.batch_id, accepted.file_name, accepted.file_sha256, ingested_at, document, schema],
    )

    loaded = con.execute(
        "SELECT count(*) FROM raw.encounters WHERE batch_id = ? AND file_name = ?",
        [accepted.batch_id, accepted.file_name],
    ).fetchone()[0]
    if loaded != len(accepted.rows):
        raise PipelineError("raw row count after insert does not match the validated file")
