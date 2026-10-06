"""ops.batch_audit: one row per (batch, file), and its CSV export.

The audit holds counts, statuses and reason codes only, never row values. A
batch-level failure that cannot be tied to one file uses file_name
'manifest.json'.

For an accepted file, the Task 4 counts come from the file's encounter history
outcomes and are reconciled before the row is written:
    received_count       = new_encounter + new_version + duplicate + stale + quarantined
    history_rows_written = new_encounter + new_version + stale_new_version
accepted_count is new_encounter + new_version. For a rejected batch the Task 4
columns are NULL: nothing was classified.

Timestamps are UTC stored as TIMESTAMP. DuckDB's TIMESTAMPTZ needs pytz to be
read back into Python, and pytz is not a dependency.
"""

from __future__ import annotations

import csv
import logging
import os
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

import duckdb

from pipeline.errors import PipelineError

log = logging.getLogger(__name__)

AUDIT_COLUMNS = (
    "batch_id",
    "file_name",
    "source_system",
    "expected_count",
    "received_count",
    "accepted_count",
    "new_encounter_count",
    "new_version_count",
    "duplicate_count",
    "stale_count",
    "stale_new_version_count",
    "quarantined_count",
    "history_rows_written",
    "current_changed_count",
    "reconciliation_status",
    "status",
    "reason",
    "start_time",
    "end_time",
)

# A file whose counts do not reconcile fails its batch's transaction, so this is
# the only value ever stored; rejected batches leave the column NULL.
RECONCILED = "RECONCILED"


class BatchStatus(StrEnum):
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"


@dataclass(frozen=True)
class AuditRow:
    batch_id: str
    file_name: str
    source_system: str | None
    expected_count: int | None
    received_count: int | None
    accepted_count: int | None
    status: BatchStatus
    reason: str | None
    start_time: datetime  # timezone-aware
    end_time: datetime  # timezone-aware
    # Task 4 history counts: set for an accepted file, NULL (not evaluated) for a rejected one.
    new_encounter_count: int | None = None
    new_version_count: int | None = None
    duplicate_count: int | None = None
    stale_count: int | None = None
    stale_new_version_count: int | None = None
    quarantined_count: int | None = None
    history_rows_written: int | None = None
    current_changed_count: int | None = None
    reconciliation_status: str | None = None


_CREATE_TABLE = f"""
CREATE TABLE IF NOT EXISTS ops.batch_audit (
    batch_id                VARCHAR   NOT NULL,
    file_name               VARCHAR   NOT NULL,
    source_system           VARCHAR,
    expected_count          INTEGER,
    received_count          INTEGER,
    accepted_count          INTEGER,
    new_encounter_count     INTEGER,
    new_version_count       INTEGER,
    duplicate_count         INTEGER,
    stale_count             INTEGER,
    stale_new_version_count INTEGER,
    quarantined_count       INTEGER,
    history_rows_written    INTEGER,
    current_changed_count   INTEGER,
    reconciliation_status   VARCHAR   CHECK (reconciliation_status IN ('{RECONCILED}')),
    status                  VARCHAR   NOT NULL CHECK (status IN ('ACCEPTED', 'REJECTED')),
    reason                  VARCHAR,
    start_time              TIMESTAMP NOT NULL,
    end_time                TIMESTAMP NOT NULL,
    PRIMARY KEY (batch_id, file_name)
)
"""


def ensure_audit_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS ops")
    con.execute(_CREATE_TABLE)
    present = {
        row[0]
        for row in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog = current_database() AND table_schema = 'ops' AND table_name = 'batch_audit'"
        ).fetchall()
    }
    if not present.issuperset(AUDIT_COLUMNS):
        # A database from before Task 4 has no history for its batches; patching
        # the columns in would hide that, so refuse it instead.
        raise PipelineError("ops.batch_audit predates the Task 4 columns; rebuild the database from the landing data")


def db_timestamp(value: datetime) -> datetime:
    """UTC wall-clock value for a TIMESTAMP column.

    DuckDB drops a datetime's offset instead of converting it, so convert here
    and refuse naive values rather than guess their zone.
    """
    if value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(UTC).replace(tzinfo=None)


def processed_batch_ids(con: duckdb.DuckDBPyConnection) -> set[str]:
    """Batches with any audit row (ACCEPTED or REJECTED); every other batch is pending."""
    return {row[0] for row in con.execute("SELECT DISTINCT batch_id FROM ops.batch_audit").fetchall()}


def insert_audit_rows(con: duckdb.DuckDBPyConnection, rows: Sequence[AuditRow]) -> None:
    """Insert audit rows inside the caller's transaction."""
    sql = (
        f"INSERT INTO ops.batch_audit ({', '.join(AUDIT_COLUMNS)}) "
        f"VALUES ({', '.join('?' * len(AUDIT_COLUMNS))})"
    )
    for row in rows:
        values = [getattr(row, column) for column in AUDIT_COLUMNS]
        values[AUDIT_COLUMNS.index("status")] = str(row.status)
        values[AUDIT_COLUMNS.index("start_time")] = db_timestamp(row.start_time)
        values[AUDIT_COLUMNS.index("end_time")] = db_timestamp(row.end_time)
        con.execute(sql, values)


def _csv_value(value: object) -> object:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat(timespec="milliseconds") + "Z"  # stored as UTC
    return value


def export_csv(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    """Write the whole audit to `path`, sorted by batch_id, file_name, via a temp file and rename."""
    rows = con.execute(
        f"SELECT {', '.join(AUDIT_COLUMNS)} FROM ops.batch_audit ORDER BY batch_id, file_name"
    ).fetchall()

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    try:
        with tmp.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(AUDIT_COLUMNS)
            writer.writerows([_csv_value(value) for value in row] for row in rows)
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)

    log.info("batch_audit_exported", extra={"step": "export", "file_name": path.name})
