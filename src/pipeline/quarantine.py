"""Task 6 quarantine: ops.quarantine, one table of every rejected or unresolved row.

Rebuilt on every run, inside the mart rebuild transaction, from what earlier
steps store:

    level / source             what it is                              lineage
    FILE / BATCH_VALIDATION    a file of a batch rejected by Task 1    batch, file; row_count = rows received
    FILE / PUBLISH_GATE        a file of a batch the gate rejected     batch, file; row_count = rows received
    ROW / PUBLISH_GATE         a row of a gate-rejected batch that     batch, file, row
                               failed an error-level check
                               (ops.gate_rejected_issues)
    ROW / HISTORY_ORDERING     a Task 4 row that could not be placed   batch, file, row
                               in version order (outcome QUARANTINED)
    VERSION / VERSION_DQ       a version with an ERROR in              its first arrival, plus version_key
                               clean.version_dq_issues

A rejected file stands for all its rows; its rows are not listed one by one,
because a rejected file's records are not trusted (batch_004's Epic file is
truncated). A version ERROR is listed once, at the version's first arrival;
later duplicate or stale copies are already accounted for as duplicates and
stale rows in the audit.

The Task 4 and Task 6 kinds stay apart: a HISTORY_ORDERING row is counted in
batch_audit.quarantined_count and has no version; a VERSION_DQ entry is a
version that stays in the history, and in the current state when it is the
latest (is_current_version), but is excluded from analytics.

No values: only lineage, keys, source_record_id, codes and, for a FILE entry,
the audit reason text, which Task 1 keeps PHI-free. reason_codes are sorted,
distinct and joined with '|'.
"""

from __future__ import annotations

import logging
from collections import defaultdict

import duckdb

from pipeline import dq_rules
from pipeline.batch_audit import BatchStatus
from pipeline.dimensions import insert_rows
from pipeline.encounter_history import CURRENT_VIEW, OUTCOMES_TABLE, Outcome
from pipeline.errors import PipelineError, ReasonCode
from pipeline.publish_gate import TABLE as GATE_TABLE
from pipeline.version_dq import TABLE as ISSUES_TABLE

log = logging.getLogger(__name__)

TABLE = "ops.quarantine"
ORDER_BY = "batch_id, file_name, source_row_number NULLS FIRST, quarantine_level"

COLUMNS = (
    "batch_id",
    "file_name",
    "source_row_number",
    "source_system",
    "quarantine_level",
    "quarantine_source",
    "batch_status",
    "source_record_id",
    "encounter_key",
    "version_key",
    "is_current_version",
    "reason_codes",
    "reason_detail",
    "row_count",
)

FILE, ROW, VERSION = "FILE", "ROW", "VERSION"
BATCH_VALIDATION, PUBLISH_GATE, HISTORY_ORDERING, VERSION_DQ = (
    "BATCH_VALIDATION", "PUBLISH_GATE", "HISTORY_ORDERING", "VERSION_DQ",
)  # fmt: skip

_DDL = f"""
    batch_id VARCHAR NOT NULL,
    file_name VARCHAR NOT NULL,
    source_row_number INTEGER,
    source_system VARCHAR,
    quarantine_level VARCHAR NOT NULL CHECK (quarantine_level IN ('{FILE}', '{ROW}', '{VERSION}')),
    quarantine_source VARCHAR NOT NULL
        CHECK (quarantine_source IN ('{BATCH_VALIDATION}', '{PUBLISH_GATE}', '{HISTORY_ORDERING}', '{VERSION_DQ}')),
    batch_status VARCHAR NOT NULL CHECK (batch_status IN ('{BatchStatus.ACCEPTED}', '{BatchStatus.REJECTED}')),
    source_record_id VARCHAR,
    encounter_key VARCHAR,
    version_key VARCHAR,
    is_current_version BOOLEAN,
    reason_codes VARCHAR NOT NULL,
    reason_detail VARCHAR,
    row_count INTEGER,
    CHECK ((quarantine_level = '{FILE}') = (source_row_number IS NULL)),
    CHECK ((quarantine_level = '{VERSION}') = (version_key IS NOT NULL)),
    CHECK ((quarantine_level = '{VERSION}') = (is_current_version IS NOT NULL))"""


def _entry(batch_id, file_name, row_number, system, level, source, status, *, record_id=None, encounter_key=None,
           version_key=None, is_current=None, codes=(), detail=None, row_count=1) -> dict[str, object]:  # fmt: skip
    reason_codes = dq_rules.join_codes(codes)
    if reason_codes is None:
        raise PipelineError("a quarantine entry needs a reason code")
    return {
        "batch_id": batch_id, "file_name": file_name, "source_row_number": row_number, "source_system": system,
        "quarantine_level": level, "quarantine_source": source, "batch_status": str(status),
        "source_record_id": record_id, "encounter_key": encounter_key, "version_key": version_key,
        "is_current_version": is_current, "reason_codes": reason_codes, "reason_detail": detail,
        "row_count": row_count,
    }  # fmt: skip


def _file_entries(con: duckdb.DuckDBPyConnection) -> list[dict[str, object]]:
    entries = []
    for batch_id, file_name, system, received, reason in con.execute(
        f"SELECT batch_id, file_name, source_system, received_count, reason FROM ops.batch_audit "
        f"WHERE status = '{BatchStatus.REJECTED}'"
    ).fetchall():
        codes = dq_rules.audit_reason_codes(reason)
        source = PUBLISH_GATE if ReasonCode.DQ_GATE_FAILED in codes else BATCH_VALIDATION
        entries.append(_entry(batch_id, file_name, None, system, FILE, source, BatchStatus.REJECTED,
                              codes=codes, detail=reason, row_count=received))
    return entries


def _gate_row_entries(con: duckdb.DuckDBPyConnection) -> list[dict[str, object]]:
    by_row: dict[tuple, list[str]] = defaultdict(list)
    for batch_id, file_name, row_number, system, record_id, reason in con.execute(
        f"SELECT batch_id, file_name, source_row_number, source_system, source_record_id, reason_code FROM {GATE_TABLE}"
    ).fetchall():
        by_row[(batch_id, file_name, row_number, system, record_id)].append(reason)
    return [
        _entry(batch_id, file_name, row_number, system, ROW, PUBLISH_GATE, BatchStatus.REJECTED,
               record_id=record_id, codes=codes)
        for (batch_id, file_name, row_number, system, record_id), codes in by_row.items()
    ]  # fmt: skip


def _history_entries(con: duckdb.DuckDBPyConnection) -> list[dict[str, object]]:
    return [
        _entry(batch_id, file_name, row_number, system, ROW, HISTORY_ORDERING, BatchStatus.ACCEPTED,
               record_id=record_id, encounter_key=encounter_key, codes=[reason])
        for batch_id, file_name, row_number, system, record_id, encounter_key, reason in con.execute(
            f"SELECT batch_id, file_name, source_row_number, source_system, source_record_id, encounter_key, "
            f"outcome_reason FROM {OUTCOMES_TABLE} WHERE outcome = '{Outcome.QUARANTINED}'"
        ).fetchall()
    ]  # fmt: skip


def _version_entries(con: duckdb.DuckDBPyConnection) -> list[dict[str, object]]:
    return [
        _entry(batch_id, file_name, row_number, system, VERSION, VERSION_DQ, BatchStatus.ACCEPTED,
               record_id=record_id, encounter_key=encounter_key, version_key=version_key, is_current=is_current,
               codes=codes)
        for batch_id, file_name, row_number, system, record_id, encounter_key, version_key, is_current, codes
        in con.execute(f"""
            SELECT i.source_batch_id, i.source_file_name, i.source_row_number, i.source_system, i.source_record_id,
                   i.encounter_key, i.version_key, c.version_key IS NOT NULL, list(i.reason_code ORDER BY i.reason_code)
            FROM {ISSUES_TABLE} AS i LEFT JOIN {CURRENT_VIEW} AS c USING (version_key)
            WHERE i.severity = '{dq_rules.Severity.ERROR}'
            GROUP BY ALL""").fetchall()
    ]  # fmt: skip


def quarantine_rows(con: duckdb.DuckDBPyConnection) -> list[dict[str, object]]:
    """Every quarantine entry, in ORDER_BY order."""
    rows = [*_file_entries(con), *_gate_row_entries(con), *_history_entries(con), *_version_entries(con)]
    rows.sort(key=lambda r: (r["batch_id"], r["file_name"], r["source_row_number"] is not None,
                             r["source_row_number"] or 0, r["quarantine_level"]))  # fmt: skip
    keys = [(r["batch_id"], r["file_name"], r["source_row_number"], r["quarantine_level"]) for r in rows]
    if len(set(keys)) != len(keys):
        raise PipelineError("duplicate quarantine entry")
    return rows


def write_quarantine(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Drop and recreate ops.quarantine inside the caller's transaction; returns its row count.

    Needs clean.version_dq_issues, so it runs after version_dq.write_version_issues.
    """
    rows = quarantine_rows(con)
    con.execute("CREATE SCHEMA IF NOT EXISTS ops")
    con.execute(f"CREATE OR REPLACE TABLE {TABLE} ({_DDL})")
    insert_rows(con, TABLE, rows)
    log.info("quarantine_built", extra={"step": "dq"})
    return {TABLE: len(rows)}
