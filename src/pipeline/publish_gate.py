"""Task 6 publish gate, and ops.gate_rejected_issues.

A batch is published only if its share of ERROR rows is not MORE than the
threshold (DQ_GATE_MAX_ERROR_SHARE, 5% by default):

    error rows      received rows that fail an error-level row or version check
                    (dq_rules.GATE_CHECKS): the Task 4 row quarantine
                    (no source_record_id, unplaceable last_updated_ts, same-
                    timestamp conflict), and every row whose version has an
                    ERROR (facility unresolved, admit date unusable), including
                    duplicate and stale copies of that version
    received rows   every raw row of the batch
    fails           error_rows > threshold x received_rows, in exact Decimal
                    arithmetic; a batch with no rows passes
WARNINGs never count.

The gate runs inside the batch's own transaction, after the Task 4
reconciliation and before the audit rows are written, on the batch's row
outcomes and the cleaned fields of their versions. The version ERRORs come from
the same classification that builds clean.version_dq_issues (version_dq), so
the two cannot disagree. A failure raises PublishGateFailed: the whole batch
transaction rolls back (raw rows, history, cleaned fields), and the batch
processor records the batch as REJECTED with DQ_GATE_FAILED and stores the
failing rows here, in one small transaction.

ops.gate_rejected_issues keeps, for a batch the gate rejected, one row per
(file, row, check) that failed: lineage, source_record_id and codes only, no
values. It is the only record of those rows, because the batch's derived
state was rolled back, so it is insert-only and never rebuilt.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal

import duckdb

from pipeline import dq_rules
from pipeline.dq_rules import CHECKS_BY_CODE, Severity
from pipeline.encounter_history import OUTCOMES_TABLE, Outcome
from pipeline.errors import PipelineError
from pipeline.version_dq import derive_issues
from pipeline.version_fields import TABLE as FIELDS_TABLE

log = logging.getLogger(__name__)

TABLE = "ops.gate_rejected_issues"

COLUMNS = (
    "batch_id",
    "file_name",
    "source_row_number",
    "source_system",
    "source_record_id",
    "check_code",
    "reason_code",
)

_CREATE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    batch_id          VARCHAR NOT NULL,
    file_name         VARCHAR NOT NULL,
    source_row_number INTEGER NOT NULL,
    source_system     VARCHAR NOT NULL,
    source_record_id  VARCHAR,
    check_code        VARCHAR NOT NULL,
    reason_code       VARCHAR NOT NULL,
    PRIMARY KEY (batch_id, file_name, source_row_number, check_code)
)
"""


@dataclass(frozen=True)
class GateIssue:
    file_name: str
    source_row_number: int
    source_system: str
    source_record_id: str | None
    check_code: str
    reason_code: str


@dataclass(frozen=True)
class GateResult:
    batch_id: str
    received_rows: int
    error_rows: int
    max_error_share: Decimal
    issues: tuple[GateIssue, ...]  # every (row, check) that failed, in lineage order

    @property
    def failed(self) -> bool:
        return exceeds(self.error_rows, self.received_rows, self.max_error_share)


def exceeds(error_rows: int, received_rows: int, max_error_share: Decimal) -> bool:
    """True when MORE than max_error_share of the rows are error rows. Exact; never a float."""
    return received_rows > 0 and Decimal(error_rows) > max_error_share * Decimal(received_rows)


def threshold_pct(max_error_share: Decimal) -> Decimal:
    return (max_error_share * 100).quantize(Decimal("0.01"))


def ensure_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS ops")
    con.execute(_CREATE_TABLE)


def row_issues(outcome: str, outcome_reason: str | None, field_codes: Mapping[str, str | None]) -> list[tuple[str, str]]:
    """(check_code, reason_code) of every error-level check one row fails; pure.

    A quarantined row has no version: its Task 4 reason is the issue. Any other
    row is judged by its version's cleaned fields, through the same
    classification as clean.version_dq_issues.
    """
    if outcome == Outcome.QUARANTINED:
        _, check_code = dq_rules.classify(dq_rules.SRC_OUTCOME, outcome_reason)
        if CHECKS_BY_CODE[check_code].severity != Severity.ERROR:
            raise PipelineError(f"Task 4 quarantine reason {outcome_reason} is not an error-level check")
        return [(check_code, outcome_reason)]
    return [(i.check_code, i.reason_code) for i in derive_issues(field_codes) if i.severity == Severity.ERROR]


def evaluate(con: duckdb.DuckDBPyConnection, batch_id: str, max_error_share: Decimal) -> GateResult:
    """The gate for one batch already classified into the history, inside the caller's transaction."""
    columns = [source.split(".", 1)[1] for source in dq_rules.GATE_VERSION_SOURCES]
    records = con.execute(
        f"""
        SELECT o.file_name, o.source_row_number, o.source_system, o.source_record_id, o.outcome, o.outcome_reason,
               {", ".join(f"f.{c}" for c in columns)}
        FROM {OUTCOMES_TABLE} AS o
        LEFT JOIN {FIELDS_TABLE} AS f USING (version_key)
        WHERE o.batch_id = ?
        ORDER BY o.file_name, o.source_row_number
        """,
        [batch_id],
    ).fetchall()

    issues, error_rows = [], 0
    for file_name, row_number, system, record_id, outcome, reason, *codes in records:
        found = row_issues(outcome, reason, dict(zip(dq_rules.GATE_VERSION_SOURCES, codes, strict=True)))
        error_rows += bool(found)
        issues.extend(GateIssue(file_name, row_number, system, record_id, check, code) for check, code in found)
    result = GateResult(batch_id, len(records), error_rows, max_error_share, tuple(issues))
    log.log(
        logging.WARNING if result.failed else logging.INFO,
        "publish_gate_evaluated",
        extra={
            "step": "dq",
            "batch_id": batch_id,
            "received_count": result.received_rows,
            "error_count": result.error_rows,
            "status": "FAILED" if result.failed else "PASSED",
        },
    )
    return result


def insert_rejected_issues(con: duckdb.DuckDBPyConnection, result: GateResult) -> None:
    """Store a rejected batch's failing rows, inside the caller's transaction."""
    if not result.issues:
        return
    rows = [{"batch_id": result.batch_id, **{c: getattr(i, c) for c in COLUMNS[1:]}} for i in result.issues]
    schema = json.dumps([{c: ("INTEGER" if c == "source_row_number" else "VARCHAR") for c in COLUMNS}])
    con.execute(
        f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) "
        f"SELECT {', '.join(f'r.{c}' for c in COLUMNS)} FROM (SELECT unnest(from_json(?, ?)) AS r)",
        [json.dumps(rows, ensure_ascii=False), schema],
    )
