"""Process landing batches in order, each one all-or-nothing.

For every pending batch, all validation runs in memory first: the manifest, then
each file (SHA-256 over the raw bytes, decoding and parsing, schema contract,
record shape, row count). Only then is the batch written, in one transaction:

* every file passed: raw rows, file records, the batch's encounter history
  (Task 4 row outcomes and new versions) and ACCEPTED audit rows carrying the
  reconciled Task 4 counts;
* any file failed: only REJECTED audit rows. Failed files carry their own
  reason codes; the files that passed carry SIBLING_FILE_REJECTED.

The history step classifies only the batch's own rows against the history of
the encounters they touch; nothing else is recomputed. If a file's counts do
not reconcile, the transaction rolls back and the run fails.

A rejection is a data outcome, not an error: processing continues with the next
batch. Redeliveries are flagged, never re-loaded: a file whose bytes were
already ingested in an earlier batch is still accepted (its rows classify as
duplicates or stale) with DUPLICATE_FILE in its audit reason, and an already
processed batch folder whose files have changed is skipped as before, with a
warning in the log. Logs carry allow-listed keys only and never row values.

rebuild_derived replays the same per-batch history step over raw, in batch_id
order; it is also how a database from before Task 4 is migrated.
"""

from __future__ import annotations

import contextlib
import logging
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from pipeline import batch_audit, encounter_history, raw_store
from pipeline.batch_audit import RECONCILED, AuditRow, BatchStatus, processed_batch_ids
from pipeline.csv_reader import parse_csv
from pipeline.errors import BatchRejected, PipelineError, ReasonCode, ValidationFailure, format_reasons
from pipeline.manifest import (
    MANIFEST_FILE_NAME,
    Manifest,
    ManifestEntry,
    check_row_count,
    check_sha256,
    load_manifest,
    sha256_hex,
)
from pipeline.raw_store import AcceptedFile
from pipeline.schema_contract import SchemaContracts, describe_mismatch, match_header
from pipeline.source_conventions import SourceConventions

log = logging.getLogger(__name__)

BATCH_DIR_NAME = re.compile(r"batch_\d{3}")

# Audit reason / log reason codes for redeliveries. They flag, not reject, so
# they are not batch-rejection ReasonCodes.
DUPLICATE_FILE = "DUPLICATE_FILE"
BATCH_REDELIVERED_CHANGED = "BATCH_REDELIVERED_CHANGED"
RECONCILIATION_FAILED = "RECONCILIATION_FAILED"


@dataclass(frozen=True)
class BatchResult:
    batch_id: str
    status: BatchStatus
    accepted_rows: int  # rows that created a new encounter or version; 0 when rejected
    received_rows: int = 0  # raw rows written; 0 when rejected


@dataclass(frozen=True)
class FileCheck:
    entry: ManifestEntry
    received_count: int | None  # None if the file could not be parsed to the end
    failures: list[ValidationFailure]
    accepted: AcceptedFile | None  # set only when the file passed every check


def _now() -> datetime:
    return datetime.now(UTC)


def _codes(failures: Sequence[ValidationFailure]) -> str:
    return "|".join(dict.fromkeys(str(f.code) for f in failures))


def list_batch_dirs(landing_dir: Path) -> list[Path]:
    """Batch folders sorted by name; never relies on filesystem iteration order."""
    entries = list(landing_dir.iterdir())
    batch_dirs = sorted((p for p in entries if p.is_dir() and BATCH_DIR_NAME.fullmatch(p.name)), key=lambda p: p.name)
    if len(batch_dirs) != len(entries):
        log.warning("landing_entries_ignored", extra={"step": "discover"})
    return batch_dirs


def run_pending_batches(
    con: duckdb.DuckDBPyConnection,
    landing_dir: Path,
    contracts: SchemaContracts,
    conventions: Mapping[str, SourceConventions],
) -> list[BatchResult]:
    """Process every batch that has no batch_audit row yet, in batch_id order."""
    encounter_history.ensure_tables(con)
    processed = processed_batch_ids(con)
    results = []
    for batch_dir in list_batch_dirs(landing_dir):
        if batch_dir.name in processed:
            log.info("batch_already_processed", extra={"step": "discover", "batch_id": batch_dir.name})
            _warn_if_redelivered_with_changes(con, batch_dir)
            continue
        results.append(process_batch(con, batch_dir, contracts, conventions))
    return results


def _warn_if_redelivered_with_changes(con: duckdb.DuckDBPyConnection, batch_dir: Path) -> None:
    """Log (never re-load) an accepted batch folder whose files differ from what was ingested.

    A rejected batch stored no file hashes, so there is nothing to compare.
    """
    stored = dict(
        con.execute(
            "SELECT file_name, file_sha256 FROM raw.ingested_files WHERE batch_id = ?", [batch_dir.name]
        ).fetchall()
    )
    if not stored:
        return
    delivered = {p.name for p in batch_dir.glob("*.csv") if p.is_file()}
    changed = delivered != set(stored) or any(
        sha256_hex((batch_dir / name).read_bytes()) != sha for name, sha in stored.items()
    )
    if changed:
        log.warning(
            "batch_redelivered_with_changes",
            extra={"step": "discover", "batch_id": batch_dir.name, "reason_code": BATCH_REDELIVERED_CHANGED},
        )


def process_batch(
    con: duckdb.DuckDBPyConnection,
    batch_dir: Path,
    contracts: SchemaContracts,
    conventions: Mapping[str, SourceConventions],
) -> BatchResult:
    batch_id = batch_dir.name
    start = _now()
    log.info("batch_started", extra={"step": "validate", "batch_id": batch_id})

    try:
        manifest = load_manifest(batch_dir, contracts)
    except BatchRejected as rejection:
        end = _now()
        row = AuditRow(
            batch_id=batch_id,
            file_name=MANIFEST_FILE_NAME,
            source_system=None,
            expected_count=None,
            received_count=None,
            accepted_count=0,
            status=BatchStatus.REJECTED,
            reason=format_reasons(rejection.failures),
            start_time=start,
            end_time=end,
        )
        raw_store.write_rejected_batch(con, batch_id, [row])
        _log_outcome(batch_id, BatchStatus.REJECTED, 0, start, end, reason_code=_codes(rejection.failures))
        return BatchResult(batch_id, BatchStatus.REJECTED, 0)

    checks = [_check_file(batch_dir, manifest, entry, contracts) for entry in manifest.entries]
    end = _now()

    if any(check.failures for check in checks):
        rows = [_audit_row(batch_id, check, BatchStatus.REJECTED, start, end) for check in checks]
        raw_store.write_rejected_batch(con, batch_id, rows)
        failed = [f for check in checks for f in check.failures]
        _log_outcome(batch_id, BatchStatus.REJECTED, 0, start, end, reason_code=_codes(failed))
        return BatchResult(batch_id, BatchStatus.REJECTED, 0)

    files = [check.accepted for check in checks]
    duplicates = _duplicate_files(con, files)
    rows = [
        replace(_audit_row(batch_id, check, BatchStatus.ACCEPTED, start, end), reason=duplicates.get(check.entry.file_name))
        for check in checks
    ]
    final_rows: list[AuditRow] = []

    def add_history(con: duckdb.DuckDBPyConnection) -> list[AuditRow]:
        """Runs inside the batch transaction, after the raw rows are written."""
        final_rows[:] = _classify_into_history(con, batch_id, rows, conventions)
        return final_rows

    raw_store.write_accepted_batch(con, batch_id, files, rows, ingested_at=end, before_audit=add_history)
    accepted_rows = sum(row.accepted_count for row in final_rows)
    received_rows = sum(len(f.rows) for f in files)
    _log_outcome(batch_id, BatchStatus.ACCEPTED, accepted_rows, start, end, rows=final_rows)
    return BatchResult(batch_id, BatchStatus.ACCEPTED, accepted_rows, received_rows)


def _classify_into_history(
    con: duckdb.DuckDBPyConnection,
    batch_id: str,
    rows: Sequence[AuditRow],
    conventions: Mapping[str, SourceConventions],
) -> list[AuditRow]:
    """Classify an accepted batch already in raw into the history; return its reconciled audit rows.

    The one Task 4 step for a batch, inside the caller's transaction: used by
    the incremental load and, batch by batch, by rebuild_derived.
    """
    encounter_history.apply_batch(con, batch_id, conventions)
    counts = encounter_history.file_counts(con, batch_id)
    return [_reconciled(row, counts) for row in rows]


def rebuild_derived(con: duckdb.DuckDBPyConnection, conventions: Mapping[str, SourceConventions]) -> list[str]:
    """Rebuild the Task 4 history and audit counts from raw; returns the batch ids replayed.

    In one transaction: migrate a pre-Task-4 ops.batch_audit if needed, empty
    the history tables, then replay every accepted batch in batch_id order
    through _classify_into_history, the same step an incremental load runs,
    and overwrite the batch's accepted_count and Task 4 audit columns. Raw
    rows, file records and every other audit value (status, reason, timings)
    are left as they are. If anything fails, everything rolls back, so a
    pre-Task-4 database stays pre-Task-4 and is still refused by normal runs.
    """
    log.info("derived_rebuild_started", extra={"step": "rebuild"})
    con.begin()
    try:
        if not batch_audit.has_task4_columns(con):
            batch_audit.migrate_audit_table(con)
            log.warning("batch_audit_migrated", extra={"step": "rebuild"})
        encounter_history.ensure_tables(con)
        encounter_history.clear_history(con)
        batch_ids = [r[0] for r in con.execute("SELECT DISTINCT batch_id FROM raw.ingested_files ORDER BY batch_id").fetchall()]
        for batch_id in batch_ids:
            rows = batch_audit.read_audit_rows(con, batch_id)
            ingested = {r[0] for r in con.execute("SELECT file_name FROM raw.ingested_files WHERE batch_id = ?", [batch_id]).fetchall()}
            if {r.file_name for r in rows} != ingested or any(r.status != BatchStatus.ACCEPTED for r in rows):
                raise PipelineError(f"audit rows of {batch_id} do not match its ingested files")
            batch_audit.update_task4_counts(con, _classify_into_history(con, batch_id, rows, conventions))
        con.commit()
    except BaseException as exc:
        with contextlib.suppress(duckdb.Error):  # a failed commit may already have rolled back
            con.rollback()
        log.error("derived_rebuild_rolled_back", extra={"step": "rebuild", "error_type": type(exc).__name__})
        raise
    log.info("derived_rebuild_finished", extra={"step": "rebuild", "status": "COMPLETED"})
    return batch_ids


def _duplicate_files(con: duckdb.DuckDBPyConnection, files: Sequence[AcceptedFile]) -> dict[str, str]:
    """Audit reason for each file whose exact bytes were already ingested in an earlier batch."""
    reasons = {}
    for accepted in files:
        first_batch = con.execute(
            "SELECT min(batch_id) FROM raw.ingested_files WHERE file_sha256 = ? AND batch_id <> ?",
            [accepted.file_sha256, accepted.batch_id],
        ).fetchone()[0]
        if first_batch is not None:
            reasons[accepted.file_name] = f"{DUPLICATE_FILE}(first_batch={first_batch})"
            log.warning(
                "duplicate_file_delivered",
                extra={
                    "step": "validate",
                    "batch_id": accepted.batch_id,
                    "file_name": accepted.file_name,
                    "source_system": accepted.source_system,
                    "reason_code": DUPLICATE_FILE,
                },
            )
    return reasons


def _reconciled(row: AuditRow, counts: Mapping[str, encounter_history.FileCounts]) -> AuditRow:
    """The accepted audit row with its Task 4 counts, or PipelineError if they do not reconcile.

    Two separate checks: every received raw row has exactly one outcome, and
    every history row written is accounted for by an outcome that creates one.
    """
    c = counts.get(row.file_name, encounter_history.FileCounts())
    rows_reconcile = row.received_count == c.outcome_rows == (
        c.new_encounter + c.new_version + c.duplicate + c.stale + c.quarantined
    )
    history_reconciles = c.history_rows_written == c.new_encounter + c.new_version + c.stale_new_version
    if not (rows_reconcile and history_reconciles):
        log.error(
            "reconciliation_failed",
            extra={
                "step": "history",
                "batch_id": row.batch_id,
                "file_name": row.file_name,
                "received_count": row.received_count,
                "accepted_count": c.accepted,
                "duplicate_count": c.duplicate,
                "stale_count": c.stale,
                "quarantined_count": c.quarantined,
                "reason_code": RECONCILIATION_FAILED,
            },
        )
        raise PipelineError(f"Task 4 counts do not reconcile for {row.batch_id}/{row.file_name}")
    return replace(
        row,
        accepted_count=c.accepted,
        new_encounter_count=c.new_encounter,
        new_version_count=c.new_version,
        duplicate_count=c.duplicate,
        stale_count=c.stale,
        stale_new_version_count=c.stale_new_version,
        quarantined_count=c.quarantined,
        history_rows_written=c.history_rows_written,
        current_changed_count=c.current_changed,
        reconciliation_status=RECONCILED,
    )


def _check_file(
    batch_dir: Path, manifest: Manifest, entry: ManifestEntry, contracts: SchemaContracts
) -> FileCheck:
    contract = contracts.source_systems[entry.source_system]
    content = (batch_dir / entry.file_name).read_bytes()  # read once; hash and parse the same bytes
    file_sha256 = sha256_hex(content)

    failures: list[ValidationFailure] = []
    sha_failure = check_sha256(entry, file_sha256)
    if sha_failure:
        failures.append(sha_failure)

    parsed = parse_csv(content, contract.encoding)
    failures.extend(parsed.failures)

    match = None
    if parsed.header:
        match = match_header(contracts, entry.source_system, parsed.header)
        if match is None:
            failures.append(describe_mismatch(contracts, entry.source_system, parsed.header))

    if parsed.record_count is not None:
        count_failure = check_row_count(entry, parsed.record_count)
        if count_failure:
            failures.append(count_failure)

    context = {
        "step": "validate",
        "batch_id": manifest.batch_id,
        "file_name": entry.file_name,
        "source_system": entry.source_system,
        "expected_count": entry.row_count,
        "received_count": parsed.record_count,
    }
    if failures or match is None:
        log.warning("file_failed_validation", extra={**context, "reason_code": _codes(failures)})
        return FileCheck(entry, parsed.record_count, failures, None)

    log.info("file_validated", extra={**context, "schema_version": match.version.version})
    accepted = AcceptedFile(
        batch_id=manifest.batch_id,
        file_name=entry.file_name,
        source_system=entry.source_system,
        schema_version=match.version.version,
        file_sha256=file_sha256,
        manifest_row_count=entry.row_count,
        delivered_at=manifest.delivered_at,
        source_header=tuple(parsed.header),
        rows=tuple(match.to_canonical(record) for record in parsed.records),
    )
    return FileCheck(entry, parsed.record_count, [], accepted)


def _audit_row(
    batch_id: str, check: FileCheck, status: BatchStatus, start: datetime, end: datetime
) -> AuditRow:
    if status == BatchStatus.ACCEPTED:
        reason, accepted_count = None, check.received_count
    elif check.failures:
        reason, accepted_count = format_reasons(check.failures), 0
    else:
        reason, accepted_count = str(ReasonCode.SIBLING_FILE_REJECTED), 0
    return AuditRow(
        batch_id=batch_id,
        file_name=check.entry.file_name,
        source_system=check.entry.source_system,
        expected_count=check.entry.row_count,
        received_count=check.received_count,
        accepted_count=accepted_count,
        status=status,
        reason=reason,
        start_time=start,
        end_time=end,
    )


def _log_outcome(
    batch_id: str,
    status: BatchStatus,
    accepted_rows: int,
    start: datetime,
    end: datetime,
    reason_code: str | None = None,
    rows: Sequence[AuditRow] = (),
) -> None:
    extra = {
        "step": "store",
        "batch_id": batch_id,
        "status": str(status),
        "accepted_count": accepted_rows,
        "duration_ms": round((end - start).total_seconds() * 1000),
    }
    if rows:  # accepted: the batch totals of the reconciled audit rows
        for key in ("received_count", "duplicate_count", "stale_count", "quarantined_count"):
            extra[key] = sum(getattr(row, key) for row in rows)
    if reason_code:
        extra["reason_code"] = reason_code
    level = logging.INFO if status == BatchStatus.ACCEPTED else logging.WARNING
    log.log(level, f"batch_{status.lower()}", extra=extra)
