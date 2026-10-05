"""Process landing batches in order, each one all-or-nothing.

For every pending batch, all validation runs in memory first: the manifest, then
each file (SHA-256 over the raw bytes, decoding and parsing, schema contract,
record shape, row count). Only then is the batch written, in one transaction:

* every file passed: raw rows, file records and ACCEPTED audit rows;
* any file failed: only REJECTED audit rows. Failed files carry their own
  reason codes; the files that passed carry SIBLING_FILE_REJECTED.

A rejection is a data outcome, not an error: processing continues with the next
batch. Logs carry allow-listed keys only and never row values.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from pipeline import raw_store
from pipeline.batch_audit import AuditRow, BatchStatus, processed_batch_ids
from pipeline.csv_reader import parse_csv
from pipeline.errors import BatchRejected, ReasonCode, ValidationFailure, format_reasons
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

log = logging.getLogger(__name__)

BATCH_DIR_NAME = re.compile(r"batch_\d{3}")


@dataclass(frozen=True)
class BatchResult:
    batch_id: str
    status: BatchStatus
    accepted_rows: int  # raw rows written; 0 when rejected


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
    con: duckdb.DuckDBPyConnection, landing_dir: Path, contracts: SchemaContracts
) -> list[BatchResult]:
    """Process every batch that has no batch_audit row yet, in batch_id order."""
    processed = processed_batch_ids(con)
    results = []
    for batch_dir in list_batch_dirs(landing_dir):
        if batch_dir.name in processed:
            log.info("batch_already_processed", extra={"step": "discover", "batch_id": batch_dir.name})
            continue
        results.append(process_batch(con, batch_dir, contracts))
    return results


def process_batch(con: duckdb.DuckDBPyConnection, batch_dir: Path, contracts: SchemaContracts) -> BatchResult:
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
    rows = [_audit_row(batch_id, check, BatchStatus.ACCEPTED, start, end) for check in checks]
    raw_store.write_accepted_batch(con, batch_id, files, rows, ingested_at=end)
    accepted_rows = sum(len(f.rows) for f in files)
    _log_outcome(batch_id, BatchStatus.ACCEPTED, accepted_rows, start, end)
    return BatchResult(batch_id, BatchStatus.ACCEPTED, accepted_rows)


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
) -> None:
    extra = {
        "step": "store",
        "batch_id": batch_id,
        "status": str(status),
        "accepted_count": accepted_rows,
        "duration_ms": round((end - start).total_seconds() * 1000),
    }
    if reason_code:
        extra["reason_code"] = reason_code
    level = logging.INFO if status == BatchStatus.ACCEPTED else logging.WARNING
    log.log(level, f"batch_{status.lower()}", extra=extra)
