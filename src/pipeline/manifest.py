"""Batch manifest: parse and check it, then check each file against its entry.

Manifest-level problems are collected and raised together as BatchRejected.
Failure details refer to manifest entries by their 1-based position and to
fields by manifest key name. Delivered values (batch ids, file names, hashes)
are never echoed.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pipeline.errors import BatchRejected, ReasonCode, ValidationFailure
from pipeline.schema_contract import SchemaContracts

MANIFEST_FILE_NAME = "manifest.json"

_SHA256_HEX = re.compile(r"[0-9a-fA-F]{64}")


@dataclass(frozen=True)
class ManifestEntry:
    file_name: str
    source_system: str
    row_count: int
    sha256: str  # lower-case hex


@dataclass(frozen=True)
class Manifest:
    batch_id: str
    delivered_at: str  # kept as delivered text; parsing it belongs to a later stage
    entries: tuple[ManifestEntry, ...]  # sorted by file_name


def load_manifest(batch_dir: Path, contracts: SchemaContracts) -> Manifest:
    """Read and check `batch_dir/manifest.json` against the CSV files in the folder."""
    path = batch_dir / MANIFEST_FILE_NAME
    if not path.is_file():
        raise BatchRejected([ValidationFailure(ReasonCode.MANIFEST_MISSING)])
    csv_files = [p.name for p in batch_dir.iterdir() if p.is_file() and p.suffix.lower() == ".csv"]
    return parse_manifest(path.read_bytes(), batch_dir.name, csv_files, contracts)


def parse_manifest(
    content: bytes,
    folder_batch_id: str,
    csv_files_present: Iterable[str],
    contracts: SchemaContracts,
) -> Manifest:
    """Check manifest bytes; raise BatchRejected listing every problem found."""
    try:
        raw = json.loads(content.decode("utf-8-sig"))
    except ValueError:  # covers UnicodeDecodeError and JSONDecodeError
        raise BatchRejected([ValidationFailure(ReasonCode.MANIFEST_INVALID, "not UTF-8 JSON")]) from None
    if not isinstance(raw, dict) or not isinstance(raw.get("files"), list) or not raw["files"]:
        raise BatchRejected([ValidationFailure(ReasonCode.MANIFEST_INVALID, "files must be a non-empty list")])

    failures: list[ValidationFailure] = []

    batch_id = raw.get("batch_id")
    if not isinstance(batch_id, str) or not batch_id:
        failures.append(ValidationFailure(ReasonCode.MANIFEST_INVALID, "field=batch_id"))
    elif batch_id != folder_batch_id:
        failures.append(ValidationFailure(ReasonCode.BATCH_ID_MISMATCH, "manifest batch_id differs from folder name"))

    delivered_at = raw.get("delivered_at")
    if not isinstance(delivered_at, str) or not delivered_at.strip():
        failures.append(ValidationFailure(ReasonCode.MANIFEST_INVALID, "field=delivered_at"))

    entries: dict[str, ManifestEntry] = {}
    positions: dict[str, int] = {}
    for position, item in enumerate(raw["files"], start=1):
        entry, entry_failures = _parse_entry(position, item, contracts)
        failures.extend(entry_failures)
        if entry is None:
            continue
        if entry.file_name in entries:
            failures.append(ValidationFailure(ReasonCode.MANIFEST_INVALID, f"entry={position},duplicate=file_name"))
            continue
        entries[entry.file_name] = entry
        positions[entry.file_name] = position

    present = set(csv_files_present)
    for file_name in sorted(entries, key=positions.__getitem__):
        if file_name not in present:
            failures.append(ValidationFailure(ReasonCode.FILE_MISSING, f"entry={positions[file_name]}"))
    listed = {
        item["file_name"]
        for item in raw["files"]
        if isinstance(item, dict) and isinstance(item.get("file_name"), str)
    }
    unlisted = present - listed
    if unlisted:
        failures.append(ValidationFailure(ReasonCode.UNEXPECTED_FILE, f"count={len(unlisted)}"))

    if failures:
        raise BatchRejected(failures)
    return Manifest(batch_id, delivered_at, tuple(entries[name] for name in sorted(entries)))


def _parse_entry(
    position: int, item: Any, contracts: SchemaContracts
) -> tuple[ManifestEntry | None, list[ValidationFailure]]:
    def invalid(field: str) -> ValidationFailure:
        return ValidationFailure(ReasonCode.MANIFEST_INVALID, f"entry={position},field={field}")

    if not isinstance(item, dict):
        return None, [ValidationFailure(ReasonCode.MANIFEST_INVALID, f"entry={position}")]

    file_name = item.get("file_name")
    source_system = item.get("source_system")
    row_count = item.get("row_count")
    sha256 = item.get("sha256")

    failures = []
    if not isinstance(file_name, str) or not file_name:
        failures.append(invalid("file_name"))
    if not isinstance(source_system, str) or not source_system:
        failures.append(invalid("source_system"))
    if type(row_count) is not int or row_count < 0:  # rejects bool, float and strings
        failures.append(invalid("row_count"))
    if not isinstance(sha256, str) or not _SHA256_HEX.fullmatch(sha256):
        failures.append(invalid("sha256"))
    if failures:
        return None, failures

    contract = contracts.source_systems.get(source_system)
    if contract is None:
        return None, [ValidationFailure(ReasonCode.UNKNOWN_SOURCE_SYSTEM, f"entry={position}")]
    # Only the contract's file name is accepted, so a manifest cannot point outside the batch folder.
    if file_name != contract.file_name:
        return None, [invalid("file_name")]

    return ManifestEntry(file_name, source_system, row_count, sha256.lower()), []


def sha256_hex(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def check_sha256(entry: ManifestEntry, actual_sha256: str) -> ValidationFailure | None:
    if actual_sha256 != entry.sha256:
        return ValidationFailure(ReasonCode.SHA256_MISMATCH)
    return None


def check_row_count(entry: ManifestEntry, record_count: int) -> ValidationFailure | None:
    if record_count != entry.row_count:
        return ValidationFailure(
            ReasonCode.ROW_COUNT_MISMATCH, f"expected={entry.row_count},received={record_count}"
        )
    return None
