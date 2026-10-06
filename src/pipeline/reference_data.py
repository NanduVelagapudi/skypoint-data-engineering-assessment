"""Loaders for the reference files and curated config that parsers need as inputs.

Kept out of the parsers so they stay free of file I/O. Every loader validates
what it reads and raises ConfigError, naming the file and row number but never
a value.
"""

from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from pipeline.errors import ConfigError
from pipeline.parsers.facility import Facility, FacilityAlias, FacilityIndex, build_facility_index
from pipeline.parsers.icd10 import ICD10_CODE
from pipeline.parsers.npi import parse_npi

FACILITY_MASTER_FILE = "source_systems_and_facilities.json"
ICD10_REFERENCE_FILE = "icd10_reference.csv"
ROSTER_DIR = "provider_roster"
ROSTER_COLUMNS = (
    "as_of_date",
    "npi",
    "provider_last_name",
    "provider_first_name",
    "credential",
    "specialty",
    "primary_facility_id",
    "employment_status",
)
_ROSTER_FILE_NAME = re.compile(r"roster_(\d{4}-\d{2}-\d{2})\.csv")


@dataclass(frozen=True)
class Icd10Entry:
    """One row of reference/icd10_reference.csv."""

    icd10_code: str
    description: str
    category: str
    is_chronic: bool
    source_row_number: int  # 1-based data row


@dataclass(frozen=True)
class FacilityRecord:
    """One facility of the master, with every attribute it carries."""

    facility_id: str
    facility_name: str
    source_system: str
    facility_type: str
    city: str
    state: str
    bed_count: int
    ownership: str
    go_live_date: date
    source_position: int  # 1-based position in the master's facilities list


@dataclass(frozen=True)
class RosterEntry:
    """One provider row of one roster snapshot."""

    as_of_date: date
    npi: str
    provider_last_name: str
    provider_first_name: str
    credential: str
    specialty: str
    primary_facility_id: str
    employment_status: str
    file_name: str
    source_row_number: int  # 1-based data row


@dataclass(frozen=True)
class RosterSnapshot:
    as_of_date: date
    file_name: str
    entries: tuple[RosterEntry, ...]


@dataclass(frozen=True)
class CleaningReference:
    """What the Task 2 field cleaning needs besides the source conventions."""

    facility_index: FacilityIndex
    icd10_codes: frozenset[str]
    roster_npis: frozenset[str]  # every NPI in any roster snapshot


def _read_csv(path: Path, what: str) -> tuple[list[str], list[dict[str, str]]]:
    try:
        with path.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle, strict=True)
            rows = list(reader)
            return list(reader.fieldnames or []), rows
    except (OSError, UnicodeDecodeError, csv.Error) as exc:
        raise ConfigError(f"{what} {path.name} cannot be read") from exc


def load_icd10_reference(path: Path) -> tuple[Icd10Entry, ...]:
    """Every row of reference/icd10_reference.csv, checked to be in the normalised format."""
    header, rows = _read_csv(path, "ICD-10 reference")
    if not rows:
        raise ConfigError("ICD-10 reference is empty")
    if not {"icd10_code", "description", "category", "is_chronic"} <= set(header):
        raise ConfigError(f"ICD-10 reference {path.name} cannot be read")
    entries = []
    for number, row in enumerate(rows, start=1):
        if not ICD10_CODE.fullmatch(row["icd10_code"] or ""):
            raise ConfigError("ICD-10 reference has a code that is not in the normalised format")
        if row["is_chronic"] not in ("Y", "N"):
            raise ConfigError(f"ICD-10 reference row {number}: is_chronic must be Y or N")
        if not (row["description"] or "").strip() or not (row["category"] or "").strip():
            raise ConfigError(f"ICD-10 reference row {number}: description and category are required")
        entries.append(Icd10Entry(row["icd10_code"], row["description"], row["category"], row["is_chronic"] == "Y", number))
    if len({e.icd10_code for e in entries}) != len(entries):
        raise ConfigError("ICD-10 reference has duplicate codes")
    return tuple(entries)


def load_icd10_codes(path: Path) -> frozenset[str]:
    """The icd10_code column of reference/icd10_reference.csv, checked to be in the normalised format."""
    return frozenset(entry.icd10_code for entry in load_icd10_reference(path))


def _read_json(path: Path, what: str) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ConfigError(f"{what} {path.name} cannot be read as JSON") from exc


def load_facility_master(path: Path) -> tuple[Facility, ...]:
    """The facilities list of reference/source_systems_and_facilities.json."""
    raw = _read_json(path, "facility master")
    try:
        return tuple(
            Facility(f["facility_id"], f["source_system"], f["facility_name"], f["facility_type"])
            for f in raw["facilities"]
        )
    except (KeyError, TypeError) as exc:
        raise ConfigError("facility master: missing or malformed key") from exc


def load_facility_records(path: Path) -> tuple[FacilityRecord, ...]:
    """The facilities list of the master with all attributes, typed and checked."""
    raw = _read_json(path, "facility master")
    records = []
    try:
        for position, f in enumerate(raw["facilities"], start=1):
            bed_count = f["bed_count"]
            if not isinstance(bed_count, int) or isinstance(bed_count, bool) or bed_count < 0:
                raise ConfigError(f"facility master entry {position}: bed_count must be a non-negative integer")
            text = {k: f[k] for k in ("facility_id", "facility_name", "source_system", "facility_type", "city", "state", "ownership")}
            if not all(isinstance(v, str) and v.strip() for v in text.values()):
                raise ConfigError(f"facility master entry {position}: text attributes must be non-blank strings")
            records.append(FacilityRecord(**text, bed_count=bed_count,
                                          go_live_date=date.fromisoformat(f["go_live_date"]), source_position=position))
    except (KeyError, TypeError, ValueError) as exc:  # ValueError: go_live_date is not an ISO date
        raise ConfigError("facility master: missing or malformed key") from exc
    if not records:
        raise ConfigError("facility master is empty")
    if len({r.facility_id for r in records}) != len(records):
        raise ConfigError("facility master has duplicate facility_id values")
    return tuple(records)


def load_roster_snapshots(directory: Path) -> tuple[RosterSnapshot, ...]:
    """Every reference/provider_roster/roster_YYYY-MM-DD.csv, oldest first.

    Each file's as_of_date column must equal the date in its name, every field
    must be filled, and each NPI must be a valid NPI appearing once per snapshot.
    """
    try:
        paths = sorted(p for p in directory.iterdir() if p.is_file())
    except OSError as exc:
        raise ConfigError(f"provider roster folder {directory.name} cannot be read") from exc
    snapshots = []
    for path in paths:
        match = _ROSTER_FILE_NAME.fullmatch(path.name)
        if match is None:
            raise ConfigError(f"provider roster folder holds an unexpected file {path.name}")
        try:
            as_of = date.fromisoformat(match.group(1))
        except ValueError as exc:
            raise ConfigError(f"roster file {path.name} has an invalid date in its name") from exc
        header, rows = _read_csv(path, "roster file")
        if tuple(header) != ROSTER_COLUMNS:
            raise ConfigError(f"roster file {path.name} does not have the expected columns")
        entries = []
        for number, row in enumerate(rows, start=1):
            if any(not (row[c] or "").strip() for c in ROSTER_COLUMNS):
                raise ConfigError(f"roster file {path.name} row {number}: every field is required")
            if row["as_of_date"] != as_of.isoformat():
                raise ConfigError(f"roster file {path.name} row {number}: as_of_date differs from the file name")
            if parse_npi(row["npi"]).cleaned_value != row["npi"]:
                raise ConfigError(f"roster file {path.name} row {number}: npi is not a valid 10-digit NPI")
            entries.append(RosterEntry(as_of_date=as_of, **{c: row[c] for c in ROSTER_COLUMNS[1:]},
                                       file_name=path.name, source_row_number=number))
        if len({e.npi for e in entries}) != len(entries):
            raise ConfigError(f"roster file {path.name} lists an NPI more than once")
        if not entries:
            raise ConfigError(f"roster file {path.name} is empty")
        snapshots.append(RosterSnapshot(as_of, path.name, tuple(entries)))
    if not snapshots:
        raise ConfigError("provider roster folder has no snapshots")
    return tuple(sorted(snapshots, key=lambda s: s.as_of_date))


def load_cleaning_reference(reference_dir: Path, aliases_path: Path) -> CleaningReference:
    """The reference data the Task 2 field cleaning uses, loaded and checked once per run."""
    snapshots = load_roster_snapshots(reference_dir / ROSTER_DIR)
    return CleaningReference(
        facility_index=load_facility_index(reference_dir / FACILITY_MASTER_FILE, aliases_path),
        icd10_codes=load_icd10_codes(reference_dir / ICD10_REFERENCE_FILE),
        roster_npis=frozenset(entry.npi for snapshot in snapshots for entry in snapshot.entries),
    )


def load_facility_aliases(path: Path) -> tuple[FacilityAlias, ...]:
    """config/facility_aliases.json, flattened to one entry per alias."""
    raw = _read_json(path, "facility aliases")
    try:
        return tuple(
            FacilityAlias(source_system, facility_id, entry["facility_name"], alias)
            for source_system, facilities in raw["source_systems"].items()
            for facility_id, entry in facilities.items()
            for alias in entry["aliases"]
        )
    except (KeyError, TypeError, AttributeError) as exc:
        raise ConfigError("facility aliases: missing or malformed key") from exc


def load_facility_index(master_path: Path, aliases_path: Path) -> FacilityIndex:
    return build_facility_index(load_facility_master(master_path), load_facility_aliases(aliases_path))
