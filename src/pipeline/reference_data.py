"""Loaders for the reference files and curated config that parsers need as inputs.

Kept out of the parsers so they stay free of file I/O.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

from pipeline.errors import ConfigError
from pipeline.parsers.facility import Facility, FacilityAlias, FacilityIndex, build_facility_index
from pipeline.parsers.icd10 import ICD10_CODE


def load_icd10_codes(path: Path) -> frozenset[str]:
    """The icd10_code column of reference/icd10_reference.csv, checked to be in the normalised format."""
    try:
        with path.open(encoding="utf-8-sig", newline="") as handle:
            codes = [row["icd10_code"] for row in csv.DictReader(handle)]
    except (OSError, UnicodeDecodeError, csv.Error, KeyError) as exc:
        raise ConfigError(f"ICD-10 reference {path.name} cannot be read") from exc
    if not codes:
        raise ConfigError("ICD-10 reference is empty")
    if not all(ICD10_CODE.fullmatch(code) for code in codes):
        raise ConfigError("ICD-10 reference has a code that is not in the normalised format")
    if len(set(codes)) != len(codes):
        raise ConfigError("ICD-10 reference has duplicate codes")
    return frozenset(codes)


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
