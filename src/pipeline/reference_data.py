"""Loaders for reference files in the data pack that parsers need as inputs.

Kept out of the parsers so they stay free of file I/O.
"""

from __future__ import annotations

import csv
from pathlib import Path

from pipeline.errors import ConfigError
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
