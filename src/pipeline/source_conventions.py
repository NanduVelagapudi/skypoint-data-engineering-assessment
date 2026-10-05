"""Per-source-system conventions, read from the data pack's reference metadata.

data/reference/source_systems_and_facilities.json is the only documentation of
how each system writes dates, amounts and timestamps, so it is the single
source for these conventions; nothing here is hard-coded per system.

The two-digit-year rule has no structured field: the reference states it only
in free-text notes ("two-digit years mean 20xx", Athena). It is read with one
strict pattern. A system whose notes state no rule gets no century, and the
date parser then rejects two-digit years instead of guessing.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pipeline.errors import ConfigError

_TWO_DIGIT_YEAR_RULE = re.compile(r"two-digit years mean (\d{2})xx", re.IGNORECASE)


class DateOrder(StrEnum):
    MDY = "MDY"
    DMY = "DMY"


class AmountUnit(StrEnum):
    USD = "USD"
    USD_CENTS = "USD_CENTS"


@dataclass(frozen=True)
class SourceConventions:
    source_system: str
    date_order: DateOrder
    amount_unit: AmountUnit
    timestamp_timezone: str  # IANA name, e.g. "UTC" or "America/Chicago"
    two_digit_year_century: int | None  # e.g. 2000, or None when the reference states no rule


def load_source_conventions(path: Path) -> dict[str, SourceConventions]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ConfigError(f"reference file {path.name} cannot be read as JSON") from exc
    return parse_source_conventions(raw)


def parse_source_conventions(raw: Mapping[str, Any]) -> dict[str, SourceConventions]:
    conventions: dict[str, SourceConventions] = {}
    try:
        for entry in raw["source_systems"]:
            name = entry["source_system"]
            century = _TWO_DIGIT_YEAR_RULE.search(entry.get("notes", ""))
            conventions[name] = SourceConventions(
                source_system=name,
                date_order=DateOrder(entry["date_order"]),
                amount_unit=AmountUnit(entry["amount_unit"]),
                timestamp_timezone=entry["timestamp_timezone"],
                two_digit_year_century=int(century.group(1)) * 100 if century else None,
            )
            ZoneInfo(entry["timestamp_timezone"])
    except (KeyError, TypeError, ValueError, ZoneInfoNotFoundError) as exc:
        raise ConfigError("reference source_systems: missing, malformed or unsupported value") from exc
    if not conventions:
        raise ConfigError("reference source_systems is empty")
    return conventions
