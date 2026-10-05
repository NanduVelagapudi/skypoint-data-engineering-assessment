"""Schema contracts: which delivered headers each source system may send.

A header is accepted only if it exactly matches, in names and order, one of the
header versions configured for its source system. A known change, such as the
Athena batch_003 layout, is a new version in config/schema_contracts.json, not
a code change. A header that matches no configured version is an unknown
schema change and rejects the batch.

After a match, fields are mapped to canonical columns by header name, never by
position. A canonical column that the matched version does not have is None.

Mismatch details name only configured columns, counts and column positions.
Delivered header values are never echoed: a file sent without a header would
otherwise put its first data row (PHI) into the audit and logs.
"""

from __future__ import annotations

import codecs
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pipeline.errors import ConfigError, ReasonCode, ValidationFailure

# Canonical columns become raw-table column names, so they must be safe identifiers.
COLUMN_NAME = re.compile(r"[a-z][a-z0-9_]*")


@dataclass(frozen=True)
class HeaderVersion:
    version: str
    columns: tuple[str, ...]  # delivered header, in delivered order
    renames: Mapping[str, str]  # delivered name -> canonical name

    def canonical_name(self, delivered: str) -> str:
        return self.renames.get(delivered, delivered)


@dataclass(frozen=True)
class SourceContract:
    source_system: str
    file_name: str
    encoding: str
    header_versions: tuple[HeaderVersion, ...]


@dataclass(frozen=True)
class SchemaContracts:
    canonical_columns: tuple[str, ...]
    source_systems: Mapping[str, SourceContract]


@dataclass(frozen=True)
class SchemaMatch:
    version: HeaderVersion
    canonical_columns: tuple[str, ...]
    positions: Mapping[str, int]  # canonical column -> position in the delivered header

    def to_canonical(self, record: Sequence[str]) -> dict[str, str | None]:
        """Record values keyed by canonical column; None where this version has no such column."""
        return {
            column: record[self.positions[column]] if column in self.positions else None
            for column in self.canonical_columns
        }


def load_contracts(path: Path) -> SchemaContracts:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ConfigError(f"schema contract file {path.name} cannot be read as JSON") from exc
    return parse_contracts(raw)


def parse_contracts(raw: Mapping[str, Any]) -> SchemaContracts:
    try:
        contracts = SchemaContracts(
            canonical_columns=tuple(raw["canonical_columns"]),
            source_systems={
                name: SourceContract(
                    source_system=name,
                    file_name=spec["file_name"],
                    encoding=spec["encoding"],
                    header_versions=tuple(
                        HeaderVersion(v["version"], tuple(v["columns"]), dict(v.get("renames", {})))
                        for v in spec["header_versions"]
                    ),
                )
                for name, spec in raw["source_systems"].items()
            },
        )
    except (KeyError, TypeError, AttributeError) as exc:
        raise ConfigError("schema contract: missing or malformed key") from exc
    _validate(contracts)
    return contracts


def _require(condition: object, message: str) -> None:
    if not condition:
        raise ConfigError(f"schema contract: {message}")


def _validate(contracts: SchemaContracts) -> None:
    canonical = contracts.canonical_columns
    _require(canonical, "canonical_columns is empty")
    _require(len(set(canonical)) == len(canonical), "canonical_columns has duplicates")
    for column in canonical:
        _require(isinstance(column, str) and COLUMN_NAME.fullmatch(column), f"invalid canonical column {column!r}")

    systems = contracts.source_systems.values()
    _require(systems, "no source systems configured")
    file_names = [c.file_name for c in systems]
    _require(len(set(file_names)) == len(file_names), "a file_name is used by more than one source system")
    version_ids = [v.version for c in systems for v in c.header_versions]
    _require(len(set(version_ids)) == len(version_ids), "header version ids are not unique")

    for contract in systems:
        try:
            codecs.lookup(contract.encoding)
        except LookupError:
            raise ConfigError(f"schema contract: unknown encoding for {contract.source_system}") from None
        versions = contract.header_versions
        _require(versions, f"{contract.source_system} has no header versions")
        _require(
            len({v.columns for v in versions}) == len(versions),
            f"{contract.source_system} has two identical header versions",
        )
        for v in versions:
            where = f"{contract.source_system}/{v.version}"
            _require(v.columns and len(set(v.columns)) == len(v.columns), f"{where}: columns empty or duplicated")
            _require(set(v.renames) <= set(v.columns), f"{where}: renames a column it does not have")
            mapped = [v.canonical_name(c) for c in v.columns]
            _require(len(set(mapped)) == len(mapped), f"{where}: two columns map to one canonical column")
            _require(set(mapped) <= set(canonical), f"{where}: maps to a column not in canonical_columns")


def match_header(contracts: SchemaContracts, source_system: str, header: Sequence[str]) -> SchemaMatch | None:
    """The configured version whose columns equal `header` exactly, or None."""
    for version in contracts.source_systems[source_system].header_versions:
        if tuple(header) == version.columns:
            positions = {version.canonical_name(name): i for i, name in enumerate(header)}
            return SchemaMatch(version, contracts.canonical_columns, positions)
    return None


def describe_mismatch(contracts: SchemaContracts, source_system: str, header: Sequence[str]) -> ValidationFailure:
    """PHI-safe description of how `header` differs from the closest configured version."""
    versions = contracts.source_systems[source_system].header_versions
    delivered = list(header)
    # Most shared column names wins; reversed() makes the latest version win ties.
    closest = max(reversed(versions), key=lambda v: len(set(v.columns) & set(delivered)))

    expected = set(closest.columns)
    seen: set[str] = set()
    unexpected_positions: list[int] = []
    duplicate_positions: list[int] = []
    for position, name in enumerate(delivered, start=1):
        if name in seen:
            duplicate_positions.append(position)
        elif name not in expected:
            unexpected_positions.append(position)
        seen.add(name)
    missing = [column for column in closest.columns if column not in seen]

    parts = [
        f"closest_version={closest.version}",
        f"columns={len(delivered)}",
        f"expected_columns={len(closest.columns)}",
    ]
    if missing:
        parts.append("missing=" + "|".join(missing))
    if unexpected_positions:
        parts.append("unexpected_positions=" + "|".join(map(str, unexpected_positions)))
    if duplicate_positions:
        parts.append("duplicate_positions=" + "|".join(map(str, duplicate_positions)))
    if not (missing or unexpected_positions or duplicate_positions):
        parts.append("order_differs")
    return ValidationFailure(ReasonCode.UNKNOWN_SCHEMA_CHANGE, ",".join(parts))
