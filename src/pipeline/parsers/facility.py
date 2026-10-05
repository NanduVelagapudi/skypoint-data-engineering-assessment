"""facility_name -> facility_id, resolved within the row's source system.

The lookup key is (source_system, normalised name), so the same name in two
systems can never resolve across them. Names are normalised the same way as
the categorical fields (case, runs of whitespace, spaces around hyphens) and
then matched exactly against the facility master names plus the explicit
aliases in config/facility_aliases.json. There is no fuzzy matching and no
partial or prefix matching:

    blank                       NULL, FACILITY_MISSING
    no exact match in system    NULL, FACILITY_UNRESOLVED (quarantined later; never guessed)

build_facility_index() refuses any configuration where one normalised name
would point at two facilities in the same system, so a resolution can never be
ambiguous. The reference data is passed in, so resolving does no file I/O.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from pipeline.errors import ConfigError
from pipeline.parsers.categorical import normalise_key
from pipeline.parsers.result import FieldReason, ParseResult


@dataclass(frozen=True)
class Facility:
    facility_id: str
    source_system: str
    facility_name: str
    facility_type: str


@dataclass(frozen=True)
class FacilityAlias:
    source_system: str
    facility_id: str
    facility_name: str  # the master name, as written next to the alias in config
    alias: str


@dataclass(frozen=True)
class FacilityIndex:
    facilities: Mapping[str, Facility]  # facility_id -> Facility
    by_name: Mapping[tuple[str, str], str]  # (source_system, normalised name) -> facility_id


def build_facility_index(facilities: Iterable[Facility], aliases: Iterable[FacilityAlias]) -> FacilityIndex:
    """Index master names and aliases; raise ConfigError on anything inconsistent or ambiguous."""
    by_id: dict[str, Facility] = {}
    by_name: dict[tuple[str, str], str] = {}

    def add(source_system: str, name: str, facility_id: str) -> None:
        key = (source_system, normalise_key(name))
        if not key[1]:
            raise ConfigError(f"empty facility name or alias for {facility_id}")
        if key in by_name:
            raise ConfigError(f"facility name {name!r} is listed more than once in {source_system}")
        by_name[key] = facility_id

    for facility in facilities:
        if facility.facility_id in by_id:
            raise ConfigError(f"duplicate facility_id {facility.facility_id}")
        by_id[facility.facility_id] = facility
        add(facility.source_system, facility.facility_name, facility.facility_id)

    for alias in aliases:
        facility = by_id.get(alias.facility_id)
        if facility is None:
            raise ConfigError(f"alias for unknown facility_id {alias.facility_id}")
        if facility.source_system != alias.source_system:
            raise ConfigError(f"alias for {alias.facility_id} is listed under the wrong source system")
        if facility.facility_name != alias.facility_name:
            raise ConfigError(f"facility_name for {alias.facility_id} does not match the facility master")
        add(alias.source_system, alias.alias, alias.facility_id)

    if not by_id:
        raise ConfigError("facility master is empty")
    return FacilityIndex(facilities=by_id, by_name=by_name)


def resolve_facility(raw: str | None, source_system: str, index: FacilityIndex) -> ParseResult[str]:
    """The facility_id for `raw` within `source_system`, or NULL with a reason."""
    text = (raw or "").strip()
    if not text:
        return ParseResult.invalid(raw, FieldReason.FACILITY_MISSING)
    facility_id = index.by_name.get((source_system, normalise_key(text)))
    if facility_id is None:
        return ParseResult.invalid(raw, FieldReason.FACILITY_UNRESOLVED)
    return ParseResult.valid(raw, facility_id)
