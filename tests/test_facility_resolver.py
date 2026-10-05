"""Facility resolution tests. Facility names are organisation names from the reference data, not PHI."""

import pytest
from conftest import REAL_DATA_DIR, REPO_ROOT

from pipeline.errors import ConfigError
from pipeline.parsers.facility import Facility, FacilityAlias, build_facility_index, resolve_facility
from pipeline.parsers.result import FieldReason
from pipeline.reference_data import load_facility_aliases, load_facility_index, load_facility_master

MASTER_PATH = REAL_DATA_DIR / "reference" / "source_systems_and_facilities.json"
ALIASES_PATH = REPO_ROOT / "config" / "facility_aliases.json"
INDEX = load_facility_index(MASTER_PATH, ALIASES_PATH)

EPIC, MEDITECH, ATHENA = "EPIC_NORTH", "LEGACY_MEDITECH", "ATHENA_CLINICS"

CANONICAL = {
    (EPIC, "Lakeshore General Hospital"): "FAC001",
    (EPIC, "St. Brendan Medical Center"): "FAC002",
    (EPIC, "Maple Grove Pediatrics"): "FAC003",
    (MEDITECH, "Riverbend Community Hospital"): "FAC004",
    (MEDITECH, "Harbor Point Behavioral Health"): "FAC005",
    (ATHENA, "Cedar Valley Family Clinic"): "FAC006",
    (ATHENA, "Eastgate Urgent Care"): "FAC007",
    (ATHENA, "Summit Ridge Orthopedics"): "FAC008",
}

# Every alias in config/facility_aliases.json, written out so a config change shows up here.
ALIASES = {
    (EPIC, "Lakeshore Gen Hosp"): "FAC001",
    (EPIC, "Lakeshore General"): "FAC001",
    (EPIC, "St Brendan Medical Center"): "FAC002",
    (EPIC, "ST BRENDAN MED CTR"): "FAC002",
    (EPIC, "Saint Brendan Medical Center"): "FAC002",
    (EPIC, "St. Brendan's Medical Center"): "FAC002",
    (EPIC, "Maple Grove Pediatric Clinic"): "FAC003",
    (EPIC, "MAPLE GROVE PEDS"): "FAC003",
    (MEDITECH, "Riverbend CH"): "FAC004",
    (MEDITECH, "Riverbend Community Hosp."): "FAC004",
    (MEDITECH, "RIVERBEND COMM HOSP"): "FAC004",
    (MEDITECH, "HARBOR PT BEHAVIORAL HLTH"): "FAC005",
    (MEDITECH, "Harborpoint Behavioral Health"): "FAC005",
    (MEDITECH, "Harbor Point BH"): "FAC005",
    (ATHENA, "Cedar Vly Family Clinic"): "FAC006",
    (ATHENA, "Cedar Valley Clinic"): "FAC006",
    (ATHENA, "EASTGATE UC"): "FAC007",
    (ATHENA, "Eastgate Urgent Care Center"): "FAC007",
    (ATHENA, "East Gate Urgent Care"): "FAC007",
    (ATHENA, "Summit Ridge Ortho"): "FAC008",
    (ATHENA, "SUMMIT RIDGE ORTHOPAEDICS"): "FAC008",
    (ATHENA, "Summit Ridge Orthopedic Clinic"): "FAC008",
}


# --- reference data ---


def test_real_master_has_eight_facilities_each_in_one_system():
    facilities = load_facility_master(MASTER_PATH)

    assert [f.facility_id for f in facilities] == [f"FAC00{i}" for i in range(1, 9)]
    assert {(f.source_system, f.facility_name): f.facility_id for f in facilities} == CANONICAL


def test_alias_config_is_exactly_the_documented_list():
    configured = {(a.source_system, a.alias): a.facility_id for a in load_facility_aliases(ALIASES_PATH)}

    assert configured == ALIASES


# --- resolution ---


@pytest.mark.parametrize("key, facility_id", CANONICAL.items())
def test_master_names_resolve(key, facility_id):
    source_system, name = key
    result = resolve_facility(name, source_system, INDEX)

    assert (result.cleaned_value, result.reason_code) == (facility_id, None)
    assert result.raw_value == name


@pytest.mark.parametrize("key, facility_id", ALIASES.items())
def test_every_alias_resolves_within_its_system(key, facility_id):
    source_system, alias = key

    assert resolve_facility(alias, source_system, INDEX).cleaned_value == facility_id


@pytest.mark.parametrize(
    "raw, source_system, facility_id",
    [
        ("LAKESHORE GENERAL HOSPITAL", EPIC, "FAC001"),
        ("lakeshore general hospital ", EPIC, "FAC001"),
        ("  Lakeshore   General\tHospital  ", EPIC, "FAC001"),
        ("Maple Grove Pediatrics ", EPIC, "FAC003"),
        ("CEDAR VALLEY FAMILY CLINIC", ATHENA, "FAC006"),
        ("st brendan med ctr", EPIC, "FAC002"),
        ("harbor point bh", MEDITECH, "FAC005"),
    ],
)
def test_case_and_whitespace_are_normalised(raw, source_system, facility_id):
    result = resolve_facility(raw, source_system, INDEX)

    assert result.cleaned_value == facility_id
    assert result.raw_value == raw  # raw kept exactly as delivered


@pytest.mark.parametrize("key", list(CANONICAL) + list(ALIASES))
def test_names_never_resolve_in_another_source_system(key):
    source_system, name = key
    for other in {EPIC, MEDITECH, ATHENA} - {source_system}:
        result = resolve_facility(name, other, INDEX)
        assert result.cleaned_value is None
        assert result.reason_code == FieldReason.FACILITY_UNRESOLVED


@pytest.mark.parametrize("raw", [None, "", "   "])
def test_blank_facility_is_missing(raw):
    result = resolve_facility(raw, EPIC, INDEX)

    assert result.cleaned_value is None
    assert result.reason_code == FieldReason.FACILITY_MISSING


@pytest.mark.parametrize(
    "raw, source_system",
    [
        ("Westfield Surgical Center", ATHENA),  # accepted data, not in the facility master
        ("TEST FACILITY - DO NOT USE", EPIC),  # accepted data, a test record
        ("Saint Brendan Medic", EPIC),  # truncated value from rejected batch_004; no prefix matching
        ("Lakeshore", EPIC),
        ("Lakeshore General Hospital East", EPIC),
        ("St. Brendan Medical Ctr", EPIC),  # close to an alias, but not listed
        ("Riverbend", MEDITECH),
        ("N/A", ATHENA),
        ("Lakeshore General Hospital", "UNKNOWN_SYSTEM"),
    ],
)
def test_unlisted_names_are_unresolved_never_guessed(raw, source_system):
    result = resolve_facility(raw, source_system, INDEX)

    assert result.cleaned_value is None
    assert result.reason_code == FieldReason.FACILITY_UNRESOLVED
    assert result.raw_value == raw


def test_resolution_is_deterministic_and_independent_of_alias_order():
    reversed_index = build_facility_index(
        reversed(load_facility_master(MASTER_PATH)), reversed(load_facility_aliases(ALIASES_PATH))
    )

    assert reversed_index.by_name == INDEX.by_name
    for (source_system, name), facility_id in {**CANONICAL, **ALIASES}.items():
        results = {resolve_facility(name, source_system, INDEX).cleaned_value for _ in range(3)}
        assert results == {facility_id}


# --- index validation: ambiguity and inconsistency are configuration errors ---

A = Facility("F1", "SYS_A", "Alpha Hospital", "Hospital")
B = Facility("F2", "SYS_A", "Beta Clinic", "Clinic")
C = Facility("F3", "SYS_B", "Alpha Hospital", "Hospital")


def test_same_name_in_two_systems_resolves_separately():
    index = build_facility_index([A, B, C], [])

    assert resolve_facility("Alpha Hospital", "SYS_A", index).cleaned_value == "F1"
    assert resolve_facility("Alpha Hospital", "SYS_B", index).cleaned_value == "F3"


@pytest.mark.parametrize(
    "facilities, aliases, message",
    [
        ([A, B], [FacilityAlias("SYS_A", "F2", "Beta Clinic", "ALPHA hospital")], "more than once"),
        ([A, B], [FacilityAlias("SYS_A", "F1", "Alpha Hospital", "Alpha Hosp"),
                  FacilityAlias("SYS_A", "F2", "Beta Clinic", "alpha  hosp")], "more than once"),
        ([A, A], [], "duplicate facility_id"),
        ([A], [FacilityAlias("SYS_A", "F9", "Alpha Hospital", "Alpha Hosp")], "unknown facility_id"),
        ([A, C], [FacilityAlias("SYS_B", "F1", "Alpha Hospital", "Alpha Hosp")], "wrong source system"),
        ([A], [FacilityAlias("SYS_A", "F1", "Alpha Hosp", "Alpha H")], "does not match"),
        ([A], [FacilityAlias("SYS_A", "F1", "Alpha Hospital", "   ")], "empty"),
        ([], [], "empty"),
    ],
)
def test_inconsistent_or_ambiguous_config_is_refused(facilities, aliases, message):
    with pytest.raises(ConfigError, match=message):
        build_facility_index(facilities, aliases)


def test_unreadable_or_malformed_files_are_config_errors(tmp_path):
    bad_json = tmp_path / "bad.json"
    bad_json.write_text("{not json", encoding="utf-8")
    no_facilities = tmp_path / "master.json"
    no_facilities.write_text('{"source_systems": []}', encoding="utf-8")
    no_aliases_key = tmp_path / "aliases.json"
    no_aliases_key.write_text('{"source_systems": {"SYS_A": {"F1": {"facility_name": "Alpha Hospital"}}}}', encoding="utf-8")

    for loader, path in [
        (load_facility_master, tmp_path / "absent.json"),
        (load_facility_master, bad_json),
        (load_facility_master, no_facilities),
        (load_facility_aliases, bad_json),
        (load_facility_aliases, no_aliases_key),
    ]:
        with pytest.raises(ConfigError):
            loader(path)
