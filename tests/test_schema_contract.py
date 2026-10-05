"""Schema contract tests. Config-error cases use in-memory dicts, so nothing is written."""

import copy
import json

import pytest
from conftest import CONTRACT_PATH, REPO_ROOT, contract_header

from pipeline.errors import ConfigError, ReasonCode
from pipeline.schema_contract import describe_mismatch, load_contracts, match_header, parse_contracts

CONTRACTS = load_contracts(CONTRACT_PATH)


def _values(header):
    """Fake record whose values name their delivered column, e.g. 'val-total_charge'."""
    return [f"val-{column}" for column in header]


def test_real_contract_loads():
    assert len(CONTRACTS.canonical_columns) == 22
    assert set(CONTRACTS.source_systems) == {"EPIC_NORTH", "LEGACY_MEDITECH", "ATHENA_CLINICS"}


@pytest.mark.parametrize(
    "source_system, version",
    [
        ("EPIC_NORTH", "epic_north_v1"),
        ("LEGACY_MEDITECH", "legacy_meditech_v1"),
        ("ATHENA_CLINICS", "athena_clinics_v1"),
        ("ATHENA_CLINICS", "athena_clinics_v2"),
    ],
)
def test_every_configured_version_is_matched(source_system, version):
    match = match_header(CONTRACTS, source_system, contract_header(source_system, version))

    assert match is not None
    assert match.version.version == version


def test_v1_maps_by_name_and_absent_column_is_none():
    header = contract_header("EPIC_NORTH")
    record = _values(header)
    record[header.index("payer_name")] = ""  # delivered empty

    row = match_header(CONTRACTS, "EPIC_NORTH", header).to_canonical(record)

    assert list(row) == list(CONTRACTS.canonical_columns)
    assert row["billed_amount"] == "val-billed_amount"
    assert row["attending_npi"] == "val-attending_npi"
    assert row["payer_name"] == ""  # empty stays empty
    assert row["encounter_source"] is None  # not in this version: NULL, not ""


def test_athena_v1_is_still_accepted_with_null_encounter_source():
    header = contract_header("ATHENA_CLINICS", "athena_clinics_v1")

    row = match_header(CONTRACTS, "ATHENA_CLINICS", header).to_canonical(_values(header))

    assert row["billed_amount"] == "val-billed_amount"
    assert row["encounter_source"] is None


def test_athena_v2_renames_reorder_and_added_column():
    header = contract_header("ATHENA_CLINICS", "athena_clinics_v2")

    row = match_header(CONTRACTS, "ATHENA_CLINICS", header).to_canonical(_values(header))

    assert row["billed_amount"] == "val-total_charge"
    assert row["attending_npi"] == "val-attending_provider_npi"
    assert row["encounter_source"] == "val-encounter_source"
    # Reordered columns are still found by name.
    assert row["patient_last_name"] == "val-patient_last_name"
    assert row["admit_date"] == "val-admit_date"
    assert "total_charge" not in row and "attending_provider_npi" not in row


def test_added_column_is_unknown_and_its_name_is_not_echoed():
    header = contract_header("EPIC_NORTH") + ["Doe"]

    assert match_header(CONTRACTS, "EPIC_NORTH", header) is None
    failure = describe_mismatch(CONTRACTS, "EPIC_NORTH", header)

    assert failure.code == ReasonCode.UNKNOWN_SCHEMA_CHANGE
    assert failure.detail == "closest_version=epic_north_v1,columns=22,expected_columns=21,unexpected_positions=22"
    assert "Doe" not in str(failure)


def test_missing_column_is_unknown():
    header = [c for c in contract_header("EPIC_NORTH") if c != "payer_name"]

    assert match_header(CONTRACTS, "EPIC_NORTH", header) is None
    assert "missing=payer_name" in describe_mismatch(CONTRACTS, "EPIC_NORTH", header).detail


def test_order_only_change_to_a_configured_version_is_unknown():
    header = contract_header("LEGACY_MEDITECH")
    header[0], header[1] = header[1], header[0]

    assert match_header(CONTRACTS, "LEGACY_MEDITECH", header) is None
    assert describe_mismatch(CONTRACTS, "LEGACY_MEDITECH", header).detail.endswith(",order_differs")


def test_athena_rename_is_not_accepted_for_another_system():
    header = ["total_charge" if c == "billed_amount" else c for c in contract_header("EPIC_NORTH")]

    assert match_header(CONTRACTS, "EPIC_NORTH", header) is None
    detail = describe_mismatch(CONTRACTS, "EPIC_NORTH", header).detail
    assert "missing=billed_amount" in detail and "unexpected_positions=19" in detail


def test_duplicate_column_is_unknown():
    header = contract_header("EPIC_NORTH")
    header[4] = "patient_mrn"  # patient_mrn is also at position 4

    detail = describe_mismatch(CONTRACTS, "EPIC_NORTH", header).detail

    assert match_header(CONTRACTS, "EPIC_NORTH", header) is None
    assert "missing=patient_first_name" in detail and "duplicate_positions=5" in detail


def test_case_and_whitespace_differences_are_not_guessed():
    header = contract_header("EPIC_NORTH")
    header[0] = "Source_System"
    header[10] = " admit_date"

    assert match_header(CONTRACTS, "EPIC_NORTH", header) is None
    assert "unexpected_positions=1|11" in describe_mismatch(CONTRACTS, "EPIC_NORTH", header).detail


def test_closest_version_is_the_one_sharing_most_columns():
    header = contract_header("ATHENA_CLINICS", "athena_clinics_v2") + ["new_col"]

    assert "closest_version=athena_clinics_v2" in describe_mismatch(CONTRACTS, "ATHENA_CLINICS", header).detail


def test_headerless_file_never_echoes_its_first_data_row():
    first_row = [f"Doe{chr(65 + i)} Jane" for i in range(21)]

    failure = describe_mismatch(CONTRACTS, "EPIC_NORTH", first_row)

    assert failure.code == ReasonCode.UNKNOWN_SCHEMA_CHANGE
    assert "Doe" not in str(failure) and "Jane" not in str(failure)


# --- configuration errors are system failures (ConfigError), not batch rejections ---


def _raw():
    return copy.deepcopy(json.loads(CONTRACT_PATH.read_text(encoding="utf-8")))


def _athena_v2(raw):
    return raw["source_systems"]["ATHENA_CLINICS"]["header_versions"][1]


def test_rename_to_a_column_outside_canonical_columns_is_a_config_error():
    raw = _raw()
    _athena_v2(raw)["renames"]["total_charge"] = "charge_usd"

    with pytest.raises(ConfigError, match="not in canonical_columns"):
        parse_contracts(raw)


def test_two_columns_mapping_to_one_canonical_column_is_a_config_error():
    raw = _raw()
    _athena_v2(raw)["renames"]["encounter_source"] = "billed_amount"

    with pytest.raises(ConfigError, match="map to one canonical column"):
        parse_contracts(raw)


def test_identical_header_versions_are_a_config_error():
    raw = _raw()
    versions = raw["source_systems"]["EPIC_NORTH"]["header_versions"]
    versions.append({**versions[0], "version": "epic_north_v2"})

    with pytest.raises(ConfigError, match="identical header versions"):
        parse_contracts(raw)


def test_duplicate_version_ids_are_a_config_error():
    raw = _raw()
    _athena_v2(raw)["version"] = "athena_clinics_v1"

    with pytest.raises(ConfigError, match="not unique"):
        parse_contracts(raw)


def test_unsafe_canonical_column_name_is_a_config_error():
    raw = _raw()
    raw["canonical_columns"].append("x; drop table")

    with pytest.raises(ConfigError, match="invalid canonical column"):
        parse_contracts(raw)


def test_unknown_encoding_is_a_config_error():
    raw = _raw()
    raw["source_systems"]["EPIC_NORTH"]["encoding"] = "no-such-codec"

    with pytest.raises(ConfigError, match="unknown encoding"):
        parse_contracts(raw)


def test_missing_key_is_a_config_error():
    raw = _raw()
    del raw["source_systems"]["EPIC_NORTH"]["file_name"]

    with pytest.raises(ConfigError, match="missing or malformed"):
        parse_contracts(raw)


def test_unreadable_contract_file_is_a_config_error():
    with pytest.raises(ConfigError, match="cannot be read"):
        load_contracts(REPO_ROOT / "config" / "does_not_exist.json")
