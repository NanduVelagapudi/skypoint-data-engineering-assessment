"""Reference loaders for Task 5: ICD-10 rows, the full facility master and the roster snapshots.

The real reference files are only read. Failure cases use files written to tmp_path.
"""

import json
from datetime import date

import pytest
from conftest import REAL_DATA_DIR, REPO_ROOT

from pipeline.errors import ConfigError
from pipeline.parsers.npi import npi_check_digit
from pipeline.reference_data import (
    ROSTER_COLUMNS,
    load_cleaning_reference,
    load_facility_records,
    load_icd10_codes,
    load_icd10_reference,
    load_roster_snapshots,
)

REFERENCE = REAL_DATA_DIR / "reference"
ALIASES = REPO_ROOT / "config" / "facility_aliases.json"
ICD_HEADER = "icd10_code,description,category,is_chronic\n"


def valid_npi(first_nine: str) -> str:
    return first_nine + str(npi_check_digit(first_nine))


def write_roster(folder, as_of, rows, *, name=None, header=ROSTER_COLUMNS):
    folder.mkdir(parents=True, exist_ok=True)
    lines = [",".join(header)] + [",".join(r) for r in rows]
    (folder / (name or f"roster_{as_of}.csv")).write_text("\n".join(lines) + "\n", encoding="utf-8")


def roster_row(as_of, npi, **changes):
    row = dict(zip(ROSTER_COLUMNS, [as_of, npi, "Zzlast", "Zzfirst", "MD", "Cardiology", "FAC001", "Employed"]))
    row.update(changes)
    return [row[c] for c in ROSTER_COLUMNS]


# --- real reference files ---


def test_icd10_reference_rows():
    entries = load_icd10_reference(REFERENCE / "icd10_reference.csv")

    assert len(entries) == 43
    assert sum(e.is_chronic for e in entries) == 14
    assert len({e.category for e in entries}) == 15
    assert {e.category for e in entries if e.is_chronic} == {"ASTHMA", "CKD", "COPD", "DIABETES", "HEART_FAILURE", "HYPERTENSION"}
    assert [e.source_row_number for e in entries] == list(range(1, 44))
    assert frozenset(e.icd10_code for e in entries) == load_icd10_codes(REFERENCE / "icd10_reference.csv")


def test_facility_records_carry_every_master_attribute():
    records = load_facility_records(REFERENCE / "source_systems_and_facilities.json")

    assert [r.facility_id for r in records] == [f"FAC00{i}" for i in range(1, 9)]
    assert [r.source_position for r in records] == list(range(1, 9))
    assert all(isinstance(r.bed_count, int) and isinstance(r.go_live_date, date) for r in records)
    assert {r.facility_type for r in records} == {
        "Acute Care Hospital", "Clinic", "Critical Access Hospital", "Behavioral Health Hospital", "Urgent Care", "Specialty Clinic",
    }


def test_roster_snapshots_oldest_first():
    snapshots = load_roster_snapshots(REFERENCE / "provider_roster")

    assert [(s.as_of_date, s.file_name, len(s.entries)) for s in snapshots] == [
        (date(2024, 1, 1), "roster_2024-01-01.csv", 50),
        (date(2024, 7, 1), "roster_2024-07-01.csv", 51),
        (date(2025, 1, 1), "roster_2025-01-01.csv", 51),
    ]
    assert len({e.npi for s in snapshots for e in s.entries}) == 54
    first = snapshots[0].entries[0]
    assert (first.file_name, first.source_row_number, first.as_of_date) == ("roster_2024-01-01.csv", 1, date(2024, 1, 1))


def test_cleaning_reference_bundles_what_field_cleaning_needs():
    reference = load_cleaning_reference(REFERENCE, ALIASES)

    assert len(reference.icd10_codes) == 43
    assert len(reference.roster_npis) == 54
    assert len(reference.facility_index.facilities) == 8


# --- failures ---


def test_icd10_is_chronic_must_be_y_or_n(tmp_path):
    path = tmp_path / "icd.csv"
    path.write_text(ICD_HEADER + "E11.9,Type 2 diabetes,DIABETES,yes\n", encoding="utf-8")

    with pytest.raises(ConfigError, match="is_chronic"):
        load_icd10_reference(path)


def test_icd10_description_and_category_are_required(tmp_path):
    path = tmp_path / "icd.csv"
    path.write_text(ICD_HEADER + "E11.9,,DIABETES,Y\n", encoding="utf-8")

    with pytest.raises(ConfigError, match="row 1"):
        load_icd10_reference(path)


@pytest.mark.parametrize(
    "change, message",
    [
        ({"bed_count": "320"}, "bed_count"),
        ({"bed_count": True}, "bed_count"),
        ({"go_live_date": "June 2021"}, "malformed"),
        ({"city": " "}, "non-blank"),
    ],
)
def test_facility_master_values_are_checked(tmp_path, change, message):
    master = json.loads((REFERENCE / "source_systems_and_facilities.json").read_text(encoding="utf-8"))
    master["facilities"][0].update(change)
    path = tmp_path / "master.json"
    path.write_text(json.dumps(master), encoding="utf-8")

    with pytest.raises(ConfigError, match=message):
        load_facility_records(path)


def test_facility_master_ids_must_be_unique(tmp_path):
    master = json.loads((REFERENCE / "source_systems_and_facilities.json").read_text(encoding="utf-8"))
    master["facilities"][1]["facility_id"] = master["facilities"][0]["facility_id"]
    path = tmp_path / "master.json"
    path.write_text(json.dumps(master), encoding="utf-8")

    with pytest.raises(ConfigError, match="duplicate facility_id"):
        load_facility_records(path)


def test_valid_synthetic_roster_loads(tmp_path):
    write_roster(tmp_path, "2024-07-01", [roster_row("2024-07-01", valid_npi("123456789"))])
    write_roster(tmp_path, "2024-01-01", [roster_row("2024-01-01", valid_npi("123456789"))])

    assert [s.as_of_date for s in load_roster_snapshots(tmp_path)] == [date(2024, 1, 1), date(2024, 7, 1)]


@pytest.mark.parametrize(
    "build, message",
    [
        (lambda d: write_roster(d, "2024-01-01", [roster_row("2024-02-01", valid_npi("123456789"))]), "differs from the file name"),
        (lambda d: write_roster(d, "2024-01-01", [roster_row("2024-01-01", valid_npi("123456789"))] * 2), "more than once"),
        (lambda d: write_roster(d, "2024-01-01", [roster_row("2024-01-01", "1234567890")]), "not a valid 10-digit NPI"),
        (lambda d: write_roster(d, "2024-01-01", [roster_row("2024-01-01", valid_npi("123456789"), specialty="")]), "every field"),
        (lambda d: write_roster(d, "2024-01-01", [], header=ROSTER_COLUMNS[::-1]), "expected columns"),
        (lambda d: write_roster(d, "2024-01-01", [roster_row("2024-01-01", valid_npi("123456789"))], name="roster_jan.csv"), "unexpected file"),
        (lambda d: write_roster(d, "2024-01-01", []), "is empty"),
        (lambda d: d.mkdir(), "no snapshots"),
    ],
)
def test_invalid_rosters_are_config_errors(tmp_path, build, message):
    folder = tmp_path / "roster"
    build(folder)

    with pytest.raises(ConfigError, match=message):
        load_roster_snapshots(folder)


def test_roster_errors_never_quote_values(tmp_path):
    write_roster(tmp_path, "2024-01-01", [roster_row("2024-01-01", valid_npi("123456789"), provider_last_name="")])

    with pytest.raises(ConfigError) as error:
        load_roster_snapshots(tmp_path)

    assert "Zz" not in str(error.value) and valid_npi("123456789") not in str(error.value)
