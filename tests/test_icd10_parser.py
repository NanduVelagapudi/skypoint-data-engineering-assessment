"""ICD-10 parser tests. Codes are synthetic shapes modelled on the data pack."""

import pytest
from conftest import REAL_DATA_DIR

from pipeline.errors import ConfigError
from pipeline.parsers.icd10 import parse_diagnosis
from pipeline.parsers.result import FieldReason
from pipeline.reference_data import load_icd10_codes

REFERENCE = frozenset({"E11.9", "I10", "J45.909", "S72.001A", "S06.0X1A", "Z00.00"})


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("E11.9", "E11.9"),
        ("I10", "I10"),
        ("e11.9", "E11.9"),
        ("  E11.9  ", "E11.9"),
        ("E119", "E11.9"),
        ("j45909", "J45.909"),
        ("S72001A", "S72.001A"),
        ("s72.001a", "S72.001A"),
        ("S060X1A", "S06.0X1A"),
        ("E11.9 - Type 2 diabetes mellitus without complications", "E11.9"),
        ("I10 - Essential hypertension", "I10"),
        ("e119-diabetes", "E11.9"),
        ("Z00.00 -", "Z00.00"),
    ],
)
def test_codes_in_the_reference(raw, expected):
    result = parse_diagnosis(raw, REFERENCE)

    assert (result.cleaned_value, result.reason_code, result.warning_flag) == (expected, None, None)
    assert result.raw_value == raw


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("J18.9", "J18.9"),
        ("j189", "J18.9"),
        ("M54.50 - Low back pain", "M54.50"),
        ("U07.1", "U07.1"),
        ("R51", "R51"),
    ],
)
def test_valid_format_not_in_reference_keeps_the_code_with_a_warning(raw, expected):
    result = parse_diagnosis(raw, REFERENCE)

    assert result.cleaned_value == expected
    assert result.reason_code is None
    assert result.warning_flag == FieldReason.DX_NOT_IN_REFERENCE


@pytest.mark.parametrize("raw", ["250", "250.0", "250.00", "401.9", " 428.0 ", "250.00 - Diabetes"])
def test_icd9_is_null_and_never_converted(raw):
    result = parse_diagnosis(raw, REFERENCE)

    assert result.cleaned_value is None
    assert result.reason_code == FieldReason.DX_ICD9


@pytest.mark.parametrize(
    "raw, reason",
    [
        (None, FieldReason.DX_MISSING),
        ("", FieldReason.DX_MISSING),
        ("   ", FieldReason.DX_MISSING),
        ("N/A", FieldReason.DX_PLACEHOLDER),
        ("PENDING", FieldReason.DX_PLACEHOLDER),
        ("UNKNOWN", FieldReason.DX_UNPARSEABLE),
        ("see notes", FieldReason.DX_UNPARSEABLE),
        ("ABC12", FieldReason.DX_UNPARSEABLE),
        ("E1.19", FieldReason.DX_UNPARSEABLE),
        ("E11.", FieldReason.DX_UNPARSEABLE),
        ("E11.12345", FieldReason.DX_UNPARSEABLE),
        ("E11 9", FieldReason.DX_UNPARSEABLE),
        ("1E1.9", FieldReason.DX_UNPARSEABLE),
        ("4019", FieldReason.DX_UNPARSEABLE),
        ("- E11.9", FieldReason.DX_UNPARSEABLE),
        ("Ｅ11.9", FieldReason.DX_UNPARSEABLE),  # full-width letter is not guessed
    ],
)
def test_missing_and_unparseable_values_are_null(raw, reason):
    result = parse_diagnosis(raw, REFERENCE)

    assert result.cleaned_value is None
    assert result.reason_code == reason


# --- reference loader ---


def test_real_reference_loads_in_the_normalised_format():
    codes = load_icd10_codes(REAL_DATA_DIR / "reference" / "icd10_reference.csv")

    assert len(codes) == 43
    assert "E11.9" in codes and "I10" in codes
    assert all(parse_diagnosis(code, codes).warning_flag is None for code in codes)


def test_bad_reference_files_are_config_errors(tmp_path):
    missing_column = tmp_path / "a.csv"
    missing_column.write_text("code,description\nE11.9,x\n", encoding="utf-8")
    bad_code = tmp_path / "b.csv"
    bad_code.write_text("icd10_code,description\ne119,x\n", encoding="utf-8")
    duplicate = tmp_path / "c.csv"
    duplicate.write_text("icd10_code,description\nE11.9,x\nE11.9,y\n", encoding="utf-8")
    empty = tmp_path / "d.csv"
    empty.write_text("icd10_code,description\n", encoding="utf-8")

    for path in (missing_column, bad_code, duplicate, empty, tmp_path / "absent.csv"):
        with pytest.raises(ConfigError):
            load_icd10_codes(path)
