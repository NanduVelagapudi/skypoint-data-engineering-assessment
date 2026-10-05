"""NPI parser tests. NPIs are synthetic, built with an independent Luhn check below."""

import pytest

from pipeline.parsers.npi import npi_check_digit, npi_not_in_roster, parse_npi
from pipeline.parsers.result import FieldReason

CMS_EXAMPLE = "1234567893"  # the worked example in CMS's NPI check-digit guidance


def luhn_valid(number: str) -> bool:
    """Standard Luhn over a whole number, written independently of npi_check_digit."""
    total = 0
    for index, char in enumerate(reversed(number)):
        digit = int(char) * (2 if index % 2 else 1)
        total += digit - 9 if digit > 9 else digit
    return total % 10 == 0


def make_npi(first_nine: str) -> str:
    [check] = [d for d in "0123456789" if luhn_valid("80840" + first_nine + d)]
    return first_nine + check


VALID = make_npi("987654321")
OTHER_VALID = make_npi("100000000")


def test_check_digit_matches_the_cms_example_and_the_independent_luhn():
    assert npi_check_digit("123456789") == 3
    for first_nine in ("123456789", "987654321", "100000000", "999999999", "000000000", "555123456"):
        assert make_npi(first_nine) == first_nine + str(npi_check_digit(first_nine))


@pytest.mark.parametrize(
    "raw, expected",
    [
        (CMS_EXAMPLE, CMS_EXAMPLE),
        (VALID, VALID),
        (f"  {VALID}  ", VALID),
        (f"{VALID}.0", VALID),
        (f" {OTHER_VALID}.0 ", OTHER_VALID),
    ],
)
def test_valid_npis(raw, expected):
    result = parse_npi(raw)

    assert (result.cleaned_value, result.reason_code) == (expected, None)
    assert isinstance(result.cleaned_value, str)
    assert result.raw_value == raw


@pytest.mark.parametrize(
    "raw, reason",
    [
        (None, FieldReason.NPI_MISSING),
        ("", FieldReason.NPI_MISSING),
        ("   ", FieldReason.NPI_MISSING),
        ("N/A", FieldReason.NPI_PLACEHOLDER),
        ("123456789", FieldReason.NPI_INVALID_FORMAT),
        ("12345678901", FieldReason.NPI_INVALID_FORMAT),
        ("123456789.0", FieldReason.NPI_INVALID_FORMAT),
        ("NPI-123456", FieldReason.NPI_INVALID_FORMAT),
        ("123-456-7893", FieldReason.NPI_INVALID_FORMAT),
        ("12345 67893", FieldReason.NPI_INVALID_FORMAT),
        ("1234567893.00", FieldReason.NPI_INVALID_FORMAT),
        ("1.234567893E9", FieldReason.NPI_INVALID_FORMAT),
        ("123456789X", FieldReason.NPI_INVALID_FORMAT),
        ("١٢٣٤٥٦٧٨٩٣", FieldReason.NPI_INVALID_FORMAT),  # non-ASCII digits are not guessed
        ("1234567890", FieldReason.NPI_CHECKSUM_FAILED),
        ("1234567894", FieldReason.NPI_CHECKSUM_FAILED),
        ("1234567894.0", FieldReason.NPI_CHECKSUM_FAILED),
    ],
)
def test_invalid_npis_are_null_with_a_reason(raw, reason):
    result = parse_npi(raw)

    assert result.cleaned_value is None
    assert result.reason_code == reason


def test_every_single_digit_change_fails_the_checksum():
    for position in range(10):
        for replacement in "0123456789":
            if replacement == VALID[position]:
                continue
            changed = VALID[:position] + replacement + VALID[position + 1 :]
            assert parse_npi(changed).reason_code == FieldReason.NPI_CHECKSUM_FAILED


def test_roster_check_is_separate_from_validity():
    roster = frozenset({CMS_EXAMPLE})

    assert parse_npi(VALID).is_valid  # valid regardless of any roster
    assert npi_not_in_roster(VALID, roster) == FieldReason.NPI_NOT_IN_ROSTER
    assert npi_not_in_roster(CMS_EXAMPLE, roster) is None
    assert npi_not_in_roster(None, roster) is None  # an invalid NPI is reported by parse_npi instead
