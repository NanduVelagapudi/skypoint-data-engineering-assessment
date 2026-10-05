"""Task 3 field functions. All names, dates and ZIPs here are synthetic."""

from datetime import date

import pytest

from pipeline.parsers.patient import (
    AgeBand,
    Sex,
    age_band,
    normalise_first_given_name,
    normalise_last_name,
    normalise_sex,
    zip3,
)
from pipeline.parsers.result import FieldReason

# --- names ---


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("Testerson", "TESTERSON"),
        ("  testerson  ", "TESTERSON"),
        ("O'Testa", "OTESTA"),
        ("O Testa", "OTESTA"),
        ("OTESTA", "OTESTA"),
        ("Smith-Testa", "SMITHTESTA"),
        ("De La Testa", "DELATESTA"),
        ("St. Testa", "STTESTA"),
    ],
)
def test_last_name_drops_case_whitespace_and_punctuation(raw, expected):
    result = normalise_last_name(raw)

    assert (result.cleaned_value, result.reason_code) == (expected, None)


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("Anna", "ANNA"),
        ("  anna  ", "ANNA"),
        ("Anna B", "ANNA"),
        ("Anna B.", "ANNA"),
        ("anna   b.", "ANNA"),
        ("ANNA B C", "ANNA"),
        ("Anna-Lee", "ANNALEE"),
        ("J", "J"),  # a one-letter first token is the given name, not an initial
        ("J. Testname", "J"),
        ("Mary Testa", "MARY"),  # only the first token is the given name
    ],
)
def test_first_given_name_is_the_first_token_without_punctuation(raw, expected):
    result = normalise_first_given_name(raw)

    assert (result.cleaned_value, result.reason_code) == (expected, None)


@pytest.mark.parametrize("raw", [None, "", "   ", "'-."])
def test_missing_last_name(raw):
    result = normalise_last_name(raw)

    assert (result.cleaned_value, result.reason_code) == (None, FieldReason.LAST_NAME_MISSING)


@pytest.mark.parametrize("raw", [None, "", "   ", "'-.", ". B"])
def test_missing_first_given_name(raw):
    # ". B": the first token has no letters, and the initial after it is not promoted.
    result = normalise_first_given_name(raw)

    assert (result.cleaned_value, result.reason_code) == (None, FieldReason.FIRST_NAME_MISSING)


def test_name_results_never_show_values_in_repr():
    assert "TESTERSON" not in repr(normalise_last_name("Testerson")).upper()
    assert "ANNA" not in repr(normalise_first_given_name("Anna B")).upper()


# --- sex ---


@pytest.mark.parametrize(
    "raw, expected",
    [("F", Sex.F), ("f", Sex.F), ("Female", Sex.F), (" FEMALE ", Sex.F), ("M", Sex.M), ("Male", Sex.M), ("male", Sex.M)],
)
def test_sex_values(raw, expected):
    result = normalise_sex(raw)

    assert (result.cleaned_value, result.warning_flag) == (expected, None)


@pytest.mark.parametrize(
    "raw, flag",
    [
        (None, FieldReason.SEX_MISSING),
        ("", FieldReason.SEX_MISSING),
        ("U", FieldReason.SEX_UNMAPPED),
        ("X", FieldReason.SEX_UNMAPPED),
        ("Other", FieldReason.SEX_UNMAPPED),
        ("N/A", FieldReason.SEX_UNMAPPED),
    ],
)
def test_other_sex_values_are_unknown_and_flagged(raw, flag):
    result = normalise_sex(raw)

    assert (result.cleaned_value, result.warning_flag) == (Sex.UNKNOWN, flag)


# --- age band ---

ADMIT = date(2024, 6, 15)


@pytest.mark.parametrize(
    "dob, expected",
    [
        (ADMIT, AgeBand.AGE_0_17),  # age 0 on the day of birth
        (date(2006, 6, 16), AgeBand.AGE_0_17),  # 17, turns 18 the next day
        (date(2006, 6, 15), AgeBand.AGE_18_39),  # 18th birthday
        (date(1984, 6, 16), AgeBand.AGE_18_39),  # 39
        (date(1984, 6, 15), AgeBand.AGE_40_64),  # 40th birthday
        (date(1959, 6, 16), AgeBand.AGE_40_64),  # 64
        (date(1959, 6, 15), AgeBand.AGE_65_PLUS),  # 65th birthday
        (date(1920, 1, 1), AgeBand.AGE_65_PLUS),
    ],
)
def test_age_band_boundaries(dob, expected):
    result = age_band(dob, ADMIT)

    assert (result.cleaned_value, result.warning_flag) == (expected, None)
    assert result.raw_value is None  # the DOB is never carried in the result


def test_leap_day_birthday_counts_from_march_first_in_other_years():
    assert age_band(date(2006, 2, 28), date(2024, 2, 28)).cleaned_value == AgeBand.AGE_18_39
    assert age_band(date(2004, 2, 29), date(2022, 2, 28)).cleaned_value == AgeBand.AGE_0_17
    assert age_band(date(2004, 2, 29), date(2022, 3, 1)).cleaned_value == AgeBand.AGE_18_39


@pytest.mark.parametrize(
    "dob, admit, flag",
    [
        (None, ADMIT, FieldReason.AGE_BAND_DOB_UNAVAILABLE),
        (date(1980, 1, 1), None, FieldReason.AGE_BAND_ADMIT_UNAVAILABLE),
        (None, None, FieldReason.AGE_BAND_DOB_UNAVAILABLE),
        (date(2024, 6, 16), ADMIT, FieldReason.AGE_BAND_DOB_AFTER_ADMIT),
    ],
)
def test_unknown_age_band(dob, admit, flag):
    result = age_band(dob, admit)

    assert (result.cleaned_value, result.warning_flag) == (AgeBand.UNKNOWN, flag)


# --- ZIP3 ---


@pytest.mark.parametrize("raw", ["53201", " 53201 ", "53201-1234"])
def test_zip3_keeps_only_the_first_three_digits(raw):
    result = zip3(raw)

    assert (result.cleaned_value, result.reason_code) == ("532", None)


@pytest.mark.parametrize(
    "raw, reason",
    [
        (None, FieldReason.ZIP_MISSING),
        ("", FieldReason.ZIP_MISSING),
        ("   ", FieldReason.ZIP_MISSING),
        ("5320", FieldReason.ZIP_INVALID),
        ("532011", FieldReason.ZIP_INVALID),
        ("ABCDE", FieldReason.ZIP_INVALID),
        ("53201-12", FieldReason.ZIP_INVALID),
        ("53 201", FieldReason.ZIP_INVALID),
        ("５３２０１", FieldReason.ZIP_INVALID),  # full-width digits are not guessed
    ],
)
def test_invalid_or_missing_zip_is_null_with_a_reason(raw, reason):
    result = zip3(raw)

    assert (result.cleaned_value, result.reason_code) == (None, reason)
