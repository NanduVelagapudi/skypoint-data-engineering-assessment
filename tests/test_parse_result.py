from pathlib import Path

import pytest
from conftest import REAL_DATA_DIR

from pipeline.errors import ConfigError
from pipeline.parsers.result import FieldReason, ParseResult
from pipeline.source_conventions import AmountUnit, DateOrder, load_source_conventions, parse_source_conventions

# --- ParseResult ---


def test_valid_and_invalid_constructors():
    ok = ParseResult.valid(" 5 ", 5)
    bad = ParseResult.invalid("x", FieldReason.AMOUNT_UNPARSEABLE)

    assert (ok.raw_value, ok.cleaned_value, ok.reason_code, ok.warning_flag, ok.is_valid) == (" 5 ", 5, None, None, True)
    assert (bad.raw_value, bad.cleaned_value, bad.reason_code, bad.is_valid) == ("x", None, FieldReason.AMOUNT_UNPARSEABLE, False)


def test_a_result_needs_exactly_one_of_value_and_reason():
    with pytest.raises(ValueError):
        ParseResult("x", 5, FieldReason.AMOUNT_UNPARSEABLE)
    with pytest.raises(ValueError):
        ParseResult("x", None, None)


def test_warning_flag_can_accompany_a_valid_value():
    result = ParseResult.valid("x", 1, FieldReason.TIMESTAMP_AMBIGUOUS_LOCAL_TIME)

    assert result.is_valid and result.warning_flag == FieldReason.TIMESTAMP_AMBIGUOUS_LOCAL_TIME


def test_repr_never_shows_raw_or_cleaned_values():
    text = repr(ParseResult.valid("ZZRAW-1980-07-15", "ZZCLEAN"))

    assert "ZZRAW" not in text and "ZZCLEAN" not in text


# --- source conventions from the reference metadata ---


def test_real_reference_conventions():
    conventions = load_source_conventions(REAL_DATA_DIR / "reference" / "source_systems_and_facilities.json")

    epic, meditech, athena = conventions["EPIC_NORTH"], conventions["LEGACY_MEDITECH"], conventions["ATHENA_CLINICS"]
    assert (epic.date_order, epic.amount_unit, epic.timestamp_timezone) == (DateOrder.MDY, AmountUnit.USD, "UTC")
    assert (meditech.date_order, meditech.amount_unit, meditech.timestamp_timezone) == (
        DateOrder.DMY,
        AmountUnit.USD_CENTS,
        "America/Chicago",
    )
    assert (athena.date_order, athena.amount_unit, athena.timestamp_timezone) == (DateOrder.MDY, AmountUnit.USD, "UTC")
    # Only Athena's notes state a two-digit-year rule.
    assert (epic.two_digit_year_century, meditech.two_digit_year_century, athena.two_digit_year_century) == (None, None, 2000)


def _reference(**overrides):
    entry = {
        "source_system": "SYS",
        "date_order": "MDY",
        "amount_unit": "USD",
        "timestamp_timezone": "UTC",
        "notes": "",
    }
    return {"source_systems": [{**entry, **overrides}]}


@pytest.mark.parametrize(
    "overrides",
    [{"date_order": "YMD"}, {"amount_unit": "EUR"}, {"timestamp_timezone": "Mars/Olympus"}, {"timestamp_timezone": ""}],
)
def test_unsupported_reference_values_are_config_errors(overrides):
    with pytest.raises(ConfigError):
        parse_source_conventions(_reference(**overrides))


def test_missing_reference_key_is_a_config_error():
    raw = _reference()
    del raw["source_systems"][0]["amount_unit"]

    with pytest.raises(ConfigError):
        parse_source_conventions(raw)


def test_unreadable_reference_file_is_a_config_error():
    with pytest.raises(ConfigError):
        load_source_conventions(Path(REAL_DATA_DIR / "reference" / "does_not_exist.json"))


def test_century_is_read_from_the_notes_rule_only():
    assert parse_source_conventions(_reference(notes="Two-digit years mean 19xx."))["SYS"].two_digit_year_century == 1900
    assert parse_source_conventions(_reference(notes="Numeric dates are month-first."))["SYS"].two_digit_year_century is None
