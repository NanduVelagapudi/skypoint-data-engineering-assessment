"""Amount parser tests. Inputs are synthetic shapes modelled on the data pack, not real values."""

from decimal import Decimal

import pytest

from pipeline.parsers.amount import parse_amount
from pipeline.parsers.result import FieldReason
from pipeline.source_conventions import AmountUnit

USD, CENTS = AmountUnit.USD, AmountUnit.USD_CENTS


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("1234.50", "1234.50"),
        ("1234.5", "1234.50"),
        ("999", "999.00"),
        ("0.00", "0.00"),
        ("$1234.50", "1234.50"),
        ("$1,234.50", "1234.50"),
        ("$12,345,678.90", "12345678.90"),
        ("$ 1234.5", "1234.50"),
        ("  $1,234.50  ", "1234.50"),
        ("USD 1,234.50", "1234.50"),
        ("USD 99.10", "99.10"),
        ("1,234.50 USD", "1234.50"),
        ("usd 5", "5.00"),
        ("$1.25K", "1250.00"),
        ("$12.5K", "12500.00"),
        ("$1.23456K", "1234.56"),
        ("7K", "7000.00"),
    ],
)
def test_valid_usd_amounts(raw, expected):
    result = parse_amount(raw, USD)

    assert result.reason_code is None
    assert result.cleaned_value == Decimal(expected)
    assert result.raw_value == raw  # original kept, whitespace included


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("123456", "1234.56"),
        ("1,234,567", "12345.67"),
        (" 99999", "999.99"),
        ("123456.0", "1234.56"),
        ("123456.00", "1234.56"),
        ("5", "0.05"),
        ("0", "0.00"),
    ],
)
def test_meditech_cents_are_divided_by_100(raw, expected):
    result = parse_amount(raw, CENTS)

    assert result.reason_code is None
    assert result.cleaned_value == Decimal(expected)


@pytest.mark.parametrize(
    "raw, reason",
    [
        (None, FieldReason.AMOUNT_MISSING),
        ("", FieldReason.AMOUNT_MISSING),
        ("   ", FieldReason.AMOUNT_MISSING),
        ("N/A", FieldReason.AMOUNT_PLACEHOLDER),
        ("n/a", FieldReason.AMOUNT_PLACEHOLDER),
        ("PENDING", FieldReason.AMOUNT_PLACEHOLDER),
        ("TBD", FieldReason.AMOUNT_PLACEHOLDER),
        ("#VALUE!", FieldReason.AMOUNT_SPREADSHEET_ERROR),
        ("#DIV/0!", FieldReason.AMOUNT_SPREADSHEET_ERROR),
        ("#N/A", FieldReason.AMOUNT_SPREADSHEET_ERROR),
        ("see billing note", FieldReason.AMOUNT_UNPARSEABLE),
        ("12..50", FieldReason.AMOUNT_UNPARSEABLE),
        ("1,23.00", FieldReason.AMOUNT_UNPARSEABLE),
        ("1 234.00", FieldReason.AMOUNT_UNPARSEABLE),
        ("-100.00", FieldReason.AMOUNT_UNPARSEABLE),
        ("(100.00)", FieldReason.AMOUNT_UNPARSEABLE),
        ("$100 USD", FieldReason.AMOUNT_UNPARSEABLE),
        ("USD $100", FieldReason.AMOUNT_UNPARSEABLE),
        ("€100", FieldReason.AMOUNT_UNPARSEABLE),
        (".50", FieldReason.AMOUNT_UNPARSEABLE),
        ("١٢٣", FieldReason.AMOUNT_UNPARSEABLE),  # non-ASCII digits are not guessed
        ("12.345", FieldReason.AMOUNT_FRACTIONAL_CENTS),
        ("$1.234567K", FieldReason.AMOUNT_FRACTIONAL_CENTS),
    ],
)
def test_invalid_usd_amounts_are_null_with_a_reason_never_zero(raw, reason):
    result = parse_amount(raw, USD)

    assert result.cleaned_value is None
    assert result.reason_code == reason
    assert result.raw_value == raw


@pytest.mark.parametrize(
    "raw, reason",
    [
        ("123456.5", FieldReason.AMOUNT_FRACTIONAL_CENTS),
        ("123456.01", FieldReason.AMOUNT_FRACTIONAL_CENTS),
        ("12K", FieldReason.AMOUNT_K_SUFFIX_NOT_ALLOWED),
        ("1.5K", FieldReason.AMOUNT_K_SUFFIX_NOT_ALLOWED),
        ("$123456", FieldReason.AMOUNT_UNPARSEABLE),
        ("123456 USD", FieldReason.AMOUNT_UNPARSEABLE),
        ("N/A", FieldReason.AMOUNT_PLACEHOLDER),
        ("", FieldReason.AMOUNT_MISSING),
        ("#VALUE!", FieldReason.AMOUNT_SPREADSHEET_ERROR),
        ("12..50", FieldReason.AMOUNT_UNPARSEABLE),
    ],
)
def test_invalid_meditech_amounts(raw, reason):
    result = parse_amount(raw, CENTS)

    assert result.cleaned_value is None
    assert result.reason_code == reason


def test_values_are_decimals_with_exactly_two_places():
    values = [parse_amount(raw, USD).cleaned_value for raw in ("0.1", "0.2", "$1.25K", "99,999,999,999.99")]
    cents = parse_amount("1", CENTS).cleaned_value

    assert all(isinstance(v, Decimal) for v in [*values, cents])
    assert all(v.as_tuple().exponent == -2 for v in [*values, cents])
    assert values[0] + values[1] == Decimal("0.30")  # exact, unlike floats
    assert values[3] == Decimal("99999999999.99")
    assert cents == Decimal("0.01")
