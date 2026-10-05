"""Date parser tests. Inputs are synthetic shapes modelled on the data pack, not real values."""

from datetime import UTC, date, datetime, timedelta, timezone

import pytest

from pipeline.parsers.dates import admit_period, discharge_before_admit, parse_date
from pipeline.parsers.result import FieldReason
from pipeline.source_conventions import DateOrder

MDY, DMY = DateOrder.MDY, DateOrder.DMY
DELIVERED = datetime(2025, 1, 20, 6, 0, tzinfo=UTC)


def parse(raw, order=MDY, century=None, delivered=DELIVERED):
    return parse_date(raw, date_order=order, delivered_at=delivered, two_digit_year_century=century)


@pytest.mark.parametrize(
    "raw, order, century, expected",
    [
        # Epic / Athena: month-first
        ("03/04/2024", MDY, None, date(2024, 3, 4)),
        ("3/4/2024", MDY, None, date(2024, 3, 4)),
        ("12/31/2024", MDY, None, date(2024, 12, 31)),
        # Meditech: day-first, with / or -
        ("03/04/2024", DMY, None, date(2024, 4, 3)),
        ("03-04-2024", DMY, None, date(2024, 4, 3)),
        ("31/12/2024", DMY, None, date(2024, 12, 31)),
        # Athena two-digit year (reference: 20xx)
        ("03-04-24", MDY, 2000, date(2024, 3, 4)),
        ("03/04/24", MDY, 2000, date(2024, 3, 4)),
        # ISO, with and without a time part; not affected by the date order
        ("2024-03-04", MDY, None, date(2024, 3, 4)),
        ("2024-03-04", DMY, None, date(2024, 3, 4)),
        ("2024-03-04T23:59:59", MDY, None, date(2024, 3, 4)),
        ("2024-02-29", MDY, None, date(2024, 2, 29)),
        # Month names, month-first and day-first; not affected by the date order
        ("Mar 4, 2024", MDY, None, date(2024, 3, 4)),
        ("March 04, 2024", MDY, None, date(2024, 3, 4)),
        ("Sept 9, 2024", MDY, None, date(2024, 9, 9)),
        ("mar 4, 2024", MDY, None, date(2024, 3, 4)),
        ("4 Mar 2024", DMY, None, date(2024, 3, 4)),
        ("04 March 2024", DMY, None, date(2024, 3, 4)),
        ("4 Mar 2024", MDY, None, date(2024, 3, 4)),
        ("  2024-03-04  ", MDY, None, date(2024, 3, 4)),
    ],
)
def test_valid_dates(raw, order, century, expected):
    result = parse(raw, order, century)

    assert result.reason_code is None
    assert result.cleaned_value == expected
    assert result.raw_value == raw


def test_ambiguous_numeric_date_follows_each_systems_order():
    assert parse("05/06/2024", MDY).cleaned_value == date(2024, 5, 6)
    assert parse("05/06/2024", DMY).cleaned_value == date(2024, 6, 5)


@pytest.mark.parametrize(
    "raw, order, century, reason",
    [
        (None, MDY, None, FieldReason.DATE_MISSING),
        ("", MDY, None, FieldReason.DATE_MISSING),
        ("   ", MDY, None, FieldReason.DATE_MISSING),
        ("TBD", MDY, None, FieldReason.DATE_PLACEHOLDER),
        ("tbd", MDY, None, FieldReason.DATE_PLACEHOLDER),
        ("02/30/2024", MDY, None, FieldReason.DATE_INVALID),
        ("13/01/2024", MDY, None, FieldReason.DATE_INVALID),  # not swapped to 1 Jan
        ("01/13/2024", DMY, None, FieldReason.DATE_INVALID),
        ("2023-02-29", MDY, None, FieldReason.DATE_INVALID),
        ("2024-00-10", MDY, None, FieldReason.DATE_INVALID),
        ("Feb 30, 2024", MDY, None, FieldReason.DATE_INVALID),
        ("2024-03-04T25:00:00", MDY, None, FieldReason.DATE_INVALID),
        ("0000-01-01", MDY, None, FieldReason.DATE_INVALID),
        ("03/04/24", MDY, None, FieldReason.DATE_CENTURY_UNKNOWN),  # no rule stated for this source
        ("2024/03/04", MDY, None, FieldReason.DATE_UNPARSEABLE),
        ("03/04-2024", MDY, None, FieldReason.DATE_UNPARSEABLE),
        ("20240304", MDY, None, FieldReason.DATE_UNPARSEABLE),
        ("4th March 2024", MDY, None, FieldReason.DATE_UNPARSEABLE),
        ("Foo 4, 2024", MDY, None, FieldReason.DATE_UNPARSEABLE),
        ("2024-3-4", MDY, None, FieldReason.DATE_UNPARSEABLE),
    ],
)
def test_invalid_dates_are_null_with_a_reason(raw, order, century, reason):
    result = parse(raw, order, century)

    assert result.cleaned_value is None
    assert result.reason_code == reason


def test_date_after_delivery_is_null():
    assert parse("2025-01-20").cleaned_value == date(2025, 1, 20)  # the delivery day itself is allowed
    after = parse("2025-01-21")

    assert after.cleaned_value is None
    assert after.reason_code == FieldReason.DATE_AFTER_DELIVERY
    assert parse("01-21-25", MDY, 2000).reason_code == FieldReason.DATE_AFTER_DELIVERY


def test_delivery_cutoff_is_the_utc_date_of_delivered_at():
    # 01:00 on 20 Jan at +05:30 is still 19 Jan in UTC.
    delivered = datetime(2025, 1, 20, 1, 0, tzinfo=timezone(timedelta(hours=5, minutes=30)))

    assert parse("2025-01-19", delivered=delivered).reason_code is None
    assert parse("2025-01-20", delivered=delivered).reason_code == FieldReason.DATE_AFTER_DELIVERY


def test_naive_delivered_at_is_a_caller_error():
    with pytest.raises(ValueError):
        parse("2024-03-04", delivered=datetime(2025, 1, 20))


def test_dob_is_parsed_like_any_date_and_not_shown_in_repr():
    result = parse("07/15/1980", MDY)

    assert result.cleaned_value == date(1980, 7, 15)
    assert "1980" not in repr(result)


def test_discharge_before_admit_is_flagged_separately_and_nulls_nothing():
    admit = parse("2024-03-10")
    discharge = parse("2024-03-05")

    assert admit.cleaned_value == date(2024, 3, 10)
    assert discharge.cleaned_value == date(2024, 3, 5)  # still parsed, not nulled
    assert discharge.reason_code is None
    assert discharge_before_admit(admit.cleaned_value, discharge.cleaned_value) == FieldReason.DISCHARGE_BEFORE_ADMIT
    assert discharge_before_admit(date(2024, 3, 10), date(2024, 3, 10)) is None
    assert discharge_before_admit(date(2024, 3, 10), None) is None
    assert discharge_before_admit(None, date(2024, 3, 5)) is None


@pytest.mark.parametrize(
    "admit, expected",
    [
        (date(2024, 1, 1), (2024, 1, 1)),
        (date(2024, 3, 31), (2024, 1, 3)),
        (date(2024, 4, 1), (2024, 2, 4)),
        (date(2024, 9, 30), (2024, 3, 9)),
        (date(2024, 12, 31), (2024, 4, 12)),
    ],
)
def test_admit_period(admit, expected):
    period = admit_period(admit)

    assert (period.year, period.quarter, period.month) == expected
