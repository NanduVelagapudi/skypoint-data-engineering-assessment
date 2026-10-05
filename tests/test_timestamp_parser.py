"""Timestamp parser tests. Inputs are synthetic shapes modelled on the data pack, not real values."""

from datetime import UTC, datetime, timedelta

import pytest

from pipeline.parsers.result import FieldReason
from pipeline.parsers.timestamps import parse_timestamp
from pipeline.source_conventions import DateOrder

CHICAGO = "America/Chicago"


def epic(raw):
    return parse_timestamp(raw, timezone_name="UTC", date_order=DateOrder.MDY)


def athena(raw):
    return parse_timestamp(raw, timezone_name="UTC", date_order=DateOrder.MDY)


def meditech(raw):
    return parse_timestamp(raw, timezone_name=CHICAGO, date_order=DateOrder.DMY)


def utc(*args):
    return datetime(*args, tzinfo=UTC)


def assert_utc_instant(result, expected):
    assert result.reason_code is None
    assert isinstance(result.cleaned_value, datetime)  # an instant, not a formatted string
    assert result.cleaned_value.utcoffset() == timedelta(0)
    assert result.cleaned_value == expected


def test_epic_z_is_utc():
    assert_utc_instant(epic("2024-03-05T14:30:00Z"), utc(2024, 3, 5, 14, 30))


def test_athena_naive_value_is_utc():
    result = athena("2024-03-05 14:30:00")

    assert_utc_instant(result, utc(2024, 3, 5, 14, 30))
    assert result.warning_flag is None


def test_athena_explicit_offset_wins_over_the_source_zone():
    assert_utc_instant(athena("2024-03-05T09:30:00-05:00"), utc(2024, 3, 5, 14, 30))
    assert_utc_instant(athena("2024-03-05T20:00:00+05:30"), utc(2024, 3, 5, 14, 30))


def test_meditech_chicago_winter_and_summer_are_converted_to_utc():
    assert_utc_instant(meditech("05/03/2024 08:30:00"), utc(2024, 3, 5, 14, 30))  # CST, UTC-6
    assert_utc_instant(meditech("05/07/2024 09:30:00"), utc(2024, 7, 5, 14, 30))  # CDT, UTC-5


def test_meditech_uses_day_first_order():
    assert meditech("01/02/2024 12:00:00").cleaned_value == utc(2024, 2, 1, 18, 0)


def test_explicit_offset_also_wins_for_a_local_time_source():
    assert_utc_instant(meditech("2024-03-05T14:30:00Z"), utc(2024, 3, 5, 14, 30))


def test_fall_back_ambiguous_time_takes_the_earlier_instant_and_is_flagged():
    # 3 Nov 2024: 01:30 happens twice in Chicago (CDT -5, then CST -6).
    result = meditech("03/11/2024 01:30:00")

    assert_utc_instant(result, utc(2024, 11, 3, 6, 30))  # the earlier instant (CDT)
    assert result.warning_flag == FieldReason.TIMESTAMP_AMBIGUOUS_LOCAL_TIME


def test_spring_forward_nonexistent_time_is_null():
    # 10 Mar 2024: Chicago clocks jump from 02:00 to 03:00.
    result = meditech("10/03/2024 02:30:00")

    assert result.cleaned_value is None
    assert result.reason_code == FieldReason.TIMESTAMP_NONEXISTENT_LOCAL_TIME


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("10/03/2024 01:59:59", utc(2024, 3, 10, 7, 59, 59)),  # last second of CST
        ("10/03/2024 03:00:00", utc(2024, 3, 10, 8, 0)),  # first second of CDT
        ("03/11/2024 00:59:59", utc(2024, 11, 3, 5, 59, 59)),  # before the repeated hour
        ("03/11/2024 02:00:00", utc(2024, 11, 3, 8, 0)),  # after it, unambiguous CST
    ],
)
def test_dst_boundaries_are_not_flagged(raw, expected):
    result = meditech(raw)

    assert_utc_instant(result, expected)
    assert result.warning_flag is None


@pytest.mark.parametrize(
    "raw, reason",
    [
        (None, FieldReason.TIMESTAMP_MISSING),
        ("", FieldReason.TIMESTAMP_MISSING),
        ("   ", FieldReason.TIMESTAMP_MISSING),
        ("TBD", FieldReason.TIMESTAMP_PLACEHOLDER),
        ("2024-02-30T10:00:00Z", FieldReason.TIMESTAMP_INVALID),
        ("2024-03-05T24:00:00Z", FieldReason.TIMESTAMP_INVALID),
        ("2024-03-05T10:60:00Z", FieldReason.TIMESTAMP_INVALID),
        ("2024-03-05T10:00:00+25:00", FieldReason.TIMESTAMP_INVALID),
        ("yesterday afternoon", FieldReason.TIMESTAMP_UNPARSEABLE),
        ("2024-03-05", FieldReason.TIMESTAMP_UNPARSEABLE),
        ("2024-03-05T10:00Z", FieldReason.TIMESTAMP_UNPARSEABLE),
        ("05/03/24 08:30:00", FieldReason.TIMESTAMP_UNPARSEABLE),
        ("2024-03-05T10:00:00 PST", FieldReason.TIMESTAMP_UNPARSEABLE),
    ],
)
def test_invalid_timestamps_are_null_with_a_reason(raw, reason):
    result = athena(raw)

    assert result.cleaned_value is None
    assert result.reason_code == reason
    assert result.raw_value == raw


def test_impossible_meditech_date_is_invalid():
    assert meditech("31/02/2024 10:00:00").reason_code == FieldReason.TIMESTAMP_INVALID
