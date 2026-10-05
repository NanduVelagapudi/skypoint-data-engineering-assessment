"""admit_date, discharge_date and patient_dob -> datetime.date.

Accepted shapes, each matched in full:
    ISO          2024-03-05   2024-03-05T14:30:00 (the time part is dropped)
    numeric      03/05/2024   3/5/2024   03-05-2024   03-05-24
                 read with the source's date order (MDY or DMY); both
                 separators must be the same
    month name   Mar 5, 2024   March 05, 2024   5 Mar 2024   05 March 2024
                 unambiguous, so the date order is not used

A two-digit year uses the century the source's reference notes state (Athena:
20xx); without a stated rule it is NULL with DATE_CENTURY_UNKNOWN. An
impossible date (02/30/2024, month 13) is never swapped or repaired.

A date after the batch's delivery date is NULL with DATE_AFTER_DELIVERY. The
cutoff is delivered_at's UTC calendar date and is passed in by the caller.

patient_dob is parsed with the same function, but only for use in memory
(age band, patient linkage). Callers must never persist the parsed DOB.

A discharge before admission is not a parsing failure: discharge_date stays as
parsed, and discharge_before_admit() reports the problem separately.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime

from pipeline.parsers.result import FieldReason, ParseResult, is_placeholder
from pipeline.source_conventions import DateOrder

_MONTHS = {
    name: number
    for number, names in enumerate(
        [
            ("jan", "january"),
            ("feb", "february"),
            ("mar", "march"),
            ("apr", "april"),
            ("may",),
            ("jun", "june"),
            ("jul", "july"),
            ("aug", "august"),
            ("sep", "sept", "september"),
            ("oct", "october"),
            ("nov", "november"),
            ("dec", "december"),
        ],
        start=1,
    )
    for name in names
}

_ISO = re.compile(r"([0-9]{4})-([0-9]{2})-([0-9]{2})(?:T([0-9]{2}):([0-9]{2}):([0-9]{2}))?", re.ASCII)
_NUMERIC = re.compile(r"([0-9]{1,2})([/-])([0-9]{1,2})\2([0-9]{4}|[0-9]{2})", re.ASCII)
_MONTH_FIRST = re.compile(r"([A-Za-z]+)\.? ([0-9]{1,2}),? ([0-9]{4})", re.ASCII)
_DAY_FIRST = re.compile(r"([0-9]{1,2}) ([A-Za-z]+)\.? ([0-9]{4})", re.ASCII)


@dataclass(frozen=True)
class Period:
    year: int
    quarter: int
    month: int


def parse_date(
    raw: str | None,
    *,
    date_order: DateOrder,
    delivered_at: datetime,
    two_digit_year_century: int | None,
) -> ParseResult[date]:
    """Parse one date field. `delivered_at` must be timezone-aware."""
    if delivered_at.tzinfo is None:
        raise ValueError("delivered_at must be timezone-aware")  # a caller bug, not bad data
    text = (raw or "").strip()
    if not text:
        return ParseResult.invalid(raw, FieldReason.DATE_MISSING)
    if is_placeholder(text):
        return ParseResult.invalid(raw, FieldReason.DATE_PLACEHOLDER)

    parts = _split(text, date_order, two_digit_year_century)
    if isinstance(parts, FieldReason):
        return ParseResult.invalid(raw, parts)

    try:
        parsed = date(*parts)
    except ValueError:  # impossible calendar date
        return ParseResult.invalid(raw, FieldReason.DATE_INVALID)

    if parsed > delivered_at.astimezone(UTC).date():
        return ParseResult.invalid(raw, FieldReason.DATE_AFTER_DELIVERY)
    return ParseResult.valid(raw, parsed)


def _split(text: str, date_order: DateOrder, century: int | None) -> tuple[int, int, int] | FieldReason:
    """(year, month, day) from the matching format, or the reason none fits."""
    if m := _ISO.fullmatch(text):
        year, month, day, hour, minute, second = m.groups()
        if hour is not None and not (int(hour) < 24 and int(minute) < 60 and int(second) < 60):
            return FieldReason.DATE_INVALID
        return int(year), int(month), int(day)

    if m := _NUMERIC.fullmatch(text):
        first, _, second, year = m.groups()
        month, day = (first, second) if date_order == DateOrder.MDY else (second, first)
        if len(year) == 2:
            if century is None:
                return FieldReason.DATE_CENTURY_UNKNOWN
            return century + int(year), int(month), int(day)
        return int(year), int(month), int(day)

    if m := _MONTH_FIRST.fullmatch(text):
        name, day, year = m.groups()
    elif m := _DAY_FIRST.fullmatch(text):
        day, name, year = m.groups()
    else:
        return FieldReason.DATE_UNPARSEABLE
    month = _MONTHS.get(name.lower())
    if month is None:
        return FieldReason.DATE_UNPARSEABLE
    return int(year), month, int(day)


def discharge_before_admit(admit: date | None, discharge: date | None) -> FieldReason | None:
    """DISCHARGE_BEFORE_ADMIT when both dates are known and discharge is earlier.

    Kept apart from parse_date: neither date is nulled, the row is only flagged.
    """
    if admit is not None and discharge is not None and discharge < admit:
        return FieldReason.DISCHARGE_BEFORE_ADMIT
    return None


def admit_period(admit: date) -> Period:
    """Year, quarter and month derived from a parsed admit_date."""
    return Period(year=admit.year, quarter=(admit.month - 1) // 3 + 1, month=admit.month)
