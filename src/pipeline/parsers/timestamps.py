"""last_updated_ts -> timezone-aware datetime in UTC.

Accepted shapes, each matched in full:
    ISO       2024-03-05T14:30:00Z   2024-03-05T14:30:00-05:00   2024-03-05 14:30:00
              ('T' or one space between date and time)
    numeric   05/03/2024 14:30:00    (read with the source's date order; 4-digit year)

An explicit Z or +/-HH:MM offset in the value wins. A value without one is
local wall-clock time in the source's reference time zone (UTC for Epic and
Athena, America/Chicago for Meditech).

Daylight-saving edges in the source zone:
    ambiguous (fall-back hour occurs twice)   the earlier instant, flagged
                                              TIMESTAMP_AMBIGUOUS_LOCAL_TIME
    nonexistent (spring-forward gap)          NULL, TIMESTAMP_NONEXISTENT_LOCAL_TIME
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from pipeline.parsers.result import FieldReason, ParseResult, is_placeholder
from pipeline.source_conventions import DateOrder

_ISO = re.compile(
    r"([0-9]{4})-([0-9]{2})-([0-9]{2})[T ]([0-9]{2}):([0-9]{2}):([0-9]{2})"
    r"(Z|[+-][0-9]{2}:[0-9]{2})?",
    re.ASCII | re.IGNORECASE,
)
_NUMERIC = re.compile(
    r"([0-9]{1,2})([/-])([0-9]{1,2})\2([0-9]{4}) ([0-9]{2}):([0-9]{2}):([0-9]{2})",
    re.ASCII,
)


def parse_timestamp(raw: str | None, *, timezone_name: str, date_order: DateOrder) -> ParseResult[datetime]:
    """Parse one timestamp; `timezone_name` is the source's IANA zone for values without an offset."""
    text = (raw or "").strip()
    if not text:
        return ParseResult.invalid(raw, FieldReason.TIMESTAMP_MISSING)
    if is_placeholder(text):
        return ParseResult.invalid(raw, FieldReason.TIMESTAMP_PLACEHOLDER)

    offset_text = None
    if m := _ISO.fullmatch(text):
        year, month, day, hour, minute, second, offset_text = m.groups()
    elif m := _NUMERIC.fullmatch(text):
        first, _, second_part, year, hour, minute, second = m.groups()
        month, day = (first, second_part) if date_order == DateOrder.MDY else (second_part, first)
    else:
        return ParseResult.invalid(raw, FieldReason.TIMESTAMP_UNPARSEABLE)

    try:
        wall_clock = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))
    except ValueError:  # impossible date or time of day
        return ParseResult.invalid(raw, FieldReason.TIMESTAMP_INVALID)

    if offset_text:
        offset = _offset(offset_text)
        if offset is None:
            return ParseResult.invalid(raw, FieldReason.TIMESTAMP_INVALID)
        return ParseResult.valid(raw, wall_clock.replace(tzinfo=offset).astimezone(UTC))

    zone = ZoneInfo(timezone_name)
    local = wall_clock.replace(tzinfo=zone)  # fold=0: the earlier of two possible instants
    if local.astimezone(UTC).astimezone(zone).replace(tzinfo=None) != wall_clock:
        return ParseResult.invalid(raw, FieldReason.TIMESTAMP_NONEXISTENT_LOCAL_TIME)
    ambiguous = local.utcoffset() != local.replace(fold=1).utcoffset()
    warning = FieldReason.TIMESTAMP_AMBIGUOUS_LOCAL_TIME if ambiguous else None
    return ParseResult.valid(raw, local.astimezone(UTC), warning)


def _offset(text: str) -> timezone | None:
    if text.upper() == "Z":
        return UTC
    hours, minutes = int(text[1:3]), int(text[4:6])
    if hours > 23 or minutes > 59:
        return None
    sign = -1 if text[0] == "-" else 1
    return timezone(sign * timedelta(hours=hours, minutes=minutes))
