"""attending_npi -> validated 10-digit NPI string.

Harmless formatting is cleaned: surrounding whitespace, and a trailing ".0"
left by a spreadsheet that stored the NPI as a number. Nothing else is
removed: prefixes, separators or letters make the value NPI_INVALID_FORMAT.

Validation follows CMS: exactly 10 digits, and the 10th digit must be the Luhn
check digit of the first nine digits prefixed with 80840.

Whether a valid NPI appears in a provider roster is a separate question, and
npi_not_in_roster() answers it; the parser never assumes a valid NPI belongs
to a provider.
"""

from __future__ import annotations

import re
from collections.abc import Set

from pipeline.parsers.result import FieldReason, ParseResult, is_placeholder

_TEN_DIGITS = re.compile(r"[0-9]{10}", re.ASCII)
NPI_PREFIX = "80840"  # CMS: health-industry prefix used only for the check digit


def npi_check_digit(first_nine: str) -> int:
    """Luhn check digit for an NPI's first nine digits, computed over 80840 + those digits."""
    total = 0
    # Walking right to left, the digit next to where the check digit goes is doubled.
    for position, char in enumerate(reversed(NPI_PREFIX + first_nine)):
        digit = int(char)
        if position % 2 == 0:
            digit *= 2
            if digit > 9:
                digit -= 9
        total += digit
    return (10 - total % 10) % 10


def parse_npi(raw: str | None) -> ParseResult[str]:
    text = (raw or "").strip()
    if not text:
        return ParseResult.invalid(raw, FieldReason.NPI_MISSING)
    if is_placeholder(text):
        return ParseResult.invalid(raw, FieldReason.NPI_PLACEHOLDER)

    if text.endswith(".0"):
        text = text[:-2]
    if not _TEN_DIGITS.fullmatch(text):
        return ParseResult.invalid(raw, FieldReason.NPI_INVALID_FORMAT)
    if npi_check_digit(text[:9]) != int(text[9]):
        return ParseResult.invalid(raw, FieldReason.NPI_CHECKSUM_FAILED)
    return ParseResult.valid(raw, text)


def npi_not_in_roster(npi: str | None, roster_npis: Set[str]) -> FieldReason | None:
    """NPI_NOT_IN_ROSTER for a valid NPI found in none of the roster snapshots passed in.

    Kept apart from parse_npi: the NPI stays valid; this only reports that no
    roster knows it. Point-in-time provider lookup is a later step.
    """
    if npi is not None and npi not in roster_npis:
        return FieldReason.NPI_NOT_IN_ROSTER
    return None
