"""billed_amount -> Decimal USD with two decimal places.

Accepted shapes (after stripping whitespace), with at most one currency marker:
    1234.5   1,234.50   $1,234.50   $ 1234.50   USD 1,234.50   1,234.50 USD   $1.25K
Thousands separators must be correctly grouped. Nothing is rounded: a value
with fractions of a cent is NULL. Invalid text is NULL with a reason, never 0.

Units come from the source system's reference conventions:
    USD        the number is dollars; a K suffix multiplies by 1,000
    USD_CENTS  the number is an integer count of cents (a trailing .0 is fine);
               a K suffix or a currency marker is refused, because either one
               leaves it unclear whether the number is cents or dollars
Signs and parentheses are not accepted: the data pack has none and the brief
gives no rule for them.
"""

from __future__ import annotations

import re
from decimal import Decimal

from pipeline.parsers.result import FieldReason, ParseResult, is_placeholder
from pipeline.source_conventions import AmountUnit

CENT = Decimal("0.01")

# Literal error values a spreadsheet writes into a cell.
SPREADSHEET_ERRORS = frozenset(
    {"#NULL!", "#DIV/0!", "#VALUE!", "#REF!", "#NAME?", "#NUM!", "#N/A", "#SPILL!", "#CALC!"}
)

_AMOUNT = re.compile(
    r"""
    (?:(?P<usd_prefix>USD)\s*|(?P<dollar>\$)\s*)?
    (?P<integer>[0-9]{1,3}(?:,[0-9]{3})+|[0-9]+)
    (?:\.(?P<fraction>[0-9]+))?
    (?P<k>\s*K)?
    (?:\s*(?P<usd_suffix>USD))?
    """,
    re.VERBOSE | re.IGNORECASE | re.ASCII,
)


def parse_amount(raw: str | None, amount_unit: AmountUnit) -> ParseResult[Decimal]:
    text = (raw or "").strip()
    if not text:
        return ParseResult.invalid(raw, FieldReason.AMOUNT_MISSING)
    if is_placeholder(text):
        return ParseResult.invalid(raw, FieldReason.AMOUNT_PLACEHOLDER)
    if text.upper() in SPREADSHEET_ERRORS:
        return ParseResult.invalid(raw, FieldReason.AMOUNT_SPREADSHEET_ERROR)

    match = _AMOUNT.fullmatch(text)
    markers = sum(bool(match and match.group(g)) for g in ("usd_prefix", "dollar", "usd_suffix"))
    if match is None or markers > 1:
        return ParseResult.invalid(raw, FieldReason.AMOUNT_UNPARSEABLE)

    number = Decimal(match.group("integer").replace(",", "") + "." + (match.group("fraction") or "0"))

    if amount_unit == AmountUnit.USD_CENTS:
        if match.group("k"):
            return ParseResult.invalid(raw, FieldReason.AMOUNT_K_SUFFIX_NOT_ALLOWED)
        if markers:
            return ParseResult.invalid(raw, FieldReason.AMOUNT_UNPARSEABLE)
        if number != number.to_integral_value():
            return ParseResult.invalid(raw, FieldReason.AMOUNT_FRACTIONAL_CENTS)
        dollars = number / 100
    else:
        dollars = number * 1000 if match.group("k") else number

    if dollars != dollars.quantize(CENT):
        return ParseResult.invalid(raw, FieldReason.AMOUNT_FRACTIONAL_CENTS)
    return ParseResult.valid(raw, dollars.quantize(CENT))
