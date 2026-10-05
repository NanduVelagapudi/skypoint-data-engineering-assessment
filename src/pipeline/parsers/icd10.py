"""primary_dx_code -> ICD-10 code in the reference format (upper case, dot after the 3rd character).

Normalisation: strip, drop a trailing description after the first '-'
("E11.9 - Type 2 diabetes"), upper-case, and insert the dot after the third
character when it is missing and the code is longer than three characters
("e119" -> "E11.9"). ICD-10 codes never contain '-', so that split cannot cut
a code.

Four outcomes, as the brief requires:
    in the reference                 the code, no reason
    valid ICD-10 format, not in it   the code, warning flag DX_NOT_IN_REFERENCE
    legacy ICD-9 (numeric 3 digits,  NULL, DX_ICD9; never mapped to ICD-10
      optional .d or .dd)
    anything else                    NULL, DX_UNPARSEABLE (or DX_MISSING / DX_PLACEHOLDER)

ICD-9 V and E codes are not recognised as ICD-9: their shapes overlap valid
ICD-10 codes, so telling them apart would be a guess. None occur in the data.
The reference codes are passed in, so the parser does no file I/O.
"""

from __future__ import annotations

import re
from collections.abc import Set

from pipeline.parsers.result import FieldReason, ParseResult, is_placeholder

# Letter, digit, alphanumeric; then optionally a dot and 1-4 alphanumerics.
ICD10_CODE = re.compile(r"[A-Z][0-9][0-9A-Z](?:\.[0-9A-Z]{1,4})?", re.ASCII)
_ICD9_NUMERIC = re.compile(r"[0-9]{3}(?:\.[0-9]{1,2})?", re.ASCII)


def parse_diagnosis(raw: str | None, reference_codes: Set[str]) -> ParseResult[str]:
    text = (raw or "").strip()
    if not text:
        return ParseResult.invalid(raw, FieldReason.DX_MISSING)
    if is_placeholder(text):
        return ParseResult.invalid(raw, FieldReason.DX_PLACEHOLDER)

    code = text.split("-", 1)[0].strip().upper()
    if _ICD9_NUMERIC.fullmatch(code):
        return ParseResult.invalid(raw, FieldReason.DX_ICD9)
    if "." not in code and len(code) > 3:
        code = f"{code[:3]}.{code[3:]}"
    if not ICD10_CODE.fullmatch(code):
        return ParseResult.invalid(raw, FieldReason.DX_UNPARSEABLE)

    if code in reference_codes:
        return ParseResult.valid(raw, code)
    return ParseResult.valid(raw, code, FieldReason.DX_NOT_IN_REFERENCE)
