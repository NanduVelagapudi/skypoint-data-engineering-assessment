"""Patient fields for Task 3: name normalisation, sex, age band and ZIP3.

Names, DOB and full ZIP are PHI. These functions use them in memory only, to
build a linkage key, an age band and a ZIP3; nothing they return is meant to be
stored except the sex category, the age band and the ZIP3. ParseResult keeps
raw values out of repr().

Name normalisation (the brief's minimum linkage rule):
    last name    trim, upper-case, then drop every character that is not a
                 letter or digit (spaces, apostrophes, hyphens, periods)
    first name   trim, upper-case, keep only the first whitespace token, then
                 drop its punctuation. Later tokens, such as middle initials
                 ("A" or "A."), are ignored. The first token is kept even when
                 it is a single letter.
"""

from __future__ import annotations

import re
from datetime import date
from enum import StrEnum

from pipeline.parsers.result import FieldReason, ParseResult


class Sex(StrEnum):
    F = "F"
    M = "M"
    UNKNOWN = "UNKNOWN"


class AgeBand(StrEnum):
    AGE_0_17 = "0-17"
    AGE_18_39 = "18-39"
    AGE_40_64 = "40-64"
    AGE_65_PLUS = "65+"
    UNKNOWN = "UNKNOWN"


_SEX = {"F": Sex.F, "FEMALE": Sex.F, "M": Sex.M, "MALE": Sex.M}
_ZIP = re.compile(r"[0-9]{5}(?:-[0-9]{4})?", re.ASCII)


def _alnum(text: str) -> str:
    return "".join(char for char in text if char.isalnum())


def normalise_last_name(raw: str | None) -> ParseResult[str]:
    normalised = _alnum((raw or "").strip().upper())
    if not normalised:
        return ParseResult.invalid(raw, FieldReason.LAST_NAME_MISSING)
    return ParseResult.valid(raw, normalised)


def normalise_first_given_name(raw: str | None) -> ParseResult[str]:
    tokens = (raw or "").strip().upper().split()
    given = _alnum(tokens[0]) if tokens else ""
    if not given:
        return ParseResult.invalid(raw, FieldReason.FIRST_NAME_MISSING)
    return ParseResult.valid(raw, given)


def normalise_sex(raw: str | None) -> ParseResult[Sex]:
    """F/Female -> F, M/Male -> M. Anything else is UNKNOWN, flagged, and can never link."""
    text = (raw or "").strip()
    if not text:
        return ParseResult.valid(raw, Sex.UNKNOWN, FieldReason.SEX_MISSING)
    sex = _SEX.get(text.upper())
    if sex is None:
        return ParseResult.valid(raw, Sex.UNKNOWN, FieldReason.SEX_UNMAPPED)
    return ParseResult.valid(raw, sex)


def age_band(dob: date | None, admit: date | None) -> ParseResult[AgeBand]:
    """Age in whole years on the admit date. raw_value is None so the DOB is never carried."""
    if dob is None:
        return ParseResult.valid(None, AgeBand.UNKNOWN, FieldReason.AGE_BAND_DOB_UNAVAILABLE)
    if admit is None:
        return ParseResult.valid(None, AgeBand.UNKNOWN, FieldReason.AGE_BAND_ADMIT_UNAVAILABLE)
    if dob > admit:
        return ParseResult.valid(None, AgeBand.UNKNOWN, FieldReason.AGE_BAND_DOB_AFTER_ADMIT)
    age = admit.year - dob.year - ((admit.month, admit.day) < (dob.month, dob.day))
    if age <= 17:
        band = AgeBand.AGE_0_17
    elif age <= 39:
        band = AgeBand.AGE_18_39
    elif age <= 64:
        band = AgeBand.AGE_40_64
    else:
        band = AgeBand.AGE_65_PLUS
    return ParseResult.valid(None, band)


def zip3(raw: str | None) -> ParseResult[str]:
    """First three digits of a 5-digit (or ZIP+4) ZIP. The full ZIP is never returned."""
    text = (raw or "").strip()
    if not text:
        return ParseResult.invalid(raw, FieldReason.ZIP_MISSING)
    if not _ZIP.fullmatch(text):
        return ParseResult.invalid(raw, FieldReason.ZIP_INVALID)
    return ParseResult.valid(raw, text[:3])
