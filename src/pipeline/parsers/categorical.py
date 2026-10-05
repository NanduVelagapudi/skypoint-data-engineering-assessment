"""encounter_type, claim_status and payer_name -> canonical categories.

The brief fixes the target categories; the source spellings below are every
value observed in batches 001-003, mapped as recorded in DECISIONS.md. All
mappings live here, in one table per field, and nowhere else.

Spellings are matched on a normalised key: upper case, runs of whitespace
collapsed, and no spaces around hyphens ("Closed - Paid" == "CLOSED-PAID").
Nothing else is guessed:
    blank encounter_type / payer   UNKNOWN, with a *_MISSING warning flag
    blank claim_status             NULL, CLAIM_STATUS_MISSING (no UNKNOWN status exists)
    any spelling not in the table  NULL, *_UNMAPPED; never put in UNKNOWN or a category
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from enum import StrEnum

from pipeline.parsers.result import FieldReason, ParseResult


class EncounterType(StrEnum):
    INPATIENT = "INPATIENT"
    OBSERVATION = "OBSERVATION"
    EMERGENCY = "EMERGENCY"
    OUTPATIENT = "OUTPATIENT"
    TELEHEALTH = "TELEHEALTH"
    URGENT_CARE = "URGENT_CARE"
    UNKNOWN = "UNKNOWN"


class ClaimStatus(StrEnum):
    SUBMITTED = "SUBMITTED"
    PAID = "PAID"
    DENIED = "DENIED"
    VOID = "VOID"


class PayerCategory(StrEnum):
    MEDICARE = "MEDICARE"
    MEDICAID = "MEDICAID"
    COMMERCIAL = "COMMERCIAL"
    SELF_PAY = "SELF_PAY"
    OTHER = "OTHER"
    UNKNOWN = "UNKNOWN"


def normalise_key(text: str) -> str:
    """Case- and spacing-insensitive lookup key for a categorical spelling."""
    return re.sub(r"\s*-\s*", "-", " ".join(text.split())).upper()


def _table(spellings: Mapping[StrEnum, Sequence[str]]) -> dict[str, StrEnum]:
    table: dict[str, StrEnum] = {}
    for category, values in spellings.items():
        for value in values:
            key = normalise_key(value)
            if table.get(key, category) != category:
                raise ValueError(f"spelling {value!r} maps to two categories")
            table[key] = category
    return table


ENCOUNTER_TYPES = _table(
    {
        EncounterType.EMERGENCY: ("ED", "ER", "Emergency", "Emergency Room", "EMERGENCY DEPT"),
        EncounterType.INPATIENT: ("Inpatient", "IP", "INPT", "IN-PATIENT", "inpatient admission"),
        EncounterType.OBSERVATION: ("Observation", "OBS", "Obs Stay"),
        EncounterType.OUTPATIENT: ("Outpatient", "OP", "Out Patient", "Office Visit", "Clinic Visit"),
        EncounterType.TELEHEALTH: ("Telehealth", "TELEMED", "Video Visit", "Virtual"),
        EncounterType.URGENT_CARE: ("Urgent Care", "UC", "Walk-in Urgent"),
        EncounterType.UNKNOWN: ("OTHER",),
    }
)

CLAIM_STATUSES = _table(
    {
        ClaimStatus.PAID: ("Paid", "Paid in Full", "Closed-Paid"),
        ClaimStatus.DENIED: ("Denied", "Rejected"),
        ClaimStatus.SUBMITTED: ("Submitted", "Pending", "In Process"),
        ClaimStatus.VOID: ("Void", "VOIDED", "Cancelled"),
    }
)

PAYER_CATEGORIES = _table(
    {
        PayerCategory.MEDICARE: ("Medicare", "MCR", "Medicare Part A", "Medicare - Part B"),
        PayerCategory.MEDICAID: ("Medicaid", "IA Medicaid", "State Medicaid Plan", "BadgerCare Plus"),
        PayerCategory.COMMERCIAL: (
            "Aetna",
            "Aetna Inc",
            "Cigna",
            "Cigna Health",
            "UnitedHealthcare",
            "UHC",
            "BCBS",
            "Blue Cross Blue Shield",
        ),
        PayerCategory.SELF_PAY: ("Self Pay", "Self-Pay", "SelfPay", "Uninsured"),
        PayerCategory.OTHER: ("TRICARE", "Workers Comp"),
        PayerCategory.UNKNOWN: ("N/A",),
    }
)


def _lookup(
    raw: str | None,
    table: Mapping[str, StrEnum],
    unmapped: FieldReason,
    missing: FieldReason,
    blank_category: StrEnum | None,
) -> ParseResult[StrEnum]:
    text = (raw or "").strip()
    if not text:
        if blank_category is None:
            return ParseResult.invalid(raw, missing)
        return ParseResult.valid(raw, blank_category, missing)
    category = table.get(normalise_key(text))
    if category is None:
        return ParseResult.invalid(raw, unmapped)
    return ParseResult.valid(raw, category)


def parse_encounter_type(raw: str | None) -> ParseResult[EncounterType]:
    return _lookup(
        raw,
        ENCOUNTER_TYPES,
        FieldReason.ENCOUNTER_TYPE_UNMAPPED,
        FieldReason.ENCOUNTER_TYPE_MISSING,
        EncounterType.UNKNOWN,
    )


def parse_claim_status(raw: str | None) -> ParseResult[ClaimStatus]:
    return _lookup(raw, CLAIM_STATUSES, FieldReason.CLAIM_STATUS_UNMAPPED, FieldReason.CLAIM_STATUS_MISSING, None)


def parse_payer_category(raw: str | None) -> ParseResult[PayerCategory]:
    """payer_category from payer_name. The raw payer name stays in raw_value for the caller to keep."""
    return _lookup(
        raw,
        PAYER_CATEGORIES,
        FieldReason.PAYER_UNMAPPED,
        FieldReason.PAYER_MISSING,
        PayerCategory.UNKNOWN,
    )
