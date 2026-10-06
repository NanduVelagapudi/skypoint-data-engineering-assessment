"""The common parser result type and the central list of field reason codes."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Generic, TypeVar

T = TypeVar("T")


class FieldReason(StrEnum):
    """Why a field value is NULL, or what is notable about a valid value.

    These say what happened, not how bad it is: Task 6 maps each code to a
    severity. Batch-level rejection codes live separately in errors.ReasonCode.
    """

    AMOUNT_MISSING = "AMOUNT_MISSING"
    AMOUNT_PLACEHOLDER = "AMOUNT_PLACEHOLDER"
    AMOUNT_SPREADSHEET_ERROR = "AMOUNT_SPREADSHEET_ERROR"
    AMOUNT_UNPARSEABLE = "AMOUNT_UNPARSEABLE"
    AMOUNT_FRACTIONAL_CENTS = "AMOUNT_FRACTIONAL_CENTS"
    AMOUNT_K_SUFFIX_NOT_ALLOWED = "AMOUNT_K_SUFFIX_NOT_ALLOWED"

    DATE_MISSING = "DATE_MISSING"
    DATE_PLACEHOLDER = "DATE_PLACEHOLDER"
    DATE_UNPARSEABLE = "DATE_UNPARSEABLE"
    DATE_INVALID = "DATE_INVALID"
    DATE_CENTURY_UNKNOWN = "DATE_CENTURY_UNKNOWN"
    DATE_AFTER_DELIVERY = "DATE_AFTER_DELIVERY"
    DISCHARGE_BEFORE_ADMIT = "DISCHARGE_BEFORE_ADMIT"
    LENGTH_OF_STAY_ADMIT_UNAVAILABLE = "LENGTH_OF_STAY_ADMIT_UNAVAILABLE"
    LENGTH_OF_STAY_DISCHARGE_UNAVAILABLE = "LENGTH_OF_STAY_DISCHARGE_UNAVAILABLE"

    TIMESTAMP_MISSING = "TIMESTAMP_MISSING"
    TIMESTAMP_PLACEHOLDER = "TIMESTAMP_PLACEHOLDER"
    TIMESTAMP_UNPARSEABLE = "TIMESTAMP_UNPARSEABLE"
    TIMESTAMP_INVALID = "TIMESTAMP_INVALID"
    TIMESTAMP_NONEXISTENT_LOCAL_TIME = "TIMESTAMP_NONEXISTENT_LOCAL_TIME"
    TIMESTAMP_AMBIGUOUS_LOCAL_TIME = "TIMESTAMP_AMBIGUOUS_LOCAL_TIME"

    ENCOUNTER_TYPE_MISSING = "ENCOUNTER_TYPE_MISSING"
    ENCOUNTER_TYPE_UNMAPPED = "ENCOUNTER_TYPE_UNMAPPED"
    CLAIM_STATUS_MISSING = "CLAIM_STATUS_MISSING"
    CLAIM_STATUS_UNMAPPED = "CLAIM_STATUS_UNMAPPED"
    PAYER_MISSING = "PAYER_MISSING"
    PAYER_UNMAPPED = "PAYER_UNMAPPED"

    DX_MISSING = "DX_MISSING"
    DX_PLACEHOLDER = "DX_PLACEHOLDER"
    DX_ICD9 = "DX_ICD9"
    DX_UNPARSEABLE = "DX_UNPARSEABLE"
    DX_NOT_IN_REFERENCE = "DX_NOT_IN_REFERENCE"

    NPI_MISSING = "NPI_MISSING"
    NPI_PLACEHOLDER = "NPI_PLACEHOLDER"
    NPI_INVALID_FORMAT = "NPI_INVALID_FORMAT"
    NPI_CHECKSUM_FAILED = "NPI_CHECKSUM_FAILED"
    NPI_NOT_IN_ROSTER = "NPI_NOT_IN_ROSTER"
    # Point-in-time provider lookup (dim_provider): why an encounter has no provider row.
    PROVIDER_ADMIT_DATE_UNKNOWN = "PROVIDER_ADMIT_DATE_UNKNOWN"
    PROVIDER_NOT_ON_ROSTER_AT_ADMIT = "PROVIDER_NOT_ON_ROSTER_AT_ADMIT"

    FACILITY_MISSING = "FACILITY_MISSING"
    FACILITY_UNRESOLVED = "FACILITY_UNRESOLVED"

    LAST_NAME_MISSING = "LAST_NAME_MISSING"
    FIRST_NAME_MISSING = "FIRST_NAME_MISSING"
    SEX_MISSING = "SEX_MISSING"
    SEX_UNMAPPED = "SEX_UNMAPPED"
    AGE_BAND_DOB_UNAVAILABLE = "AGE_BAND_DOB_UNAVAILABLE"
    AGE_BAND_ADMIT_UNAVAILABLE = "AGE_BAND_ADMIT_UNAVAILABLE"
    AGE_BAND_DOB_AFTER_ADMIT = "AGE_BAND_DOB_AFTER_ADMIT"
    ZIP_MISSING = "ZIP_MISSING"
    ZIP_INVALID = "ZIP_INVALID"
    PATIENT_MRN_MISSING = "PATIENT_MRN_MISSING"
    PATIENT_UNLINKED_NO_DOB = "PATIENT_UNLINKED_NO_DOB"
    PATIENT_UNLINKED_INCOMPLETE = "PATIENT_UNLINKED_INCOMPLETE"
    PATIENT_UNLINKED_CONFLICT = "PATIENT_UNLINKED_CONFLICT"


# Placeholder words seen in the data pack or named in the brief. Compared after
# stripping and upper-casing. Anything else that does not parse is UNPARSEABLE.
PLACEHOLDER_TOKENS = frozenset({"N/A", "PENDING", "TBD"})


@dataclass(frozen=True)
class ParseResult(Generic[T]):
    """One parsed field.

    Exactly one of cleaned_value and reason_code is set. warning_flag may be
    set on a valid value (for example an ambiguous local time). raw_value and
    cleaned_value are left out of repr() so a logged or asserted result never
    prints a patient value.
    """

    raw_value: str | None = field(repr=False)
    cleaned_value: T | None = field(repr=False)
    reason_code: FieldReason | None = None
    warning_flag: FieldReason | None = None

    def __post_init__(self) -> None:
        if (self.cleaned_value is None) == (self.reason_code is None):
            raise ValueError("a ParseResult needs a cleaned value or a reason code, not both")

    @property
    def is_valid(self) -> bool:
        return self.reason_code is None

    @classmethod
    def valid(cls, raw_value: str | None, cleaned_value: T, warning_flag: FieldReason | None = None) -> ParseResult[T]:
        return cls(raw_value, cleaned_value, None, warning_flag)

    @classmethod
    def invalid(cls, raw_value: str | None, reason_code: FieldReason) -> ParseResult[T]:
        return cls(raw_value, None, reason_code)


def is_placeholder(text: str) -> bool:
    return text.strip().upper() in PLACEHOLDER_TOKENS
