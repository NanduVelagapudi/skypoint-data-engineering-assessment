"""Categorical mapping tests. The spellings are the category labels observed in batches 001-003."""

import pytest

from pipeline.parsers.categorical import (
    CLAIM_STATUSES,
    ENCOUNTER_TYPES,
    PAYER_CATEGORIES,
    ClaimStatus,
    EncounterType,
    PayerCategory,
    normalise_key,
    parse_claim_status,
    parse_encounter_type,
    parse_payer_category,
)
from pipeline.parsers.result import FieldReason

OBSERVED_ENCOUNTER_TYPES = {
    "ED": EncounterType.EMERGENCY,
    "ER": EncounterType.EMERGENCY,
    "Emergency": EncounterType.EMERGENCY,
    "Emergency Room": EncounterType.EMERGENCY,
    "EMERGENCY DEPT": EncounterType.EMERGENCY,
    "Inpatient": EncounterType.INPATIENT,
    "IP": EncounterType.INPATIENT,
    "INPT": EncounterType.INPATIENT,
    "IN-PATIENT": EncounterType.INPATIENT,
    "inpatient admission": EncounterType.INPATIENT,
    "Observation": EncounterType.OBSERVATION,
    "OBS": EncounterType.OBSERVATION,
    "Obs Stay": EncounterType.OBSERVATION,
    "Outpatient": EncounterType.OUTPATIENT,
    "OP": EncounterType.OUTPATIENT,
    "Out Patient": EncounterType.OUTPATIENT,
    "Office Visit": EncounterType.OUTPATIENT,
    "Clinic Visit": EncounterType.OUTPATIENT,
    "Telehealth": EncounterType.TELEHEALTH,
    "TELEMED": EncounterType.TELEHEALTH,
    "Video Visit": EncounterType.TELEHEALTH,
    "Virtual": EncounterType.TELEHEALTH,
    "Urgent Care": EncounterType.URGENT_CARE,
    "URGENT CARE": EncounterType.URGENT_CARE,
    "UC": EncounterType.URGENT_CARE,
    "Walk-in Urgent": EncounterType.URGENT_CARE,
    "OTHER": EncounterType.UNKNOWN,
}

OBSERVED_CLAIM_STATUSES = {
    "Paid in Full": ClaimStatus.PAID,
    "Closed - Paid": ClaimStatus.PAID,
    "Paid": ClaimStatus.PAID,
    "PAID": ClaimStatus.PAID,
    "Denied": ClaimStatus.DENIED,
    "DENIED": ClaimStatus.DENIED,
    "Rejected": ClaimStatus.DENIED,
    "Submitted": ClaimStatus.SUBMITTED,
    "SUBMITTED": ClaimStatus.SUBMITTED,
    "Pending": ClaimStatus.SUBMITTED,
    "In Process": ClaimStatus.SUBMITTED,
    "Void": ClaimStatus.VOID,
    "VOIDED": ClaimStatus.VOID,
    "CANCELLED": ClaimStatus.VOID,
    "Cancelled": ClaimStatus.VOID,
}

OBSERVED_PAYERS = {
    "Medicare": PayerCategory.MEDICARE,
    "MCR": PayerCategory.MEDICARE,
    "MEDICARE PART A": PayerCategory.MEDICARE,
    "Medicare - Part B": PayerCategory.MEDICARE,
    "Medicaid": PayerCategory.MEDICAID,
    "MEDICAID": PayerCategory.MEDICAID,
    "IA Medicaid": PayerCategory.MEDICAID,
    "State Medicaid Plan": PayerCategory.MEDICAID,
    "BadgerCare Plus": PayerCategory.MEDICAID,
    "Aetna": PayerCategory.COMMERCIAL,
    "AETNA INC": PayerCategory.COMMERCIAL,
    "Cigna": PayerCategory.COMMERCIAL,
    "CIGNA HEALTH": PayerCategory.COMMERCIAL,
    "UnitedHealthcare": PayerCategory.COMMERCIAL,
    "UHC": PayerCategory.COMMERCIAL,
    "BCBS": PayerCategory.COMMERCIAL,
    "Blue Cross Blue Shield": PayerCategory.COMMERCIAL,
    "Self Pay": PayerCategory.SELF_PAY,
    "self-pay": PayerCategory.SELF_PAY,
    "SELFPAY": PayerCategory.SELF_PAY,
    "Uninsured": PayerCategory.SELF_PAY,
    "TRICARE": PayerCategory.OTHER,
    "Tricare": PayerCategory.OTHER,
    "Workers Comp": PayerCategory.OTHER,
    "N/A": PayerCategory.UNKNOWN,
}


# --- every observed spelling ---


@pytest.mark.parametrize("raw, expected", OBSERVED_ENCOUNTER_TYPES.items())
def test_every_observed_encounter_type(raw, expected):
    result = parse_encounter_type(raw)

    assert (result.cleaned_value, result.reason_code, result.warning_flag) == (expected, None, None)
    assert result.raw_value == raw


@pytest.mark.parametrize("raw, expected", OBSERVED_CLAIM_STATUSES.items())
def test_every_observed_claim_status(raw, expected):
    result = parse_claim_status(raw)

    assert (result.cleaned_value, result.reason_code, result.warning_flag) == (expected, None, None)


@pytest.mark.parametrize("raw, expected", OBSERVED_PAYERS.items())
def test_every_observed_payer(raw, expected):
    result = parse_payer_category(raw)

    assert (result.cleaned_value, result.reason_code, result.warning_flag) == (expected, None, None)
    assert result.raw_value == raw  # raw payer name kept


# --- normalisation ---


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("  er  ", EncounterType.EMERGENCY),
        ("emergency   room", EncounterType.EMERGENCY),
        ("In - Patient", EncounterType.INPATIENT),
        ("INPATIENT ADMISSION", EncounterType.INPATIENT),
        ("obs\tstay", EncounterType.OBSERVATION),
        ("walk-IN urgent", EncounterType.URGENT_CARE),
        ("other", EncounterType.UNKNOWN),
    ],
)
def test_encounter_type_ignores_case_and_spacing(raw, expected):
    assert parse_encounter_type(raw).cleaned_value == expected


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("Closed-Paid", ClaimStatus.PAID),
        ("closed  -  paid", ClaimStatus.PAID),
        (" paid in full ", ClaimStatus.PAID),
        ("in process", ClaimStatus.SUBMITTED),
        ("voided", ClaimStatus.VOID),
        ("cancelled", ClaimStatus.VOID),
    ],
)
def test_claim_status_ignores_case_and_spacing(raw, expected):
    assert parse_claim_status(raw).cleaned_value == expected


def test_normalised_key():
    assert normalise_key("  Closed  -  Paid ") == "CLOSED-PAID"
    assert normalise_key("Medicare - Part B") == "MEDICARE-PART B"
    assert normalise_key("obs\tstay") == "OBS STAY"


# --- blanks and unmapped values ---


@pytest.mark.parametrize("raw", [None, "", "   "])
def test_blank_encounter_type_is_unknown_with_a_missing_flag(raw):
    result = parse_encounter_type(raw)

    assert result.cleaned_value == EncounterType.UNKNOWN
    assert result.reason_code is None
    assert result.warning_flag == FieldReason.ENCOUNTER_TYPE_MISSING


@pytest.mark.parametrize("raw", [None, "", "   "])
def test_blank_payer_is_unknown_with_a_missing_flag(raw):
    result = parse_payer_category(raw)

    assert result.cleaned_value == PayerCategory.UNKNOWN
    assert result.warning_flag == FieldReason.PAYER_MISSING


@pytest.mark.parametrize("raw", [None, "", "   "])
def test_blank_claim_status_is_null_because_there_is_no_unknown_status(raw):
    result = parse_claim_status(raw)

    assert result.cleaned_value is None
    assert result.reason_code == FieldReason.CLAIM_STATUS_MISSING


@pytest.mark.parametrize(
    "parse, raw, reason",
    [
        (parse_encounter_type, "Home Visit", FieldReason.ENCOUNTER_TYPE_UNMAPPED),
        (parse_encounter_type, "Emergent", FieldReason.ENCOUNTER_TYPE_UNMAPPED),
        (parse_encounter_type, "N/A", FieldReason.ENCOUNTER_TYPE_UNMAPPED),
        (parse_claim_status, "Partially Paid", FieldReason.CLAIM_STATUS_UNMAPPED),
        (parse_claim_status, "Appealed", FieldReason.CLAIM_STATUS_UNMAPPED),
        (parse_payer_category, "Humana", FieldReason.PAYER_UNMAPPED),
        (parse_payer_category, "Medicare Advantage", FieldReason.PAYER_UNMAPPED),
    ],
)
def test_unmapped_values_are_null_never_a_category(parse, raw, reason):
    result = parse(raw)

    assert result.cleaned_value is None
    assert result.reason_code == reason
    assert result.raw_value == raw


# --- table integrity ---


def test_tables_only_produce_the_briefs_categories():
    assert set(ENCOUNTER_TYPES.values()) == set(EncounterType)
    assert set(CLAIM_STATUSES.values()) == set(ClaimStatus)
    assert set(PAYER_CATEGORIES.values()) == set(PayerCategory)
