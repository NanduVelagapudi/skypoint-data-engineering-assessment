"""Task 6 group (a): DQ check catalog, reason-code classification, clean.version_dq_issues and ops.dq_report.

Unit tests need no database. The integration tests run synthetic batches
through main() into tmp_path with the real reference files (read only);
synthetic patient values are obviously fake. Real-data pins and parity live
in test_incremental_parity.py.
"""

import hashlib
import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from conftest import REAL_DATA_DIR, REPO_ROOT, attach, epic_file, synthetic_file, write_batch

from pipeline import dq_report, dq_rules, version_dq
from pipeline.batch_processor import DUPLICATE_FILE
from pipeline.clean_patients import PHI_COLUMNS
from pipeline.dimensions import ProviderLookup, build_provider_rows
from pipeline.dq_rules import CHECKS, CHECKS_BY_CODE, CODE_MAP, Classification, Kind, Scope, Severity
from pipeline.encounter_facts import length_of_stay
from pipeline.encounter_history import OutcomeReason
from pipeline.errors import PipelineError, ReasonCode
from pipeline.main import main
from pipeline.parsers.patient import Sex, age_band, normalise_sex, zip3
from pipeline.parsers.result import FieldReason
from pipeline.parsers.timestamps import parse_timestamp
from pipeline.patient_identity import IdentityRow, LinkageKey, resolve_identities
from pipeline.reference_data import load_cleaning_reference, load_warehouse_reference
from pipeline.source_conventions import load_source_conventions
from pipeline.version_fields import FIELD_COLUMNS, clean_fields

pytestmark = pytest.mark.usefixtures("restore_pipeline_logger")

REFERENCE_DIR = REAL_DATA_DIR / "reference"
CONVENTIONS = load_source_conventions(REFERENCE_DIR / "source_systems_and_facilities.json")
EPIC = "encounters_epic_north.csv"
SENTINEL = "ZZPHI"

# The approved Task 6 severities (design review, decision 5).
APPROVED_SEVERITIES = {
    "MANIFEST_VALID": "ERROR", "PUBLISH_GATE": "ERROR", "FILE_SHA256_MATCH": "ERROR", "FILE_ENCODING_VALID": "ERROR",
    "FILE_CSV_PARSEABLE": "ERROR", "RECORD_SHAPE_VALID": "ERROR", "SCHEMA_CONTRACT_MATCH": "ERROR",
    "ROW_COUNT_MATCH": "ERROR",
    "SOURCE_RECORD_ID_PRESENT": "ERROR", "LAST_UPDATED_TS_VALID": "ERROR", "VERSION_CONFLICT_SAME_TS": "ERROR",
    "FACILITY_RESOLVED": "ERROR", "ADMIT_DATE_VALID": "ERROR",
    "DISCHARGE_DATE_VALID": "WARNING", "DISCHARGE_NOT_BEFORE_ADMIT": "WARNING", "ENCOUNTER_TYPE_MAPPED": "WARNING",
    "PAYER_MAPPED": "WARNING", "CLAIM_STATUS_MAPPED": "WARNING", "PRIMARY_DX_VALID": "WARNING",
    "PRIMARY_DX_IN_REFERENCE": "WARNING", "ATTENDING_NPI_VALID": "WARNING", "ATTENDING_NPI_IN_ROSTER": "WARNING",
    "BILLED_AMOUNT_VALID": "WARNING", "PATIENT_LINKED": "WARNING", "AGE_BAND_KNOWN": "WARNING",
    "PATIENT_KEY_PRESENT": "WARNING", "SEX_KNOWN": "WARNING", "ZIP3_VALID": "WARNING",
    "LAST_UPDATED_TS_UNAMBIGUOUS": "WARNING", "PROVIDER_ON_ROSTER_AT_ADMIT": "WARNING",
    "DUPLICATE_ROW": "INFO", "STALE_ROW": "INFO", "STALE_REPLAY_CONFLICT": "WARNING",
    "SCHEMA_VERSION_CHANGED": "WARNING", "DUPLICATE_FILE": "WARNING",
    "RECON_CURRENT_ROWS": "INFO",
}  # fmt: skip
INVARIANTS = {
    "FK_FACT_FACILITY", "FK_FACT_DIAGNOSIS", "FK_FACT_PAYER", "FK_FACT_PROVIDER", "FK_FACT_PATIENT", "FK_FACT_DATE",
    "CURRENT_MATCHES_HISTORY", "VALUE_REASON_CONSISTENT", "KEY_UNIQUE",
    "RECON_RAW_ROWS", "RECON_ROW_OUTCOMES", "RECON_PATIENT_ROWS", "RECON_HISTORY_ROWS", "RECON_VERSION_FIELD_ROWS",
    "RECON_FACT_VERSION_ROWS", "RECON_DQ_PARTITION", "RECON_QUARANTINE_ROWS", "REJECTED_BATCH_NOT_LOADED",
}  # fmt: skip
# Derivative codes: represented by their parent check, never counted again.
APPROVED_DERIVATIVES = {
    (dq_rules.SRC_LENGTH_OF_STAY, "LENGTH_OF_STAY_ADMIT_UNAVAILABLE"): "ADMIT_DATE_VALID",
    (dq_rules.SRC_LENGTH_OF_STAY, "LENGTH_OF_STAY_DISCHARGE_UNAVAILABLE"): "DISCHARGE_DATE_VALID",
    (dq_rules.SRC_LENGTH_OF_STAY, "DISCHARGE_BEFORE_ADMIT"): "DISCHARGE_NOT_BEFORE_ADMIT",
    (dq_rules.SRC_PROVIDER, "PROVIDER_ADMIT_DATE_UNKNOWN"): "ADMIT_DATE_VALID",
    (dq_rules.SRC_PROVIDER, "NPI_MISSING"): "ATTENDING_NPI_VALID",
    (dq_rules.SRC_PROVIDER, "NPI_PLACEHOLDER"): "ATTENDING_NPI_VALID",
    (dq_rules.SRC_PROVIDER, "NPI_INVALID_FORMAT"): "ATTENDING_NPI_VALID",
    (dq_rules.SRC_PROVIDER, "NPI_CHECKSUM_FAILED"): "ATTENDING_NPI_VALID",
    (dq_rules.SRC_PROVIDER, "NPI_NOT_IN_ROSTER"): "ATTENDING_NPI_IN_ROSTER",
    (dq_rules.SRC_AGE_BAND, "AGE_BAND_ADMIT_UNAVAILABLE"): "ADMIT_DATE_VALID",
    (dq_rules.SRC_LINK, "PATIENT_MRN_MISSING"): "PATIENT_KEY_PRESENT",
    (dq_rules.SRC_LINKAGE_NAME, "LAST_NAME_MISSING"): "PATIENT_LINKED",
    (dq_rules.SRC_LINKAGE_NAME, "FIRST_NAME_MISSING"): "PATIENT_LINKED",
}


# --- the catalog -----------------------------------------------------------


def test_every_approved_check_has_its_approved_severity():
    assert {c.code: str(c.severity) for c in CHECKS if not c.invariant} == APPROVED_SEVERITIES


def test_pipeline_invariants_are_errors_that_must_reconcile():
    invariants = {c.code for c in CHECKS if c.invariant}

    assert invariants == INVARIANTS
    assert all(CHECKS_BY_CODE[c].severity == Severity.ERROR for c in invariants)
    assert all(CHECKS_BY_CODE[c].kind == Kind.RECONCILIATION for c in invariants)


def test_only_manifest_gate_and_rejection_checks_are_batch_scoped():
    assert {c.code for c in CHECKS if c.scope == Scope.BATCH} == {"MANIFEST_VALID", "PUBLISH_GATE", "REJECTED_BATCH_NOT_LOADED"}


def test_version_checks_name_their_warehouse_column():
    for code in dq_rules.VERSION_CHECKS:
        assert CHECKS_BY_CODE[code].field_name, code
    assert all(c.field_name is None for c in CHECKS if c.code not in dq_rules.VERSION_CHECKS)


# --- reason-code classification --------------------------------------------


def test_every_reason_code_is_classified():
    """Every FieldReason, OutcomeReason and ReasonCode member (and the duplicate-file flag) maps somewhere."""
    classified_codes = {code for _, code in CODE_MAP}

    for enum in (FieldReason, OutcomeReason, ReasonCode):
        missing = {m.value for m in enum} - classified_codes
        assert not missing, (enum.__name__, missing)
    assert DUPLICATE_FILE in classified_codes


def test_each_code_has_exactly_one_classification_where_it_is_read():
    keys = [key for key, _ in dq_rules._MAPPING]

    assert len(keys) == len(set(keys)) == len(CODE_MAP)
    for (source, code), (classification, check_code) in CODE_MAP.items():
        if classification == Classification.CONSEQUENCE:
            assert (source, code, check_code) == (dq_rules.SRC_AUDIT, "SIBLING_FILE_REJECTED", None)
        else:
            assert check_code in CHECKS_BY_CODE, (source, code)


def test_derivative_codes_are_exactly_the_approved_ones():
    derivatives = {key: check for key, (kind, check) in CODE_MAP.items() if kind == Classification.DERIVATIVE}

    assert derivatives == APPROVED_DERIVATIVES


def test_every_quality_check_is_fed_by_a_code_or_computed():
    fed = {check for kind, check in CODE_MAP.values() if kind == Classification.CHECK}
    computed = {"STALE_REPLAY_CONFLICT"}  # from match_type on a STALE row, which keeps its outcome reason

    quality = {c.code for c in CHECKS if not c.invariant and c.code != "RECON_CURRENT_ROWS"}
    assert quality - fed == computed


def test_an_unclassified_code_is_a_pipeline_error():
    with pytest.raises(PipelineError):
        dq_rules.classify(dq_rules.SRC_FACILITY, "SOMETHING_NEW")
    with pytest.raises(PipelineError):
        dq_rules.classify(dq_rules.SRC_ADMIT, "FACILITY_UNRESOLVED")  # a real code, but not one admit_date can carry


def _luhn_npi(prefix9: str) -> str:
    digits = [int(d) for d in "80840" + prefix9]
    total = 0
    for i, d in enumerate(reversed(digits)):
        if i % 2 == 0:
            d *= 2
            d = d - 9 if d > 9 else d
        total += d
    return prefix9 + str((10 - total % 10) % 10)


@pytest.fixture(scope="module")
def cleaning_reference():
    return load_cleaning_reference(REFERENCE_DIR, REPO_ROOT / "config" / "facility_aliases.json")


def test_every_code_the_field_parsers_produce_is_classified(cleaning_reference, roster_npi):
    """Vary one field at a time over awkward values, for each source; every reason and warning must classify."""
    unknown_npi = next(n for n in (_luhn_npi(f"{i:09d}") for i in range(100, 200)) if n not in cleaning_reference.roster_npis)
    base = {
        "facility_name": "Lakeshore General Hospital", "admit_date": "03/05/2024", "discharge_date": "03/07/2024",
        "encounter_type": "IP", "claim_status": "Paid", "payer_name": "Medicare", "primary_dx_code": "E11.9",
        "attending_npi": roster_npi, "billed_amount": "500000",
    }  # fmt: skip
    dates = ["", "N/A", "02/30/2024", "2099-01-01", "garbage", "03/05/24", "2024-03-01", "03/01/2024"]
    variations = {
        "facility_name": ["", "Nowhere Clinic", "lakeshore general hospital"],
        "admit_date": dates,
        "discharge_date": dates,
        "encounter_type": ["", "zzz", "OTHER"],
        "claim_status": ["", "zzz", "Void"],
        "payer_name": ["", "zzz", "N/A"],
        "primary_dx_code": ["", "N/A", "250.00", "Z99.99", "@@", "e119"],
        "attending_npi": ["", "N/A", "12345", "1234567890", unknown_npi, f"{roster_npi}.0"],
        "billed_amount": ["", "N/A", "#VALUE!", "12.345", "5K", "abc", "$5,000.00", "-5", "100.5"],
    }
    seen = set()
    delivered = datetime(2025, 1, 6, 6, tzinfo=UTC)
    for system, convention in CONVENTIONS.items():
        for field, values in variations.items():
            for value in values:
                fields = clean_fields({**base, field: value}, source_system=system, delivered_at=delivered,
                                      convention=convention, reference=cleaning_reference)
                for column in FIELD_COLUMNS:
                    if column.endswith(("_reason", "_warning")) and fields[column] is not None:
                        source = f"{dq_rules.FIELDS}.{column}"
                        dq_rules.classify(source, fields[column])  # raises if unclassified
                        seen.add((source, fields[column]))
    # The grid reaches every field source, so the check above is not hollow.
    assert {s for s, _ in seen} == {s for s in dq_rules.VERSION_SOURCES if s.startswith(f"{dq_rules.FIELDS}.")}


def test_every_code_the_patient_provider_and_timestamp_steps_produce_is_classified():
    for raw in ("", "Female", "X"):
        flag = normalise_sex(raw).warning_flag
        if flag:
            dq_rules.classify(dq_rules.SRC_SEX, flag)
    for dob, admit in ((None, date(2024, 1, 1)), (date(1990, 1, 1), None), (date(2025, 1, 1), date(2024, 1, 1))):
        dq_rules.classify(dq_rules.SRC_AGE_BAND, age_band(dob, admit).warning_flag)
    for raw in ("", "ABCDE"):
        dq_rules.classify(dq_rules.SRC_ZIP3, zip3(raw).reason_code)
    key_a, key_b = (LinkageKey("A", "B", date(1990, 1, 1), Sex.F), LinkageKey("A", "C", date(1990, 1, 1), Sex.F))
    identities = resolve_identities([
        IdentityRow("EPIC_NORTH", "", None, False), IdentityRow("EPIC_NORTH", "m1", None, False),
        IdentityRow("EPIC_NORTH", "m2", None, True), IdentityRow("EPIC_NORTH", "m3", key_a, True),
        IdentityRow("EPIC_NORTH", "m3", key_b, True),
    ], b"test-only")  # fmt: skip
    reasons = {identity.reason for identity in identities.values()}
    assert reasons == {FieldReason.PATIENT_MRN_MISSING, FieldReason.PATIENT_UNLINKED_NO_DOB,
                       FieldReason.PATIENT_UNLINKED_INCOMPLETE, FieldReason.PATIENT_UNLINKED_CONFLICT}
    for reason in reasons:
        dq_rules.classify(dq_rules.SRC_LINK, reason)
    dq_rules.classify(dq_rules.SRC_PATIENT_KEY, FieldReason.PATIENT_MRN_MISSING)
    reference = load_warehouse_reference(REFERENCE_DIR)
    lookup = ProviderLookup(build_provider_rows(reference.roster, (f.facility_id for f in reference.facilities)))
    some_npi = reference.roster[0].entries[0].npi
    for npi, admit, npi_reason in ((None, None, "NPI_MISSING"), ("1234567893", date(2024, 1, 1), None),
                                   (some_npi, None, None), (some_npi, date(9999, 1, 1), None)):
        _, reason = lookup.at(npi, admit, npi_reason)
        if reason:
            dq_rules.classify(dq_rules.SRC_PROVIDER, reason)
    for admit, discharge in ((None, date(2024, 1, 1)), (date(2024, 1, 2), None), (date(2024, 1, 2), date(2024, 1, 1))):
        dq_rules.classify(dq_rules.SRC_LENGTH_OF_STAY, length_of_stay(admit, discharge)[1])
    chicago = CONVENTIONS["LEGACY_MEDITECH"]
    for raw in ("", "N/A", "garbage", "2024-02-30 10:00:00", "2024-03-10 02:30:00"):
        reason = parse_timestamp(raw, timezone_name=chicago.timestamp_timezone, date_order=chicago.date_order).reason_code
        dq_rules.classify(dq_rules.SRC_OUTCOME, reason)


def test_audit_reason_codes_drop_details_and_must_be_classified():
    assert dq_rules.audit_reason_codes(
        "SHA256_MISMATCH; MALFORMED_RECORD(record=18,fields=3,expected=21); ROW_COUNT_MISMATCH(expected=22,received=18)"
    ) == ["SHA256_MISMATCH", "MALFORMED_RECORD", "ROW_COUNT_MISMATCH"]
    assert dq_rules.audit_reason_codes("DUPLICATE_FILE(first_batch=batch_001)") == ["DUPLICATE_FILE"]
    assert dq_rules.audit_reason_codes(None) == []
    with pytest.raises(PipelineError):
        dq_rules.audit_reason_codes("NOT_A_CODE")


def test_join_codes_is_sorted_and_distinct():
    assert dq_rules.join_codes(["B", "A", "B", None]) == "A|B"
    assert dq_rules.join_codes([]) is None


# --- version issue derivation ----------------------------------------------


@pytest.mark.parametrize(
    "source, code, check_code, severity",
    [
        (dq_rules.SRC_FACILITY, "FACILITY_UNRESOLVED", "FACILITY_RESOLVED", "ERROR"),
        (dq_rules.SRC_FACILITY, "FACILITY_MISSING", "FACILITY_RESOLVED", "ERROR"),
        (dq_rules.SRC_ADMIT, "DATE_INVALID", "ADMIT_DATE_VALID", "ERROR"),
        (dq_rules.SRC_ADMIT, "DATE_AFTER_DELIVERY", "ADMIT_DATE_VALID", "ERROR"),
        (dq_rules.SRC_DISCHARGE, "DATE_MISSING", "DISCHARGE_DATE_VALID", "WARNING"),
        (dq_rules.SRC_DISCHARGE_WARNING, "DISCHARGE_BEFORE_ADMIT", "DISCHARGE_NOT_BEFORE_ADMIT", "WARNING"),
        (dq_rules.SRC_ENCOUNTER_TYPE, "ENCOUNTER_TYPE_UNMAPPED", "ENCOUNTER_TYPE_MAPPED", "WARNING"),
        (dq_rules.SRC_ENCOUNTER_TYPE_WARNING, "ENCOUNTER_TYPE_MISSING", "ENCOUNTER_TYPE_MAPPED", "WARNING"),
        (dq_rules.SRC_CLAIM_STATUS, "CLAIM_STATUS_UNMAPPED", "CLAIM_STATUS_MAPPED", "WARNING"),
        (dq_rules.SRC_PAYER_WARNING, "PAYER_MISSING", "PAYER_MAPPED", "WARNING"),
        (dq_rules.SRC_DX, "DX_ICD9", "PRIMARY_DX_VALID", "WARNING"),
        (dq_rules.SRC_DX_WARNING, "DX_NOT_IN_REFERENCE", "PRIMARY_DX_IN_REFERENCE", "WARNING"),
        (dq_rules.SRC_NPI, "NPI_CHECKSUM_FAILED", "ATTENDING_NPI_VALID", "WARNING"),
        (dq_rules.SRC_NPI_WARNING, "NPI_NOT_IN_ROSTER", "ATTENDING_NPI_IN_ROSTER", "WARNING"),
        (dq_rules.SRC_AMOUNT, "AMOUNT_PLACEHOLDER", "BILLED_AMOUNT_VALID", "WARNING"),
        (dq_rules.SRC_LINK, "PATIENT_UNLINKED_NO_DOB", "PATIENT_LINKED", "WARNING"),
        (dq_rules.SRC_SEX, "SEX_UNMAPPED", "SEX_KNOWN", "WARNING"),
        (dq_rules.SRC_AGE_BAND, "AGE_BAND_DOB_UNAVAILABLE", "AGE_BAND_KNOWN", "WARNING"),
        (dq_rules.SRC_ZIP3, "ZIP_INVALID", "ZIP3_VALID", "WARNING"),
        (dq_rules.SRC_PATIENT_KEY, "PATIENT_MRN_MISSING", "PATIENT_KEY_PRESENT", "WARNING"),
        (dq_rules.SRC_PROVIDER, "PROVIDER_NOT_ON_ROSTER_AT_ADMIT", "PROVIDER_ON_ROSTER_AT_ADMIT", "WARNING"),
        (dq_rules.SRC_TIMESTAMP_WARNING, "TIMESTAMP_AMBIGUOUS_LOCAL_TIME", "LAST_UPDATED_TS_UNAMBIGUOUS", "WARNING"),
    ],
)
def test_each_version_check_classification(source, code, check_code, severity):
    [issue] = version_dq.derive_issues({source: code})

    assert (issue.check_code, issue.reason_code, str(issue.severity)) == (check_code, code, severity)
    assert issue.field_name == CHECKS_BY_CODE[check_code].field_name


def test_a_clean_version_has_no_issues():
    assert version_dq.derive_issues({source: None for source in dq_rules.VERSION_SOURCES}) == []


def test_derivative_codes_are_not_double_counted():
    """An invalid admit date and an invalid NPI, with every code that follows from them."""
    codes = {
        dq_rules.SRC_ADMIT: "DATE_INVALID",
        dq_rules.SRC_AGE_BAND: "AGE_BAND_ADMIT_UNAVAILABLE",
        dq_rules.SRC_LENGTH_OF_STAY: "LENGTH_OF_STAY_ADMIT_UNAVAILABLE",
        dq_rules.SRC_NPI: "NPI_INVALID_FORMAT",
        dq_rules.SRC_PROVIDER: "NPI_INVALID_FORMAT",
        dq_rules.SRC_LINK: "PATIENT_MRN_MISSING",
        dq_rules.SRC_PATIENT_KEY: "PATIENT_MRN_MISSING",
    }

    issues = version_dq.derive_issues(codes)

    assert [(i.check_code, i.reason_code) for i in issues] == [
        ("ADMIT_DATE_VALID", "DATE_INVALID"),
        ("ATTENDING_NPI_VALID", "NPI_INVALID_FORMAT"),
        ("PATIENT_KEY_PRESENT", "PATIENT_MRN_MISSING"),
    ]


def test_provider_admit_date_unknown_is_counted_under_the_admit_date_only():
    issues = version_dq.derive_issues({dq_rules.SRC_ADMIT: "DATE_MISSING", dq_rules.SRC_PROVIDER: "PROVIDER_ADMIT_DATE_UNKNOWN"})

    assert [i.check_code for i in issues] == ["ADMIT_DATE_VALID"]


def test_two_codes_for_one_version_check_are_a_pipeline_error():
    with pytest.raises(PipelineError):
        version_dq.derive_issues({dq_rules.SRC_PAYER: "PAYER_UNMAPPED", dq_rules.SRC_PAYER_WARNING: "PAYER_MISSING"})


def test_ambiguous_local_timestamp_is_the_only_timestamp_warning():
    meditech, epic = CONVENTIONS["LEGACY_MEDITECH"], CONVENTIONS["EPIC_NORTH"]

    assert version_dq.timestamp_warning("2024-11-03 01:30:00", meditech) == "TIMESTAMP_AMBIGUOUS_LOCAL_TIME"
    assert version_dq.timestamp_warning("2024-11-03 01:30:00", epic) is None  # Epic is UTC
    assert version_dq.timestamp_warning("2024-11-03T01:30:00-05:00", meditech) is None  # explicit offset wins


# --- report rows -----------------------------------------------------------


def test_structural_results_for_a_failing_and_a_sibling_file():
    failing = dq_report.structural_results(["SHA256_MISMATCH", "MALFORMED_RECORD", "ROW_COUNT_MISMATCH"], 18)
    sibling = dq_report.structural_results(["SIBLING_FILE_REJECTED"], 22)

    assert {k: v[0] for k, v in failing.items()} == {
        "FILE_SHA256_MATCH": "FAIL", "FILE_ENCODING_VALID": "PASS", "FILE_CSV_PARSEABLE": "PASS",
        "RECORD_SHAPE_VALID": "FAIL", "SCHEMA_CONTRACT_MATCH": "PASS", "ROW_COUNT_MATCH": "FAIL",
    }  # fmt: skip
    assert {v[0] for v in sibling.values()} == {"PASS"}


def test_checks_a_file_never_reached_are_not_evaluated():
    encoding = dq_report.structural_results(["ENCODING_ERROR"], None)
    header = dq_report.structural_results(["HEADER_MISSING"], None)

    assert encoding["FILE_ENCODING_VALID"] == ("FAIL", ["ENCODING_ERROR"])
    for check in ("FILE_CSV_PARSEABLE", "RECORD_SHAPE_VALID", "SCHEMA_CONTRACT_MATCH", "ROW_COUNT_MATCH"):
        assert encoding[check] == ("NOT_EVALUATED", ["ENCODING_ERROR"]), check
    assert header["SCHEMA_CONTRACT_MATCH"] == ("FAIL", ["HEADER_MISSING"])
    assert header["ROW_COUNT_MATCH"] == ("NOT_EVALUATED", ["HEADER_MISSING"])
    assert header["FILE_CSV_PARSEABLE"] == ("PASS", [])


@pytest.mark.parametrize(
    "code, observed, expected, status",
    [
        ("FACILITY_RESOLVED", 0, None, "PASS"),
        ("FACILITY_RESOLVED", 2, None, "FAIL"),
        ("DISCHARGE_DATE_VALID", 2, None, "WARN"),
        ("DUPLICATE_ROW", 30, None, "PASS"),
        ("ROW_COUNT_MATCH", 18, 22, "FAIL"),
        ("RECON_RAW_ROWS", 3, 3, "PASS"),
    ],
)
def test_status_by_kind_and_severity(code, observed, expected, status):
    assert dq_report.evaluated_status(CHECKS_BY_CODE[code], observed, expected) == status


def test_percentages_are_two_decimals_rounded_half_even():
    row = dq_report.report_row(CHECKS_BY_CODE["FACILITY_RESOLVED"], "batch_001", "ACCEPTED", EPIC, "EPIC_NORTH",
                               evaluated=2863, observed=44)
    recon = dq_report.report_row(CHECKS_BY_CODE["RECON_RAW_ROWS"], "batch_001", "ACCEPTED", EPIC, "EPIC_NORTH",
                                 evaluated=3, observed=3, expected=3)

    assert row["observed_pct"] == Decimal("1.54")
    assert dq_report._pct(1, 8) == Decimal("12.50") and dq_report._pct(1, 400) == Decimal("0.25")
    assert recon["observed_pct"] is None and recon["expected_count"] == 3
    assert list(row) == list(dq_report.COLUMNS)


# --- synthetic pipeline runs -----------------------------------------------


def scenario_rows():
    return [
        ("E1", {}),  # passes every check
        ("E1", {}),  # exact duplicate in the batch: DUPLICATE_ROW (INFO)
        ("E2", {"facility_name": "Nowhere Clinic", "discharge_date": ""}),  # facility ERROR, discharge WARNING
        ("E3", {"admit_date": "02/30/2024"}),  # admit ERROR; age band, stay, provider follow from it
        ("E4", {"attending_npi": "12345"}),  # NPI WARNING; the provider reason follows from it
        ("", {}),  # Task 4 row quarantine: no source_record_id
    ]


def query(env, sql, params=()):
    con = attach(Path(env["RAW_DB_PATH"]))
    try:
        return con.execute(sql, params).fetchall()
    finally:
        con.close()


def report(env, batch_id):
    columns = ", ".join(dq_report.COLUMNS)
    rows = query(env, f"SELECT {columns} FROM {dq_report.TABLE} WHERE batch_id = ? ORDER BY {dq_report.ORDER_BY}", [batch_id])
    return [dict(zip(dq_report.COLUMNS, r, strict=True)) for r in rows]


@pytest.fixture
def scenario(landing_dir, pipeline_env, roster_npi):
    spec = epic_file(scenario_rows(), roster_npi=roster_npi)
    write_batch(landing_dir, "batch_001", [spec])
    write_batch(landing_dir, "batch_002", [spec], manifest_overrides={EPIC: {"sha256": "0" * 64}})  # rejected
    assert main(pipeline_env) == 0
    return pipeline_env


def test_version_issues_of_the_scenario(scenario):
    issues = query(scenario, f"SELECT source_record_id, check_code, field_name, reason_code, severity "
                             f"FROM {version_dq.TABLE} ORDER BY source_record_id, check_code")

    assert issues == [
        ("E2", "DISCHARGE_DATE_VALID", "discharge_date", "DATE_MISSING", "WARNING"),
        ("E2", "FACILITY_RESOLVED", "facility_id", "FACILITY_UNRESOLVED", "ERROR"),
        ("E3", "ADMIT_DATE_VALID", "admit_date", "DATE_INVALID", "ERROR"),
        ("E4", "ATTENDING_NPI_VALID", "attending_npi", "NPI_INVALID_FORMAT", "WARNING"),
    ]
    # Derivative codes are present in the facts but were not counted again.
    assert query(scenario, "SELECT source_record_id, provider_sk_reason, length_of_stay_days_reason, age_band_reason "
                           "FROM mart.fact_encounter_version WHERE source_record_id IN ('E3', 'E4') ORDER BY 1") == [
        ("E3", "PROVIDER_ADMIT_DATE_UNKNOWN", "LENGTH_OF_STAY_ADMIT_UNAVAILABLE", "AGE_BAND_ADMIT_UNAVAILABLE"),
        ("E4", "NPI_INVALID_FORMAT", None, None),
    ]


def test_error_versions_stay_in_history_and_current_state(scenario):
    errors = {r[0] for r in query(scenario, f"SELECT version_key FROM {version_dq.TABLE} WHERE severity = 'ERROR'")}
    in_facts = {r[0] for r in query(scenario, "SELECT version_key FROM mart.fact_encounter_version")}
    in_current = {r[0] for r in query(scenario, "SELECT version_key FROM mart.fact_encounter_current")}

    assert len(errors) == 2
    assert errors <= in_facts and errors <= in_current
    assert query(scenario, "SELECT sum(quarantined_count) FROM ops.batch_audit WHERE batch_id = 'batch_001'") == [(1,)]


def test_every_version_issue_has_valid_version_lineage(scenario):
    unresolved = query(scenario, f"""
        SELECT count(*) FROM {version_dq.TABLE} i
        ANTI JOIN clean.encounter_versions v
          ON v.version_key = i.version_key AND v.encounter_key = i.encounter_key
         AND v.source_system = i.source_system AND v.source_record_id = i.source_record_id
         AND v.first_seen_batch_id = i.source_batch_id AND v.first_seen_file_name = i.source_file_name
         AND v.first_seen_source_row_number = i.source_row_number""")
    raw_record_ids = query(scenario, f"""
        SELECT count(*) FROM {version_dq.TABLE} i JOIN raw.encounters e
          ON e.batch_id = i.source_batch_id AND e.file_name = i.source_file_name
         AND e.source_row_number = i.source_row_number AND e.source_record_id = i.source_record_id""")

    assert unresolved == [(0,)]
    assert raw_record_ids == [(4,)]


def test_report_of_the_accepted_batch(scenario):
    rows = report(scenario, "batch_001")
    by_check = {r["check_code"]: r for r in rows if r["file_name"] == EPIC}

    assert [r["check_code"] for r in rows if r["file_name"] is None] == ["MANIFEST_VALID", "PUBLISH_GATE"]
    assert set(by_check) == {c.code for c in dq_report.FILE_CHECKS}  # every file check has a row
    assert len(rows) == 2 + len(dq_report.FILE_CHECKS)
    [gate] = [r for r in rows if r["check_code"] == "PUBLISH_GATE"]
    # E2 and E3 (version ERRORs) and the row without an id; the fixture's threshold is 1 (never blocks).
    assert (gate["evaluated_count"], gate["observed_count"], gate["observed_pct"], gate["threshold_pct"], gate["status"]) == (
        6, 3, Decimal("50.00"), Decimal("100.00"), "PASS")

    def summary(code):
        r = by_check[code]
        return r["evaluated_count"], r["observed_count"], r["expected_count"], r["observed_pct"], r["status"], r["reason_codes"]

    assert summary("FACILITY_RESOLVED") == (4, 1, None, Decimal("25.00"), "FAIL", "FACILITY_UNRESOLVED")
    assert summary("ADMIT_DATE_VALID") == (4, 1, None, Decimal("25.00"), "FAIL", "DATE_INVALID")
    assert summary("DISCHARGE_DATE_VALID") == (4, 1, None, Decimal("25.00"), "WARN", "DATE_MISSING")
    assert summary("ATTENDING_NPI_VALID") == (4, 1, None, Decimal("25.00"), "WARN", "NPI_INVALID_FORMAT")
    assert summary("SOURCE_RECORD_ID_PRESENT") == (6, 1, None, Decimal("16.67"), "FAIL", "SOURCE_RECORD_ID_MISSING")
    assert summary("DUPLICATE_ROW") == (6, 1, None, Decimal("16.67"), "PASS", "DUPLICATE_IN_BATCH")
    assert summary("ROW_COUNT_MATCH") == (1, 6, 6, None, "PASS", None)
    assert summary("RECON_RAW_ROWS") == (6, 6, 6, None, "PASS", None)
    assert summary("RECON_HISTORY_ROWS") == (4, 4, 4, None, "PASS", None)
    assert summary("RECON_CURRENT_ROWS") == (4, 4, None, Decimal("100.00"), "PASS", None)
    assert summary("RECON_QUARANTINE_ROWS") == (6, 3, 3, None, "PASS", None)  # 1 Task 4 row + 2 ERROR versions
    # Zero-count checks still have a row, and derivative codes left no trace.
    for code in ("CLAIM_STATUS_MAPPED", "AGE_BAND_KNOWN", "PROVIDER_ON_ROSTER_AT_ADMIT", "STALE_ROW"):
        assert summary(code)[1:] == (0, None, Decimal("0.00"), "PASS", None), code
    assert all(r["status"] == "PASS" for r in rows if CHECKS_BY_CODE[r["check_code"]].invariant)
    assert {r["batch_status"] for r in rows} == {"ACCEPTED"}
    assert {r["threshold_pct"] for r in rows if r["check_code"] != "PUBLISH_GATE"} == {None}  # only the gate has one


def test_report_of_the_rejected_batch(scenario):
    rows = report(scenario, "batch_002")
    evaluated = {r["check_code"]: (r["status"], r["reason_codes"]) for r in rows if r["status"] != "NOT_EVALUATED"}
    not_evaluated = [r for r in rows if r["status"] == "NOT_EVALUATED"]

    assert evaluated == {
        "MANIFEST_VALID": ("PASS", None),
        "REJECTED_BATCH_NOT_LOADED": ("PASS", None),
        "FILE_SHA256_MATCH": ("FAIL", "SHA256_MISMATCH"),
        "FILE_ENCODING_VALID": ("PASS", None),
        "FILE_CSV_PARSEABLE": ("PASS", None),
        "RECORD_SHAPE_VALID": ("PASS", None),
        "SCHEMA_CONTRACT_MATCH": ("PASS", None),
        "ROW_COUNT_MATCH": ("PASS", None),
    }
    assert len(not_evaluated) == len(dq_report.FILE_CHECKS) - len(dq_rules.STRUCTURAL_FILE_CHECKS) + 1  # + the gate
    assert [r["check_code"] for r in not_evaluated if r["file_name"] is None] == ["PUBLISH_GATE"]
    assert {(r["reason_codes"], r["evaluated_count"], r["observed_count"]) for r in not_evaluated} == {("BATCH_REJECTED", None, None)}
    assert {r["batch_status"] for r in rows} == {"REJECTED"}
    assert query(scenario, f"SELECT count(*) FROM {version_dq.TABLE} WHERE source_batch_id = 'batch_002'") == [(0,)]


def test_a_batch_rejected_at_the_manifest_gets_only_batch_rows(landing_dir, pipeline_env, roster_npi):
    batch_dir = write_batch(landing_dir, "batch_001", [epic_file(scenario_rows(), roster_npi=roster_npi)])
    (batch_dir / "manifest.json").unlink()

    assert main(pipeline_env) == 0

    rows = report(pipeline_env, "batch_001")
    assert [(r["check_code"], r["status"], r["reason_codes"]) for r in rows] == [
        ("MANIFEST_VALID", "FAIL", "MANIFEST_MISSING"),
        ("PUBLISH_GATE", "NOT_EVALUATED", "BATCH_REJECTED"),
        ("REJECTED_BATCH_NOT_LOADED", "PASS", None),
    ]


def test_dq_build_events_log_error_and_warning_counts(scenario, capsys):
    capsys.readouterr()
    assert main(scenario) == 0  # nothing pending: the DQ tables are rebuilt and logged again

    events = {e["event"]: e for e in map(json.loads, capsys.readouterr().out.splitlines())
              if e["event"] in ("version_dq_issues_built", "dq_report_built")}  # fmt: skip
    issues = query(scenario, f"SELECT severity, count(*) FROM {version_dq.TABLE} GROUP BY 1")
    statuses = dict(query(scenario, f"SELECT status, count(*) FROM {dq_report.TABLE} GROUP BY 1"))

    # Version issues: E2 and E3 ERRORs; E2 discharge and E4 NPI WARNINGs.
    assert dict(issues) == {"ERROR": 2, "WARNING": 2}
    assert (events["version_dq_issues_built"]["error_count"], events["version_dq_issues_built"]["warning_count"]) == (2, 2)
    # Report rows: FAIL = facility, admit date, missing id (batch_001) and SHA-256 (batch_002); WARN = discharge, NPI.
    assert (statuses["FAIL"], statuses["WARN"]) == (4, 2)
    assert (events["dq_report_built"]["error_count"], events["dq_report_built"]["warning_count"]) == (4, 2)


def test_a_schema_version_change_is_flagged_with_a_code(landing_dir, pipeline_env):
    """Athena v1 in batch_001, the configured v2 layout in batch_002: both accepted, the change flagged."""
    write_batch(landing_dir, "batch_001", [synthetic_file("ATHENA_CLINICS", tag="a")])
    write_batch(landing_dir, "batch_002", [synthetic_file("ATHENA_CLINICS", tag="b", version="athena_clinics_v2")])

    assert main(pipeline_env) == 0

    rows = [r for b in ("batch_001", "batch_002") for r in report(pipeline_env, b) if r["check_code"] == "SCHEMA_VERSION_CHANGED"]
    assert [(r["batch_id"], r["observed_count"], r["status"], r["reason_codes"]) for r in rows] == [
        ("batch_001", 0, "PASS", None),  # no earlier file of this source
        ("batch_002", 1, "WARN", "SCHEMA_VERSION_CHANGED"),
    ]
    assert dq_rules.classify(dq_rules.SRC_SCHEMA_VERSION, rows[1]["reason_codes"]) == (
        Classification.CHECK, "SCHEMA_VERSION_CHANGED")


def test_report_order_and_columns(scenario):
    con = attach(Path(scenario["RAW_DB_PATH"]))
    try:
        columns = [r[0] for r in con.execute(f"DESCRIBE {dq_report.TABLE}").fetchall()]
        stored = con.execute(f"SELECT * FROM {dq_report.TABLE} ORDER BY {dq_report.ORDER_BY}").fetchall()
        rebuilt = dq_report.report_rows(con, Decimal(1))
    finally:
        con.close()

    assert columns == list(dq_report.COLUMNS)
    assert [tuple(r.values()) for r in rebuilt] == stored  # report_rows returns the ORDER_BY order
    keys = [(r[0], r[3] is not None, r[3] or "", r[5]) for r in stored]
    assert keys == sorted(keys) and len(set(keys)) == len(keys)


def dq_digest(env):
    con = attach(Path(env["RAW_DB_PATH"]))
    try:
        return {t: hashlib.sha256(repr(con.execute(f"SELECT * FROM {t} ORDER BY ALL").fetchall()).encode()).hexdigest()
                for t in (version_dq.TABLE, dq_report.TABLE)}
    finally:
        con.close()


def test_repeated_runs_and_rebuild_give_identical_dq_tables(scenario):
    first = dq_digest(scenario)

    assert main(scenario) == 0
    assert dq_digest(scenario) == first
    assert main(scenario, ["--rebuild-derived"]) == 0
    assert dq_digest(scenario) == first


def test_a_failed_invariant_rolls_back_the_mart_and_exits_1(scenario, capsys):
    con = attach(Path(scenario["RAW_DB_PATH"]))
    try:  # an extra raw row the audit never accepted: raw no longer reconciles with received_count
        con.execute("INSERT INTO raw.encounters SELECT * REPLACE (99 AS source_row_number) FROM raw.encounters "
                    "WHERE batch_id = 'batch_001' AND source_row_number = 1")
        before = {t: con.execute(f"SELECT * FROM {t} ORDER BY ALL").fetchall()
                  for t in ("mart.fact_encounter_version", "mart.dim_patient", version_dq.TABLE, dq_report.TABLE)}
    finally:
        con.close()
    capsys.readouterr()

    assert main(scenario) == 1

    events = [line for line in capsys.readouterr().out.splitlines() if '"dq_invariant_failed"' in line]
    assert len(events) == 1 and '"reason_code": "RECON_RAW_ROWS"' in events[0]
    con = attach(Path(scenario["RAW_DB_PATH"]))
    try:
        after = {t: con.execute(f"SELECT * FROM {t} ORDER BY ALL").fetchall() for t in before}
    finally:
        con.close()
    assert after == before  # the mart and the DQ tables rolled back together


# --- no PHI ----------------------------------------------------------------


def test_dq_tables_have_no_phi_columns():
    for columns in (version_dq.COLUMNS, dq_report.COLUMNS):
        assert not {c for c in columns if c in PHI_COLUMNS or c.removesuffix("_raw") in PHI_COLUMNS}
        assert not any(c.endswith("_raw") for c in columns)


def test_phi_and_raw_values_never_reach_the_dq_tables(landing_dir, pipeline_env, roster_npi):
    """The sentinel is in every PHI field, and in raw values that fail checks (facility, amount, dates)."""
    phi = {c: f"{SENTINEL}-{c}" for c in PHI_COLUMNS}
    rows = [
        ("E1", phi),
        ("E2", {**phi, "facility_name": f"{SENTINEL} clinic", "billed_amount": f"{SENTINEL}",
                "admit_date": SENTINEL, "attending_npi": SENTINEL, "primary_dx_code": SENTINEL}),
    ]
    write_batch(landing_dir, "batch_001", [epic_file(rows, roster_npi=roster_npi)])

    assert main(pipeline_env) == 0

    assert query(pipeline_env, "SELECT count(*) FROM raw.encounters WHERE patient_mrn LIKE ?", [f"%{SENTINEL}%"]) == [(2,)]
    issues = query(pipeline_env, f"SELECT * FROM {version_dq.TABLE}")
    report_rows = query(pipeline_env, f"SELECT * FROM {dq_report.TABLE}")
    assert issues and report_rows
    assert SENTINEL not in repr(issues)
    assert SENTINEL not in repr(report_rows)
