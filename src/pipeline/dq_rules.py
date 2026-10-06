"""Task 6 data-quality checks: the catalog and the reason-code classification.

Pure definitions, no I/O. Every check has a code, a category, the layer and
grain it looks at, the scope of its report row (one per file, or one per
batch), a severity and a kind:

    QUALITY         observed_count = units that fail; status PASS, or FAIL
                    (ERROR) / WARN (WARNING) when any fail
    OBSERVATION     observed_count = units counted; INFO, always PASS
    RECONCILIATION  observed_count must equal expected_count
    GATE            the publish gate: FAIL when the share of rows failing
                    error-level checks is MORE than the configured threshold

Severity:
    ERROR    a row-level ERROR quarantines the row (Task 4 row quarantine) and
             a version-level ERROR flags the version; Task 1 file checks
             reject the batch. Pipeline invariants (`invariant=True`) are
             ERRORs too, but a failure is a pipeline bug: it raises
             PipelineError and rolls back, so it is never stored as a finding.
    WARNING  the row or version is kept and flagged
    INFO     a count, not a defect (duplicates, stale replays, current rows)

The checks reuse the reason codes Tasks 1-5 already produce; no value is
parsed again here. CODE_MAP classifies every code by where it is read
(`source`), because the same code can mean different things in different
places (DATE_INVALID on admit_date vs discharge_date):

    CHECK        counted by `check_code`
    DERIVATIVE   follows from another check's failure and is represented by
                 that parent check (`check_code`), so it is not counted again
                 (LENGTH_OF_STAY_*, PROVIDER_ADMIT_DATE_UNKNOWN, the NPI codes
                 repeated in provider_sk_reason, AGE_BAND_ADMIT_UNAVAILABLE, ...)
    CONSEQUENCE  a status, not a check (SIBLING_FILE_REJECTED)

A code with no entry for its source is a pipeline error: a new reason code
can never escape classification.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum

from pipeline.encounter_history import OutcomeReason
from pipeline.errors import PipelineError, ReasonCode
from pipeline.parsers.result import FieldReason


class Severity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"


class Category(StrEnum):
    FILE_VALIDATION = "FILE_VALIDATION"
    SCHEMA = "SCHEMA"
    ROW_COUNT = "ROW_COUNT"
    DUPLICATE = "DUPLICATE"
    QUARANTINE = "QUARANTINE"
    VALIDITY = "VALIDITY"
    NULL = "NULL"
    REFERENTIAL_INTEGRITY = "REFERENTIAL_INTEGRITY"
    RECONCILIATION = "RECONCILIATION"
    PUBLISH_GATE = "PUBLISH_GATE"


class Layer(StrEnum):
    LANDING = "LANDING"
    RAW = "RAW"
    CLEAN = "CLEAN"
    MART = "MART"
    ALL = "ALL"


class Grain(StrEnum):
    BATCH = "BATCH"
    FILE = "FILE"
    ROW = "ROW"
    VERSION = "VERSION"


class Scope(StrEnum):
    BATCH = "BATCH"
    FILE = "FILE"


class Kind(StrEnum):
    QUALITY = "QUALITY"
    OBSERVATION = "OBSERVATION"
    RECONCILIATION = "RECONCILIATION"
    GATE = "GATE"


class Classification(StrEnum):
    CHECK = "CHECK"
    DERIVATIVE = "DERIVATIVE"
    CONSEQUENCE = "CONSEQUENCE"


@dataclass(frozen=True)
class Check:
    code: str
    category: Category
    layer: Layer
    grain: Grain
    severity: Severity
    kind: Kind
    scope: Scope = Scope.FILE
    field_name: str | None = None  # version checks: the warehouse column the issue is about
    invariant: bool = False  # a failure is a pipeline error, never a stored finding


def _file(code: str, category: Category, kind: Kind = Kind.QUALITY) -> Check:
    return Check(code, category, Layer.LANDING, Grain.FILE, Severity.ERROR, kind)


def _row(code: str, category: Category, severity: Severity, kind: Kind = Kind.QUALITY) -> Check:
    return Check(code, category, Layer.CLEAN, Grain.ROW, severity, kind)


def _version(code: str, field_name: str, severity: Severity = Severity.WARNING, *,
             category: Category = Category.VALIDITY, layer: Layer = Layer.CLEAN) -> Check:
    return Check(code, category, layer, Grain.VERSION, severity, Kind.QUALITY, field_name=field_name)


def _invariant(code: str, category: Category, layer: Layer, grain: Grain, scope: Scope = Scope.FILE) -> Check:
    return Check(code, category, layer, grain, Severity.ERROR, Kind.RECONCILIATION, scope, invariant=True)


ERROR, WARNING, INFO = Severity.ERROR, Severity.WARNING, Severity.INFO

CHECKS: tuple[Check, ...] = (
    # Task 1 structural checks: any failure rejects the whole batch.
    Check("MANIFEST_VALID", Category.FILE_VALIDATION, Layer.LANDING, Grain.BATCH, ERROR, Kind.QUALITY, Scope.BATCH),
    # The publish gate: a batch whose share of ERROR rows is above the threshold is rejected.
    Check("PUBLISH_GATE", Category.PUBLISH_GATE, Layer.CLEAN, Grain.BATCH, ERROR, Kind.GATE, Scope.BATCH),
    _file("FILE_SHA256_MATCH", Category.FILE_VALIDATION),
    _file("FILE_ENCODING_VALID", Category.FILE_VALIDATION),
    _file("FILE_CSV_PARSEABLE", Category.FILE_VALIDATION),
    _file("RECORD_SHAPE_VALID", Category.FILE_VALIDATION),
    _file("SCHEMA_CONTRACT_MATCH", Category.SCHEMA),
    _file("ROW_COUNT_MATCH", Category.ROW_COUNT, Kind.RECONCILIATION),
    # File observations of an accepted file.
    Check("SCHEMA_VERSION_CHANGED", Category.SCHEMA, Layer.LANDING, Grain.FILE, WARNING, Kind.QUALITY),
    Check("DUPLICATE_FILE", Category.DUPLICATE, Layer.LANDING, Grain.FILE, WARNING, Kind.QUALITY),
    # Task 4 row outcomes. The three ERRORs are the Task 4 row quarantine.
    _row("SOURCE_RECORD_ID_PRESENT", Category.QUARANTINE, ERROR),
    _row("LAST_UPDATED_TS_VALID", Category.QUARANTINE, ERROR),
    _row("VERSION_CONFLICT_SAME_TS", Category.QUARANTINE, ERROR),
    _row("DUPLICATE_ROW", Category.DUPLICATE, INFO, Kind.OBSERVATION),
    _row("STALE_ROW", Category.DUPLICATE, INFO, Kind.OBSERVATION),
    _row("STALE_REPLAY_CONFLICT", Category.DUPLICATE, WARNING),
    # Version checks (clean.version_dq_issues).
    _version("FACILITY_RESOLVED", "facility_id", ERROR),
    _version("ADMIT_DATE_VALID", "admit_date", ERROR),
    _version("DISCHARGE_DATE_VALID", "discharge_date"),
    _version("DISCHARGE_NOT_BEFORE_ADMIT", "discharge_date"),
    _version("ENCOUNTER_TYPE_MAPPED", "encounter_type"),
    _version("CLAIM_STATUS_MAPPED", "claim_status"),
    _version("PAYER_MAPPED", "payer_category"),
    _version("PRIMARY_DX_VALID", "primary_dx_code"),
    _version("PRIMARY_DX_IN_REFERENCE", "primary_dx_code", category=Category.REFERENTIAL_INTEGRITY),
    _version("ATTENDING_NPI_VALID", "attending_npi"),
    _version("ATTENDING_NPI_IN_ROSTER", "attending_npi", category=Category.REFERENTIAL_INTEGRITY),
    _version("BILLED_AMOUNT_VALID", "billed_amount_usd"),
    _version("LAST_UPDATED_TS_UNAMBIGUOUS", "last_updated_ts_utc"),
    _version("PATIENT_KEY_PRESENT", "patient_key", category=Category.NULL),
    _version("PATIENT_LINKED", "patient_link_status"),
    _version("AGE_BAND_KNOWN", "age_band"),
    _version("SEX_KNOWN", "sex"),
    _version("ZIP3_VALID", "zip3"),
    _version("PROVIDER_ON_ROSTER_AT_ADMIT", "provider_sk", category=Category.REFERENTIAL_INTEGRITY, layer=Layer.MART),
    # Pipeline invariants: always PASS when stored; a failure raises and rolls back.
    _invariant("FK_FACT_FACILITY", Category.REFERENTIAL_INTEGRITY, Layer.MART, Grain.VERSION),
    _invariant("FK_FACT_DIAGNOSIS", Category.REFERENTIAL_INTEGRITY, Layer.MART, Grain.VERSION),
    _invariant("FK_FACT_PAYER", Category.REFERENTIAL_INTEGRITY, Layer.MART, Grain.VERSION),
    _invariant("FK_FACT_PROVIDER", Category.REFERENTIAL_INTEGRITY, Layer.MART, Grain.VERSION),
    _invariant("FK_FACT_PATIENT", Category.REFERENTIAL_INTEGRITY, Layer.MART, Grain.VERSION),
    _invariant("FK_FACT_DATE", Category.REFERENTIAL_INTEGRITY, Layer.MART, Grain.VERSION),
    _invariant("CURRENT_MATCHES_HISTORY", Category.RECONCILIATION, Layer.MART, Grain.VERSION),
    _invariant("VALUE_REASON_CONSISTENT", Category.NULL, Layer.MART, Grain.VERSION),
    _invariant("KEY_UNIQUE", Category.DUPLICATE, Layer.ALL, Grain.VERSION),
    _invariant("RECON_RAW_ROWS", Category.RECONCILIATION, Layer.RAW, Grain.ROW),
    _invariant("RECON_ROW_OUTCOMES", Category.RECONCILIATION, Layer.CLEAN, Grain.ROW),
    _invariant("RECON_PATIENT_ROWS", Category.RECONCILIATION, Layer.CLEAN, Grain.ROW),
    _invariant("RECON_HISTORY_ROWS", Category.RECONCILIATION, Layer.CLEAN, Grain.VERSION),
    _invariant("RECON_VERSION_FIELD_ROWS", Category.RECONCILIATION, Layer.CLEAN, Grain.VERSION),
    _invariant("RECON_FACT_VERSION_ROWS", Category.RECONCILIATION, Layer.MART, Grain.VERSION),
    _invariant("RECON_DQ_PARTITION", Category.RECONCILIATION, Layer.CLEAN, Grain.VERSION),
    _invariant("RECON_QUARANTINE_ROWS", Category.RECONCILIATION, Layer.CLEAN, Grain.ROW),
    _invariant("REJECTED_BATCH_NOT_LOADED", Category.RECONCILIATION, Layer.ALL, Grain.BATCH, Scope.BATCH),
    # Current rows whose version came from this file: changes as later batches arrive.
    Check("RECON_CURRENT_ROWS", Category.RECONCILIATION, Layer.MART, Grain.VERSION, INFO, Kind.OBSERVATION),
)

CHECKS_BY_CODE = {c.code: c for c in CHECKS}
if len(CHECKS_BY_CODE) != len(CHECKS):
    raise AssertionError("duplicate check code in CHECKS")

# The Task 1 checks a rejected batch is evaluated on; every other file check is NOT_EVALUATED there.
STRUCTURAL_FILE_CHECKS = (
    "FILE_SHA256_MATCH",
    "FILE_ENCODING_VALID",
    "FILE_CSV_PARSEABLE",
    "RECORD_SHAPE_VALID",
    "SCHEMA_CONTRACT_MATCH",
    "ROW_COUNT_MATCH",
)
VERSION_CHECKS = tuple(c.code for c in CHECKS if c.grain == Grain.VERSION and c.kind == Kind.QUALITY)

# --- where each reason code is read ---------------------------------------

FIELDS = "encounter_version_fields"
PATIENTS = "encounter_patients"
FACTS = "fact_encounter_version"
SRC_FACILITY = f"{FIELDS}.facility_id_reason"
SRC_ADMIT = f"{FIELDS}.admit_date_reason"
SRC_DISCHARGE = f"{FIELDS}.discharge_date_reason"
SRC_DISCHARGE_WARNING = f"{FIELDS}.discharge_date_warning"
SRC_ENCOUNTER_TYPE = f"{FIELDS}.encounter_type_reason"
SRC_ENCOUNTER_TYPE_WARNING = f"{FIELDS}.encounter_type_warning"
SRC_CLAIM_STATUS = f"{FIELDS}.claim_status_reason"
SRC_PAYER = f"{FIELDS}.payer_category_reason"
SRC_PAYER_WARNING = f"{FIELDS}.payer_category_warning"
SRC_DX = f"{FIELDS}.primary_dx_code_reason"
SRC_DX_WARNING = f"{FIELDS}.primary_dx_code_warning"
SRC_NPI = f"{FIELDS}.attending_npi_reason"
SRC_NPI_WARNING = f"{FIELDS}.attending_npi_warning"
SRC_AMOUNT = f"{FIELDS}.billed_amount_usd_reason"
SRC_LINK = f"{PATIENTS}.patient_link_reason"
SRC_SEX = f"{PATIENTS}.sex_reason"
SRC_AGE_BAND = f"{PATIENTS}.age_band_reason"
SRC_ZIP3 = f"{PATIENTS}.zip3_reason"
SRC_PATIENT_KEY = f"{FACTS}.patient_key_reason"
SRC_PROVIDER = f"{FACTS}.provider_sk_reason"
SRC_LENGTH_OF_STAY = f"{FACTS}.length_of_stay_days_reason"
SRC_TIMESTAMP_WARNING = "raw.last_updated_ts.warning"  # re-parsed from the version's first-seen raw row
SRC_LINKAGE_NAME = "patient_identity.linkage_name"  # in memory only: an incomplete linkage key
SRC_OUTCOME = "encounter_row_outcomes.outcome_reason"
SRC_AUDIT = "batch_audit.reason"
SRC_SCHEMA_VERSION = "ingested_files.schema_version"  # an accepted file's matched version vs its source's previous one

# Audit reason / log code for a file whose exact bytes were already ingested
# (batch_processor.DUPLICATE_FILE; a test keeps the two equal).
DUPLICATE_FILE = "DUPLICATE_FILE"

# The code for a file whose matched schema version differs from its source system's previous
# accepted file. Like DUPLICATE_FILE it flags a file, so it is a code, never a config value.
SCHEMA_VERSION_CHANGED = "SCHEMA_VERSION_CHANGED"

# Version sources, in the order the version issues are derived.
VERSION_SOURCES = (
    SRC_FACILITY, SRC_ADMIT, SRC_DISCHARGE, SRC_DISCHARGE_WARNING, SRC_ENCOUNTER_TYPE, SRC_ENCOUNTER_TYPE_WARNING,
    SRC_CLAIM_STATUS, SRC_PAYER, SRC_PAYER_WARNING, SRC_DX, SRC_DX_WARNING, SRC_NPI, SRC_NPI_WARNING, SRC_AMOUNT,
    SRC_LINK, SRC_SEX, SRC_AGE_BAND, SRC_ZIP3, SRC_PATIENT_KEY, SRC_PROVIDER, SRC_LENGTH_OF_STAY,
    SRC_TIMESTAMP_WARNING,
)  # fmt: skip

F = FieldReason
DATE_CODES = (F.DATE_MISSING, F.DATE_PLACEHOLDER, F.DATE_UNPARSEABLE, F.DATE_INVALID, F.DATE_CENTURY_UNKNOWN,
              F.DATE_AFTER_DELIVERY)  # fmt: skip
INVALID_NPI_CODES = (F.NPI_MISSING, F.NPI_PLACEHOLDER, F.NPI_INVALID_FORMAT, F.NPI_CHECKSUM_FAILED)
UNPARSEABLE_TIMESTAMP_CODES = (F.TIMESTAMP_MISSING, F.TIMESTAMP_PLACEHOLDER, F.TIMESTAMP_UNPARSEABLE,
                               F.TIMESTAMP_INVALID, F.TIMESTAMP_NONEXISTENT_LOCAL_TIME)  # fmt: skip
MANIFEST_CODES = (ReasonCode.MANIFEST_MISSING, ReasonCode.MANIFEST_INVALID, ReasonCode.BATCH_ID_MISMATCH,
                  ReasonCode.UNKNOWN_SOURCE_SYSTEM, ReasonCode.FILE_MISSING, ReasonCode.UNEXPECTED_FILE)  # fmt: skip

CHECK, DERIVATIVE, CONSEQUENCE = Classification.CHECK, Classification.DERIVATIVE, Classification.CONSEQUENCE


def _entries(source: str, codes: Iterable[str], classification: Classification, check_code: str | None):
    return [((source, str(code)), (classification, check_code)) for code in codes]


_MAPPING = [
    *_entries(SRC_FACILITY, (F.FACILITY_MISSING, F.FACILITY_UNRESOLVED), CHECK, "FACILITY_RESOLVED"),
    *_entries(SRC_ADMIT, DATE_CODES, CHECK, "ADMIT_DATE_VALID"),
    *_entries(SRC_DISCHARGE, DATE_CODES, CHECK, "DISCHARGE_DATE_VALID"),
    *_entries(SRC_DISCHARGE_WARNING, (F.DISCHARGE_BEFORE_ADMIT,), CHECK, "DISCHARGE_NOT_BEFORE_ADMIT"),
    *_entries(SRC_ENCOUNTER_TYPE, (F.ENCOUNTER_TYPE_UNMAPPED,), CHECK, "ENCOUNTER_TYPE_MAPPED"),
    *_entries(SRC_ENCOUNTER_TYPE_WARNING, (F.ENCOUNTER_TYPE_MISSING,), CHECK, "ENCOUNTER_TYPE_MAPPED"),
    *_entries(SRC_CLAIM_STATUS, (F.CLAIM_STATUS_MISSING, F.CLAIM_STATUS_UNMAPPED), CHECK, "CLAIM_STATUS_MAPPED"),
    *_entries(SRC_PAYER, (F.PAYER_UNMAPPED,), CHECK, "PAYER_MAPPED"),
    *_entries(SRC_PAYER_WARNING, (F.PAYER_MISSING,), CHECK, "PAYER_MAPPED"),
    *_entries(SRC_DX, (F.DX_MISSING, F.DX_PLACEHOLDER, F.DX_ICD9, F.DX_UNPARSEABLE), CHECK, "PRIMARY_DX_VALID"),
    *_entries(SRC_DX_WARNING, (F.DX_NOT_IN_REFERENCE,), CHECK, "PRIMARY_DX_IN_REFERENCE"),
    *_entries(SRC_NPI, INVALID_NPI_CODES, CHECK, "ATTENDING_NPI_VALID"),
    *_entries(SRC_NPI_WARNING, (F.NPI_NOT_IN_ROSTER,), CHECK, "ATTENDING_NPI_IN_ROSTER"),
    *_entries(SRC_AMOUNT, (F.AMOUNT_MISSING, F.AMOUNT_PLACEHOLDER, F.AMOUNT_SPREADSHEET_ERROR, F.AMOUNT_UNPARSEABLE,
                           F.AMOUNT_FRACTIONAL_CENTS, F.AMOUNT_K_SUFFIX_NOT_ALLOWED), CHECK, "BILLED_AMOUNT_VALID"),
    *_entries(SRC_LINK, (F.PATIENT_UNLINKED_NO_DOB, F.PATIENT_UNLINKED_INCOMPLETE, F.PATIENT_UNLINKED_CONFLICT),
              CHECK, "PATIENT_LINKED"),
    *_entries(SRC_LINK, (F.PATIENT_MRN_MISSING,), DERIVATIVE, "PATIENT_KEY_PRESENT"),
    *_entries(SRC_LINKAGE_NAME, (F.LAST_NAME_MISSING, F.FIRST_NAME_MISSING), DERIVATIVE, "PATIENT_LINKED"),
    *_entries(SRC_SEX, (F.SEX_MISSING, F.SEX_UNMAPPED), CHECK, "SEX_KNOWN"),
    *_entries(SRC_AGE_BAND, (F.AGE_BAND_DOB_UNAVAILABLE, F.AGE_BAND_DOB_AFTER_ADMIT), CHECK, "AGE_BAND_KNOWN"),
    *_entries(SRC_AGE_BAND, (F.AGE_BAND_ADMIT_UNAVAILABLE,), DERIVATIVE, "ADMIT_DATE_VALID"),
    *_entries(SRC_ZIP3, (F.ZIP_MISSING, F.ZIP_INVALID), CHECK, "ZIP3_VALID"),
    *_entries(SRC_PATIENT_KEY, (F.PATIENT_MRN_MISSING,), CHECK, "PATIENT_KEY_PRESENT"),
    *_entries(SRC_PROVIDER, (F.PROVIDER_NOT_ON_ROSTER_AT_ADMIT,), CHECK, "PROVIDER_ON_ROSTER_AT_ADMIT"),
    *_entries(SRC_PROVIDER, INVALID_NPI_CODES, DERIVATIVE, "ATTENDING_NPI_VALID"),
    *_entries(SRC_PROVIDER, (F.NPI_NOT_IN_ROSTER,), DERIVATIVE, "ATTENDING_NPI_IN_ROSTER"),
    *_entries(SRC_PROVIDER, (F.PROVIDER_ADMIT_DATE_UNKNOWN,), DERIVATIVE, "ADMIT_DATE_VALID"),
    *_entries(SRC_LENGTH_OF_STAY, (F.LENGTH_OF_STAY_ADMIT_UNAVAILABLE,), DERIVATIVE, "ADMIT_DATE_VALID"),
    *_entries(SRC_LENGTH_OF_STAY, (F.LENGTH_OF_STAY_DISCHARGE_UNAVAILABLE,), DERIVATIVE, "DISCHARGE_DATE_VALID"),
    *_entries(SRC_LENGTH_OF_STAY, (F.DISCHARGE_BEFORE_ADMIT,), DERIVATIVE, "DISCHARGE_NOT_BEFORE_ADMIT"),
    *_entries(SRC_TIMESTAMP_WARNING, (F.TIMESTAMP_AMBIGUOUS_LOCAL_TIME,), CHECK, "LAST_UPDATED_TS_UNAMBIGUOUS"),
    # Task 4 row outcomes.
    *_entries(SRC_OUTCOME, (OutcomeReason.SOURCE_RECORD_ID_MISSING,), CHECK, "SOURCE_RECORD_ID_PRESENT"),
    *_entries(SRC_OUTCOME, UNPARSEABLE_TIMESTAMP_CODES, CHECK, "LAST_UPDATED_TS_VALID"),
    *_entries(SRC_OUTCOME, (OutcomeReason.VERSION_CONFLICT_SAME_TS,), CHECK, "VERSION_CONFLICT_SAME_TS"),
    *_entries(SRC_OUTCOME, (OutcomeReason.DUPLICATE_OF_HELD_VERSION, OutcomeReason.DUPLICATE_IN_BATCH),
              CHECK, "DUPLICATE_ROW"),
    *_entries(SRC_OUTCOME, (OutcomeReason.STALE_REPLAY, OutcomeReason.STALE_NEW_VERSION), CHECK, "STALE_ROW"),
    # Task 1 batch rejection codes and the duplicate-file flag, as stored in batch_audit.reason.
    *_entries(SRC_AUDIT, MANIFEST_CODES, CHECK, "MANIFEST_VALID"),
    *_entries(SRC_AUDIT, (ReasonCode.SHA256_MISMATCH,), CHECK, "FILE_SHA256_MATCH"),
    *_entries(SRC_AUDIT, (ReasonCode.ENCODING_ERROR,), CHECK, "FILE_ENCODING_VALID"),
    *_entries(SRC_AUDIT, (ReasonCode.CSV_PARSE_ERROR,), CHECK, "FILE_CSV_PARSEABLE"),
    *_entries(SRC_AUDIT, (ReasonCode.MALFORMED_RECORD,), CHECK, "RECORD_SHAPE_VALID"),
    *_entries(SRC_AUDIT, (ReasonCode.HEADER_MISSING, ReasonCode.UNKNOWN_SCHEMA_CHANGE), CHECK, "SCHEMA_CONTRACT_MATCH"),
    *_entries(SRC_AUDIT, (ReasonCode.ROW_COUNT_MISMATCH,), CHECK, "ROW_COUNT_MATCH"),
    *_entries(SRC_AUDIT, (ReasonCode.SIBLING_FILE_REJECTED,), CONSEQUENCE, None),
    *_entries(SRC_AUDIT, (ReasonCode.DQ_GATE_FAILED,), CHECK, "PUBLISH_GATE"),
    *_entries(SRC_AUDIT, (DUPLICATE_FILE,), CHECK, "DUPLICATE_FILE"),
    *_entries(SRC_SCHEMA_VERSION, (SCHEMA_VERSION_CHANGED,), CHECK, "SCHEMA_VERSION_CHANGED"),
]  # fmt: skip

CODE_MAP: dict[tuple[str, str], tuple[Classification, str | None]] = dict(_MAPPING)
if len(CODE_MAP) != len(_MAPPING):
    raise AssertionError("a (source, code) pair is classified twice")
for _classification, _check_code in CODE_MAP.values():
    if (_check_code is None) != (_classification == CONSEQUENCE) or (_check_code and _check_code not in CHECKS_BY_CODE):
        raise AssertionError("CODE_MAP refers to an unknown check")


def classify(source: str, code: str) -> tuple[Classification, str | None]:
    """How a reason code read from `source` is counted; PipelineError if it has no classification."""
    try:
        return CODE_MAP[(source, str(code))]
    except KeyError:
        raise PipelineError(f"reason code {code} from {source} has no DQ classification") from None


_LEADING_CODE = re.compile(r"[A-Z0-9_]+")


def audit_reason_codes(reason: str | None) -> list[str]:
    """The codes in a batch_audit reason, e.g. 'SHA256_MISMATCH; ROW_COUNT_MISMATCH(expected=22,received=18)'.

    Details in brackets are dropped; every code must be classified.
    """
    codes = []
    for part in (reason or "").split(";"):
        part = part.strip()
        if not part:
            continue
        match = _LEADING_CODE.match(part)
        if match is None:
            raise PipelineError("batch_audit reason has no leading code")
        classify(SRC_AUDIT, match.group())
        codes.append(match.group())
    return codes


def join_codes(codes: Iterable[str]) -> str | None:
    """Sorted, distinct, joined with '|'; None when there are none."""
    unique = sorted({str(c) for c in codes if c})
    return "|".join(unique) if unique else None


# --- the publish gate --------------------------------------------------------

# Row and version checks that are ERRORs: a row failing any of them counts against the gate,
# and is quarantined (Task 4 row quarantine) or flagged (a version ERROR).
GATE_CHECKS = tuple(
    c.code for c in CHECKS
    if c.severity == Severity.ERROR and c.grain in (Grain.ROW, Grain.VERSION) and not c.invariant
)
# Where the version ERRORs are read. The gate runs inside the batch transaction, before
# the patient and mart rebuilds, so every version ERROR must come from the cleaned fields.
GATE_VERSION_SOURCES = tuple(
    source for source in VERSION_SOURCES
    if any(src == source and kind == CHECK and CHECKS_BY_CODE[check].severity == Severity.ERROR
           for (src, _), (kind, check) in CODE_MAP.items())
)
if any(not source.startswith(f"{FIELDS}.") for source in GATE_VERSION_SOURCES):
    raise AssertionError("a version ERROR is read from outside clean.encounter_version_fields")
