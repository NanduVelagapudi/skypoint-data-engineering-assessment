"""Task 6 DQ report: ops.dq_report, one row per (batch_id, file_name, check_code).

Every check in dq_rules.CHECKS that applies gets a row, including checks with
nothing to report, so the report shows that each check ran:

    accepted batch   every file check for each file, plus MANIFEST_VALID and
                     PUBLISH_GATE (batch scope)
    rejected batch   the Task 1 structural checks for each file, as PASS, FAIL
                     or NOT_EVALUATED (an earlier failure in the same file
                     stopped them); every other file check NOT_EVALUATED with
                     reason BATCH_REJECTED; MANIFEST_VALID, PUBLISH_GATE and
                     REJECTED_BATCH_NOT_LOADED (batch scope). A batch rejected at
                     the manifest has no file list, so it gets only the batch
                     rows.
    gate-rejected    as a rejected batch, except that the error-level row and
    batch            version checks (dq_rules.GATE_CHECKS) are counted from
                     ops.gate_rejected_issues. Its versions were rolled back, so
                     these checks count rows there: evaluated_count is the
                     file's received rows and observed_count the failing rows.
Batch-scope rows have file_name NULL.

PUBLISH_GATE: evaluated_count = the batch's received rows, observed_count = its
rows failing an error-level check, threshold_pct = the configured threshold.
FAIL means more than the threshold share failed. For an accepted batch it is
recomputed from the stored tables with the same rules the gate used; a
rejected-by-gate batch is always FAIL; a batch rejected by Task 1 never reached
the gate (NOT_EVALUATED).

Counts (dq_rules describes the check kinds):
    evaluated_count  the population checked: 1 for a file or batch check, the
                     file's received rows for a row check, the versions first
                     seen in the file for a version check
    observed_count   QUALITY: units that fail; OBSERVATION: units counted;
                     RECONCILIATION: the measured side
    expected_count   RECONCILIATION only: the value observed_count must equal
    observed_pct     QUALITY and OBSERVATION: observed / evaluated x 100,
                     DECIMAL(7,2), rounded half-even; NULL otherwise
    threshold_pct    PUBLISH_GATE only: the threshold in percent
    reason_codes     the codes behind a FAIL or WARN, sorted and joined with
                     '|'; BATCH_REJECTED, or the blocking file codes, for
                     NOT_EVALUATED

Pipeline invariants are evaluated for every accepted file. A failure is a
pipeline bug, not a finding: it raises PipelineError inside the mart rebuild
transaction, so the mart and the DQ tables roll back together and the run
exits 1. Stored invariant rows are therefore always PASS.

The report is rebuilt on every run from the stored tables, with no run
timestamps, so the same state always gives the same rows. Some rows of an
earlier batch legitimately change when a later batch arrives (for example
RECON_CURRENT_ROWS, as versions are superseded). ORDER_BY is the report's
fixed ordering.
"""

from __future__ import annotations

import logging
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from decimal import ROUND_HALF_EVEN, Decimal
from functools import partial

import duckdb

from pipeline import dq_rules
from pipeline.batch_audit import BatchStatus
from pipeline.dimensions import insert_rows
from pipeline.dq_rules import CHECKS, CHECKS_BY_CODE, Check, Grain, Kind, Scope, Severity
from pipeline.encounter_history import CURRENT_VIEW, OUTCOMES_TABLE, VERSIONS_TABLE, MatchType, Outcome, OutcomeReason
from pipeline.errors import PipelineError, ReasonCode
from pipeline.manifest import MANIFEST_FILE_NAME
from pipeline.publish_gate import TABLE as GATE_TABLE
from pipeline.publish_gate import exceeds, threshold_pct
from pipeline.quarantine import HISTORY_ORDERING, VERSION_DQ
from pipeline.quarantine import TABLE as QUARANTINE_TABLE
from pipeline.version_dq import TABLE as ISSUES_TABLE
from pipeline.version_fields import TABLE as FIELDS_TABLE

log = logging.getLogger(__name__)

TABLE = "ops.dq_report"
ORDER_BY = "batch_id, file_name NULLS FIRST, check_code"

COLUMNS = (
    "batch_id",
    "batch_status",
    "scope",
    "file_name",
    "source_system",
    "check_code",
    "check_category",
    "layer",
    "grain",
    "severity",
    "evaluated_count",
    "observed_count",
    "expected_count",
    "observed_pct",
    "threshold_pct",
    "status",
    "reason_codes",
)

PASS, FAIL, WARN, NOT_EVALUATED = "PASS", "FAIL", "WARN", "NOT_EVALUATED"
BATCH_REJECTED = "BATCH_REJECTED"

_DDL = f"""
    batch_id VARCHAR NOT NULL,
    batch_status VARCHAR NOT NULL CHECK (batch_status IN ('{BatchStatus.ACCEPTED}', '{BatchStatus.REJECTED}')),
    scope VARCHAR NOT NULL CHECK (scope IN ('{Scope.FILE}', '{Scope.BATCH}')),
    file_name VARCHAR,
    source_system VARCHAR,
    check_code VARCHAR NOT NULL,
    check_category VARCHAR NOT NULL,
    layer VARCHAR NOT NULL,
    grain VARCHAR NOT NULL,
    severity VARCHAR NOT NULL CHECK (severity IN ('{Severity.ERROR}', '{Severity.WARNING}', '{Severity.INFO}')),
    evaluated_count INTEGER,
    observed_count INTEGER,
    expected_count INTEGER,
    observed_pct DECIMAL(7,2),
    threshold_pct DECIMAL(7,2),
    status VARCHAR NOT NULL CHECK (status IN ('{PASS}', '{FAIL}', '{WARN}', '{NOT_EVALUATED}')),
    reason_codes VARCHAR,
    CHECK ((scope = '{Scope.BATCH}') = (file_name IS NULL))"""

FILE_CHECKS = tuple(c for c in CHECKS if c.scope == Scope.FILE)
UNKNOWN_VALUED = ("age_band", "sex")  # NOT NULL; 'UNKNOWN' always carries a reason
_HISTORY_CREATING = (Outcome.NEW_ENCOUNTER, Outcome.NEW_VERSION)

FileKey = tuple[str, str]


# --- one row ---------------------------------------------------------------


def _pct(observed: int, evaluated: int) -> Decimal | None:
    if not evaluated:
        return None
    return (Decimal(observed) * 100 / Decimal(evaluated)).quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN)


def evaluated_status(check: Check, observed: int, expected: int | None, threshold: Decimal | None = None,
                     evaluated: int | None = None) -> str:  # fmt: skip
    if check.kind == Kind.GATE:
        return FAIL if exceeds(observed, evaluated, threshold) else PASS
    if check.kind == Kind.RECONCILIATION:
        return PASS if observed == expected else FAIL
    if check.kind == Kind.OBSERVATION or observed == 0:
        return PASS
    return FAIL if check.severity == Severity.ERROR else WARN


def report_row(
    check: Check,
    batch_id: str,
    batch_status: str,
    file_name: str | None,
    source_system: str | None,
    *,
    evaluated: int | None = None,
    observed: int | None = None,
    expected: int | None = None,
    status: str | None = None,
    reason_codes: Iterable[str] = (),
    threshold: Decimal | None = None,
) -> dict[str, object]:
    """One report row; status is derived from the counts unless given (NOT_EVALUATED)."""
    if status is None:
        status = evaluated_status(check, observed, expected, threshold, evaluated)
    pct = _pct(observed, evaluated) if check.kind != Kind.RECONCILIATION and observed is not None else None
    return {
        "batch_id": batch_id,
        "batch_status": batch_status,
        "scope": str(check.scope),
        "file_name": file_name,
        "source_system": source_system,
        "check_code": check.code,
        "check_category": str(check.category),
        "layer": str(check.layer),
        "grain": str(check.grain),
        "severity": str(check.severity),
        "evaluated_count": evaluated,
        "observed_count": observed,
        "expected_count": expected if check.kind == Kind.RECONCILIATION else None,
        "observed_pct": pct,
        "threshold_pct": threshold_pct(threshold) if threshold is not None else None,
        "status": status,
        "reason_codes": dq_rules.join_codes(reason_codes),
    }


# --- what the stored tables hold, per file ---------------------------------


def _per_file(con: duckdb.DuckDBPyConnection, sql: str) -> dict[FileKey, int]:
    return {(b, f): n for b, f, n in con.execute(sql).fetchall()}


class _State:
    """Counts read once from the stored tables, keyed by (batch_id, file_name)."""

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self.raw_rows = _per_file(con, "SELECT batch_id, file_name, count(*) FROM raw.encounters GROUP BY ALL")
        self.patient_rows = _per_file(con, "SELECT batch_id, file_name, count(*) FROM clean.encounter_patients GROUP BY ALL")
        self.versions = _per_file(con, f"SELECT first_seen_batch_id, first_seen_file_name, count(*) FROM {VERSIONS_TABLE} GROUP BY ALL")
        self.field_rows = _per_file(con, f"SELECT source_batch_id, source_file_name, count(*) FROM {FIELDS_TABLE} GROUP BY ALL")
        self.fact_rows = _per_file(con, "SELECT source_batch_id, source_file_name, count(*) FROM mart.fact_encounter_version GROUP BY ALL")
        self.current_rows = _per_file(con, "SELECT source_batch_id, source_file_name, count(*) FROM mart.fact_encounter_current GROUP BY ALL")
        self.schema_changes = {
            (b, f)
            for b, f, version, previous in con.execute(
                "SELECT batch_id, file_name, schema_version, "
                "lag(schema_version) OVER (PARTITION BY source_system ORDER BY batch_id) FROM raw.ingested_files"
            ).fetchall()
            if previous is not None and previous != version
        }
        self._read_outcomes(con)
        self._read_issues(con)
        self._read_gate(con)
        self.quarantine_rows = _per_file(
            con,
            f"SELECT batch_id, file_name, count(*) FROM {QUARANTINE_TABLE} "
            f"WHERE quarantine_source IN ('{HISTORY_ORDERING}', '{VERSION_DQ}') GROUP BY ALL",
        )
        self.error_versions = _per_file(
            con,
            f"SELECT source_batch_id, source_file_name, count(DISTINCT version_key) FROM {ISSUES_TABLE} "
            f"WHERE severity = '{Severity.ERROR}' GROUP BY ALL",
        )
        self.invariants = {code: _per_file(con, sql) for code, sql in _invariant_sql(con).items()}

    def _read_outcomes(self, con: duckdb.DuckDBPyConnection) -> None:
        self.outcome_rows: Counter[FileKey] = Counter()
        self.history_expected: Counter[FileKey] = Counter()  # outcomes that write a history version
        self.row_checks: dict[FileKey, Counter[str]] = defaultdict(Counter)
        self.row_reasons: dict[FileKey, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
        for b, f, outcome, reason, match, n in con.execute(
            f"SELECT batch_id, file_name, outcome, outcome_reason, match_type, count(*) FROM {OUTCOMES_TABLE} GROUP BY ALL"
        ).fetchall():
            key = (b, f)
            self.outcome_rows[key] += n
            if outcome in _HISTORY_CREATING or reason == OutcomeReason.STALE_NEW_VERSION:
                self.history_expected[key] += n
            if reason is not None:
                _, check_code = dq_rules.classify(dq_rules.SRC_OUTCOME, reason)
                self.row_checks[key][check_code] += n
                self.row_reasons[key][check_code].add(reason)
            if outcome == Outcome.STALE and match == MatchType.CONFLICT:
                self.row_checks[key]["STALE_REPLAY_CONFLICT"] += n
                self.row_reasons[key]["STALE_REPLAY_CONFLICT"].add(reason)

    def _read_gate(self, con: duckdb.DuckDBPyConnection) -> None:
        """Gate counts: recomputed for accepted batches, stored for the batches the gate rejected."""
        self.gate = {
            b: (received, errors)
            for b, received, errors in con.execute(f"""
                SELECT o.batch_id, count(*),
                       count(*) FILTER (WHERE o.outcome = '{Outcome.QUARANTINED}' OR e.version_key IS NOT NULL)
                FROM {OUTCOMES_TABLE} o
                LEFT JOIN (SELECT DISTINCT version_key FROM {ISSUES_TABLE} WHERE severity = '{Severity.ERROR}') e
                  USING (version_key)
                GROUP BY 1""").fetchall()
        }
        self.gate_rejected_rows = dict(con.execute(
            f"SELECT batch_id, count(DISTINCT (file_name, source_row_number)) FROM {GATE_TABLE} GROUP BY 1"
        ).fetchall())
        self.gate_counts: dict[FileKey, Counter[str]] = defaultdict(Counter)
        self.gate_reasons: dict[FileKey, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
        for b, f, check_code, reason, n in con.execute(
            f"SELECT batch_id, file_name, check_code, reason_code, count(DISTINCT source_row_number) "
            f"FROM {GATE_TABLE} GROUP BY ALL"
        ).fetchall():
            self.gate_counts[(b, f)][check_code] += n
            self.gate_reasons[(b, f)][check_code].add(reason)

    def _read_issues(self, con: duckdb.DuckDBPyConnection) -> None:
        self.issue_counts: dict[FileKey, Counter[str]] = defaultdict(Counter)
        self.issue_reasons: dict[FileKey, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
        for b, f, check_code, reason, n in con.execute(
            f"SELECT source_batch_id, source_file_name, check_code, reason_code, count(*) FROM {ISSUES_TABLE} GROUP BY ALL"
        ).fetchall():
            self.issue_counts[(b, f)][check_code] += n
            self.issue_reasons[(b, f)][check_code].add(reason)


def _invariant_sql(con: duckdb.DuckDBPyConnection) -> dict[str, str]:
    """Per accepted file: the measured value of each invariant whose expected value is 0, or a count."""
    fact = "mart.fact_encounter_version"
    fact_columns = [r[0] for r in con.execute(f"DESCRIBE {fact}").fetchall()]
    nullable = [c for c in fact_columns if f"{c}_reason" in fact_columns and c not in UNKNOWN_VALUED]
    consistency = " + ".join(
        [f"count(*) FILTER (WHERE ({c} IS NULL) <> ({c}_reason IS NOT NULL))" for c in nullable]
        + [f"count(*) FILTER (WHERE ({c} = 'UNKNOWN') <> ({c}_reason IS NOT NULL))" for c in UNKNOWN_VALUED]
    )
    by_file = "source_batch_id, source_file_name"

    def orphans(column: str, dim: str, key: str) -> str:
        return f"{column} IS NOT NULL AND {column} NOT IN (SELECT {key} FROM {dim})"

    def fk(condition: str) -> str:
        return f"SELECT {by_file}, count(*) FILTER (WHERE {condition}) FROM {fact} GROUP BY ALL"

    current_columns = "encounter_key, version_key, version_count, source_batch_id, source_file_name, source_row_number"
    duplicate_keys = " UNION ALL ".join(
        f"SELECT {b} AS b, {f} AS f, count(*) OVER (PARTITION BY {key}) AS n FROM {table}"
        for table, key, b, f in (
            (fact, "version_key", "source_batch_id", "source_file_name"),
            ("mart.fact_encounter_current", "encounter_key", "source_batch_id", "source_file_name"),
            (VERSIONS_TABLE, "version_key", "first_seen_batch_id", "first_seen_file_name"),
            (FIELDS_TABLE, "version_key", "source_batch_id", "source_file_name"),
            (ISSUES_TABLE, "version_key, check_code", "source_batch_id", "source_file_name"),
        )
    )
    return {
        "FK_FACT_FACILITY": fk(orphans("facility_id", "mart.dim_facility", "facility_id")),
        "FK_FACT_DIAGNOSIS": fk(orphans("primary_dx_code", "mart.dim_diagnosis", "icd10_code")),
        "FK_FACT_PAYER": fk(orphans("payer_category", "mart.dim_payer", "payer_category")),
        "FK_FACT_PROVIDER": fk(orphans("provider_sk", "mart.dim_provider", "provider_sk")),
        "FK_FACT_PATIENT": fk(orphans("patient_key", "mart.dim_patient", "patient_key")),
        "FK_FACT_DATE": fk(
            f"({orphans('admit_date', 'mart.dim_date', 'date_key')}) OR "
            f"({orphans('discharge_date', 'mart.dim_date', 'date_key')})"
        ),
        "VALUE_REASON_CONSISTENT": f"SELECT {by_file}, {consistency} FROM {fact} GROUP BY ALL",
        "CURRENT_MATCHES_HISTORY": f"""
            WITH a AS (SELECT {current_columns} FROM mart.fact_encounter_current),
                 b AS (SELECT {current_columns} FROM {CURRENT_VIEW})
            SELECT {by_file}, count(*) FROM (
                (SELECT * FROM a EXCEPT ALL SELECT * FROM b) UNION ALL (SELECT * FROM b EXCEPT ALL SELECT * FROM a)
            ) GROUP BY ALL""",
        "KEY_UNIQUE": f"SELECT b, f, count(*) FILTER (WHERE n > 1) FROM ({duplicate_keys}) GROUP BY ALL",
        # Each version once by its worst severity (pass, warning only, error), plus issues whose
        # version or lineage is not in the history: the sum must equal the versions of the file.
        "RECON_DQ_PARTITION": f"""
            WITH worst AS (
                SELECT version_key, max(CASE severity WHEN '{Severity.ERROR}' THEN 2 ELSE 1 END) AS rank
                FROM {ISSUES_TABLE} GROUP BY 1)
            SELECT b, f, sum(n) FROM (
                SELECT v.first_seen_batch_id AS b, v.first_seen_file_name AS f,
                       count(*) FILTER (WHERE w.rank IS NULL) + count(*) FILTER (WHERE w.rank = 1)
                       + count(*) FILTER (WHERE w.rank = 2) AS n
                FROM {VERSIONS_TABLE} v LEFT JOIN worst w USING (version_key) GROUP BY ALL
                UNION ALL
                SELECT i.source_batch_id, i.source_file_name, count(DISTINCT i.version_key)
                FROM {ISSUES_TABLE} i ANTI JOIN {VERSIONS_TABLE} v
                  ON v.version_key = i.version_key AND v.first_seen_batch_id = i.source_batch_id
                 AND v.first_seen_file_name = i.source_file_name
                 AND v.first_seen_source_row_number = i.source_row_number
                GROUP BY ALL
            ) GROUP BY ALL""",
    }


# Tables a rejected batch must have no rows in, with the column holding the batch.
_BATCH_COLUMNS = (
    ("raw.encounters", "batch_id"),
    ("raw.ingested_files", "batch_id"),
    (OUTCOMES_TABLE, "batch_id"),
    ("clean.encounter_patients", "batch_id"),
    (VERSIONS_TABLE, "first_seen_batch_id"),
    (FIELDS_TABLE, "source_batch_id"),
    (ISSUES_TABLE, "source_batch_id"),
    ("mart.fact_encounter_version", "source_batch_id"),
    ("mart.fact_encounter_current", "source_batch_id"),
)


def _rows_loaded_for_batch(con: duckdb.DuckDBPyConnection, batch_id: str) -> int:
    return sum(
        con.execute(f"SELECT count(*) FROM {table} WHERE {column} = ?", [batch_id]).fetchone()[0]
        for table, column in _BATCH_COLUMNS
    )


# --- rows per batch and file -----------------------------------------------


def _accepted_file_rows(state: _State, batch_id: str, audit: Mapping[str, object]) -> list[dict[str, object]]:
    file_name, system = audit["file_name"], audit["source_system"]
    key = (batch_id, file_name)
    received, versions = audit["received_count"], state.versions.get(key, 0)
    codes = dq_rules.audit_reason_codes(audit["reason"])
    quarantine_expected = audit["quarantined_count"] + state.error_versions.get(key, 0)
    measured = {
        "RECON_RAW_ROWS": (received, state.raw_rows.get(key, 0), received),
        "RECON_QUARANTINE_ROWS": (received, state.quarantine_rows.get(key, 0), quarantine_expected),
        "RECON_ROW_OUTCOMES": (received, state.outcome_rows[key], received),
        "RECON_PATIENT_ROWS": (received, state.patient_rows.get(key, 0), received),
        "RECON_HISTORY_ROWS": (versions, versions, state.history_expected[key]),
        "RECON_VERSION_FIELD_ROWS": (versions, state.field_rows.get(key, 0), versions),
        "RECON_FACT_VERSION_ROWS": (versions, state.fact_rows.get(key, 0), versions),
        "RECON_DQ_PARTITION": (versions, state.invariants["RECON_DQ_PARTITION"].get(key, 0), versions),
        **{code: (versions, counts.get(key, 0), 0) for code, counts in state.invariants.items()
           if code != "RECON_DQ_PARTITION"},
    }  # fmt: skip

    rows = []
    for check in FILE_CHECKS:
        row = partial(report_row, check, batch_id, BatchStatus.ACCEPTED, file_name, system)
        if check.code == "ROW_COUNT_MATCH":
            rows.append(row(evaluated=1, observed=received, expected=audit["expected_count"]))
        elif check.code in dq_rules.STRUCTURAL_FILE_CHECKS:
            rows.append(row(evaluated=1, observed=0))  # an accepted file passed every Task 1 check
        elif check.code == "SCHEMA_VERSION_CHANGED":
            changed = key in state.schema_changes
            rows.append(row(evaluated=1, observed=int(changed),
                            reason_codes=[dq_rules.SCHEMA_VERSION_CHANGED] if changed else []))
        elif check.code == "DUPLICATE_FILE":
            rows.append(row(evaluated=1, observed=int("DUPLICATE_FILE" in codes),
                            reason_codes=[c for c in codes if c == "DUPLICATE_FILE"]))
        elif check.grain == Grain.ROW and check.kind != Kind.RECONCILIATION:
            rows.append(row(evaluated=received, observed=state.row_checks[key][check.code],
                            reason_codes=state.row_reasons[key][check.code]))
        elif check.code in dq_rules.VERSION_CHECKS:
            rows.append(row(evaluated=versions, observed=state.issue_counts[key][check.code],
                            reason_codes=state.issue_reasons[key][check.code]))
        elif check.code == "RECON_CURRENT_ROWS":
            rows.append(row(evaluated=versions, observed=state.current_rows.get(key, 0)))
        else:
            evaluated, observed, expected = measured[check.code]
            rows.append(row(evaluated=evaluated, observed=observed, expected=expected))
    return rows


_PARSE_BLOCKERS = (ReasonCode.ENCODING_ERROR, ReasonCode.CSV_PARSE_ERROR, ReasonCode.HEADER_MISSING)


def structural_results(codes: Sequence[str], received: int | None) -> dict[str, tuple[str, list[str]]]:
    """Task 1 check -> (status, reason codes) for a file of a rejected batch, from its audit codes.

    A check fails when its code is present. A check the file never reached is
    NOT_EVALUATED, with the codes that stopped it: after an encoding error the
    CSV was not parsed; when the file was not read to the end (received_count
    NULL) the record shape, header match and row count are unknown.
    """
    present = set(codes)
    blockers = [c for c in codes if c in _PARSE_BLOCKERS]
    by_check: dict[str, list[str]] = defaultdict(list)
    for code in codes:
        _, check_code = dq_rules.classify(dq_rules.SRC_AUDIT, code)
        if check_code is not None:
            by_check[check_code].append(code)

    results = {}
    for check_code in dq_rules.STRUCTURAL_FILE_CHECKS:
        if by_check[check_code]:
            results[check_code] = (FAIL, by_check[check_code])
        elif check_code == "FILE_CSV_PARSEABLE" and ReasonCode.ENCODING_ERROR in present:
            results[check_code] = (NOT_EVALUATED, [ReasonCode.ENCODING_ERROR])
        elif check_code in ("RECORD_SHAPE_VALID", "SCHEMA_CONTRACT_MATCH", "ROW_COUNT_MATCH") and received is None:
            results[check_code] = (NOT_EVALUATED, blockers)
        else:
            results[check_code] = (PASS, [])
    return results


def _rejected_file_rows(state: _State, batch_id: str, audit: Mapping[str, object]) -> list[dict[str, object]]:
    file_name, system, received = audit["file_name"], audit["source_system"], audit["received_count"]
    codes = dq_rules.audit_reason_codes(audit["reason"])
    structural = structural_results(codes, received)
    by_gate = ReasonCode.DQ_GATE_FAILED in codes
    key = (batch_id, file_name)
    rows = []
    for check in FILE_CHECKS:
        row = partial(report_row, check, batch_id, BatchStatus.REJECTED, file_name, system)
        if by_gate and check.code in dq_rules.GATE_CHECKS:  # counted on rows: the versions were rolled back
            rows.append(row(evaluated=received, observed=state.gate_counts[key][check.code],
                            reason_codes=state.gate_reasons[key][check.code]))
            continue
        if check.code not in structural:
            rows.append(row(status=NOT_EVALUATED, reason_codes=[BATCH_REJECTED]))
            continue
        status, reasons = structural[check.code]
        if status == NOT_EVALUATED:
            rows.append(row(status=NOT_EVALUATED, reason_codes=reasons))
        elif check.code == "ROW_COUNT_MATCH":
            rows.append(row(evaluated=1, observed=received, expected=audit["expected_count"], reason_codes=reasons))
        else:
            rows.append(row(evaluated=1, observed=int(status == FAIL), reason_codes=reasons))
    return rows


def _gate_row(state: _State, batch_id: str, status: str, audits: Sequence[Mapping[str, object]],
              max_error_share: Decimal) -> dict[str, object]:  # fmt: skip
    check = CHECKS_BY_CODE["PUBLISH_GATE"]
    row = partial(report_row, check, batch_id, status, None, None, threshold=max_error_share)
    if status == BatchStatus.ACCEPTED:
        received, errors = state.gate.get(batch_id, (0, 0))
        return row(evaluated=received, observed=errors)
    if any(ReasonCode.DQ_GATE_FAILED in dq_rules.audit_reason_codes(a["reason"]) for a in audits):
        received = sum(a["received_count"] for a in audits)
        return row(evaluated=received, observed=state.gate_rejected_rows.get(batch_id, 0), status=FAIL,
                   reason_codes=[ReasonCode.DQ_GATE_FAILED])
    return row(status=NOT_EVALUATED, reason_codes=[BATCH_REJECTED])


def report_rows(con: duckdb.DuckDBPyConnection, max_error_share: Decimal) -> list[dict[str, object]]:
    """Every report row, in ORDER_BY order. Raises PipelineError if an invariant fails."""
    audit_columns = ("batch_id", "file_name", "source_system", "expected_count", "received_count",
                     "quarantined_count", "status", "reason")  # fmt: skip
    by_batch: dict[str, list[dict[str, object]]] = defaultdict(list)
    for record in con.execute(
        f"SELECT {', '.join(audit_columns)} FROM ops.batch_audit ORDER BY batch_id, file_name"
    ).fetchall():
        values = dict(zip(audit_columns, record, strict=True))
        by_batch[values["batch_id"]].append(values)

    state = _State(con)
    rows: list[dict[str, object]] = []
    for batch_id, audits in by_batch.items():
        status = audits[0]["status"]
        manifest = [a for a in audits if a["file_name"] == MANIFEST_FILE_NAME]
        manifest_codes = dq_rules.audit_reason_codes(manifest[0]["reason"]) if manifest else []
        rows.append(report_row(CHECKS_BY_CODE["MANIFEST_VALID"], batch_id, status, None, None,
                               evaluated=1, observed=int(bool(manifest)), reason_codes=manifest_codes))
        rows.append(_gate_row(state, batch_id, status, audits, max_error_share))
        if status == BatchStatus.REJECTED:
            rows.append(report_row(CHECKS_BY_CODE["REJECTED_BATCH_NOT_LOADED"], batch_id, status, None, None,
                                   evaluated=1, observed=_rows_loaded_for_batch(con, batch_id), expected=0))
        for audit in audits:
            if audit["file_name"] == MANIFEST_FILE_NAME:
                continue
            if status == BatchStatus.ACCEPTED:
                rows.extend(_accepted_file_rows(state, batch_id, audit))
            else:
                rows.extend(_rejected_file_rows(state, batch_id, audit))

    _raise_on_failed_invariant(rows)
    rows.sort(key=lambda r: (r["batch_id"], r["file_name"] is not None, r["file_name"] or "", r["check_code"]))
    keys = [(r["batch_id"], r["file_name"], r["check_code"]) for r in rows]
    if len(set(keys)) != len(keys):
        raise PipelineError("duplicate dq_report row")
    return rows


def _raise_on_failed_invariant(rows: Iterable[Mapping[str, object]]) -> None:
    for row in rows:
        if CHECKS_BY_CODE[row["check_code"]].invariant and row["status"] == FAIL:
            log.error(
                "dq_invariant_failed",
                extra={"step": "dq", "batch_id": row["batch_id"], "file_name": row["file_name"],
                       "reason_code": row["check_code"]},
            )  # fmt: skip
            raise PipelineError(f"DQ invariant {row['check_code']} failed for {row['batch_id']}")


def write_dq_report(con: duckdb.DuckDBPyConnection, max_error_share: Decimal) -> dict[str, int]:
    """Drop and recreate ops.dq_report inside the caller's transaction; returns its row count.

    Needs clean.version_dq_issues, ops.quarantine and the facts, so it runs last in the mart rebuild.
    """
    rows = report_rows(con, max_error_share)
    con.execute("CREATE SCHEMA IF NOT EXISTS ops")
    con.execute(f"CREATE OR REPLACE TABLE {TABLE} ({_DDL})")
    insert_rows(con, TABLE, rows)
    statuses = Counter(row["status"] for row in rows)
    log.info("dq_report_built", extra={"step": "dq", "error_count": statuses[FAIL], "warning_count": statuses[WARN]})
    return {TABLE: len(rows)}
