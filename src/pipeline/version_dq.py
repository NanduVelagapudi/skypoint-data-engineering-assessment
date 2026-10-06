"""Task 6 version-level DQ issues: clean.version_dq_issues.

One row per (version_key, check_code) for every version check a version
fails, with the reason code that caused it and the check's severity. A
version with no row passes every version check. Lineage is the version's
first arrival, as in the facts.

The table holds keys, lineage, codes and severities only: no field values,
raw or cleaned. The facts and the history are not touched: a version with an
ERROR issue stays in fact_encounter_version and, when it is the latest, in
fact_encounter_current. Analytics exclude it by joining this table.

The codes are read from where earlier stages store them (dq_rules.VERSION_SOURCES):
    clean.encounter_version_fields   field reasons and warnings
    clean.encounter_patients         link, sex, age band and ZIP3 reasons of the
                                     version's first-seen row
    mart.fact_encounter_version      patient_key, provider and length-of-stay
                                     reasons, which the mart computes
    raw.encounters.last_updated_ts   re-parsed with the source's conventions for
                                     the ambiguous-local-time warning, which is
                                     not stored anywhere else (a non-PHI field)
Derivative codes are classified but not counted (dq_rules.CODE_MAP).

The table is rebuilt on every run, inside the mart rebuild transaction,
because encounter_patients is rebuilt on every run and a re-link can change a
version's patient warnings. The same inputs always give the same rows.
"""

from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass

import duckdb

from pipeline import dq_rules
from pipeline.dimensions import insert_rows
from pipeline.dq_rules import CHECKS_BY_CODE, VERSION_SOURCES, Classification, Severity
from pipeline.encounter_history import VERSIONS_TABLE
from pipeline.errors import PipelineError
from pipeline.parsers.timestamps import parse_timestamp
from pipeline.source_conventions import SourceConventions

log = logging.getLogger(__name__)

TABLE = "clean.version_dq_issues"

COLUMNS = (
    "version_key",
    "encounter_key",
    "source_system",
    "source_record_id",
    "source_batch_id",
    "source_file_name",
    "source_row_number",
    "check_code",
    "field_name",
    "reason_code",
    "severity",
)

_DDL = f"""
    version_key VARCHAR NOT NULL, encounter_key VARCHAR NOT NULL,
    source_system VARCHAR NOT NULL, source_record_id VARCHAR NOT NULL,
    source_batch_id VARCHAR NOT NULL, source_file_name VARCHAR NOT NULL, source_row_number INTEGER NOT NULL,
    check_code VARCHAR NOT NULL, field_name VARCHAR NOT NULL, reason_code VARCHAR NOT NULL,
    severity VARCHAR NOT NULL CHECK (severity IN ('{Severity.ERROR}', '{Severity.WARNING}')),
    PRIMARY KEY (version_key, check_code)"""

# Source column -> the SQL expression that reads it (the timestamp is re-parsed in Python).
_SQL_SOURCES = {
    source: f"{alias}.{source.split('.', 1)[1]}"
    for source, alias in (
        *((s, "f") for s in VERSION_SOURCES if s.startswith(f"{dq_rules.FIELDS}.")),
        *((s, "p") for s in VERSION_SOURCES if s.startswith(f"{dq_rules.PATIENTS}.")),
        *((s, "fv") for s in VERSION_SOURCES if s.startswith(f"{dq_rules.FACTS}.")),
    )
}

_VERSION_ROWS = f"""
SELECT v.version_key, v.encounter_key, v.source_system, v.source_record_id,
       v.first_seen_batch_id, v.first_seen_file_name, v.first_seen_source_row_number,
       e.last_updated_ts, {", ".join(_SQL_SOURCES.values())}
FROM {VERSIONS_TABLE} AS v
JOIN clean.encounter_version_fields AS f USING (version_key)
JOIN clean.encounter_patients AS p
  ON p.batch_id = v.first_seen_batch_id
 AND p.file_name = v.first_seen_file_name
 AND p.source_row_number = v.first_seen_source_row_number
JOIN mart.fact_encounter_version AS fv USING (version_key)
JOIN raw.encounters AS e
  ON e.batch_id = v.first_seen_batch_id
 AND e.file_name = v.first_seen_file_name
 AND e.source_row_number = v.first_seen_source_row_number
ORDER BY v.version_key
"""


@dataclass(frozen=True)
class Issue:
    check_code: str
    field_name: str
    reason_code: str
    severity: Severity


def derive_issues(codes: Mapping[str, str | None]) -> list[Issue]:
    """The issues of one version from its reason codes by source; pure.

    Every non-NULL code must be classified. Derivative codes and consequences
    add nothing. At most one issue per check: two sources feeding one check
    (a reason and a warning on the same field) are mutually exclusive by
    construction, so a second one is a pipeline error.
    """
    issues: dict[str, Issue] = {}
    for source in VERSION_SOURCES:
        code = codes.get(source)
        if code is None:
            continue
        classification, check_code = dq_rules.classify(source, code)
        if classification != Classification.CHECK:
            continue
        check = CHECKS_BY_CODE[check_code]
        if check_code in issues:
            raise PipelineError(f"two reason codes for the version check {check_code}")
        issues[check_code] = Issue(check_code, check.field_name, str(code), check.severity)
    return sorted(issues.values(), key=lambda i: i.check_code)


def timestamp_warning(raw_ts: str | None, convention: SourceConventions) -> str | None:
    """The parser's warning for a version's last_updated_ts (ambiguous local time), if any."""
    result = parse_timestamp(raw_ts, timezone_name=convention.timestamp_timezone, date_order=convention.date_order)
    return result.warning_flag and str(result.warning_flag)


def issue_rows(con: duckdb.DuckDBPyConnection, conventions: Mapping[str, SourceConventions]) -> list[dict[str, object]]:
    """Every version's issues as table rows, ordered by (version_key, check_code)."""
    cursor = con.execute(_VERSION_ROWS)
    names = [d[0] for d in cursor.description]
    records = cursor.fetchall()
    versions = con.execute(f"SELECT count(*) FROM {VERSIONS_TABLE}").fetchone()[0]
    if len(records) != versions:
        raise PipelineError("a version lacks its cleaned fields, patient row, fact row or raw row")

    sources = list(_SQL_SOURCES)
    rows = []
    for record in records:
        named = dict(zip(names, record, strict=True))
        codes = dict(zip(sources, record[8:], strict=True))
        codes[dq_rules.SRC_TIMESTAMP_WARNING] = timestamp_warning(
            named["last_updated_ts"], conventions[named["source_system"]]
        )
        for issue in derive_issues(codes):
            rows.append({
                "version_key": named["version_key"],
                "encounter_key": named["encounter_key"],
                "source_system": named["source_system"],
                "source_record_id": named["source_record_id"],
                "source_batch_id": named["first_seen_batch_id"],
                "source_file_name": named["first_seen_file_name"],
                "source_row_number": named["first_seen_source_row_number"],
                "check_code": issue.check_code,
                "field_name": issue.field_name,
                "reason_code": issue.reason_code,
                "severity": str(issue.severity),
            })  # fmt: skip
    return rows


def write_version_issues(
    con: duckdb.DuckDBPyConnection, conventions: Mapping[str, SourceConventions]
) -> dict[str, int]:
    """Drop and recreate clean.version_dq_issues inside the caller's transaction; returns its row count.

    Needs the facts, so it runs after warehouse.write_facts.
    """
    rows = issue_rows(con, conventions)
    con.execute("CREATE SCHEMA IF NOT EXISTS clean")
    con.execute(f"CREATE OR REPLACE TABLE {TABLE} ({_DDL})")
    insert_rows(con, TABLE, rows)
    severities = Counter(row["severity"] for row in rows)
    log.info(
        "version_dq_issues_built",
        extra={"step": "dq", "error_count": severities[Severity.ERROR], "warning_count": severities[Severity.WARNING]},
    )
    return {TABLE: len(rows)}
