"""The README queries and a PHI scan of every exported CSV, on the real data pack.

One run of the pipeline (batches 001-004 in one go) into tmp_path; the data
pack is only read. The scan prints nothing it finds: failures name a file,
a column and a kind of value, never the value.

PHI scan:
  MRN and phone   every cell of every CSV, as whole values and as tokens: no hit at all.
  patient names   name tokens of 3+ characters, every cell of every CSV except
                  dim_provider's provider names. Tokens that are controlled
                  vocabulary (reference files other than the roster's name
                  columns, config, the parsers' mapping tables, reason and enum
                  codes, column names) are not counted, and nor is the word TEST:
                  synthetic test records use it as a patient name and in the
                  documented facility value "TEST FACILITY - DO NOT USE". A
                  remaining hit is allowed only in a raw free-text column of
                  encounter_version_fields and only when it is not the row's own
                  patient; those coincidences are pinned, so a new one fails.
  DOB, full ZIP   row by row through each row's lineage: no cell may equal the
                  row's own DOB (as delivered or as an ISO date) or contain its
                  5-digit ZIP.
"""

import csv
import logging
import re
from collections import Counter
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path

import pytest
from conftest import (
    CONTRACT_PATH,
    REAL_DATA_DIR,
    REPO_ROOT,
    TEST_PATIENT_KEY_SECRET,
    attach,
    independent_monthly_totals,
)

from pipeline import encounter_history, patient_identity
from pipeline.exports import EXPORTED_TABLES, file_name
from pipeline.main import main
from pipeline.parsers import amount, categorical, patient, result
from pipeline.parsers.dates import parse_date
from pipeline.source_conventions import load_source_conventions

SQL_DIR = REPO_ROOT / "sql"
CONVENTIONS = load_source_conventions(REAL_DATA_DIR / "reference" / "source_systems_and_facilities.json")
EXPORT_FILES = sorted([*(file_name(t) for t in EXPORTED_TABLES), "batch_audit.csv"])
PROVIDER_NAME_COLUMNS = {("dim_provider.csv", "provider_last_name"), ("dim_provider.csv", "provider_first_name")}
GENERIC_MARKERS = {"TEST"}
LINEAGE_COLUMNS = {
    "encounter_versions.csv": ("first_seen_batch_id", "first_seen_file_name", "first_seen_source_row_number"),
    "encounter_row_outcomes.csv": ("batch_id", "file_name", "source_row_number"),
    "encounter_patients.csv": ("batch_id", "file_name", "source_row_number"),
}
SOURCE_LINEAGE = ("source_batch_id", "source_file_name", "source_row_number")


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("real_exports")
    env = {
        "DATA_DIR": str(REAL_DATA_DIR),
        "OUTPUT_DIR": str(tmp / "output"),
        "RAW_DB_PATH": str(tmp / "work" / "raw.duckdb"),
        "SCHEMA_CONTRACT_PATH": str(CONTRACT_PATH),
        "LOG_LEVEL": "WARNING",
        "PATIENT_KEY_HMAC_SECRET": TEST_PATIENT_KEY_SECRET,
    }
    logger = logging.getLogger("pipeline")
    handlers, level = logger.handlers[:], logger.level
    try:
        assert main(env) == 0
    finally:
        logger.handlers[:] = handlers
        logger.setLevel(level)
    con = attach(Path(env["RAW_DB_PATH"]))
    yield {"con": con, "output": Path(env["OUTPUT_DIR"])}
    con.close()


def query(con, name, **params):
    return con.execute((SQL_DIR / name).read_text(encoding="utf-8"), params).fetchall()


def by_month(rows):
    """Query 1/5 rows (year, month, facility, name, type, encounters, billed) summed per month."""
    totals = Counter()
    amounts = Counter()
    for year, month, _, _, _, encounters, billed in rows:
        totals[(year, month)] += encounters
        amounts[(year, month)] += billed or Decimal("0.00")
    return {m: (totals[m], amounts[m]) for m in totals}


# --- the six README queries ---


def test_q1_equals_the_independent_monthly_totals(run):
    rows = query(run["con"], "q1_monthly_volume_current.sql")

    assert by_month(rows) == independent_monthly_totals(run["con"])
    # Current encounters with an admit month, excluding VOID and Task 6 ERROR versions: 3,080 before
    # Task 6, less 30 non-VOID current versions with an unresolved facility (31, one of them VOID).
    # The 18 with an unusable admit date never had a month.
    assert sum(r[5] for r in rows) == 3050


def test_q5_as_of_batch_002_equals_the_independent_totals(run):
    rows = query(run["con"], "q5_monthly_volume_as_of.sql", as_of_batch="batch_002")

    assert by_month(rows) == independent_monthly_totals(run["con"], "batch_002")
    assert sum(r[5] for r in rows) == 2984  # 3,013 before Task 6, less 29 unresolved facilities known at batch_002


@pytest.mark.parametrize("batch", ["batch_003", "batch_004"])
def test_q5_as_of_the_latest_batch_is_query_1(run, batch):
    assert query(run["con"], "q5_monthly_volume_as_of.sql", as_of_batch=batch) == query(
        run["con"], "q1_monthly_volume_current.sql")


def test_q2_version_history(run):
    con = run["con"]
    system, record_id = con.execute(
        "SELECT source_system, source_record_id FROM mart.fact_encounter_current WHERE version_count = 3 "
        "ORDER BY encounter_key LIMIT 1").fetchone()

    rows = query(con, "q2_encounter_version_history.sql", source_system=system, source_record_id=record_id)

    assert len(rows) == 3
    assert [r[1] for r in rows] == sorted(r[1] for r in rows)  # ordered by last_updated_ts_utc
    assert [r[6] for r in rows] == [False, False, True]  # the latest version is current
    assert [r[2] for r in rows] == sorted(r[2] for r in rows)  # each version arrived no earlier than the last


def test_q3_lineage_resolves_to_the_raw_row_and_an_accepted_file(run):
    con = run["con"]
    keys = [r[0] for r in con.execute("SELECT encounter_key FROM mart.fact_encounter_current ORDER BY encounter_key").fetchall()]

    for key in keys[::50]:  # every 50th encounter
        [(_, system, record_id, batch, file, row, status, _)] = query(con, "q3_export_row_lineage.sql", encounter_key=key)
        assert status == "ACCEPTED"
        raw = con.execute(
            "SELECT source_record_id FROM raw.encounters WHERE batch_id = ? AND file_name = ? AND source_row_number = ?",
            [batch, file, row],
        ).fetchall()
        assert raw == [(record_id,)], key


def test_q4_provider_at_encounter(run):
    rows = query(run["con"], "q4_provider_at_encounter.sql")

    assert len(rows) == 3274
    with_provider = [r for r in rows if r[3] is not None]
    assert len(with_provider) == 3218
    assert all(valid_from <= admit < valid_to for _, admit, _, _, _, valid_from, valid_to, _ in with_provider)
    assert Counter(r[7] for r in rows if r[3] is None) == {
        "NPI_NOT_IN_ROSTER": 18, "PROVIDER_ADMIT_DATE_UNKNOWN": 18,
        "NPI_PLACEHOLDER": 2, "NPI_INVALID_FORMAT": 9, "NPI_CHECKSUM_FAILED": 4, "NPI_MISSING": 5,
    }  # fmt: skip


def test_q6_lists_the_rejected_batch_004_files_and_the_error_versions(run):
    rows = query(run["con"], "q6_quarantined_and_rejected.sql")

    assert len(rows) == 53
    assert [r for r in rows if r[0] == "FILE"] == [
        ("FILE", "BATCH_VALIDATION", "batch_004", "encounters_athena_clinics.csv", None, None, None, None,
         "SIBLING_FILE_REJECTED", "SIBLING_FILE_REJECTED"),
        ("FILE", "BATCH_VALIDATION", "batch_004", "encounters_epic_north.csv", None, None, None, None,
         "MALFORMED_RECORD|ROW_COUNT_MISMATCH|SHA256_MISMATCH",
         "SHA256_MISMATCH; MALFORMED_RECORD(record=18,fields=3,expected=21); ROW_COUNT_MISMATCH(expected=22,received=18)"),
        ("FILE", "BATCH_VALIDATION", "batch_004", "encounters_legacy_meditech.csv", None, None, None, None,
         "SIBLING_FILE_REJECTED", "SIBLING_FILE_REJECTED"),
    ]  # fmt: skip
    versions = [r for r in rows if r[0] == "VERSION"]
    assert Counter(r[2] for r in versions) == {"batch_001": 44, "batch_002": 5, "batch_003": 1}
    assert Counter(r[8] for r in versions) == {
        "FACILITY_UNRESOLVED": 32, "DATE_INVALID": 12, "DATE_AFTER_DELIVERY": 4, "DATE_PLACEHOLDER": 2}
    assert Counter(r[7] for r in versions) == {True: 49, False: 1}
    assert not [r for r in rows if r[0] == "ROW"]  # no Task 4 row quarantine in batches 001-003


def test_dq_csvs_on_the_real_data(run):
    output = run["output"]

    def rows(name):
        with (output / name).open(encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))

    issues, quarantine_rows, report = rows("version_dq_issues.csv"), rows("quarantine.csv"), rows("dq_report.csv")
    assert len(issues) == 1219 and Counter(r["severity"] for r in issues) == {"ERROR": 50, "WARNING": 1169}
    assert len(quarantine_rows) == 53
    assert len(report) == 621
    gate = [(r["batch_id"], r["observed_count"], r["observed_pct"], r["threshold_pct"], r["status"])
            for r in report if r["check_code"] == "PUBLISH_GATE"]  # fmt: skip
    assert gate == [
        ("batch_001", "44", "1.54", "5.00", "PASS"),
        ("batch_002", "5", "0.85", "5.00", "PASS"),
        ("batch_003", "1", "0.53", "5.00", "PASS"),
        ("batch_004", "", "", "5.00", "NOT_EVALUATED"),
    ]


# --- PHI scan of every exported CSV ---


def tokens(text):
    return {t.upper() for t in re.findall(r"[A-Za-z0-9]+", text or "")}


def controlled_vocabulary():
    vocab = set()
    for path in [*(REAL_DATA_DIR / "reference").rglob("*"), *(REPO_ROOT / "config").rglob("*")]:
        if not path.is_file():
            continue
        if path.parent.name == "provider_roster":  # roster names are people: not vocabulary
            with path.open(encoding="utf-8-sig", newline="") as handle:
                for row in csv.DictReader(handle):
                    vocab |= set().union(*(tokens(v) for k, v in row.items() if not k.startswith("provider_")))
        else:
            vocab |= tokens(path.read_text(encoding="utf-8-sig"))
    for table in (categorical.ENCOUNTER_TYPES, categorical.CLAIM_STATUSES, categorical.PAYER_CATEGORIES):
        vocab |= set().union(*(tokens(spelling) for spelling in table))
    for module in (categorical, result, encounter_history, patient, patient_identity):
        for value in vars(module).values():
            if isinstance(value, type) and issubclass(value, StrEnum):
                vocab |= set().union(*(tokens(member.value) for member in value))
    return vocab | result.PLACEHOLDER_TOKENS | set().union(*(tokens(e) for e in amount.SPREADSHEET_ERRORS))


def raw_phi(con):
    phi = {}
    for batch, file, row, system, delivered, mrn, first, last, phone, dob, zip_code in con.execute(
        "SELECT e.batch_id, e.file_name, e.source_row_number, i.source_system, i.delivered_at, e.patient_mrn, "
        "e.patient_first_name, e.patient_last_name, e.patient_phone, e.patient_dob, e.patient_zip "
        "FROM raw.encounters e JOIN raw.ingested_files i USING (batch_id, file_name)"
    ).fetchall():
        c = CONVENTIONS[system]
        parsed = parse_date(dob, date_order=c.date_order, delivered_at=datetime.fromisoformat(delivered),
                            two_digit_year_century=c.two_digit_year_century).cleaned_value
        phi[(batch, file, row)] = {
            "mrn": re.sub(r"[^A-Za-z0-9]", "", mrn or "").upper(),
            "names": {t for t in tokens(first) | tokens(last) if len(t) >= 3},
            "phone": re.sub(r"\D", "", phone or ""),
            "dob": {v for v in ((dob or "").strip(), parsed.isoformat() if parsed else "") if v},
            "zip": re.sub(r"\D", "", zip_code or "")[:5],
        }
    return phi


@pytest.fixture(scope="module")
def scan(run):
    phi = raw_phi(run["con"])
    mrns = {p["mrn"] for p in phi.values() if p["mrn"]}
    phones = {p["phone"] for p in phi.values() if len(p["phone"]) >= 7}
    names = set().union(*(p["names"] for p in phi.values()))
    vocab = controlled_vocabulary() | GENERIC_MARKERS
    found, lineage_rows = Counter(), 0
    for path in sorted(run["output"].glob("*.csv")):
        with path.open(encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            header_vocab = set().union(*(tokens(c) for c in reader.fieldnames))
            lineage = LINEAGE_COLUMNS.get(path.name) or (SOURCE_LINEAGE if "source_batch_id" in reader.fieldnames else None)
            for record in reader:
                own = phi[(record[lineage[0]], record[lineage[1]], int(record[lineage[2]]))] if lineage else None
                lineage_rows += own is not None
                for column, cell in record.items():
                    cell_tokens = tokens(cell)
                    if (cell_tokens | {re.sub(r"[^A-Za-z0-9]", "", cell).upper()}) & mrns:
                        found[(path.name, column, "MRN")] += 1
                    digits = re.sub(r"\D", "", cell)
                    if digits in phones or cell_tokens & phones:
                        found[(path.name, column, "PHONE")] += 1
                    if (path.name, column) not in PROVIDER_NAME_COLUMNS:
                        hit = (cell_tokens - vocab - header_vocab) & names
                        if hit:
                            kind = "OWN_NAME" if own and hit & own["names"] else "NAME_TOKEN"
                            found[(path.name, column, kind)] += 1
                    if own:
                        if cell.strip() in own["dob"]:
                            found[(path.name, column, "DOB")] += 1
                        if len(own["zip"]) == 5 and own["zip"] in re.findall(r"\d+", cell):
                            found[(path.name, column, "ZIP5")] += 1
    return {"found": found, "lineage_rows": lineage_rows, "name_tokens": names, "vocab": vocab}


def test_every_export_file_was_scanned(run):
    assert sorted(p.name for p in run["output"].glob("*.csv")) == EXPORT_FILES


def test_the_name_scan_is_not_hollowed_out_by_the_vocabulary(scan):
    # Of the patient name tokens, only a handful are also controlled vocabulary.
    assert len(scan["name_tokens"]) == 116
    assert len(scan["name_tokens"] & scan["vocab"]) == 3  # 2 vocabulary words, plus the TEST marker


def test_no_mrn_phone_dob_or_full_zip_in_any_export(scan):
    leaks = {k: v for k, v in scan["found"].items() if k[2] in ("MRN", "PHONE", "DOB", "ZIP5", "OWN_NAME")}

    assert leaks == {}
    assert scan["lineage_rows"] > 20000  # DOB and ZIP were checked row by row through lineage


def test_patient_name_tokens_only_as_pinned_coincidences(scan):
    """A free-text amount in 7 versions is a word that is also another patient's name, never the row's own."""
    names = {k: v for k, v in scan["found"].items() if k[2] == "NAME_TOKEN"}

    assert names == {("encounter_version_fields.csv", "billed_amount_raw", "NAME_TOKEN"): 7}
