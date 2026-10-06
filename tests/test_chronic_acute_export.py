"""Task 7: chronic_acute_encounters.csv.

Synthetic batches run through main() into tmp_path, with the real reference
files (read only). Rows are valid apart from the changes each test makes, and
every row has the same fake patient unless a test gives it another first name
(patients link on last name, first name, DOB and sex). The real-data row
count, readmission count and byte-level parity are pinned in
test_incremental_parity.py; the real-data PHI scan in test_real_data_exports.py
covers this file too.
"""

import csv
import shutil
from pathlib import Path

import pytest
from conftest import (
    REAL_DATA_DIR,
    FileSpec,
    attach,
    contract_header,
    csv_bytes,
    make_pre_task4,
    write_batch,
)

from pipeline.chronic_acute_export import COLUMNS, FILE_NAME, QUERY, export_rows
from pipeline.errors import PipelineError
from pipeline import exports
from pipeline.exports import check_no_phi_columns, format_value
from pipeline.main import main
from pipeline.reference_data import load_warehouse_reference

pytestmark = pytest.mark.usefixtures("restore_pipeline_logger")

T1, T2 = "2024-03-01T10:00:00Z", "2024-03-02T10:00:00Z"
FILES = {"EPIC_NORTH": "encounters_epic_north.csv", "ATHENA_CLINICS": "encounters_athena_clinics.csv"}
FACILITIES = {"EPIC_NORTH": "Lakeshore General Hospital", "ATHENA_CLINICS": "Cedar Valley Family Clinic"}

# The brief's column order, written out again so a reordering of COLUMNS fails here.
BRIEF_COLUMNS = [
    "encounter_key", "source_system", "source_record_id", "facility_id", "facility_name", "facility_type",
    "patient_key", "age_band", "sex", "patient_zip3", "admit_date", "discharge_date", "length_of_stay_days",
    "encounter_type", "primary_dx_code", "dx_description", "chronic_category", "attending_npi",
    "attending_specialty_at_encounter", "attending_employment_status_at_encounter", "payer_category",
    "billed_amount_usd", "claim_status", "readmit_30d_flag", "version_count", "source_batch_id",
    "source_file_name", "source_row_number",
]  # fmt: skip


def encounters(rows, *, npi, system="EPIC_NORTH", ts=T1) -> FileSpec:
    """A file whose rows are valid (and qualify for the export) apart from the given changes."""
    header = contract_header(system)
    records = []
    for record_id, changes in rows:
        values = {
            "source_system": system, "source_record_id": record_id, "facility_name": FACILITIES[system],
            "patient_mrn": f"MRN-zz-{record_id or 'none'}", "patient_first_name": "Testfirst",
            "patient_last_name": "Testlast", "patient_dob": "01/02/1980", "patient_sex": "F", "patient_zip": "00000",
            "patient_phone": "000-0000", "admit_date": "03/05/2024", "discharge_date": "03/07/2024",
            "encounter_type": "IP", "attending_npi": npi, "attending_provider_name": "x", "primary_dx_code": "E11.9",
            "chief_complaint": "x", "payer_name": "Medicare", "billed_amount": "$5,000.00", "claim_status": "Paid",
            "last_updated_ts": ts,
        }  # fmt: skip
        values.update(changes)
        records.append([values.get(column, "") for column in header])
    return FileSpec(FILES[system], system, csv_bytes(header, records), len(records))


def read_export(env) -> tuple[list[str], list[dict[str, str]]]:
    with (Path(env["OUTPUT_DIR"]) / FILE_NAME).open(encoding="utf-8", newline="") as handle:
        header, *rows = list(csv.reader(handle))
    return header, [dict(zip(header, row, strict=True)) for row in rows]


def exported(env) -> dict[str, dict[str, str]]:
    """Export rows by source_record_id."""
    return {row["source_record_id"]: row for row in read_export(env)[1]}


def load(landing_dir, env, *batches) -> dict[str, dict[str, str]]:
    for batch_id, specs in batches:
        write_batch(landing_dir, batch_id, specs)
        assert main(env) == 0, batch_id
    return exported(env)


# --- the six conditions ---

CONDITION_ROWS = [
    ("KEEP", {}),
    # 1. facility resolved
    ("C1_UNRESOLVED", {"facility_name": "Nowhere Clinic"}),
    ("C1_OTHER_FACILITY", {"facility_name": "St. Brendan Medical Center"}),
    # 2. claim status is not VOID (IS DISTINCT FROM: a NULL claim status is kept)
    ("C2_VOID", {"claim_status": "Void"}),
    ("C2_NULL", {"claim_status": ""}),
    ("C2_DENIED", {"claim_status": "Denied"}),
    ("C2_SUBMITTED", {"claim_status": "Pending"}),
    # 3. encounter type
    ("C3_OBSERVATION", {"encounter_type": "OBS"}),
    ("C3_EMERGENCY", {"encounter_type": "ED"}),
    ("C3_OUTPATIENT", {"encounter_type": "OP"}),
    ("C3_URGENT_CARE", {"encounter_type": "UC"}),
    ("C3_MISSING", {"encounter_type": ""}),
    # 4. primary diagnosis in the reference with is_chronic = Y
    ("C4_OTHER_CHRONIC", {"primary_dx_code": "J44.9"}),
    ("C4_NOT_CHRONIC", {"primary_dx_code": "F32.9"}),
    ("C4_NOT_IN_REFERENCE", {"primary_dx_code": "Z99.89"}),  # valid, but is_chronic unknown
    ("C4_INVALID", {"primary_dx_code": "250.00"}),  # ICD-9
    ("C4_MISSING", {"primary_dx_code": ""}),
    # 5. admit date in calendar 2024
    ("C5_2023", {"admit_date": "12/31/2023", "discharge_date": "01/02/2024"}),
    ("C5_FIRST_DAY", {"admit_date": "01/01/2024", "discharge_date": "01/02/2024"}),
    ("C5_LAST_DAY", {"admit_date": "12/31/2024", "discharge_date": "01/02/2025"}),
    ("C5_2025", {"admit_date": "01/01/2025", "discharge_date": "01/03/2025"}),
    ("C5_INVALID", {"admit_date": "02/30/2024"}),
    # 6. valid billed amount of at least 5,000.00
    ("C6_BELOW", {"billed_amount": "$4,999.99"}),
    ("C6_ABOVE", {"billed_amount": "$12,345.60"}),
    ("C6_PLACEHOLDER", {"billed_amount": "N/A"}),
    ("C6_MISSING", {"billed_amount": ""}),
]
KEPT = {
    "KEEP", "C1_OTHER_FACILITY", "C2_NULL", "C2_DENIED", "C2_SUBMITTED", "C3_OBSERVATION", "C3_EMERGENCY",
    "C4_OTHER_CHRONIC", "C5_FIRST_DAY", "C5_LAST_DAY", "C6_ABOVE",
}  # fmt: skip


@pytest.fixture
def conditions(landing_dir, pipeline_env, roster_npi):
    rows = load(landing_dir, pipeline_env, ("batch_001", [encounters(CONDITION_ROWS, npi=roster_npi)]))
    return pipeline_env, rows


def test_only_rows_meeting_all_six_conditions_are_exported(conditions):
    _, rows = conditions

    assert set(rows) == KEPT


def test_a_null_claim_status_is_kept(conditions):
    _, rows = conditions

    assert rows["C2_NULL"]["claim_status"] == ""
    assert rows["C2_DENIED"]["claim_status"] == "DENIED"


def test_exported_values(conditions, roster_npi):
    env, rows = conditions
    keep = rows["KEEP"]

    assert {k: keep[k] for k in (
        "source_system", "facility_id", "facility_name", "facility_type", "admit_date", "discharge_date",
        "length_of_stay_days", "encounter_type", "primary_dx_code", "chronic_category", "attending_npi",
        "payer_category", "billed_amount_usd", "claim_status", "version_count", "source_batch_id",
        "source_file_name", "source_row_number")} == {
        "source_system": "EPIC_NORTH", "facility_id": "FAC001", "facility_name": "Lakeshore General Hospital",
        "facility_type": "Acute Care Hospital", "admit_date": "2024-03-05", "discharge_date": "2024-03-07",
        "length_of_stay_days": "2", "encounter_type": "INPATIENT", "primary_dx_code": "E11.9",
        "chronic_category": "DIABETES", "attending_npi": roster_npi, "payer_category": "MEDICARE",
        "billed_amount_usd": "5000.00", "claim_status": "PAID", "version_count": "1", "source_batch_id": "batch_001",
        "source_file_name": "encounters_epic_north.csv", "source_row_number": "1",
    }  # fmt: skip
    assert keep["dx_description"] == "Type 2 diabetes mellitus without complications"
    assert rows["C4_OTHER_CHRONIC"]["chronic_category"] == "COPD"
    assert rows["C1_OTHER_FACILITY"]["facility_id"] == "FAC002"
    con = attach(Path(env["RAW_DB_PATH"]))
    try:
        fact = con.execute(
            "SELECT encounter_key, patient_key, age_band, sex, zip3 FROM mart.fact_encounter_current "
            "WHERE source_record_id = 'KEEP'").fetchone()  # fmt: skip
    finally:
        con.close()
    assert (keep["encounter_key"], keep["patient_key"], keep["age_band"], keep["sex"], keep["patient_zip3"]) == tuple(
        format_value(v) for v in fact)
    assert keep["patient_key"] != ""


def test_number_and_date_formats(conditions):
    env, rows = conditions
    content = (Path(env["OUTPUT_DIR"]) / FILE_NAME).read_bytes()

    assert b"\r" not in content and content.endswith(b"\n")  # LF line endings
    assert rows["C6_ABOVE"]["billed_amount_usd"] == "12345.60"
    assert rows["C5_LAST_DAY"]["admit_date"] == "2024-12-31" and rows["C5_LAST_DAY"]["discharge_date"] == "2025-01-02"
    assert rows["C5_FIRST_DAY"]["length_of_stay_days"] == "1"
    assert rows["KEEP"]["readmit_30d_flag"] in ("0", "1") and rows["C3_EMERGENCY"]["readmit_30d_flag"] == ""
    assert not [p for p in Path(env["OUTPUT_DIR"]).iterdir() if p.suffix == ".tmp"]


def test_the_columns_are_in_the_brief_order(conditions):
    header, _ = read_export(conditions[0])

    assert header == BRIEF_COLUMNS == list(COLUMNS)


def test_the_export_is_rewritten_identically(conditions):
    env, _ = conditions
    before = (Path(env["OUTPUT_DIR"]) / FILE_NAME).read_bytes()

    assert main(env) == 0

    assert (Path(env["OUTPUT_DIR"]) / FILE_NAME).read_bytes() == before


# --- Task 6 ERROR versions ---


def test_a_current_error_version_is_left_out_with_no_fallback(landing_dir, pipeline_env, roster_npi):
    rows = load(
        landing_dir, pipeline_env,
        ("batch_001", [encounters([("E1", {}), ("E2", {})], npi=roster_npi)]),
        ("batch_002", [encounters([("E1", {"facility_name": "Nowhere Clinic"})], npi=roster_npi, ts=T2)]),
    )  # fmt: skip

    assert set(rows) == {"E2"}  # E1's batch_001 version qualified, but its current version is an ERROR


def test_the_error_clause_excludes_a_version_on_its_own(conditions):
    """An ERROR in clean.version_dq_issues excludes a version that meets all six conditions; a WARNING does not."""
    env, _ = conditions
    con = attach(Path(env["RAW_DB_PATH"]))
    try:
        assert {r[2] for r in export_rows(con)} == KEPT
        for record_id, severity in (("KEEP", "ERROR"), ("C6_ABOVE", "WARNING")):
            con.execute(
                "INSERT INTO clean.version_dq_issues "
                "SELECT version_key, encounter_key, source_system, source_record_id, source_batch_id, "
                "       source_file_name, source_row_number, 'ZZ_PROBE', 'admit_date', 'ZZ_PROBE', ? "
                "FROM mart.fact_encounter_current WHERE source_record_id = ?",
                [severity, record_id],
            )
        assert {r[2] for r in export_rows(con)} == KEPT - {"KEEP"}
    finally:
        con.close()


# --- readmit_30d_flag ---

INDEX = {"admit_date": "03/05/2024", "discharge_date": "03/07/2024"}  # discharged 2024-03-07


def patient(name: str, **changes) -> dict[str, str]:
    return {"patient_first_name": name, **changes}


def follow_up(name: str, admit: str, discharge: str | None = None, **changes) -> dict[str, str]:
    """An INPATIENT encounter of patient `name` admitted on `admit` (discharged the same day by default)."""
    return patient(name, **{"admit_date": admit, "discharge_date": discharge or admit, **changes})


# case -> (index changes, follow-up encounters, expected flag on the index row)
READMIT_CASES = {
    "DAY0": ({}, [follow_up("Dayzero", "03/07/2024", "03/09/2024")], "0"),
    "DAY1": ({}, [follow_up("Dayone", "03/08/2024", "03/10/2024")], "1"),
    "DAY30": ({}, [follow_up("Daythirty", "04/06/2024", "04/08/2024")], "1"),
    "DAY31": ({}, [follow_up("Daythirtyone", "04/07/2024", "04/09/2024")], "0"),
    "OTHER_FACILITY": ({}, [follow_up("Otherfacility", "03/17/2024", facility_name="St. Brendan Medical Center")], "1"),
    "OTHER_PATIENT": ({}, [follow_up("Someoneelse", "03/17/2024")], "0"),
    "VOID": ({}, [follow_up("Voidfollow", "03/17/2024", claim_status="Void")], "0"),
    "NULL_CLAIM": ({}, [follow_up("Nullclaim", "03/17/2024", claim_status="")], "1"),
    "UNRESOLVED": ({}, [follow_up("Unresolved", "03/17/2024", facility_name="Nowhere Clinic")], "0"),
    "BAD_ADMIT": ({}, [follow_up("Badadmit", "02/30/2024", "03/20/2024")], "0"),
    "NO_DISCHARGE": ({}, [follow_up("Nodischarge", "03/17/2024", discharge_date="")], "0"),
    "BAD_DISCHARGE": ({}, [follow_up("Baddischarge", "03/17/2024", discharge_date="13/45/2024")], "0"),
    "NOT_INPATIENT": ({}, [follow_up("Edfollow", "03/17/2024", encounter_type="ED")], "0"),
    # The follow-up is not in the export (2025, not chronic, under 5,000.00), but it still counts.
    "NOT_IN_EXPORT": (
        {"admit_date": "12/20/2024", "discharge_date": "12/22/2024"},
        [follow_up("Notinexport", "01/01/2025", "01/03/2025", primary_dx_code="F32.9", billed_amount="$100.00")],
        "1",
    ),
    "NULL_DISCHARGE": ({"discharge_date": ""}, [follow_up("Nulldischarge", "03/08/2024")], "0"),
    "OBSERVATION": ({"encounter_type": "OBS"}, [follow_up("Observation", "03/08/2024")], ""),
    "EMERGENCY": ({"encounter_type": "ED"}, [follow_up("Emergency", "03/08/2024")], ""),
    # A blank MRN has no patient_key: NULL keys never match each other.
    "NO_PATIENT_KEY": ({"patient_mrn": ""}, [follow_up("Nopatientkey", "03/08/2024", patient_mrn="")], "0"),
    # Two follow-ups, one inside the window: one is enough.
    "TWO_FOLLOW_UPS": ({}, [follow_up("Twofollowups", "05/01/2024"), follow_up("Twofollowups", "03/20/2024")], "1"),
}
# Each case's own patient (the follow-up's first name), except OTHER_PATIENT's index.
INDEX_PATIENT = {case: ups[0]["patient_first_name"] for case, (_, ups, _) in READMIT_CASES.items()}
INDEX_PATIENT["OTHER_PATIENT"] = "Otherpatient"


@pytest.fixture
def readmissions(landing_dir, pipeline_env, roster_npi):
    rows = []
    for case, (index_changes, ups, _) in READMIT_CASES.items():
        rows.append((case, patient(INDEX_PATIENT[case], **{**INDEX, **index_changes})))
        rows.extend((f"{case}-F{i}", changes) for i, changes in enumerate(ups, 1))
    return load(landing_dir, pipeline_env, ("batch_001", [encounters(rows, npi=roster_npi)]))


@pytest.mark.parametrize("case", READMIT_CASES)
def test_readmit_30d_flag(readmissions, case):
    assert readmissions[case]["readmit_30d_flag"] == READMIT_CASES[case][2]


def test_readmission_follow_ups_need_not_be_in_the_export(readmissions):
    assert "NOT_IN_EXPORT-F1" not in readmissions
    assert readmissions["NOT_IN_EXPORT"]["readmit_30d_flag"] == "1"
    assert readmissions["NULL_DISCHARGE"]["discharge_date"] == ""  # the index row itself is exported


def test_a_later_batch_can_set_the_flag(landing_dir, pipeline_env, roster_npi):
    first = load(landing_dir, pipeline_env, ("batch_001", [encounters([("A", {})], npi=roster_npi)]))
    assert first["A"]["readmit_30d_flag"] == "0"

    rows = load(landing_dir, pipeline_env, ("batch_002", [encounters([("A-F", {"admit_date": "03/20/2024",
                                                                              "discharge_date": "03/21/2024"})],
                                                                     npi=roster_npi, ts=T2)]))  # fmt: skip

    assert rows["A"]["readmit_30d_flag"] == "1"


# --- sort order and lineage ---


def test_rows_are_sorted_by_admit_date_source_system_and_source_record_id(landing_dir, pipeline_env, roster_npi):
    epic = encounters([
        ("R2", {}), ("R10", {}), ("R1", {"admit_date": "03/04/2024"}), ("R0", {"admit_date": "03/06/2024"}),
    ], npi=roster_npi)  # fmt: skip
    athena = encounters([("R9", {}), ("R1", {"admit_date": "03/06/2024"})], npi=roster_npi, system="ATHENA_CLINICS")
    write_batch(landing_dir, "batch_001", [epic, athena])
    assert main(pipeline_env) == 0

    _, rows = read_export(pipeline_env)

    assert [(r["admit_date"], r["source_system"], r["source_record_id"]) for r in rows] == [
        ("2024-03-04", "EPIC_NORTH", "R1"),
        ("2024-03-05", "ATHENA_CLINICS", "R9"),
        ("2024-03-05", "EPIC_NORTH", "R10"),  # text order: R10 before R2
        ("2024-03-05", "EPIC_NORTH", "R2"),
        ("2024-03-06", "ATHENA_CLINICS", "R1"),
        ("2024-03-06", "EPIC_NORTH", "R0"),
    ]


def test_lineage_points_to_the_row_that_supplied_the_current_version(landing_dir, pipeline_env, roster_npi):
    rows = load(
        landing_dir, pipeline_env,
        ("batch_001", [encounters([("A", {}), ("B", {})], npi=roster_npi)]),
        ("batch_002", [encounters([("X", {"billed_amount": "$1.00"}), ("B", {"billed_amount": "$7,500.00"})],
                                  npi=roster_npi, ts=T2)]),
    )  # fmt: skip

    assert (rows["A"]["source_batch_id"], rows["A"]["source_row_number"], rows["A"]["version_count"]) == (
        "batch_001", "1", "1")
    assert (rows["B"]["source_batch_id"], rows["B"]["source_file_name"], rows["B"]["source_row_number"],
            rows["B"]["version_count"], rows["B"]["billed_amount_usd"]) == (
        "batch_002", "encounters_epic_north.csv", "2", "2", "7500.00")  # fmt: skip


# --- provider at the encounter ---


def changed_provider():
    """An NPI whose specialty or employment status differs between the first two roster snapshots."""
    first, second = sorted(load_warehouse_reference(REAL_DATA_DIR / "reference").roster, key=lambda s: s.as_of_date)[:2]
    later = {e.npi: e for e in second.entries}
    for entry in sorted(first.entries, key=lambda e: e.npi):
        newer = later.get(entry.npi)
        if newer and (entry.specialty, entry.employment_status) != (newer.specialty, newer.employment_status):
            return entry, newer, second.as_of_date
    pytest.skip("no provider changes specialty or employment status between the first two snapshots")


def test_provider_attributes_are_point_in_time(landing_dir, pipeline_env):
    before, after, changed_on = changed_provider()
    assert changed_on.isoformat() == "2024-07-01"
    rows = load(landing_dir, pipeline_env, ("batch_001", [encounters([
        ("BEFORE", {"admit_date": "06/30/2024", "discharge_date": "07/02/2024"}),
        ("AFTER", {"admit_date": "07/01/2024", "discharge_date": "07/02/2024"}),
        ("NO_PROVIDER", {"attending_npi": "123"}),  # invalid NPI: no provider row
    ], npi=before.npi)]))  # fmt: skip

    def provider(record_id):
        row = rows[record_id]
        return row["attending_specialty_at_encounter"], row["attending_employment_status_at_encounter"]

    assert provider("BEFORE") == (before.specialty, before.employment_status)
    assert provider("AFTER") == (after.specialty, after.employment_status)
    assert provider("NO_PROVIDER") == ("", "")
    assert rows["NO_PROVIDER"]["attending_npi"] == ""


# --- PHI ---

SENTINELS = {
    "patient_mrn": "ZZMRNSENTINEL07",
    "patient_first_name": "Zzfirstsentinel",
    "patient_last_name": "Zzlastsentinel",
    "patient_phone": "555-019-4321",
    "chief_complaint": "Zzcomplaint sentinel call 555-019-4321",
    "attending_provider_name": "Zzprovidersentinel",
    "patient_dob": "02/29/1904",
    "patient_zip": "99954",
}


def test_no_phi_reaches_the_export(landing_dir, pipeline_env, roster_npi):
    rows = load(landing_dir, pipeline_env, ("batch_001", [encounters([("S1", SENTINELS)], npi=roster_npi)]))
    text = (Path(pipeline_env["OUTPUT_DIR"]) / FILE_NAME).read_text(encoding="utf-8").lower()

    assert set(rows) == {"S1"}  # the sentinel row is exported, so the scan below is meaningful
    for value in [*SENTINELS.values(), "ZZMRN", "Zzfirst", "Zzlast", "Zzcomplaint", "Zzprovider", "5550194321",
                  "1904-02-29"]:  # fmt: skip
        assert value.lower() not in text, "a sentinel value"
    assert rows["S1"]["patient_zip3"] == "999"  # ZIP3 is allowed; the full ZIP is not


def test_the_export_has_no_phi_column_and_reads_no_raw_table():
    check_no_phi_columns(FILE_NAME, COLUMNS)
    with pytest.raises(PipelineError, match="PHI columns"):
        check_no_phi_columns(FILE_NAME, [*COLUMNS, "patient_last_name"])
    assert "raw." not in QUERY


# --- byte-identical across incremental, one-shot, rerun and rebuild runs ---

PARITY_BATCHES = {
    "batch_001": [
        ("A", {}),
        ("B", {}),
        ("C", {"encounter_type": "ED"}),
        ("D", {"claim_status": "", "patient_first_name": "Other"}),
    ],
    "batch_002": [
        ("A-F", {"admit_date": "03/20/2024", "discharge_date": "03/21/2024", "billed_amount": "$10.00"}),
        ("B", {"billed_amount": "$4,000.00"}),  # drops out of the export
        ("C", {"encounter_type": "ED", "billed_amount": "$9,999.99"}),  # stays, now from batch_002
        ("E", {"facility_name": "Nowhere Clinic"}),
    ],
}


def env_for(base: Path, pipeline_env, landing: Path) -> dict[str, str]:
    return {**pipeline_env, "DATA_DIR": str(landing.parent), "OUTPUT_DIR": str(base / "output"),
            "RAW_DB_PATH": str(base / "work" / "raw.duckdb")}  # fmt: skip


def test_incremental_one_shot_rerun_and_rebuild_exports_are_byte_identical(tmp_path, pipeline_env, roster_npi):
    specs = {b: [encounters(rows, npi=roster_npi, ts=T1 if b == "batch_001" else T2)] for b, rows in PARITY_BATCHES.items()}
    outputs = {}

    landing = tmp_path / "incremental" / "data" / "landing"
    incremental = env_for(tmp_path / "incremental", pipeline_env, landing)
    for batch_id, batch in specs.items():
        write_batch(landing, batch_id, batch)
        assert main(incremental) == 0
        outputs[batch_id] = (Path(incremental["OUTPUT_DIR"]) / FILE_NAME).read_bytes()
    assert main(incremental) == 0
    outputs["rerun"] = (Path(incremental["OUTPUT_DIR"]) / FILE_NAME).read_bytes()

    landing = tmp_path / "one_shot" / "data" / "landing"
    one_shot = env_for(tmp_path / "one_shot", pipeline_env, landing)
    for batch_id, batch in specs.items():
        write_batch(landing, batch_id, batch)
    assert main(one_shot) == 0
    outputs["one_shot"] = (Path(one_shot["OUTPUT_DIR"]) / FILE_NAME).read_bytes()

    for name, source in (("rebuild", one_shot), ("rebuild_live", incremental)):
        rebuild = env_for(tmp_path / name, pipeline_env, Path(source["DATA_DIR"]) / "landing")
        Path(rebuild["RAW_DB_PATH"]).parent.mkdir(parents=True)
        shutil.copyfile(source["RAW_DB_PATH"], rebuild["RAW_DB_PATH"])
        if name == "rebuild":
            make_pre_task4(Path(rebuild["RAW_DB_PATH"]))
        assert main(rebuild, ["--rebuild-derived"]) == 0
        outputs[name] = (Path(rebuild["OUTPUT_DIR"]) / FILE_NAME).read_bytes()

    final = outputs["batch_002"]
    assert {name: content == final for name, content in outputs.items()} == {
        "batch_001": False, "batch_002": True, "rerun": True, "one_shot": True, "rebuild": True, "rebuild_live": True}
    rows = exported(incremental)
    assert {k: (r["readmit_30d_flag"], r["source_batch_id"], r["version_count"]) for k, r in rows.items()} == {
        "A": ("1", "batch_001", "1"), "C": ("", "batch_002", "2"), "D": ("0", "batch_001", "1")}


def test_a_failed_write_keeps_the_previous_export_and_leaves_no_temp_file(conditions, monkeypatch):
    env, _ = conditions
    path = Path(env["OUTPUT_DIR"]) / FILE_NAME
    before = path.read_bytes()
    replace = exports.os.replace

    def fail_for_this_export(src, dst):
        if Path(dst).name == FILE_NAME:
            raise OSError("disk full")
        replace(src, dst)

    monkeypatch.setattr(exports.os, "replace", fail_for_this_export)

    assert main(env) == 1
    assert path.read_bytes() == before
    assert not [p for p in path.parent.iterdir() if p.suffix == ".tmp"]
