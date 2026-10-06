"""Task 5 dimensions: dim_provider SCD2 and its point-in-time lookup, and the other four dimensions.

SCD2 tests build roster snapshots in memory (synthetic NPIs with valid check
digits). Pipeline tests run synthetic batches through main() into tmp_path,
with the real reference files (read only).
"""

import hashlib
import json
from datetime import date, timedelta
from pathlib import Path

import pytest
from conftest import FileSpec, attach, contract_header, csv_bytes, state_digest, write_batch

from pipeline import dimensions
from pipeline.dimensions import (
    VALID_FROM_EARLIEST,
    VALID_TO_OPEN,
    ProviderLookup,
    build_provider_rows,
    diagnosis_rows,
    payer_rows,
    provider_sk,
)
from pipeline.errors import ConfigError
from pipeline.main import main
from pipeline.parsers.npi import npi_check_digit
from pipeline.reference_data import Icd10Entry, RosterEntry, RosterSnapshot

pytestmark = pytest.mark.usefixtures("restore_pipeline_logger")

JAN24, JUL24, JAN25 = date(2024, 1, 1), date(2024, 7, 1), date(2025, 1, 1)
FACILITIES = [f"FAC00{i}" for i in range(1, 9)]
NPI_A = "100000001" + str(npi_check_digit("100000001"))
NPI_B = "100000002" + str(npi_check_digit("100000002"))
NPI_C = "100000003" + str(npi_check_digit("100000003"))


def entry(npi, as_of, row=1, **changes):
    values = dict(provider_last_name="Zzlast", provider_first_name="Zzfirst", credential="MD", specialty="Cardiology",
                  primary_facility_id="FAC001", employment_status="Affiliated")
    values.update(changes)
    return RosterEntry(as_of_date=as_of, npi=npi, **values, file_name=f"roster_{as_of}.csv", source_row_number=row)


def snapshot(as_of, *entries):
    return RosterSnapshot(as_of, f"roster_{as_of}.csv", tuple(entries))


def rows_of(*snapshots):
    return build_provider_rows(snapshots, FACILITIES)


def spans(rows, npi=NPI_A):
    return [(r.valid_from, r.valid_to, r.is_current) for r in rows if r.npi == npi]


# --- SCD2 rows ---


def test_unchanged_provider_is_one_open_row_back_to_the_earliest_date():
    rows = rows_of(snapshot(JAN24, entry(NPI_A, JAN24, 7)), snapshot(JUL24, entry(NPI_A, JUL24)), snapshot(JAN25, entry(NPI_A, JAN25)))

    assert spans(rows) == [(VALID_FROM_EARLIEST, VALID_TO_OPEN, True)]
    (row,) = rows
    assert (row.snapshot_as_of_date, row.source_file_name, row.source_row_number) == (JAN24, "roster_2024-01-01.csv", 7)


@pytest.mark.parametrize(
    "attribute, value",
    [
        ("provider_last_name", "Zzother"),
        ("provider_first_name", "Zzother"),
        ("credential", "DO"),
        ("specialty", "Nephrology"),
        ("primary_facility_id", "FAC002"),
        ("employment_status", "Employed"),
    ],
)
def test_a_change_in_any_tracked_attribute_starts_a_new_row(attribute, value):
    rows = rows_of(snapshot(JAN24, entry(NPI_A, JAN24)), snapshot(JUL24, entry(NPI_A, JUL24, **{attribute: value})))

    assert spans(rows) == [(VALID_FROM_EARLIEST, JUL24, False), (JUL24, VALID_TO_OPEN, True)]
    assert getattr(rows[1], attribute) == value


def test_a_change_and_its_reversal_are_three_rows():
    rows = rows_of(
        snapshot(JAN24, entry(NPI_A, JAN24)),
        snapshot(JUL24, entry(NPI_A, JUL24, employment_status="Employed")),
        snapshot(JAN25, entry(NPI_A, JAN25)),
    )

    assert spans(rows) == [(VALID_FROM_EARLIEST, JUL24, False), (JUL24, JAN25, False), (JAN25, VALID_TO_OPEN, True)]


def test_a_dropped_provider_row_ends_at_the_snapshot_that_drops_it():
    rows = rows_of(snapshot(JAN24, entry(NPI_A, JAN24), entry(NPI_B, JAN24)), snapshot(JUL24, entry(NPI_B, JUL24)))

    assert spans(rows) == [(VALID_FROM_EARLIEST, JUL24, False)]


def test_a_provider_added_later_does_not_apply_backwards():
    rows = rows_of(snapshot(JAN24, entry(NPI_B, JAN24)), snapshot(JUL24, entry(NPI_A, JUL24), entry(NPI_B, JUL24)))

    assert spans(rows) == [(JUL24, VALID_TO_OPEN, True)]


def test_a_provider_back_after_a_gap_gets_a_new_row():
    rows = rows_of(snapshot(JAN24, entry(NPI_A, JAN24)), snapshot(JUL24), snapshot(JAN25, entry(NPI_A, JAN25)))

    assert spans(rows) == [(VALID_FROM_EARLIEST, JUL24, False), (JAN25, VALID_TO_OPEN, True)]


def test_rows_do_not_depend_on_snapshot_order():
    snaps = [snapshot(JAN24, entry(NPI_A, JAN24)), snapshot(JUL24, entry(NPI_A, JUL24, credential="DO")), snapshot(JAN25)]

    assert rows_of(*snaps) == rows_of(*reversed(snaps))


def test_provider_sk_is_sha256_of_npi_and_valid_from():
    payload = json.dumps(["provider/v1", NPI_A, "2024-07-01"], separators=(",", ":"))

    assert provider_sk(NPI_A, JUL24) == hashlib.sha256(payload.encode()).hexdigest()


def test_a_primary_facility_outside_the_master_is_refused():
    with pytest.raises(ConfigError, match="primary_facility_id"):
        build_provider_rows([snapshot(JAN24, entry(NPI_A, JAN24, primary_facility_id="FAC999"))], FACILITIES)


def test_two_snapshots_with_one_date_are_refused():
    with pytest.raises(ConfigError, match="same as_of_date"):
        build_provider_rows([snapshot(JAN24, entry(NPI_A, JAN24)), snapshot(JAN24, entry(NPI_B, JAN24))], FACILITIES)


# --- point-in-time lookup ---


CHANGING = rows_of(
    snapshot(JAN24, entry(NPI_A, JAN24, employment_status="Affiliated"), entry(NPI_B, JAN24)),
    snapshot(JUL24, entry(NPI_A, JUL24, employment_status="Employed"), entry(NPI_C, JUL24)),
    snapshot(JAN25, entry(NPI_A, JAN25, specialty="Nephrology"), entry(NPI_B, JAN25), entry(NPI_C, JAN25)),
)
LOOKUP = ProviderLookup(CHANGING)


def row_starting(npi, valid_from):
    return next(r.provider_sk for r in CHANGING if r.npi == npi and r.valid_from == valid_from)


@pytest.mark.parametrize(
    "admit, starts",
    [
        (date(1900, 1, 1), VALID_FROM_EARLIEST),
        (date(2023, 12, 31), VALID_FROM_EARLIEST),  # the earliest snapshot applies backwards
        (JAN24, VALID_FROM_EARLIEST),
        (date(2024, 6, 30), VALID_FROM_EARLIEST),
        (JUL24, JUL24),  # a snapshot holds from its as_of_date ...
        (date(2024, 12, 31), JUL24),  # ... until the next one
        (JAN25, JAN25),
        (date(2030, 1, 1), JAN25),  # the latest snapshot stays open
    ],
)
def test_point_in_time_boundaries(admit, starts):
    assert LOOKUP.at(NPI_A, admit) == (row_starting(NPI_A, starts), None)


def test_dropped_and_re_added_provider_has_no_row_in_the_gap():
    assert LOOKUP.at(NPI_B, date(2024, 6, 30)) == (row_starting(NPI_B, VALID_FROM_EARLIEST), None)
    assert LOOKUP.at(NPI_B, JUL24) == (None, "PROVIDER_NOT_ON_ROSTER_AT_ADMIT")
    assert LOOKUP.at(NPI_B, JAN25) == (row_starting(NPI_B, JAN25), None)


def test_provider_added_later_has_no_row_before_its_first_snapshot():
    assert LOOKUP.at(NPI_C, date(2024, 6, 30)) == (None, "PROVIDER_NOT_ON_ROSTER_AT_ADMIT")
    assert LOOKUP.at(NPI_C, date(2023, 3, 1)) == (None, "PROVIDER_NOT_ON_ROSTER_AT_ADMIT")
    assert LOOKUP.at(NPI_C, JUL24) == (row_starting(NPI_C, JUL24), None)


def test_reasons_when_there_is_no_provider_and_no_fallback():
    unknown = "100000009" + str(npi_check_digit("100000009"))

    assert LOOKUP.at(None, JUL24, "NPI_CHECKSUM_FAILED") == (None, "NPI_CHECKSUM_FAILED")  # the NPI's own reason
    assert LOOKUP.at(unknown, JUL24) == (None, "NPI_NOT_IN_ROSTER")
    assert LOOKUP.at(unknown, None) == (None, "NPI_NOT_IN_ROSTER")  # in no roster at all wins
    assert LOOKUP.at(NPI_A, None) == (None, "PROVIDER_ADMIT_DATE_UNKNOWN")


def test_at_most_one_row_covers_any_date_and_the_lookup_finds_it():
    day = date(2023, 12, 25)
    while day <= date(2025, 1, 8):
        for npi in (NPI_A, NPI_B, NPI_C):
            covering = [r.provider_sk for r in CHANGING if r.npi == npi and r.valid_from <= day < r.valid_to]
            assert len(covering) <= 1, (npi, day)
            assert LOOKUP.at(npi, day)[0] == (covering[0] if covering else None), (npi, day)
        day += timedelta(days=1)


# --- other dimensions ---


def test_diagnosis_rows_add_observed_codes_with_unknown_attributes():
    reference = [Icd10Entry("E11.9", "Type 2 diabetes", "DIABETES", True, 1)]

    rows = diagnosis_rows(reference, ["Z99.89", "E11.9"])

    assert [(r["icd10_code"], r["in_reference"], r["is_chronic"], r["source_row_number"]) for r in rows] == [
        ("E11.9", True, True, 1),
        ("Z99.89", False, None, None),  # chronic status unknown, not "N"
    ]


def test_payer_rows_are_the_six_categories():
    assert [r["payer_category"] for r in payer_rows()] == ["MEDICARE", "MEDICAID", "COMMERCIAL", "SELF_PAY", "OTHER", "UNKNOWN"]


# --- rebuilt in the pipeline ---


def epic_file(rows):
    header = contract_header("EPIC_NORTH")
    records = []
    for record_id, admit, discharge, dx in rows:
        values = {column: f"{column}-zz" for column in header}
        values.update(source_system="EPIC_NORTH", source_record_id=record_id, last_updated_ts="2024-03-01T10:00:00Z",
                      admit_date=admit, discharge_date=discharge, primary_dx_code=dx)
        records.append([values[column] for column in header])
    return FileSpec("encounters_epic_north.csv", "EPIC_NORTH", csv_bytes(header, records), len(records))


BATCH = [epic_file([("E1", "03/05/2024", "03/07/2024", "Z99.89"), ("E2", "11/30/2024", "", "E11.9")])]


def mart_counts(env):
    con = attach(Path(env["RAW_DB_PATH"]))
    try:
        return {t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in dimensions._DDL}
    finally:
        con.close()


def test_dimensions_are_built_by_every_run(landing_dir, pipeline_env):
    write_batch(landing_dir, "batch_001", BATCH)

    assert main(pipeline_env) == 0

    assert mart_counts(pipeline_env) == {
        "mart.dim_provider": 65,  # the real roster
        "mart.dim_facility": 8,
        "mart.dim_diagnosis": 44,  # 43 reference codes + Z99.89 seen in the batch
        "mart.dim_payer": 6,
        "mart.dim_date": 366,  # the synthetic dates span 2024 only
    }


def test_dimensions_are_rebuilt_identically(landing_dir, pipeline_env):
    write_batch(landing_dir, "batch_001", BATCH)
    main(pipeline_env)
    before = state_digest(Path(pipeline_env["RAW_DB_PATH"]), exclude_run_times=False)

    assert main(pipeline_env) == 0

    assert state_digest(Path(pipeline_env["RAW_DB_PATH"]), exclude_run_times=False) == before


def test_a_failed_rebuild_keeps_the_previous_dimensions(landing_dir, pipeline_env, monkeypatch):
    write_batch(landing_dir, "batch_001", BATCH)
    main(pipeline_env)
    before = mart_counts(pipeline_env)
    original = dimensions._insert

    def fail_on_dates(con, table, rows):
        if table == dimensions.DATE_TABLE:
            raise RuntimeError("simulated failure while building dim_date")
        original(con, table, rows)

    monkeypatch.setattr(dimensions, "_insert", fail_on_dates)

    assert main(pipeline_env) == 1
    assert mart_counts(pipeline_env) == before


def test_sql_point_in_time_join_matches_the_lookup(landing_dir, pipeline_env):
    write_batch(landing_dir, "batch_001", BATCH)
    main(pipeline_env)
    con = attach(Path(pipeline_env["RAW_DB_PATH"]))
    try:
        cursor = con.execute(f"SELECT * FROM {dimensions.PROVIDER_TABLE}")
        names = [d[0] for d in cursor.description]
        rows = [dimensions.ProviderRow(**dict(zip(names, r, strict=True))) for r in cursor.fetchall()]
        lookup = ProviderLookup(rows)
        for npi in sorted({r.npi for r in rows}):
            for admit in (date(2023, 6, 1), date(2024, 6, 30), date(2024, 7, 1), date(2024, 12, 31), date(2025, 1, 1)):
                found = con.execute(
                    f"SELECT provider_sk FROM {dimensions.PROVIDER_TABLE} WHERE npi = ? AND ? >= valid_from AND ? < valid_to",
                    [npi, admit, admit],
                ).fetchall()
                assert [r[0] for r in found] == ([lookup.at(npi, admit)[0]] if lookup.at(npi, admit)[0] else [])
    finally:
        con.close()
