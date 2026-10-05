"""Task 3 on the real data pack (read only), built into a temp database. Asserts counts only."""

import logging
from collections import Counter, defaultdict
from pathlib import Path

import pytest
from conftest import CONTRACT_PATH, REAL_DATA_DIR, TEST_PATIENT_KEY_SECRET

from pipeline.clean_patients import CLEAN_COLUMNS, PHI_COLUMNS
from pipeline.main import main
from pipeline.raw_store import open_store
from pipeline.schema_contract import load_contracts

CONTRACTS = load_contracts(CONTRACT_PATH)


@pytest.fixture(scope="module")
def con(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("task3_real")
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
    connection = open_store(Path(env["RAW_DB_PATH"]), CONTRACTS.canonical_columns)
    yield connection
    connection.close()


def identities(con):
    """(source_system, MRN) -> (patient_key, status, reason). Raw MRNs stay inside this test."""
    rows = con.execute(
        "SELECT DISTINCT f.source_system, e.patient_mrn, c.patient_key, c.patient_link_status, c.patient_link_reason "
        "FROM raw.encounters e JOIN raw.ingested_files f USING (batch_id, file_name) "
        "JOIN clean.encounter_patients c USING (batch_id, file_name, source_row_number)"
    ).fetchall()
    return {(s, m): (k, status, reason) for s, m, k, status, reason in rows}, len(rows)


def test_one_clean_row_per_accepted_row_and_no_phi_columns(con):
    columns = [row[0] for row in con.execute("DESCRIBE clean.encounter_patients").fetchall()]

    assert con.execute("SELECT count(*) FROM clean.encounter_patients").fetchone() == (3636,)
    assert columns == list(CLEAN_COLUMNS)
    assert not PHI_COLUMNS & set(columns)


def test_linkage_counts(con):
    resolved, distinct_rows = identities(con)
    linked = {k: v for k, v in resolved.items() if v[1] == "LINKED"}
    systems_per_key = defaultdict(set)
    mrns_per_key_and_system = Counter()
    for (system, _), (key, _, _) in linked.items():
        systems_per_key[key].add(system)
        mrns_per_key_and_system[(key, system)] += 1

    assert len(resolved) == distinct_rows == 1056  # one key per identity
    assert len(linked) == 1044
    assert len(systems_per_key) == 895  # person groups
    assert sum(len(s) > 1 for s in systems_per_key.values()) == 142
    assert Counter(len(s) for s in systems_per_key.values()) == {1: 753, 2: 135, 3: 7}
    assert max(mrns_per_key_and_system.values()) == 1  # no same-system collisions


def test_unlinked_identities_are_the_twelve_without_dob(con):
    resolved, _ = identities(con)
    unlinked = [v for v in resolved.values() if v[1] == "UNLINKED"]

    assert len(unlinked) == 12
    assert {reason for _, _, reason in unlinked} == {"PATIENT_UNLINKED_NO_DOB"}
    assert con.execute(
        "SELECT count(*) FROM clean.encounter_patients WHERE patient_link_status = 'UNLINKED'"
    ).fetchone() == (34,)
    assert con.execute("SELECT count(DISTINCT patient_key), count(*) FILTER (WHERE patient_key IS NULL) "
                       "FROM clean.encounter_patients").fetchone() == (907, 0)


def test_age_band_counts(con):
    bands = dict(con.execute("SELECT age_band, count(*) FROM clean.encounter_patients GROUP BY ALL").fetchall())
    reasons = dict(con.execute(
        "SELECT age_band_reason, count(*) FROM clean.encounter_patients WHERE age_band = 'UNKNOWN' GROUP BY ALL"
    ).fetchall())

    assert bands == {"0-17": 264, "18-39": 541, "40-64": 1771, "65+": 1006, "UNKNOWN": 54}
    assert reasons == {"AGE_BAND_DOB_UNAVAILABLE": 34, "AGE_BAND_ADMIT_UNAVAILABLE": 18, "AGE_BAND_DOB_AFTER_ADMIT": 2}


def test_every_row_has_a_three_digit_zip3(con):
    assert con.execute(
        "SELECT count(*) FILTER (WHERE length(zip3) = 3 AND zip3 SIMILAR TO '[0-9]{3}'), count(*) FILTER (WHERE zip3 IS NULL) "
        "FROM clean.encounter_patients"
    ).fetchone() == (3636, 0)


def test_sex_is_normalised_to_f_or_m(con):
    assert set(con.execute("SELECT DISTINCT sex FROM clean.encounter_patients").fetchall()) == {("F",), ("M",)}
