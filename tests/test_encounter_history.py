"""Task 4 classifier, keys, history tables and the current-state view.

All rows are synthetic. Classifier tests build rows in memory; storage tests
land them in a DuckDB file under tmp_path through raw_store, as the pipeline will.
"""

import hashlib
import json
import random
from collections import Counter
from datetime import UTC, datetime

import duckdb
import pytest
from conftest import CONTRACT_PATH, REAL_DATA_DIR

from pipeline import encounter_history
from pipeline.clean_patients import PHI_COLUMNS
from pipeline.encounter_history import (
    BatchRow,
    HeldVersion,
    MatchType,
    Outcome,
    OutcomeReason,
    classify_batch,
    compare_values,
    encounter_key,
    observe_row,
    version_key,
)
from pipeline.parsers.result import FieldReason
from pipeline.raw_store import AcceptedFile, open_store, write_accepted_batch
from pipeline.schema_contract import load_contracts
from pipeline.source_conventions import load_source_conventions

CONTRACTS = load_contracts(CONTRACT_PATH)
CANONICAL = CONTRACTS.canonical_columns
CONVENTIONS = load_source_conventions(REAL_DATA_DIR / "reference" / "source_systems_and_facilities.json")
FILES = {system: contract.file_name for system, contract in CONTRACTS.source_systems.items()}

EPIC, MEDITECH, ATHENA = "EPIC_NORTH", "LEGACY_MEDITECH", "ATHENA_CLINICS"
T1, T2, T3 = "2024-03-01T10:00:00Z", "2024-03-02T10:00:00Z", "2024-03-03T10:00:00Z"


def values(record_id="E0000001", ts=T1, system=EPIC, **changes):
    """Canonical values of a v1-layout row: encounter_source is absent (NULL)."""
    row = {column: f"{column}-zz" for column in CANONICAL}
    row.update(source_system=system, source_record_id=record_id, last_updated_ts=ts, encounter_source=None)
    row.update(changes)
    return row


def row(number, batch="batch_001", system=EPIC, **kwargs):
    return observe_row(batch, FILES[system], number, system, values(system=system, **kwargs), CONVENTIONS)


def held_after(held, rows, classified):
    """History after a classified batch, as load_held_versions would return it."""
    by_lineage = {(r.file_name, r.source_row_number): r for r in rows}
    new = {key: list(versions) for key, versions in held.items()}
    for v in classified.versions:
        first_seen = by_lineage[(v.first_seen_file_name, v.first_seen_source_row_number)]
        new.setdefault(v.encounter_key, []).append(HeldVersion(v.version_key, v.last_updated_ts_utc, first_seen.values))
    return new


def run(*batches):
    """Classify batches in order, each against the history left by the earlier ones."""
    held, results = {}, []
    for rows in batches:
        classified = classify_batch(rows, held)
        held = held_after(held, rows, classified)
        results.append(classified)
    return results


def outcomes(classified):
    return [(o.source_row_number, o.outcome, o.outcome_reason) for o in classified.outcomes]


def key(record_id="E0000001", system=EPIC):
    return encounter_key(system, record_id)


# --- keys ---


def test_keys_are_sha256_of_the_documented_payloads():
    ts = datetime(2024, 3, 1, 10, 0, tzinfo=UTC)
    enc_payload = json.dumps(["encounter/v1", EPIC, "E0000001"], separators=(",", ":"))
    ver_payload = json.dumps(["encounter_version/v1", EPIC, "E0000001", "2024-03-01T10:00:00.000000Z"], separators=(",", ":"))

    assert encounter_key(EPIC, "E0000001") == hashlib.sha256(enc_payload.encode()).hexdigest()
    assert version_key(EPIC, "E0000001", ts) == hashlib.sha256(ver_payload.encode()).hexdigest()


def test_source_system_is_part_of_the_encounter_key():
    assert encounter_key(MEDITECH, "100001") != encounter_key(ATHENA, "100001")


def test_version_key_depends_on_the_instant_not_the_text():
    a = row(1, system=ATHENA, record_id="100001", ts="2024-03-05 14:30:00")  # Athena: UTC without offset
    b = row(2, system=ATHENA, record_id="100001", ts="2024-03-05T09:30:00-05:00")

    assert a.last_updated_ts_utc == b.last_updated_ts_utc
    assert version_key(ATHENA, "100001", a.last_updated_ts_utc) == version_key(ATHENA, "100001", b.last_updated_ts_utc)


def test_version_key_refuses_a_naive_timestamp():
    with pytest.raises(ValueError):
        version_key(EPIC, "E0000001", datetime(2024, 3, 1, 10, 0))


# --- fingerprint guard ---


def test_compare_values_exact_equivalent_and_conflict():
    first = values(system=ATHENA, record_id="100001", ts="2024-03-05 14:30:00")
    reformatted = values(system=ATHENA, record_id="100001", ts="2024-03-05T09:30:00-05:00", encounter_source="Referral")

    assert compare_values(first, dict(first)) == MatchType.EXACT
    assert compare_values(first, reformatted) == MatchType.EQUIVALENT
    assert compare_values(first, {**first, "billed_amount": "999.00"}) == MatchType.CONFLICT


def test_an_empty_string_is_compared_but_an_absent_column_is_not():
    first = values()

    assert compare_values(first, {**first, "payer_name": ""}) == MatchType.CONFLICT
    assert compare_values(first, {**first, "payer_name": None}) == MatchType.EQUIVALENT


def test_batch_row_repr_hides_values():
    text = repr(row(1, patient_last_name="Zztesterson")) + repr(HeldVersion("k", datetime.now(UTC), {"x": "Zztesterson"}))

    assert "Zztesterson" not in text


# --- classification ---


def test_first_occurrence_is_a_new_encounter():
    (batch,) = run([row(1)])

    assert outcomes(batch) == [(1, Outcome.NEW_ENCOUNTER, None)]
    (version,) = batch.versions
    assert (version.encounter_key, version.arrival_outcome) == (key(), Outcome.NEW_ENCOUNTER)
    assert (version.first_seen_batch_id, version.first_seen_file_name, version.first_seen_source_row_number) == (
        "batch_001", FILES[EPIC], 1)
    assert batch.outcomes[0].held_current_version_key is None


def test_newer_timestamp_in_a_later_batch_is_a_new_version():
    first, second = run([row(1, ts=T1)], [row(1, batch="batch_002", ts=T2, claim_status="Paid")])

    assert outcomes(second) == [(1, Outcome.NEW_VERSION, None)]
    assert second.outcomes[0].held_current_version_key == first.versions[0].version_key
    assert second.versions[0].arrival_outcome == Outcome.NEW_VERSION


def test_exact_duplicate_in_the_same_batch():
    (batch,) = run([row(1), row(2)])

    assert outcomes(batch) == [(1, Outcome.NEW_ENCOUNTER, None), (2, Outcome.DUPLICATE, OutcomeReason.DUPLICATE_IN_BATCH)]
    assert batch.outcomes[1].match_type == MatchType.EXACT
    assert len(batch.versions) == 1


def test_duplicate_of_the_current_version_in_a_later_batch():
    _, second = run([row(1)], [row(7, batch="batch_002")])

    assert outcomes(second) == [(7, Outcome.DUPLICATE, OutcomeReason.DUPLICATE_OF_HELD_VERSION)]
    assert second.versions == ()


def test_replay_of_a_superseded_version_is_stale_not_duplicate():
    """Stale is checked before duplicate, so an identical replay of an old version is STALE."""
    *_, third = run([row(1, ts=T1)], [row(1, batch="batch_002", ts=T2)], [row(1, batch="batch_003", ts=T1)])

    assert outcomes(third) == [(1, Outcome.STALE, OutcomeReason.STALE_REPLAY)]
    assert third.outcomes[0].match_type == MatchType.EXACT
    assert third.versions == ()


def test_reformatted_replay_from_a_new_schema_version_is_equivalent():
    """Athena v2 rewrites the timestamp with an offset and adds encounter_source; still the same version."""
    athena = dict(system=ATHENA, record_id="100001")
    *_, third = run(
        [row(1, ts="2024-03-05 14:30:00", **athena)],
        [row(1, batch="batch_002", ts="2024-04-01 09:00:00", **athena)],
        [row(1, batch="batch_003", ts="2024-03-05T09:30:00-05:00", encounter_source="Referral", **athena)],
    )

    assert outcomes(third) == [(1, Outcome.STALE, OutcomeReason.STALE_REPLAY)]
    assert third.outcomes[0].match_type == MatchType.EQUIVALENT


def test_stale_row_with_a_never_held_older_version_goes_to_history_only():
    *_, third = run(
        [row(1, ts=T1)],
        [row(1, batch="batch_002", ts=T3)],
        [row(1, batch="batch_003", ts=T2, billed_amount="12.34", claim_status="Void")],
    )

    assert outcomes(third) == [(1, Outcome.STALE, OutcomeReason.STALE_NEW_VERSION)]
    assert third.outcomes[0].match_type is None
    (version,) = third.versions
    assert version.arrival_outcome == Outcome.STALE
    assert version.last_updated_ts_utc == datetime(2024, 3, 2, 10, 0, tzinfo=UTC)


def test_stale_replay_with_different_values_stays_stale_and_is_marked_conflict():
    *_, third = run(
        [row(1, ts=T1)], [row(1, batch="batch_002", ts=T2)], [row(1, batch="batch_003", ts=T1, billed_amount="1.00")]
    )

    assert outcomes(third) == [(1, Outcome.STALE, OutcomeReason.STALE_REPLAY)]
    assert third.outcomes[0].match_type == MatchType.CONFLICT


def test_same_timestamp_conflict_with_held_version_is_quarantined():
    _, second = run([row(1, ts=T1)], [row(1, batch="batch_002", ts=T1, billed_amount="1.00")])

    assert outcomes(second) == [(1, Outcome.QUARANTINED, OutcomeReason.VERSION_CONFLICT_SAME_TS)]
    assert second.outcomes[0].match_type == MatchType.CONFLICT
    assert second.outcomes[0].version_key == version_key(EPIC, "E0000001", datetime(2024, 3, 1, 10, 0, tzinfo=UTC))
    assert second.versions == ()


def test_same_timestamp_conflict_within_a_batch_keeps_the_first_arrival():
    (batch,) = run([row(5, billed_amount="1.00"), row(2, billed_amount="2.00")])

    assert outcomes(batch) == [
        (2, Outcome.NEW_ENCOUNTER, None),
        (5, Outcome.QUARANTINED, OutcomeReason.VERSION_CONFLICT_SAME_TS),
    ]
    assert batch.versions[0].first_seen_source_row_number == 2


@pytest.mark.parametrize("record_id", [None, "", "   "])
def test_missing_source_record_id_is_quarantined(record_id):
    (batch,) = run([row(1, record_id=record_id)])

    (outcome,) = batch.outcomes
    assert (outcome.outcome, outcome.outcome_reason) == (Outcome.QUARANTINED, OutcomeReason.SOURCE_RECORD_ID_MISSING)
    assert outcome.encounter_key is None and outcome.version_key is None
    assert batch.versions == ()


@pytest.mark.parametrize(
    "system, ts, reason",
    [
        (EPIC, "", FieldReason.TIMESTAMP_MISSING),
        (EPIC, "not a time", FieldReason.TIMESTAMP_UNPARSEABLE),
        (EPIC, "2024-02-30T10:00:00Z", FieldReason.TIMESTAMP_INVALID),
        (MEDITECH, "10/03/2024 02:30:00", FieldReason.TIMESTAMP_NONEXISTENT_LOCAL_TIME),  # 10 Mar 2024, spring forward
    ],
)
def test_unparseable_timestamp_is_quarantined_with_the_parser_reason(system, ts, reason):
    (batch,) = run([row(1, system=system, record_id="100001", ts=ts)])

    (outcome,) = batch.outcomes
    assert (outcome.outcome, outcome.outcome_reason) == (Outcome.QUARANTINED, reason)
    assert outcome.encounter_key == key("100001", system) and outcome.version_key is None
    assert batch.versions == ()


def test_two_new_versions_in_one_batch_are_both_kept_whatever_the_file_order():
    _, second = run([row(1, ts=T1)], [row(1, batch="batch_002", ts=T3), row(2, batch="batch_002", ts=T2)])

    assert outcomes(second) == [(1, Outcome.NEW_VERSION, None), (2, Outcome.NEW_VERSION, None)]
    assert len(second.versions) == 2


def test_new_encounter_goes_to_its_earliest_version_not_its_first_row():
    (batch,) = run([row(1, ts=T3), row(2, ts=T1)])

    assert outcomes(batch) == [(1, Outcome.NEW_VERSION, None), (2, Outcome.NEW_ENCOUNTER, None)]


def test_duplicate_of_current_after_the_newer_row_in_the_file_is_not_stale():
    """batch_002 pattern: the new version comes first in the file, the re-sent current version after it."""
    _, second = run([row(1, ts=T1)], [row(1, batch="batch_002", ts=T2), row(2, batch="batch_002", ts=T1)])

    assert outcomes(second) == [
        (1, Outcome.NEW_VERSION, None),
        (2, Outcome.DUPLICATE, OutcomeReason.DUPLICATE_OF_HELD_VERSION),
    ]


def test_meditech_versions_are_ordered_by_instant_not_text():
    """Day-first text: '02/03/2024' (2 Mar) sorts before '10/02/2024' (10 Feb) as text."""
    meditech = dict(system=MEDITECH, record_id="100001")
    _, second = run([row(1, ts="02/03/2024 10:00:00", **meditech)], [row(1, batch="batch_002", ts="10/02/2024 10:00:00", **meditech)])

    assert outcomes(second) == [(1, Outcome.STALE, OutcomeReason.STALE_NEW_VERSION)]


def test_same_record_id_in_two_systems_are_two_encounters():
    (batch,) = run([row(1, system=MEDITECH, record_id="100001", ts="01/03/2024 10:00:00"),
                    row(1, system=ATHENA, record_id="100001", ts="2024-03-01 10:00:00")])

    assert [o.outcome for o in batch.outcomes] == [Outcome.NEW_ENCOUNTER, Outcome.NEW_ENCOUNTER]
    assert len({v.encounter_key for v in batch.versions}) == 2


def test_rows_from_two_batches_are_refused():
    with pytest.raises(ValueError):
        classify_batch([row(1), row(2, batch="batch_002")], {})


def mixed_batch():
    """Every outcome in one batch_002, against a batch_001 history."""
    first = [row(1, ts=T2), row(2, record_id="E0000002", ts=T1), row(3, record_id="E0000003", ts=T1)]
    second = [
        row(1, batch="batch_002", ts=T1),                               # stale new version
        row(2, batch="batch_002", ts=T3),                               # new version, before ...
        row(3, batch="batch_002", ts=T2),                               # ... the re-sent held current: duplicate
        row(4, batch="batch_002", record_id="E0000002", ts=T2),         # new version
        row(5, batch="batch_002", record_id="E0000002", ts=T1),         # duplicate of held version
        row(6, batch="batch_002", record_id="E0000004", ts=T2),         # later version of a new encounter
        row(7, batch="batch_002", record_id="E0000004", ts=T1),         # new encounter (earliest version)
        row(8, batch="batch_002", record_id="E0000004", ts=T1),         # duplicate in batch
        row(9, batch="batch_002", record_id="E0000003", ts=T1, billed_amount="1.00"),  # same-ts conflict
        row(10, batch="batch_002", record_id=""),                       # missing id
        row(11, batch="batch_002", record_id="E0000005", ts="N/A"),     # placeholder timestamp
        row(12, batch="batch_002", record_id="E0000002", ts=T2, encounter_source="Walk-in"),  # equivalent to row 4
    ]
    return first, second


def test_result_does_not_depend_on_row_order():
    first, second = mixed_batch()
    held = held_after({}, first, classify_batch(first, {}))
    expected = classify_batch(second, held)

    for seed in range(20):
        shuffled = second[:]
        random.Random(seed).shuffle(shuffled)
        assert classify_batch(shuffled, held) == expected


def test_every_row_gets_exactly_one_outcome_and_counts_reconcile():
    first, second = mixed_batch()
    held = held_after({}, first, classify_batch(first, {}))
    classified = classify_batch(second, held)

    counts = Counter(o.outcome for o in classified.outcomes)
    assert sorted(o.source_row_number for o in classified.outcomes) == list(range(1, 13))
    assert counts == {
        Outcome.NEW_ENCOUNTER: 1,
        Outcome.NEW_VERSION: 3,
        Outcome.DUPLICATE: 4,
        Outcome.STALE: 1,
        Outcome.QUARANTINED: 3,
    }
    stale_new = sum(o.outcome_reason == OutcomeReason.STALE_NEW_VERSION for o in classified.outcomes)
    assert len(classified.versions) == counts[Outcome.NEW_ENCOUNTER] + counts[Outcome.NEW_VERSION] + stale_new


# --- storage and the current-state view ---


@pytest.fixture
def store(tmp_path):
    con = open_store(tmp_path / "work" / "raw.duckdb", CANONICAL)
    encounter_history.ensure_tables(con)
    yield con
    con.close()


def land(con, batch_id, rows_by_system):
    """Write synthetic rows to raw as an accepted batch (no audit rows needed here)."""
    files = [
        AcceptedFile(
            batch_id=batch_id,
            file_name=FILES[system],
            source_system=system,
            schema_version="test",
            file_sha256="0" * 64,
            manifest_row_count=len(rows),
            delivered_at="2025-01-06T06:00:00Z",
            source_header=tuple(CANONICAL),
            rows=tuple(rows),
        )
        for system, rows in rows_by_system.items()
    ]
    write_accepted_batch(con, batch_id, files, [], ingested_at=datetime(2025, 1, 6, 7, 0, tzinfo=UTC))


def apply(con, batch_id, rows_by_system):
    land(con, batch_id, rows_by_system)
    return encounter_history.apply_batch(con, batch_id, CONVENTIONS)


def current(con):
    cursor = con.execute(f"SELECT * FROM {encounter_history.CURRENT_VIEW}")
    names = [d[0] for d in cursor.description]
    return {r["source_record_id"]: r for r in (dict(zip(names, rec)) for rec in cursor.fetchall())}


def test_history_and_current_state_across_batches(store):
    apply(store, "batch_001", {EPIC: [values("E0000001", T1), values("E0000002", T1), values("E0000002", T1)]})
    apply(store, "batch_002", {EPIC: [values("E0000001", T3, claim_status="Void"), values("E0000001", T1)]})
    apply(store, "batch_003", {EPIC: [values("E0000001", T2), values("", T1)]})

    versions = store.execute(
        f"SELECT source_record_id, last_updated_ts_utc, first_seen_batch_id, arrival_outcome "
        f"FROM {encounter_history.VERSIONS_TABLE} ORDER BY 1, 2"
    ).fetchall()
    assert versions == [
        ("E0000001", datetime(2024, 3, 1, 10, 0), "batch_001", "NEW_ENCOUNTER"),
        ("E0000001", datetime(2024, 3, 2, 10, 0), "batch_003", "STALE"),
        ("E0000001", datetime(2024, 3, 3, 10, 0), "batch_002", "NEW_VERSION"),
        ("E0000002", datetime(2024, 3, 1, 10, 0), "batch_001", "NEW_ENCOUNTER"),
    ]

    state = current(store)
    assert set(state) == {"E0000001", "E0000002"}  # the quarantined row has no version
    e1 = state["E0000001"]
    assert e1["last_updated_ts_utc"] == datetime(2024, 3, 3, 10, 0)  # the late older version is not current
    assert (e1["source_batch_id"], e1["source_file_name"], e1["source_row_number"]) == ("batch_002", FILES[EPIC], 1)
    assert (e1["version_count"], e1["encounter_first_seen_batch_id"]) == (3, "batch_001")
    e2 = state["E0000002"]
    assert (e2["version_count"], e2["source_row_number"]) == (1, 2)  # first arrival (row 2), not the duplicate (row 3)

    outcome_rows = store.execute(
        f"SELECT batch_id, source_row_number, outcome, outcome_reason FROM {encounter_history.OUTCOMES_TABLE} ORDER BY 1, 2"
    ).fetchall()
    assert outcome_rows == [
        ("batch_001", 1, "NEW_ENCOUNTER", None),
        ("batch_001", 2, "NEW_ENCOUNTER", None),
        ("batch_001", 3, "DUPLICATE", "DUPLICATE_IN_BATCH"),
        ("batch_002", 1, "NEW_VERSION", None),
        ("batch_002", 2, "DUPLICATE", "DUPLICATE_OF_HELD_VERSION"),  # current before batch_002, so not stale
        ("batch_003", 1, "STALE", "STALE_NEW_VERSION"),
        ("batch_003", 2, "QUARANTINED", "SOURCE_RECORD_ID_MISSING"),
    ]
    assert store.execute(f"SELECT count(*) FROM {encounter_history.OUTCOMES_TABLE}").fetchone()[0] == store.execute(
        "SELECT count(*) FROM raw.encounters").fetchone()[0]


def test_held_versions_are_compared_with_their_first_seen_raw_row(store):
    apply(store, "batch_001", {EPIC: [values("E0000001", T1)]})
    second = apply(store, "batch_002", {EPIC: [values("E0000001", T1, billed_amount="1.00")]})

    assert [(o.outcome, o.outcome_reason, o.match_type) for o in second.outcomes] == [
        (Outcome.QUARANTINED, OutcomeReason.VERSION_CONFLICT_SAME_TS, MatchType.CONFLICT)
    ]


def test_stored_timestamps_are_utc(store):
    apply(store, "batch_001", {MEDITECH: [values("100001", "05/03/2024 10:00:00", MEDITECH)]})

    stored = store.execute(f"SELECT last_updated_ts_utc FROM {encounter_history.VERSIONS_TABLE}").fetchone()[0]
    assert stored == datetime(2024, 3, 5, 16, 0)  # 5 March, 10:00 America/Chicago (CST, UTC-6)


def test_applying_the_same_batch_twice_is_refused(store):
    land(store, "batch_001", {EPIC: [values("E0000001", T1)]})
    encounter_history.apply_batch(store, "batch_001", CONVENTIONS)

    with pytest.raises(duckdb.ConstraintException):
        encounter_history.apply_batch(store, "batch_001", CONVENTIONS)


def test_history_tables_hold_no_phi_columns(store):
    for table in (encounter_history.VERSIONS_TABLE, encounter_history.OUTCOMES_TABLE, encounter_history.CURRENT_VIEW):
        schema, name = table.split(".")
        columns = {r[0] for r in store.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ?", [schema, name]
        ).fetchall()}
        assert columns and not (columns & PHI_COLUMNS)
