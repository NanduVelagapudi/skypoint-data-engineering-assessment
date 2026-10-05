"""Manifest tests. Synthetic manifests are in-memory bytes; the real data pack is only read."""

import copy
import json

import pytest
from conftest import CONTRACT_PATH, REAL_DATA_DIR

from pipeline.csv_reader import parse_csv
from pipeline.errors import BatchRejected, ReasonCode
from pipeline.manifest import check_row_count, check_sha256, load_manifest, parse_manifest, sha256_hex
from pipeline.schema_contract import load_contracts, match_header

CONTRACTS = load_contracts(CONTRACT_PATH)
LANDING = REAL_DATA_DIR / "landing"

EPIC, MEDITECH, ATHENA = (
    "encounters_epic_north.csv",
    "encounters_legacy_meditech.csv",
    "encounters_athena_clinics.csv",
)
PRESENT = [EPIC, MEDITECH, ATHENA]
VALID = {
    "batch_id": "batch_009",
    "delivered_at": "2025-03-03T06:00:00Z",
    "files": [
        {"file_name": EPIC, "source_system": "EPIC_NORTH", "row_count": 3, "sha256": "a" * 64},
        {"file_name": MEDITECH, "source_system": "LEGACY_MEDITECH", "row_count": 0, "sha256": "B" * 64},
        {"file_name": ATHENA, "source_system": "ATHENA_CLINICS", "row_count": 7, "sha256": "c" * 64},
    ],
}


def _valid():
    return copy.deepcopy(VALID)


def _parse(raw, present=PRESENT, folder="batch_009"):
    return parse_manifest(json.dumps(raw).encode("utf-8"), folder, present, CONTRACTS)


def _rejection(raw, **kwargs) -> list[str]:
    with pytest.raises(BatchRejected) as exc_info:
        _parse(raw, **kwargs)
    return [str(f) for f in exc_info.value.failures]


# --- valid manifests ---


def test_valid_manifest_is_parsed_and_entries_sorted_by_file_name():
    manifest = _parse(_valid())

    assert manifest.batch_id == "batch_009"
    assert manifest.delivered_at == "2025-03-03T06:00:00Z"
    assert [e.file_name for e in manifest.entries] == sorted(PRESENT)
    assert next(e for e in manifest.entries if e.file_name == MEDITECH).sha256 == "b" * 64


def test_manifest_with_utf8_bom_is_accepted():
    content = b"\xef\xbb\xbf" + json.dumps(_valid()).encode("utf-8")

    assert parse_manifest(content, "batch_009", PRESENT, CONTRACTS).batch_id == "batch_009"


@pytest.mark.parametrize("batch_id", ["batch_001", "batch_002", "batch_003", "batch_004"])
def test_real_manifests_load(batch_id):
    manifest = load_manifest(LANDING / batch_id, CONTRACTS)

    assert manifest.batch_id == batch_id
    assert [e.file_name for e in manifest.entries] == sorted(PRESENT)


# --- per-file SHA-256 and row-count checks ---


def test_sha256_is_computed_over_raw_bytes():
    entry = load_manifest(LANDING / "batch_001", CONTRACTS).entries[0]
    content = (LANDING / "batch_001" / entry.file_name).read_bytes()

    assert check_sha256(entry, sha256_hex(content)) is None
    assert check_sha256(entry, sha256_hex(content + b"\n")).code == ReasonCode.SHA256_MISMATCH


def test_row_count_check():
    entry = next(e for e in _parse(_valid()).entries if e.file_name == EPIC)  # row_count 3

    assert check_row_count(entry, 3) is None
    assert str(check_row_count(entry, 2)) == "ROW_COUNT_MISMATCH(expected=3,received=2)"


@pytest.mark.parametrize("batch_id", ["batch_001", "batch_002", "batch_003"])
def test_every_file_in_batches_001_to_003_matches_its_manifest_and_contract(batch_id):
    for entry in load_manifest(LANDING / batch_id, CONTRACTS).entries:
        content = (LANDING / batch_id / entry.file_name).read_bytes()
        parsed = parse_csv(content)

        assert check_sha256(entry, sha256_hex(content)) is None
        assert parsed.failures == []
        assert check_row_count(entry, parsed.record_count) is None
        assert match_header(CONTRACTS, entry.source_system, parsed.header) is not None


def test_real_batch_004_epic_file_fails_hash_row_count_and_record_checks():
    manifest = load_manifest(LANDING / "batch_004", CONTRACTS)
    epic = next(e for e in manifest.entries if e.source_system == "EPIC_NORTH")
    content = (LANDING / "batch_004" / epic.file_name).read_bytes()
    parsed = parse_csv(content)

    assert check_sha256(epic, sha256_hex(content)).code == ReasonCode.SHA256_MISMATCH
    assert str(check_row_count(epic, parsed.record_count)) == "ROW_COUNT_MISMATCH(expected=22,received=18)"
    assert [str(f) for f in parsed.failures] == ["MALFORMED_RECORD(record=18,fields=3,expected=21)"]


def test_real_batch_004_meditech_and_athena_files_are_individually_valid():
    manifest = load_manifest(LANDING / "batch_004", CONTRACTS)
    for entry in (e for e in manifest.entries if e.source_system != "EPIC_NORTH"):
        content = (LANDING / "batch_004" / entry.file_name).read_bytes()
        parsed = parse_csv(content)

        assert check_sha256(entry, sha256_hex(content)) is None
        assert parsed.failures == []
        assert check_row_count(entry, parsed.record_count) is None


# --- manifest-level rejections ---


def test_missing_manifest_is_rejected():
    with pytest.raises(BatchRejected) as exc_info:
        load_manifest(REAL_DATA_DIR / "reference", CONTRACTS)  # a real folder with no manifest.json

    assert [str(f) for f in exc_info.value.failures] == ["MANIFEST_MISSING"]


@pytest.mark.parametrize("content", [b"{not json", b"\xff\xfe{}", b"[]", b'{"files": []}', b'{"files": "x"}'])
def test_unreadable_or_shapeless_manifest_is_rejected(content):
    with pytest.raises(BatchRejected) as exc_info:
        parse_manifest(content, "batch_009", PRESENT, CONTRACTS)

    assert [f.code for f in exc_info.value.failures] == [ReasonCode.MANIFEST_INVALID]


def test_batch_id_mismatch_does_not_echo_either_id():
    reasons = _rejection(_valid(), folder="batch_010")

    assert reasons == ["BATCH_ID_MISMATCH(manifest batch_id differs from folder name)"]


@pytest.mark.parametrize("field", ["batch_id", "delivered_at"])
def test_missing_top_level_field_is_rejected(field):
    raw = _valid()
    del raw[field]

    assert _rejection(raw) == [f"MANIFEST_INVALID(field={field})"]


@pytest.mark.parametrize(
    "field, value",
    [
        ("row_count", -1),
        ("row_count", "3"),
        ("row_count", 3.0),
        ("row_count", True),
        ("row_count", None),
        ("sha256", "abc"),
        ("sha256", "g" * 64),
        ("file_name", ""),
        ("source_system", None),
    ],
)
def test_invalid_entry_field_is_rejected(field, value):
    raw = _valid()
    raw["files"][1][field] = value

    assert f"MANIFEST_INVALID(entry=2,field={field})" in _rejection(raw)


def test_unknown_source_system_is_rejected():
    raw = _valid()
    raw["files"][0]["source_system"] = "CERNER_SOUTH"

    reasons = _rejection(raw)

    assert "UNKNOWN_SOURCE_SYSTEM(entry=1)" in reasons
    assert "CERNER" not in "; ".join(reasons)


def test_file_name_must_match_the_contract_for_its_source_system():
    raw = _valid()
    raw["files"][0]["file_name"] = "../../outside.csv"

    reasons = _rejection(raw, present=[MEDITECH, ATHENA])

    assert reasons == ["MANIFEST_INVALID(entry=1,field=file_name)"]


def test_duplicate_entry_is_rejected():
    raw = _valid()
    raw["files"].append(copy.deepcopy(raw["files"][0]))

    assert _rejection(raw) == ["MANIFEST_INVALID(entry=4,duplicate=file_name)"]


def test_listed_file_missing_from_folder_is_rejected():
    assert _rejection(_valid(), present=[EPIC, ATHENA]) == ["FILE_MISSING(entry=2)"]


def test_unlisted_csv_in_folder_is_rejected_without_echoing_its_name():
    reasons = _rejection(_valid(), present=PRESENT + ["Doe_Jane_export.csv"])

    assert reasons == ["UNEXPECTED_FILE(count=1)"]


def test_file_name_case_is_not_guessed():
    reasons = _rejection(_valid(), present=[EPIC.upper(), MEDITECH, ATHENA])

    assert reasons == ["FILE_MISSING(entry=1)", "UNEXPECTED_FILE(count=1)"]


def test_all_problems_are_reported_together():
    raw = _valid()
    raw["files"][0]["row_count"] = -5
    raw["files"][2]["sha256"] = "short"

    reasons = _rejection(raw, folder="batch_010")

    assert reasons == [
        "BATCH_ID_MISMATCH(manifest batch_id differs from folder name)",
        "MANIFEST_INVALID(entry=1,field=row_count)",
        "MANIFEST_INVALID(entry=3,field=sha256)",
    ]
