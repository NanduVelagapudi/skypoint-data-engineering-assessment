"""Linkage and patient_key. Identities are synthetic."""

import hashlib
import hmac
import json
import random
from datetime import date

import pytest

from pipeline.parsers.patient import Sex
from pipeline.parsers.result import FieldReason
from pipeline.patient_identity import (
    IdentityRow,
    LinkageKey,
    LinkStatus,
    build_linkage_key,
    linked_patient_key,
    resolve_identities,
    unlinked_patient_key,
)

SECRET = b"unit-test-secret"
DOB = date(1980, 7, 15)


def row(system, mrn, last="Testerson", first="Anna", dob=DOB, sex=Sex.F):
    return IdentityRow(system, mrn, build_linkage_key(last, first, dob, sex), dob is not None)


# --- linkage key ---


def test_equivalent_spellings_give_the_same_linkage_key():
    a = build_linkage_key("O'Testa", "Anna B.", DOB, Sex.F)
    b = build_linkage_key("  otesta ", "ANNA", DOB, Sex.F)

    assert a == b == LinkageKey("OTESTA", "ANNA", DOB, Sex.F)


@pytest.mark.parametrize(
    "last, first, dob, sex",
    [
        ("Testerson", "Anna", None, Sex.F),
        ("Testerson", "Anna", DOB, Sex.UNKNOWN),
        ("", "Anna", DOB, Sex.F),
        ("Testerson", "  ", DOB, Sex.F),
    ],
)
def test_all_four_fields_are_required(last, first, dob, sex):
    assert build_linkage_key(last, first, dob, sex) is None


def test_linkage_key_repr_hides_phi():
    text = repr(build_linkage_key("Testerson", "Anna", DOB, Sex.F)) + repr(row("EPIC_NORTH", "EP0000001"))

    assert "TESTERSON" not in text.upper() and "1980" not in text and "EP0000001" not in text


# --- patient_key ---


def test_key_is_hmac_sha256_of_the_documented_payloads():
    key = LinkageKey("TESTERSON", "ANNA", DOB, Sex.F)
    linked_payload = json.dumps(["patient_key/v1", "LINKED", "TESTERSON", "ANNA", "1980-07-15", "F"], separators=(",", ":"))
    unlinked_payload = json.dumps(["patient_key/v1", "UNLINKED", "EPIC_NORTH", "EP0000001"], separators=(",", ":"))

    assert linked_patient_key(SECRET, key) == hmac.new(SECRET, linked_payload.encode(), hashlib.sha256).hexdigest()
    assert unlinked_patient_key(SECRET, "EPIC_NORTH", "EP0000001") == hmac.new(
        SECRET, unlinked_payload.encode(), hashlib.sha256
    ).hexdigest()
    assert len(linked_patient_key(SECRET, key)) == 64


def test_same_identity_in_two_systems_gets_one_linked_key():
    resolved = resolve_identities(
        [row("EPIC_NORTH", "EP0000001", "O'Testa", "Anna B."), row("LEGACY_MEDITECH", "MT0000001", "OTESTA", "ANNA")],
        SECRET,
    )

    epic, meditech = resolved[("EPIC_NORTH", "EP0000001")], resolved[("LEGACY_MEDITECH", "MT0000001")]
    assert epic.patient_key == meditech.patient_key
    assert epic.link_status == meditech.link_status == LinkStatus.LINKED
    assert epic.reason is None


@pytest.mark.parametrize(
    "change",
    [{"last": "Testerby"}, {"first": "Anne"}, {"dob": date(1980, 7, 16)}, {"sex": Sex.M}],
)
def test_any_differing_field_gives_a_different_key(change):
    resolved = resolve_identities([row("EPIC_NORTH", "EP0000001"), row("LEGACY_MEDITECH", "MT0000001", **change)], SECRET)

    assert resolved[("EPIC_NORTH", "EP0000001")].patient_key != resolved[("LEGACY_MEDITECH", "MT0000001")].patient_key


def test_missing_dob_never_links_and_unlinked_keys_are_source_scoped():
    resolved = resolve_identities(
        [row("EPIC_NORTH", "XX0000001", dob=None), row("LEGACY_MEDITECH", "XX0000001", dob=None)], SECRET
    )

    epic, meditech = resolved[("EPIC_NORTH", "XX0000001")], resolved[("LEGACY_MEDITECH", "XX0000001")]
    assert epic.link_status == meditech.link_status == LinkStatus.UNLINKED
    assert epic.reason == meditech.reason == FieldReason.PATIENT_UNLINKED_NO_DOB
    assert epic.patient_key != meditech.patient_key  # same name and MRN text, different systems
    assert epic.patient_key == unlinked_patient_key(SECRET, "EPIC_NORTH", "XX0000001")


def test_linked_and_unlinked_namespaces_cannot_collide():
    linked = linked_patient_key(SECRET, LinkageKey("EPIC_NORTH", "EP0000001", DOB, Sex.F))
    unlinked = unlinked_patient_key(SECRET, "EPIC_NORTH", "EP0000001")

    assert linked != unlinked


def test_dob_on_any_row_of_an_identity_is_enough_to_link():
    resolved = resolve_identities(
        [row("EPIC_NORTH", "EP0000001", dob=None), row("EPIC_NORTH", "EP0000001"), row("LEGACY_MEDITECH", "MT0000001")],
        SECRET,
    )

    assert resolved[("EPIC_NORTH", "EP0000001")].patient_key == resolved[("LEGACY_MEDITECH", "MT0000001")].patient_key


def test_dob_but_unknown_sex_is_unlinked_incomplete():
    identity = resolve_identities([row("ATHENA_CLINICS", "AT0000001", sex=Sex.UNKNOWN)], SECRET)[("ATHENA_CLINICS", "AT0000001")]

    assert (identity.link_status, identity.reason) == (LinkStatus.UNLINKED, FieldReason.PATIENT_UNLINKED_INCOMPLETE)
    assert identity.patient_key == unlinked_patient_key(SECRET, "ATHENA_CLINICS", "AT0000001")


def test_conflicting_attributes_within_one_identity_are_not_linked():
    resolved = resolve_identities(
        [row("EPIC_NORTH", "EP0000001"), row("EPIC_NORTH", "EP0000001", dob=date(1981, 1, 1)), row("LEGACY_MEDITECH", "MT0000001")],
        SECRET,
    )

    epic = resolved[("EPIC_NORTH", "EP0000001")]
    assert (epic.link_status, epic.reason) == (LinkStatus.UNLINKED, FieldReason.PATIENT_UNLINKED_CONFLICT)
    assert epic.patient_key != resolved[("LEGACY_MEDITECH", "MT0000001")].patient_key


def test_blank_mrn_gets_no_key():
    identity = resolve_identities([row("EPIC_NORTH", "   ")], SECRET)[("EPIC_NORTH", "")]

    assert (identity.patient_key, identity.reason) == (None, FieldReason.PATIENT_MRN_MISSING)


def test_mrn_whitespace_does_not_split_an_identity():
    resolved = resolve_identities([row("EPIC_NORTH", "EP0000001", dob=None), row("EPIC_NORTH", " EP0000001 ", dob=None)], SECRET)

    assert list(resolved) == [("EPIC_NORTH", "EP0000001")]


def test_keys_are_deterministic_and_independent_of_row_order():
    rows = [row("EPIC_NORTH", f"EP000000{i}", first=f"Anna{i}") for i in range(5)] + [row("LEGACY_MEDITECH", "MT0000001", dob=None)]
    shuffled = rows[:]
    random.Random(7).shuffle(shuffled)

    assert resolve_identities(rows, SECRET) == resolve_identities(shuffled, SECRET) == resolve_identities(rows, SECRET)


def test_the_secret_changes_every_key():
    rows = [row("EPIC_NORTH", "EP0000001"), row("LEGACY_MEDITECH", "MT0000002", dob=None)]
    first, second = resolve_identities(rows, SECRET), resolve_identities(rows, b"another-secret")

    for identity in first:
        assert first[identity].patient_key != second[identity].patient_key
