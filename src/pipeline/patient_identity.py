"""Patient linkage and the pseudonymous patient_key.

Identity: within a source system, patient_mrn identifies a patient, so an
identity is (source_system, MRN), never the MRN alone.

Linkage uses only the brief's minimum rule: normalised last name, first given
name, DOB and sex (F or M), all four required. No fuzzy or nickname matching,
and no fallback rule when one is missing. Identities with the same linkage key
are one person, in any source system.

patient_key = HMAC-SHA256(secret, canonical JSON payload), hex-encoded:
    LINKED    ["patient_key/v1", "LINKED", last, first, dob ISO date, sex]
    UNLINKED  ["patient_key/v1", "UNLINKED", source_system, MRN]
The namespace in the payload keeps the two kinds from ever colliding, and the
UNLINKED payload includes the source system, so an unlinked key can never join
identities across systems. JSON encoding makes the payload unambiguous.

An identity is resolved from all of its rows:
    exactly one linkage key across its rows   LINKED
    no complete key, no valid DOB anywhere     UNLINKED, PATIENT_UNLINKED_NO_DOB
    no complete key, but a DOB somewhere       UNLINKED, PATIENT_UNLINKED_INCOMPLETE
    more than one key                          UNLINKED, PATIENT_UNLINKED_CONFLICT
    blank MRN                                  no key,   PATIENT_MRN_MISSING

The key is derived from the identity's current attributes, so it is the same on
every re-run with the same secret and data. An identity that is unlinked today
gets a different (LINKED) key if a later batch supplies a valid DOB. There is no
persistent patient master that would keep the old key.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from enum import StrEnum

from pipeline.parsers.patient import Sex, normalise_first_given_name, normalise_last_name
from pipeline.parsers.result import FieldReason

KEY_VERSION = "patient_key/v1"


class LinkStatus(StrEnum):
    LINKED = "LINKED"
    UNLINKED = "UNLINKED"


@dataclass(frozen=True)
class LinkageKey:
    last: str
    first: str
    dob: date
    sex: Sex

    def __repr__(self) -> str:  # holds PHI; never print it
        return "LinkageKey(<redacted>)"


@dataclass(frozen=True)
class IdentityRow:
    """One raw row's view of its identity. Built in memory, never stored."""

    source_system: str
    mrn: str
    linkage_key: LinkageKey | None
    has_dob: bool

    def __repr__(self) -> str:
        return f"IdentityRow(source_system={self.source_system!r}, <redacted>)"


@dataclass(frozen=True)
class PatientIdentity:
    patient_key: str | None
    link_status: LinkStatus
    reason: FieldReason | None


def build_linkage_key(
    last_name: str | None, first_name: str | None, dob: date | None, sex: Sex
) -> LinkageKey | None:
    """The four-field key, or None if any part is missing (sex must be F or M)."""
    last = normalise_last_name(last_name).cleaned_value
    first = normalise_first_given_name(first_name).cleaned_value
    if dob is None or last is None or first is None or sex == Sex.UNKNOWN:
        return None
    return LinkageKey(last, first, dob, sex)


def _hmac(secret: bytes, payload: list[str]) -> str:
    message = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hmac.new(secret, message, hashlib.sha256).hexdigest()


def linked_patient_key(secret: bytes, key: LinkageKey) -> str:
    return _hmac(secret, [KEY_VERSION, LinkStatus.LINKED, key.last, key.first, key.dob.isoformat(), key.sex])


def unlinked_patient_key(secret: bytes, source_system: str, mrn: str) -> str:
    return _hmac(secret, [KEY_VERSION, LinkStatus.UNLINKED, source_system, mrn])


def resolve_identities(rows: Iterable[IdentityRow], secret: bytes) -> dict[tuple[str, str], PatientIdentity]:
    """PatientIdentity per (source_system, stripped MRN), from all of that identity's rows."""
    by_identity: dict[tuple[str, str], list[IdentityRow]] = defaultdict(list)
    for row in rows:
        by_identity[(row.source_system, row.mrn.strip())].append(row)

    resolved: dict[tuple[str, str], PatientIdentity] = {}
    for (source_system, mrn), identity_rows in by_identity.items():
        if not mrn:
            resolved[(source_system, mrn)] = PatientIdentity(None, LinkStatus.UNLINKED, FieldReason.PATIENT_MRN_MISSING)
            continue
        keys = {row.linkage_key for row in identity_rows if row.linkage_key is not None}
        if len(keys) == 1:
            identity = PatientIdentity(linked_patient_key(secret, keys.pop()), LinkStatus.LINKED, None)
        else:
            if keys:
                reason = FieldReason.PATIENT_UNLINKED_CONFLICT
            elif any(row.has_dob for row in identity_rows):
                reason = FieldReason.PATIENT_UNLINKED_INCOMPLETE
            else:
                reason = FieldReason.PATIENT_UNLINKED_NO_DOB
            identity = PatientIdentity(unlinked_patient_key(secret, source_system, mrn), LinkStatus.UNLINKED, reason)
        resolved[(source_system, mrn)] = identity
    return resolved
