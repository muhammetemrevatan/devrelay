import base64
import json
from collections.abc import Callable
from dataclasses import FrozenInstanceError, asdict, replace
from datetime import UTC, datetime, timedelta
from secrets import compare_digest
from typing import Literal, cast
from uuid import UUID

import pytest
from pydantic import SecretStr

from devrelay.application.contracts import claims
from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    InvalidClaimRequest,
    InvalidLeaseProof,
    LeaseProof,
    LeaseProofVerifier,
    WorkerClaimRequest,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.bootstrap import security
from devrelay.bootstrap.security import lease_proof_guard

WORKER_ID = UUID("22222222-2222-4222-8222-222222222222")
REQUEST_ID = UUID("11111111-1111-4111-8111-111111111111")
OTHER_ID = UUID("33333333-3333-4333-8333-333333333333")
TOKEN = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"
OTHER_TOKEN = base64.urlsafe_b64encode(bytes(reversed(range(32)))).decode().rstrip("=")
DIGEST = "1ad0d5caa96c8e04aea24aa9b144f316cdf016e3d869c9d99fae72a198725ea8"
FINGERPRINT = "525e75862a9dba4b8a64c50c3f859d791cb33ff3fefc3919fd98dee485c2cd26"


@pytest.fixture
def authorization() -> WorkerAuthorization:
    return WorkerAuthorization(
        worker_id=WORKER_ID,
        allowed_repository_ids=frozenset((OTHER_ID,)),
        credential_expires_at=datetime(2026, 10, 10, tzinfo=UTC),
    )


@pytest.fixture
def claim_request() -> WorkerClaimRequest:
    return WorkerClaimRequest(claim_request_id=REQUEST_ID, proof=LeaseProof(TOKEN))


def test_generation_requests_explicit_secure_entropy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sizes: list[int] = []

    def generate(size: int) -> str:
        sizes.append(size)
        return TOKEN

    monkeypatch.setattr(claims, "token_urlsafe", generate)

    proof = LeaseProof.generate()

    assert sizes == [32]
    assert proof.reveal() == TOKEN
    assert TOKEN not in repr(proof)
    assert TOKEN not in str(proof)


@pytest.mark.parametrize(
    "token",
    [
        "",
        TOKEN[:-1],
        TOKEN + "=",
        TOKEN + "x",
        TOKEN[:-1] + "9",
        TOKEN[:-1] + "\N{SNOWMAN}",
        TOKEN[:-1] + "+",
        TOKEN[:-1] + "/",
        TOKEN[:-1] + "\n",
        "replace-with-" + "a" * 30,
        cast(str, None),
        cast(str, TOKEN.encode()),
    ],
)
def test_noncanonical_or_untyped_proof_is_rejected_without_disclosure(
    token: str,
) -> None:
    with pytest.raises(InvalidLeaseProof) as caught:
        LeaseProof(token)

    assert str(caught.value) == "Lease proof requires a canonical 32-byte secret."
    assert caught.value.args == ("Lease proof requires a canonical 32-byte secret.",)


def test_proof_explicit_access_preserves_urlsafe_secret() -> None:
    token = base64.urlsafe_b64encode(bytes([255]) * 32).decode().rstrip("=")

    proof = LeaseProof(token)

    assert proof.reveal() == token
    assert str(proof) == "**********"
    assert token not in repr(proof)


@pytest.mark.parametrize("mutation", ["replace", "delete", "extend"])
def test_secret_wrapper_is_immutable(mutation: str) -> None:
    proof = LeaseProof(TOKEN)

    with pytest.raises(AttributeError):
        if mutation == "replace":
            proof._encoded = OTHER_TOKEN
        elif mutation == "delete":
            del proof._encoded
        else:
            setattr(proof, mutation, OTHER_TOKEN)

    assert proof.reveal() == TOKEN


def test_verifier_has_stable_versioned_digest(
    claim_request: WorkerClaimRequest,
) -> None:
    verifier = LeaseProofVerifier.from_proof(
        worker_id=WORKER_ID, claim_request_id=REQUEST_ID, proof=claim_request.proof
    )

    result = verifier.matches(
        claim_request.proof, worker_id=WORKER_ID, claim_request_id=REQUEST_ID
    )

    assert result
    assert verifier.version == 1
    assert verifier.digest.hex() == DIGEST
    assert TOKEN not in repr(verifier)
    assert DIGEST not in repr(verifier)


@pytest.mark.parametrize(
    "worker_id,request_id", [(OTHER_ID, REQUEST_ID), (WORKER_ID, OTHER_ID)]
)
def test_same_proof_does_not_authorize_other_context(
    claim_request: WorkerClaimRequest, worker_id: UUID, request_id: UUID
) -> None:
    verifier = LeaseProofVerifier.from_proof(
        worker_id=WORKER_ID, claim_request_id=REQUEST_ID, proof=claim_request.proof
    )

    result = verifier.matches(
        claim_request.proof, worker_id=worker_id, claim_request_id=request_id
    )

    assert not result


@pytest.mark.parametrize(
    "token",
    [OTHER_TOKEN, base64.urlsafe_b64encode(bytes.fromhex(DIGEST)).decode().rstrip("=")],
)
def test_other_proof_or_stored_digest_cannot_authorize(
    claim_request: WorkerClaimRequest, token: str
) -> None:
    verifier = LeaseProofVerifier.from_proof(
        worker_id=WORKER_ID, claim_request_id=REQUEST_ID, proof=claim_request.proof
    )
    presented = LeaseProof(token)

    result = verifier.matches(
        presented, worker_id=WORKER_ID, claim_request_id=REQUEST_ID
    )

    assert not result


def test_context_mismatch_still_compares_fixed_size_digests(
    claim_request: WorkerClaimRequest, monkeypatch: pytest.MonkeyPatch
) -> None:
    verifier = LeaseProofVerifier.from_proof(
        worker_id=WORKER_ID, claim_request_id=REQUEST_ID, proof=claim_request.proof
    )
    comparisons: list[tuple[int, int]] = []
    compare = compare_digest

    def tracked(expected: bytes, presented: bytes) -> bool:
        comparisons.append((len(expected), len(presented)))
        return compare(expected, presented)

    monkeypatch.setattr(claims, "compare_digest", tracked)

    result = verifier.matches(
        claim_request.proof, worker_id=OTHER_ID, claim_request_id=REQUEST_ID
    )

    assert not result
    assert comparisons == [(32, 32)]


@pytest.mark.parametrize(
    "digest",
    [b"", b"x" * 31, b"x" * 33, cast(bytes, bytearray(32)), cast(bytes, DIGEST)],
)
def test_verifier_rejects_wrong_size_or_mutable_digest(digest: bytes) -> None:
    with pytest.raises(InvalidLeaseProof):
        LeaseProofVerifier(
            worker_id=WORKER_ID, claim_request_id=REQUEST_ID, digest=digest
        )


@pytest.mark.parametrize("version", [0, 2, True, "1"])
def test_verifier_rejects_unknown_or_coerced_version(version: object) -> None:
    with pytest.raises(InvalidClaimRequest):
        LeaseProofVerifier(
            worker_id=WORKER_ID,
            claim_request_id=REQUEST_ID,
            digest=bytes.fromhex(DIGEST),
            version=cast(Literal[1], version),
        )


def test_authentication_supplies_worker_and_stable_claim_fingerprint(
    claim_request: WorkerClaimRequest, authorization: WorkerAuthorization
) -> None:
    identity = claim_request.identify(authorization)

    assert identity.worker_id == authorization.worker_id
    assert identity.claim_request_id == REQUEST_ID
    assert identity.protocol_version == identity.fingerprint_version == 1
    assert identity.fingerprint == FINGERPRINT
    assert identity.verifier.digest.hex() == DIGEST
    assert TOKEN not in repr(claim_request)
    assert TOKEN not in repr(identity)
    assert FINGERPRINT not in repr(identity)


def test_identical_reconstructed_request_resolves_same_identity(
    claim_request: WorkerClaimRequest, authorization: WorkerAuthorization
) -> None:
    stored = claim_request.identify(authorization)
    retry = WorkerClaimRequest(claim_request_id=REQUEST_ID, proof=LeaseProof(TOKEN))
    presented = retry.identify(authorization)

    stored.require_replay(presented)

    assert stored.fingerprint == presented.fingerprint
    assert stored.verifier.digest == presented.verifier.digest


@pytest.mark.parametrize("change", ["proof", "claim_request", "worker"])
def test_conflicting_replay_is_rejected_without_disclosure(
    claim_request: WorkerClaimRequest, authorization: WorkerAuthorization, change: str
) -> None:
    stored = claim_request.identify(authorization)
    retry = (
        replace(claim_request, proof=LeaseProof(OTHER_TOKEN))
        if change == "proof"
        else claim_request
    )
    if change == "claim_request":
        retry = replace(claim_request, claim_request_id=OTHER_ID)
    authority = (
        replace(authorization, worker_id=OTHER_ID)
        if change == "worker"
        else authorization
    )
    presented = retry.identify(authority)

    with pytest.raises(ClaimReplayConflict) as caught:
        stored.require_replay(presented)

    assert str(caught.value) == "Claim replay conflicts with the original input."
    assert TOKEN not in repr(caught.value)
    assert OTHER_TOKEN not in repr(caught.value)


def test_credential_rotation_does_not_change_original_request_identity(
    claim_request: WorkerClaimRequest, authorization: WorkerAuthorization
) -> None:
    stored = claim_request.identify(authorization)
    rotated = replace(
        authorization,
        credential_expires_at=authorization.credential_expires_at + timedelta(hours=1),
    )
    presented = claim_request.identify(rotated)

    stored.require_replay(presented)

    assert stored.fingerprint == presented.fingerprint
    assert stored.verifier.digest == presented.verifier.digest


def test_receipt_input_contains_only_hashes_not_raw_secret(
    claim_request: WorkerClaimRequest, authorization: WorkerAuthorization
) -> None:
    identity = claim_request.identify(authorization)

    record = asdict(identity)

    assert set(record) == {
        "worker_id",
        "claim_request_id",
        "protocol_version",
        "verifier",
        "fingerprint",
        "fingerprint_version",
    }
    assert TOKEN not in repr(record)
    assert record["verifier"]["digest"] == bytes.fromhex(DIGEST)


@pytest.mark.parametrize("version", [0, 2, True, "1"])
def test_claim_rejects_unknown_or_coerced_protocol(version: object) -> None:
    with pytest.raises(InvalidClaimRequest):
        WorkerClaimRequest(
            claim_request_id=REQUEST_ID,
            proof=LeaseProof(TOKEN),
            protocol_version=cast(Literal[1], version),
        )


def test_untyped_proof_cannot_be_claim_input() -> None:
    with pytest.raises(InvalidLeaseProof) as caught:
        WorkerClaimRequest(claim_request_id=REQUEST_ID, proof=cast(LeaseProof, TOKEN))

    assert TOKEN not in str(caught.value)


def test_raw_worker_id_cannot_replace_authenticated_authorization(
    claim_request: WorkerClaimRequest,
) -> None:
    with pytest.raises(InvalidClaimRequest):
        claim_request.identify(cast(WorkerAuthorization, WORKER_ID))


def test_string_claim_uuid_is_not_coerced(claim_request: WorkerClaimRequest) -> None:
    with pytest.raises(InvalidClaimRequest):
        replace(claim_request, claim_request_id=cast(UUID, str(REQUEST_ID)))


@pytest.mark.parametrize(
    "change", ["fingerprint", "fingerprint_version", "verifier_context"]
)
def test_restored_receipt_rejects_inconsistent_metadata(
    claim_request: WorkerClaimRequest, authorization: WorkerAuthorization, change: str
) -> None:
    original = claim_request.identify(authorization)
    other_verifier = LeaseProofVerifier.from_proof(
        worker_id=OTHER_ID, claim_request_id=REQUEST_ID, proof=claim_request.proof
    )

    with pytest.raises(InvalidClaimRequest):
        if change == "fingerprint":
            replace(original, fingerprint="0" * 64)
        elif change == "fingerprint_version":
            replace(original, fingerprint_version=cast(Literal[1], 2))
        else:
            replace(original, verifier=other_verifier)


@pytest.mark.parametrize("field", ["fingerprint", "worker_id"])
def test_request_identity_is_immutable(
    claim_request: WorkerClaimRequest, authorization: WorkerAuthorization, field: str
) -> None:
    identity = claim_request.identify(authorization)

    with pytest.raises(FrozenInstanceError):
        setattr(identity, field, None)


def test_generic_json_serialization_cannot_export_raw_proof(
    claim_request: WorkerClaimRequest,
) -> None:
    with pytest.raises(TypeError):
        json.dumps(claim_request.proof)


@pytest.mark.parametrize("position", [0, 2, None])
def test_known_credential_guard_checks_all_values_without_disclosure(
    monkeypatch: pytest.MonkeyPatch, position: int | None
) -> None:
    tokens = (OTHER_TOKEN, "unrelated_authentication_credential", "another_credential")
    sizes: list[int] = []
    compile_verifier = security.token_verifier

    def tracked_compilation(token: SecretStr) -> Callable[[str], bool]:
        verify = compile_verifier(token)

        def tracked(presented: str) -> bool:
            sizes.append(len(presented))
            return verify(presented)

        return tracked

    monkeypatch.setattr(security, "token_verifier", tracked_compilation)
    credentials = tuple(
        SecretStr(TOKEN if index == position else token)
        for index, token in enumerate(tokens)
    )
    guard = lease_proof_guard(credentials)
    proof = LeaseProof(TOKEN)

    if position is None:
        result = guard(proof)
        assert result is None
    else:
        with pytest.raises(InvalidLeaseProof) as caught:
            guard(proof)
        assert str(caught.value) == "Lease proof must be separate from credentials."

    assert sizes == [43, 43, 43]
    assert TOKEN not in repr(guard)


def test_credential_guard_accepts_independent_proof() -> None:
    guard = lease_proof_guard((SecretStr(OTHER_TOKEN),))
    proof = LeaseProof(TOKEN)

    result = guard(proof)

    assert result is None


def test_guard_without_configured_credentials_fails_closed() -> None:
    with pytest.raises(ValueError) as caught:
        lease_proof_guard(())

    assert str(caught.value) == "Lease proof guard requires known credentials."


def test_raw_proof_cannot_bypass_guard() -> None:
    guard = lease_proof_guard((SecretStr(OTHER_TOKEN),))

    with pytest.raises(InvalidLeaseProof) as caught:
        guard(cast(LeaseProof, TOKEN))

    assert TOKEN not in str(caught.value)


def test_raw_proof_cannot_bypass_verifier(claim_request: WorkerClaimRequest) -> None:
    verifier = LeaseProofVerifier.from_proof(
        worker_id=WORKER_ID, claim_request_id=REQUEST_ID, proof=claim_request.proof
    )

    with pytest.raises(InvalidLeaseProof) as caught:
        verifier.matches(
            cast(LeaseProof, TOKEN), worker_id=WORKER_ID, claim_request_id=REQUEST_ID
        )

    assert TOKEN not in str(caught.value)


@pytest.mark.parametrize("field", ["worker_id", "claim_request_id"])
def test_untyped_verifier_identity_is_rejected(field: str) -> None:
    invalid = cast(UUID, "invalid")

    with pytest.raises(InvalidClaimRequest):
        LeaseProofVerifier(
            worker_id=invalid if field == "worker_id" else WORKER_ID,
            claim_request_id=invalid if field == "claim_request_id" else REQUEST_ID,
            digest=bytes.fromhex(DIGEST),
        )


@pytest.mark.parametrize("value", [None, "wrong", FINGERPRINT.upper()])
def test_restored_fingerprint_is_strictly_validated(
    claim_request: WorkerClaimRequest, authorization: WorkerAuthorization, value: object
) -> None:
    identity = claim_request.identify(authorization)

    with pytest.raises(InvalidClaimRequest):
        replace(identity, fingerprint=cast(str, value))
