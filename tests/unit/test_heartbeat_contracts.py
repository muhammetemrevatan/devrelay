from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, cast
from uuid import UUID

import pytest

from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    LeaseProof,
    WorkerClaimRequest,
)
from devrelay.application.contracts.heartbeats import (
    MAXIMUM_LEASE_REVISION,
    HeartbeatRequest,
    HeartbeatResult,
    LeaseAuthorityObservation,
)
from devrelay.application.contracts.worker_recovery import AssignmentReference
from devrelay.application.contracts.workers import WorkerAuthorization

NOW = datetime(2026, 10, 12, tzinfo=UTC)
PROOF = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"


def authorization() -> WorkerAuthorization:
    return WorkerAuthorization(
        worker_id=UUID(int=1),
        allowed_repository_ids=frozenset({UUID(int=3)}),
        credential_expires_at=NOW + timedelta(seconds=300),
    )


def request() -> HeartbeatRequest:
    return HeartbeatRequest(
        claim=WorkerClaimRequest(claim_request_id=UUID(int=2), proof=LeaseProof(PROOF)),
        heartbeat_request_id=UUID(int=4),
        attempt_id=UUID(int=5),
        generation=1,
        expected_lease_revision=1,
    )


def observation() -> LeaseAuthorityObservation:
    return LeaseAuthorityObservation(
        reference=AssignmentReference(
            worker_id=UUID(int=1),
            job_id=UUID(int=6),
            repository_id=UUID(int=3),
            attempt_id=UUID(int=5),
            generation=1,
            issued_at=NOW,
            expires_at=NOW + timedelta(seconds=120),
        ),
        lease_revision=2,
        observed_at=NOW + timedelta(seconds=32),
        updated_at=NOW + timedelta(seconds=30),
        current_expires_at=NOW + timedelta(seconds=150),
    )


def result() -> HeartbeatResult:
    return HeartbeatResult(
        heartbeat_request_id=UUID(int=4),
        authority=observation(),
        renewed=True,
        stop_required=False,
    )


def test_renewed_authority_preserves_original_claim_anchors() -> None:
    original = observation()

    renewed = replace(
        original,
        lease_revision=3,
        observed_at=NOW + timedelta(seconds=62),
        updated_at=NOW + timedelta(seconds=60),
        current_expires_at=NOW + timedelta(seconds=180),
    )

    assert renewed.reference == original.reference
    assert renewed.current_expires_at > original.current_expires_at
    assert renewed.reference.expires_at == NOW + timedelta(seconds=120)


@pytest.mark.parametrize("field", ["lease_revision", "expected_lease_revision"])
@pytest.mark.parametrize("value", [True, False, 0, -1, 1.0, "1", 1 << 63])
def test_revisions_reject_coercion_and_out_of_range_values(
    field: str, value: object
) -> None:
    original = observation() if field == "lease_revision" else request()

    with pytest.raises(ValueError, match="bounded positive integer"):
        replace(original, **{field: cast(Any, value)})


def test_lease_revision_accepts_maximum_boundary() -> None:
    original = observation()

    accepted = replace(original, lease_revision=MAXIMUM_LEASE_REVISION)

    assert accepted.lease_revision == MAXIMUM_LEASE_REVISION


def test_initial_authority_revision_retains_original_lease() -> None:
    original = observation()

    initial = replace(
        original,
        lease_revision=1,
        updated_at=original.reference.issued_at,
        current_expires_at=original.reference.expires_at,
    )

    assert initial.updated_at == initial.reference.issued_at
    assert initial.current_expires_at == initial.reference.expires_at


@pytest.mark.parametrize("change", ["expiry", "update"])
def test_initial_revision_cannot_include_authority_mutation(change: str) -> None:
    observed = observation()
    initial = replace(
        observed,
        lease_revision=1,
        updated_at=observed.reference.issued_at,
        current_expires_at=observed.reference.expires_at,
    )
    mutations: dict[str, dict[str, Any]] = {
        "expiry": {
            "current_expires_at": initial.current_expires_at + timedelta(seconds=1)
        },
        "update": {"updated_at": initial.updated_at + timedelta(seconds=1)},
    }

    with pytest.raises(ValueError):
        replace(initial, **mutations[change])


@pytest.mark.parametrize("revision", [1, 2])
def test_revocation_preserves_original_or_latest_renewal_fields(revision: int) -> None:
    original = observation()
    if revision == 1:
        original = replace(
            original,
            lease_revision=1,
            updated_at=original.reference.issued_at,
            current_expires_at=original.reference.expires_at,
        )
    revoked = replace(original, revoked_at=original.observed_at)
    assert revoked.updated_at == original.updated_at
    assert revoked.lease_revision == original.lease_revision
    assert revoked.current_expires_at == original.current_expires_at
    assert revoked.revoked_at is not None
    assert revoked.revoked_at > revoked.updated_at
    heartbeat = HeartbeatResult(
        heartbeat_request_id=UUID(int=4),
        authority=revoked,
        renewed=False,
        stop_required=True,
    )
    assert heartbeat.stop_required
    with pytest.raises(ValueError):
        replace(heartbeat, stop_required=False)


@pytest.mark.parametrize(
    "field,value",
    [
        ("updated_at", NOW - timedelta(microseconds=1)),
        ("observed_at", NOW + timedelta(seconds=29)),
        ("current_expires_at", NOW + timedelta(seconds=119)),
        ("current_expires_at", NOW + timedelta(seconds=330, microseconds=1)),
        ("revoked_at", NOW + timedelta(seconds=29)),
        ("revoked_at", NOW + timedelta(seconds=33)),
        ("reference", None),
    ],
)
def test_lease_observation_rejects_inconsistent_chronology(
    field: str, value: object
) -> None:
    original = observation()

    with pytest.raises(ValueError):
        replace(original, **{field: cast(Any, value)})


def test_live_lease_update_cannot_reach_its_deadline() -> None:
    original = observation()

    with pytest.raises(ValueError):
        replace(
            original,
            updated_at=original.current_expires_at,
            observed_at=original.current_expires_at,
        )


def test_observation_accepts_maximum_ttl_boundary() -> None:
    original = observation()

    accepted = replace(
        original, current_expires_at=original.updated_at + timedelta(seconds=300)
    )

    assert accepted.current_expires_at - accepted.updated_at == timedelta(seconds=300)


@pytest.mark.parametrize(
    "value",
    [NOW.replace(tzinfo=None), "now"],
)
@pytest.mark.parametrize(
    "field", ["observed_at", "updated_at", "current_expires_at", "revoked_at"]
)
def test_observation_requires_utc_datetimes(field: str, value: object) -> None:
    original = observation()

    with pytest.raises(ValueError):
        replace(original, **{field: cast(Any, value)})


@pytest.mark.parametrize("field", ["observed_at", "updated_at", "current_expires_at"])
def test_observation_requires_all_authority_timestamps(field: str) -> None:
    original = observation()

    with pytest.raises(ValueError):
        replace(original, **{field: cast(Any, None)})


@pytest.mark.parametrize(
    "field", ["observed_at", "updated_at", "current_expires_at", "revoked_at"]
)
def test_observation_normalizes_aware_timestamps_to_utc(field: str) -> None:
    original = observation()
    if field == "revoked_at":
        original = replace(original, revoked_at=original.updated_at)
    timestamp = getattr(original, field)
    shifted = timestamp.astimezone(timezone(timedelta(hours=3)))

    accepted = replace(original, **{field: shifted})

    assert getattr(accepted, field) == timestamp
    assert getattr(accepted, field).tzinfo is UTC


@pytest.mark.parametrize("field", ["generation", "protocol_version"])
@pytest.mark.parametrize("value", [True, False, 0, 2, 1.0, "1"])
def test_heartbeat_request_requires_strict_initial_versions(
    field: str, value: object
) -> None:
    original = request()

    with pytest.raises(ValueError):
        replace(original, **{field: cast(Any, value)})


@pytest.mark.parametrize("field", ["heartbeat_request_id", "attempt_id", "claim"])
def test_heartbeat_request_requires_typed_original_inputs(field: str) -> None:
    original = request()

    with pytest.raises(ValueError):
        replace(original, **{field: cast(Any, "invalid")})


def test_heartbeat_fingerprint_is_stable_and_secret_free() -> None:
    original = request()
    authenticated = authorization()

    identity = original.identify(authenticated)

    assert (
        identity.fingerprint
        == "d2a33e0be27d17242cd742c0ab52278001e80262c9471b1cea35686fcfcb9445"
    )
    assert PROOF not in repr(original)
    assert PROOF not in repr(identity)
    assert identity.fingerprint not in repr(identity)
    assert identity.claim.worker_id == authenticated.worker_id


def test_exact_heartbeat_fingerprint_matches_replay() -> None:
    identity = request().identify(authorization())

    identity.require_replay(identity.fingerprint)

    assert identity.protocol_version == 1


@pytest.mark.parametrize(
    "change", ["worker", "proof", "claim", "request", "attempt", "revision"]
)
def test_heartbeat_fingerprint_binds_actor_original_proof_and_operation(
    change: str,
) -> None:
    original = request()
    authenticated = authorization()
    accepted = original.identify(authenticated)
    candidate = original
    if change == "worker":
        authenticated = replace(authenticated, worker_id=UUID(int=10))
    elif change == "proof":
        candidate = replace(
            original,
            claim=replace(original.claim, proof=LeaseProof("A" * 43)),
        )
    elif change == "claim":
        candidate = replace(
            original, claim=replace(original.claim, claim_request_id=UUID(int=10))
        )
    elif change == "request":
        candidate = replace(original, heartbeat_request_id=UUID(int=10))
    elif change == "attempt":
        candidate = replace(original, attempt_id=UUID(int=10))
    else:
        candidate = replace(original, expected_lease_revision=2)
    changed = candidate.identify(authenticated)

    with pytest.raises(ClaimReplayConflict):
        changed.require_replay(accepted.fingerprint)


@pytest.mark.parametrize("value", ["A" * 64, "a" * 63, "é" * 64, 1, None])
def test_heartbeat_replay_rejects_malformed_fingerprint(value: object) -> None:
    identity = request().identify(authorization())

    with pytest.raises(ClaimReplayConflict):
        identity.require_replay(cast(str, value))


def test_heartbeat_result_validates_current_request_and_scope() -> None:
    completed = result()
    original, authenticated = request(), authorization()

    completed.require_correlation(original, authenticated)

    assert completed.authority.reference.worker_id == authenticated.worker_id


@pytest.mark.parametrize("reason", ["expired", "revoked"])
def test_expired_or_revoked_authority_requires_stop(reason: str) -> None:
    observed = observation()
    stopped = (
        replace(observed, observed_at=observed.current_expires_at)
        if reason == "expired"
        else replace(observed, revoked_at=observed.updated_at)
    )

    with pytest.raises(ValueError, match="cannot renew or execute"):
        HeartbeatResult(
            heartbeat_request_id=UUID(int=4),
            authority=stopped,
            renewed=False,
            stop_required=False,
        )


def test_expired_stop_result_preserves_historical_evidence() -> None:
    observed = observation()
    expired = replace(observed, observed_at=observed.current_expires_at)

    stopped = HeartbeatResult(
        heartbeat_request_id=UUID(int=4),
        authority=expired,
        renewed=False,
        stop_required=True,
    )

    assert stopped.authority.reference == observed.reference


@pytest.mark.parametrize("field", ["renewed", "stop_required"])
def test_heartbeat_result_rejects_coerced_flags(field: str) -> None:
    original = result()

    with pytest.raises(ValueError):
        replace(original, **{field: cast(Any, 1)})


def test_stopped_result_cannot_claim_renewal() -> None:
    original = result()

    with pytest.raises(ValueError):
        replace(original, stop_required=True)


@pytest.mark.parametrize(
    "change", ["request", "attempt", "worker", "repository", "revision", "credential"]
)
def test_result_correlation_rejects_changed_request_or_scope(change: str) -> None:
    completed = result()
    original, authenticated = request(), authorization()
    if change == "request":
        original = replace(original, heartbeat_request_id=UUID(int=10))
    elif change == "attempt":
        original = replace(original, attempt_id=UUID(int=10))
    elif change == "worker":
        authenticated = replace(authenticated, worker_id=UUID(int=10))
    elif change == "repository":
        authenticated = replace(
            authenticated, allowed_repository_ids=frozenset({UUID(int=10)})
        )
    elif change == "revision":
        original = replace(original, expected_lease_revision=3)
    else:
        authenticated = replace(
            authenticated, credential_expires_at=completed.authority.observed_at
        )

    with pytest.raises(ValueError, match="current request scope"):
        completed.require_correlation(original, authenticated)


def test_renewal_cannot_exceed_current_credential_expiry() -> None:
    completed = result()
    original = request()
    authenticated = replace(
        authorization(),
        credential_expires_at=completed.authority.current_expires_at
        - timedelta(microseconds=1),
    )

    with pytest.raises(ValueError):
        completed.require_correlation(original, authenticated)


def test_unrenewed_observation_retains_server_deadline_with_shorter_credential() -> (
    None
):
    completed = replace(result(), renewed=False)
    original = request()
    authenticated = replace(
        authorization(), credential_expires_at=NOW + timedelta(seconds=100)
    )

    completed.require_correlation(original, authenticated)

    assert completed.authority.current_expires_at > authenticated.credential_expires_at


def test_later_duplicate_observation_does_not_claim_another_renewal() -> None:
    completed = replace(
        result(), authority=replace(observation(), lease_revision=3), renewed=False
    )
    original, authenticated = request(), authorization()

    completed.require_correlation(original, authenticated)

    assert not completed.renewed and completed.authority.lease_revision == 3


def test_renewed_response_requires_one_revision_increment() -> None:
    completed = replace(result(), authority=replace(observation(), lease_revision=3))
    original, authenticated = request(), authorization()

    with pytest.raises(ValueError):
        completed.require_correlation(original, authenticated)


def test_revision_exhaustion_cannot_report_another_renewal() -> None:
    completed = replace(
        result(),
        authority=replace(observation(), lease_revision=MAXIMUM_LEASE_REVISION),
    )
    original = replace(request(), expected_lease_revision=MAXIMUM_LEASE_REVISION)
    authenticated = authorization()

    with pytest.raises(ValueError):
        completed.require_correlation(original, authenticated)


@pytest.mark.parametrize("field", ["heartbeat_request_id", "authority"])
def test_heartbeat_result_requires_typed_reference_and_request(field: str) -> None:
    original = result()

    with pytest.raises(ValueError):
        replace(original, **{field: cast(Any, None)})


def test_heartbeat_request_is_immutable() -> None:
    original = request()

    with pytest.raises(FrozenInstanceError):
        original.attempt_id = UUID(int=10)  # type: ignore[misc]  # Verify frozen input.

    assert original.attempt_id == UUID(int=5)
