import asyncio
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import UUID

import pytest
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncEngine

from devrelay.application.contracts.assignments import (
    ClaimPersistenceError,
    ClaimScopeRejected,
)
from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    InvalidLeaseProof,
    LeaseProof,
    WorkerClaimRequest,
)
from devrelay.application.contracts.heartbeats import (
    HeartbeatAdmissionRejected,
    HeartbeatIdentity,
    HeartbeatLimits,
    HeartbeatRejected,
    HeartbeatRequest,
    HeartbeatResult,
    LeaseAuthorityObservation,
)
from devrelay.application.contracts.worker_recovery import AssignmentReference
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.heartbeats import HeartbeatAssignedJob
from devrelay.bootstrap.claims import job_heartbeater
from devrelay.config.control import ControlSettings
from devrelay.config.worker_claims import WorkerClaimsSettings

NOW = datetime(2026, 10, 12, tzinfo=UTC)
PROOF = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"


def authority() -> WorkerAuthorization:
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


def result() -> HeartbeatResult:
    return HeartbeatResult(
        heartbeat_request_id=UUID(int=4),
        authority=LeaseAuthorityObservation(
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
            observed_at=NOW + timedelta(seconds=30),
            updated_at=NOW + timedelta(seconds=30),
            current_expires_at=NOW + timedelta(seconds=150),
        ),
        renewed=True,
        stop_required=False,
    )


class Store:
    def __init__(self, response: HeartbeatResult, calls: list[str]) -> None:
        self.response = response
        self.calls = calls
        self.identity: HeartbeatIdentity | None = None
        self.authorization: WorkerAuthorization | None = None
        self.error: BaseException | None = None

    async def heartbeat(
        self, identity: HeartbeatIdentity, authorization: WorkerAuthorization
    ) -> HeartbeatResult:
        self.calls.append("heartbeat")
        self.identity, self.authorization = identity, authorization
        if self.error is not None:
            raise self.error
        return self.response


def test_proof_guard_precedes_scoped_identity_and_heartbeat_storage() -> None:
    pending, scope, response = request(), authority(), result()
    calls: list[str] = []
    store = Store(response, calls)
    guarded: list[LeaseProof] = []

    def guard(proof: LeaseProof) -> None:
        calls.append("proof")
        guarded.append(proof)

    service = HeartbeatAssignedJob(store, guard)

    observed = asyncio.run(service.execute(pending, scope))

    assert calls == ["proof", "heartbeat"]
    assert guarded == [pending.claim.proof]
    assert store.identity is not None
    assert store.identity.claim.worker_id == scope.worker_id
    assert store.identity.claim.claim_request_id == pending.claim.claim_request_id
    assert store.identity.heartbeat_request_id == pending.heartbeat_request_id
    assert store.authorization is scope
    assert observed is response
    assert PROOF not in repr(store.identity)


def test_rejected_reused_credential_never_reaches_storage() -> None:
    calls: list[str] = []
    store = Store(result(), calls)

    def guard(proof: LeaseProof) -> None:
        raise InvalidLeaseProof("Known credential cannot be a lease proof.")

    service, pending, scope = HeartbeatAssignedJob(store, guard), request(), authority()

    with pytest.raises(InvalidLeaseProof):
        asyncio.run(service.execute(pending, scope))

    assert calls == []
    assert store.identity is None


@pytest.mark.parametrize(
    "error",
    [
        HeartbeatRejected("Heartbeat rejected."),
        HeartbeatAdmissionRejected("Heartbeat capacity exhausted."),
        ClaimReplayConflict("Heartbeat input conflicts."),
        ClaimScopeRejected("Heartbeat scope denied."),
        ClaimPersistenceError("Heartbeat storage unavailable."),
        asyncio.CancelledError(),
    ],
)
def test_classified_failure_and_cancellation_propagate_without_returning_authority(
    error: BaseException,
) -> None:
    calls: list[str] = []
    store = Store(result(), calls)
    store.error = error
    service = HeartbeatAssignedJob(store, lambda proof: calls.append("proof"))
    pending, scope = request(), authority()

    with pytest.raises(type(error)) as caught:
        asyncio.run(service.execute(pending, scope))

    assert caught.value is error
    assert calls == ["proof", "heartbeat"]


@pytest.mark.parametrize("field", ["worker_id", "repository_id", "attempt_id"])
def test_uncorrelated_storage_authority_cannot_escape_use_case(field: str) -> None:
    response = result()
    changes: dict[str, Any] = {field: UUID(int=99)}
    reference = replace(response.authority.reference, **changes)
    response = replace(
        response, authority=replace(response.authority, reference=reference)
    )
    calls: list[str] = []
    service = HeartbeatAssignedJob(Store(response, calls), lambda proof: None)
    pending, scope = request(), authority()

    with pytest.raises(ValueError, match="does not match"):
        asyncio.run(service.execute(pending, scope))

    assert calls == ["heartbeat"]


def test_uncorrelated_request_id_cannot_escape_use_case() -> None:
    calls: list[str] = []
    response = replace(result(), heartbeat_request_id=UUID(int=99))
    service = HeartbeatAssignedJob(Store(response, calls), lambda proof: None)
    pending, scope = request(), authority()

    with pytest.raises(ValueError, match="does not match"):
        asyncio.run(service.execute(pending, scope))

    assert calls == ["heartbeat"]


def test_ahead_request_revision_cannot_accept_an_older_observation() -> None:
    calls: list[str] = []
    response = replace(result(), renewed=False)
    pending = replace(request(), expected_lease_revision=3)
    service = HeartbeatAssignedJob(Store(response, calls), lambda proof: None)
    scope = authority()

    with pytest.raises(ValueError, match="does not match"):
        asyncio.run(service.execute(pending, scope))

    assert calls == ["heartbeat"]


def test_exact_replay_can_return_newer_current_authority_without_renewing() -> None:
    calls: list[str] = []
    original = result()
    response = replace(
        original,
        renewed=False,
        authority=replace(
            original.authority,
            lease_revision=3,
            updated_at=NOW + timedelta(seconds=40),
            observed_at=NOW + timedelta(seconds=42),
            current_expires_at=NOW + timedelta(seconds=160),
        ),
    )
    service = HeartbeatAssignedJob(Store(response, calls), lambda proof: None)
    pending, scope = request(), authority()

    observed = asyncio.run(service.execute(pending, scope))

    assert observed is response
    assert not observed.renewed
    assert observed.authority.lease_revision == 3


def test_expired_current_authority_returns_only_stop_observation() -> None:
    calls: list[str] = []
    original = result()
    response = replace(
        original,
        renewed=False,
        stop_required=True,
        authority=replace(
            original.authority, observed_at=original.authority.current_expires_at
        ),
    )
    service = HeartbeatAssignedJob(Store(response, calls), lambda proof: None)
    pending, scope = request(), authority()

    observed = asyncio.run(service.execute(pending, scope))

    assert observed.stop_required
    assert not observed.renewed
    assert calls == ["heartbeat"]


def test_renewal_cannot_exceed_current_shortened_credential() -> None:
    calls: list[str] = []
    service = HeartbeatAssignedJob(Store(result(), calls), lambda proof: None)
    scope = replace(authority(), credential_expires_at=NOW + timedelta(seconds=140))
    pending = request()

    with pytest.raises(ValueError, match="does not match"):
        asyncio.run(service.execute(pending, scope))

    assert calls == ["heartbeat"]


def test_heartbeat_limits_have_bounded_finite_defaults() -> None:
    limits = HeartbeatLimits()

    assert limits.lease_seconds == 120
    assert limits.minimum_interval_seconds == 5
    assert limits.attempt_receipts == 4096
    assert limits.worker_receipts == 100_000
    assert limits.global_receipts == 1_000_000


@pytest.mark.parametrize(
    "field,minimum,maximum",
    [
        ("lease_seconds", 30, 300),
        ("minimum_interval_seconds", 1, 10),
        ("attempt_receipts", 1, 1_000_000),
        ("worker_receipts", 1, 1_000_000),
        ("global_receipts", 1, 10_000_000),
    ],
)
@pytest.mark.parametrize("boundary", ["minimum", "maximum"])
def test_heartbeat_limits_accept_exact_boundaries(
    field: str, minimum: int, maximum: int, boundary: str
) -> None:
    value = minimum if boundary == "minimum" else maximum
    changes: dict[str, Any] = {field: value}

    limits = HeartbeatLimits(**changes)

    assert getattr(limits, field) == value


@pytest.mark.parametrize(
    "field,minimum,maximum",
    [
        ("lease_seconds", 30, 300),
        ("minimum_interval_seconds", 1, 10),
        ("attempt_receipts", 1, 1_000_000),
        ("worker_receipts", 1, 1_000_000),
        ("global_receipts", 1, 10_000_000),
    ],
)
@pytest.mark.parametrize("malformed", ["below", "above", True, False, 1.0, "5", None])
def test_heartbeat_limits_reject_out_of_range_or_coerced_values(
    field: str, minimum: int, maximum: int, malformed: Any
) -> None:
    value = (
        minimum - 1
        if malformed == "below"
        else maximum + 1
        if malformed == "above"
        else malformed
    )
    changes: dict[str, Any] = {field: value}

    with pytest.raises(ValueError, match="bounded positive integers"):
        HeartbeatLimits(**changes)


def test_heartbeat_limits_are_immutable() -> None:
    limits = HeartbeatLimits()

    with pytest.raises(FrozenInstanceError):
        limits.lease_seconds = 300  # type: ignore[misc]  # Verify immutable policy.


def test_composition_requires_explicit_claim_gate(
    worker_configuration: ControlSettings,
) -> None:
    engine = cast(AsyncEngine, object())

    with pytest.raises(ValueError, match="explicitly enabled claims"):
        job_heartbeater(worker_configuration, engine)


def test_composition_guards_enrolled_credentials_before_storage(
    worker_configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import devrelay.bootstrap.claims as composition

    calls: list[str] = []
    writer = Store(result(), calls)
    settings = worker_configuration.model_copy(
        update={
            "worker_claims": WorkerClaimsSettings(),
            "workers": tuple(
                worker.model_copy(update={"auth_token": SecretStr(PROOF)})
                for worker in worker_configuration.workers
            ),
        }
    )
    monkeypatch.setattr(composition, "heartbeat_store", lambda *args: writer)
    service = job_heartbeater(settings, cast(AsyncEngine, object()))
    pending, scope = request(), authority()

    with pytest.raises(InvalidLeaseProof, match="separate from credentials"):
        asyncio.run(service.execute(pending, scope))

    assert calls == []
