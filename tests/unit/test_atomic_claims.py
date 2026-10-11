import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from devrelay.application.claims import ClaimJob
from devrelay.application.contracts.assignments import (
    ClaimAdmissionRejected,
    ClaimLimits,
    ClaimResult,
    NoWorkReason,
)
from devrelay.application.contracts.claims import (
    ClaimRequestIdentity,
    InvalidLeaseProof,
    LeaseProof,
    WorkerClaimRequest,
)
from devrelay.application.contracts.eligibility import PreparedJobClaim
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.task_source import TaskSourceUnavailable

NOW = datetime(2026, 10, 11, tzinfo=UTC)


class Store:
    def __init__(self, calls: list[str], recovered: bool = False) -> None:
        self.calls = calls
        self.recovered = recovered
        self.denied = False
        self.identities: list[ClaimRequestIdentity] = []

    async def recover(
        self, identity: ClaimRequestIdentity, authorization: WorkerAuthorization
    ) -> ClaimResult | None:
        self.calls.append("recover")
        self.identities.append(identity)
        return (
            ClaimResult(recorded_at=NOW, recovered=True, no_work=NoWorkReason.EMPTY)
            if self.recovered
            else None
        )

    async def check_admission(self, authorization: WorkerAuthorization) -> None:
        self.calls.append("admission")
        if self.denied:
            raise ClaimAdmissionRejected("New claim exceeds polling or storage limits.")

    async def claim(
        self,
        identity: ClaimRequestIdentity,
        authorization: WorkerAuthorization,
        observation: PreparedJobClaim | None,
    ) -> ClaimResult:
        self.calls.append("claim")
        self.identities.append(identity)
        return ClaimResult(recorded_at=NOW, recovered=False, no_work=NoWorkReason.EMPTY)


class Preparer:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls
        self.error: Exception | asyncio.CancelledError | None = None

    async def execute(
        self, authorization: WorkerAuthorization
    ) -> PreparedJobClaim | None:
        self.calls.append("prepare")
        if self.error is not None:
            raise self.error
        return None


def authority() -> WorkerAuthorization:
    return WorkerAuthorization(
        worker_id=uuid4(),
        allowed_repository_ids=frozenset({uuid4()}),
        credential_expires_at=NOW + timedelta(hours=1),
    )


@pytest.mark.parametrize("recovered", [True, False])
def test_recovery_precedes_new_admission_and_provider_io(recovered: bool) -> None:
    calls: list[str] = []
    store, preparer = Store(calls, recovered), Preparer(calls)
    request = WorkerClaimRequest(claim_request_id=uuid4(), proof=LeaseProof.generate())
    authorization = authority()
    service = ClaimJob(store, preparer, lambda proof: calls.append("proof"))

    result = asyncio.run(service.execute(request, authorization))

    assert calls == (
        ["proof", "recover"]
        if recovered
        else ["proof", "recover", "admission", "prepare", "claim"]
    )
    assert result.recovered is recovered
    assert store.identities[0].worker_id == authorization.worker_id
    assert request.proof.reveal() not in repr(result)


def test_durable_admission_denial_prevents_provider_io() -> None:
    calls: list[str] = []
    store = Store(calls)
    store.denied = True
    service = ClaimJob(store, Preparer(calls), lambda proof: None)
    request = WorkerClaimRequest(claim_request_id=uuid4(), proof=LeaseProof.generate())
    authorization = authority()

    with pytest.raises(ClaimAdmissionRejected):
        asyncio.run(service.execute(request, authorization))

    assert calls == ["recover", "admission"]


@pytest.mark.parametrize(
    "error", [TaskSourceUnavailable("Unavailable."), asyncio.CancelledError()]
)
def test_provider_failure_or_cancellation_never_records_no_work(
    error: Exception | asyncio.CancelledError,
) -> None:
    calls: list[str] = []
    preparer = Preparer(calls)
    preparer.error = error
    service = ClaimJob(Store(calls), preparer, lambda proof: None)
    request = WorkerClaimRequest(claim_request_id=uuid4(), proof=LeaseProof.generate())
    authorization = authority()

    with pytest.raises(type(error)):
        asyncio.run(service.execute(request, authorization))

    assert calls == ["recover", "admission", "prepare"]


def test_known_credential_reuse_is_denied_before_storage() -> None:
    calls: list[str] = []

    def reject(proof: LeaseProof) -> None:
        raise InvalidLeaseProof("Lease proof must be separate from credentials.")

    service = ClaimJob(Store(calls), Preparer(calls), reject)
    request = WorkerClaimRequest(claim_request_id=uuid4(), proof=LeaseProof.generate())
    authorization = authority()

    with pytest.raises(InvalidLeaseProof):
        asyncio.run(service.execute(request, authorization))

    assert not calls


@pytest.mark.parametrize(
    "field,value",
    [
        ("lease_seconds", 29),
        ("lease_seconds", 301),
        ("worker_receipts", 0),
        ("global_receipts", 10_000_001),
        ("historical_workers", 257),
        ("worker_receipts", True),
    ],
)
def test_claim_limits_reject_unbounded_or_untyped_values(
    field: str, value: int
) -> None:
    values = {field: value}

    with pytest.raises(ValueError):
        ClaimLimits(**values)
