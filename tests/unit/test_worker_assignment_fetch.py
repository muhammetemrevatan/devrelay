import asyncio
from dataclasses import replace
from datetime import timedelta
from uuid import UUID, uuid4

import pytest

from devrelay.application.contracts.assignment_delivery import (
    AssignmentDelivery,
    AssignmentUnavailable,
)
from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    LeaseProof,
    WorkerClaimRequest,
)
from devrelay.application.contracts.worker_recovery import (
    WorkerRecoveryPhase,
    WorkerRecoveryState,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.control_gateway import ControlUnavailable
from devrelay.application.worker_claims import PrepareWorkerClaim, RequestWorkerClaim
from tests.assignment_fixtures import NOW, assigned_result
from tests.unit.test_worker_claim_exchange import Gateway, Journal


class Local(Journal):
    def __init__(self, state: WorkerRecoveryState, calls: list[str]) -> None:
        super().__init__(calls)
        self.state = state

    async def load_state(self) -> WorkerRecoveryState:
        return self.state


class Remote(Gateway):
    def __init__(
        self,
        authorization: WorkerAuthorization,
        delivery: AssignmentDelivery,
        calls: list[str],
    ) -> None:
        super().__init__(authorization, calls)
        self.delivery = delivery
        self.received: tuple[UUID, WorkerClaimRequest, WorkerAuthorization] | None = (
            None
        )

    async def read_assignment(
        self,
        attempt_id: UUID,
        request: WorkerClaimRequest,
        authorization: WorkerAuthorization,
    ) -> AssignmentDelivery:
        self.calls.append("read")
        self.received = attempt_id, request, authorization
        if self.error is not None:
            raise self.error
        return self.delivery


def prepare() -> tuple[
    RequestWorkerClaim, Local, Remote, AssignmentDelivery, list[str]
]:
    authorization = WorkerAuthorization(
        worker_id=uuid4(),
        allowed_repository_ids=frozenset({uuid4()}),
        credential_expires_at=NOW + timedelta(hours=1),
    )
    request = WorkerClaimRequest(claim_request_id=uuid4(), proof=LeaseProof.generate())
    delivery = AssignmentDelivery.from_result(
        assigned_result(authorization),
        worker_id=authorization.worker_id,
        claim_request_id=request.claim_request_id,
    )
    calls: list[str] = []
    local = Local(
        WorkerRecoveryState(
            phase=WorkerRecoveryPhase.ASSIGNED,
            request=request,
            assignment=delivery.outcome,
        ),
        calls,
    )
    remote = Remote(authorization, delivery, calls)
    return (
        RequestWorkerClaim(PrepareWorkerClaim(local), remote, authorization),
        local,
        remote,
        delivery,
        calls,
    )


def test_fetch_uses_durable_assignment_without_generating_or_completing_a_claim() -> (
    None
):
    worker, local, remote, delivery, calls = prepare()
    original = local.state

    received = asyncio.run(worker.fetch_assignment())

    assert received == delivery and calls == ["read"]
    assert local.state is original
    assert remote.received is not None and remote.received[1] is original.request


@pytest.mark.parametrize(
    "phase", [WorkerRecoveryPhase.IDLE, WorkerRecoveryPhase.PENDING]
)
def test_unassigned_worker_cannot_fetch_or_generate_a_new_request(
    phase: WorkerRecoveryPhase,
) -> None:
    worker, local, remote, delivery, calls = prepare()
    request = local.state.request if phase is WorkerRecoveryPhase.PENDING else None
    local.state = WorkerRecoveryState(phase=phase, request=request)

    with pytest.raises(AssignmentUnavailable):
        asyncio.run(worker.fetch_assignment())

    assert calls == [] and local.state.request is request


def test_delivery_cannot_silently_renew_the_original_lease() -> None:
    worker, local, remote, delivery, calls = prepare()
    reference = delivery.outcome.assignment
    assert reference is not None
    remote.delivery = replace(
        delivery,
        outcome=replace(
            delivery.outcome,
            assignment=replace(
                reference, expires_at=reference.expires_at + timedelta(seconds=1)
            ),
        ),
    )
    original = local.state

    with pytest.raises(ClaimReplayConflict):
        asyncio.run(worker.fetch_assignment())

    assert local.state is original and calls == ["read"]


@pytest.mark.parametrize(
    "error", [ControlUnavailable("Failure."), asyncio.CancelledError()]
)
def test_failed_fetch_preserves_assigned_recovery(
    error: Exception | asyncio.CancelledError,
) -> None:
    worker, local, remote, delivery, calls = prepare()
    remote.error = error
    original = local.state

    with pytest.raises(type(error)):
        asyncio.run(worker.fetch_assignment())

    assert calls == ["read"] and local.state is original
