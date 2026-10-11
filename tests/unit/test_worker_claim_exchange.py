import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from devrelay.application.contracts.acknowledgements import (
    AcknowledgementResult,
    WorkerAcknowledgementIntent,
    WorkerAcknowledgementRequest,
)
from devrelay.application.contracts.assignment_delivery import AssignmentDelivery
from devrelay.application.contracts.assignments import NoWorkReason
from devrelay.application.contracts.claims import WorkerClaimRequest
from devrelay.application.contracts.worker_recovery import (
    AcknowledgementCompletion,
    ClaimExchange,
    WorkerClaimOutcome,
    WorkerRecoveryPhase,
    WorkerRecoveryState,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.control_gateway import (
    ControlAccessDenied,
    ControlClaimRejected,
    ControlUnavailable,
    InvalidControlResponse,
)
from devrelay.application.ports.worker_journal import (
    WorkerStateBusy,
    WorkerStateUnavailable,
)
from devrelay.application.worker_claims import PrepareWorkerClaim, RequestWorkerClaim

NOW = datetime(2026, 10, 11, tzinfo=UTC)


class Journal:
    async def record_acknowledgement(
        self, request: WorkerClaimRequest, completion: AcknowledgementCompletion
    ) -> WorkerRecoveryState:
        raise WorkerStateUnavailable("This fake has no assigned journal.")

    def __init__(self, calls: list[str]) -> None:
        self.calls = calls
        self.pending: WorkerClaimRequest | None = None
        self.completed: WorkerClaimOutcome | None = None
        self.error: Exception | None = None

    async def load_state(self) -> WorkerRecoveryState:
        return WorkerRecoveryState(
            phase=WorkerRecoveryPhase.PENDING
            if self.pending
            else WorkerRecoveryPhase.IDLE,
            request=self.pending,
            last_no_work=self.completed,
        )

    async def get_or_create_acknowledgement(
        self, request: WorkerClaimRequest, candidate: WorkerAcknowledgementIntent
    ) -> WorkerAcknowledgementIntent:
        raise WorkerStateUnavailable("This fake has no assigned journal.")

    async def load_pending(self) -> WorkerClaimRequest | None:
        return self.pending

    async def get_or_create_pending(
        self, candidate: WorkerClaimRequest
    ) -> WorkerClaimRequest:
        self.calls.append("journal")
        self.pending = self.pending or candidate
        return self.pending

    async def record_outcome(
        self, request: WorkerClaimRequest, outcome: WorkerClaimOutcome
    ) -> WorkerRecoveryState:
        self.calls.append("complete")
        if self.error is not None:
            raise self.error
        self.pending = None
        self.completed = outcome
        return await self.load_state()


class Gateway:
    async def acknowledge(
        self, request: WorkerAcknowledgementRequest, authorization: WorkerAuthorization
    ) -> AcknowledgementResult:
        raise AssertionError("This fixture only tests claim exchange.")

    def __init__(self, authorization: WorkerAuthorization, calls: list[str]) -> None:
        self.authorization = authorization
        self.calls = calls
        self.inputs: list[WorkerClaimRequest] = []
        self.error: Exception | asyncio.CancelledError | None = None
        self.started = asyncio.Event()
        self.release: asyncio.Event | None = None

    async def identify(self) -> WorkerAuthorization:
        return self.authorization

    async def read_assignment(
        self,
        attempt_id: UUID,
        request: WorkerClaimRequest,
        authorization: WorkerAuthorization,
    ) -> AssignmentDelivery:
        raise AssertionError("This fixture only tests claim exchange.")

    async def claim(
        self, request: WorkerClaimRequest, authorization: WorkerAuthorization
    ) -> ClaimExchange:
        self.calls.append("http")
        self.inputs.append(request)
        self.started.set()
        if self.release is not None:
            await self.release.wait()
        if self.error is not None:
            raise self.error
        return ClaimExchange(
            outcome=WorkerClaimOutcome(
                worker_id=authorization.worker_id,
                claim_request_id=request.claim_request_id,
                recorded_at=NOW,
                no_work=NoWorkReason.EMPTY,
            ),
            recovered=False,
            lease_authority_active=False,
        )


def setup() -> tuple[RequestWorkerClaim, Journal, Gateway, list[str]]:
    calls: list[str] = []
    authorization = WorkerAuthorization(
        worker_id=uuid4(),
        allowed_repository_ids=frozenset({uuid4()}),
        credential_expires_at=NOW + timedelta(hours=1),
    )
    journal = Journal(calls)
    gateway = Gateway(authorization, calls)
    return (
        RequestWorkerClaim(PrepareWorkerClaim(journal), gateway, authorization),
        journal,
        gateway,
        calls,
    )


def test_local_input_precedes_http_and_completion_precedes_return() -> None:
    worker, journal, gateway, calls = setup()

    result = asyncio.run(worker.execute())

    assert calls == ["journal", "http", "complete"]
    assert journal.completed == result.outcome and journal.pending is None
    assert len(gateway.inputs) == 1


@pytest.mark.parametrize(
    "error_type",
    [
        ControlAccessDenied,
        ControlUnavailable,
        InvalidControlResponse,
        ControlClaimRejected,
        asyncio.CancelledError,
    ],
)
def test_transport_failure_keeps_original_input_for_explicit_retry(
    error_type: Callable[[str], Exception | asyncio.CancelledError],
) -> None:
    worker, journal, gateway, calls = setup()
    failure = error_type("Failure.")
    gateway.error = failure

    async def scenario() -> None:
        with pytest.raises(type(failure)):
            await worker.execute()
        gateway.error = None
        await worker.execute()

    asyncio.run(scenario())

    assert calls == ["journal", "http", "http", "complete"]
    assert gateway.inputs[0] is gateway.inputs[1]
    assert journal.completed is not None


def test_failed_completion_never_reports_success_or_generates_replacement() -> None:
    worker, journal, gateway, calls = setup()
    journal.error = WorkerStateUnavailable("Commit failed.")

    with pytest.raises(WorkerStateUnavailable):
        asyncio.run(worker.execute())

    assert calls == ["journal", "http", "complete"]
    assert journal.pending is gateway.inputs[0] and journal.completed is None


def test_concurrent_exchange_is_rejected_without_queued_http_or_new_proof() -> None:
    worker, journal, gateway, calls = setup()
    gateway.release = asyncio.Event()

    async def scenario() -> None:
        active = asyncio.create_task(worker.execute())
        await gateway.started.wait()
        with pytest.raises(WorkerStateBusy):
            await worker.execute()
        assert gateway.release is not None
        gateway.release.set()
        await active

    asyncio.run(scenario())

    assert calls == ["journal", "http", "complete"]
    assert len(gateway.inputs) == 1 and journal.completed is not None
