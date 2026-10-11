import asyncio
from uuid import uuid4

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
    ClaimExchange,
    WorkerClaimOutcome,
    WorkerRecoveryPhase,
    WorkerRecoveryState,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.control_gateway import ControlGateway
from devrelay.application.ports.worker_journal import (
    WorkerClaimJournal,
    WorkerStateBusy,
)


class PrepareWorkerClaim:
    """Prepare recoverable local input; no HTTP request or assignment is made."""

    def __init__(self, journal: WorkerClaimJournal) -> None:
        self._journal = journal

    async def prepare(self) -> WorkerClaimRequest:
        pending = await self._journal.load_pending()
        if pending is not None:
            return pending
        candidate = WorkerClaimRequest(
            claim_request_id=uuid4(), proof=LeaseProof.generate()
        )
        return await self._journal.get_or_create_pending(candidate)

    async def recover(self) -> WorkerRecoveryState:
        return await self._journal.load_state()

    async def record_outcome(
        self, request: WorkerClaimRequest, outcome: WorkerClaimOutcome
    ) -> WorkerRecoveryState:
        """Local commit only; transport authentication and execution are external."""
        return await self._journal.record_outcome(request, outcome)


class RequestWorkerClaim:
    """One explicit exchange, serialized through local completion; no polling/launch."""

    def __init__(
        self,
        preparation: PrepareWorkerClaim,
        gateway: ControlGateway,
        authorization: WorkerAuthorization,
    ) -> None:
        self._preparation = preparation
        self._gateway = gateway
        self._authorization = authorization
        self._lock = asyncio.Lock()

    async def execute(self) -> ClaimExchange:
        if self._lock.locked():
            raise WorkerStateBusy("A worker claim exchange is already active.")
        async with self._lock:
            request = await self._preparation.prepare()
            result = await self._gateway.claim(request, self._authorization)
            await self._preparation.record_outcome(request, result.outcome)
            return result

    async def recover(self) -> WorkerRecoveryState:
        return await self._preparation.recover()

    async def fetch_assignment(self) -> AssignmentDelivery:
        if self._lock.locked():
            raise WorkerStateBusy("A worker claim exchange is already active.")
        async with self._lock:
            state = await self._preparation.recover()
            if (
                state.phase is not WorkerRecoveryPhase.ASSIGNED
                or state.request is None
                or state.assignment is None
                or state.assignment.assignment is None
            ):
                raise AssignmentUnavailable("A durable assigned claim is required.")
            delivery = await self._gateway.read_assignment(
                state.assignment.assignment.attempt_id,
                state.request,
                self._authorization,
            )
            if delivery.outcome != state.assignment:
                raise ClaimReplayConflict(
                    "Assignment delivery changed original recovery anchors."
                )
            return delivery
