import asyncio
from uuid import uuid4

from devrelay.application.contracts.acknowledgements import (
    AcknowledgementResult,
    WorkerAcknowledgementIntent,
    WorkerAcknowledgementRequest,
)
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
    AcknowledgementCompletion,
    ClaimExchange,
    WorkerClaimOutcome,
    WorkerRecoveryPhase,
    WorkerRecoveryState,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.control_gateway import (
    ControlGateway,
    InvalidControlResponse,
)
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

    async def record_acknowledgement(
        self, request: WorkerClaimRequest, completion: AcknowledgementCompletion
    ) -> WorkerRecoveryState:
        return await self._journal.record_acknowledgement(request, completion)

    async def record_outcome(
        self, request: WorkerClaimRequest, outcome: WorkerClaimOutcome
    ) -> WorkerRecoveryState:
        """Local commit only; transport authentication and execution are external."""
        return await self._journal.record_outcome(request, outcome)

    async def prepare_acknowledgement(
        self, delivery: AssignmentDelivery
    ) -> WorkerAcknowledgementIntent:
        state = await self._journal.load_state()
        if (
            state.request is None
            or state.assignment != delivery.outcome
            or state.assignment.assignment is None
        ):
            raise ClaimReplayConflict(
                "Acknowledgement requires the original assignment."
            )
        if state.acknowledgement is None and not delivery.lease_authority_active:
            raise AssignmentUnavailable("Active assignment is required for new intent.")
        definition_digest = await asyncio.to_thread(delivery.definition.fingerprint)
        reference = state.assignment.assignment
        candidate = WorkerAcknowledgementIntent(
            acknowledgement_request_id=uuid4(),
            claim_request_id=state.request.claim_request_id,
            attempt_id=reference.attempt_id,
            generation=reference.generation,
            snapshot_id=delivery.definition.snapshot.id,
            base_revision=delivery.definition.base_revision,
            definition_digest=definition_digest,
        )
        return await self._journal.get_or_create_acknowledgement(
            state.request, candidate
        )


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
            return await self._fetch_assignment(state)

    async def _fetch_assignment(self, state: WorkerRecoveryState) -> AssignmentDelivery:
        if (
            state.phase
            not in (
                WorkerRecoveryPhase.ASSIGNED,
                WorkerRecoveryPhase.ACKNOWLEDGEMENT_PENDING,
                WorkerRecoveryPhase.ACKNOWLEDGED,
            )
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
        if state.acknowledgement is not None:
            digest = await asyncio.to_thread(delivery.definition.fingerprint)
            if (
                digest != state.acknowledgement.definition_digest
                or delivery.definition.snapshot.id != state.acknowledgement.snapshot_id
                or delivery.definition.base_revision
                != state.acknowledgement.base_revision
            ):
                raise ClaimReplayConflict(
                    "Assignment changed pinned acknowledgement input."
                )
        return delivery

    async def prepare_acknowledgement(self) -> WorkerAcknowledgementIntent:
        """Journal input only; no acknowledgement HTTP request or process launch."""
        if self._lock.locked():
            raise WorkerStateBusy("A worker claim exchange is already active.")
        async with self._lock:
            state = await self._preparation.recover()
            if state.acknowledgement is not None:
                return state.acknowledgement
            delivery = await self._fetch_assignment(state)
            return await self._preparation.prepare_acknowledgement(delivery)

    async def acknowledge(self) -> AcknowledgementResult:
        """Explicit exchange; commit historical evidence before returning."""
        if self._lock.locked():
            raise WorkerStateBusy("A worker claim exchange is already active.")
        async with self._lock:
            state = await self._preparation.recover()
            if state.acknowledgement is None:
                delivery = await self._fetch_assignment(state)
                await self._preparation.prepare_acknowledgement(delivery)
                state = await self._preparation.recover()
            if (
                state.request is None
                or state.assignment is None
                or state.acknowledgement is None
            ):
                raise AssignmentUnavailable(
                    "Durable acknowledgement input is required."
                )
            result = await self._gateway.acknowledge(
                WorkerAcknowledgementRequest(
                    intent=state.acknowledgement, claim=state.request
                ),
                self._authorization,
            )
            try:
                completion = AcknowledgementCompletion.from_result(
                    result, state.assignment, state.acknowledgement
                )
            except ValueError:
                raise InvalidControlResponse(
                    "Control acknowledgement changed original anchors."
                ) from None
            await self._preparation.record_acknowledgement(state.request, completion)
            return result
