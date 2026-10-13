import asyncio
from collections.abc import Callable
from time import monotonic
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
from devrelay.application.contracts.heartbeats import (
    HeartbeatRequest,
    WorkerHeartbeatExchange,
)
from devrelay.application.contracts.worker_recovery import (
    AcknowledgementCompletion,
    ClaimExchange,
    WorkerClaimOutcome,
    WorkerHeartbeatIntent,
    WorkerRecoveryPhase,
    WorkerRecoveryState,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.control_gateway import (
    ControlClaimRejected,
    ControlGateway,
    ControlHeartbeatGateway,
    InvalidControlResponse,
)
from devrelay.application.ports.worker_journal import (
    WorkerClaimJournal,
    WorkerHeartbeatJournal,
    WorkerStateBusy,
    WorkerStateUnavailable,
)
from devrelay.application.worker_heartbeats import HeartbeatPolicy, HeartbeatSupervisor


class PrepareWorkerClaim:
    """Prepare recoverable local input; no HTTP request or assignment is made."""

    def __init__(
        self,
        journal: WorkerClaimJournal,
        *,
        heartbeat_journal: WorkerHeartbeatJournal | None = None,
    ) -> None:
        self._journal = journal
        self._heartbeat_journal = heartbeat_journal

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

    async def prepare_heartbeat(
        self, request: WorkerClaimRequest, intent: WorkerHeartbeatIntent
    ) -> WorkerHeartbeatIntent:
        if self._heartbeat_journal is None:
            raise WorkerStateUnavailable("Heartbeat recovery is not configured.")
        return await self._heartbeat_journal.get_or_create_heartbeat(request, intent)

    async def record_heartbeat(
        self, request: WorkerClaimRequest, intent: WorkerHeartbeatIntent
    ) -> WorkerRecoveryState:
        if self._heartbeat_journal is None:
            raise WorkerStateUnavailable("Heartbeat recovery is not configured.")
        return await self._heartbeat_journal.record_heartbeat(request, intent)

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
        *,
        heartbeat_gateway: ControlHeartbeatGateway | None = None,
        monotonic_clock: Callable[[], float] | None = None,
    ) -> None:
        self._preparation = preparation
        self._gateway = gateway
        self._authorization = authorization
        self._lock = asyncio.Lock()
        self._heartbeat_gateway = heartbeat_gateway
        self._monotonic = monotonic if monotonic_clock is None else monotonic_clock
        self._supervision_clock = monotonic_clock
        self._heartbeat_observation: WorkerHeartbeatExchange | None = None
        self._supervisor: HeartbeatSupervisor | None = None

    def heartbeat_supervisor(
        self, *, policy: HeartbeatPolicy | None = None
    ) -> HeartbeatSupervisor:
        """One explicit maintenance lineage; injected clock must include suspend."""
        if policy is not None and not isinstance(policy, HeartbeatPolicy):
            raise ValueError("Heartbeat supervision requires a typed policy.")
        if self._supervision_clock is None or self._heartbeat_gateway is None:
            raise WorkerStateUnavailable(
                "Heartbeat supervision requires transport and an explicit clock."
            )
        if self._supervisor is not None:
            if policy is not None and policy != self._supervisor.policy:
                raise WorkerStateUnavailable(
                    "Heartbeat supervision policy cannot change within a lineage."
                )
            return self._supervisor
        first = True

        async def exchange() -> WorkerHeartbeatExchange:
            nonlocal first
            if first:
                first = False
                state = await self._preparation.recover()
                return await self.heartbeat(
                    renew=state.pending_heartbeat is None
                    and state.last_heartbeat is None
                )
            return await self.heartbeat()

        self._supervisor = HeartbeatSupervisor(
            exchange,
            self._authorization,
            self._supervision_clock,
            HeartbeatPolicy() if policy is None else policy,
        )
        return self._supervisor

    async def heartbeat(self, *, renew: bool = True) -> WorkerHeartbeatExchange:
        """One durable exchange; restarts replay input to discover fresh authority."""
        if type(renew) is not bool:
            raise ValueError("Heartbeat renewal intent must be a boolean.")
        if self._supervisor is not None and self._supervisor.stop is not None:
            renew = False
        if self._lock.locked():
            raise WorkerStateBusy("A worker claim exchange is already active.")
        if self._heartbeat_gateway is None:
            raise WorkerStateUnavailable("Heartbeat transport is not configured.")
        async with self._lock:
            state = await self._preparation.recover()
            if self._supervisor is not None and self._supervisor.stop is not None:
                renew = False
            reference = state.assignment.assignment if state.assignment else None
            if (
                state.phase is not WorkerRecoveryPhase.ACKNOWLEDGED
                or state.request is None
                or reference is None
                or state.completion is None
            ):
                raise AssignmentUnavailable(
                    "Heartbeat requires durable acknowledgement."
                )
            intent = state.pending_heartbeat
            if intent is None:
                cached = self._heartbeat_observation
                if state.last_heartbeat is not None and (
                    not renew or cached is None or cached.intent != state.last_heartbeat
                ):
                    intent = state.last_heartbeat
                else:
                    if not renew:
                        raise AssignmentUnavailable(
                            "Heartbeat observation requires a recorded input."
                        )
                    observation = cached.result if cached else None
                    if observation is not None and observation.stop_required:
                        raise ControlClaimRejected(
                            "Stopped heartbeat authority cannot renew."
                        )
                    intent = await self._preparation.prepare_heartbeat(
                        state.request,
                        WorkerHeartbeatIntent(
                            heartbeat_request_id=uuid4(),
                            claim_request_id=state.request.claim_request_id,
                            attempt_id=reference.attempt_id,
                            generation=reference.generation,
                            expected_lease_revision=observation.authority.lease_revision
                            if observation
                            else 1,
                        ),
                    )
            request = HeartbeatRequest(
                claim=state.request,
                heartbeat_request_id=intent.heartbeat_request_id,
                attempt_id=intent.attempt_id,
                generation=intent.generation,
                expected_lease_revision=intent.expected_lease_revision,
                protocol_version=intent.protocol_version,
            )
            started_at = self._monotonic()
            if (
                self._supervisor is not None
                and self._supervisor.stop is not None
                and intent != state.last_heartbeat
            ):
                raise ControlClaimRejected(
                    "Uncertain heartbeat input requires restarted recovery."
                )
            result = await self._heartbeat_gateway.heartbeat(
                request, self._authorization, reference=reference
            )
            try:
                result.require_correlation(request, self._authorization)
                if result.authority.reference != reference:
                    raise ValueError("Heartbeat changed original assignment anchors.")
            except ValueError:
                raise InvalidControlResponse(
                    "Control heartbeat failed correlation."
                ) from None
            await self._preparation.record_heartbeat(state.request, intent)
            exchange = WorkerHeartbeatExchange(
                intent=intent,
                result=result,
                request_started_at=started_at,
                received_at=self._monotonic(),
            )
            self._heartbeat_observation = exchange
            return exchange

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
