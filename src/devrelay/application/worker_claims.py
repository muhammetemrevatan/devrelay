import asyncio
from collections.abc import Awaitable, Callable
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
from devrelay.application.contracts.base_sources import (
    BaseMaterialization,
    BaseMaterializationPolicy,
    BaseSourceRequest,
    BaseSourceSnapshot,
)
from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    LeaseProof,
    WorkerClaimRequest,
)
from devrelay.application.contracts.executions import (
    ExecutionPolicy,
    WorkerExecutionIntent,
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
from devrelay.application.contracts.workspace_recovery import WorkspaceRecovery
from devrelay.application.contracts.workspaces import WorkspaceReservation
from devrelay.application.materialize_base import MaterializeAssignedBase
from devrelay.application.ports.base_materialization import BaseMaterializationPort
from devrelay.application.ports.base_source import BaseSourcePort
from devrelay.application.ports.control_gateway import (
    ControlClaimRejected,
    ControlGateway,
    ControlHeartbeatGateway,
    InvalidControlResponse,
)
from devrelay.application.ports.worker_execution_journal import WorkerExecutionJournal
from devrelay.application.ports.worker_journal import (
    WorkerClaimJournal,
    WorkerHeartbeatJournal,
    WorkerStateBusy,
    WorkerStateUnavailable,
)
from devrelay.application.ports.worker_workspace_journal import WorkerWorkspaceJournal
from devrelay.application.ports.workspace_reservation import (
    WorkspaceReservationPort,
    WorkspaceUnavailable,
)
from devrelay.application.worker_executions import PrepareAssignedExecution
from devrelay.application.worker_heartbeats import HeartbeatPolicy, HeartbeatSupervisor
from devrelay.application.worker_workspaces import (
    RecoverAssignedWorkspace,
    ReserveAssignedWorkspace,
    _validate_original_input,
)

_DEFAULT_MATERIALIZATION_POLICY = BaseMaterializationPolicy()

type BaseSourceBinding = Callable[
    [WorkerClaimRequest, WorkerAuthorization, AssignmentDelivery],
    Awaitable[BaseSourcePort],
]


class PrepareWorkerClaim:
    """Prepare recoverable local input; no HTTP request or assignment is made."""

    def __init__(
        self,
        journal: WorkerClaimJournal,
        *,
        heartbeat_journal: WorkerHeartbeatJournal | None = None,
        execution_journal: WorkerExecutionJournal | None = None,
        workspace_journal: WorkerWorkspaceJournal | None = None,
    ) -> None:
        self._journal = journal
        self._heartbeat_journal = heartbeat_journal
        self._execution_journal = execution_journal
        self._workspace_journal = workspace_journal
        self._base_materialization_started = False

    async def reserve_workspace(
        self,
        delivery: AssignmentDelivery,
        supervisor: HeartbeatSupervisor,
        workspace: WorkspaceReservationPort,
    ) -> WorkspaceReservation:
        if self._workspace_journal is None:
            raise WorkerStateUnavailable("Workspace recovery is not configured.")
        return await ReserveAssignedWorkspace(
            self._workspace_journal, supervisor, workspace
        ).execute(delivery)

    async def recover_workspace(
        self, delivery: AssignmentDelivery
    ) -> WorkspaceRecovery | None:
        if self._workspace_journal is None:
            raise WorkerStateUnavailable("Workspace recovery is not configured.")
        return await RecoverAssignedWorkspace(self._workspace_journal).execute(delivery)

    async def materialize_base(
        self,
        delivery: AssignmentDelivery,
        supervisor: HeartbeatSupervisor,
        workspace: BaseMaterializationPort,
        reservation: WorkspaceReservation,
        source: BaseSourcePort,
        *,
        policy: BaseMaterializationPolicy = _DEFAULT_MATERIALIZATION_POLICY,
    ) -> BaseMaterialization:
        if self._workspace_journal is None:
            raise WorkerStateUnavailable("Workspace recovery is not configured.")
        if self._base_materialization_started:
            raise WorkspaceUnavailable(
                "Base materialization has already been requested."
            )
        self._base_materialization_started = True
        return await MaterializeAssignedBase(
            self._workspace_journal, source, workspace, supervisor, policy
        ).execute(delivery, reservation)

    async def prepare_execution(
        self,
        delivery: AssignmentDelivery,
        supervisor: HeartbeatSupervisor,
        policy: ExecutionPolicy,
    ) -> WorkerExecutionIntent:
        if self._execution_journal is None:
            raise WorkerStateUnavailable("Execution recovery is not configured.")
        return await PrepareAssignedExecution(
            self._execution_journal, supervisor, policy
        ).execute(delivery)

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


class _DeferredControlSource:
    """Bind only after the live consumer validated protected execution history."""

    def __init__(
        self,
        preparation: PrepareWorkerClaim,
        bind: BaseSourceBinding,
        authorization: WorkerAuthorization,
        delivery: AssignmentDelivery,
    ) -> None:
        self._preparation, self._bind = preparation, bind
        self._authorization, self._delivery = authorization, delivery

    async def read(self, request: BaseSourceRequest) -> BaseSourceSnapshot:
        state = await self._preparation.recover()
        await asyncio.to_thread(_validate_original_input, state, self._delivery)
        if state.request is None:
            raise WorkerStateUnavailable("Protected source input is unavailable.")
        source = await self._bind(state.request, self._authorization, self._delivery)
        return await source.read(request)


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
        base_source_binding: BaseSourceBinding | None = None,
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
        self._base_source_binding = base_source_binding

    async def reserve_workspace(
        self, delivery: AssignmentDelivery, workspace: WorkspaceReservationPort
    ) -> WorkspaceReservation:
        if self._lock.locked():
            raise WorkerStateBusy("A worker claim exchange is already active.")
        async with self._lock:
            return await self._preparation.reserve_workspace(
                delivery, self.heartbeat_supervisor(), workspace
            )

    async def recover_workspace(
        self, delivery: AssignmentDelivery
    ) -> WorkspaceRecovery | None:
        if self._lock.locked():
            raise WorkerStateBusy("A worker claim exchange is already active.")
        async with self._lock:
            return await self._preparation.recover_workspace(delivery)

    async def materialize_base(
        self,
        delivery: AssignmentDelivery,
        workspace: BaseMaterializationPort,
        reservation: WorkspaceReservation,
        source: BaseSourcePort | None = None,
        *,
        policy: BaseMaterializationPolicy = _DEFAULT_MATERIALIZATION_POLICY,
    ) -> BaseMaterialization:
        # Preparation and the live scope each bound materialization admission.
        # Keep heartbeat exchange free
        # to renew authority while the separately fenced export is in progress.
        if source is None:
            if self._base_source_binding is None:
                raise WorkerStateUnavailable("Control base delivery is not configured.")
            source = _DeferredControlSource(
                self._preparation,
                self._base_source_binding,
                self._authorization,
                delivery,
            )
        return await self._preparation.materialize_base(
            delivery,
            self.heartbeat_supervisor(),
            workspace,
            reservation,
            source,
            policy=policy,
        )

    async def prepare_execution(
        self, delivery: AssignmentDelivery, policy: ExecutionPolicy
    ) -> WorkerExecutionIntent:
        """Commit or recover original execution input; never starts a process."""
        if self._lock.locked():
            raise WorkerStateBusy("A worker claim exchange is already active.")
        async with self._lock:
            return await self._preparation.prepare_execution(
                delivery, self.heartbeat_supervisor(), policy
            )

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
