import asyncio
from uuid import uuid4

from devrelay.application.contracts.acknowledgements import WorkerAcknowledgementIntent
from devrelay.application.contracts.assignment_delivery import (
    AssignedJobDefinition,
    AssignmentDelivery,
)
from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    WorkerClaimRequest,
)
from devrelay.application.contracts.executions import WorkerExecutionIntent
from devrelay.application.contracts.worker_recovery import (
    AcknowledgementCompletion,
    AssignmentReference,
    WorkerClaimOutcome,
    WorkerRecoveryPhase,
    WorkerRecoveryState,
)
from devrelay.application.contracts.workspace_recovery import (
    WorkerWorkspaceCompletion,
    WorkerWorkspaceIntent,
    WorkspaceDirectoryIdentity,
    WorkspaceLocation,
    WorkspacePreparationDecision,
    WorkspaceRecovery,
)
from devrelay.application.contracts.workspaces import (
    WorkspaceBinding,
    WorkspaceReservation,
)
from devrelay.application.ports.worker_workspace_journal import WorkerWorkspaceJournal
from devrelay.application.ports.workspace_reservation import (
    WorkspaceReservationPort,
    WorkspaceUnavailable,
)
from devrelay.application.worker_executions import ExecutionAuthorityGuard


def _validate_original_input(
    state: WorkerRecoveryState, delivery: AssignmentDelivery
) -> None:
    if (
        not isinstance(state, WorkerRecoveryState)
        or state.phase is not WorkerRecoveryPhase.ACKNOWLEDGED
        or not isinstance(state.request, WorkerClaimRequest)
        or not isinstance(state.assignment, WorkerClaimOutcome)
        or not isinstance(state.assignment.assignment, AssignmentReference)
        or not isinstance(state.acknowledgement, WorkerAcknowledgementIntent)
        or not isinstance(state.completion, AcknowledgementCompletion)
        or not isinstance(state.execution, WorkerExecutionIntent)
    ):
        raise WorkspaceUnavailable("Durable acknowledged execution is required.")
    acknowledgement, reference = state.acknowledgement, state.assignment.assignment
    if (
        not isinstance(delivery, AssignmentDelivery)
        or not isinstance(delivery.definition, AssignedJobDefinition)
        or state.assignment != delivery.outcome
        or state.request.claim_request_id != acknowledgement.claim_request_id
        or acknowledgement.attempt_id != reference.attempt_id
        or acknowledgement.generation != reference.generation
        or state.execution.acknowledgement != acknowledgement
        or state.completion.intent != acknowledgement
        or not reference.issued_at
        <= state.completion.acknowledged_at
        < reference.expires_at
        or acknowledgement.snapshot_id != delivery.definition.snapshot.id
        or acknowledgement.base_revision != delivery.definition.base_revision
        or acknowledgement.definition_digest != delivery.definition.fingerprint()
    ):
        raise ClaimReplayConflict("Workspace changed original execution pins.")


def _historical_workspace(state: WorkerRecoveryState) -> WorkspaceRecovery | None:
    if type(state.workspace_history_unknown) is not bool:
        raise WorkspaceUnavailable("Workspace history is unavailable.")
    intent, completion = state.workspace_intent, state.workspace_completion
    if state.workspace_history_unknown:
        if intent is not None or completion is not None:
            raise WorkspaceUnavailable("Workspace history is unavailable.")
        return WorkspaceRecovery(intent=None, completion=None, history_unknown=True)
    if intent is None:
        if completion is not None:
            raise WorkspaceUnavailable("Workspace history is unavailable.")
        return None
    if (
        not isinstance(intent, WorkerWorkspaceIntent)
        or intent.execution != state.execution
    ):
        raise ClaimReplayConflict("Workspace history changed original execution.")
    if completion is not None and (
        not isinstance(completion, WorkerWorkspaceCompletion)
        or completion.intent != intent
    ):
        raise ClaimReplayConflict("Workspace history changed original preparation.")
    return WorkspaceRecovery(intent=intent, completion=completion)


class RecoverAssignedWorkspace:
    """Read historical evidence only, with no filesystem or authority dependency."""

    def __init__(self, journal: WorkerWorkspaceJournal) -> None:
        self._journal = journal

    async def execute(self, delivery: AssignmentDelivery) -> WorkspaceRecovery | None:
        state = await self._journal.load_state()
        await asyncio.to_thread(_validate_original_input, state, delivery)
        return _historical_workspace(state)


class ReserveAssignedWorkspace:
    """Commit one preparation before creation; never retry or restore live use.

    Cancellation or authority loss may leave durable pending/completed evidence
    and resources. Valid directory observations are recorded historically before
    the final live guard. No path deletes, adopts or recreates a directory.
    """

    def __init__(
        self,
        journal: WorkerWorkspaceJournal,
        supervisor: ExecutionAuthorityGuard,
        workspace: WorkspaceReservationPort,
    ) -> None:
        self._journal, self._supervisor, self._workspace = (
            journal,
            supervisor,
            workspace,
        )
        self._used = False
        self._expected = self._read_binding()

    def _read_binding(self) -> WorkspaceBinding:
        try:
            binding = self._workspace.binding
        except Exception:
            raise WorkspaceUnavailable("Workspace binding is unavailable.") from None
        if not isinstance(binding, WorkspaceBinding):
            raise WorkspaceUnavailable("Workspace binding is unavailable.")
        return binding

    def _require_binding(self) -> None:
        if self._read_binding() != self._expected:
            raise WorkspaceUnavailable("Workspace binding changed original input.")

    def _require_authority(self) -> None:
        self._require_binding()
        self._supervisor.require_current_assignment(self._expected.execution.reference)
        self._require_binding()

    def _validate(
        self, state: WorkerRecoveryState, delivery: AssignmentDelivery
    ) -> None:
        _validate_original_input(state, delivery)
        if (
            state.execution != self._expected.execution.intent
            or state.assignment is None
            or state.assignment.assignment != self._expected.execution.reference
            or delivery.definition != self._expected.definition
        ):
            raise ClaimReplayConflict("Workspace changed original execution pins.")
        if _historical_workspace(state) is not None:
            raise WorkspaceUnavailable(
                "Workspace preparation requires historical recovery."
            )

    async def execute(self, delivery: AssignmentDelivery) -> WorkspaceReservation:
        if self._used:
            raise WorkspaceUnavailable(
                "Workspace reservation has already been requested."
            )
        self._used = True
        self._require_authority()
        state = await self._journal.load_state()
        self._require_authority()
        await asyncio.to_thread(self._validate, state, delivery)
        self._require_authority()
        location = await self._workspace.prepare()
        self._require_authority()
        if not isinstance(location, WorkspaceLocation):
            raise WorkspaceUnavailable("Workspace root observation is unavailable.")
        candidate = WorkerWorkspaceIntent(
            workspace_request_id=uuid4(),
            execution=self._expected.execution.intent,
            location=location,
        )
        assert state.request is not None
        self._require_authority()
        decision = await self._journal.get_or_create_workspace(state.request, candidate)
        self._require_authority()
        if (
            not isinstance(decision, WorkspacePreparationDecision)
            or type(decision.created) is not bool
            or not decision.created
            or decision.intent != candidate
        ):
            raise WorkspaceUnavailable("Workspace creation was not newly admitted.")
        self._require_authority()
        reservation = await self._workspace.reserve(location)
        # Loss of authority cannot erase a valid historical native observation.
        self._require_binding()
        if (
            not isinstance(reservation, WorkspaceReservation)
            or reservation.binding != self._expected
            or not isinstance(reservation.identity, WorkspaceDirectoryIdentity)
            or reservation.directory_name
            != str(self._expected.execution.intent.execution_id)
        ):
            raise WorkspaceUnavailable("Workspace reservation changed original input.")
        try:
            completion = WorkerWorkspaceCompletion(
                intent=candidate, identity=reservation.identity
            )
        except ValueError:
            raise WorkspaceUnavailable(
                "Workspace child observation is unavailable."
            ) from None
        self._require_binding()
        recorded = await self._journal.record_workspace_completion(
            state.request, completion
        )
        self._require_binding()
        if (
            not isinstance(recorded, WorkerWorkspaceCompletion)
            or recorded != completion
        ):
            raise WorkspaceUnavailable("Workspace journal changed completion evidence.")
        self._require_authority()
        return reservation
