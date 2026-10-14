import asyncio

from devrelay.application.contracts.acknowledgements import WorkerAcknowledgementIntent
from devrelay.application.contracts.assignment_delivery import AssignmentDelivery
from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    WorkerClaimRequest,
)
from devrelay.application.contracts.executions import WorkerExecutionIntent
from devrelay.application.contracts.worker_recovery import (
    AcknowledgementCompletion,
    WorkerClaimOutcome,
    WorkerRecoveryPhase,
    WorkerRecoveryState,
)
from devrelay.application.contracts.workspaces import (
    WorkspaceBinding,
    WorkspaceDirectoryIdentity,
    WorkspaceReservation,
)
from devrelay.application.ports.worker_execution_journal import WorkerExecutionJournal
from devrelay.application.ports.workspace_reservation import (
    WorkspaceReservationPort,
    WorkspaceUnavailable,
)
from devrelay.application.worker_executions import ExecutionAuthorityGuard


class ReserveAssignedWorkspace:
    """Reserve once under fresh authority; never prepare a checkout or recover use.

    Cancellation or a failed post-reservation guard may leave resources behind.
    The owning port retains those resources; this consumer never deletes them or
    retries. A returned directory observation grants no execution permission.
    """

    def __init__(
        self,
        journal: WorkerExecutionJournal,
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

    def _validate(
        self, state: WorkerRecoveryState, delivery: AssignmentDelivery
    ) -> None:
        expected = self._expected
        execution, reference = expected.execution.intent, expected.execution.reference
        if (
            not isinstance(state, WorkerRecoveryState)
            or state.phase is not WorkerRecoveryPhase.ACKNOWLEDGED
            or not isinstance(state.request, WorkerClaimRequest)
            or not isinstance(state.assignment, WorkerClaimOutcome)
            or not isinstance(state.acknowledgement, WorkerAcknowledgementIntent)
            or not isinstance(state.completion, AcknowledgementCompletion)
            or not isinstance(state.execution, WorkerExecutionIntent)
        ):
            raise WorkspaceUnavailable("Durable acknowledged execution is required.")
        if (
            not isinstance(delivery, AssignmentDelivery)
            or state.assignment != delivery.outcome
            or state.assignment.assignment != reference
            or state.request.claim_request_id
            != execution.acknowledgement.claim_request_id
            or state.acknowledgement != execution.acknowledgement
            or state.completion.intent != execution.acknowledgement
            or state.execution != execution
            or delivery.definition != expected.definition
            or delivery.definition.fingerprint()
            != execution.acknowledgement.definition_digest
        ):
            raise ClaimReplayConflict("Workspace changed original execution pins.")

    async def execute(self, delivery: AssignmentDelivery) -> WorkspaceReservation:
        if self._used:
            raise WorkspaceUnavailable(
                "Workspace reservation has already been requested."
            )
        self._used = True
        state = await self._journal.load_state()
        await asyncio.to_thread(self._validate, state, delivery)
        self._require_binding()
        self._supervisor.require_current_assignment(self._expected.execution.reference)
        self._require_binding()
        reservation = await self._workspace.reserve()
        self._require_binding()
        if (
            not isinstance(reservation, WorkspaceReservation)
            or reservation.binding != self._expected
            or not isinstance(reservation.identity, WorkspaceDirectoryIdentity)
            or reservation.directory_name
            != str(self._expected.execution.intent.execution_id)
        ):
            raise WorkspaceUnavailable("Workspace reservation changed original input.")
        self._supervisor.require_current_assignment(self._expected.execution.reference)
        self._require_binding()
        return reservation
