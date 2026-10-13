import asyncio
from typing import Protocol
from uuid import uuid4

from devrelay.application.contracts.assignment_delivery import (
    AssignmentDelivery,
    AssignmentUnavailable,
)
from devrelay.application.contracts.claims import ClaimReplayConflict
from devrelay.application.contracts.executions import (
    ExecutionPolicy,
    WorkerExecutionIntent,
)
from devrelay.application.contracts.worker_recovery import (
    AssignmentReference,
    WorkerRecoveryPhase,
)
from devrelay.application.ports.worker_execution_journal import WorkerExecutionJournal


class ExecutionAuthorityGuard(Protocol):
    def require_current_assignment(self, reference: AssignmentReference) -> None: ...


class PrepareAssignedExecution:
    """Commit pinned preparation under fresh coordination; never launch a process."""

    def __init__(
        self,
        journal: WorkerExecutionJournal,
        supervisor: ExecutionAuthorityGuard,
        policy: ExecutionPolicy,
    ) -> None:
        if not isinstance(policy, ExecutionPolicy):
            raise ValueError("Execution preparation requires a typed policy.")
        self._journal = journal
        self._supervisor = supervisor
        self._policy = policy

    async def execute(self, delivery: AssignmentDelivery) -> WorkerExecutionIntent:
        state = await self._journal.load_state()
        if (
            not isinstance(delivery, AssignmentDelivery)
            or state.phase is not WorkerRecoveryPhase.ACKNOWLEDGED
            or state.request is None
            or state.assignment is None
            or state.assignment.assignment is None
            or state.acknowledgement is None
            or state.completion is None
        ):
            raise AssignmentUnavailable(
                "Durable completed acknowledgement is required."
            )
        if state.assignment != delivery.outcome:
            raise ClaimReplayConflict("Execution delivery changed original assignment.")
        acknowledgement = state.acknowledgement
        digest = await asyncio.to_thread(delivery.definition.fingerprint)
        if (
            acknowledgement.snapshot_id != delivery.definition.snapshot.id
            or acknowledgement.base_revision != delivery.definition.base_revision
            or acknowledgement.definition_digest != digest
        ):
            raise ClaimReplayConflict(
                "Execution delivery changed accepted definition pins."
            )
        existing = state.execution
        candidate = WorkerExecutionIntent(
            execution_id=existing.execution_id if existing is not None else uuid4(),
            acknowledgement=acknowledgement,
            policy=self._policy,
        )
        if existing is not None:
            existing.require_same_input(candidate)
            return existing
        reference = state.assignment.assignment
        self._supervisor.require_current_assignment(reference)
        recorded = await self._journal.get_or_create_execution(state.request, candidate)
        if not isinstance(recorded, WorkerExecutionIntent):
            raise ClaimReplayConflict(
                "Execution journal returned incompatible preparation."
            )
        recorded.require_same_input(candidate)
        self._supervisor.require_current_assignment(reference)
        return recorded
