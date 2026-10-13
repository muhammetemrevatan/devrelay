from datetime import timedelta
from uuid import uuid4

from devrelay.application.contracts.assignment_delivery import AssignmentDelivery
from devrelay.application.contracts.executions import (
    ExecutionIsolation,
    ExecutionPolicy,
    ExecutionRuntime,
    WorkerExecutionIntent,
)
from devrelay.application.contracts.processes import ExecutionProcessBinding
from devrelay.application.contracts.worker_recovery import (
    AcknowledgementCompletion,
    WorkerRecoveryPhase,
    WorkerRecoveryState,
)
from devrelay.application.contracts.workspaces import WorkspaceBinding
from tests.acknowledgement_fixtures import (
    acknowledgement_inputs,
    acknowledgement_intent,
)


def workspace_inputs() -> tuple[
    WorkerRecoveryState, AssignmentDelivery, WorkspaceBinding
]:
    request, delivery = acknowledgement_inputs()
    acknowledgement = acknowledgement_intent(delivery)
    reference = delivery.outcome.assignment
    assert reference is not None
    intent = WorkerExecutionIntent(
        execution_id=uuid4(),
        acknowledgement=acknowledgement,
        policy=ExecutionPolicy(
            runtime=ExecutionRuntime.CODEX,
            isolation=ExecutionIsolation.WINDOWS_RESTRICTED,
            policy_digest="a" * 64,
        ),
    )
    state = WorkerRecoveryState(
        phase=WorkerRecoveryPhase.ACKNOWLEDGED,
        request=request,
        assignment=delivery.outcome,
        acknowledgement=acknowledgement,
        completion=AcknowledgementCompletion(
            intent=acknowledgement,
            acknowledged_at=reference.issued_at + timedelta(seconds=1),
        ),
        execution=intent,
    )
    binding = WorkspaceBinding(
        execution=ExecutionProcessBinding(intent=intent, reference=reference),
        definition=delivery.definition,
    )
    return state, delivery, binding


def workspace_binding() -> WorkspaceBinding:
    return workspace_inputs()[2]
