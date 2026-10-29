import asyncio
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import cast
from uuid import uuid4

import pytest

import devrelay.application.worker_claims as claims_module
from devrelay.application.contracts.assignment_delivery import AssignmentDelivery
from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    LeaseProof,
    WorkerClaimRequest,
)
from devrelay.application.contracts.coding_result_receipts import CodingResultAcceptance
from devrelay.application.contracts.coding_result_requests import (
    WorkerCodingResultIntent,
)
from devrelay.application.contracts.editable_workspaces import EditableWorkspace
from devrelay.application.contracts.executions import WorkerExecutionIntent
from devrelay.application.contracts.worker_recovery import (
    AcknowledgementCompletion,
    WorkerRecoveryPhase,
    WorkerRecoveryState,
)
from devrelay.application.contracts.worker_result_recovery import (
    WorkerCodingResultState,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.contracts.workspace_recovery import (
    WorkerWorkspaceCompletion,
    WorkerWorkspaceIntent,
    WorkspaceDirectoryIdentity,
)
from devrelay.application.lease_deadlines import LeaseDeadlineRejected
from devrelay.application.ports.control_gateway import ControlGateway
from devrelay.application.ports.worker_journal import (
    WorkerClaimJournal,
    WorkerStateUnavailable,
)
from devrelay.application.worker_claims import PrepareWorkerClaim, RequestWorkerClaim
from devrelay.application.worker_heartbeats import HeartbeatSupervisor
from tests.unit.test_candidate_objects import commit
from tests.unit.test_coding_results import context as result_context
from tests.unit.test_worker_coding import Context as CodingContext
from tests.unit.test_worker_coding import context as coding_context
from tests.unit.test_worker_coding_results import Gateway, Journal


@dataclass
class Context:
    coding: CodingContext
    journal: Journal
    gateway: Gateway
    client: RequestWorkerClaim

    async def run(self) -> CodingResultAcceptance:
        workspace = self.coding.workspace
        return await self.client.run_coding_result(
            workspace.delivery,
            self.coding.service,
            workspace.binding.execution.intent,
            workspace.git_preparation,
            workspace.editable,
            commit=commit(),
        )


def context(monkeypatch: pytest.MonkeyPatch, *, enabled: bool = True) -> Context:
    coding = coding_context()
    workspace = coding.workspace
    execution = workspace.binding.execution.intent
    reference = workspace.delivery.outcome.assignment
    assert reference is not None
    workspace_intent = WorkerWorkspaceIntent(
        workspace_request_id=uuid4(),
        execution=execution,
        location=workspace.location,
    )
    original = WorkerRecoveryState(
        phase=WorkerRecoveryPhase.ACKNOWLEDGED,
        request=WorkerClaimRequest(
            claim_request_id=execution.acknowledgement.claim_request_id,
            proof=LeaseProof.generate(),
        ),
        assignment=workspace.delivery.outcome,
        acknowledgement=execution.acknowledgement,
        completion=AcknowledgementCompletion(
            intent=execution.acknowledgement,
            acknowledged_at=reference.issued_at + timedelta(seconds=1),
        ),
        execution=execution,
        workspace_intent=workspace_intent,
        workspace_completion=WorkerWorkspaceCompletion(
            intent=workspace_intent,
            identity=workspace.reservation.identity,
        ),
    )
    journal = Journal(WorkerCodingResultState(original=original), coding.calls)
    gateway = Gateway(workspace.delivery.definition, coding.calls)
    authorization = WorkerAuthorization(
        worker_id=reference.worker_id,
        allowed_repository_ids=frozenset({reference.repository_id}),
        credential_expires_at=reference.expires_at + timedelta(hours=1),
    )
    preparation = PrepareWorkerClaim(
        cast(WorkerClaimJournal, journal), result_journal=journal
    )
    client = RequestWorkerClaim(
        preparation,
        cast(ControlGateway, object()),
        authorization,
        coding_result_gateway=gateway if enabled else None,
    )
    monkeypatch.setattr(
        client,
        "heartbeat_supervisor",
        lambda: cast(HeartbeatSupervisor, coding.guard),
    )
    return Context(coding, journal, gateway, client)


def test_actual_coding_runner_delivers_original_pins_after_checks_and_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = context(monkeypatch)
    original = value.journal.state.original

    result = asyncio.run(value.run())

    assert not result.recovered
    assert len(value.gateway.submitted) == 1
    request = value.gateway.submitted[0]
    assert request.intent.execution == value.coding.workspace.binding.execution.intent
    assert request.intent.result.cycles[-1].passed
    assert value.journal.state.intent == request.intent
    assert value.journal.state.receipt == result.receipt
    assert value.journal.state.original == original
    assert value.coding.workspace.frozen is not None
    assert (
        request.intent.result.final_candidate == value.coding.workspace.frozen.snapshot
    )
    assert value.coding.calls.index("load") < value.coding.calls.index("code-0")
    assert value.coding.calls.index("check-0-lint") < value.coding.calls.index("commit")
    assert value.coding.calls.index("commit") < value.coding.calls.index("post")


@pytest.mark.parametrize("retained", ["result", "unknown"])
def test_retained_coding_history_requires_recovery_before_any_runtime(
    monkeypatch: pytest.MonkeyPatch, retained: str
) -> None:
    value = context(monkeypatch)
    workspace = value.coding.workspace
    execution = workspace.binding.execution.intent
    report = result_context(
        definition=workspace.delivery.definition, execution_id=execution.execution_id
    ).result
    intent = WorkerCodingResultIntent(
        result_request_id=uuid4(), execution=execution, result=report
    )
    value.journal.state = (
        replace(
            value.journal.state,
            intent=intent,
            target_branch=workspace.delivery.definition.target_branch,
        )
        if retained == "result"
        else replace(value.journal.state, history_unknown=True)
    )

    with pytest.raises(WorkerStateUnavailable, match="requires recovery"):
        asyncio.run(value.run())

    assert value.coding.calls == ["load"]
    assert not value.gateway.submitted
    assert workspace.frozen is None


@pytest.mark.parametrize(
    "changed", ["definition", "execution", "location", "completion", "unknown"]
)
def test_protected_execution_and_workspace_mismatch_prevents_runtime(
    monkeypatch: pytest.MonkeyPatch, changed: str
) -> None:
    value = context(monkeypatch)
    workspace = value.coding.workspace
    execution = workspace.binding.execution.intent
    editable = workspace.editable
    delivery = workspace.delivery
    original = value.journal.state.original
    if changed == "definition":
        delivery = replace(
            delivery,
            definition=replace(
                delivery.definition,
                checks=tuple(reversed(delivery.definition.checks)),
            ),
        )
    elif changed == "execution":
        execution = replace(
            execution, policy=replace(execution.policy, policy_digest="f" * 64)
        )
    elif changed == "location":
        editable = replace(
            editable,
            location=replace(editable.location, root_directory=r"C:\other-root"),
        )
    elif changed == "completion":
        assert original.workspace_completion is not None
        value.journal.state = replace(
            value.journal.state,
            original=replace(
                original,
                workspace_completion=replace(
                    original.workspace_completion,
                    identity=WorkspaceDirectoryIdentity(
                        volume_serial_number=1, file_index=99
                    ),
                ),
            ),
        )
    else:
        value.journal.state = replace(
            value.journal.state,
            original=replace(
                original,
                workspace_intent=None,
                workspace_completion=None,
                workspace_history_unknown=True,
            ),
        )

    with pytest.raises(ClaimReplayConflict):
        asyncio.run(
            value.client.run_coding_result(
                delivery,
                value.coding.service,
                execution,
                workspace.git_preparation,
                editable,
                commit=commit(),
            )
        )

    assert value.coding.calls == ["load"]
    assert not value.gateway.submitted
    assert workspace.frozen is None


def test_authority_loss_after_journal_validation_prevents_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = context(monkeypatch)
    original_validation = claims_module._require_new_coding_input

    def expire_after_validation(
        state: WorkerCodingResultState,
        delivery: AssignmentDelivery,
        execution: WorkerExecutionIntent,
        editable: EditableWorkspace,
    ) -> None:
        original_validation(state, delivery, execution, editable)
        value.coding.guard.stopped = True

    monkeypatch.setattr(
        claims_module, "_require_new_coding_input", expire_after_validation
    )

    with pytest.raises(LeaseDeadlineRejected):
        asyncio.run(value.run())

    assert value.coding.calls == ["load"]
    assert not value.gateway.submitted
    assert value.coding.workspace.frozen is None


def test_duplicate_client_call_never_reenters_runtime_or_result_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> tuple[list[str], list[str]]:
        value = context(monkeypatch)
        await value.run()
        before = list(value.coding.calls)

        with pytest.raises(WorkerStateUnavailable, match="already been requested"):
            await value.run()

        assert len(value.gateway.submitted) == 1
        return before, value.coding.calls

    before, after = asyncio.run(scenario())

    assert before == after


def test_disabled_result_delivery_refuses_before_journal_or_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = context(monkeypatch, enabled=False)

    with pytest.raises(WorkerStateUnavailable, match="not enabled"):
        asyncio.run(value.run())

    assert value.coding.calls == []
    assert not value.gateway.submitted
    assert value.coding.workspace.frozen is None
