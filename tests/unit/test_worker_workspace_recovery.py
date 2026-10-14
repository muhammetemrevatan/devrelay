import asyncio
from dataclasses import replace
from typing import Any
from uuid import UUID

import pytest

import devrelay.application.worker_workspaces as workspace_consumers
from devrelay.application.contracts.claims import ClaimReplayConflict
from devrelay.application.contracts.workspace_recovery import WorkerWorkspaceCompletion
from devrelay.application.lease_deadlines import LeaseDeadlineRejected
from devrelay.application.ports.worker_journal import WorkerStateUnavailable
from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from devrelay.application.worker_workspaces import RecoverAssignedWorkspace
from tests.unit.test_worker_workspace_reservation import context, pending_intent


@pytest.mark.parametrize("history", ["none", "pending", "completed", "unknown"])
def test_recovery_reads_only_exact_historical_evidence_without_native_or_clock(
    history: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup = context()
    intent, completion = None, None
    if history == "unknown":
        setup.journal.state = replace(
            setup.journal.state, workspace_history_unknown=True
        )
    elif history != "none":
        intent = pending_intent(setup)
        if history == "completed":
            completion = WorkerWorkspaceCompletion(
                intent=intent, identity=setup.workspace.reservation.identity
            )
        setup.journal.state = replace(
            setup.journal.state,
            workspace_intent=intent,
            workspace_completion=completion,
        )
    original = setup.journal.state
    setup.execution.guard.stopped = True
    setup.workspace.binding = None

    def forbidden_nonce() -> UUID:
        raise AssertionError("Historical recovery must not generate new input.")

    monkeypatch.setattr(workspace_consumers, "uuid4", forbidden_nonce)
    recovery = asyncio.run(
        RecoverAssignedWorkspace(setup.journal).execute(setup.execution.delivery)
    )

    if history == "none":
        assert recovery is None
    else:
        assert recovery is not None
        assert recovery.intent is intent
        assert recovery.completion is completion
        assert recovery.history_unknown is (history == "unknown")
        assert original.request.proof.reveal() not in repr(recovery)
    assert setup.execution.calls == ["recover"]
    assert setup.journal.state is original
    assert setup.journal.preparation_writes == setup.journal.completion_writes == 0
    assert setup.workspace.preparations == setup.workspace.requests == 0


def test_multiple_historical_reads_preserve_same_input_and_completion() -> None:
    setup = context()
    intent = pending_intent(setup)
    completion = WorkerWorkspaceCompletion(
        intent=intent, identity=setup.workspace.reservation.identity
    )
    setup.journal.state = replace(
        setup.journal.state, workspace_intent=intent, workspace_completion=completion
    )
    service = RecoverAssignedWorkspace(setup.journal)

    async def scenario() -> None:
        first = await service.execute(setup.execution.delivery)
        second = await service.execute(setup.execution.delivery)
        assert first == second
        assert first is not None and second is not None
        assert first.intent is second.intent is intent
        assert first.completion is second.completion is completion

    asyncio.run(scenario())

    assert setup.workspace.requests == 0


def test_late_authority_loss_after_creation_is_recoverable_historically() -> None:
    setup = context()
    setup.workspace.after_create = lambda: setattr(
        setup.execution.guard, "stopped", True
    )

    async def scenario() -> None:
        with pytest.raises(LeaseDeadlineRejected):
            await setup.service.execute(setup.execution.delivery)
        recovered = await RecoverAssignedWorkspace(setup.journal).execute(
            setup.execution.delivery
        )
        assert recovered is not None
        assert recovered.completion is setup.journal.state.workspace_completion
        assert recovered.intent is setup.journal.state.workspace_intent

    asyncio.run(scenario())

    assert setup.workspace.requests == 1


@pytest.mark.parametrize("phase", ["input", "completion"])
def test_cancelled_commit_recovery_never_restarts_native_creation(phase: str) -> None:
    setup = context()

    async def scenario() -> None:
        entered, release = asyncio.Event(), asyncio.Event()
        if phase == "input":
            setup.journal.preparation_entered, setup.journal.preparation_release = (
                entered,
                release,
            )
        else:
            setup.journal.completion_entered, setup.journal.completion_release = (
                entered,
                release,
            )
        task = asyncio.create_task(setup.service.execute(setup.execution.delivery))
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        release.set()
        setup.execution.guard.stopped = True
        historical = await RecoverAssignedWorkspace(setup.journal).execute(
            setup.execution.delivery
        )
        assert historical is not None
        assert historical.intent is setup.journal.state.workspace_intent
        assert (historical.completion is not None) == (phase == "completion")

    asyncio.run(scenario())

    assert setup.workspace.requests == (1 if phase == "completion" else 0)


@pytest.mark.parametrize(
    "field,value",
    [
        ("provider_repository_id", 999),
        ("repository_slug", "different/repository"),
        ("base_revision", "b" * 40),
        ("authorized_branch", "agent/changed"),
        ("policy_version", "changed"),
    ],
)
def test_changed_definition_cannot_recover_another_workspace(
    field: str, value: Any
) -> None:
    setup = context()
    setup.journal.state = replace(
        setup.journal.state, workspace_intent=pending_intent(setup)
    )
    delivery = replace(
        setup.execution.delivery,
        definition=replace(setup.execution.delivery.definition, **{field: value}),
    )

    with pytest.raises(ClaimReplayConflict):
        asyncio.run(RecoverAssignedWorkspace(setup.journal).execute(delivery))

    assert setup.workspace.requests == 0


def test_history_with_changed_execution_is_rejected() -> None:
    setup = context()
    intent = pending_intent(setup)
    changed = replace(
        intent, execution=replace(intent.execution, execution_id=UUID(int=997))
    )
    object.__setattr__(setup.journal.state, "workspace_intent", changed)

    with pytest.raises(ClaimReplayConflict):
        asyncio.run(
            RecoverAssignedWorkspace(setup.journal).execute(setup.execution.delivery)
        )

    assert setup.journal.preparation_writes == 0


def test_completion_for_other_preparation_cannot_be_recovered() -> None:
    setup = context()
    intent = pending_intent(setup)
    setup.journal.state = replace(setup.journal.state, workspace_intent=intent)
    changed = replace(intent, workspace_request_id=UUID(int=999))
    completion = WorkerWorkspaceCompletion(
        intent=changed, identity=setup.workspace.reservation.identity
    )
    object.__setattr__(setup.journal.state, "workspace_completion", completion)

    with pytest.raises(ClaimReplayConflict):
        asyncio.run(
            RecoverAssignedWorkspace(setup.journal).execute(setup.execution.delivery)
        )

    assert setup.workspace.requests == 0


def test_orphan_completion_is_unavailable_even_without_pending_input() -> None:
    setup = context()
    completion = WorkerWorkspaceCompletion(
        intent=pending_intent(setup), identity=setup.workspace.reservation.identity
    )
    object.__setattr__(setup.journal.state, "workspace_completion", completion)

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(
            RecoverAssignedWorkspace(setup.journal).execute(setup.execution.delivery)
        )

    assert setup.workspace.preparations == 0


@pytest.mark.parametrize("invalid", [1, None, "unknown"])
def test_untyped_history_flag_is_not_interpreted_as_known_absence(invalid: Any) -> None:
    setup = context()
    object.__setattr__(setup.journal.state, "workspace_history_unknown", invalid)

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(
            RecoverAssignedWorkspace(setup.journal).execute(setup.execution.delivery)
        )

    assert setup.journal.state.workspace_history_unknown is invalid


def test_history_unknown_with_source_input_is_rejected_without_clearing_it() -> None:
    setup = context()
    intent = pending_intent(setup)
    setup.journal.state = replace(setup.journal.state, workspace_intent=intent)
    object.__setattr__(setup.journal.state, "workspace_history_unknown", True)

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(
            RecoverAssignedWorkspace(setup.journal).execute(setup.execution.delivery)
        )

    assert setup.journal.state.workspace_intent is intent
    assert setup.journal.state.workspace_history_unknown is True


def test_journal_load_failure_is_preserved_without_native_fallback() -> None:
    setup = context()
    setup.journal.error = WorkerStateUnavailable("Protected journal is unavailable.")

    with pytest.raises(WorkerStateUnavailable):
        asyncio.run(
            RecoverAssignedWorkspace(setup.journal).execute(setup.execution.delivery)
        )

    assert setup.workspace.preparations == setup.workspace.requests == 0


def test_cancelled_historical_read_leaves_inputs_unchanged() -> None:
    setup = context()
    original = setup.journal.state

    async def scenario() -> None:
        setup.journal.entered, setup.journal.release = asyncio.Event(), asyncio.Event()
        task = asyncio.create_task(
            RecoverAssignedWorkspace(setup.journal).execute(setup.execution.delivery)
        )
        await setup.journal.entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        setup.journal.release.set()

    asyncio.run(scenario())

    assert setup.journal.state is original
    assert setup.journal.preparation_writes == setup.journal.completion_writes == 0
