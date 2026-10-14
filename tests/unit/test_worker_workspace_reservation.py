import asyncio
import threading
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import Any, cast
from uuid import UUID

import pytest

from devrelay.application.contracts.assignment_delivery import (
    AssignedJobDefinition,
    AssignmentDelivery,
)
from devrelay.application.contracts.claims import ClaimReplayConflict
from devrelay.application.contracts.worker_recovery import (
    AssignmentReference,
    WorkerRecoveryState,
)
from devrelay.application.contracts.workspaces import (
    WorkspaceBinding,
    WorkspaceDirectoryIdentity,
    WorkspaceReservation,
)
from devrelay.application.lease_deadlines import LeaseDeadlineRejected
from devrelay.application.ports.worker_journal import WorkerStateUnavailable
from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from devrelay.application.worker_workspaces import ReserveAssignedWorkspace
from tests.unit.test_worker_execution_authority import Clock, live_supervisor
from tests.unit.test_worker_execution_preparation import (
    context as execution_context,
)
from tests.workspace_fixtures import workspace_inputs


class Guard:
    def __init__(self, reference: AssignmentReference, calls: list[str]) -> None:
        self.reference, self.calls = reference, calls
        self.stopped = False
        self.after_check: Callable[[], None] = lambda: None

    def require_current_assignment(self, reference: AssignmentReference) -> None:
        self.calls.append("authority")
        if self.stopped or reference != self.reference:
            raise LeaseDeadlineRejected("Current assignment authority unavailable.")
        self.after_check()


@dataclass
class ExecutionContext:
    delivery: AssignmentDelivery
    guard: Guard
    calls: list[str]


class Journal:
    def __init__(self, state: WorkerRecoveryState, calls: list[str]) -> None:
        self.state: Any = state
        self.calls = calls
        self.error: Exception | None = None
        self.entered: asyncio.Event | None = None
        self.release: asyncio.Event | None = None
        self.writes = 0

    async def load_state(self) -> WorkerRecoveryState:
        self.calls.append("recover")
        if self.entered is not None:
            self.entered.set()
        if self.release is not None:
            await self.release.wait()
        if self.error is not None:
            raise self.error
        return cast(WorkerRecoveryState, self.state)

    async def get_or_create_execution(self, request: Any, candidate: Any) -> Any:
        self.writes += 1
        raise AssertionError("Reservation must not create or alter durable execution.")


class Workspace:
    def __init__(self, binding: WorkspaceBinding, calls: list[str]) -> None:
        self.binding: Any = binding
        self.calls = calls
        self.reservation = WorkspaceReservation(
            binding=binding,
            directory_name=str(binding.execution.intent.execution_id),
            identity=WorkspaceDirectoryIdentity(volume_serial_number=10, file_index=20),
        )
        self.returned: Any = self.reservation
        self.error: Exception | None = None
        self.after_create: Callable[[], None] = lambda: None
        self.entered: asyncio.Event | None = None
        self.release: asyncio.Event | None = None
        self.created = False
        self.requests = 0

    async def reserve(self) -> WorkspaceReservation:
        self.calls.append("reserve")
        self.requests += 1
        if self.error is not None:
            raise self.error
        self.created = True
        self.after_create()
        if self.entered is not None:
            self.entered.set()
        if self.release is not None:
            await self.release.wait()
        return cast(WorkspaceReservation, self.returned)


@dataclass
class Context:
    execution: ExecutionContext
    journal: Journal
    workspace: Workspace
    service: ReserveAssignedWorkspace


def context() -> Context:
    state, delivery, binding = workspace_inputs()
    calls: list[str] = []
    guard = Guard(binding.execution.reference, calls)
    execution = ExecutionContext(delivery, guard, calls)
    journal, workspace = Journal(state, calls), Workspace(binding, calls)
    return Context(
        execution,
        journal,
        workspace,
        ReserveAssignedWorkspace(journal, execution.guard, workspace),
    )


def test_reservation_uses_existing_exact_execution_between_fresh_guards() -> None:
    setup = context()
    original = setup.journal.state
    delivery = replace(setup.execution.delivery, lease_authority_active=False)

    reservation = asyncio.run(setup.service.execute(delivery))

    assert reservation is setup.workspace.reservation
    assert setup.execution.calls == ["recover", "authority", "reserve", "authority"]
    assert setup.workspace.requests == 1
    assert setup.journal.state is original
    assert setup.journal.writes == 0
    assert original.request.proof.reveal() not in repr(reservation)


@pytest.mark.parametrize(
    "field", ["execution", "completion", "acknowledgement", "request", "assignment"]
)
@pytest.mark.parametrize("invalid", [None, "untrusted"])
def test_corrupt_durable_input_cannot_create_a_directory(
    field: str, invalid: Any
) -> None:
    setup = context()
    values = {
        name: getattr(setup.journal.state, name)
        for name in setup.journal.state.__dataclass_fields__
    }
    values[field] = invalid
    corrupt = object.__new__(WorkerRecoveryState)
    for name, value in values.items():
        object.__setattr__(corrupt, name, value)
    setup.journal.state = corrupt

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert setup.workspace.requests == 0
    assert setup.journal.writes == 0


def test_untyped_durable_state_cannot_create_a_directory() -> None:
    setup = context()
    setup.journal.state = {"execution": "untrusted"}

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert setup.workspace.requests == 0


@pytest.mark.parametrize("change", ["execution_id", "policy", "acknowledgement"])
def test_different_durable_execution_is_not_repinned(change: str) -> None:
    setup = context()
    state = setup.journal.state
    intent = state.execution
    assert intent is not None
    if change == "execution_id":
        intent = replace(intent, execution_id=UUID(int=778))
    elif change == "policy":
        intent = replace(intent, policy=replace(intent.policy, policy_digest="b" * 64))
    else:
        acknowledgement = replace(
            intent.acknowledgement, acknowledgement_request_id=UUID(int=888)
        )
        intent = replace(intent, acknowledgement=acknowledgement)
        state = replace(
            state,
            acknowledgement=acknowledgement,
            completion=replace(state.completion, intent=acknowledgement),
            execution=intent,
        )
    setup.journal.state = replace(state, execution=intent)

    with pytest.raises(ClaimReplayConflict):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert setup.workspace.requests == 0
    assert setup.journal.state.execution is intent


@pytest.mark.parametrize(
    "field,value",
    [
        ("provider_repository_id", 999),
        ("repository_slug", "owner/different"),
        ("source_task_id", UUID(int=999)),
        ("authorized_branch", "agent/different"),
        ("target_branch", "different"),
        ("base_revision", "b" * 40),
        ("policy_version", "different"),
        ("max_repair_cycles", 5),
    ],
)
def test_full_delivered_definition_mismatch_blocks_reservation(
    field: str, value: Any
) -> None:
    setup = context()
    definition = replace(setup.execution.delivery.definition, **{field: value})
    delivery = replace(setup.execution.delivery, definition=definition)

    with pytest.raises(ClaimReplayConflict):
        asyncio.run(setup.service.execute(delivery))

    assert setup.workspace.requests == 0


def test_changed_check_policy_is_part_of_full_definition() -> None:
    setup = context()
    definition = setup.execution.delivery.definition
    check = replace(definition.checks[0], argv=("different",))
    delivery = replace(
        setup.execution.delivery,
        definition=replace(definition, checks=(check, *definition.checks[1:])),
    )

    with pytest.raises(ClaimReplayConflict):
        asyncio.run(setup.service.execute(delivery))

    assert setup.workspace.requests == 0


def test_changed_original_reference_blocks_reservation() -> None:
    setup = context()
    outcome = setup.execution.delivery.outcome
    reference = outcome.assignment
    assert reference is not None
    outcome = replace(
        outcome,
        assignment=replace(
            reference, expires_at=reference.expires_at + timedelta(seconds=1)
        ),
    )
    setup.journal.state = replace(setup.journal.state, assignment=outcome)
    delivery = replace(setup.execution.delivery, outcome=outcome)

    with pytest.raises(ClaimReplayConflict):
        asyncio.run(setup.service.execute(delivery))

    assert setup.workspace.requests == 0


def test_stopped_authority_never_creates_despite_historical_execution() -> None:
    setup = context()
    setup.execution.guard.stopped = True

    with pytest.raises(LeaseDeadlineRejected):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert setup.execution.calls == ["recover", "authority"]
    assert not setup.workspace.created


def test_authority_loss_after_creation_retains_resource_and_refuses_retry() -> None:
    setup = context()
    setup.workspace.after_create = lambda: setattr(
        setup.execution.guard, "stopped", True
    )

    with pytest.raises(LeaseDeadlineRejected):
        asyncio.run(setup.service.execute(setup.execution.delivery))
    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert setup.workspace.created
    assert setup.workspace.requests == 1
    assert setup.journal.writes == 0


@pytest.mark.parametrize("returned", [None, True, {"directory_name": "untrusted"}])
def test_malformed_port_output_never_returns_success(returned: Any) -> None:
    setup = context()
    setup.workspace.returned = returned

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert setup.workspace.created
    assert setup.workspace.requests == 1


def test_other_execution_output_is_rejected_without_touching_it() -> None:
    setup, foreign = context(), context()
    setup.workspace.returned = foreign.workspace.reservation

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert setup.workspace.requests == 1
    assert foreign.workspace.requests == 0


def test_tampered_directory_name_is_rejected() -> None:
    setup = context()
    object.__setattr__(setup.workspace.reservation, "directory_name", "../foreign")

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert setup.workspace.requests == 1


def test_tampered_native_identity_is_rejected() -> None:
    setup = context()
    object.__setattr__(setup.workspace.reservation, "identity", None)

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert setup.workspace.created


def test_supplier_binding_change_before_call_does_not_touch_foreign_resource() -> None:
    setup, foreign = context(), context()
    setup.workspace.binding = foreign.workspace.binding

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert setup.workspace.requests == 0
    assert foreign.workspace.requests == 0


def test_binding_changed_inside_precreation_guard_is_fenced() -> None:
    setup, foreign = context(), context()
    setup.execution.guard.after_check = lambda: setattr(
        setup.workspace, "binding", foreign.workspace.binding
    )

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert setup.workspace.requests == 0
    assert foreign.workspace.requests == 0


def test_binding_changed_inside_postcreation_guard_cannot_escape_final_fence() -> None:
    setup, foreign = context(), context()

    def change_only_after_creation() -> None:
        if setup.workspace.created:
            setup.workspace.binding = foreign.workspace.binding

    setup.execution.guard.after_check = change_only_after_creation

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert setup.workspace.created
    assert setup.workspace.requests == 1
    assert foreign.workspace.requests == 0


def test_supplier_binding_change_during_call_never_returns_foreign_resource() -> None:
    setup, foreign = context(), context()
    setup.workspace.after_create = lambda: setattr(
        setup.workspace, "binding", foreign.workspace.binding
    )

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert setup.workspace.created
    assert setup.workspace.requests == 1
    assert foreign.workspace.requests == 0


def test_load_failure_latches_consumer_without_any_native_action() -> None:
    setup = context()
    setup.journal.error = WorkerStateUnavailable("Protected state unavailable.")

    with pytest.raises(WorkerStateUnavailable):
        asyncio.run(setup.service.execute(setup.execution.delivery))
    setup.journal.error = None
    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert setup.execution.calls == ["recover"]
    assert setup.workspace.requests == 0


def test_port_failure_latches_consumer_without_retry() -> None:
    setup = context()
    setup.workspace.error = WorkspaceUnavailable("Directory uncertain.")

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.execution.delivery))
    setup.workspace.error = None
    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert setup.workspace.requests == 1


def test_concurrent_and_repeated_requests_cannot_reserve_twice() -> None:
    setup = context()

    async def scenario() -> None:
        setup.journal.entered, setup.journal.release = asyncio.Event(), asyncio.Event()
        first = asyncio.create_task(setup.service.execute(setup.execution.delivery))
        await setup.journal.entered.wait()
        with pytest.raises(WorkspaceUnavailable):
            await setup.service.execute(setup.execution.delivery)
        setup.journal.release.set()
        assert await first is setup.workspace.reservation
        with pytest.raises(WorkspaceUnavailable):
            await setup.service.execute(setup.execution.delivery)

    asyncio.run(scenario())

    assert setup.workspace.requests == 1
    assert setup.execution.calls.count("recover") == 1


def test_cancellation_during_recovery_cannot_be_retried() -> None:
    setup = context()

    async def scenario() -> None:
        setup.journal.entered, setup.journal.release = asyncio.Event(), asyncio.Event()
        first = asyncio.create_task(setup.service.execute(setup.execution.delivery))
        await setup.journal.entered.wait()
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        setup.journal.release.set()
        with pytest.raises(WorkspaceUnavailable):
            await setup.service.execute(setup.execution.delivery)

    asyncio.run(scenario())

    assert setup.workspace.requests == 0


def test_cancellation_after_native_creation_preserves_uncertainty() -> None:
    setup = context()

    async def scenario() -> None:
        setup.workspace.entered, setup.workspace.release = (
            asyncio.Event(),
            asyncio.Event(),
        )
        first = asyncio.create_task(setup.service.execute(setup.execution.delivery))
        await setup.workspace.entered.wait()
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        setup.workspace.release.set()
        with pytest.raises(WorkspaceUnavailable):
            await setup.service.execute(setup.execution.delivery)

    asyncio.run(scenario())

    assert setup.workspace.created
    assert setup.workspace.requests == 1
    assert setup.execution.calls == ["recover", "authority", "reserve"]
    assert setup.journal.writes == 0


def test_expiry_while_definition_digest_is_off_loop_blocks_creation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup = context()
    entered, release = threading.Event(), threading.Event()
    original = AssignedJobDefinition.fingerprint

    def paused(definition: AssignedJobDefinition) -> str:
        entered.set()
        assert release.wait(3)
        return original(definition)

    monkeypatch.setattr(AssignedJobDefinition, "fingerprint", paused)

    async def scenario() -> None:
        first = asyncio.create_task(setup.service.execute(setup.execution.delivery))
        assert await asyncio.to_thread(entered.wait, 3)
        setup.execution.guard.stopped = True
        release.set()
        with pytest.raises(LeaseDeadlineRejected):
            await first

    asyncio.run(scenario())

    assert setup.workspace.requests == 0


def test_real_h4_clock_expiry_during_creation_rejects_late_directory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup, clock = context(), Clock()
    heartbeat = execution_context()
    heartbeat.delivery = setup.execution.delivery

    async def scenario() -> None:
        async with live_supervisor(heartbeat, clock, monkeypatch) as supervisor:
            service = ReserveAssignedWorkspace(
                setup.journal, supervisor, setup.workspace
            )
            assert supervisor.deadline is not None
            stop_at = supervisor.deadline.stop_at
            setup.workspace.after_create = lambda: setattr(clock, "value", stop_at)
            with pytest.raises(LeaseDeadlineRejected):
                await service.execute(setup.execution.delivery)
            assert supervisor.stop_required

    asyncio.run(scenario())

    assert setup.workspace.created
    assert setup.workspace.requests == 1
