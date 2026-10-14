import asyncio
import threading
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import Any, cast
from uuid import UUID

import pytest

import devrelay.application.worker_workspaces as workspace_consumers
from devrelay.application.contracts.assignment_delivery import (
    AssignedJobDefinition,
    AssignmentDelivery,
)
from devrelay.application.contracts.claims import ClaimReplayConflict
from devrelay.application.contracts.worker_recovery import (
    AssignmentReference,
    WorkerRecoveryState,
)
from devrelay.application.contracts.workspace_recovery import (
    WorkerWorkspaceCompletion,
    WorkerWorkspaceIntent,
    WorkspaceLocation,
    WorkspacePreparationDecision,
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
        self.preparation_writes = 0
        self.completion_writes = 0
        self.after_preparation: Callable[[], None] = lambda: None
        self.after_completion: Callable[[], None] = lambda: None
        self.preparation_entered: asyncio.Event | None = None
        self.preparation_release: asyncio.Event | None = None
        self.completion_entered: asyncio.Event | None = None
        self.completion_release: asyncio.Event | None = None
        self.preparation_error: Exception | None = None
        self.completion_error: Exception | None = None
        self.decision_override: Callable[[WorkspacePreparationDecision], Any] = (
            lambda value: value
        )
        self.completion_override: Callable[[WorkerWorkspaceCompletion], Any] = (
            lambda value: value
        )

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

    async def get_or_create_workspace(
        self, request: Any, candidate: WorkerWorkspaceIntent
    ) -> WorkspacePreparationDecision:
        self.calls.append("input")
        self.preparation_writes += 1
        assert request == self.state.request
        if self.preparation_error is not None:
            raise self.preparation_error
        existing = self.state.workspace_intent
        if existing is None:
            self.state = replace(self.state, workspace_intent=candidate)
            decision = WorkspacePreparationDecision(intent=candidate, created=True)
        else:
            existing.require_same_input(candidate)
            decision = WorkspacePreparationDecision(intent=existing, created=False)
        self.after_preparation()
        if self.preparation_entered is not None:
            self.preparation_entered.set()
        if self.preparation_release is not None:
            await self.preparation_release.wait()
        return cast(WorkspacePreparationDecision, self.decision_override(decision))

    async def record_workspace_completion(
        self, request: Any, completion: WorkerWorkspaceCompletion
    ) -> WorkerWorkspaceCompletion:
        self.calls.append("complete")
        self.completion_writes += 1
        assert request == self.state.request
        assert self.state.workspace_intent == completion.intent
        if self.completion_error is not None:
            raise self.completion_error
        if self.state.workspace_completion is None:
            self.state = replace(self.state, workspace_completion=completion)
        else:
            assert self.state.workspace_completion == completion
        self.after_completion()
        if self.completion_entered is not None:
            self.completion_entered.set()
        if self.completion_release is not None:
            await self.completion_release.wait()
        return cast(WorkerWorkspaceCompletion, self.completion_override(completion))


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
        self.preparations = 0
        self.location = WorkspaceLocation(
            root_directory=r"C:\fixture",
            policy_digest="c" * 64,
            identity=WorkspaceDirectoryIdentity(volume_serial_number=10, file_index=15),
        )
        self.prepared: Any = self.location
        self.prepare_error: Exception | None = None
        self.after_prepare: Callable[[], None] = lambda: None

    async def prepare(self) -> WorkspaceLocation:
        self.calls.append("prepare")
        self.preparations += 1
        if self.prepare_error is not None:
            raise self.prepare_error
        self.after_prepare()
        return cast(WorkspaceLocation, self.prepared)

    async def reserve(self, location: WorkspaceLocation) -> WorkspaceReservation:
        self.calls.append("reserve")
        self.requests += 1
        assert location == self.location
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
    assert [call for call in setup.execution.calls if call != "authority"] == [
        "recover",
        "prepare",
        "input",
        "reserve",
        "complete",
    ]
    assert setup.workspace.requests == 1
    assert setup.journal.state.execution is original.execution
    assert setup.journal.state.workspace_intent is not None
    assert setup.journal.state.workspace_completion.identity == reservation.identity
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

    assert setup.execution.calls == ["authority"]
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

    assert [call for call in setup.execution.calls if call != "authority"] == [
        "recover"
    ]
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
    assert [call for call in setup.execution.calls if call != "authority"] == [
        "recover",
        "prepare",
        "input",
        "reserve",
    ]
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


def pending_intent(setup: Context) -> WorkerWorkspaceIntent:
    return WorkerWorkspaceIntent(
        workspace_request_id=UUID(int=901),
        execution=setup.journal.state.execution,
        location=setup.workspace.location,
    )


@pytest.mark.parametrize("history", ["pending", "completed", "unknown"])
def test_historical_workspace_input_blocks_even_native_preparation(
    history: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup = context()
    if history == "unknown":
        setup.journal.state = replace(
            setup.journal.state, workspace_history_unknown=True
        )
    else:
        intent = pending_intent(setup)
        completion = (
            WorkerWorkspaceCompletion(
                intent=intent, identity=setup.workspace.reservation.identity
            )
            if history == "completed"
            else None
        )
        setup.journal.state = replace(
            setup.journal.state,
            workspace_intent=intent,
            workspace_completion=completion,
        )

    def forbidden_nonce() -> UUID:
        raise AssertionError(
            "Historical state cannot generate a new preparation nonce."
        )

    monkeypatch.setattr(workspace_consumers, "uuid4", forbidden_nonce)

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert setup.workspace.preparations == setup.workspace.requests == 0
    assert setup.journal.preparation_writes == setup.journal.completion_writes == 0


@pytest.mark.parametrize("invalid", [1, None, "false"])
def test_untyped_history_flag_cannot_authorize_creation(invalid: Any) -> None:
    setup = context()
    object.__setattr__(setup.journal.state, "workspace_history_unknown", invalid)

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert setup.workspace.preparations == 0


def test_history_unknown_with_retained_input_is_corrupt_and_never_creates() -> None:
    setup = context()
    setup.journal.state = replace(
        setup.journal.state, workspace_intent=pending_intent(setup)
    )
    object.__setattr__(setup.journal.state, "workspace_history_unknown", True)

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert setup.workspace.preparations == 0


@pytest.mark.parametrize("observed", [None, True, {"root_directory": r"C:\foreign"}])
def test_malformed_root_observation_prevents_durable_admission(observed: Any) -> None:
    setup = context()
    setup.workspace.prepared = observed

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert setup.journal.preparation_writes == 0
    assert setup.workspace.requests == 0


def test_stop_during_root_preparation_does_not_commit_creation_input() -> None:
    setup = context()
    setup.workspace.after_prepare = lambda: setattr(
        setup.execution.guard, "stopped", True
    )

    with pytest.raises(LeaseDeadlineRejected):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert setup.workspace.preparations == 1
    assert setup.journal.preparation_writes == 0
    assert setup.workspace.requests == 0


def test_preparation_commit_failure_cannot_call_native_creation() -> None:
    setup = context()
    setup.journal.preparation_error = WorkerStateUnavailable(
        "Protected commit unavailable."
    )

    with pytest.raises(WorkerStateUnavailable):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert setup.journal.state.workspace_intent is None
    assert setup.workspace.requests == 0


def test_stop_after_preparation_commit_retains_pending_without_creation() -> None:
    setup = context()
    setup.journal.after_preparation = lambda: setattr(
        setup.execution.guard, "stopped", True
    )

    with pytest.raises(LeaseDeadlineRejected):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert setup.journal.state.workspace_intent is not None
    assert setup.journal.state.workspace_completion is None
    assert setup.workspace.requests == 0


@pytest.mark.parametrize("created", [False, 1, None])
def test_only_exact_new_commit_decision_can_admit_creation(created: Any) -> None:
    setup = context()

    def altered(decision: WorkspacePreparationDecision) -> WorkspacePreparationDecision:
        object.__setattr__(decision, "created", created)
        return decision

    setup.journal.decision_override = altered

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert setup.journal.state.workspace_intent is not None
    assert setup.workspace.requests == 0


@pytest.mark.parametrize("field", ["workspace_request_id", "location", "execution"])
def test_new_decision_must_return_exact_original_candidate(field: str) -> None:
    setup = context()

    def altered(decision: WorkspacePreparationDecision) -> WorkspacePreparationDecision:
        intent = decision.intent
        if field == "workspace_request_id":
            intent = replace(intent, workspace_request_id=UUID(int=990))
        elif field == "location":
            intent = replace(
                intent, location=replace(intent.location, root_directory=r"C:\foreign")
            )
        else:
            intent = replace(
                intent, execution=replace(intent.execution, execution_id=UUID(int=991))
            )
        return replace(decision, intent=intent)

    setup.journal.decision_override = altered

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert setup.workspace.requests == 0


def test_untyped_commit_decision_cannot_admit_creation() -> None:
    setup = context()
    setup.journal.decision_override = lambda value: {"created": True}

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert setup.workspace.requests == 0


def test_completion_commit_precedes_final_authority_check_even_after_stop() -> None:
    setup = context()
    setup.workspace.after_create = lambda: setattr(
        setup.execution.guard, "stopped", True
    )

    with pytest.raises(LeaseDeadlineRejected):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert setup.journal.state.workspace_completion is not None
    assert (
        setup.journal.state.workspace_completion.identity
        == setup.workspace.reservation.identity
    )
    assert setup.execution.calls[-2:] == ["complete", "authority"]


def test_completion_commit_failure_retains_pending_and_native_directory() -> None:
    setup = context()
    setup.journal.completion_error = WorkerStateUnavailable(
        "Historical commit unavailable."
    )

    with pytest.raises(WorkerStateUnavailable):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert setup.workspace.created
    assert setup.journal.state.workspace_intent is not None
    assert setup.journal.state.workspace_completion is None


def test_stop_after_completion_commit_retains_exact_receipt() -> None:
    setup = context()
    setup.journal.after_completion = lambda: setattr(
        setup.execution.guard, "stopped", True
    )

    with pytest.raises(LeaseDeadlineRejected):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert (
        setup.journal.state.workspace_completion.intent
        is setup.journal.state.workspace_intent
    )
    assert setup.workspace.requests == 1


@pytest.mark.parametrize("returned", [None, True, {"identity": "foreign"}])
def test_malformed_completion_reply_never_returns_live_success(returned: Any) -> None:
    setup = context()
    setup.journal.completion_override = lambda completion: returned

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert setup.journal.state.workspace_completion is not None


def test_changed_child_identity_in_completion_reply_cannot_return_success() -> None:
    setup = context()
    setup.journal.completion_override = lambda completion: replace(
        completion, identity=replace(completion.identity, file_index=21)
    )

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert setup.journal.state.workspace_completion.identity.file_index == 20


@pytest.mark.parametrize("field", ["workspace_request_id", "location"])
def test_completion_reply_cannot_substitute_preparation_or_root(field: str) -> None:
    setup = context()

    def changed(completion: WorkerWorkspaceCompletion) -> WorkerWorkspaceCompletion:
        intent = completion.intent
        if field == "workspace_request_id":
            intent = replace(intent, workspace_request_id=UUID(int=995))
        else:
            intent = replace(
                intent, location=replace(intent.location, root_directory=r"C:\foreign")
            )
        return replace(completion, intent=intent)

    setup.journal.completion_override = changed

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert (
        setup.journal.state.workspace_completion.intent
        is setup.journal.state.workspace_intent
    )


def test_supplier_replacement_after_input_commit_cannot_create_foreign_directory() -> (
    None
):
    setup, foreign = context(), context()
    setup.journal.after_preparation = lambda: setattr(
        setup.workspace, "binding", foreign.workspace.binding
    )

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert setup.journal.state.workspace_intent is not None
    assert setup.workspace.requests == foreign.workspace.requests == 0


def test_supplier_replacement_after_creation_does_not_record_foreign_completion() -> (
    None
):
    setup, foreign = context(), context()
    setup.workspace.after_create = lambda: setattr(
        setup.workspace, "binding", foreign.workspace.binding
    )

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.execution.delivery))

    assert setup.workspace.created
    assert setup.journal.completion_writes == 0


@pytest.mark.parametrize("phase", ["input", "completion"])
def test_cancelled_commits_retain_exact_input_without_recreation(phase: str) -> None:
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
        first = asyncio.create_task(setup.service.execute(setup.execution.delivery))
        await entered.wait()
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        release.set()
        with pytest.raises(WorkspaceUnavailable):
            await setup.service.execute(setup.execution.delivery)

    asyncio.run(scenario())

    assert setup.journal.state.workspace_intent is not None
    assert (setup.journal.state.workspace_completion is not None) == (
        phase == "completion"
    )
    assert setup.workspace.requests == (1 if phase == "completion" else 0)


@pytest.mark.parametrize("phase", ["native", "completion"])
def test_caller_timeout_preserves_pending_or_completed_evidence(phase: str) -> None:
    setup = context()

    async def scenario() -> None:
        entered, release = asyncio.Event(), asyncio.Event()
        if phase == "native":
            setup.workspace.entered, setup.workspace.release = entered, release
        else:
            setup.journal.completion_entered, setup.journal.completion_release = (
                entered,
                release,
            )
        task = asyncio.create_task(setup.service.execute(setup.execution.delivery))
        await entered.wait()
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(task, timeout=0)
        release.set()
        with pytest.raises(WorkspaceUnavailable):
            await setup.service.execute(setup.execution.delivery)

    asyncio.run(scenario())

    assert setup.workspace.created
    assert setup.workspace.requests == 1
    assert setup.journal.state.workspace_intent is not None
    assert (setup.journal.state.workspace_completion is not None) == (
        phase == "completion"
    )


@pytest.mark.parametrize("different_root", [False, True])
def test_atomic_concurrent_commit_admits_only_one_native_creation(
    different_root: bool,
) -> None:
    setup = context()
    other = Workspace(setup.workspace.binding, setup.execution.calls)
    if different_root:
        other.location = replace(other.location, root_directory=r"C:\other")
        other.prepared = other.location
    initial_state = setup.journal.state

    class RacingJournal(Journal):
        def __init__(self) -> None:
            super().__init__(initial_state, setup.execution.calls)
            self.loaded = 0
            self.both_loaded = asyncio.Event()

        async def load_state(self) -> WorkerRecoveryState:
            self.loaded += 1
            if self.loaded == 2:
                self.both_loaded.set()
            await self.both_loaded.wait()
            return cast(WorkerRecoveryState, initial_state)

    async def scenario() -> RacingJournal:
        journal = RacingJournal()
        consumers = [
            ReserveAssignedWorkspace(journal, setup.execution.guard, workspace)
            for workspace in (setup.workspace, other)
        ]
        results = await asyncio.gather(
            *(consumer.execute(setup.execution.delivery) for consumer in consumers),
            return_exceptions=True,
        )
        assert sum(isinstance(result, WorkspaceReservation) for result in results) == 1
        rejected = [result for result in results if isinstance(result, Exception)]
        assert len(rejected) == 1
        assert isinstance(
            rejected[0], ClaimReplayConflict if different_root else WorkspaceUnavailable
        )
        return journal

    journal = asyncio.run(scenario())

    assert setup.workspace.requests + other.requests == 1
    assert journal.preparation_writes == 2
    assert journal.completion_writes == 1
