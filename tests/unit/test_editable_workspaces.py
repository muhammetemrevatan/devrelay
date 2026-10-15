import asyncio
import threading
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any, cast

import pytest

import devrelay.application.editable_workspaces as handoffs
import devrelay.application.worker_workspaces as workspace_consumers
from devrelay.application.contracts.assignment_delivery import AssignmentDelivery
from devrelay.application.contracts.base_sources import (
    BaseMaterialization,
    BaseMaterializationPolicy,
)
from devrelay.application.contracts.claims import ClaimReplayConflict
from devrelay.application.contracts.editable_workspaces import EditableWorkspace
from devrelay.application.editable_workspaces import HandoffMaterializedBase
from devrelay.application.lease_deadlines import LeaseDeadlineRejected
from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from tests.unit.test_materialize_base import Journal
from tests.unit.test_materialize_base import context as materialization_context
from tests.unit.test_worker_execution_authority import Clock, live_supervisor
from tests.unit.test_worker_execution_preparation import context as execution_context
from tests.unit.test_worker_workspace_reservation import Guard


class Workspace:
    def __init__(
        self, original: Any, result: BaseMaterialization, calls: list[str]
    ) -> None:
        self.binding: Any = original.binding
        self.location: Any = original.location
        self.reservation: Any = original.reservation
        self.materialization: Any = result
        self.calls = calls
        self.seals = 0
        self.mutable = False
        self.error: Exception | None = None
        self.after_handoff: Callable[[], None] = lambda: None
        self.result_override: Callable[[EditableWorkspace], Any] = lambda result: result
        self.entered: asyncio.Event | None = None
        self.release: asyncio.Event | None = None
        self.cancellation_seals: list[int] = []

    def seal(self) -> None:
        self.calls.append("seal")
        self.seals += 1

    async def handoff(self, result: BaseMaterialization) -> EditableWorkspace:
        self.calls.append("handoff")
        assert result is self.materialization
        if self.entered is not None:
            self.entered.set()
        if self.release is not None:
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                self.cancellation_seals.append(self.seals)
                raise
        self.mutable = True
        if self.error is not None:
            raise self.error
        self.after_handoff()
        returned = EditableWorkspace(materialization=result, location=self.location)
        return cast(EditableWorkspace, self.result_override(returned))


@dataclass
class Context:
    service: HandoffMaterializedBase
    workspace: Workspace
    journal: Journal
    guard: Guard
    delivery: AssignmentDelivery
    materialization: BaseMaterialization
    calls: list[str]


def context(policy: BaseMaterializationPolicy | None = None) -> Context:
    original = materialization_context()
    snapshot = original.snapshot
    result = BaseMaterialization(
        reservation=original.reservation,
        source=snapshot.request,
        root_tree_sha=snapshot.root_tree_sha,
        manifest_digest=snapshot.fingerprint(),
        file_count=len(snapshot.files),
        total_bytes=sum(len(item.content) for item in snapshot.files),
    )
    workspace = Workspace(original.workspace, result, original.calls)
    service = HandoffMaterializedBase(
        original.journal,
        original.guard,
        workspace,  # type: ignore[arg-type]
        policy or BaseMaterializationPolicy(),
    )
    return Context(
        service,
        workspace,
        original.journal,
        original.guard,
        original.delivery,
        result,
        original.calls,
    )


def actions(setup: Context) -> list[str]:
    return [call for call in setup.calls if call != "authority"]


def test_handoff_returns_editable_original_without_journal_mutation_or_launch() -> None:
    setup = context()
    state = setup.journal.state

    result = asyncio.run(setup.service.execute(setup.delivery, setup.materialization))

    assert result.materialization is setup.materialization
    assert result.location == setup.workspace.location
    assert setup.workspace.mutable
    assert setup.workspace.seals == 0
    assert setup.journal.state is state and setup.journal.writes == 0
    assert actions(setup) == ["journal", "handoff"]
    assert state.request.proof.reveal() not in repr(result)


def test_equal_reconstructed_materialization_is_not_live_capability() -> None:
    setup = context()
    copied = replace(setup.materialization)
    assert copied == setup.materialization

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.delivery, copied))

    assert actions(setup) == ["seal"]
    assert not setup.workspace.mutable


def test_equal_reconstructed_reservation_cannot_replace_live_reservation() -> None:
    setup = context()
    setup.workspace.reservation = replace(setup.materialization.reservation)

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.delivery, setup.materialization))

    assert actions(setup) == ["seal"]


@pytest.mark.parametrize("history", ["pending", "missing", "unknown"])
def test_historical_or_incomplete_journal_cannot_handoff(history: str) -> None:
    setup = context()
    state = setup.journal.state
    setup.journal.state = replace(
        state,
        workspace_intent=state.workspace_intent if history == "pending" else None,
        workspace_completion=None,
        workspace_history_unknown=history == "unknown",
    )

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.delivery, setup.materialization))

    assert actions(setup) == ["journal", "seal"]
    assert not setup.workspace.mutable and setup.journal.writes == 0


@pytest.mark.parametrize("field", ["root_directory", "policy_digest", "identity"])
def test_changed_live_root_is_rejected_against_durable_original(field: str) -> None:
    setup = context()
    location = setup.workspace.location
    values = {
        "root_directory": r"C:\other",
        "policy_digest": "b" * 64,
        "identity": replace(location.identity, file_index=99),
    }
    setup.workspace.location = replace(location, **{field: values[field]})

    with pytest.raises(ClaimReplayConflict):
        asyncio.run(setup.service.execute(setup.delivery, setup.materialization))

    assert not setup.workspace.mutable


@pytest.mark.parametrize(
    "field,value",
    [
        ("provider_repository_id", 99),
        ("repository_slug", "other/repository"),
        ("base_revision", "f" * 40),
        ("authorized_branch", "agent/other"),
        ("policy_version", "other"),
    ],
)
def test_changed_complete_definition_cannot_handoff(field: str, value: Any) -> None:
    setup = context()
    delivery = replace(
        setup.delivery, definition=replace(setup.delivery.definition, **{field: value})
    )

    with pytest.raises(ClaimReplayConflict):
        asyncio.run(setup.service.execute(delivery, setup.materialization))

    assert not setup.workspace.mutable and setup.workspace.seals == 1


def test_different_completed_child_cannot_handoff() -> None:
    setup = context()
    state = setup.journal.state
    setup.journal.state = replace(
        state,
        workspace_completion=replace(
            state.workspace_completion,
            identity=replace(setup.materialization.reservation.identity, file_index=99),
        ),
    )

    with pytest.raises(ClaimReplayConflict):
        asyncio.run(setup.service.execute(setup.delivery, setup.materialization))

    assert not setup.workspace.mutable


def test_initial_authority_loss_prevents_journal_and_native_release() -> None:
    setup = context()
    setup.guard.stopped = True

    with pytest.raises(LeaseDeadlineRejected):
        asyncio.run(setup.service.execute(setup.delivery, setup.materialization))

    assert actions(setup) == ["seal"]


@pytest.mark.parametrize(
    "anchor", ["request", "assignment", "acknowledgement", "completion", "execution"]
)
def test_missing_original_acknowledged_execution_never_releases_handles(
    anchor: str,
) -> None:
    setup = context()
    object.__setattr__(setup.journal.state, anchor, None)

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.delivery, setup.materialization))

    assert actions(setup) == ["journal", "seal"]
    assert not setup.workspace.mutable


def test_late_authority_loss_rejects_already_mutable_scope_without_retry() -> None:
    setup = context()
    setup.workspace.after_handoff = lambda: setattr(setup.guard, "stopped", True)

    with pytest.raises(LeaseDeadlineRejected):
        asyncio.run(setup.service.execute(setup.delivery, setup.materialization))
    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.delivery, setup.materialization))

    assert setup.workspace.mutable and setup.workspace.seals == 1
    assert actions(setup).count("handoff") == 1 and setup.journal.writes == 0


@pytest.mark.parametrize("returned", [None, True, {}])
def test_untyped_handoff_result_is_never_accepted(returned: Any) -> None:
    setup = context()
    setup.workspace.result_override = lambda result: returned

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.delivery, setup.materialization))

    assert setup.workspace.mutable and setup.workspace.seals == 1


def test_equal_reconstructed_result_materialization_is_rejected() -> None:
    setup = context()
    setup.workspace.result_override = lambda result: replace(
        result, materialization=replace(result.materialization)
    )

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.delivery, setup.materialization))

    assert setup.workspace.seals == 1


def test_changed_result_location_is_rejected() -> None:
    setup = context()
    setup.workspace.result_override = lambda result: replace(
        result, location=replace(result.location, policy_digest="a" * 64)
    )

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.delivery, setup.materialization))

    assert setup.workspace.seals == 1


def test_native_partial_release_failure_preserves_error_and_blocks_retry() -> None:
    setup = context()
    setup.workspace.error = WorkspaceUnavailable("Partial handle release failed.")

    with pytest.raises(WorkspaceUnavailable, match="Partial handle"):
        asyncio.run(setup.service.execute(setup.delivery, setup.materialization))
    with pytest.raises(WorkspaceUnavailable, match="already"):
        asyncio.run(setup.service.execute(setup.delivery, setup.materialization))

    assert setup.workspace.mutable and setup.workspace.seals == 1
    assert actions(setup).count("handoff") == 1


def test_success_is_one_shot_and_cannot_handoff_twice() -> None:
    setup = context()

    async def scenario() -> None:
        await setup.service.execute(setup.delivery, setup.materialization)
        with pytest.raises(WorkspaceUnavailable):
            await setup.service.execute(setup.delivery, setup.materialization)

    asyncio.run(scenario())

    assert actions(setup).count("handoff") == 1 and setup.workspace.seals == 0


def test_concurrent_call_cannot_release_handles_twice_or_seal_active_handoff() -> None:
    setup = context()

    async def scenario() -> None:
        setup.workspace.entered, setup.workspace.release = (
            asyncio.Event(),
            asyncio.Event(),
        )
        first = asyncio.create_task(
            setup.service.execute(setup.delivery, setup.materialization)
        )
        await setup.workspace.entered.wait()
        with pytest.raises(WorkspaceUnavailable):
            await setup.service.execute(setup.delivery, setup.materialization)
        assert setup.workspace.seals == 0
        setup.workspace.release.set()
        await first

    asyncio.run(scenario())

    assert setup.workspace.mutable and actions(setup).count("handoff") == 1


def test_cancellation_seals_original_before_cancelling_pending_handoff() -> None:
    setup = context()

    async def scenario() -> None:
        setup.workspace.entered, setup.workspace.release = (
            asyncio.Event(),
            asyncio.Event(),
        )
        pending = asyncio.create_task(
            setup.service.execute(setup.delivery, setup.materialization)
        )
        await setup.workspace.entered.wait()
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        await asyncio.sleep(0)

    asyncio.run(scenario())

    assert setup.workspace.seals == 1
    assert setup.workspace.cancellation_seals == [1]
    assert setup.journal.writes == 0


def test_timeout_seals_before_cancelling_waiting_native_wrapper() -> None:
    setup = context(BaseMaterializationPolicy(operation_timeout_seconds=0.05))

    async def scenario() -> None:
        setup.workspace.release = asyncio.Event()
        with pytest.raises(WorkspaceUnavailable, match="wait budget"):
            await setup.service.execute(setup.delivery, setup.materialization)
        await asyncio.sleep(0)

    asyncio.run(scenario())

    assert setup.workspace.cancellation_seals == [1]
    assert setup.workspace.seals == 1 and not setup.workspace.mutable


def test_binding_swap_during_handoff_seals_captured_original_scope() -> None:
    setup, foreign = context(), context()

    def swap() -> None:
        setup.workspace.binding = foreign.workspace.binding
        setup.workspace.seal = foreign.workspace.seal  # type: ignore[method-assign]

    setup.workspace.after_handoff = swap

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.delivery, setup.materialization))

    assert setup.workspace.seals == 1 and foreign.workspace.seals == 0


def test_journal_validation_and_fingerprints_stay_off_event_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup = context()
    original = workspace_consumers._validate_original_input
    loop_thread = threading.get_ident()
    observed: list[int] = []

    def validate(*args: Any) -> None:
        observed.append(threading.get_ident())
        original(*args)

    monkeypatch.setattr(handoffs, "_validate_original_input", validate)

    asyncio.run(setup.service.execute(setup.delivery, setup.materialization))

    assert observed and all(identity != loop_thread for identity in observed)


def test_actual_h4_guard_checks_its_clock_and_rejects_late_handoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup, execution, clock = context(), execution_context(), Clock()
    execution.delivery = setup.delivery

    async def scenario() -> None:
        async with live_supervisor(execution, clock, monkeypatch) as supervisor:
            service = HandoffMaterializedBase(
                setup.journal,
                supervisor,
                setup.workspace,  # type: ignore[arg-type]
            )
            setup.workspace.after_handoff = lambda: setattr(clock, "value", 10000)
            with pytest.raises(LeaseDeadlineRejected):
                await service.execute(setup.delivery, setup.materialization)

    asyncio.run(scenario())

    assert setup.workspace.mutable and setup.workspace.seals == 1


def test_actual_h4_guard_stops_pending_release_at_its_suspend_inclusive_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup, execution, clock = context(), execution_context(), Clock()
    execution.delivery = setup.delivery

    async def scenario() -> None:
        async with live_supervisor(execution, clock, monkeypatch) as supervisor:
            service = HandoffMaterializedBase(
                setup.journal,
                supervisor,
                setup.workspace,  # type: ignore[arg-type]
            )
            setup.workspace.entered = asyncio.Event()
            setup.workspace.release = asyncio.Event()
            pending = asyncio.create_task(
                service.execute(setup.delivery, setup.materialization)
            )
            await setup.workspace.entered.wait()
            assert supervisor.deadline is not None
            clock.value = supervisor.deadline.stop_at
            with pytest.raises(LeaseDeadlineRejected):
                await pending
            await asyncio.sleep(0)
            assert supervisor.stop_required

    asyncio.run(scenario())

    assert not setup.workspace.mutable
    assert setup.workspace.cancellation_seals == [1]
    assert setup.workspace.seals == 1 and setup.journal.writes == 0


@pytest.mark.parametrize("scope", ["materialization", "reservation", "location"])
def test_changed_scope_during_handoff_rejects_late_mutability(scope: str) -> None:
    setup = context()

    def change_scope() -> None:
        if scope == "location":
            setup.workspace.location = replace(
                setup.workspace.location, policy_digest="a" * 64
            )
        else:
            setattr(setup.workspace, scope, replace(getattr(setup.workspace, scope)))

    setup.workspace.after_handoff = change_scope

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.delivery, setup.materialization))

    assert setup.workspace.mutable and setup.workspace.seals == 1
