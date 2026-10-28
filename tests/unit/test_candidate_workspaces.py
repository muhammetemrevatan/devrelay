import asyncio
import threading
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any, cast

import pytest

import devrelay.application.candidate_workspaces as candidates
from devrelay.application.candidate_workspaces import PrepareCandidateCapture
from devrelay.application.contracts.assignment_delivery import AssignmentDelivery
from devrelay.application.contracts.base_sources import (
    BaseMaterialization,
    BaseMaterializationPolicy,
)
from devrelay.application.contracts.claims import ClaimReplayConflict
from devrelay.application.contracts.editable_workspaces import EditableWorkspace
from devrelay.application.contracts.git_sources import (
    GitBaseSourceSnapshot,
    GitWorkspacePreparation,
)
from devrelay.application.lease_deadlines import LeaseDeadlineRejected
from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from tests.unit.test_git_workspaces import Workspace as GitWorkspace
from tests.unit.test_git_workspaces import context as git_context
from tests.unit.test_materialize_base import Journal
from tests.unit.test_worker_execution_authority import Clock, live_supervisor
from tests.unit.test_worker_execution_preparation import context as execution_context
from tests.unit.test_worker_workspace_reservation import Guard


class Workspace(GitWorkspace):
    def __init__(self, original: GitWorkspace, calls: list[str]) -> None:
        super().__init__(original, original.materialization, calls)
        self.source_snapshot = original.source_snapshot
        self.git_preparation: Any = GitWorkspacePreparation(
            materialization=self.materialization,
            branch=self.binding.definition.authorized_branch,
            metadata_digest="d" * 64,
        )
        self.editable: Any = None
        self.candidate_capture_preparation: Any = None
        self.after_prepare: Callable[[], None] = lambda: None
        self.candidate_result_override: Callable[[GitWorkspacePreparation], Any] = (
            lambda x: x
        )
        self.prepare_error: Exception | None = None
        self.entered: asyncio.Event | None = None
        self.release: asyncio.Event | None = None
        self.cancellation_seals: list[int] = []
        self.received_source: GitBaseSourceSnapshot | None = None

    async def prepare_candidate_capture(
        self, source: GitBaseSourceSnapshot
    ) -> GitWorkspacePreparation:
        self.calls.append("candidate")
        self.received_source = source
        source.validate()
        if self.entered is not None:
            self.entered.set()
        if self.release is not None:
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                self.cancellation_seals.append(self.seals)
                raise
        if self.prepare_error is not None:
            raise self.prepare_error
        preparation = cast(GitWorkspacePreparation, self.git_preparation)
        self.candidate_capture_preparation = preparation
        self.after_prepare()
        return cast(
            GitWorkspacePreparation, self.candidate_result_override(preparation)
        )


@dataclass
class Context:
    service: PrepareCandidateCapture
    workspace: Workspace
    journal: Journal
    guard: Guard
    delivery: AssignmentDelivery
    materialization: BaseMaterialization
    source: GitBaseSourceSnapshot
    calls: list[str]


def context(policy: BaseMaterializationPolicy | None = None) -> Context:
    original = git_context()
    workspace = Workspace(original.workspace, original.calls)
    guard = Guard(workspace.binding.execution.reference, original.calls)
    service = PrepareCandidateCapture(
        original.journal,
        guard,
        workspace,  # type: ignore[arg-type]
        policy or BaseMaterializationPolicy(),
    )
    return Context(
        service,
        workspace,
        original.journal,
        guard,
        original.delivery,
        original.materialization,
        original.source,
        original.calls,
    )


def actions(setup: Context) -> list[str]:
    return [call for call in setup.calls if call != "authority"]


def test_candidate_preparation_retains_original_git_source_without_handoff() -> None:
    setup = context()
    state, preparation = setup.journal.state, setup.workspace.git_preparation

    result = asyncio.run(setup.service.execute(setup.delivery, setup.materialization))

    assert result is preparation
    assert setup.workspace.candidate_capture_preparation is preparation
    assert setup.workspace.editable is None and not setup.workspace.mutable
    assert setup.workspace.received_source is not None
    assert setup.workspace.received_source.snapshot is setup.source.snapshot
    assert setup.workspace.received_source.raw_commit == setup.source.raw_commit
    assert setup.journal.state is state and setup.journal.writes == 0
    assert actions(setup) == ["journal", "candidate"]
    assert setup.workspace.seals == 0


@pytest.mark.parametrize(
    "mutation",
    [
        "foreign_materialization",
        "foreign_reservation",
        "missing_location",
        "missing_git",
        "foreign_git_materialization",
        "foreign_git_branch",
        "missing_source",
        "literal_source",
        "already_editable",
        "already_prepared",
    ],
)
def test_protected_original_is_required_before_any_journal_or_native_call(
    mutation: str,
) -> None:
    setup = context()
    materialization = setup.materialization
    match mutation:
        case "foreign_materialization":
            materialization = replace(materialization)
        case "foreign_reservation":
            setup.workspace.reservation = replace(materialization.reservation)
        case "missing_location":
            setup.workspace.location = None
        case "missing_git":
            setup.workspace.git_preparation = None
        case "foreign_git_materialization":
            setup.workspace.git_preparation = replace(
                setup.workspace.git_preparation,
                materialization=replace(materialization),
            )
        case "foreign_git_branch":
            setup.workspace.git_preparation = replace(
                setup.workspace.git_preparation, branch="agent/foreign"
            )
        case "missing_source":
            setup.workspace.source_snapshot = None
        case "literal_source":
            setup.workspace.source_snapshot = replace(
                setup.source.snapshot, raw_commit=None
            )
        case "already_editable":
            setup.workspace.editable = EditableWorkspace(
                materialization=materialization, location=setup.workspace.location
            )
        case "already_prepared":
            setup.workspace.candidate_capture_preparation = (
                setup.workspace.git_preparation
            )

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.delivery, materialization))

    assert actions(setup) == ["seal"] and setup.journal.writes == 0


@pytest.mark.parametrize("history", ["missing", "pending", "unknown", "foreign_child"])
def test_unverified_workspace_history_cannot_retain_source(history: str) -> None:
    setup = context()
    state = setup.journal.state
    if history == "foreign_child":
        setup.journal.state = replace(
            state,
            workspace_completion=replace(
                state.workspace_completion,
                identity=replace(
                    setup.materialization.reservation.identity, file_index=99
                ),
            ),
        )
    else:
        setup.journal.state = replace(
            state,
            workspace_intent=state.workspace_intent if history == "pending" else None,
            workspace_completion=None,
            workspace_history_unknown=history == "unknown",
        )

    with pytest.raises((WorkspaceUnavailable, ClaimReplayConflict)):
        asyncio.run(setup.service.execute(setup.delivery, setup.materialization))

    assert actions(setup) == ["journal", "seal"]
    assert setup.workspace.received_source is None and setup.journal.writes == 0


@pytest.mark.parametrize(
    "anchor", ["request", "assignment", "acknowledgement", "completion", "execution"]
)
def test_missing_original_execution_anchor_blocks_candidate_opt_in(anchor: str) -> None:
    setup = context()
    object.__setattr__(setup.journal.state, anchor, None)

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.delivery, setup.materialization))

    assert actions(setup) == ["journal", "seal"]


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
def test_candidate_retention_requires_complete_original_definition(
    field: str, value: Any
) -> None:
    setup = context()
    delivery = replace(
        setup.delivery, definition=replace(setup.delivery.definition, **{field: value})
    )

    with pytest.raises(ClaimReplayConflict):
        asyncio.run(setup.service.execute(delivery, setup.materialization))

    assert actions(setup) == ["journal", "seal"]


@pytest.mark.parametrize("field", ["root_directory", "policy_digest", "identity"])
def test_durable_root_pins_are_independent_of_current_root_cache(field: str) -> None:
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

    assert setup.workspace.received_source is None and setup.workspace.seals == 1


@pytest.mark.parametrize("mutation", ["request", "commit", "manifest"])
def test_changed_source_is_rejected_before_native_retention(mutation: str) -> None:
    setup = context()
    snapshot = setup.source.snapshot
    if mutation == "request":
        snapshot = replace(
            snapshot, request=replace(snapshot.request, repository_slug="other/project")
        )
    elif mutation == "commit":
        snapshot = replace(snapshot)
        object.__setattr__(
            snapshot, "raw_commit", setup.source.raw_commit + b"changed\n"
        )
    else:
        setup.workspace.materialization = setup.materialization = replace(
            setup.materialization, manifest_digest="e" * 64
        )
        setup.workspace.git_preparation = replace(
            setup.workspace.git_preparation, materialization=setup.materialization
        )
    setup.workspace.source_snapshot = snapshot

    with pytest.raises((WorkspaceUnavailable, ValueError)):
        asyncio.run(setup.service.execute(setup.delivery, setup.materialization))

    assert setup.workspace.received_source is None
    assert setup.workspace.seals == 1 and setup.journal.writes == 0


@pytest.mark.parametrize("reply", [None, True, {}, "copy", "materialization"])
def test_only_exact_accepted_native_preparation_can_complete_opt_in(reply: Any) -> None:
    setup = context()

    def override(preparation: GitWorkspacePreparation) -> Any:
        if reply == "copy":
            return replace(preparation)
        if reply == "materialization":
            return replace(preparation, materialization=replace(setup.materialization))
        return reply

    setup.workspace.candidate_result_override = override

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.delivery, setup.materialization))

    assert actions(setup) == ["journal", "candidate", "seal"]
    assert setup.journal.writes == 0


@pytest.mark.parametrize("cache", ["preparation", "candidate", "editable", "binding"])
def test_native_callback_cannot_change_live_scope_before_acceptance(cache: str) -> None:
    setup, foreign = context(), context()

    def change() -> None:
        if cache == "preparation":
            setup.workspace.git_preparation = replace(setup.workspace.git_preparation)
        elif cache == "candidate":
            setup.workspace.candidate_capture_preparation = replace(
                setup.workspace.git_preparation
            )
        elif cache == "editable":
            setup.workspace.editable = EditableWorkspace(
                materialization=setup.materialization, location=setup.workspace.location
            )
        else:
            setup.workspace.binding = foreign.workspace.binding
            setup.workspace.seal = foreign.workspace.seal  # type: ignore[method-assign]

    setup.workspace.after_prepare = change

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.delivery, setup.materialization))

    assert setup.workspace.seals == 1 and foreign.workspace.seals == 0


def test_success_is_one_shot_without_sealing_prepared_scope() -> None:
    setup = context()

    async def exercise() -> None:
        await setup.service.execute(setup.delivery, setup.materialization)
        with pytest.raises(WorkspaceUnavailable, match="already started"):
            await setup.service.execute(setup.delivery, setup.materialization)

    asyncio.run(exercise())

    assert actions(setup) == ["journal", "candidate"] and setup.workspace.seals == 0


def test_concurrent_call_cannot_retain_twice_or_seal_pending_original() -> None:
    setup = context()

    async def exercise() -> None:
        setup.workspace.entered, setup.workspace.release = (
            asyncio.Event(),
            asyncio.Event(),
        )
        pending = asyncio.create_task(
            setup.service.execute(setup.delivery, setup.materialization)
        )
        await setup.workspace.entered.wait()
        with pytest.raises(WorkspaceUnavailable, match="already started"):
            await setup.service.execute(setup.delivery, setup.materialization)
        assert setup.workspace.seals == 0
        setup.workspace.release.set()
        await pending

    asyncio.run(exercise())

    assert actions(setup).count("candidate") == 1


def test_cancellation_seals_original_before_cancelling_native_wrapper() -> None:
    setup = context()

    async def exercise() -> None:
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
        with pytest.raises(WorkspaceUnavailable, match="already started"):
            await setup.service.execute(setup.delivery, setup.materialization)

    asyncio.run(exercise())

    assert setup.workspace.cancellation_seals == [1]
    assert setup.workspace.candidate_capture_preparation is None
    assert setup.journal.writes == 0


def test_timeout_retains_uncertain_native_resources_without_retry() -> None:
    setup = context(BaseMaterializationPolicy(operation_timeout_seconds=0.05))

    async def exercise() -> None:
        setup.workspace.release = asyncio.Event()
        with pytest.raises(WorkspaceUnavailable, match="wait budget"):
            await setup.service.execute(setup.delivery, setup.materialization)
        await asyncio.sleep(0)

    asyncio.run(exercise())

    assert setup.workspace.cancellation_seals == [1]
    assert setup.workspace.candidate_capture_preparation is None
    assert setup.journal.writes == 0


def test_native_failure_seals_and_preserves_original_error_without_retry() -> None:
    setup = context()
    setup.workspace.prepare_error = WorkspaceUnavailable("Partial native setup failed.")

    with pytest.raises(WorkspaceUnavailable, match="Partial native"):
        asyncio.run(setup.service.execute(setup.delivery, setup.materialization))
    with pytest.raises(WorkspaceUnavailable, match="already started"):
        asyncio.run(setup.service.execute(setup.delivery, setup.materialization))

    assert setup.workspace.seals == 1 and actions(setup).count("candidate") == 1


def test_initial_authority_loss_prevents_even_journal_disclosure() -> None:
    setup = context()
    setup.guard.stopped = True

    with pytest.raises(LeaseDeadlineRejected):
        asyncio.run(setup.service.execute(setup.delivery, setup.materialization))

    assert actions(setup) == ["seal"]


def test_late_authority_loss_rejects_native_side_effect_without_retry() -> None:
    setup = context()
    setup.workspace.after_prepare = lambda: setattr(setup.guard, "stopped", True)

    with pytest.raises(LeaseDeadlineRejected):
        asyncio.run(setup.service.execute(setup.delivery, setup.materialization))

    assert setup.workspace.candidate_capture_preparation is not None
    assert setup.workspace.seals == 1 and setup.journal.writes == 0


def test_source_validation_stays_off_event_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup = context()
    original = GitBaseSourceSnapshot
    loop_thread = threading.get_ident()
    observed: list[int] = []

    def construct(**kwargs: Any) -> GitBaseSourceSnapshot:
        observed.append(threading.get_ident())
        return original(**kwargs)

    monkeypatch.setattr(candidates, "GitBaseSourceSnapshot", construct)

    asyncio.run(setup.service.execute(setup.delivery, setup.materialization))

    assert observed and all(identity != loop_thread for identity in observed)


def test_actual_h4_clock_rejects_late_preparation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup, execution, clock = context(), execution_context(), Clock()
    execution.delivery = setup.delivery

    async def exercise() -> None:
        async with live_supervisor(execution, clock, monkeypatch) as supervisor:
            service = PrepareCandidateCapture(
                setup.journal,
                supervisor,
                setup.workspace,  # type: ignore[arg-type]
            )
            setup.workspace.after_prepare = lambda: setattr(clock, "value", 10000)
            with pytest.raises(LeaseDeadlineRejected):
                await service.execute(setup.delivery, setup.materialization)

    asyncio.run(exercise())

    assert setup.workspace.candidate_capture_preparation is not None
    assert setup.workspace.seals == 1


def test_actual_h4_suspend_deadline_interrupts_pending_preparation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup, execution, clock = context(), execution_context(), Clock()
    execution.delivery = setup.delivery

    async def exercise() -> None:
        async with live_supervisor(execution, clock, monkeypatch) as supervisor:
            service = PrepareCandidateCapture(
                setup.journal,
                supervisor,
                setup.workspace,  # type: ignore[arg-type]
            )
            setup.workspace.entered, setup.workspace.release = (
                asyncio.Event(),
                asyncio.Event(),
            )
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

    asyncio.run(exercise())

    assert setup.workspace.cancellation_seals == [1]
    assert setup.workspace.candidate_capture_preparation is None
    assert setup.journal.writes == 0
