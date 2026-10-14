import asyncio
import threading
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any, cast
from uuid import UUID

import pytest

import devrelay.application.materialize_base as materialization
from devrelay.application.contracts.assignment_delivery import AssignmentDelivery
from devrelay.application.contracts.base_sources import (
    BaseFileMode,
    BaseMaterialization,
    BaseMaterializationPolicy,
    BaseSourceFile,
    BaseSourceRequest,
    BaseSourceSnapshot,
    git_blob_sha,
)
from devrelay.application.contracts.claims import ClaimReplayConflict
from devrelay.application.contracts.worker_recovery import (
    WorkerRecoveryPhase,
    WorkerRecoveryState,
)
from devrelay.application.contracts.workspace_recovery import (
    WorkerWorkspaceCompletion,
    WorkerWorkspaceIntent,
    WorkspaceDirectoryIdentity,
    WorkspaceLocation,
)
from devrelay.application.contracts.workspaces import (
    WorkspaceBinding,
    WorkspaceReservation,
)
from devrelay.application.lease_deadlines import LeaseDeadlineRejected
from devrelay.application.materialize_base import MaterializeAssignedBase
from devrelay.application.ports.base_source import (
    BaseSourceUnavailable,
    InvalidBaseSourceData,
)
from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from tests.unit.test_worker_execution_authority import Clock, live_supervisor
from tests.unit.test_worker_execution_preparation import context as execution_context
from tests.unit.test_worker_workspace_reservation import Guard
from tests.workspace_fixtures import workspace_inputs


class Journal:
    def __init__(self, state: WorkerRecoveryState, calls: list[str]) -> None:
        self.state: Any = state
        self.calls = calls
        self.writes = 0

    async def load_state(self) -> WorkerRecoveryState:
        self.calls.append("journal")
        return cast(WorkerRecoveryState, self.state)

    async def get_or_create_workspace(self, request: Any, candidate: Any) -> Any:
        self.writes += 1
        raise AssertionError("Base export cannot mutate protected preparation.")

    async def record_workspace_completion(self, request: Any, completion: Any) -> Any:
        self.writes += 1
        raise AssertionError("Base export cannot mutate protected completion.")


class Source:
    def __init__(self, snapshot: BaseSourceSnapshot, calls: list[str]) -> None:
        self.snapshot: Any = snapshot
        self.calls = calls
        self.request: BaseSourceRequest | None = None
        self.error: Exception | None = None
        self.after_read: Callable[[], None] = lambda: None
        self.entered: asyncio.Event | None = None
        self.release: asyncio.Future[BaseSourceSnapshot] | None = None

    async def read(self, request: BaseSourceRequest) -> BaseSourceSnapshot:
        self.calls.append("source")
        self.request = request
        if self.error is not None:
            raise self.error
        if self.entered is not None:
            self.entered.set()
        if self.release is not None:
            return await asyncio.shield(self.release)
        self.after_read()
        return cast(BaseSourceSnapshot, self.snapshot)


class Workspace:
    def __init__(self, binding: WorkspaceBinding, calls: list[str]) -> None:
        self.binding: Any = binding
        self.calls = calls
        self.location: Any = WorkspaceLocation(
            root_directory=r"C:\fixture",
            policy_digest="c" * 64,
            identity=WorkspaceDirectoryIdentity(volume_serial_number=10, file_index=15),
        )
        self.reservation: Any = WorkspaceReservation(
            binding=binding,
            directory_name=str(binding.execution.intent.execution_id),
            identity=WorkspaceDirectoryIdentity(volume_serial_number=10, file_index=20),
        )
        self.seals = 0
        self.sealed = False
        self.verify_error: Exception | None = None
        self.verify_returned: Any = None
        self.after_verify: Callable[[], None] = lambda: None
        self.after_write: Callable[[], None] = lambda: None
        self.write_error: Exception | None = None
        self.result_override: Callable[[BaseMaterialization], Any] = lambda value: value
        self.entered: asyncio.Event | None = None
        self.release: asyncio.Future[BaseMaterialization] | None = None
        self.written: list[str] = []

    def seal(self) -> None:
        self.calls.append("seal")
        self.seals += 1
        self.sealed = True

    async def verify(self, reservation: WorkspaceReservation) -> None:
        self.calls.append("verify")
        assert reservation is self.reservation
        if self.verify_error is not None:
            raise self.verify_error
        self.after_verify()
        return cast(None, self.verify_returned)

    async def materialize(
        self, reservation: WorkspaceReservation, snapshot: BaseSourceSnapshot
    ) -> BaseMaterialization:
        self.calls.append("write")
        assert reservation is self.reservation
        assert not self.sealed
        self.written.append(snapshot.files[0].path if snapshot.files else "empty")
        if self.write_error is not None:
            raise self.write_error
        result = BaseMaterialization(
            reservation=reservation,
            source=snapshot.request,
            root_tree_sha=snapshot.root_tree_sha,
            manifest_digest=snapshot.fingerprint(),
            file_count=len(snapshot.files),
            total_bytes=sum(len(item.content) for item in snapshot.files),
        )
        if self.entered is not None:
            self.entered.set()
        if self.release is not None:
            return await asyncio.shield(self.release)
        self.after_write()
        return cast(BaseMaterialization, self.result_override(result))


@dataclass
class Context:
    service: MaterializeAssignedBase
    journal: Journal
    source: Source
    workspace: Workspace
    guard: Guard
    delivery: AssignmentDelivery
    reservation: WorkspaceReservation
    snapshot: BaseSourceSnapshot
    calls: list[str]


def context(policy: BaseMaterializationPolicy | None = None) -> Context:
    state, delivery, binding = workspace_inputs()
    calls: list[str] = []
    workspace = Workspace(binding, calls)
    intent = WorkerWorkspaceIntent(
        workspace_request_id=UUID(int=901),
        execution=binding.execution.intent,
        location=workspace.location,
    )
    state = replace(
        state,
        workspace_intent=intent,
        workspace_completion=WorkerWorkspaceCompletion(
            intent=intent, identity=workspace.reservation.identity
        ),
    )
    definition = delivery.definition
    request = BaseSourceRequest(
        repository_id=definition.repository_id,
        provider_repository_id=definition.provider_repository_id,
        repository_slug=definition.repository_slug,
        base_revision=definition.base_revision,
    )
    content = b"base fixture\n"
    snapshot = BaseSourceSnapshot(
        request=request,
        root_tree_sha="d" * 40,
        files=(
            BaseSourceFile(
                path="README.md",
                blob_sha=git_blob_sha(content),
                mode=BaseFileMode.REGULAR,
                content=content,
            ),
        ),
    )
    source, journal = Source(snapshot, calls), Journal(state, calls)
    guard = Guard(binding.execution.reference, calls)
    service = MaterializeAssignedBase(
        journal, source, workspace, guard, policy or BaseMaterializationPolicy()
    )
    return Context(
        service,
        journal,
        source,
        workspace,
        guard,
        delivery,
        workspace.reservation,
        snapshot,
        calls,
    )


def actions(setup: Context) -> list[str]:
    return [call for call in setup.calls if call != "authority"]


def test_base_export_verifies_live_scope_and_accepted_source_without_journal_writes() -> (
    None
):
    setup = context()
    original = setup.journal.state

    result = asyncio.run(setup.service.execute(setup.delivery, setup.reservation))

    assert actions(setup) == ["journal", "verify", "source", "write"]
    assert result.reservation is setup.reservation
    assert result.source == setup.snapshot.request
    assert result.root_tree_sha == setup.snapshot.root_tree_sha
    assert result.manifest_digest == setup.snapshot.fingerprint()
    assert result.file_count == 1
    assert result.total_bytes == len(setup.snapshot.files[0].content)
    assert setup.source.request == setup.snapshot.request
    assert setup.journal.state is original
    assert setup.journal.writes == 0
    assert setup.workspace.seals == 0
    assert original.request.proof.reveal() not in repr(result)


def test_empty_verified_tree_is_valid_bounded_materialization() -> None:
    setup = context()
    setup.source.snapshot = replace(setup.snapshot, files=())

    result = asyncio.run(setup.service.execute(setup.delivery, setup.reservation))

    assert result.file_count == result.total_bytes == 0
    assert setup.workspace.seals == 0


def test_reconstructed_equal_reservation_is_not_live_process_evidence() -> None:
    setup = context()
    historical_copy = replace(setup.reservation)
    assert historical_copy == setup.reservation

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.delivery, historical_copy))

    assert actions(setup) == ["seal"]
    assert not setup.workspace.written


@pytest.mark.parametrize("history", ["pending", "unknown", "none"])
def test_without_known_completed_workspace_no_source_or_native_writes(
    history: str,
) -> None:
    setup = context()
    state = setup.journal.state
    if history == "pending":
        state = replace(state, workspace_completion=None)
    else:
        state = replace(
            state,
            workspace_intent=None,
            workspace_completion=None,
            workspace_history_unknown=history == "unknown",
        )
    setup.journal.state = state

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.delivery, setup.reservation))

    assert actions(setup) == ["journal", "seal"]
    assert setup.journal.state is state
    assert setup.source.request is None


@pytest.mark.parametrize("field", ["root_directory", "policy_digest", "identity"])
def test_journal_root_pins_must_match_live_retained_scope(field: str) -> None:
    setup = context()
    location = setup.workspace.location
    values: dict[str, Any] = {
        "root_directory": r"C:\other",
        "policy_digest": "b" * 64,
        "identity": replace(location.identity, file_index=16),
    }
    setup.workspace.location = replace(location, **{field: values[field]})

    with pytest.raises(ClaimReplayConflict):
        asyncio.run(setup.service.execute(setup.delivery, setup.reservation))

    assert actions(setup) == ["journal", "seal"]


def test_completion_for_different_child_cannot_be_materialized() -> None:
    setup = context()
    state = setup.journal.state
    setup.journal.state = replace(
        state,
        workspace_completion=replace(
            state.workspace_completion,
            identity=replace(setup.reservation.identity, file_index=21),
        ),
    )

    with pytest.raises(ClaimReplayConflict):
        asyncio.run(setup.service.execute(setup.delivery, setup.reservation))

    assert setup.source.request is None
    assert setup.workspace.seals == 1


@pytest.mark.parametrize(
    "field,value",
    [
        ("provider_repository_id", 99),
        ("repository_slug", "other/repo"),
        ("base_revision", "b" * 40),
        ("authorized_branch", "agent/other"),
        ("policy_version", "other"),
    ],
)
def test_changed_full_delivery_is_denied_before_source(field: str, value: Any) -> None:
    setup = context()
    delivery = replace(
        setup.delivery, definition=replace(setup.delivery.definition, **{field: value})
    )

    with pytest.raises(ClaimReplayConflict):
        asyncio.run(setup.service.execute(delivery, setup.reservation))

    assert setup.source.request is None
    assert not setup.workspace.written


@pytest.mark.parametrize(
    "field,value",
    [
        ("repository_id", UUID(int=994)),
        ("provider_repository_id", 99),
        ("repository_slug", "other/repo"),
        ("base_revision", "b" * 40),
    ],
)
def test_source_snapshot_requires_all_exact_repository_and_base_pins(
    field: str, value: Any
) -> None:
    setup = context()
    setup.source.snapshot = replace(
        setup.snapshot, request=replace(setup.snapshot.request, **{field: value})
    )

    with pytest.raises(InvalidBaseSourceData):
        asyncio.run(setup.service.execute(setup.delivery, setup.reservation))

    assert not setup.workspace.written
    assert setup.workspace.seals == 1


@pytest.mark.parametrize("snapshot", [None, True, {"files": []}])
def test_untyped_source_manifest_is_rejected_before_writes(snapshot: Any) -> None:
    setup = context()
    setup.source.snapshot = snapshot

    with pytest.raises(InvalidBaseSourceData):
        asyncio.run(setup.service.execute(setup.delivery, setup.reservation))

    assert not setup.workspace.written


@pytest.mark.parametrize(
    "path", ["../escape", ".git/config", "folder/CON", "x:stream", "trailing."]
)
def test_entire_returned_manifest_is_revalidated_off_loop_before_writing(
    path: str,
) -> None:
    setup = context()
    object.__setattr__(setup.snapshot.files[0], "path", path)

    with pytest.raises(InvalidBaseSourceData):
        asyncio.run(setup.service.execute(setup.delivery, setup.reservation))

    assert not setup.workspace.written


def test_blob_bytes_must_still_match_the_declared_git_blob() -> None:
    setup = context()
    object.__setattr__(setup.snapshot.files[0], "content", b"changed")

    with pytest.raises(InvalidBaseSourceData):
        asyncio.run(setup.service.execute(setup.delivery, setup.reservation))

    assert not setup.workspace.written


def test_case_collisions_across_files_are_rejected_before_any_write() -> None:
    setup = context()
    first = setup.snapshot.files[0]
    second = replace(first, path="readme.md")
    object.__setattr__(setup.snapshot, "files", (first, second))

    with pytest.raises(InvalidBaseSourceData):
        asyncio.run(setup.service.execute(setup.delivery, setup.reservation))

    assert not setup.workspace.written


def test_source_read_failure_seals_scope_without_mutation_or_retry() -> None:
    setup = context()
    setup.source.error = BaseSourceUnavailable("Bounded source read failed.")

    with pytest.raises(BaseSourceUnavailable):
        asyncio.run(setup.service.execute(setup.delivery, setup.reservation))
    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.delivery, setup.reservation))

    assert actions(setup).count("source") == 1
    assert setup.workspace.seals == 1
    assert setup.journal.writes == 0


def test_native_live_verification_failure_never_reads_source() -> None:
    setup = context()
    setup.workspace.verify_error = WorkspaceUnavailable("Retained directory changed.")

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.delivery, setup.reservation))

    assert actions(setup) == ["journal", "verify", "seal"]
    assert setup.source.request is None


def test_stopped_initial_authority_seals_without_journal_or_source_access() -> None:
    setup = context()
    setup.guard.stopped = True

    with pytest.raises(LeaseDeadlineRejected):
        asyncio.run(setup.service.execute(setup.delivery, setup.reservation))

    assert actions(setup) == ["seal"]


def test_authority_loss_before_accepting_source_reply_cannot_start_writes() -> None:
    setup = context()
    setup.source.after_read = lambda: setattr(setup.guard, "stopped", True)

    with pytest.raises(LeaseDeadlineRejected):
        asyncio.run(setup.service.execute(setup.delivery, setup.reservation))

    assert not setup.workspace.written
    assert setup.workspace.seals == 1


def test_authority_loss_after_native_write_rejects_late_result_and_keeps_files() -> (
    None
):
    setup = context()
    setup.workspace.after_write = lambda: setattr(setup.guard, "stopped", True)

    with pytest.raises(LeaseDeadlineRejected):
        asyncio.run(setup.service.execute(setup.delivery, setup.reservation))

    assert setup.workspace.written == ["README.md"]
    assert setup.workspace.seals == 1
    assert setup.journal.writes == 0


def test_captured_original_seal_cannot_be_replaced_by_foreign_supplier() -> None:
    setup, foreign = context(), context()

    def replace_supplier() -> None:
        setup.workspace.binding = foreign.workspace.binding
        setup.workspace.seal = foreign.workspace.seal  # type: ignore[method-assign]

    setup.source.after_read = replace_supplier

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.delivery, setup.reservation))

    assert setup.workspace.seals == 1
    assert foreign.workspace.seals == 0
    assert not setup.workspace.written


@pytest.mark.parametrize(
    "field,value",
    [
        ("manifest_digest", "b" * 64),
        ("root_tree_sha", "e" * 40),
        ("file_count", 2),
        ("total_bytes", 1),
    ],
)
def test_native_result_must_match_independently_validated_manifest(
    field: str, value: Any
) -> None:
    setup = context()
    setup.workspace.result_override = lambda result: replace(result, **{field: value})

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.delivery, setup.reservation))

    assert setup.workspace.written
    assert setup.workspace.seals == 1


def test_equal_but_reconstructed_result_reservation_cannot_return_success() -> None:
    setup = context()
    setup.workspace.result_override = lambda result: replace(
        result, reservation=replace(result.reservation)
    )

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.delivery, setup.reservation))

    assert setup.workspace.seals == 1


@pytest.mark.parametrize("returned", [None, True, {"file_count": 1}])
def test_untyped_result_never_certifies_materialization(returned: Any) -> None:
    setup = context()
    setup.workspace.result_override = lambda result: returned

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.delivery, setup.reservation))

    assert setup.workspace.written


def test_one_shot_success_cannot_materialize_again() -> None:
    setup = context()

    async def scenario() -> None:
        await setup.service.execute(setup.delivery, setup.reservation)
        with pytest.raises(WorkspaceUnavailable):
            await setup.service.execute(setup.delivery, setup.reservation)

    asyncio.run(scenario())

    assert actions(setup).count("write") == 1
    assert setup.workspace.seals == 0


@pytest.mark.parametrize("phase", ["source", "native"])
def test_cancellation_seals_before_return_and_ignores_late_native_or_source_result(
    phase: str,
) -> None:
    setup = context()

    async def scenario() -> None:
        ready = asyncio.Event()
        if phase == "source":
            setup.source.entered = ready
            setup.source.release = asyncio.get_running_loop().create_future()
        else:
            setup.workspace.entered = ready
            setup.workspace.release = asyncio.get_running_loop().create_future()
        task = asyncio.create_task(
            setup.service.execute(setup.delivery, setup.reservation)
        )
        await ready.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert setup.workspace.sealed
        if phase == "source":
            assert setup.source.release is not None
            setup.source.release.set_result(setup.snapshot)
        else:
            assert setup.workspace.release is not None
            setup.workspace.release.set_exception(
                WorkspaceUnavailable("Late native failure.")
            )
            setup.workspace.release.exception()
        await asyncio.sleep(0)
        with pytest.raises(WorkspaceUnavailable):
            await setup.service.execute(setup.delivery, setup.reservation)

    asyncio.run(scenario())

    assert setup.workspace.seals == 1
    assert bool(setup.workspace.written) == (phase == "native")
    assert setup.journal.writes == 0


@pytest.mark.parametrize("phase", ["source", "native"])
def test_watchdog_detects_expiry_while_operation_has_not_returned(phase: str) -> None:
    setup = context()

    async def scenario() -> None:
        ready = asyncio.Event()
        if phase == "source":
            setup.source.entered = ready
            setup.source.release = asyncio.get_running_loop().create_future()
        else:
            setup.workspace.entered = ready
            setup.workspace.release = asyncio.get_running_loop().create_future()
        task = asyncio.create_task(
            setup.service.execute(setup.delivery, setup.reservation)
        )
        await ready.wait()
        setup.guard.stopped = True
        with pytest.raises(LeaseDeadlineRejected):
            await task
        assert setup.workspace.sealed
        if phase == "source":
            assert setup.source.release is not None
            setup.source.release.cancel()
        else:
            assert setup.workspace.release is not None
            setup.workspace.release.cancel()

    asyncio.run(scenario())

    assert setup.workspace.seals == 1
    assert bool(setup.workspace.written) == (phase == "native")


@pytest.mark.parametrize("total", [False, True])
def test_finite_wait_budget_seals_even_with_valid_unchanging_authority(
    total: bool,
) -> None:
    policy = BaseMaterializationPolicy(
        operation_timeout_seconds=1 if total else 0.05,
        total_timeout_seconds=0.05 if total else 1,
    )
    setup = context(policy)

    async def scenario() -> None:
        setup.source.release = asyncio.get_running_loop().create_future()
        with pytest.raises(WorkspaceUnavailable, match="wait budget"):
            await setup.service.execute(setup.delivery, setup.reservation)
        assert setup.workspace.sealed
        setup.source.release.cancel()

    asyncio.run(scenario())

    assert setup.guard.stopped is False
    assert not setup.workspace.written


def test_authority_loss_during_off_loop_manifest_validation_prevents_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup = context()
    entered, release = threading.Event(), threading.Event()
    original = materialization._validate_snapshot

    def paused(
        snapshot: BaseSourceSnapshot, request: BaseSourceRequest
    ) -> tuple[str, int, int]:
        entered.set()
        assert release.wait(3)
        return original(snapshot, request)

    monkeypatch.setattr(materialization, "_validate_snapshot", paused)

    async def scenario() -> None:
        task = asyncio.create_task(
            setup.service.execute(setup.delivery, setup.reservation)
        )
        assert await asyncio.to_thread(entered.wait, 3)
        setup.guard.stopped = True
        with pytest.raises(LeaseDeadlineRejected):
            await task
        assert setup.workspace.sealed
        release.set()

    asyncio.run(scenario())

    assert not setup.workspace.written


def test_real_h4_suspend_clock_jump_is_seen_while_native_materialization_waits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup, clock = context(), Clock()
    heartbeat = execution_context()
    heartbeat.delivery = setup.delivery

    async def scenario() -> None:
        async with live_supervisor(heartbeat, clock, monkeypatch) as supervisor:
            service = MaterializeAssignedBase(
                setup.journal, setup.source, setup.workspace, supervisor
            )
            setup.workspace.entered = asyncio.Event()
            setup.workspace.release = asyncio.get_running_loop().create_future()
            task = asyncio.create_task(
                service.execute(setup.delivery, setup.reservation)
            )
            await setup.workspace.entered.wait()
            assert supervisor.deadline is not None
            clock.value = supervisor.deadline.stop_at
            with pytest.raises(LeaseDeadlineRejected):
                await task
            assert setup.workspace.sealed
            assert supervisor.stop_required
            setup.workspace.release.cancel()

    asyncio.run(scenario())

    assert setup.workspace.written == ["README.md"]
    assert setup.journal.writes == 0


@pytest.mark.parametrize(
    "field", ["request", "assignment", "acknowledgement", "completion", "execution"]
)
def test_missing_durable_original_evidence_denies_source_access(field: str) -> None:
    setup = context()
    object.__setattr__(setup.journal.state, field, None)

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.delivery, setup.reservation))

    assert actions(setup) == ["journal", "seal"]
    assert setup.source.request is None
    assert not setup.workspace.written


def test_regressed_acknowledgement_phase_cannot_use_completed_workspace() -> None:
    setup = context()
    object.__setattr__(
        setup.journal.state, "phase", WorkerRecoveryPhase.ACKNOWLEDGEMENT_PENDING
    )

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.delivery, setup.reservation))

    assert actions(setup) == ["journal", "seal"]


@pytest.mark.parametrize(
    "anchor",
    [
        "acknowledgement",
        "completion",
        "execution",
        "workspace_intent",
        "workspace_completion",
    ],
)
def test_changed_durable_original_anchor_denies_source_access(anchor: str) -> None:
    setup = context()
    state = setup.journal.state
    if anchor == "acknowledgement":
        changed = replace(
            state.acknowledgement, acknowledgement_request_id=UUID(int=912)
        )
    elif anchor == "completion":
        changed = replace(
            state.completion,
            intent=replace(
                state.completion.intent, acknowledgement_request_id=UUID(int=912)
            ),
        )
    elif anchor == "execution":
        changed = replace(state.execution, execution_id=UUID(int=913))
    elif anchor == "workspace_intent":
        changed = replace(
            state.workspace_intent,
            execution=replace(state.execution, execution_id=UUID(int=913)),
        )
    else:
        changed = replace(
            state.workspace_completion,
            intent=replace(state.workspace_intent, workspace_request_id=UUID(int=914)),
        )
    object.__setattr__(state, anchor, changed)

    with pytest.raises(ClaimReplayConflict):
        asyncio.run(setup.service.execute(setup.delivery, setup.reservation))

    assert actions(setup) == ["journal", "seal"]
    assert setup.source.request is None
    assert setup.journal.writes == 0


@pytest.mark.parametrize("stage", ["source", "native"])
@pytest.mark.parametrize("scope", ["reservation", "location"])
def test_scope_changes_during_wait_reject_late_result(stage: str, scope: str) -> None:
    setup = context()

    def change_scope() -> None:
        if scope == "reservation":
            setup.workspace.reservation = replace(setup.reservation)
        else:
            setup.workspace.location = replace(
                setup.workspace.location, root_directory=r"C:\different"
            )

    if stage == "source":
        setup.source.after_read = change_scope
    else:
        setup.workspace.after_write = change_scope

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.delivery, setup.reservation))

    assert bool(setup.workspace.written) == (stage == "native")
    assert setup.workspace.seals == 1


def test_concurrent_execute_is_rejected_before_another_source_read() -> None:
    setup = context()

    async def scenario() -> None:
        setup.source.entered = asyncio.Event()
        setup.source.release = asyncio.get_running_loop().create_future()
        first = asyncio.create_task(
            setup.service.execute(setup.delivery, setup.reservation)
        )
        await setup.source.entered.wait()
        with pytest.raises(WorkspaceUnavailable):
            await setup.service.execute(setup.delivery, setup.reservation)
        assert setup.workspace.seals == 0
        setup.source.release.set_result(setup.snapshot)
        await first

    asyncio.run(scenario())

    assert actions(setup).count("source") == actions(setup).count("write") == 1


def test_partial_native_write_failure_retains_files_and_consumes_admission() -> None:
    setup = context()
    setup.workspace.write_error = WorkspaceUnavailable("Disk write failed.")

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.delivery, setup.reservation))
    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(setup.service.execute(setup.delivery, setup.reservation))

    assert setup.workspace.written == ["README.md"]
    assert setup.workspace.seals == 1
    assert setup.journal.writes == 0


def test_cancelled_owned_operation_observes_original_scope_already_sealed() -> None:
    setup = context()
    observed: list[bool] = []

    class CancellationSource(Source):
        async def read(self, request: BaseSourceRequest) -> BaseSourceSnapshot:
            assert self.entered is not None
            self.entered.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                observed.append(setup.workspace.sealed)
                raise
            raise AssertionError("Unreachable source reply.")

    async def scenario() -> None:
        source = CancellationSource(setup.snapshot, setup.calls)
        source.entered = asyncio.Event()
        service = MaterializeAssignedBase(
            setup.journal, source, setup.workspace, setup.guard
        )
        task = asyncio.create_task(service.execute(setup.delivery, setup.reservation))
        await source.entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0)

    asyncio.run(scenario())

    assert observed == [True]
    assert setup.workspace.seals == 1
    assert not setup.workspace.written
