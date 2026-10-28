import asyncio
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import cast
from uuid import UUID, uuid4

import pytest

from devrelay.application.contracts.assignment_delivery import AssignmentDelivery
from devrelay.application.contracts.base_sources import (
    BaseMaterialization,
    BaseSourceFile,
    git_blob_sha,
)
from devrelay.application.contracts.candidates import CandidateCommitSpec
from devrelay.application.contracts.coding_results import (
    CodingCycleEvidence,
    SuccessfulCodingResult,
)
from devrelay.application.contracts.editable_workspaces import EditableWorkspace
from devrelay.application.contracts.frozen_candidates import FrozenCandidate
from devrelay.application.contracts.git_sources import GitWorkspacePreparation
from devrelay.application.contracts.processes import ProcessStopStatus
from devrelay.application.contracts.submissions import CheckPolicy
from devrelay.application.contracts.trusted_processes import (
    TrustedProcessResult,
    TrustedProcessStopReason,
)
from devrelay.application.contracts.workspace_recovery import (
    WorkspaceDirectoryIdentity,
    WorkspaceLocation,
)
from devrelay.application.contracts.workspaces import WorkspaceReservation
from devrelay.application.lease_deadlines import LeaseDeadlineRejected
from devrelay.application.ports.candidate_workspace import CandidateWorkspacePort
from devrelay.application.worker_coding import (
    CodingRunPolicy,
    CodingRunUnavailable,
    RunAssignedCoding,
)
from devrelay.infrastructure.workspace.candidate_objects import build_candidate_snapshot
from tests.git_source_fixtures import git_workspace_inputs
from tests.unit.test_candidate_objects import commit
from tests.unit.test_worker_workspace_reservation import Guard


class Clock:
    def __init__(self) -> None:
        self.value = 10.0

    def __call__(self) -> float:
        return self.value


class Workspace:
    def __init__(self, repairs: int, calls: list[str]) -> None:
        _, delivery, binding, fixture = git_workspace_inputs()
        definition = replace(
            delivery.definition,
            max_repair_cycles=repairs,
            checks=(
                replace(delivery.definition.checks[0], id="tests"),
                replace(delivery.definition.checks[0], id="lint"),
            ),
        )
        self.delivery = replace(delivery, definition=definition)
        intent = replace(
            binding.execution.intent,
            acknowledgement=replace(
                binding.execution.intent.acknowledgement,
                definition_digest=definition.fingerprint(),
            ),
        )
        self.binding = replace(
            binding,
            execution=replace(binding.execution, intent=intent),
            definition=definition,
        )
        self.source = fixture.snapshot
        self.location = WorkspaceLocation(
            root_directory=r"C:\workspace",
            policy_digest="b" * 64,
            identity=WorkspaceDirectoryIdentity(volume_serial_number=1, file_index=1),
        )
        self.reservation = WorkspaceReservation(
            binding=self.binding,
            directory_name=str(intent.execution_id),
            identity=WorkspaceDirectoryIdentity(volume_serial_number=1, file_index=2),
        )
        self.materialization = BaseMaterialization(
            reservation=self.reservation,
            source=self.source.request,
            root_tree_sha=self.source.root_tree_sha,
            manifest_digest=self.source.fingerprint(),
            file_count=len(self.source.files),
            total_bytes=sum(len(item.content) for item in self.source.files),
        )
        self.git_preparation = GitWorkspacePreparation(
            materialization=self.materialization,
            branch=definition.authorized_branch,
            metadata_digest="c" * 64,
        )
        self.candidate_capture_preparation = self.git_preparation
        self.editable = EditableWorkspace(
            materialization=self.materialization, location=self.location
        )
        self.calls = calls
        self.frozen: FrozenCandidate | None = None
        self.seals = 0
        self.verify_hook: Callable[[], None] = lambda: None
        self.capture_transform: Callable[[FrozenCandidate], FrozenCandidate] = (
            lambda value: value
        )
        self.release_transform: Callable[[EditableWorkspace], EditableWorkspace] = (
            lambda value: value
        )
        self.commit_override: CandidateCommitSpec | None = None

    def seal(self) -> None:
        self.calls.append("workspace-seal")
        self.seals += 1

    async def capture_candidate(
        self,
        preparation: GitWorkspacePreparation,
        editable: EditableWorkspace,
        *,
        capture_id: UUID,
        cycle: int,
        commit: CandidateCommitSpec,
    ) -> FrozenCandidate:
        assert preparation is self.git_preparation and editable is self.editable
        assert self.frozen is None
        self.calls.append(f"capture-{cycle}")
        content = f"Changed candidate {cycle}\n".encode()
        first, *rest = self.source.files
        files = (
            BaseSourceFile(
                path=first.path,
                mode=first.mode,
                content=content,
                blob_sha=git_blob_sha(content),
            ),
            *rest,
        )
        assert self.source.raw_commit is not None
        from devrelay.application.contracts.git_sources import GitBaseSourceSnapshot

        snapshot = build_candidate_snapshot(
            execution_id=self.binding.execution.intent.execution_id,
            cycle=cycle,
            source=GitBaseSourceSnapshot(
                snapshot=self.source, raw_commit=self.source.raw_commit
            ),
            branch=preparation.branch,
            files=files,
            commit=self.commit_override or commit,
        )
        root = self.location.root_directory + "\\" + self.reservation.directory_name
        candidate = FrozenCandidate(
            capture_id=capture_id,
            editable=editable,
            preparation=preparation,
            snapshot=snapshot,
            git_directory=root + "\\.git\\devrelay\\" + str(capture_id),
            git_identity=WorkspaceDirectoryIdentity(
                volume_serial_number=1, file_index=10 + cycle
            ),
        )
        self.frozen = self.capture_transform(candidate)
        return self.frozen

    async def verify_candidate(self, candidate: FrozenCandidate) -> None:
        assert candidate is self.frozen
        self.calls.append(f"verify-{candidate.snapshot.cycle}")
        self.verify_hook()

    async def release_candidate(self, candidate: FrozenCandidate) -> EditableWorkspace:
        assert candidate is self.frozen
        self.calls.append(f"release-{candidate.snapshot.cycle}")
        self.frozen = None
        return self.release_transform(self.editable)


class Runtime:
    def __init__(self, workspace: Workspace, calls: list[str]) -> None:
        self.workspace, self.calls = workspace, calls
        self.binding = workspace.binding.execution
        self.seals = 0
        self.failed_cycles = 0
        self.coding_exit = 0
        self.stop_status = ProcessStopStatus.EMPTY_VERIFIED
        self.reason = TrustedProcessStopReason.EXITED
        self.execution_id = self.binding.intent.execution_id
        self.previous: list[CodingCycleEvidence | None] = []
        self.after_code: Callable[[], None] = lambda: None
        self.after_check: Callable[[], None] = lambda: None
        self.entered: asyncio.Event | None = None
        self.release: asyncio.Event | None = None
        self.finished: asyncio.Event | None = None
        self.cancel_seals: tuple[int, int] | None = None
        self.swallow_cancel = False
        self.output = b"SUCCESS: model text is not check authority\x00\x1b\r\n"

    def seal(self) -> None:
        self.calls.append("runtime-seal")
        self.seals += 1

    def result(self, exit_code: int) -> TrustedProcessResult:
        return TrustedProcessResult(
            execution_id=self.execution_id,
            reason=self.reason,
            exit_code=exit_code,
            stdout=self.output,
            stderr=b"",
            stop_status=self.stop_status,
        )

    async def code(
        self,
        delivery: AssignmentDelivery,
        editable: EditableWorkspace,
        *,
        cycle: int,
        previous: CodingCycleEvidence | None,
    ) -> TrustedProcessResult:
        assert (
            delivery == self.workspace.delivery and editable is self.workspace.editable
        )
        self.calls.append(f"code-{cycle}")
        self.previous.append(previous)
        if self.entered is not None:
            self.entered.set()
        if self.release is not None:
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                self.cancel_seals = self.seals, self.workspace.seals
                if not self.swallow_cancel:
                    raise
                await self.release.wait()
        self.after_code()
        if self.finished is not None:
            self.finished.set()
        return self.result(self.coding_exit)

    async def check(
        self, candidate: FrozenCandidate, check: CheckPolicy
    ) -> TrustedProcessResult:
        assert (
            candidate is self.workspace.frozen
            and check in self.workspace.delivery.definition.checks
        )
        self.calls.append(f"check-{candidate.snapshot.cycle}-{check.id}")
        self.after_check()
        return self.result(
            1
            if candidate.snapshot.cycle < self.failed_cycles
            and check.id == self.workspace.delivery.definition.checks[0].id
            else 0
        )


@dataclass
class Context:
    workspace: Workspace
    runtime: Runtime
    guard: Guard
    clock: Clock
    calls: list[str]
    service: RunAssignedCoding

    async def execute(self) -> SuccessfulCodingResult:
        return await self.service.execute(
            self.workspace.delivery,
            self.workspace.binding.execution.intent,
            self.workspace.git_preparation,
            self.workspace.editable,
            commit=commit(),
        )


def context(repairs: int = 2, policy: CodingRunPolicy | None = None) -> Context:
    calls: list[str] = []
    workspace = Workspace(repairs, calls)
    runtime = Runtime(workspace, calls)
    guard = Guard(workspace.binding.execution.reference, [])
    clock = Clock()
    service = RunAssignedCoding(
        cast(CandidateWorkspacePort, workspace),
        runtime,
        guard,
        monotonic_clock=clock,
        policy=policy or CodingRunPolicy(),
    )
    return Context(workspace, runtime, guard, clock, calls, service)


def test_success_freezes_real_candidate_and_runs_every_ordered_fixed_check() -> None:
    setup = context()

    result = asyncio.run(setup.execute())

    result.validate(setup.workspace.delivery.definition)
    assert len(result.cycles) == 1 and result.cycles[0].passed
    assert setup.workspace.frozen is not None
    assert result.final_candidate is setup.workspace.frozen.snapshot
    assert setup.calls == [
        "code-0",
        "capture-0",
        "verify-0",
        "check-0-tests",
        "verify-0",
        "verify-0",
        "check-0-lint",
        "verify-0",
        "runtime-seal",
    ]
    assert setup.runtime.previous == [None]
    assert setup.workspace.seals == 0
    assert "\x00" not in result.cycles[0].checks[0].diagnostic_tail
    assert "\x1b" not in result.cycles[0].checks[0].diagnostic_tail


@pytest.mark.parametrize("repairs", [1, 2, 5])
def test_failed_settled_checks_justify_only_bounded_repairs_with_full_rechecks(
    repairs: int,
) -> None:
    setup = context(repairs)
    setup.runtime.failed_cycles = repairs

    result = asyncio.run(setup.execute())

    assert len(result.cycles) == repairs + 1
    assert [item.passed for item in result.cycles] == [False] * repairs + [True]
    assert [item.check_id for cycle in result.cycles for item in cycle.checks] == [
        "tests",
        "lint",
    ] * (repairs + 1)
    assert setup.runtime.previous == [None, *result.cycles[:-1]]
    assert sum(call.startswith("release-") for call in setup.calls) == repairs
    assert setup.workspace.seals == 0


def test_exhausted_checks_never_turn_model_success_text_into_a_report() -> None:
    setup = context(0)
    setup.runtime.failed_cycles = 1

    with pytest.raises(CodingRunUnavailable, match="repair budget"):
        asyncio.run(setup.execute())

    assert setup.calls[-2:] == ["runtime-seal", "workspace-seal"]
    assert not any(call.startswith("release-") for call in setup.calls)
    assert "check-0-lint" in setup.calls


@pytest.mark.parametrize(
    "mode", ["coding_failure", "unknown", "wrong_execution", "timeout_reason"]
)
def test_unsettled_or_failed_coding_cannot_capture_or_repair(mode: str) -> None:
    setup = context()
    if mode == "coding_failure":
        setup.runtime.coding_exit = 1
    elif mode == "unknown":
        setup.runtime.stop_status = ProcessStopStatus.UNKNOWN
    elif mode == "wrong_execution":
        setup.runtime.execution_id = uuid4()
    else:
        setup.runtime.reason = TrustedProcessStopReason.RUNTIME_LIMIT

    with pytest.raises(CodingRunUnavailable):
        asyncio.run(setup.execute())

    assert setup.calls == ["code-0", "runtime-seal", "workspace-seal"]


@pytest.mark.parametrize(
    "boundary", ["before_code", "after_code", "before_check", "after_check"]
)
def test_authority_loss_fences_the_next_launch_capture_or_release(
    boundary: str,
) -> None:
    setup = context()

    def stop() -> None:
        setup.guard.stopped = True

    if boundary == "before_code":
        setup.guard.stopped = True
    elif boundary == "after_code":
        setup.runtime.after_code = stop
    elif boundary == "before_check":
        setup.workspace.verify_hook = stop
    else:
        setup.runtime.after_check = stop

    with pytest.raises(LeaseDeadlineRejected):
        asyncio.run(setup.execute())

    assert setup.calls[-2:] == ["runtime-seal", "workspace-seal"]
    assert not any(call.startswith("release-") for call in setup.calls)
    if boundary in ("before_code", "after_code"):
        assert not any(call.startswith("capture-") for call in setup.calls)
    if boundary in ("before_code", "after_code", "before_check"):
        assert not any(call.startswith("check-") for call in setup.calls)
    else:
        assert (
            setup.calls.count("check-0-tests") == 1
            and "check-0-lint" not in setup.calls
        )


def test_candidate_with_an_unconfigured_commit_is_not_checked() -> None:
    setup = context()
    setup.workspace.commit_override = replace(commit(), message="Unexpected commit\n")

    with pytest.raises(CodingRunUnavailable, match="candidate changed"):
        asyncio.run(setup.execute())

    assert not any(call.startswith("check-") for call in setup.calls)
    assert setup.calls[-2:] == ["runtime-seal", "workspace-seal"]


def test_foreign_runtime_binding_is_rejected_before_any_invocation() -> None:
    setup = context()
    setup.runtime.binding = replace(
        setup.runtime.binding,
        intent=replace(setup.runtime.binding.intent, execution_id=uuid4()),
    )

    with pytest.raises(CodingRunUnavailable):
        asyncio.run(setup.execute())

    assert setup.calls == ["runtime-seal", "workspace-seal"]


def test_changed_claim_request_is_rejected_before_coding() -> None:
    setup = context()
    setup.workspace.delivery = replace(
        setup.workspace.delivery,
        outcome=replace(setup.workspace.delivery.outcome, claim_request_id=uuid4()),
    )

    with pytest.raises(CodingRunUnavailable):
        asyncio.run(setup.execute())

    assert setup.calls == ["runtime-seal", "workspace-seal"]


def test_foreign_capture_identity_is_rejected_before_checking() -> None:
    setup = context()

    def foreign(candidate: FrozenCandidate) -> FrozenCandidate:
        replacement_id = uuid4()
        directory = candidate.git_directory.rsplit("\\", 1)[0]
        return replace(
            candidate,
            capture_id=replacement_id,
            git_directory=directory + "\\" + str(replacement_id),
        )

    setup.workspace.capture_transform = foreign

    with pytest.raises(CodingRunUnavailable, match="candidate changed"):
        asyncio.run(setup.execute())

    assert not any(call.startswith("check-") for call in setup.calls)
    assert setup.calls[-2:] == ["runtime-seal", "workspace-seal"]


def test_readonly_verification_failure_after_check_prevents_next_check_or_repair() -> (
    None
):
    setup = context()

    def changed() -> None:
        raise RuntimeError("Controlled source verification failed")

    setup.runtime.after_check = lambda: setattr(setup.workspace, "verify_hook", changed)

    with pytest.raises(RuntimeError, match="source verification"):
        asyncio.run(setup.execute())

    assert "check-0-tests" in setup.calls and "check-0-lint" not in setup.calls
    assert not any(call.startswith("release-") for call in setup.calls)
    assert setup.calls[-2:] == ["runtime-seal", "workspace-seal"]


def test_unsettled_check_cannot_trigger_repair() -> None:
    setup = context()
    setup.runtime.failed_cycles = 1
    setup.runtime.after_check = lambda: setattr(
        setup.runtime, "stop_status", ProcessStopStatus.UNKNOWN
    )

    with pytest.raises(CodingRunUnavailable, match="verified exit"):
        asyncio.run(setup.execute())

    assert not any(call.startswith("release-") for call in setup.calls)
    assert "code-1" not in setup.calls and "check-0-lint" not in setup.calls


def test_repair_release_must_return_original_editable_capability() -> None:
    setup = context()
    setup.runtime.failed_cycles = 1
    setup.workspace.release_transform = replace

    with pytest.raises(CodingRunUnavailable, match="original editable"):
        asyncio.run(setup.execute())

    assert "release-0" in setup.calls and "code-1" not in setup.calls


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -1.0, 9.0])
def test_invalid_or_reversed_own_clock_stops_before_capture(value: float) -> None:
    setup = context()
    setup.runtime.after_code = lambda: setattr(setup.clock, "value", value)

    with pytest.raises(CodingRunUnavailable, match="clock"):
        asyncio.run(setup.execute())

    assert setup.calls == ["code-0", "runtime-seal", "workspace-seal"]


def test_frozen_clock_cannot_disable_the_operation_wait_budget() -> None:
    setup = context(policy=CodingRunPolicy(coding_timeout_seconds=0.1))

    async def exercise() -> None:
        setup.runtime.release = asyncio.Event()
        with pytest.raises(CodingRunUnavailable, match="operation exceeded"):
            await setup.execute()
        await asyncio.sleep(0)

    asyncio.run(exercise())

    assert setup.runtime.cancel_seals == (1, 1)
    assert not any(call.startswith("capture-") for call in setup.calls)


def test_clock_jump_past_total_budget_prevents_capture() -> None:
    setup = context()
    setup.runtime.after_code = lambda: setattr(setup.clock, "value", 4000)

    with pytest.raises(CodingRunUnavailable, match="time budget"):
        asyncio.run(setup.execute())

    assert setup.calls == ["code-0", "runtime-seal", "workspace-seal"]


@pytest.mark.parametrize("late", [False, True])
def test_cancellation_seals_originals_before_pending_runtime_cancellation(
    late: bool,
) -> None:
    setup = context()
    setup.runtime.swallow_cancel = late

    async def exercise() -> None:
        setup.runtime.entered, setup.runtime.release, setup.runtime.finished = (
            asyncio.Event(),
            asyncio.Event(),
            asyncio.Event(),
        )
        task = asyncio.create_task(setup.execute())
        await asyncio.wait_for(setup.runtime.entered.wait(), 1)
        task.cancel()
        try:
            with pytest.raises(asyncio.CancelledError):
                await task
            await asyncio.sleep(0)
        finally:
            setup.runtime.release.set()
            if late:
                await asyncio.wait_for(setup.runtime.finished.wait(), 1)

    asyncio.run(exercise())

    assert setup.runtime.cancel_seals == (1, 1)
    assert not any(
        call.startswith(("capture-", "release-", "check-")) for call in setup.calls
    )


def test_duplicate_execute_has_no_new_runtime_or_workspace_effects() -> None:
    setup = context()

    async def exercise() -> None:
        await setup.execute()
        with pytest.raises(CodingRunUnavailable, match="already been requested"):
            await setup.execute()

    asyncio.run(exercise())

    assert setup.calls.count("code-0") == 1 and setup.calls.count("runtime-seal") == 1


def test_concurrent_duplicate_cannot_interrupt_the_original_live_run() -> None:
    setup = context()

    async def exercise() -> SuccessfulCodingResult:
        setup.runtime.entered, setup.runtime.release = asyncio.Event(), asyncio.Event()
        original = asyncio.create_task(setup.execute())
        await asyncio.wait_for(setup.runtime.entered.wait(), 1)
        try:
            with pytest.raises(CodingRunUnavailable, match="already been requested"):
                await setup.execute()
        finally:
            setup.runtime.release.set()
        return await asyncio.wait_for(original, 1)

    result = asyncio.run(exercise())

    assert len(result.cycles) == 1
    assert setup.calls.count("code-0") == 1 and setup.calls.count("runtime-seal") == 1
    assert setup.workspace.seals == 0


def test_cancellation_requested_inside_final_admission_guard_cannot_start_code() -> (
    None
):
    setup = context()

    async def exercise() -> None:
        original = asyncio.create_task(setup.execute())

        def cancel() -> None:
            original.cancel()

        setup.guard.after_check = cancel
        with pytest.raises(asyncio.CancelledError):
            await original

    asyncio.run(exercise())

    assert setup.calls == ["runtime-seal", "workspace-seal"]


def test_cancellation_at_coding_completion_cannot_advance_to_capture() -> None:
    setup = context()

    async def exercise() -> None:
        original = asyncio.create_task(setup.execute())

        def cancel() -> None:
            original.cancel()

        setup.runtime.after_code = cancel
        with pytest.raises(asyncio.CancelledError):
            await original

    asyncio.run(exercise())

    assert setup.calls == ["code-0", "runtime-seal", "workspace-seal"]
