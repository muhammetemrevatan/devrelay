import asyncio
import threading
from collections.abc import Callable
from dataclasses import replace
from typing import Any, cast
from uuid import UUID, uuid4

import pytest

import devrelay.bootstrap.workspaces as bootstrap
from devrelay.application.contracts.base_sources import (
    BaseMaterialization,
    BaseSourceFile,
    git_blob_sha,
)
from devrelay.application.contracts.candidates import CandidateCommitSpec
from devrelay.application.contracts.editable_workspaces import EditableWorkspace
from devrelay.application.contracts.frozen_candidates import FrozenCandidate
from devrelay.application.contracts.git_sources import (
    GitBaseSourceSnapshot,
    GitWorkspacePreparation,
)
from devrelay.application.contracts.workspaces import WorkspaceDirectoryIdentity
from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from devrelay.infrastructure.workspace.candidate_objects import build_candidate_snapshot
from tests.git_source_fixtures import git_workspace_inputs
from tests.unit.test_base_materialization_bootstrap import Native
from tests.unit.test_candidate_objects import commit
from tests.unit.test_editable_workspace_bootstrap import policy

_DEFAULT = object()


class CandidateNative(Native):
    def __init__(self) -> None:
        _, _, binding, fixture = git_workspace_inputs()
        super().__init__(binding, policy())
        assert fixture.snapshot.raw_commit is not None
        self.source = GitBaseSourceSnapshot(
            snapshot=fixture.snapshot, raw_commit=fixture.snapshot.raw_commit
        )
        self.helper = CandidateHelper(self)
        self.block_operation = ""
        self.operation_started = threading.Event()
        self.operation_release = threading.Event()
        self.operation_settled = threading.Event()
        self.errors: dict[str, Exception] = {}
        self.results: dict[str, object] = {}

    def operation(self, name: str, value: object) -> object:
        self.record(name)
        if name == self.block_operation:
            self.operation_started.set()
            if not self.operation_release.wait(5):
                raise AssertionError("Fixture operation was not released.")
        self.record(name + "-settled")
        self.operation_settled.set()
        if name in self.errors:
            raise self.errors[name]
        return self.results.get(name, value)

    def prepare_git_metadata(
        self,
        source: GitBaseSourceSnapshot,
        materialization: BaseMaterialization,
        branch: str,
    ) -> GitWorkspacePreparation:
        assert source is self.source
        return cast(
            GitWorkspacePreparation,
            self.operation(
                "git",
                GitWorkspacePreparation(
                    materialization=materialization,
                    branch=branch,
                    metadata_digest="a" * 64,
                ),
            ),
        )

    def prepare_candidate_capture(
        self, source: GitBaseSourceSnapshot
    ) -> CandidateHelper:
        assert source is self.source
        return cast(CandidateHelper, self.operation("candidate-prepare", self.helper))

    def handoff_materialized_base(
        self, materialization: BaseMaterialization
    ) -> EditableWorkspace:
        return cast(
            EditableWorkspace,
            self.operation(
                "handoff",
                EditableWorkspace(
                    materialization=materialization, location=self.location
                ),
            ),
        )


class CandidateHelper:
    def __init__(self, owner: CandidateNative) -> None:
        self.owner = owner
        self.captures: list[FrozenCandidate] = []

    def capture(
        self,
        preparation: GitWorkspacePreparation,
        editable: EditableWorkspace,
        *,
        capture_id: UUID,
        cycle: int,
        commit: CandidateCommitSpec,
    ) -> FrozenCandidate:
        content = b"Changed fixture source\n"
        first, *rest = self.owner.source.snapshot.files
        files = (
            BaseSourceFile(
                path=first.path,
                mode=first.mode,
                blob_sha=git_blob_sha(content),
                content=content,
            ),
            *rest,
        )
        snapshot = build_candidate_snapshot(
            execution_id=self.owner.binding.execution.intent.execution_id,
            cycle=cycle,
            source=self.owner.source,
            branch=preparation.branch,
            files=files,
            commit=commit,
        )
        worktree = (
            editable.location.root_directory
            + "\\"
            + editable.materialization.reservation.directory_name
        )
        frozen = FrozenCandidate(
            capture_id=capture_id,
            editable=editable,
            preparation=preparation,
            snapshot=snapshot,
            git_directory=worktree + "\\.git\\devrelay\\" + str(capture_id),
            git_identity=WorkspaceDirectoryIdentity(
                volume_serial_number=1, file_index=100 + cycle
            ),
        )
        self.captures.append(frozen)
        override = self.owner.results.get("capture-transform", _DEFAULT)
        if override is not _DEFAULT:
            frozen = cast(Callable[[FrozenCandidate], FrozenCandidate], override)(
                frozen
            )
        return cast(FrozenCandidate, self.owner.operation("capture", frozen))

    def verify(self, frozen: FrozenCandidate) -> None:
        assert frozen is self.captures[-1]
        self.owner.operation("verify-candidate", None)

    def release_for_repair(self, frozen: FrozenCandidate) -> EditableWorkspace:
        assert frozen is self.captures[-1]
        return cast(
            EditableWorkspace,
            self.owner.operation("release-candidate", frozen.editable),
        )


def install(monkeypatch: pytest.MonkeyPatch) -> CandidateNative:
    native = CandidateNative()
    monkeypatch.setattr(bootstrap, "WindowsWorkspaceReservation", lambda *args: native)
    monkeypatch.setattr(bootstrap, "WindowsCandidateWorkspace", CandidateHelper)
    return native


async def prepare(
    scope: bootstrap.WindowsWorkspaceScope, native: CandidateNative
) -> GitWorkspacePreparation:
    location = await scope.prepare()
    reservation = await scope.reserve(location)
    materialization = await scope.materialize(reservation, native.source.snapshot)
    return await scope.prepare_git_metadata(
        native.source, materialization, native.binding.definition.authorized_branch
    )


async def editable(
    scope: bootstrap.WindowsWorkspaceScope, native: CandidateNative
) -> tuple[GitWorkspacePreparation, EditableWorkspace]:
    preparation = await prepare(scope, native)
    assert await scope.prepare_candidate_capture(native.source) is preparation
    return preparation, await scope.handoff(preparation.materialization)


async def capture(
    scope: bootstrap.WindowsWorkspaceScope,
    preparation: GitWorkspacePreparation,
    workspace: EditableWorkspace,
    cycle: int = 0,
) -> FrozenCandidate:
    return await scope.capture_candidate(
        preparation, workspace, capture_id=uuid4(), cycle=cycle, commit=commit()
    )


def test_candidate_lifecycle_caches_only_accepted_observations_on_owning_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    native = install(monkeypatch)

    async def scenario() -> None:
        async with bootstrap.open_worker_workspace(native.binding, policy()) as scope:
            assert scope.editable is None
            assert scope.candidate_capture_preparation is None
            preparation, workspace = await editable(scope, native)
            assert scope.editable is workspace
            assert scope.candidate_capture_preparation is preparation
            assert scope.source_snapshot is None
            frozen = await capture(scope, preparation, workspace)
            await scope.verify_candidate(frozen)
            with pytest.raises(WorkspaceUnavailable):
                await capture(scope, preparation, workspace)
            assert await scope.release_candidate(frozen) is workspace
            with pytest.raises(WorkspaceUnavailable):
                await scope.verify_candidate(frozen)
            with pytest.raises(WorkspaceUnavailable):
                await scope.release_candidate(frozen)
            next_frozen = await capture(scope, preparation, workspace, cycle=1)
            await scope.verify_candidate(next_frozen)
            assert next_frozen is not frozen
            assert not native.sealed.is_set()

    asyncio.run(scenario())
    assert len(native.helper.captures) == 2
    assert native.calls[-1] == "close"
    assert len(set(native.threads)) == 1
    assert threading.get_ident() not in native.threads


@pytest.mark.parametrize("changed", ["source", "preparation", "editable", "candidate"])
def test_equal_reconstructions_never_reach_native_candidate_operations(
    monkeypatch: pytest.MonkeyPatch, changed: str
) -> None:
    native = install(monkeypatch)

    async def scenario() -> None:
        async with bootstrap.open_worker_workspace(native.binding, policy()) as scope:
            preparation = await prepare(scope, native)
            if changed == "source":
                with pytest.raises(WorkspaceUnavailable):
                    await scope.prepare_candidate_capture(
                        replace(native.source, snapshot=replace(native.source.snapshot))
                    )
                assert "candidate-prepare" not in native.calls
            await scope.prepare_candidate_capture(native.source)
            workspace = await scope.handoff(preparation.materialization)
            if changed in ("preparation", "editable"):
                with pytest.raises(WorkspaceUnavailable):
                    await capture(
                        scope,
                        replace(preparation)
                        if changed == "preparation"
                        else preparation,
                        replace(workspace) if changed == "editable" else workspace,
                    )
                assert native.helper.captures == []
            frozen = await capture(scope, preparation, workspace)
            if changed == "candidate":
                with pytest.raises(WorkspaceUnavailable):
                    await scope.verify_candidate(replace(frozen))
                with pytest.raises(WorkspaceUnavailable):
                    await scope.release_candidate(replace(frozen))
                assert "verify-candidate" not in native.calls
                assert "release-candidate" not in native.calls
            await scope.verify_candidate(frozen)
            assert not native.sealed.is_set()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "operation",
    ["candidate-prepare", "capture", "verify-candidate", "release-candidate"],
)
def test_cancelled_candidate_operations_seal_retain_fifo_and_never_adopt_late_results(
    monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    native = install(monkeypatch)

    async def scenario() -> None:
        try:
            async with bootstrap.open_worker_workspace(
                native.binding, policy()
            ) as scope:
                preparation = await prepare(scope, native)
                workspace: EditableWorkspace | None = None
                frozen: FrozenCandidate | None = None
                if operation != "candidate-prepare":
                    await scope.prepare_candidate_capture(native.source)
                    workspace = await scope.handoff(preparation.materialization)
                if operation in ("verify-candidate", "release-candidate"):
                    assert workspace is not None
                    frozen = await capture(scope, preparation, workspace)
                native.block_operation = operation
                native.operation_settled.clear()
                task: asyncio.Task[Any]
                if operation == "candidate-prepare":
                    task = asyncio.create_task(
                        scope.prepare_candidate_capture(native.source)
                    )
                elif operation == "capture":
                    assert workspace is not None
                    task = asyncio.create_task(capture(scope, preparation, workspace))
                elif operation == "verify-candidate":
                    assert frozen is not None
                    task = asyncio.create_task(scope.verify_candidate(frozen))
                else:
                    assert frozen is not None
                    task = asyncio.create_task(scope.release_candidate(frozen))
                assert await asyncio.to_thread(native.operation_started.wait, 2)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                assert native.sealed.is_set()
                assert "close" not in native.calls
                assert not native.operation_settled.is_set()
                if operation == "candidate-prepare":
                    assert scope.candidate_capture_preparation is None
                elif operation == "capture":
                    assert scope._frozen is None
                else:
                    assert scope._frozen is frozen
                with pytest.raises(WorkspaceUnavailable):
                    await scope.verify(native.reservation)
                native.operation_release.set()
                assert await asyncio.to_thread(native.operation_settled.wait, 2)
                await asyncio.sleep(0)
                if operation == "candidate-prepare":
                    assert scope.candidate_capture_preparation is None
                elif operation == "capture":
                    assert scope._frozen is None
                else:
                    assert scope._frozen is frozen
        finally:
            native.operation_release.set()

    asyncio.run(scenario())
    assert native.calls[-2:] == [operation + "-settled", "close"]
    assert len(set(native.threads)) == 1


@pytest.mark.parametrize(
    "operation", ["candidate-prepare", "capture", "release-candidate"]
)
def test_concurrent_candidate_transitions_cannot_queue_duplicate_native_work(
    monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    native = install(monkeypatch)

    async def scenario() -> None:
        try:
            async with bootstrap.open_worker_workspace(
                native.binding, policy()
            ) as scope:
                preparation = await prepare(scope, native)
                workspace: EditableWorkspace | None = None
                frozen: FrozenCandidate | None = None
                if operation != "candidate-prepare":
                    await scope.prepare_candidate_capture(native.source)
                    workspace = await scope.handoff(preparation.materialization)
                if operation == "release-candidate":
                    assert workspace is not None
                    frozen = await capture(scope, preparation, workspace)

                async def transition() -> object:
                    if operation == "candidate-prepare":
                        return await scope.prepare_candidate_capture(native.source)
                    if operation == "capture":
                        assert workspace is not None
                        return await capture(scope, preparation, workspace)
                    assert frozen is not None
                    return await scope.release_candidate(frozen)

                native.block_operation = operation
                first = asyncio.create_task(transition())
                assert await asyncio.to_thread(native.operation_started.wait, 2)
                with pytest.raises(WorkspaceUnavailable):
                    await transition()
                assert native.calls.count(operation) == 1
                assert not native.sealed.is_set()
                native.operation_release.set()
                accepted = await first
                if operation == "candidate-prepare":
                    assert (
                        accepted is scope.candidate_capture_preparation is preparation
                    )
                elif operation == "capture":
                    assert accepted is scope._frozen
                else:
                    assert accepted is scope.editable is workspace
                    assert scope._frozen is None
        finally:
            native.operation_release.set()

    asyncio.run(scenario())
    assert native.calls[-1] == "close"
    assert len(set(native.threads)) == 1


@pytest.mark.parametrize(
    ("operation", "malformed"),
    [
        ("candidate-prepare", False),
        ("candidate-prepare", True),
        ("capture", False),
        ("capture", True),
        ("verify-candidate", False),
        ("release-candidate", False),
        ("release-candidate", True),
    ],
)
def test_failed_or_malformed_candidate_results_seal_without_reusable_admission(
    monkeypatch: pytest.MonkeyPatch, operation: str, malformed: bool
) -> None:
    native = install(monkeypatch)

    async def scenario() -> None:
        async with bootstrap.open_worker_workspace(native.binding, policy()) as scope:
            preparation = await prepare(scope, native)
            if operation != "candidate-prepare":
                await scope.prepare_candidate_capture(native.source)
                workspace = await scope.handoff(preparation.materialization)
                if operation != "capture":
                    frozen = await capture(scope, preparation, workspace)
            if malformed:
                native.results[operation] = object()
            else:
                native.errors[operation] = WorkspaceUnavailable(
                    "Fixture native failure."
                )
            with pytest.raises(WorkspaceUnavailable):
                if operation == "candidate-prepare":
                    await scope.prepare_candidate_capture(native.source)
                elif operation == "capture":
                    await capture(scope, preparation, workspace)
                elif operation == "verify-candidate":
                    await scope.verify_candidate(frozen)
                else:
                    await scope.release_candidate(frozen)
            assert native.sealed.is_set()
            with pytest.raises(WorkspaceUnavailable):
                await scope.verify(native.reservation)

    asyncio.run(scenario())
    assert native.calls.count(operation) == 1


@pytest.mark.parametrize(
    "changed", ["preparation", "editable", "capture-id", "cycle", "commit"]
)
def test_valid_but_mismatched_capture_result_is_never_cached(
    monkeypatch: pytest.MonkeyPatch, changed: str
) -> None:
    native = install(monkeypatch)

    def transform(frozen: FrozenCandidate) -> FrozenCandidate:
        if changed == "preparation":
            return replace(frozen, preparation=replace(frozen.preparation))
        if changed == "editable":
            return replace(frozen, editable=replace(frozen.editable))
        if changed == "capture-id":
            identity = uuid4()
            return replace(
                frozen,
                capture_id=identity,
                git_directory=frozen.worktree + "\\.git\\devrelay\\" + str(identity),
            )
        if changed == "commit":
            return replace(
                frozen,
                snapshot=build_candidate_snapshot(
                    execution_id=frozen.snapshot.execution_id,
                    cycle=frozen.snapshot.cycle,
                    source=native.source,
                    branch=frozen.snapshot.authorized_branch,
                    files=frozen.snapshot.files,
                    commit=replace(commit(), message="Different commit intent\n"),
                ),
            )
        return replace(frozen, snapshot=replace(frozen.snapshot, cycle=1))

    native.results["capture-transform"] = transform

    async def scenario() -> None:
        async with bootstrap.open_worker_workspace(native.binding, policy()) as scope:
            preparation, workspace = await editable(scope, native)
            with pytest.raises(WorkspaceUnavailable):
                await capture(scope, preparation, workspace)
            assert scope._frozen is None
            assert native.sealed.is_set()

    asyncio.run(scenario())


def test_capture_requires_successful_handoff_and_candidate_preparation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    native = install(monkeypatch)

    async def scenario() -> None:
        async with bootstrap.open_worker_workspace(native.binding, policy()) as scope:
            preparation = await prepare(scope, native)
            with pytest.raises(WorkspaceUnavailable):
                await capture(scope, preparation, cast(EditableWorkspace, None))
            await scope.prepare_candidate_capture(native.source)
            with pytest.raises(WorkspaceUnavailable):
                await capture(scope, preparation, cast(EditableWorkspace, None))
            assert native.helper.captures == []
            assert "capture" not in native.calls

    asyncio.run(scenario())


def test_candidate_capture_opt_in_cannot_follow_editable_handoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    native = install(monkeypatch)

    async def scenario() -> None:
        async with bootstrap.open_worker_workspace(native.binding, policy()) as scope:
            preparation = await prepare(scope, native)
            await scope.handoff(preparation.materialization)
            with pytest.raises(WorkspaceUnavailable):
                await scope.prepare_candidate_capture(native.source)
            assert scope.candidate_capture_preparation is None
            assert "candidate-prepare" not in native.calls

    asyncio.run(scenario())
