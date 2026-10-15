import asyncio
import threading
from dataclasses import replace

import pytest

import devrelay.bootstrap.workspaces as bootstrap
from devrelay.application.contracts.base_sources import BaseMaterialization
from devrelay.application.contracts.editable_workspaces import EditableWorkspace
from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from devrelay.infrastructure.workspace.windows import WindowsWorkspacePolicy
from tests.base_source_fixtures import source_fixture
from tests.unit.test_base_materialization_bootstrap import Native
from tests.workspace_fixtures import workspace_binding


class EditableNative(Native):
    def __init__(self) -> None:
        super().__init__(workspace_binding(), policy())
        self.handoff_started = threading.Event()
        self.release_handoff = threading.Event()
        self.block_handoff = False
        self.handoff_error: Exception | None = None
        self.handed_off: list[BaseMaterialization] = []
        self.materialization_settled = threading.Event()

    def record(self, name: str) -> None:
        super().record(name)
        if name == "settled":
            self.materialization_settled.set()

    def handoff_materialized_base(
        self, materialization: BaseMaterialization
    ) -> EditableWorkspace:
        self.record("handoff")
        self.handed_off.append(materialization)
        self.handoff_started.set()
        if self.block_handoff and not self.release_handoff.wait(5):
            raise AssertionError("Fixture native handoff was not released.")
        self.record("handoff-settled")
        if self.handoff_error is not None:
            raise self.handoff_error
        return EditableWorkspace(
            materialization=materialization, location=self.location
        )


def policy() -> WindowsWorkspacePolicy:
    return WindowsWorkspacePolicy(
        root_directory=r"C:\workspace", protected_directories=(r"C:\private",)
    )


def install_native(monkeypatch: pytest.MonkeyPatch) -> EditableNative:
    native = EditableNative()
    monkeypatch.setattr(bootstrap, "WindowsWorkspaceReservation", lambda *args: native)
    return native


async def export(scope: bootstrap.WindowsWorkspaceScope) -> BaseMaterialization:
    location = await scope.prepare()
    reservation = await scope.reserve(location)
    return await scope.materialize(reservation, source_fixture(scope.binding).snapshot)


def test_explicit_handoff_uses_exact_cached_export_on_the_owning_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    native = install_native(monkeypatch)

    async def scenario() -> None:
        async with bootstrap.open_worker_workspace(native.binding, policy()) as scope:
            assert scope.materialization is None
            materialization = await export(scope)
            assert scope.materialization is materialization
            assert native.handed_off == []
            editable = await scope.handoff(materialization)
            assert editable.materialization is materialization
            assert editable.location is native.location
            assert not native.sealed.is_set()
            with pytest.raises(WorkspaceUnavailable):
                await scope.handoff(materialization)

    asyncio.run(scenario())

    assert native.calls == [
        "prepare",
        "reserve",
        "write",
        "settled",
        "handoff",
        "handoff-settled",
        "close",
    ]
    assert len(set(native.threads)) == 1
    assert threading.get_ident() not in native.threads
    assert native.sealed.is_set()


def test_equal_reconstructed_export_cannot_reach_native_handoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    native = install_native(monkeypatch)

    async def scenario() -> None:
        async with bootstrap.open_worker_workspace(native.binding, policy()) as scope:
            materialization = await export(scope)
            with pytest.raises(WorkspaceUnavailable):
                await scope.handoff(replace(materialization))
            assert native.handed_off == []
            assert not native.sealed.is_set()
            assert (
                await scope.handoff(materialization)
            ).materialization is materialization

    asyncio.run(scenario())


@pytest.mark.parametrize("operation", ["write", "handoff"])
def test_cancelled_native_work_seals_before_fifo_cleanup_and_does_not_adopt_late_result(
    operation: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    native = install_native(monkeypatch)
    native.block = operation == "write"
    native.block_handoff = operation == "handoff"

    async def scenario() -> None:
        try:
            async with bootstrap.open_worker_workspace(
                native.binding, policy()
            ) as scope:
                task: asyncio.Task[BaseMaterialization | EditableWorkspace]
                if operation == "write":
                    task = asyncio.create_task(export(scope))
                    started, release = native.started, native.release
                else:
                    materialization = await export(scope)
                    task = asyncio.create_task(scope.handoff(materialization))
                    started, release = native.handoff_started, native.release_handoff
                assert await asyncio.to_thread(started.wait, 2)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                assert native.sealed.is_set()
                assert "close" not in native.calls
                assert (
                    "settled" if operation == "write" else "handoff-settled"
                ) not in native.calls
                if operation == "write":
                    assert scope.materialization is None
                else:
                    assert scope.materialization is materialization
                    with pytest.raises(WorkspaceUnavailable):
                        await scope.handoff(materialization)
                assert scope.reservation is not None
                with pytest.raises(WorkspaceUnavailable):
                    await scope.verify(scope.reservation)
                release.set()
                if operation == "write":
                    assert await asyncio.to_thread(
                        native.materialization_settled.wait, 2
                    )
                    await asyncio.sleep(0)
                    assert scope.materialization is None
        finally:
            native.release.set()
            native.release_handoff.set()

    asyncio.run(scenario())

    assert native.calls[-1] == "close"
    assert len(set(native.threads)) == 1
    if operation == "handoff":
        assert native.calls[-2] == "handoff-settled"
        assert len(native.handed_off) == 1


def test_failed_native_handoff_seals_and_consumes_admission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    native = install_native(monkeypatch)
    native.handoff_error = WorkspaceUnavailable("Fixture readback failed.")

    async def scenario() -> None:
        async with bootstrap.open_worker_workspace(native.binding, policy()) as scope:
            materialization = await export(scope)
            with pytest.raises(WorkspaceUnavailable, match="readback"):
                await scope.handoff(materialization)
            assert native.sealed.is_set()
            with pytest.raises(WorkspaceUnavailable):
                await scope.handoff(materialization)

    asyncio.run(scenario())

    assert len(native.handed_off) == 1


def test_concurrent_handoff_cannot_queue_a_second_native_transition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    native = install_native(monkeypatch)
    native.block_handoff = True

    async def scenario() -> None:
        try:
            async with bootstrap.open_worker_workspace(
                native.binding, policy()
            ) as scope:
                materialization = await export(scope)
                first = asyncio.create_task(scope.handoff(materialization))
                assert await asyncio.to_thread(native.handoff_started.wait, 2)
                with pytest.raises(WorkspaceUnavailable):
                    await scope.handoff(materialization)
                assert not native.sealed.is_set()
                native.release_handoff.set()
                assert (await first).materialization is materialization
        finally:
            native.release_handoff.set()

    asyncio.run(scenario())

    assert len(native.handed_off) == 1


def test_closed_scope_cannot_handoff_retained_export(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    native = install_native(monkeypatch)

    async def scenario() -> None:
        async with bootstrap.open_worker_workspace(native.binding, policy()) as scope:
            materialization = await export(scope)
        with pytest.raises(WorkspaceUnavailable):
            await scope.handoff(materialization)

    asyncio.run(scenario())

    assert native.handed_off == []
