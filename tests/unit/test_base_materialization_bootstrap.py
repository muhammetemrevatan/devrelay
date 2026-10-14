import asyncio
import threading
from dataclasses import replace
from typing import Any

import pytest

import devrelay.bootstrap.workspaces as bootstrap
from devrelay.application.contracts.base_sources import (
    BaseMaterialization,
    BaseSourceSnapshot,
)
from devrelay.application.contracts.workspace_recovery import WorkspaceLocation
from devrelay.application.contracts.workspaces import (
    WorkspaceDirectoryIdentity,
    WorkspaceReservation,
)
from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from devrelay.infrastructure.workspace.windows import WindowsWorkspacePolicy
from tests.base_source_fixtures import source_fixture
from tests.workspace_fixtures import workspace_binding


class Native:
    def __init__(self, binding: Any, policy: WindowsWorkspacePolicy) -> None:
        self.binding = binding
        self.location = WorkspaceLocation(
            root_directory=policy.root_directory,
            policy_digest=policy.fingerprint(),
            identity=WorkspaceDirectoryIdentity(volume_serial_number=1, file_index=1),
        )
        self.reservation = WorkspaceReservation(
            binding=binding,
            directory_name=str(binding.execution.intent.execution_id),
            identity=WorkspaceDirectoryIdentity(volume_serial_number=1, file_index=2),
        )
        self.calls: list[str] = []
        self.threads: list[int] = []
        self.block = False
        self.started = threading.Event()
        self.release = threading.Event()
        self.sealed = threading.Event()

    def record(self, name: str) -> None:
        self.calls.append(name)
        self.threads.append(threading.get_ident())

    def prepare(self) -> WorkspaceLocation:
        self.record("prepare")
        return self.location

    def reserve(self, location: WorkspaceLocation) -> WorkspaceReservation:
        self.record("reserve")
        assert location == self.location
        return self.reservation

    def verify_reservation(self, reservation: WorkspaceReservation) -> None:
        self.record("verify")
        assert reservation is self.reservation

    def materialize(
        self, reservation: WorkspaceReservation, snapshot: BaseSourceSnapshot
    ) -> BaseMaterialization:
        self.record("write")
        self.started.set()
        if self.block and not self.release.wait(5):
            raise AssertionError("Fixture native operation was not released.")
        self.record("settled")
        return BaseMaterialization(
            reservation=reservation,
            source=snapshot.request,
            root_tree_sha=snapshot.root_tree_sha,
            manifest_digest=snapshot.fingerprint(),
            file_count=len(snapshot.files),
            total_bytes=sum(len(item.content) for item in snapshot.files),
        )

    def seal_materialization(self) -> None:
        self.sealed.set()

    def close(self) -> None:
        self.record("close")


def test_cached_reservation_identity_and_all_native_work_have_one_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    binding = workspace_binding()
    policy = WindowsWorkspacePolicy(
        root_directory=r"C:\workspace", protected_directories=(r"C:\private",)
    )
    native = Native(binding, policy)
    monkeypatch.setattr(bootstrap, "WindowsWorkspaceReservation", lambda *args: native)
    source = source_fixture(binding).snapshot

    async def exercise() -> None:
        async with bootstrap.open_worker_workspace(binding, policy) as scope:
            location = await scope.prepare()
            reservation = await scope.reserve(location)
            assert scope.reservation is native.reservation is reservation
            with pytest.raises(WorkspaceUnavailable):
                await scope.verify(replace(reservation))
            await scope.verify(reservation)
            result = await scope.materialize(reservation, source)
            assert result.reservation is reservation
            with pytest.raises(WorkspaceUnavailable):
                await scope.materialize(reservation, source)

    asyncio.run(exercise())
    assert native.calls == ["prepare", "reserve", "verify", "write", "settled", "close"]
    assert len(set(native.threads)) == 1
    assert threading.get_ident() not in native.threads
    assert native.sealed.is_set()


def test_cancelled_write_seals_before_fifo_cleanup_and_never_admits_more_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    binding = workspace_binding()
    policy = WindowsWorkspacePolicy(
        root_directory=r"C:\workspace", protected_directories=(r"C:\private",)
    )
    native = Native(binding, policy)
    native.block = True
    monkeypatch.setattr(bootstrap, "WindowsWorkspaceReservation", lambda *args: native)
    source = source_fixture(binding).snapshot

    async def exercise() -> None:
        try:
            async with bootstrap.open_worker_workspace(binding, policy) as scope:
                location = await scope.prepare()
                reservation = await scope.reserve(location)
                task = asyncio.create_task(scope.materialize(reservation, source))
                assert await asyncio.to_thread(native.started.wait, 2)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                assert native.sealed.is_set()
                assert "settled" not in native.calls
                with pytest.raises(WorkspaceUnavailable):
                    await scope.verify(reservation)
                with pytest.raises(WorkspaceUnavailable):
                    await scope.materialize(reservation, source)
                native.release.set()
        finally:
            native.release.set()

    asyncio.run(exercise())
    assert native.calls == ["prepare", "reserve", "write", "settled", "close"]
    assert len(set(native.threads)) == 1
