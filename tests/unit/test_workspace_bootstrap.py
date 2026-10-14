import asyncio
import threading
from collections.abc import Iterator
from dataclasses import dataclass, field

import pytest

import devrelay.bootstrap.workspaces as bootstrap
from devrelay.application.contracts.workspace_recovery import WorkspaceLocation
from devrelay.application.contracts.workspaces import (
    WorkspaceBinding,
    WorkspaceDirectoryIdentity,
    WorkspaceReservation,
)
from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from devrelay.infrastructure.workspace.windows import WindowsWorkspacePolicy
from tests.workspace_fixtures import workspace_binding


@dataclass
class NativeState:
    prepare_started: threading.Event = field(default_factory=threading.Event)
    release_prepare: threading.Event = field(default_factory=threading.Event)
    reserve_started: threading.Event = field(default_factory=threading.Event)
    release_reserve: threading.Event = field(default_factory=threading.Event)
    initialize_started: threading.Event = field(default_factory=threading.Event)
    release_initialize: threading.Event = field(default_factory=threading.Event)
    threads: list[int] = field(default_factory=list)
    calls: list[str] = field(default_factory=list)
    block_initialize: bool = False
    block_reserve: bool = False
    block_prepare: bool = False
    seals: int = 0


@pytest.fixture
def native_state(monkeypatch: pytest.MonkeyPatch) -> Iterator[NativeState]:
    state = NativeState()

    class Native:
        def __init__(
            self, binding: WorkspaceBinding, policy: WindowsWorkspacePolicy
        ) -> None:
            self.binding = binding
            self.location = WorkspaceLocation(
                root_directory=policy.root_directory,
                policy_digest=policy.fingerprint(),
                identity=WorkspaceDirectoryIdentity(
                    volume_serial_number=1, file_index=1
                ),
            )
            state.calls.append("initialize")
            state.threads.append(threading.get_ident())
            state.initialize_started.set()
            if state.block_initialize and not state.release_initialize.wait(5):
                raise RuntimeError("Fixture initialization was not released.")

        def prepare(self) -> WorkspaceLocation:
            state.calls.append("prepare")
            state.threads.append(threading.get_ident())
            state.prepare_started.set()
            if state.block_prepare and not state.release_prepare.wait(5):
                raise RuntimeError("Fixture root observation was not released.")
            return self.location

        def reserve(self, location: WorkspaceLocation) -> WorkspaceReservation:
            assert location == self.location
            state.calls.append("reserve")
            state.threads.append(threading.get_ident())
            state.reserve_started.set()
            if state.block_reserve and not state.release_reserve.wait(5):
                raise RuntimeError("Fixture reservation was not released.")
            return WorkspaceReservation(
                binding=self.binding,
                directory_name=str(self.binding.execution.intent.execution_id),
                identity=WorkspaceDirectoryIdentity(
                    volume_serial_number=1, file_index=2
                ),
            )

        def close(self) -> None:
            state.calls.append("close")
            state.threads.append(threading.get_ident())

        def seal_materialization(self) -> None:
            state.seals += 1

    monkeypatch.setattr(bootstrap, "WindowsWorkspaceReservation", Native)
    try:
        yield state
    finally:
        state.release_initialize.set()
        state.release_reserve.set()
        state.release_prepare.set()


def policy() -> WindowsWorkspacePolicy:
    return WindowsWorkspacePolicy(
        root_directory="C:\\workspaces", protected_directories=("C:\\private",)
    )


def test_factory_owns_one_thread_and_leaves_reservation_on_close(
    native_state: NativeState,
) -> None:
    binding = workspace_binding()
    main_thread = threading.get_ident()

    async def exercise() -> WorkspaceReservation:
        async with bootstrap.open_worker_workspace(binding, policy()) as scope:
            location = await scope.prepare()
            return await scope.reserve(location)

    result = asyncio.run(exercise())

    assert result.binding == binding
    assert native_state.calls == ["initialize", "prepare", "reserve", "close"]
    assert len(set(native_state.threads)) == 1
    assert main_thread not in native_state.threads


def test_cancelled_reservation_remains_one_shot_until_native_cleanup(
    native_state: NativeState,
) -> None:
    binding = workspace_binding()
    native_state.block_reserve = True

    async def exercise() -> None:
        async with bootstrap.open_worker_workspace(binding, policy()) as scope:
            location = await scope.prepare()
            task = asyncio.create_task(scope.reserve(location))
            started = await asyncio.to_thread(native_state.reserve_started.wait, 2)
            assert started
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            with pytest.raises(WorkspaceUnavailable):
                await scope.reserve(location)
            native_state.release_reserve.set()

    asyncio.run(exercise())

    assert native_state.calls == ["initialize", "prepare", "reserve", "close"]


def test_closed_scope_never_admits_reservation(native_state: NativeState) -> None:
    binding = workspace_binding()

    async def exercise() -> None:
        async with bootstrap.open_worker_workspace(binding, policy()) as scope:
            scope.stop_admission()
            with pytest.raises(WorkspaceUnavailable):
                await scope.prepare()

    asyncio.run(exercise())

    assert native_state.calls == ["initialize", "close"]


def test_cancelled_root_observation_never_admits_creation(
    native_state: NativeState,
) -> None:
    binding = workspace_binding()
    native_state.block_prepare = True
    location = WorkspaceLocation(
        root_directory=policy().root_directory,
        policy_digest=policy().fingerprint(),
        identity=WorkspaceDirectoryIdentity(volume_serial_number=1, file_index=1),
    )

    async def exercise() -> None:
        async with bootstrap.open_worker_workspace(binding, policy()) as scope:
            task = asyncio.create_task(scope.prepare())
            started = await asyncio.to_thread(native_state.prepare_started.wait, 2)
            assert started
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            with pytest.raises(WorkspaceUnavailable):
                await scope.reserve(location)
            with pytest.raises(WorkspaceUnavailable):
                await scope.prepare()
            native_state.release_prepare.set()

    asyncio.run(exercise())

    assert native_state.calls == ["initialize", "prepare", "close"]


def test_cancellation_during_setup_closes_eventual_native_resource(
    native_state: NativeState,
) -> None:
    binding = workspace_binding()
    native_state.block_initialize = True

    async def initialize() -> None:
        async with bootstrap.open_worker_workspace(binding, policy()):
            raise AssertionError("Cancelled setup must never yield a scope.")

    async def exercise() -> None:
        task = asyncio.create_task(initialize())
        started = await asyncio.to_thread(native_state.initialize_started.wait, 2)
        assert started
        task.cancel()
        native_state.release_initialize.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(exercise())

    assert native_state.calls == ["initialize", "close"]
