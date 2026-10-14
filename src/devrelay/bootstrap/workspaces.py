import asyncio
from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

from devrelay.application.contracts.workspace_recovery import WorkspaceLocation
from devrelay.application.contracts.workspaces import (
    WorkspaceBinding,
    WorkspaceReservation,
)
from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from devrelay.infrastructure.workspace.windows import (
    WindowsWorkspacePolicy,
    WindowsWorkspaceReservation,
)


class WindowsWorkspaceScope:
    """One-shot filesystem work; cancellation cannot open a second admission."""

    def __init__(
        self, native: WindowsWorkspaceReservation, executor: ThreadPoolExecutor
    ) -> None:
        self._native = native
        self._executor = executor
        self._closed = False
        self._preparation_started = False
        self._reservation_started = False
        self._location: WorkspaceLocation | None = None

    @property
    def binding(self) -> WorkspaceBinding:
        return self._native.binding

    def stop_admission(self) -> None:
        self._closed = True

    async def prepare(self) -> WorkspaceLocation:
        if self._closed or self._preparation_started:
            raise WorkspaceUnavailable("Workspace reservation is unavailable.")
        self._preparation_started = True
        future = asyncio.get_running_loop().run_in_executor(
            self._executor, self._native.prepare
        )
        future.add_done_callback(
            lambda done: None if done.cancelled() else done.exception()
        )
        location = await asyncio.shield(future)
        if not isinstance(location, WorkspaceLocation) or self._closed:
            raise WorkspaceUnavailable("Workspace location is unavailable.")
        self._location = location
        return location

    async def reserve(self, location: WorkspaceLocation) -> WorkspaceReservation:
        if (
            self._closed
            or self._reservation_started
            or self._location is None
            or location != self._location
        ):
            raise WorkspaceUnavailable("Workspace reservation is unavailable.")
        self._reservation_started = True
        future = asyncio.get_running_loop().run_in_executor(
            self._executor, self._native.reserve, location
        )
        future.add_done_callback(
            lambda done: None if done.cancelled() else done.exception()
        )
        return await asyncio.shield(future)


@asynccontextmanager
async def open_worker_workspace(
    binding: WorkspaceBinding, policy: WindowsWorkspacePolicy
) -> AsyncIterator[WindowsWorkspaceScope]:
    """Own directory handles off-loop; cleanup never deletes a reservation.

    The configured root is trusted and externally coordinator-controlled. This
    factory supplies no Git checkout, access policy or restart recovery.
    """
    executor = ThreadPoolExecutor(
        max_workers=1, thread_name_prefix="devrelay-workspace"
    )
    loop = asyncio.get_running_loop()
    native: WindowsWorkspaceReservation | None = None
    scope: WindowsWorkspaceScope | None = None

    def initialize() -> WindowsWorkspaceReservation:
        nonlocal native
        native = WindowsWorkspaceReservation(binding, policy)
        return native

    initialized = loop.run_in_executor(executor, initialize)
    initialized.add_done_callback(
        lambda done: None if done.cancelled() else done.exception()
    )
    try:
        scope = WindowsWorkspaceScope(await asyncio.shield(initialized), executor)
        yield scope
    finally:
        if scope is not None:
            scope.stop_admission()

        def close() -> None:
            if native is not None:
                native.close()

        # FIFO close follows the actual native operation, including cancelled awaits.
        cleanup = loop.run_in_executor(executor, close)
        cleanup.add_done_callback(
            lambda done: None if done.cancelled() else done.exception()
        )
        try:
            await asyncio.shield(cleanup)
        finally:
            await asyncio.to_thread(executor.shutdown, wait=True, cancel_futures=False)
