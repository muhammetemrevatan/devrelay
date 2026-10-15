import asyncio
from collections.abc import AsyncIterator, Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import TypeVar, cast

from devrelay.application.contracts.base_sources import (
    BaseMaterialization,
    BaseSourceSnapshot,
)
from devrelay.application.contracts.editable_workspaces import EditableWorkspace
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

_Result = TypeVar("_Result")


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
        self._reservation: WorkspaceReservation | None = None
        self._materialization_started = False
        self._materialization: BaseMaterialization | None = None
        self._handoff_started = False
        self._sealed = False
        self._admission = asyncio.Lock()
        self._pending: asyncio.Future[object] | None = None

    @property
    def binding(self) -> WorkspaceBinding:
        return self._native.binding

    @property
    def reservation(self) -> WorkspaceReservation | None:
        return self._reservation

    @property
    def location(self) -> WorkspaceLocation | None:
        return self._location

    @property
    def materialization(self) -> BaseMaterialization | None:
        return self._materialization

    def seal(self) -> None:
        self._sealed = True
        self._native.seal_materialization()

    def stop_admission(self) -> None:
        self._closed = True
        self.seal()

    async def _invoke(self, operation: Callable[[], _Result]) -> _Result:
        async with self._admission:
            if (
                self._closed
                or self._sealed
                or (self._pending is not None and not self._pending.done())
            ):
                raise WorkspaceUnavailable(
                    "A live native workspace operation is unavailable."
                )
            future = asyncio.get_running_loop().run_in_executor(
                self._executor, operation
            )
            self._pending = cast(asyncio.Future[object], future)
            future.add_done_callback(
                lambda done: None if done.cancelled() else done.exception()
            )
            try:
                result = await asyncio.shield(future)
                if self._closed or self._sealed:
                    raise WorkspaceUnavailable("Live workspace operation stopped.")
                return result
            finally:
                if future.done():
                    self._pending = None

    async def prepare(self) -> WorkspaceLocation:
        if self._closed or self._preparation_started:
            raise WorkspaceUnavailable("Workspace reservation is unavailable.")
        self._preparation_started = True
        location = await self._invoke(self._native.prepare)
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
        reservation = await self._invoke(lambda: self._native.reserve(location))
        if not isinstance(reservation, WorkspaceReservation):
            raise WorkspaceUnavailable("Live reservation is unavailable.")
        self._reservation = reservation
        return reservation

    async def verify(self, reservation: WorkspaceReservation) -> None:
        if reservation is not self._reservation:
            raise WorkspaceUnavailable("Live reservation changed original input.")
        await self._invoke(lambda: self._native.verify_reservation(reservation))

    async def materialize(
        self, reservation: WorkspaceReservation, snapshot: BaseSourceSnapshot
    ) -> BaseMaterialization:
        if self._materialization_started or reservation is not self._reservation:
            raise WorkspaceUnavailable("Live materialization is unavailable.")
        self._materialization_started = True
        try:
            result = await self._invoke(
                lambda: self._native.materialize(reservation, snapshot)
            )
            if not isinstance(result, BaseMaterialization):
                raise WorkspaceUnavailable("Live materialization is unavailable.")
            self._materialization = result
            return result
        except BaseException:
            self.seal()
            raise

    async def handoff(self, materialization: BaseMaterialization) -> EditableWorkspace:
        if self._handoff_started or materialization is not self._materialization:
            raise WorkspaceUnavailable("Live editable workspace is unavailable.")
        self._handoff_started = True
        try:
            return await self._invoke(
                lambda: self._native.handoff_materialized_base(materialization)
            )
        except BaseException:
            self.seal()
            raise


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
