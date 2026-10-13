import asyncio
from collections.abc import AsyncIterator, Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import TypeVar, cast

from devrelay.application.contracts.processes import (
    ExecutionProcessBinding,
    ProcessResourceLimits,
)
from devrelay.application.ports.process_scope import ProcessScopeUnavailable
from devrelay.infrastructure.worker_processes.windows import WindowsJobObject

_Result = TypeVar("_Result")


class WindowsProcessScope:
    """Own native operations on one executor; never creates or resumes a process."""

    def __init__(self, native: WindowsJobObject, executor: ThreadPoolExecutor) -> None:
        self._native = native
        self._executor = executor
        self._closed = False
        self._admission = asyncio.Lock()
        self._pending: asyncio.Future[object] | None = None

    @property
    def binding(self) -> ExecutionProcessBinding:
        return self._native.binding

    def seal(self) -> None:
        self._native.seal()

    def stop_admission(self) -> None:
        self._closed = True
        self.seal()

    async def _invoke(self, operation: Callable[[], _Result]) -> _Result:
        async with self._admission:
            if self._closed:
                raise ProcessScopeUnavailable("Windows process scope is unavailable.")
            if self._pending is not None and not self._pending.done():
                raise ProcessScopeUnavailable(
                    "A native process operation is unsettled."
                )
            future = asyncio.get_running_loop().run_in_executor(
                self._executor, operation
            )
            self._pending = cast(asyncio.Future[object], future)
            future.add_done_callback(
                lambda done: None if done.cancelled() else done.exception()
            )
            try:
                return await asyncio.shield(future)
            finally:
                if future.done():
                    self._pending = None

    async def attach_suspended_process(self, process_handle: int) -> None:
        """Borrow a caller-owned handle; suspension and launch policy are external."""
        await self._invoke(
            lambda: self._native.attach_suspended_process(process_handle)
        )

    async def terminate(self) -> None:
        self.seal()
        await self._invoke(self._native.terminate)

    async def active_processes(self) -> int:
        return await self._invoke(self._native.active_processes)


@asynccontextmanager
async def open_worker_process_scope(
    binding: ExecutionProcessBinding, limits: ProcessResourceLimits
) -> AsyncIterator[WindowsProcessScope]:
    """Create management resources only, with no replay-to-launch conversion.

    Trusted callers own process creation, effective identity and sandbox policy.
    Cleanup closes the scope; it never reports independent empty-scope evidence.
    """
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="devrelay-process")
    loop = asyncio.get_running_loop()
    native: WindowsJobObject | None = None
    scope: WindowsProcessScope | None = None

    def initialize() -> WindowsJobObject:
        nonlocal native
        native = WindowsJobObject(binding, limits)
        return native

    future = loop.run_in_executor(executor, initialize)
    future.add_done_callback(
        lambda done: None if done.cancelled() else done.exception()
    )
    try:
        initialized = await asyncio.shield(future)
        scope = WindowsProcessScope(initialized, executor)
        yield scope
    finally:
        if scope is not None:
            scope.stop_admission()

        def close() -> None:
            if native is not None:
                native.seal()
                native.close()

        # FIFO cleanup follows initialization and any uncancelled native operation.
        cleanup = loop.run_in_executor(executor, close)
        cleanup.add_done_callback(
            lambda done: None if done.cancelled() else done.exception()
        )
        try:
            await asyncio.shield(cleanup)
        finally:
            await asyncio.to_thread(executor.shutdown, wait=True, cancel_futures=False)
