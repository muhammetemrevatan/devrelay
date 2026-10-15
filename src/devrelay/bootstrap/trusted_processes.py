import asyncio
from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

from devrelay.application.contracts.processes import (
    ExecutionProcessBinding,
    ProcessResourceLimits,
)
from devrelay.application.contracts.trusted_processes import (
    TrustedProcessObservation,
    TrustedProcessSpec,
)
from devrelay.application.ports.process_scope import ProcessScopeUnavailable
from devrelay.bootstrap.processes import ProcessOperationOwner
from devrelay.infrastructure.worker_processes.trusted_windows import (
    WindowsTrustedProcess,
)
from devrelay.infrastructure.worker_processes.windows import WindowsJobObject


class WindowsTrustedProcessScope(ProcessOperationOwner):
    """Explicit trusted host commands only; no agent identity or sandbox setup."""

    def __init__(
        self, native: WindowsTrustedProcess, executor: ThreadPoolExecutor
    ) -> None:
        super().__init__(executor, native.seal)
        self._native = native
        self._prepare_started = False
        self._prepared = False
        self._resume_started = False

    @property
    def binding(self) -> ExecutionProcessBinding:
        return self._native.binding

    def seal(self) -> None:
        self._native.seal()

    async def prepare(self, spec: TrustedProcessSpec) -> None:
        if self._prepare_started:
            raise ProcessScopeUnavailable(
                "Trusted process preparation already started."
            )
        self._prepare_started = True
        await self._invoke(lambda: self._native.prepare(spec))
        self._prepared = True

    async def resume(self) -> None:
        if not self._prepared or self._resume_started:
            raise ProcessScopeUnavailable("Trusted process resume is unavailable.")
        self._resume_started = True
        await self._invoke(self._native.resume)

    async def observe(self) -> TrustedProcessObservation:
        return await self._invoke(self._native.observe)

    async def terminate(self) -> None:
        self.seal()
        await self._invoke(self._native.terminate)

    async def active_processes(self) -> int:
        return await self._invoke(self._native.active_processes)


@asynccontextmanager
async def open_trusted_process_scope(
    binding: ExecutionProcessBinding, limits: ProcessResourceLimits
) -> AsyncIterator[WindowsTrustedProcessScope]:
    """Own one explicit native process and job; cleanup is not empty evidence.

    This factory is not called by worker startup or journal recovery. It runs no
    process until a trusted caller explicitly prepares and resumes one. The caller
    supplies effective identity/policy; generated agent/check execution stays gated.
    """
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="devrelay-trusted")
    loop = asyncio.get_running_loop()
    job: WindowsJobObject | None = None
    native: WindowsTrustedProcess | None = None
    scope: WindowsTrustedProcessScope | None = None

    def initialize() -> WindowsTrustedProcess:
        nonlocal job, native
        job = WindowsJobObject(binding, limits)
        native = WindowsTrustedProcess(job)
        return native

    future = loop.run_in_executor(executor, initialize)
    future.add_done_callback(
        lambda done: None if done.cancelled() else done.exception()
    )
    try:
        initialized = await asyncio.shield(future)
        scope = WindowsTrustedProcessScope(initialized, executor)
        yield scope
    finally:
        if scope is not None:
            scope.stop_admission()

        def close() -> None:
            try:
                if native is not None:
                    native.seal()
                    native.close()
            finally:
                if job is not None:
                    job.seal()
                    job.close()

        cleanup = loop.run_in_executor(executor, close)
        cleanup.add_done_callback(
            lambda done: None if done.cancelled() else done.exception()
        )
        try:
            await asyncio.shield(cleanup)
        finally:
            await asyncio.to_thread(executor.shutdown, wait=True, cancel_futures=False)
