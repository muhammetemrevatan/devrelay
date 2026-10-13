import asyncio
from collections.abc import Awaitable, Callable
from math import isfinite

from devrelay.application.contracts.processes import (
    ExecutionProcessBinding,
    ProcessManagementPolicy,
    ProcessScopeResult,
    ProcessStopReason,
    ProcessStopStatus,
)
from devrelay.application.ports.process_scope import (
    ProcessScope,
    ProcessScopeUnavailable,
)
from devrelay.application.worker_executions import ExecutionAuthorityGuard

_DEFAULT_POLICY = ProcessManagementPolicy()


async def _wait_for_tick(task: asyncio.Future[object] | None, timeout: float) -> None:
    if task is None:
        await asyncio.sleep(timeout)
    else:
        await asyncio.wait({task}, timeout=timeout)


def _consume_completion(task: asyncio.Future[object]) -> None:
    if not task.cancelled():
        task.exception()


def _count(value: object) -> int:
    if type(value) is not int or not 0 <= value <= 128:
        raise ProcessScopeUnavailable("Process accounting is unsupported.")
    return value


class ManageExecutionProcesses:
    """Manage an existing bound scope only, never launch or release a reservation.

    Clock and supervisor must share a suspend-inclusive epoch. A timed-out native
    operation remains unresolved even if its async wrapper can later be cancelled.
    """

    def __init__(
        self,
        expected: ExecutionProcessBinding,
        scope: ProcessScope,
        supervisor: ExecutionAuthorityGuard,
        clock: Callable[[], float],
        policy: ProcessManagementPolicy = _DEFAULT_POLICY,
    ) -> None:
        if not isinstance(expected, ExecutionProcessBinding) or not isinstance(
            policy, ProcessManagementPolicy
        ):
            raise ValueError("Process management requires typed binding and policy.")
        self._expected, self._scope = expected, scope
        self._supervisor, self._clock, self._policy = supervisor, clock, policy
        self._used = False
        self._sealed = False
        self._uncertain = False
        self._reason: ProcessStopReason | None = None
        self._result: ProcessScopeResult | None = None
        self._last_clock: float | None = None
        self._operation: asyncio.Future[object] | None = None
        self._check_binding()

    @property
    def result(self) -> ProcessScopeResult | None:
        return self._result

    def _check_binding(self) -> None:
        try:
            binding = self._scope.binding
        except Exception:
            raise ProcessScopeUnavailable(
                "Process scope binding is unavailable."
            ) from None
        if (
            not isinstance(binding, ExecutionProcessBinding)
            or binding != self._expected
        ):
            raise ProcessScopeUnavailable(
                "Process scope differs from expected execution."
            )

    def _publish(
        self, status: ProcessStopStatus = ProcessStopStatus.UNKNOWN
    ) -> ProcessScopeResult:
        assert self._reason is not None
        self._result = ProcessScopeResult(
            execution_id=self._expected.intent.execution_id,
            reason=self._reason,
            status=status,
        )
        return self._result

    def _stop(self, reason: ProcessStopReason, *, uncertain: bool = False) -> None:
        if self._reason is None:
            self._reason = reason
        self._uncertain = self._uncertain or uncertain
        self._publish()
        self._check_binding()
        if not self._sealed:
            try:
                self._scope.seal()
            except Exception:
                self._uncertain = True
            else:
                self._sealed = True

    def request_stop(self) -> None:
        self._stop(ProcessStopReason.REQUESTED)

    def _read_clock(self) -> float | None:
        try:
            value = self._clock()
            if (
                type(value) not in (int, float)
                or not 0 <= value <= 2**53 - 1
                or not isfinite(value)
                or (self._last_clock is not None and value < self._last_clock)
            ):
                raise ValueError("Invalid process management clock.")
        except Exception:
            self._stop(ProcessStopReason.CLOCK_FAILED, uncertain=True)
            return None
        self._last_clock = float(value)
        return float(value)

    def _start(self, awaitable: Awaitable[object]) -> asyncio.Future[object]:
        if self._operation is not None:
            raise ProcessScopeUnavailable(
                "Another native scope operation is unresolved."
            )
        task = asyncio.ensure_future(awaitable)
        self._operation = task
        task.add_done_callback(_consume_completion)
        return task

    async def _monitor(self) -> None:
        started = self._read_clock()
        if started is None:
            return
        observations = 0
        operation_started = started
        loop = asyncio.get_running_loop()
        runtime_loop_started = loop.time()
        operation_loop_started = loop.time()
        while self._reason is None:
            now = self._read_clock()
            if now is None:
                break
            self._check_binding()
            try:
                self._supervisor.require_current_assignment(self._expected.reference)
            except Exception:
                self._stop(ProcessStopReason.AUTHORITY_LOST)
                break
            if (
                now - started >= self._policy.maximum_runtime_seconds
                or loop.time() - runtime_loop_started
                >= self._policy.maximum_runtime_seconds
            ):
                self._stop(ProcessStopReason.RUNTIME_LIMIT)
                break
            if self._operation is not None and (
                now - operation_started >= self._policy.operation_timeout_seconds
                or loop.time() - operation_loop_started
                >= self._policy.operation_timeout_seconds
            ):
                self._stop(ProcessStopReason.OBSERVATION_FAILED, uncertain=True)
                break
            if self._operation is not None and self._operation.done():
                task, self._operation = self._operation, None
                try:
                    count = _count(task.result())
                except asyncio.CancelledError:
                    self._stop(ProcessStopReason.OBSERVATION_FAILED, uncertain=True)
                    break
                except Exception:
                    self._stop(ProcessStopReason.OBSERVATION_FAILED, uncertain=True)
                    break
                if count == 0:
                    self._stop(ProcessStopReason.EMPTY)
                    break
                if observations >= self._policy.maximum_observations:
                    self._stop(ProcessStopReason.OBSERVATION_LIMIT)
                    break
                await _wait_for_tick(None, self._policy.monitor_interval_seconds)
                continue
            if self._operation is None:
                self._start(self._scope.active_processes())
                observations += 1
                operation_started, operation_loop_started = now, loop.time()
            await _wait_for_tick(self._operation, self._policy.monitor_interval_seconds)

    def _remaining_stop_budget(
        self, started: float | None, loop_deadline: float
    ) -> float:
        now = self._read_clock()
        remaining = loop_deadline - asyncio.get_running_loop().time()
        if now is not None and started is not None:
            remaining = min(
                remaining, self._policy.stop_timeout_seconds - (now - started)
            )
        if remaining <= 0:
            self._uncertain = True
        return remaining

    async def _settle(self, started: float | None, loop_deadline: float) -> bool:
        task = self._operation
        assert task is not None
        while True:
            remaining = self._remaining_stop_budget(started, loop_deadline)
            if remaining <= 0:
                return False
            if task.done():
                self._operation = None
                return True
            await _wait_for_tick(
                task, min(remaining, self._policy.monitor_interval_seconds)
            )

    async def _finish(self) -> ProcessScopeResult:
        assert self._reason is not None
        if not self._sealed:
            return self._publish()
        loop_deadline = (
            asyncio.get_running_loop().time() + self._policy.stop_timeout_seconds
        )
        started = self._read_clock()
        if self._operation is not None:
            pending = self._operation
            if not await self._settle(started, loop_deadline):
                return self._publish()
            try:
                _count(pending.result())
            except asyncio.CancelledError:
                self._uncertain = True
            except Exception:
                self._uncertain = True
        self._check_binding()
        if self._remaining_stop_budget(started, loop_deadline) <= 0:
            return self._publish()
        termination = self._start(self._scope.terminate())
        if not await self._settle(started, loop_deadline):
            return self._publish()
        try:
            termination.result()
        except asyncio.CancelledError:
            self._uncertain = True
            return self._publish()
        except Exception:
            self._uncertain = True
            return self._publish()
        for _ in range(self._policy.maximum_observations):
            self._check_binding()
            if self._remaining_stop_budget(started, loop_deadline) <= 0:
                return self._publish()
            fresh = self._start(self._scope.active_processes())
            if not await self._settle(started, loop_deadline):
                return self._publish()
            self._check_binding()
            try:
                empty = _count(fresh.result()) == 0
            except asyncio.CancelledError:
                self._uncertain = True
                return self._publish()
            except Exception:
                self._uncertain = True
                return self._publish()
            if empty:
                return self._publish(
                    ProcessStopStatus.UNKNOWN
                    if self._uncertain
                    else ProcessStopStatus.EMPTY_VERIFIED
                )
            remaining = self._remaining_stop_budget(started, loop_deadline)
            if remaining <= 0:
                return self._publish()
            await _wait_for_tick(
                None, min(remaining, self._policy.monitor_interval_seconds)
            )
        return self._publish()

    async def run(self) -> ProcessScopeResult:
        if self._used:
            raise ProcessScopeUnavailable("Process management has already started.")
        self._used = True
        cancelled = False
        try:
            await self._monitor()
        except asyncio.CancelledError:
            cancelled = True
            try:
                self._stop(
                    ProcessStopReason.CANCELLED, uncertain=self._operation is not None
                )
            except ProcessScopeUnavailable:
                raise asyncio.CancelledError from None
        except ProcessScopeUnavailable:
            if self._reason is None:
                self._reason = ProcessStopReason.OBSERVATION_FAILED
                self._publish()
            raise
        except Exception:
            self._stop(ProcessStopReason.OBSERVATION_FAILED, uncertain=True)
        cleanup = asyncio.create_task(self._finish())
        try:
            result = await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            self._uncertain = True
            try:
                self._stop(ProcessStopReason.CANCELLED, uncertain=True)
            except ProcessScopeUnavailable:
                pass
            cleanup.add_done_callback(
                lambda task: None if task.cancelled() else task.exception()
            )
            raise
        except ProcessScopeUnavailable:
            if cancelled:
                raise asyncio.CancelledError from None
            raise
        if cancelled:
            raise asyncio.CancelledError
        return result
