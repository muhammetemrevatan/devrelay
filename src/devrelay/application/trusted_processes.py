import asyncio
from collections.abc import Awaitable, Callable
from math import isfinite
from typing import TypeVar, cast

from devrelay.application.contracts.processes import (
    ExecutionProcessBinding,
    ProcessStopStatus,
)
from devrelay.application.contracts.trusted_processes import (
    TrustedProcessObservation,
    TrustedProcessPolicy,
    TrustedProcessResult,
    TrustedProcessSpec,
    TrustedProcessStopReason,
)
from devrelay.application.ports.process_scope import ProcessScopeUnavailable
from devrelay.application.ports.trusted_process import TrustedProcessScope
from devrelay.application.worker_executions import ExecutionAuthorityGuard
from devrelay.application.worker_processes import (
    _consume_completion,
    _count,
    _wait_for_tick,
)

_Value = TypeVar("_Value")
_DEFAULT_POLICY = TrustedProcessPolicy()


class _Stopped(Exception):
    def __init__(
        self, reason: TrustedProcessStopReason, *, uncertain: bool = False
    ) -> None:
        self.reason, self.uncertain = reason, uncertain


class RunTrustedProcess:
    """Run one host-owned command, with no journal replay or sandbox authority.

    The supplied clock and authority guard share a suspend-inclusive epoch. Native
    preparation owns suspension and attachment. A fresh guard before and after
    resume bounds that race; it cannot make a process start atomic with a lease.
    Unsettled operations and uncertain stop retain UNKNOWN, never capacity release.
    """

    def __init__(
        self,
        binding: ExecutionProcessBinding,
        scope: TrustedProcessScope,
        supervisor: ExecutionAuthorityGuard,
        clock: Callable[[], float],
        policy: TrustedProcessPolicy = _DEFAULT_POLICY,
    ) -> None:
        if not isinstance(binding, ExecutionProcessBinding) or not isinstance(
            policy, TrustedProcessPolicy
        ):
            raise ValueError(
                "Trusted process execution requires typed binding and policy."
            )
        binding.__post_init__()
        policy.__post_init__()
        self._binding, self._scope, self._guard = binding, scope, supervisor
        self._clock, self._policy = clock, policy
        self._check_binding()
        self._seal_original = scope.seal
        self._used = self._sealed = self._uncertain = False
        self._reason: TrustedProcessStopReason | None = None
        self._result: TrustedProcessResult | None = None
        self._last_clock: float | None = None
        self._started = self._loop_started = 0.0
        self._operation: asyncio.Future[object] | None = None
        self._stdout, self._stderr = bytearray(), bytearray()
        self._stdout_eof = self._stderr_eof = False
        self._exit_code: int | None = None

    @property
    def result(self) -> TrustedProcessResult | None:
        return self._result

    def _check_binding(self) -> None:
        try:
            actual = self._scope.binding
            matches = (
                isinstance(actual, ExecutionProcessBinding) and actual == self._binding
            )
        except Exception:
            matches = False
        if not matches:
            raise ProcessScopeUnavailable(
                "Trusted process scope changed original binding."
            )

    def _publish(
        self, status: ProcessStopStatus = ProcessStopStatus.UNKNOWN
    ) -> TrustedProcessResult:
        assert self._reason is not None
        self._result = TrustedProcessResult(
            execution_id=self._binding.intent.execution_id,
            reason=self._reason,
            exit_code=self._exit_code,
            stdout=bytes(self._stdout),
            stderr=bytes(self._stderr),
            stop_status=status,
        )
        return self._result

    def _stop(
        self, reason: TrustedProcessStopReason, *, uncertain: bool = False
    ) -> None:
        if self._reason is None:
            self._reason = reason
        self._uncertain = self._uncertain or uncertain
        if not self._sealed:
            try:
                self._seal_original()
            except Exception:
                self._uncertain = True
            else:
                self._sealed = True
        self._publish()

    def request_stop(self) -> None:
        self._stop(TrustedProcessStopReason.REQUESTED)

    def _now(self) -> float:
        try:
            value = self._clock()
            if (
                type(value) not in (int, float)
                or not 0 <= value <= 2**53 - 1
                or not isfinite(value)
                or (self._last_clock is not None and value < self._last_clock)
            ):
                raise ValueError("Invalid trusted process clock.")
        except Exception:
            raise _Stopped(
                TrustedProcessStopReason.CLOCK_FAILED, uncertain=True
            ) from None
        self._last_clock = float(value)
        return float(value)

    def _live(self) -> float:
        if self._reason is not None:
            raise _Stopped(self._reason)
        self._check_binding()
        now = self._now()
        try:
            self._guard.require_current_assignment(self._binding.reference)
        except Exception:
            raise _Stopped(TrustedProcessStopReason.AUTHORITY_LOST) from None
        self._check_binding()
        if self._runtime_remaining(now) <= 0:
            raise _Stopped(TrustedProcessStopReason.RUNTIME_LIMIT)
        return now

    def _runtime_remaining(self, now: float) -> float:
        return min(
            self._policy.maximum_runtime_seconds - (now - self._started),
            self._policy.maximum_runtime_seconds
            - (asyncio.get_running_loop().time() - self._loop_started),
        )

    def _start(
        self, operation: Callable[[], Awaitable[_Value]]
    ) -> asyncio.Future[_Value]:
        if self._operation is not None:
            raise ProcessScopeUnavailable("A trusted process operation is unsettled.")
        task = asyncio.ensure_future(operation())
        self._operation = cast(asyncio.Future[object], task)
        self._operation.add_done_callback(_consume_completion)
        return task

    async def _await(self, operation: Callable[[], Awaitable[_Value]]) -> _Value:
        started = self._live()
        loop_started = asyncio.get_running_loop().time()
        task = self._start(operation)
        while True:
            now = self._live()
            remaining = min(
                self._runtime_remaining(now),
                self._policy.operation_timeout_seconds - (now - started),
                self._policy.operation_timeout_seconds
                - (asyncio.get_running_loop().time() - loop_started),
            )
            if remaining <= 0:
                raise _Stopped(
                    TrustedProcessStopReason.OPERATION_FAILED, uncertain=True
                )
            if task.done():
                self._operation = None
                if task.cancelled():
                    raise _Stopped(
                        TrustedProcessStopReason.OPERATION_FAILED, uncertain=True
                    )
                return task.result()
            await _wait_for_tick(
                cast(asyncio.Future[object], task),
                min(remaining, self._policy.monitor_interval_seconds),
            )

    def _observe(self, observation: TrustedProcessObservation) -> None:
        if not isinstance(observation, TrustedProcessObservation):
            raise ValueError("Trusted process observation is unavailable.")
        observation.__post_init__()
        if (
            (self._stdout_eof and (observation.stdout or not observation.stdout_eof))
            or (self._stderr_eof and (observation.stderr or not observation.stderr_eof))
            or (
                self._exit_code is not None and observation.exit_code != self._exit_code
            )
        ):
            raise ValueError("Trusted process observation regressed original evidence.")
        self._exit_code = observation.exit_code
        self._stdout_eof, self._stderr_eof = (
            observation.stdout_eof,
            observation.stderr_eof,
        )
        remaining = (
            self._policy.maximum_output_bytes - len(self._stdout) - len(self._stderr)
        )
        self._stdout.extend(observation.stdout[:remaining])
        remaining -= min(remaining, len(observation.stdout))
        self._stderr.extend(observation.stderr[:remaining])

    async def _execute(self, spec: TrustedProcessSpec) -> None:
        if self._reason is not None:
            return
        if not isinstance(spec, TrustedProcessSpec):
            raise ValueError("Trusted process execution requires explicit host input.")
        spec.__post_init__()
        self._started = self._now()
        self._loop_started = asyncio.get_running_loop().time()
        if (
            await self._await(
                lambda: cast(Awaitable[object], self._scope.prepare(spec))
            )
            is not None
        ):
            raise ValueError(
                "Trusted process preparation returned unexpected evidence."
            )
        if (
            await self._await(lambda: cast(Awaitable[object], self._scope.resume()))
            is not None
        ):
            raise ValueError("Trusted process resume returned unexpected evidence.")
        for _ in range(self._policy.maximum_observations):
            observation = await self._await(self._scope.observe)
            available = (
                self._policy.maximum_output_bytes
                - len(self._stdout)
                - len(self._stderr)
            )
            self._observe(observation)
            if len(observation.stdout) + len(observation.stderr) > available:
                raise _Stopped(TrustedProcessStopReason.OUTPUT_LIMIT)
            if self._exit_code is not None and self._stdout_eof and self._stderr_eof:
                self._stop(TrustedProcessStopReason.EXITED)
                return
            now = self._live()
            await _wait_for_tick(
                None,
                min(
                    self._policy.monitor_interval_seconds, self._runtime_remaining(now)
                ),
            )
            self._live()
        self._stop(TrustedProcessStopReason.OBSERVATION_LIMIT)

    def _remaining(self, started: float | None, loop_deadline: float) -> float:
        remaining = loop_deadline - asyncio.get_running_loop().time()
        try:
            now = self._now()
        except _Stopped:
            self._uncertain = True
        else:
            if started is not None:
                remaining = min(
                    remaining, self._policy.stop_timeout_seconds - (now - started)
                )
        if remaining <= 0:
            self._uncertain = True
        return remaining

    async def _settle(self, started: float | None, deadline: float) -> bool:
        assert self._operation is not None
        while True:
            remaining = self._remaining(started, deadline)
            if remaining <= 0:
                return False
            if self._operation.done():
                self._operation = None
                return True
            await _wait_for_tick(
                self._operation, min(remaining, self._policy.monitor_interval_seconds)
            )

    async def _finish(self) -> TrustedProcessResult:
        if not self._sealed:
            return self._publish()
        deadline = asyncio.get_running_loop().time() + self._policy.stop_timeout_seconds
        try:
            started: float | None = self._now()
        except _Stopped:
            started, self._uncertain = None, True
        if self._operation is not None:
            pending = self._operation
            if not await self._settle(started, deadline):
                return self._publish()
            try:
                pending.result()
            except asyncio.CancelledError:
                self._uncertain = True
            except Exception:
                self._uncertain = True
        try:
            self._check_binding()
            if self._remaining(started, deadline) <= 0:
                return self._publish()
            termination = self._start(self._scope.terminate)
            if not await self._settle(started, deadline):
                return self._publish()
            if termination.result() is not None:
                raise ProcessScopeUnavailable(
                    "Trusted process termination returned unexpected evidence."
                )
            for _ in range(self._policy.maximum_observations):
                self._check_binding()
                if self._remaining(started, deadline) <= 0:
                    return self._publish()
                fresh = self._start(self._scope.active_processes)
                if not await self._settle(started, deadline):
                    return self._publish()
                self._check_binding()
                if _count(fresh.result()) == 0:
                    return self._publish(
                        ProcessStopStatus.UNKNOWN
                        if self._uncertain
                        else ProcessStopStatus.EMPTY_VERIFIED
                    )
                remaining = self._remaining(started, deadline)
                if remaining <= 0:
                    return self._publish()
                await _wait_for_tick(
                    None, min(remaining, self._policy.monitor_interval_seconds)
                )
        except asyncio.CancelledError:
            self._uncertain = True
        except Exception:
            self._uncertain = True
        return self._publish()

    async def run(self, spec: TrustedProcessSpec) -> TrustedProcessResult:
        if self._used:
            raise ProcessScopeUnavailable(
                "Trusted process execution has already started."
            )
        self._used = True
        cancelled = False
        try:
            await self._execute(spec)
        except asyncio.CancelledError:
            cancelled = True
            self._stop(
                TrustedProcessStopReason.CANCELLED,
                uncertain=self._operation is not None,
            )
        except _Stopped as stopped:
            self._stop(stopped.reason, uncertain=stopped.uncertain)
        except Exception:
            self._stop(TrustedProcessStopReason.OPERATION_FAILED, uncertain=True)
        cleanup = asyncio.create_task(self._finish())
        try:
            result = await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            self._stop(TrustedProcessStopReason.CANCELLED, uncertain=True)
            cleanup.add_done_callback(
                lambda done: _consume_completion(cast(asyncio.Future[object], done))
            )
            raise
        if cancelled:
            raise asyncio.CancelledError
        return result
