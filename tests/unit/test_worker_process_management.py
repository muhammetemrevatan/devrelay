import asyncio
from collections import deque
from collections.abc import Callable
from dataclasses import replace
from typing import Any, cast
from uuid import UUID

import pytest

import devrelay.application.worker_processes as management
from devrelay.application.contracts.executions import WorkerExecutionIntent
from devrelay.application.contracts.processes import (
    ExecutionProcessBinding,
    ProcessManagementPolicy,
    ProcessScopeResult,
    ProcessStopReason,
    ProcessStopStatus,
)
from devrelay.application.contracts.worker_recovery import AssignmentReference
from devrelay.application.lease_deadlines import LeaseDeadlineRejected
from devrelay.application.ports.process_scope import ProcessScopeUnavailable
from devrelay.application.worker_processes import ManageExecutionProcesses
from tests.unit.test_worker_execution_authority import Clock as ExecutionClock
from tests.unit.test_worker_execution_authority import live_supervisor
from tests.unit.test_worker_execution_preparation import context, policy


class Clock(ExecutionClock):
    pass


def binding() -> ExecutionProcessBinding:
    setup = context()
    acknowledgement = setup.journal.state.acknowledgement
    reference = setup.delivery.outcome.assignment
    assert acknowledgement is not None and reference is not None
    return ExecutionProcessBinding(
        intent=WorkerExecutionIntent(
            execution_id=UUID(int=99), acknowledgement=acknowledgement, policy=policy()
        ),
        reference=reference,
    )


class Guard:
    def __init__(self) -> None:
        self.error = False
        self.calls = 0

    def require_current_assignment(self, reference: AssignmentReference) -> None:
        self.calls += 1
        if self.error:
            raise LeaseDeadlineRejected("No current authority.")


class Scope:
    def __init__(self, expected: ExecutionProcessBinding) -> None:
        self.binding = expected
        self.calls: list[str] = []
        self.counts: deque[Any] = deque([0])
        self.fresh_counts: deque[Any] = deque([0])
        self.query_hook: Callable[[], None] = lambda: None
        self.terminate_hook: Callable[[], None] = lambda: None
        self.query_gate: asyncio.Event | None = None
        self.terminate_gate: asyncio.Event | None = None
        self.query_entered = asyncio.Event()
        self.terminate_entered = asyncio.Event()
        self.query_error = False
        self.sealed = False
        self.terminated = False
        self.active = 0
        self.maximum_active = 0

    def seal(self) -> None:
        self.calls.append("seal")
        self.sealed = True

    async def active_processes(self) -> int:
        self.calls.append("query")
        self.active += 1
        self.maximum_active = max(self.active, self.maximum_active)
        self.query_entered.set()
        try:
            self.query_hook()
            if self.query_gate is not None:
                await self.query_gate.wait()
            if self.query_error and not self.terminated:
                raise ProcessScopeUnavailable("Native accounting unavailable.")
            counts = self.fresh_counts if self.terminated else self.counts
            value = counts.popleft() if counts else 1
            return cast(int, value)
        finally:
            self.active -= 1

    async def terminate(self) -> None:
        assert self.sealed
        self.calls.append("terminate")
        self.active += 1
        self.maximum_active = max(self.active, self.maximum_active)
        self.terminate_entered.set()
        try:
            self.terminate_hook()
            if self.terminate_gate is not None:
                await self.terminate_gate.wait()
            self.terminated = True
        finally:
            self.active -= 1


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    timer = Clock()

    async def tick(task: asyncio.Future[object] | None, timeout: float) -> None:
        await asyncio.sleep(0)
        timer.value += max(timeout, 0.01)

    monkeypatch.setattr(management, "_wait_for_tick", tick)
    return timer


def test_empty_requires_seal_termination_and_fresh_post_stop_zero(clock: Clock) -> None:
    expected = binding()
    scope = Scope(expected)
    manager = ManageExecutionProcesses(expected, scope, Guard(), clock)

    result = asyncio.run(manager.run())

    assert result.status is ProcessStopStatus.EMPTY_VERIFIED
    assert result.reason is ProcessStopReason.EMPTY
    assert scope.calls == ["query", "seal", "terminate", "query"]
    assert scope.maximum_active == 1


def test_parent_exit_is_not_associated_child_empty_evidence(clock: Clock) -> None:
    expected = binding()
    scope = Scope(expected)
    scope.counts = deque([1])
    manager = ManageExecutionProcesses(
        expected, scope, Guard(), clock, ProcessManagementPolicy(maximum_observations=1)
    )

    result = asyncio.run(manager.run())

    assert result.reason is ProcessStopReason.OBSERVATION_LIMIT
    assert result.status is ProcessStopStatus.EMPTY_VERIFIED
    assert scope.calls == ["query", "seal", "terminate", "query"]


def test_transient_post_termination_count_is_polled_sequentially_to_fresh_zero(
    clock: Clock,
) -> None:
    expected = binding()
    scope = Scope(expected)
    scope.fresh_counts = deque([2, 1, 0])
    manager = ManageExecutionProcesses(expected, scope, Guard(), clock)

    result = asyncio.run(manager.run())

    assert result.status is ProcessStopStatus.EMPTY_VERIFIED
    assert scope.calls == ["query", "seal", "terminate", "query", "query", "query"]
    assert scope.maximum_active == 1


def test_positive_post_stop_count_is_unknown_at_finite_cleanup_budget(
    clock: Clock,
) -> None:
    expected = binding()
    scope = Scope(expected)
    scope.fresh_counts = deque([1] * 100)
    manager = ManageExecutionProcesses(
        expected,
        scope,
        Guard(),
        clock,
        ProcessManagementPolicy(stop_timeout_seconds=0.3),
    )

    result = asyncio.run(manager.run())

    assert result.status is ProcessStopStatus.UNKNOWN
    assert scope.calls.count("terminate") == 1
    assert scope.calls.count("query") < 10


def test_initial_binding_mismatch_never_stops_foreign_scope(clock: Clock) -> None:
    expected = binding()
    foreign = replace(
        expected, intent=replace(expected.intent, execution_id=UUID(int=100))
    )
    scope = Scope(foreign)

    with pytest.raises(ProcessScopeUnavailable):
        ManageExecutionProcesses(expected, scope, Guard(), clock)

    assert scope.calls == []


def test_binding_changed_during_observation_is_not_sealed_or_terminated(
    clock: Clock,
) -> None:
    expected = binding()
    scope = Scope(expected)
    foreign = replace(
        expected, intent=replace(expected.intent, execution_id=UUID(int=100))
    )
    scope.query_hook = lambda: setattr(scope, "binding", foreign)
    manager = ManageExecutionProcesses(expected, scope, Guard(), clock)

    with pytest.raises(ProcessScopeUnavailable):
        asyncio.run(manager.run())

    assert scope.calls == ["query"]
    assert (
        manager.result is not None
        and manager.result.status is ProcessStopStatus.UNKNOWN
    )


def test_binding_changed_after_termination_cannot_supply_empty_evidence(
    clock: Clock,
) -> None:
    expected = binding()
    scope = Scope(expected)
    foreign = replace(
        expected, intent=replace(expected.intent, execution_id=UUID(int=100))
    )
    scope.terminate_hook = lambda: setattr(scope, "binding", foreign)
    manager = ManageExecutionProcesses(expected, scope, Guard(), clock)

    with pytest.raises(ProcessScopeUnavailable):
        asyncio.run(manager.run())

    assert scope.calls == ["query", "seal", "terminate"]
    assert (
        manager.result is not None
        and manager.result.status is ProcessStopStatus.UNKNOWN
    )


def test_no_initial_authority_seals_then_stops_existing_scope(clock: Clock) -> None:
    expected = binding()
    scope, guard = Scope(expected), Guard()
    guard.error = True
    manager = ManageExecutionProcesses(expected, scope, guard, clock)

    result = asyncio.run(manager.run())

    assert result.reason is ProcessStopReason.AUTHORITY_LOST
    assert result.status is ProcessStopStatus.EMPTY_VERIFIED
    assert scope.calls == ["seal", "terminate", "query"]


def test_pre_requested_stop_seals_synchronously_before_run(clock: Clock) -> None:
    expected = binding()
    scope = Scope(expected)
    manager = ManageExecutionProcesses(expected, scope, Guard(), clock)
    manager.request_stop()
    assert scope.calls == ["seal"]

    result = asyncio.run(manager.run())

    assert result.reason is ProcessStopReason.REQUESTED
    assert result.status is ProcessStopStatus.EMPTY_VERIFIED
    assert scope.calls == ["seal", "terminate", "query"]


def test_one_shot_management_cannot_restart_after_empty_result(clock: Clock) -> None:
    expected = binding()
    scope = Scope(expected)
    manager = ManageExecutionProcesses(expected, scope, Guard(), clock)
    asyncio.run(manager.run())
    previous_calls = tuple(scope.calls)

    with pytest.raises(ProcessScopeUnavailable):
        asyncio.run(manager.run())

    assert tuple(scope.calls) == previous_calls


@pytest.mark.parametrize("count", [True, -1, 129, 1.0, None])
def test_corrupt_monitor_accounting_never_becomes_verified_after_fresh_zero(
    clock: Clock, count: Any
) -> None:
    expected = binding()
    scope = Scope(expected)
    scope.counts = deque([count])
    manager = ManageExecutionProcesses(expected, scope, Guard(), clock)

    result = asyncio.run(manager.run())

    assert result.reason is ProcessStopReason.OBSERVATION_FAILED
    assert result.status is ProcessStopStatus.UNKNOWN
    assert scope.calls == ["query", "seal", "terminate", "query"]


def test_monitor_query_error_remains_unknown_after_successful_stop(
    clock: Clock,
) -> None:
    expected = binding()
    scope = Scope(expected)
    scope.query_error = True
    manager = ManageExecutionProcesses(expected, scope, Guard(), clock)

    result = asyncio.run(manager.run())

    assert result.status is ProcessStopStatus.UNKNOWN
    assert result.reason is ProcessStopReason.OBSERVATION_FAILED
    assert scope.maximum_active == 1


def test_unresolved_query_does_not_queue_termination_or_accept_late_zero(
    clock: Clock,
) -> None:
    async def scenario() -> tuple[ManageExecutionProcesses, Scope]:
        expected = binding()
        scope = Scope(expected)
        scope.query_gate = asyncio.Event()
        manager = ManageExecutionProcesses(
            expected,
            scope,
            Guard(),
            clock,
            ProcessManagementPolicy(
                operation_timeout_seconds=0.1, stop_timeout_seconds=0.2
            ),
        )

        result = await manager.run()
        scope.query_gate.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert result.status is ProcessStopStatus.UNKNOWN
        return manager, scope

    manager, scope = asyncio.run(scenario())

    assert scope.calls == ["query", "seal"]
    assert (
        manager.result is not None
        and manager.result.status is ProcessStopStatus.UNKNOWN
    )
    assert scope.maximum_active == 1


def test_delayed_termination_does_not_queue_fresh_query_after_stop_budget(
    clock: Clock,
) -> None:
    async def scenario() -> tuple[ManageExecutionProcesses, Scope]:
        expected = binding()
        scope = Scope(expected)
        scope.terminate_gate = asyncio.Event()
        manager = ManageExecutionProcesses(
            expected,
            scope,
            Guard(),
            clock,
            ProcessManagementPolicy(stop_timeout_seconds=0.2),
        )

        result = await manager.run()
        scope.terminate_gate.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert result.status is ProcessStopStatus.UNKNOWN
        return manager, scope

    manager, scope = asyncio.run(scenario())

    assert scope.calls == ["query", "seal", "terminate"]
    assert (
        manager.result is not None
        and manager.result.status is ProcessStopStatus.UNKNOWN
    )
    assert scope.maximum_active == 1


def test_suspend_jump_stops_before_accepting_late_monitor_zero(clock: Clock) -> None:
    expected = binding()
    scope = Scope(expected)

    def suspend_jump() -> None:
        if not scope.terminated:
            clock.value = 4000

    scope.query_hook = suspend_jump
    manager = ManageExecutionProcesses(expected, scope, Guard(), clock)

    result = asyncio.run(manager.run())

    assert result.reason is ProcessStopReason.RUNTIME_LIMIT
    assert result.status is ProcessStopStatus.EMPTY_VERIFIED
    assert scope.calls == ["query", "seal", "terminate", "query"]


def test_clock_regression_keeps_stop_unknown_even_after_zero(clock: Clock) -> None:
    expected = binding()
    scope = Scope(expected)
    scope.query_hook = lambda: setattr(clock, "value", 90)
    manager = ManageExecutionProcesses(expected, scope, Guard(), clock)

    result = asyncio.run(manager.run())

    assert result.reason is ProcessStopReason.CLOCK_FAILED
    assert result.status is ProcessStopStatus.UNKNOWN
    assert scope.sealed


def test_frozen_clock_is_bounded_by_secondary_runtime_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> ProcessScopeResult:
        elapsed = Clock()
        loop = asyncio.get_running_loop()
        monkeypatch.setattr(loop, "time", elapsed)

        async def tick(task: asyncio.Future[object] | None, timeout: float) -> None:
            await asyncio.sleep(0)
            elapsed.value += timeout

        monkeypatch.setattr(management, "_wait_for_tick", tick)
        expected = binding()
        scope = Scope(expected)
        scope.counts = deque([1] * 100)
        manager = ManageExecutionProcesses(
            expected,
            scope,
            Guard(),
            lambda: 100,
            ProcessManagementPolicy(maximum_runtime_seconds=1),
        )

        return await manager.run()

    result = asyncio.run(scenario())

    assert result.reason is ProcessStopReason.RUNTIME_LIMIT
    assert result.status is ProcessStopStatus.EMPTY_VERIFIED


def test_cancellation_while_query_pending_seals_before_cleanup_and_retains_unknown(
    clock: Clock,
) -> None:
    async def scenario() -> tuple[ManageExecutionProcesses, Scope]:
        expected = binding()
        scope = Scope(expected)
        scope.query_gate = asyncio.Event()
        manager = ManageExecutionProcesses(
            expected,
            scope,
            Guard(),
            clock,
            ProcessManagementPolicy(stop_timeout_seconds=0.2),
        )
        running = asyncio.create_task(manager.run())
        await scope.query_entered.wait()

        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running

        return manager, scope

    manager, scope = asyncio.run(scenario())

    assert scope.calls == ["query", "seal"]
    assert manager.result is not None
    assert manager.result.reason is ProcessStopReason.CANCELLED
    assert manager.result.status is ProcessStopStatus.UNKNOWN


def test_cancellation_before_first_accounting_stops_without_monitor_query(
    clock: Clock,
) -> None:
    class CancelGuard(Guard):
        def require_current_assignment(self, reference: AssignmentReference) -> None:
            raise asyncio.CancelledError

    expected = binding()
    scope = Scope(expected)
    manager = ManageExecutionProcesses(expected, scope, CancelGuard(), clock)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(manager.run())

    assert scope.calls == ["seal", "terminate", "query"]
    assert manager.result is not None
    assert manager.result.reason is ProcessStopReason.CANCELLED
    assert manager.result.status is ProcessStopStatus.EMPTY_VERIFIED


def test_cancellation_during_cleanup_does_not_cancel_native_operation_or_publish_empty(
    clock: Clock,
) -> None:
    async def scenario() -> tuple[ManageExecutionProcesses, Scope]:
        expected = binding()
        scope = Scope(expected)
        scope.terminate_gate = asyncio.Event()
        manager = ManageExecutionProcesses(
            expected,
            scope,
            Guard(),
            clock,
            ProcessManagementPolicy(stop_timeout_seconds=0.5),
        )
        running = asyncio.create_task(manager.run())
        await scope.terminate_entered.wait()

        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running
        scope.terminate_gate.set()
        for _ in range(10):
            await asyncio.sleep(0)

        return manager, scope

    manager, scope = asyncio.run(scenario())

    assert (
        manager.result is not None
        and manager.result.status is ProcessStopStatus.UNKNOWN
    )
    assert scope.maximum_active == 1


def test_real_h4_guard_detects_deadline_between_process_monitor_ticks(
    clock: Clock, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup = context()

    async def scenario() -> tuple[ProcessScopeResult, Scope]:
        async with live_supervisor(setup, clock, monkeypatch) as supervisor:
            reference = setup.delivery.outcome.assignment
            acknowledgement = setup.journal.state.acknowledgement
            assert reference is not None and acknowledgement is not None
            expected = ExecutionProcessBinding(
                intent=WorkerExecutionIntent(
                    execution_id=UUID(int=99),
                    acknowledgement=acknowledgement,
                    policy=policy(),
                ),
                reference=reference,
            )
            scope = Scope(expected)
            assert supervisor.deadline is not None
            deadline = supervisor.deadline.stop_at

            def cross_deadline() -> None:
                if not scope.terminated:
                    clock.value = deadline

            scope.query_hook = cross_deadline
            manager = ManageExecutionProcesses(expected, scope, supervisor, clock)

            result = await manager.run()

            assert supervisor.stop_required
            return result, scope

    result, scope = asyncio.run(scenario())

    assert result.reason is ProcessStopReason.AUTHORITY_LOST
    assert result.status is ProcessStopStatus.EMPTY_VERIFIED
    assert scope.calls == ["query", "seal", "terminate", "query"]
