import asyncio
from collections import deque
from collections.abc import Callable
from dataclasses import replace
from typing import Any, cast
from uuid import UUID

import pytest

import devrelay.application.trusted_processes as processes
from devrelay.application.contracts.processes import (
    ExecutionProcessBinding,
    ProcessStopStatus,
)
from devrelay.application.contracts.trusted_processes import (
    TrustedProcessObservation,
    TrustedProcessPolicy,
    TrustedProcessSpec,
    TrustedProcessStopReason,
)
from devrelay.application.ports.process_scope import ProcessScopeUnavailable
from devrelay.application.trusted_processes import RunTrustedProcess
from tests.unit.test_worker_execution_authority import Clock, live_supervisor
from tests.unit.test_worker_execution_preparation import context as execution_context
from tests.unit.test_worker_process_management import Guard, binding


def spec() -> TrustedProcessSpec:
    return TrustedProcessSpec(
        executable=r"C:\trusted\fixture.exe",
        arguments=("fixture",),
        working_directory=r"C:\fixture",
        environment=(("FIXTURE", "explicit"),),
    )


def observation(
    *,
    exit_code: int | None = 0,
    stdout: bytes = b"",
    stderr: bytes = b"",
    stdout_eof: bool = True,
    stderr_eof: bool = True,
) -> TrustedProcessObservation:
    return TrustedProcessObservation(
        exit_code=exit_code,
        stdout=stdout,
        stderr=stderr,
        stdout_eof=stdout_eof,
        stderr_eof=stderr_eof,
    )


class Scope:
    def __init__(self, expected: ExecutionProcessBinding) -> None:
        self.binding = expected
        self.calls: list[str] = []
        self.sealed = False
        self.observations: deque[Any] = deque([observation()])
        self.counts: deque[Any] = deque([0])
        self.hooks: dict[str, Callable[[], None]] = {}
        self.gates: dict[str, asyncio.Event] = {}
        self.entered: dict[str, asyncio.Event] = {}
        self.errors: dict[str, Exception] = {}
        self.returned: dict[str, Any] = {}
        self.active = self.maximum_active = 0
        self.prepared: list[TrustedProcessSpec] = []

    async def call(self, operation: str) -> None:
        self.calls.append(operation)
        self.active += 1
        self.maximum_active = max(self.maximum_active, self.active)
        try:
            if operation in self.entered:
                self.entered[operation].set()
            if operation in self.gates:
                await self.gates[operation].wait()
            if operation in self.hooks:
                self.hooks[operation]()
            if operation in self.errors:
                raise self.errors[operation]
        finally:
            self.active -= 1

    def seal(self) -> None:
        self.calls.append("seal")
        self.sealed = True

    async def prepare(self, command: TrustedProcessSpec) -> None:
        assert not self.sealed
        self.prepared.append(command)
        await self.call("prepare")
        return cast(None, self.returned.get("prepare"))

    async def resume(self) -> None:
        assert not self.sealed
        await self.call("resume")
        return cast(None, self.returned.get("resume"))

    async def observe(self) -> TrustedProcessObservation:
        await self.call("observe")
        return cast(
            TrustedProcessObservation,
            self.observations.popleft()
            if self.observations
            else observation(exit_code=None, stdout_eof=False, stderr_eof=False),
        )

    async def terminate(self) -> None:
        assert self.sealed
        await self.call("terminate")
        return cast(None, self.returned.get("terminate"))

    async def active_processes(self) -> int:
        await self.call("query")
        return cast(int, self.counts.popleft() if self.counts else 0)


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    timer = Clock()

    async def tick(task: asyncio.Future[object] | None, timeout: float) -> None:
        await asyncio.sleep(0)
        timer.value += max(min(timeout, 0.01), 0.001)

    monkeypatch.setattr(processes, "_wait_for_tick", tick)
    return timer


def runner(
    clock: Clock, *, policy: TrustedProcessPolicy | None = None
) -> tuple[RunTrustedProcess, Scope, Guard]:
    expected = binding()
    scope, guard = Scope(expected), Guard()
    return (
        RunTrustedProcess(
            expected, scope, guard, clock, policy or TrustedProcessPolicy()
        ),
        scope,
        guard,
    )


def test_suspended_prepare_resume_capture_exit_then_kill_and_fresh_empty(
    clock: Clock,
) -> None:
    service, scope, guard = runner(clock)
    command = spec()
    scope.observations = deque([observation(stdout=b"out\0bytes", stderr=b"err\xff")])

    result = asyncio.run(service.run(command))

    assert result.reason is TrustedProcessStopReason.EXITED
    assert (
        result.exit_code == 0 and result.stop_status is ProcessStopStatus.EMPTY_VERIFIED
    )
    assert result.stdout == b"out\0bytes" and result.stderr == b"err\xff"
    assert "out" not in repr(result) and "err" not in repr(result)
    assert scope.prepared == [command]
    assert scope.calls == ["prepare", "resume", "observe", "seal", "terminate", "query"]
    assert scope.maximum_active == 1 and guard.calls >= 6
    assert service.result is result


def test_nonzero_exit_is_retained_as_evidence_not_passing_check(clock: Clock) -> None:
    service, scope, _ = runner(clock)
    scope.observations = deque([observation(exit_code=7)])

    result = asyncio.run(service.run(spec()))

    assert result.reason is TrustedProcessStopReason.EXITED and result.exit_code == 7
    assert result.stop_status is ProcessStopStatus.EMPTY_VERIFIED


def test_parent_exit_drains_both_streams_before_terminating_descendants(
    clock: Clock,
) -> None:
    service, scope, _ = runner(clock)
    scope.observations = deque(
        [
            observation(
                exit_code=3, stdout=b"first", stdout_eof=False, stderr_eof=False
            ),
            observation(exit_code=3, stderr=b"last", stdout_eof=True, stderr_eof=False),
            observation(exit_code=3),
        ]
    )
    scope.counts = deque([2, 1, 0])

    result = asyncio.run(service.run(spec()))

    assert scope.calls.count("observe") == scope.calls.count("query") == 3
    assert (
        result.stdout == b"first" and result.stderr == b"last" and result.exit_code == 3
    )
    assert result.stop_status is ProcessStopStatus.EMPTY_VERIFIED
    assert scope.calls.index("terminate") > scope.calls.index("seal")


def test_initial_authority_loss_never_prepares_or_resumes(clock: Clock) -> None:
    service, scope, guard = runner(clock)
    guard.error = True

    result = asyncio.run(service.run(spec()))

    assert result.reason is TrustedProcessStopReason.AUTHORITY_LOST
    assert scope.calls == ["seal", "terminate", "query"]


@pytest.mark.parametrize("stage", ["prepare", "resume", "observe"])
def test_authority_loss_after_native_boundary_rejects_late_evidence(
    clock: Clock, stage: str
) -> None:
    service, scope, guard = runner(clock)
    scope.hooks[stage] = lambda: setattr(guard, "error", True)

    result = asyncio.run(service.run(spec()))

    assert result.reason is TrustedProcessStopReason.AUTHORITY_LOST
    assert scope.calls[-3:] == ["seal", "terminate", "query"]
    if stage == "prepare":
        assert "resume" not in scope.calls
    if stage == "observe":
        assert result.exit_code is None


def test_pre_requested_stop_is_synchronous_and_never_starts_process(
    clock: Clock,
) -> None:
    service, scope, _ = runner(clock)
    service.request_stop()
    assert scope.calls == ["seal"] and service.result is not None

    result = asyncio.run(service.run(spec()))

    assert result.reason is TrustedProcessStopReason.REQUESTED
    assert scope.calls == ["seal", "terminate", "query"]


@pytest.mark.parametrize("stage", ["prepare", "resume", "observe"])
def test_native_operation_failure_keeps_unknown_even_after_fresh_zero(
    clock: Clock, stage: str
) -> None:
    service, scope, _ = runner(clock)
    scope.errors[stage] = ProcessScopeUnavailable("Fixture operation failed.")

    result = asyncio.run(service.run(spec()))

    assert result.reason is TrustedProcessStopReason.OPERATION_FAILED
    assert result.stop_status is ProcessStopStatus.UNKNOWN
    assert scope.calls[-3:] == ["seal", "terminate", "query"]


@pytest.mark.parametrize("stage", ["prepare", "resume"])
def test_unexpected_native_return_cannot_advance_to_next_stage(
    clock: Clock, stage: str
) -> None:
    service, scope, _ = runner(clock)
    scope.returned[stage] = True

    result = asyncio.run(service.run(spec()))

    assert result.reason is TrustedProcessStopReason.OPERATION_FAILED
    assert result.stop_status is ProcessStopStatus.UNKNOWN
    if stage == "prepare":
        assert "resume" not in scope.calls


@pytest.mark.parametrize("bad", [None, True, {}])
def test_untyped_observation_is_unknown(clock: Clock, bad: Any) -> None:
    service, scope, _ = runner(clock)
    scope.observations = deque([bad])

    result = asyncio.run(service.run(spec()))

    assert result.reason is TrustedProcessStopReason.OPERATION_FAILED
    assert result.stop_status is ProcessStopStatus.UNKNOWN


@pytest.mark.parametrize(
    "regression",
    ["exit", "stdout_eof", "stderr_eof", "stdout_after_eof", "stderr_after_eof"],
)
def test_observations_cannot_regress_exit_or_closed_streams(
    clock: Clock, regression: str
) -> None:
    service, scope, _ = runner(clock)
    first = observation(exit_code=0, stdout_eof=True, stderr_eof=False)
    second = observation()
    if regression == "exit":
        second = replace(second, exit_code=1)
    elif regression == "stdout_eof":
        second = replace(second, stdout_eof=False)
    elif regression == "stdout_after_eof":
        second = replace(second, stdout=b"unexpected")
    else:
        first = observation(exit_code=None, stdout_eof=False, stderr_eof=True)
        second = (
            replace(second, stderr_eof=False)
            if regression == "stderr_eof"
            else replace(second, stderr=b"unexpected")
        )
    scope.observations = deque([first, second])

    result = asyncio.run(service.run(spec()))

    assert result.reason is TrustedProcessStopReason.OPERATION_FAILED
    assert result.stop_status is ProcessStopStatus.UNKNOWN


def test_output_cap_combines_both_streams_and_keeps_only_bounded_prefix(
    clock: Clock,
) -> None:
    service, scope, _ = runner(
        clock, policy=TrustedProcessPolicy(maximum_output_bytes=7)
    )
    scope.observations = deque(
        [
            observation(
                exit_code=None,
                stdout=b"1234",
                stderr=b"56",
                stdout_eof=False,
                stderr_eof=False,
            ),
            observation(stdout=b"AB", stderr=b"CD"),
        ]
    )

    result = asyncio.run(service.run(spec()))

    assert result.reason is TrustedProcessStopReason.OUTPUT_LIMIT
    assert result.stdout == b"1234A" and result.stderr == b"56"
    assert len(result.stdout) + len(result.stderr) == 7
    assert result.stop_status is ProcessStopStatus.EMPTY_VERIFIED


def test_exact_output_cap_can_finish_without_overflow(clock: Clock) -> None:
    service, scope, _ = runner(
        clock, policy=TrustedProcessPolicy(maximum_output_bytes=4)
    )
    scope.observations = deque([observation(stdout=b"ab", stderr=b"cd")])

    result = asyncio.run(service.run(spec()))

    assert result.reason is TrustedProcessStopReason.EXITED
    assert result.stdout + result.stderr == b"abcd"


def test_observation_count_is_finite_without_root_exit(clock: Clock) -> None:
    service, scope, _ = runner(
        clock, policy=TrustedProcessPolicy(maximum_observations=2)
    )
    scope.observations.clear()

    result = asyncio.run(service.run(spec()))

    assert result.reason is TrustedProcessStopReason.OBSERVATION_LIMIT
    assert scope.calls.count("observe") == 2


def test_runtime_limit_uses_suspend_clock_and_stops_before_resume(clock: Clock) -> None:
    service, scope, _ = runner(clock)
    scope.hooks["prepare"] = lambda: setattr(clock, "value", 10000)

    result = asyncio.run(service.run(spec()))

    assert result.reason is TrustedProcessStopReason.RUNTIME_LIMIT
    assert "resume" not in scope.calls and scope.calls[-3:] == [
        "seal",
        "terminate",
        "query",
    ]


@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), -1, True, 10**400])
def test_invalid_clock_denies_start_and_never_certifies_stop(
    clock: Clock, invalid: Any
) -> None:
    service, scope, _ = runner(clock)
    clock.value = invalid

    result = asyncio.run(service.run(spec()))

    assert result.reason is TrustedProcessStopReason.CLOCK_FAILED
    assert (
        result.stop_status is ProcessStopStatus.UNKNOWN and "prepare" not in scope.calls
    )


def test_clock_regression_stays_unknown_after_fresh_stop(clock: Clock) -> None:
    service, scope, _ = runner(clock)
    scope.hooks["resume"] = lambda: setattr(clock, "value", 1)

    result = asyncio.run(service.run(spec()))

    assert result.reason is TrustedProcessStopReason.CLOCK_FAILED
    assert result.stop_status is ProcessStopStatus.UNKNOWN


@pytest.mark.parametrize("stage", ["prepare", "resume", "observe"])
def test_unsettled_native_operation_cannot_queue_stop_or_accept_late_completion(
    clock: Clock, stage: str
) -> None:
    service, scope, _ = runner(
        clock,
        policy=TrustedProcessPolicy(
            operation_timeout_seconds=0.05, stop_timeout_seconds=0.1
        ),
    )

    async def scenario() -> None:
        scope.gates[stage] = asyncio.Event()
        result = await service.run(spec())
        assert result.reason is TrustedProcessStopReason.OPERATION_FAILED
        assert result.stop_status is ProcessStopStatus.UNKNOWN
        assert "terminate" not in scope.calls and "query" not in scope.calls
        scope.gates[stage].set()
        await asyncio.sleep(0)
        assert service.result is result

    asyncio.run(scenario())

    assert scope.maximum_active == 1


@pytest.mark.parametrize("stage", ["prepare", "resume", "observe"])
def test_binding_swap_never_terminates_foreign_scope(clock: Clock, stage: str) -> None:
    service, scope, _ = runner(clock)
    foreign = replace(
        scope.binding, intent=replace(scope.binding.intent, execution_id=UUID(int=123))
    )
    scope.hooks[stage] = lambda: setattr(scope, "binding", foreign)

    result = asyncio.run(service.run(spec()))

    assert result.stop_status is ProcessStopStatus.UNKNOWN
    assert result.reason is TrustedProcessStopReason.OPERATION_FAILED
    assert "seal" in scope.calls and "terminate" not in scope.calls


@pytest.mark.parametrize("bad", [True, -1, 129, None])
def test_fresh_accounting_must_be_bounded_integer(clock: Clock, bad: Any) -> None:
    service, scope, _ = runner(clock)
    scope.counts = deque([bad])

    result = asyncio.run(service.run(spec()))

    assert result.stop_status is ProcessStopStatus.UNKNOWN


def test_live_descendants_at_stop_budget_never_release_capacity(clock: Clock) -> None:
    service, scope, _ = runner(
        clock, policy=TrustedProcessPolicy(stop_timeout_seconds=0.1)
    )
    scope.counts = deque([1] * 100)

    result = asyncio.run(service.run(spec()))

    assert result.stop_status is ProcessStopStatus.UNKNOWN
    assert scope.calls.count("query") < 100


def test_termination_failure_cannot_query_empty(clock: Clock) -> None:
    service, scope, _ = runner(clock)
    scope.errors["terminate"] = ProcessScopeUnavailable("Fixture termination failed.")

    result = asyncio.run(service.run(spec()))

    assert (
        result.stop_status is ProcessStopStatus.UNKNOWN and "query" not in scope.calls
    )


def test_unexpected_termination_result_cannot_query_or_certify_empty(
    clock: Clock,
) -> None:
    service, scope, _ = runner(clock)
    scope.returned["terminate"] = True

    result = asyncio.run(service.run(spec()))

    assert result.stop_status is ProcessStopStatus.UNKNOWN
    assert "query" not in scope.calls


@pytest.mark.parametrize("stage", ["prepare", "resume", "observe"])
def test_cancellation_seals_before_cleanup_and_preserves_cancelled_result(
    clock: Clock,
    stage: str,
) -> None:
    service, scope, _ = runner(clock)

    async def scenario() -> None:
        scope.gates[stage], scope.entered[stage] = (
            asyncio.Event(),
            asyncio.Event(),
        )
        pending = asyncio.create_task(service.run(spec()))
        await scope.entered[stage].wait()
        pending.cancel()
        scope.gates[stage].set()
        with pytest.raises(asyncio.CancelledError):
            await pending

    asyncio.run(scenario())

    assert service.result is not None
    assert service.result.reason is TrustedProcessStopReason.CANCELLED
    assert service.result.stop_status is ProcessStopStatus.UNKNOWN
    assert scope.calls[-3:] == ["seal", "terminate", "query"]


def test_captured_original_seal_is_not_replaced_by_supplier(clock: Clock) -> None:
    service, scope, _ = runner(clock)
    foreign_calls: list[str] = []

    def replace_seal() -> None:
        scope.seal = lambda: foreign_calls.append("foreign")  # type: ignore[method-assign]

    scope.hooks["observe"] = replace_seal

    result = asyncio.run(service.run(spec()))

    assert result.stop_status is ProcessStopStatus.EMPTY_VERIFIED
    assert scope.sealed and foreign_calls == []


def test_one_shot_success_never_prepares_again(clock: Clock) -> None:
    service, scope, _ = runner(clock)

    async def scenario() -> None:
        await service.run(spec())
        with pytest.raises(ProcessScopeUnavailable):
            await service.run(spec())

    asyncio.run(scenario())

    assert scope.calls.count("prepare") == scope.calls.count("resume") == 1


def test_concurrent_run_cannot_admit_second_prepare(clock: Clock) -> None:
    service, scope, _ = runner(clock)

    async def scenario() -> None:
        scope.gates["prepare"], scope.entered["prepare"] = (
            asyncio.Event(),
            asyncio.Event(),
        )
        first = asyncio.create_task(service.run(spec()))
        await scope.entered["prepare"].wait()
        with pytest.raises(ProcessScopeUnavailable):
            await service.run(spec())
        assert not scope.sealed
        scope.gates["prepare"].set()
        await first

    asyncio.run(scenario())

    assert scope.calls.count("prepare") == scope.calls.count("resume") == 1


def test_requested_stop_latches_before_pending_prepare_can_resume(clock: Clock) -> None:
    service, scope, _ = runner(clock)

    async def scenario() -> None:
        scope.gates["prepare"], scope.entered["prepare"] = (
            asyncio.Event(),
            asyncio.Event(),
        )
        pending = asyncio.create_task(service.run(spec()))
        await scope.entered["prepare"].wait()
        service.request_stop()
        assert scope.sealed
        scope.gates["prepare"].set()
        result = await pending
        assert result.reason is TrustedProcessStopReason.REQUESTED

    asyncio.run(scenario())

    assert "resume" not in scope.calls
    assert scope.calls[-3:] == ["seal", "terminate", "query"]


def test_late_native_error_keeps_stop_unknown_after_authority_loss(
    clock: Clock,
) -> None:
    service, scope, guard = runner(clock)

    def fail_after_authority_lost() -> None:
        guard.error = True
        raise ProcessScopeUnavailable("Late native failure.")

    scope.hooks["prepare"] = fail_after_authority_lost

    result = asyncio.run(service.run(spec()))

    assert result.reason is TrustedProcessStopReason.AUTHORITY_LOST
    assert result.stop_status is ProcessStopStatus.UNKNOWN
    assert "resume" not in scope.calls and scope.calls[-3:] == [
        "seal",
        "terminate",
        "query",
    ]


def test_loop_clock_bounds_runtime_even_when_supplied_clock_is_frozen(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    timer = Clock()
    service, scope, _ = runner(
        timer,
        policy=TrustedProcessPolicy(
            maximum_runtime_seconds=0.1, monitor_interval_seconds=1
        ),
    )
    scope.observations.clear()
    waits: list[float] = []

    async def tick(task: asyncio.Future[object] | None, timeout: float) -> None:
        if not scope.sealed:
            waits.append(timeout)
        if task is None:
            await asyncio.sleep(timeout)
        else:
            await asyncio.wait({task}, timeout=timeout)

    monkeypatch.setattr(processes, "_wait_for_tick", tick)

    result = asyncio.run(service.run(spec()))

    assert result.reason is TrustedProcessStopReason.RUNTIME_LIMIT
    assert "terminate" in scope.calls and max(waits) <= 0.1


def test_loop_clock_bounds_pending_operation_when_supplied_clock_is_frozen(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, scope, _ = runner(
        Clock(),
        policy=TrustedProcessPolicy(
            operation_timeout_seconds=0.05, stop_timeout_seconds=0.1
        ),
    )

    async def tick(task: asyncio.Future[object] | None, timeout: float) -> None:
        if task is None:
            await asyncio.sleep(timeout)
        else:
            await asyncio.wait({task}, timeout=timeout)

    monkeypatch.setattr(processes, "_wait_for_tick", tick)

    async def scenario() -> None:
        scope.gates["prepare"] = asyncio.Event()
        result = await service.run(spec())
        assert result.reason is TrustedProcessStopReason.OPERATION_FAILED
        assert result.stop_status is ProcessStopStatus.UNKNOWN
        assert "terminate" not in scope.calls
        scope.gates["prepare"].set()
        await asyncio.sleep(0)

    asyncio.run(scenario())


def test_late_post_termination_zero_after_suspend_jump_is_unknown(clock: Clock) -> None:
    service, scope, _ = runner(clock)
    scope.hooks["query"] = lambda: setattr(clock, "value", 10000)

    result = asyncio.run(service.run(spec()))

    assert result.stop_status is ProcessStopStatus.UNKNOWN
    assert scope.calls.count("query") == 1


def test_late_termination_after_cleanup_deadline_cannot_query_fresh_empty(
    clock: Clock,
) -> None:
    service, scope, _ = runner(clock)
    scope.hooks["terminate"] = lambda: setattr(clock, "value", 10000)

    result = asyncio.run(service.run(spec()))

    assert result.stop_status is ProcessStopStatus.UNKNOWN
    assert "query" not in scope.calls


def test_initial_binding_mismatch_never_seals_or_launches_foreign_scope(
    clock: Clock,
) -> None:
    expected = binding()
    foreign = replace(
        expected, intent=replace(expected.intent, execution_id=UUID(int=125))
    )
    scope = Scope(foreign)

    with pytest.raises(ProcessScopeUnavailable):
        RunTrustedProcess(expected, scope, Guard(), clock)

    assert scope.calls == []


def test_real_h4_own_clock_expiry_after_prepare_cannot_resume(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    execution, clock = execution_context(), Clock()
    original = binding()
    reference = execution.delivery.outcome.assignment
    acknowledgement = execution.journal.state.acknowledgement
    assert reference is not None and acknowledgement is not None
    expected = replace(
        original,
        intent=replace(original.intent, acknowledgement=acknowledgement),
        reference=reference,
    )
    scope = Scope(expected)

    async def scenario() -> None:
        async with live_supervisor(execution, clock, monkeypatch) as supervisor:
            scope.hooks["prepare"] = lambda: setattr(clock, "value", 10000)
            service = RunTrustedProcess(expected, scope, supervisor, clock)
            result = await service.run(spec())
            assert result.reason is TrustedProcessStopReason.AUTHORITY_LOST

    asyncio.run(scenario())

    assert "resume" not in scope.calls and scope.calls[-3:] == [
        "seal",
        "terminate",
        "query",
    ]
