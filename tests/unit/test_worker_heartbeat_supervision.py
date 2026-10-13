import asyncio
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest

import devrelay.application.worker_heartbeats as supervision
from devrelay.application.contracts.heartbeats import (
    HeartbeatResult,
    LeaseAuthorityObservation,
    WorkerHeartbeatExchange,
)
from devrelay.application.contracts.worker_recovery import (
    AssignmentReference,
    WorkerHeartbeatIntent,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.lease_deadlines import LeaseDeadlineRejected
from devrelay.application.ports.control_gateway import (
    ControlAccessDenied,
    ControlUnavailable,
    InvalidControlResponse,
)
from devrelay.application.ports.worker_journal import (
    WorkerStateBusy,
    WorkerStateUnavailable,
)
from devrelay.application.worker_heartbeats import (
    HeartbeatPolicy,
    HeartbeatStop,
    HeartbeatStopReason,
    HeartbeatSupervisor,
)

NOW = datetime(2026, 10, 13, tzinfo=UTC)


class Clock:
    def __init__(self) -> None:
        self.value = 100.0

    def __call__(self) -> float:
        return self.value


def authorization() -> WorkerAuthorization:
    return WorkerAuthorization(
        worker_id=UUID(int=1),
        allowed_repository_ids=frozenset({UUID(int=3)}),
        credential_expires_at=NOW + timedelta(seconds=300),
    )


def exchange(
    started: float, received: float, *, number: int = 1
) -> WorkerHeartbeatExchange:
    intent = WorkerHeartbeatIntent(
        heartbeat_request_id=UUID(int=10 + number),
        claim_request_id=UUID(int=2),
        attempt_id=UUID(int=4),
        generation=1,
        expected_lease_revision=number,
    )
    observed = NOW + timedelta(seconds=30 + 15 * (number - 1))
    return WorkerHeartbeatExchange(
        intent=intent,
        result=HeartbeatResult(
            heartbeat_request_id=intent.heartbeat_request_id,
            authority=LeaseAuthorityObservation(
                reference=AssignmentReference(
                    worker_id=UUID(int=1),
                    repository_id=UUID(int=3),
                    job_id=UUID(int=5),
                    attempt_id=UUID(int=4),
                    generation=1,
                    issued_at=NOW,
                    expires_at=NOW + timedelta(seconds=120),
                ),
                lease_revision=number + 1,
                observed_at=observed,
                updated_at=observed,
                current_expires_at=observed + timedelta(seconds=120),
            ),
            renewed=True,
            stop_required=False,
        ),
        request_started_at=started,
        received_at=received,
    )


class Remote:
    def __init__(self, clock: Clock) -> None:
        self.clock = clock
        self.calls: list[float] = []

    async def __call__(self) -> WorkerHeartbeatExchange:
        started = self.clock()
        self.calls.append(started)
        return exchange(started, self.clock(), number=len(self.calls))


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    timer = Clock()

    async def tick(
        task: asyncio.Future[WorkerHeartbeatExchange] | None, timeout: float
    ) -> None:
        await asyncio.sleep(0)
        timer.value += timeout

    monkeypatch.setattr(supervision, "_wait_for_tick", tick)
    return timer


def test_construction_requires_explicit_start_and_grants_no_initial_budget(
    clock: Clock,
) -> None:
    remote = Remote(clock)

    supervisor = HeartbeatSupervisor(remote, authorization(), clock, HeartbeatPolicy())

    assert remote.calls == []
    assert supervisor.stop_required
    assert supervisor.deadline is None
    assert supervisor.stop is None


def test_explicit_run_accepts_fresh_evidence_then_stops_at_finite_exchange_budget(
    clock: Clock,
) -> None:
    remote = Remote(clock)
    supervisor = HeartbeatSupervisor(
        remote, authorization(), clock, HeartbeatPolicy(maximum_exchanges=1)
    )

    result = asyncio.run(supervisor.run())

    assert result.reason is HeartbeatStopReason.BUDGET_EXHAUSTED
    assert result.exchange_settled
    assert remote.calls == [100]
    assert supervisor.deadline is not None
    assert supervisor.deadline.stop_at == 215
    assert supervisor.stop_required


def test_cadence_starts_after_prior_completion_and_does_not_burst(clock: Clock) -> None:
    remote = Remote(clock)
    supervisor = HeartbeatSupervisor(
        remote, authorization(), clock, HeartbeatPolicy(maximum_exchanges=3)
    )

    result = asyncio.run(supervisor.run())

    assert result.reason is HeartbeatStopReason.BUDGET_EXHAUSTED
    assert len(remote.calls) == 3
    assert remote.calls[1] - remote.calls[0] >= 15
    assert remote.calls[2] - remote.calls[1] >= 15


def test_pre_requested_stop_performs_no_exchange(clock: Clock) -> None:
    remote = Remote(clock)
    supervisor = HeartbeatSupervisor(remote, authorization(), clock, HeartbeatPolicy())
    supervisor.request_stop()

    result = asyncio.run(supervisor.run())

    assert result.reason is HeartbeatStopReason.REQUESTED
    assert result.exchange_settled
    assert remote.calls == []


def test_repeated_run_cannot_restart_stopped_lineage(clock: Clock) -> None:
    remote = Remote(clock)
    supervisor = HeartbeatSupervisor(
        remote, authorization(), clock, HeartbeatPolicy(maximum_exchanges=1)
    )
    asyncio.run(supervisor.run())

    with pytest.raises(WorkerStateBusy):
        asyncio.run(supervisor.run())

    assert len(remote.calls) == 1


@pytest.mark.parametrize(
    "error",
    [
        ControlUnavailable("Response lost."),
        ControlAccessDenied("Denied."),
        InvalidControlResponse("Invalid."),
        WorkerStateUnavailable("Unavailable."),
    ],
)
def test_exchange_failure_stops_without_retry_or_returning_authority(
    clock: Clock, error: Exception
) -> None:
    calls: list[str] = []

    async def failed() -> WorkerHeartbeatExchange:
        calls.append("exchange")
        raise error

    supervisor = HeartbeatSupervisor(failed, authorization(), clock, HeartbeatPolicy())

    result = asyncio.run(supervisor.run())

    assert result.reason is HeartbeatStopReason.EXCHANGE_FAILED
    assert calls == ["exchange"]
    assert supervisor.deadline is None


def test_inflight_initial_exchange_is_bounded_by_operation_timeout(
    clock: Clock,
) -> None:
    calls: list[str] = []

    async def blocked() -> WorkerHeartbeatExchange:
        calls.append("durable_input")
        await asyncio.Event().wait()
        raise AssertionError("Unreachable after cancellation.")

    supervisor = HeartbeatSupervisor(
        blocked, authorization(), clock, HeartbeatPolicy(exchange_timeout_seconds=1)
    )

    result = asyncio.run(supervisor.run())

    assert result.reason is HeartbeatStopReason.EXCHANGE_TIMEOUT
    assert result.exchange_settled
    assert calls == ["durable_input"]
    assert supervisor.stop_required


def test_watchdog_detects_suspend_jump_while_next_exchange_is_inflight(
    clock: Clock,
) -> None:
    calls: list[str] = []

    async def remote() -> WorkerHeartbeatExchange:
        if not calls:
            calls.append("first")
            return exchange(clock(), clock())
        calls.append("pending_original_input")
        clock.value = 300
        await asyncio.Event().wait()
        raise AssertionError("Unreachable after cancellation.")

    supervisor = HeartbeatSupervisor(
        remote, authorization(), clock, HeartbeatPolicy(exchange_timeout_seconds=30)
    )

    result = asyncio.run(supervisor.run())

    assert result.reason is HeartbeatStopReason.DEADLINE_EXPIRED
    assert calls == ["first", "pending_original_input"]
    assert result.exchange_settled
    assert supervisor.deadline is not None
    assert supervisor.deadline.lease_revision == 2


def test_late_success_with_old_receipt_cannot_revive_expired_local_deadline(
    clock: Clock,
) -> None:
    calls: list[str] = []

    async def remote() -> WorkerHeartbeatExchange:
        if not calls:
            calls.append("first")
            return exchange(clock(), clock())
        calls.append("late_success")
        response = exchange(clock(), clock(), number=2)
        clock.value = 216
        return response

    supervisor = HeartbeatSupervisor(
        remote, authorization(), clock, HeartbeatPolicy(exchange_timeout_seconds=30)
    )

    result = asyncio.run(supervisor.run())

    assert result.reason is HeartbeatStopReason.DEADLINE_EXPIRED
    assert supervisor.deadline is not None
    assert supervisor.deadline.lease_revision == 2
    assert len(calls) == 2


def test_suspend_jump_before_scheduling_next_tick_sends_no_catchup_exchange(
    clock: Clock, monkeypatch: pytest.MonkeyPatch
) -> None:
    remote = Remote(clock)
    supervisor = HeartbeatSupervisor(remote, authorization(), clock, HeartbeatPolicy())

    async def tick(
        task: asyncio.Future[WorkerHeartbeatExchange] | None, timeout: float
    ) -> None:
        await asyncio.sleep(0)
        clock.value = 300 if task is None else clock.value + timeout

    monkeypatch.setattr(supervision, "_wait_for_tick", tick)

    result = asyncio.run(supervisor.run())

    assert result.reason is HeartbeatStopReason.DEADLINE_EXPIRED
    assert len(remote.calls) == 1


@pytest.mark.parametrize(
    "invalid", [True, float("nan"), float("inf"), -1, 2**53, 10**1000]
)
def test_invalid_initial_clock_fails_closed_before_network(
    clock: Clock, invalid: Any
) -> None:
    remote = Remote(clock)
    supervisor = HeartbeatSupervisor(
        remote, authorization(), lambda: invalid, HeartbeatPolicy()
    )

    result = asyncio.run(supervisor.run())

    assert result.reason is HeartbeatStopReason.CLOCK_FAILED
    assert remote.calls == []


def test_clock_exception_does_not_expose_values_or_start_network(clock: Clock) -> None:
    remote = Remote(clock)

    def fail() -> float:
        raise RuntimeError("Sensitive clock adapter details.")

    supervisor = HeartbeatSupervisor(remote, authorization(), fail, HeartbeatPolicy())

    result = asyncio.run(supervisor.run())

    assert result.reason is HeartbeatStopReason.CLOCK_FAILED
    assert "Sensitive" not in repr(result)
    assert remote.calls == []


def test_clock_regression_during_exchange_stops_without_acceptance(
    clock: Clock,
) -> None:
    async def remote() -> WorkerHeartbeatExchange:
        response = exchange(clock(), clock())
        clock.value = 90
        return response

    supervisor = HeartbeatSupervisor(remote, authorization(), clock, HeartbeatPolicy())

    result = asyncio.run(supervisor.run())

    assert result.reason is HeartbeatStopReason.CLOCK_FAILED
    assert supervisor.deadline is None


@pytest.mark.parametrize("field", ["worker_id", "repository_id"])
def test_foreign_authority_is_rejected_before_exposure(
    clock: Clock, field: str
) -> None:
    async def remote() -> WorkerHeartbeatExchange:
        response = exchange(clock(), clock())
        changes: dict[str, Any] = {field: UUID(int=99)}
        reference = replace(response.result.authority.reference, **changes)
        return replace(
            response,
            result=replace(
                response.result,
                authority=replace(response.result.authority, reference=reference),
            ),
        )

    supervisor = HeartbeatSupervisor(remote, authorization(), clock, HeartbeatPolicy())

    result = asyncio.run(supervisor.run())

    assert result.reason is HeartbeatStopReason.INVALID_EVIDENCE
    assert supervisor.deadline is None


@pytest.mark.parametrize("future_receipt", [False, True])
def test_exchange_clock_epoch_or_receipt_mismatch_is_rejected(
    clock: Clock, future_receipt: bool
) -> None:
    async def remote() -> WorkerHeartbeatExchange:
        return exchange(
            99 if not future_receipt else clock(), 1000 if future_receipt else clock()
        )

    supervisor = HeartbeatSupervisor(remote, authorization(), clock, HeartbeatPolicy())

    result = asyncio.run(supervisor.run())

    assert result.reason is HeartbeatStopReason.INVALID_EVIDENCE


def test_control_stop_latches_without_another_renewal(clock: Clock) -> None:
    calls: list[str] = []

    async def remote() -> WorkerHeartbeatExchange:
        calls.append("exchange")
        response = exchange(clock(), clock())
        return replace(
            response, result=replace(response.result, renewed=False, stop_required=True)
        )

    supervisor = HeartbeatSupervisor(remote, authorization(), clock, HeartbeatPolicy())

    result = asyncio.run(supervisor.run())

    assert result.reason is HeartbeatStopReason.CONTROL_STOP
    assert calls == ["exchange"]
    assert supervisor.stop_required


@pytest.mark.parametrize("stage", ["durable_input", "http_commit", "local_commit"])
def test_parent_cancellation_after_durable_input_latches_before_cleanup(
    clock: Clock,
    stage: str,
) -> None:
    async def scenario() -> tuple[HeartbeatSupervisor, list[str]]:
        entered = asyncio.Event()
        calls: list[str] = []

        async def blocked() -> WorkerHeartbeatExchange:
            calls.append(stage)
            entered.set()
            await asyncio.Event().wait()
            raise AssertionError("Unreachable after cancellation.")

        supervisor = HeartbeatSupervisor(
            blocked,
            authorization(),
            clock,
            HeartbeatPolicy(exchange_timeout_seconds=30),
        )
        running = asyncio.create_task(supervisor.run())
        await entered.wait()

        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running

        assert supervisor.stop is not None
        assert supervisor.stop.reason is HeartbeatStopReason.CANCELLED
        assert supervisor.stop_required
        return supervisor, calls

    supervisor, calls = asyncio.run(scenario())

    assert supervisor.stop is not None and supervisor.stop.exchange_settled
    assert calls == [stage]


def test_concurrent_run_is_rejected_without_second_exchange(clock: Clock) -> None:
    async def scenario() -> list[str]:
        entered = asyncio.Event()
        calls: list[str] = []

        async def blocked() -> WorkerHeartbeatExchange:
            calls.append("exchange")
            entered.set()
            await asyncio.Event().wait()
            raise AssertionError("Unreachable after cancellation.")

        supervisor = HeartbeatSupervisor(
            blocked,
            authorization(),
            clock,
            HeartbeatPolicy(exchange_timeout_seconds=30),
        )
        running = asyncio.create_task(supervisor.run())
        await entered.wait()

        with pytest.raises(WorkerStateBusy):
            await supervisor.run()

        supervisor.request_stop()
        await running
        return calls

    calls = asyncio.run(scenario())

    assert calls == ["exchange"]


def test_unsettled_cleanup_reports_stop_immediately_and_ignores_late_completion(
    clock: Clock,
) -> None:
    async def scenario() -> tuple[HeartbeatStop, HeartbeatSupervisor, list[str]]:
        entered, release, cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()
        calls: list[str] = []

        async def stubborn() -> WorkerHeartbeatExchange:
            started = clock()
            calls.append("durable_input")
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                await release.wait()
                calls.append("late_local_commit")
                return exchange(started, clock())
            raise AssertionError("Unreachable before cancellation.")

        supervisor = HeartbeatSupervisor(
            stubborn,
            authorization(),
            clock,
            HeartbeatPolicy(cleanup_timeout_seconds=0.1, exchange_timeout_seconds=30),
        )
        running = asyncio.create_task(supervisor.run())
        await entered.wait()

        supervisor.request_stop()
        immediate = await supervisor.wait_stopped()
        await cancelled.wait()
        final = await running
        release.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert immediate.reason is HeartbeatStopReason.REQUESTED
        assert not immediate.exchange_settled
        assert not final.exchange_settled
        return final, supervisor, calls

    final, supervisor, calls = asyncio.run(scenario())

    assert final.reason is HeartbeatStopReason.REQUESTED
    assert supervisor.deadline is None
    assert calls == ["durable_input", "late_local_commit"]


@pytest.mark.parametrize(
    "field,minimum,maximum",
    [
        ("interval_seconds", 11, 60),
        ("exchange_timeout_seconds", 1, 30),
        ("stop_margin_seconds", 3, 30),
        ("monitor_interval_seconds", 0.05, 1),
        ("cleanup_timeout_seconds", 0.1, 5),
    ],
)
@pytest.mark.parametrize("boundary", ["minimum", "maximum"])
def test_policy_accepts_exact_bounded_timing(
    field: str, minimum: float, maximum: float, boundary: str
) -> None:
    values: dict[str, Any] = {field: minimum if boundary == "minimum" else maximum}

    policy = HeartbeatPolicy(**values)

    assert getattr(policy, field) == values[field]


@pytest.mark.parametrize(
    "field",
    [
        "interval_seconds",
        "exchange_timeout_seconds",
        "stop_margin_seconds",
        "monitor_interval_seconds",
        "cleanup_timeout_seconds",
    ],
)
@pytest.mark.parametrize(
    "invalid", [True, False, -1, 1000, float("nan"), float("inf"), 10**1000, "10"]
)
def test_policy_rejects_invalid_timing_without_coercion(
    field: str, invalid: Any
) -> None:
    values: dict[str, Any] = {field: invalid}

    with pytest.raises(ValueError):
        HeartbeatPolicy(**values)


@pytest.mark.parametrize("invalid", [True, False, 0, 4097, 1.0, "5"])
def test_exchange_count_requires_bounded_exact_integer(invalid: Any) -> None:
    with pytest.raises(ValueError):
        HeartbeatPolicy(maximum_exchanges=invalid)


def test_policy_and_stop_values_are_immutable() -> None:
    policy = HeartbeatPolicy()

    with pytest.raises(FrozenInstanceError):
        policy.maximum_exchanges = 1  # type: ignore[misc]  # Verify immutable policy.


def test_cadence_preserves_buffer_above_control_maximum_interval() -> None:
    with pytest.raises(ValueError):
        HeartbeatPolicy(interval_seconds=10)


def test_exchange_clock_failure_stops_even_when_watchdog_clock_recovers(
    clock: Clock,
) -> None:
    journal_inputs: list[str] = []

    async def failed_sampling() -> WorkerHeartbeatExchange:
        journal_inputs.append("original_pending_input")
        raise LeaseDeadlineRejected("Windows lease clock is unavailable.")

    supervisor = HeartbeatSupervisor(
        failed_sampling, authorization(), clock, HeartbeatPolicy()
    )

    result = asyncio.run(supervisor.run())

    assert result.reason is HeartbeatStopReason.CLOCK_FAILED
    assert result.exchange_settled
    assert journal_inputs == ["original_pending_input"]
    assert supervisor.deadline is None
    assert supervisor.stop_required
