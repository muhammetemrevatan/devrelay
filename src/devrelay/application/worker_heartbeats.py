import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from enum import StrEnum
from math import isfinite

from devrelay.application.contracts.heartbeats import WorkerHeartbeatExchange
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.lease_deadlines import LeaseDeadline, LeaseDeadlineRejected
from devrelay.application.ports.worker_journal import WorkerStateBusy


@dataclass(frozen=True, slots=True, kw_only=True)
class HeartbeatPolicy:
    interval_seconds: float = 15
    exchange_timeout_seconds: float = 10
    stop_margin_seconds: float = 5
    monitor_interval_seconds: float = 0.25
    cleanup_timeout_seconds: float = 1
    maximum_exchanges: int = 4096

    def __post_init__(self) -> None:
        for name, minimum, maximum in (
            ("interval_seconds", 11, 60),
            ("exchange_timeout_seconds", 1, 30),
            ("stop_margin_seconds", 3, 30),
            ("monitor_interval_seconds", 0.05, 1),
            ("cleanup_timeout_seconds", 0.1, 5),
        ):
            value = getattr(self, name)
            if (
                type(value) not in (int, float)
                or not minimum <= value <= maximum
                or not isfinite(value)
            ):
                raise ValueError(
                    "Heartbeat timing policy requires bounded finite numbers."
                )
        if (
            type(self.maximum_exchanges) is not int
            or not 1 <= self.maximum_exchanges <= 4096
        ):
            raise ValueError("Heartbeat exchange budget requires a bounded integer.")


class HeartbeatStopReason(StrEnum):
    REQUESTED = "requested"
    CANCELLED = "cancelled"
    DEADLINE_EXPIRED = "deadline_expired"
    CONTROL_STOP = "control_stop"
    EXCHANGE_FAILED = "exchange_failed"
    EXCHANGE_TIMEOUT = "exchange_timeout"
    INVALID_EVIDENCE = "invalid_evidence"
    CLOCK_FAILED = "clock_failed"
    BUDGET_EXHAUSTED = "budget_exhausted"


@dataclass(frozen=True, slots=True, kw_only=True)
class HeartbeatStop:
    """Latched coordination stop, never process containment.

    Settlement describes the async exchange task only. Control or journal executor
    work may still commit after its cancellation; this is not side-effect quiescence.
    """

    reason: HeartbeatStopReason
    exchange_settled: bool

    def __post_init__(self) -> None:
        if (
            not isinstance(self.reason, HeartbeatStopReason)
            or type(self.exchange_settled) is not bool
        ):
            raise ValueError("Heartbeat stop requires typed bounded evidence.")


async def _wait_for_tick(
    task: asyncio.Future[WorkerHeartbeatExchange] | None, timeout: float
) -> None:
    if task is None:
        await asyncio.sleep(timeout)
    else:
        await asyncio.wait({task}, timeout=timeout)


def _consume_completion(task: asyncio.Future[WorkerHeartbeatExchange]) -> None:
    if not task.cancelled():
        task.exception()


class HeartbeatSupervisor:
    """Explicit one-shot coordination supervision, with no launch or containment.

    Exchange timing and watchdog readings must use the same suspend-aware clock.
    The retained instance cannot revive its stopped execution lineage.
    """

    def __init__(
        self,
        exchange: Callable[[], Awaitable[WorkerHeartbeatExchange]],
        authorization: WorkerAuthorization,
        clock: Callable[[], float],
        policy: HeartbeatPolicy,
    ) -> None:
        if not isinstance(authorization, WorkerAuthorization) or not isinstance(
            policy, HeartbeatPolicy
        ):
            raise ValueError("Heartbeat supervision requires typed scope and policy.")
        self._exchange = exchange
        self._authorization = authorization
        self._clock = clock
        self._policy = policy
        self._deadline: LeaseDeadline | None = None
        self._stop: HeartbeatStop | None = None
        self._stop_event = asyncio.Event()
        self._used = False
        self._last_clock: float | None = None
        self._task: asyncio.Future[WorkerHeartbeatExchange] | None = None

    @property
    def policy(self) -> HeartbeatPolicy:
        return self._policy

    @property
    def deadline(self) -> LeaseDeadline | None:
        return self._deadline

    @property
    def stop_required(self) -> bool:
        return self._stop is not None or self._deadline is None

    @property
    def stop(self) -> HeartbeatStop | None:
        return self._stop

    def _latch(self, reason: HeartbeatStopReason) -> None:
        if self._stop is None:
            self._stop = HeartbeatStop(
                reason=reason, exchange_settled=self._task is None or self._task.done()
            )
            self._stop_event.set()

    def request_stop(self) -> None:
        self._latch(HeartbeatStopReason.REQUESTED)

    async def wait_stopped(self) -> HeartbeatStop:
        await self._stop_event.wait()
        assert self._stop is not None
        return self._stop

    def _read_clock(self) -> float | None:
        try:
            now = self._clock()
            if (
                type(now) not in (int, float)
                or not 0 <= now <= 2**53 - 1
                or not isfinite(now)
                or (self._last_clock is not None and now < self._last_clock)
            ):
                raise ValueError("Invalid supervision clock.")
        except Exception:
            self._latch(HeartbeatStopReason.CLOCK_FAILED)
            return None
        self._last_clock = float(now)
        return float(now)

    def _accept(
        self, exchange: WorkerHeartbeatExchange, started_at: float, now: float
    ) -> None:
        try:
            if not isinstance(exchange, WorkerHeartbeatExchange):
                raise ValueError("Heartbeat exchange requires typed evidence.")
            result, intent = exchange.result, exchange.intent
            authority = result.authority
            if (
                exchange.request_started_at < started_at
                or exchange.received_at > now
                or authority.lease_revision < intent.expected_lease_revision
                or (
                    result.renewed
                    and (
                        authority.lease_revision != intent.expected_lease_revision + 1
                        or authority.current_expires_at
                        > self._authorization.credential_expires_at
                    )
                )
                or (
                    not result.stop_required
                    and authority.observed_at
                    >= self._authorization.credential_expires_at
                )
            ):
                raise ValueError("Heartbeat evidence failed current correlation.")
            if self._deadline is None:
                deadline = LeaseDeadline.from_observation(
                    authority,
                    self._authorization,
                    request_started_at=exchange.request_started_at,
                    received_at=exchange.received_at,
                    stop_margin_seconds=self._policy.stop_margin_seconds,
                    stop_required=result.stop_required,
                )
            else:
                deadline = self._deadline.observe(
                    authority,
                    self._authorization,
                    request_started_at=exchange.request_started_at,
                    received_at=exchange.received_at,
                    stop_required=result.stop_required,
                )
            must_stop = deadline.must_stop(now)
        except ValueError, TypeError:
            self._latch(HeartbeatStopReason.INVALID_EVIDENCE)
            return
        self._deadline = deadline
        if result.stop_required:
            self._latch(HeartbeatStopReason.CONTROL_STOP)
        elif must_stop:
            self._latch(HeartbeatStopReason.DEADLINE_EXPIRED)

    async def _loop(self) -> None:
        exchanges = 0
        next_due = 0.0
        started_at = 0.0
        while self._stop is None:
            now = self._read_clock()
            if now is None:
                break
            if self._deadline is not None and self._deadline.must_stop(now):
                self._latch(HeartbeatStopReason.DEADLINE_EXPIRED)
                break
            if (
                self._task is not None
                and now - started_at >= self._policy.exchange_timeout_seconds
            ):
                self._latch(HeartbeatStopReason.EXCHANGE_TIMEOUT)
                break
            if self._task is not None and self._task.done():
                task, self._task = self._task, None
                try:
                    response = task.result()
                except asyncio.CancelledError:
                    self._latch(HeartbeatStopReason.EXCHANGE_FAILED)
                except LeaseDeadlineRejected:
                    self._latch(HeartbeatStopReason.CLOCK_FAILED)
                except Exception:
                    self._latch(HeartbeatStopReason.EXCHANGE_FAILED)
                else:
                    self._accept(response, started_at, now)
                    next_due = now + self._policy.interval_seconds
                if self._stop is None and exchanges >= self._policy.maximum_exchanges:
                    self._latch(HeartbeatStopReason.BUDGET_EXHAUSTED)
            if self._stop is not None:
                break
            if self._task is None and now >= next_due:
                started_at = now
                self._task = asyncio.ensure_future(self._exchange())
                exchanges += 1
            await _wait_for_tick(self._task, self._policy.monitor_interval_seconds)

    async def _cleanup(self) -> None:
        task = self._task
        if task is None:
            return
        task.add_done_callback(_consume_completion)
        if not task.done():
            task.cancel()
            await asyncio.wait({task}, timeout=self._policy.cleanup_timeout_seconds)
        if self._stop is not None:
            self._stop = replace(self._stop, exchange_settled=task.done())

    async def run(self) -> HeartbeatStop:
        if self._used:
            raise WorkerStateBusy("Heartbeat supervision has already started.")
        self._used = True
        try:
            await self._loop()
        except asyncio.CancelledError:
            self._latch(HeartbeatStopReason.CANCELLED)
            raise
        except Exception:
            self._latch(HeartbeatStopReason.EXCHANGE_FAILED)
        finally:
            await self._cleanup()
        assert self._stop is not None
        return self._stop
