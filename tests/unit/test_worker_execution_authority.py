import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import timedelta
from uuid import UUID

import pytest

import devrelay.application.worker_heartbeats as heartbeat_supervision
from devrelay.application.contracts.heartbeats import (
    HeartbeatResult,
    LeaseAuthorityObservation,
    WorkerHeartbeatExchange,
)
from devrelay.application.contracts.worker_recovery import WorkerHeartbeatIntent
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.lease_deadlines import LeaseDeadlineRejected
from devrelay.application.worker_executions import PrepareAssignedExecution
from devrelay.application.worker_heartbeats import (
    HeartbeatPolicy,
    HeartbeatStopReason,
    HeartbeatSupervisor,
)
from tests.unit.test_worker_execution_preparation import Context, context, policy


class Clock:
    def __init__(self) -> None:
        self.value = 100.0

    def __call__(self) -> float:
        return self.value


@asynccontextmanager
async def live_supervisor(
    setup: Context, clock: Clock, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[HeartbeatSupervisor]:
    reference = setup.delivery.outcome.assignment
    assert reference is not None
    intent = WorkerHeartbeatIntent(
        heartbeat_request_id=UUID(int=50),
        claim_request_id=setup.delivery.outcome.claim_request_id,
        attempt_id=reference.attempt_id,
        generation=1,
        expected_lease_revision=1,
    )
    ready, release = asyncio.Event(), asyncio.Event()

    async def remote() -> WorkerHeartbeatExchange:
        return WorkerHeartbeatExchange(
            intent=intent,
            result=HeartbeatResult(
                heartbeat_request_id=intent.heartbeat_request_id,
                authority=LeaseAuthorityObservation(
                    reference=reference,
                    lease_revision=2,
                    observed_at=reference.issued_at + timedelta(seconds=20),
                    updated_at=reference.issued_at + timedelta(seconds=20),
                    current_expires_at=reference.expires_at + timedelta(seconds=30),
                ),
                renewed=True,
                stop_required=False,
            ),
            request_started_at=clock(),
            received_at=clock(),
        )

    async def tick(
        task: asyncio.Future[WorkerHeartbeatExchange] | None, timeout: float
    ) -> None:
        if task is not None:
            await asyncio.sleep(0)
        else:
            ready.set()
            await release.wait()

    monkeypatch.setattr(heartbeat_supervision, "_wait_for_tick", tick)
    authorization = WorkerAuthorization(
        worker_id=reference.worker_id,
        allowed_repository_ids=frozenset({reference.repository_id}),
        credential_expires_at=reference.issued_at + timedelta(hours=1),
    )
    supervisor = HeartbeatSupervisor(remote, authorization, clock, HeartbeatPolicy())
    running = asyncio.create_task(supervisor.run())
    await ready.wait()
    try:
        yield supervisor
    finally:
        supervisor.request_stop()
        release.set()
        await running


def test_public_guard_rejects_supervisor_that_has_never_observed_authority() -> None:
    setup, clock = context(), Clock()
    reference = setup.delivery.outcome.assignment
    assert reference is not None

    async def forbidden() -> WorkerHeartbeatExchange:
        raise AssertionError("The guard must not start an exchange.")

    supervisor = HeartbeatSupervisor(
        forbidden,
        WorkerAuthorization(
            worker_id=reference.worker_id,
            allowed_repository_ids=frozenset({reference.repository_id}),
            credential_expires_at=reference.issued_at + timedelta(hours=1),
        ),
        clock,
        HeartbeatPolicy(),
    )

    with pytest.raises(LeaseDeadlineRejected):
        supervisor.require_current_assignment(reference)

    assert supervisor.deadline is None
    assert supervisor.stop_required
    assert supervisor.stop is None


def test_current_guard_uses_shared_clock_between_monitor_ticks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup, clock = context(), Clock()

    async def scenario() -> HeartbeatSupervisor:
        async with live_supervisor(setup, clock, monkeypatch) as supervisor:
            reference = setup.delivery.outcome.assignment
            assert reference is not None
            assert supervisor.deadline is not None
            clock.value = supervisor.deadline.stop_at

            with pytest.raises(LeaseDeadlineRejected):
                supervisor.require_current_assignment(reference)

            assert supervisor.stop is not None
            assert supervisor.stop.reason is HeartbeatStopReason.DEADLINE_EXPIRED
            return supervisor

    supervisor = asyncio.run(scenario())

    assert supervisor.stop_required


def test_current_guard_latches_clock_failure_before_preparation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup, clock = context(), Clock()

    async def scenario() -> HeartbeatSupervisor:
        async with live_supervisor(setup, clock, monkeypatch) as supervisor:
            reference = setup.delivery.outcome.assignment
            assert reference is not None
            clock.value = float("nan")

            with pytest.raises(LeaseDeadlineRejected):
                supervisor.require_current_assignment(reference)

            assert supervisor.stop is not None
            assert supervisor.stop.reason is HeartbeatStopReason.CLOCK_FAILED
            return supervisor

    supervisor = asyncio.run(scenario())

    assert supervisor.stop_required


def test_current_guard_rejects_another_original_assignment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup, clock = context(), Clock()

    async def scenario() -> HeartbeatSupervisor:
        async with live_supervisor(setup, clock, monkeypatch) as supervisor:
            reference = setup.delivery.outcome.assignment
            assert reference is not None
            foreign = replace(reference, attempt_id=UUID(int=99))

            with pytest.raises(LeaseDeadlineRejected):
                supervisor.require_current_assignment(foreign)

            assert supervisor.stop is not None
            assert supervisor.stop.reason is HeartbeatStopReason.INVALID_EVIDENCE
            return supervisor

    supervisor = asyncio.run(scenario())

    assert supervisor.stop_required


def test_post_commit_clock_jump_retains_execution_without_live_preparation_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup, clock = context(), Clock()

    async def scenario() -> HeartbeatSupervisor:
        async with live_supervisor(setup, clock, monkeypatch) as supervisor:
            assert supervisor.deadline is not None
            setup.journal.after_commit = lambda: setattr(clock, "value", 300)
            service = PrepareAssignedExecution(setup.journal, supervisor, policy())

            with pytest.raises(LeaseDeadlineRejected):
                await service.execute(setup.delivery)

            assert supervisor.stop is not None
            assert supervisor.stop.reason is HeartbeatStopReason.DEADLINE_EXPIRED
            return supervisor

    supervisor = asyncio.run(scenario())

    assert setup.journal.state.execution is not None
    assert setup.calls == ["recover", "commit"]
    assert supervisor.stop_required


def test_fresh_shared_authority_prepares_durable_input_without_process_launch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup, clock = context(), Clock()

    async def scenario() -> None:
        async with live_supervisor(setup, clock, monkeypatch) as supervisor:
            service = PrepareAssignedExecution(setup.journal, supervisor, policy())

            recorded = await service.execute(setup.delivery)

            assert recorded is setup.journal.state.execution
            assert not supervisor.stop_required

    asyncio.run(scenario())

    assert setup.calls == ["recover", "commit"]
