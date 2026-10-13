import asyncio
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path

import pytest

from devrelay.application.contracts.assignment_delivery import AssignmentUnavailable
from devrelay.application.contracts.claims import WorkerClaimRequest
from devrelay.application.contracts.worker_recovery import (
    WorkerHeartbeatIntent,
    WorkerRecoveryState,
)
from devrelay.application.lease_deadlines import LeaseDeadline
from devrelay.application.ports.control_gateway import (
    ControlClaimRejected,
    ControlUnavailable,
)
from devrelay.application.ports.worker_journal import (
    WorkerStateBusy,
    WorkerStateUnavailable,
)
from devrelay.application.worker_claims import PrepareWorkerClaim, RequestWorkerClaim
from devrelay.application.worker_heartbeats import HeartbeatPolicy, HeartbeatStopReason
from devrelay.bootstrap import worker as bootstrap
from devrelay.config.worker import WorkerSettings
from tests.unit.test_worker_heartbeat_exchange import context


def test_factory_is_lazy_and_retains_one_instance_and_policy() -> None:
    worker, local, endpoint, calls = context()
    policy = HeartbeatPolicy(maximum_exchanges=1)

    supervisor = worker.heartbeat_supervisor(policy=policy)
    repeated = worker.heartbeat_supervisor()

    assert supervisor is repeated and supervisor.policy == policy
    assert calls == [] and supervisor.deadline is None and supervisor.stop_required


def test_factory_rejects_implicit_legacy_clock_without_creating_input() -> None:
    original, local, endpoint, calls = context()
    worker = RequestWorkerClaim(
        PrepareWorkerClaim(local, heartbeat_journal=local),
        endpoint,
        endpoint.authorization,
        heartbeat_gateway=endpoint,
    )

    with pytest.raises(WorkerStateUnavailable):
        worker.heartbeat_supervisor()

    assert calls == [] and local.state.pending_heartbeat is None


def test_factory_rejects_policy_changes_within_one_lineage() -> None:
    worker, local, endpoint, calls = context()
    original = HeartbeatPolicy(maximum_exchanges=1)
    supervisor = worker.heartbeat_supervisor(policy=original)
    changed = replace(original, interval_seconds=16)

    with pytest.raises(WorkerStateUnavailable):
        worker.heartbeat_supervisor(policy=changed)

    assert supervisor.policy == original and calls == []


def test_first_supervised_exchange_replays_prior_manual_completion() -> None:
    worker, local, endpoint, calls = context()
    original = asyncio.run(worker.heartbeat())
    supervisor = worker.heartbeat_supervisor(
        policy=HeartbeatPolicy(maximum_exchanges=1)
    )
    calls.clear()

    stopped = asyncio.run(supervisor.run())

    assert stopped.reason is HeartbeatStopReason.BUDGET_EXHAUSTED
    assert stopped.exchange_settled
    assert local.state.last_heartbeat == original.intent
    assert (
        endpoint.heartbeats[-1].heartbeat_request_id
        == original.intent.heartbeat_request_id
    )
    assert "heartbeat_intent" not in calls
    assert supervisor.deadline is not None and supervisor.deadline.lease_revision == 2


def test_first_supervised_exchange_replays_pending_input_after_local_write_failure() -> (
    None
):
    worker, local, endpoint, calls = context()
    local.completion_failure = True
    with pytest.raises(WorkerStateUnavailable):
        asyncio.run(worker.heartbeat())
    pending = local.state.pending_heartbeat
    local.completion_failure = False
    supervisor = worker.heartbeat_supervisor(
        policy=HeartbeatPolicy(maximum_exchanges=1)
    )
    calls.clear()

    stopped = asyncio.run(supervisor.run())

    assert stopped.reason is HeartbeatStopReason.BUDGET_EXHAUSTED
    assert (
        local.state.last_heartbeat == pending and local.state.pending_heartbeat is None
    )
    assert "heartbeat_intent" not in calls
    assert endpoint.revision == 2


def test_stopped_lineage_allows_exact_observation_without_new_renewal() -> None:
    worker, local, endpoint, calls = context()
    supervisor = worker.heartbeat_supervisor(
        policy=HeartbeatPolicy(maximum_exchanges=1)
    )
    asyncio.run(supervisor.run())
    last = local.state.last_heartbeat
    calls.clear()

    recovered = asyncio.run(worker.heartbeat())

    assert recovered.intent == last and not recovered.result.renewed
    assert endpoint.revision == 2 and "heartbeat_intent" not in calls
    assert supervisor.stop_required


def test_same_client_cannot_restart_supervision_after_a_terminal_stop() -> None:
    worker, local, endpoint, calls = context()
    supervisor = worker.heartbeat_supervisor(
        policy=HeartbeatPolicy(maximum_exchanges=1)
    )
    asyncio.run(supervisor.run())
    retained = worker.heartbeat_supervisor()
    count = len(endpoint.heartbeats)

    with pytest.raises(WorkerStateBusy):
        asyncio.run(retained.run())

    assert supervisor is retained and len(endpoint.heartbeats) == count


def test_requested_stop_before_any_input_does_not_permit_a_manual_new_renewal() -> None:
    worker, local, endpoint, calls = context()
    supervisor = worker.heartbeat_supervisor()
    supervisor.request_stop()

    with pytest.raises(AssignmentUnavailable):
        asyncio.run(worker.heartbeat())

    assert calls == [] and local.state.pending_heartbeat is None


def test_stopped_lineage_does_not_send_pending_input_with_unknown_server_acceptance() -> (
    None
):
    worker, local, endpoint, calls = context()
    endpoint.error = ControlUnavailable("Failed before server acceptance.")
    with pytest.raises(ControlUnavailable):
        asyncio.run(worker.heartbeat())
    pending = local.state.pending_heartbeat
    supervisor = worker.heartbeat_supervisor()
    supervisor.request_stop()
    endpoint.error = None
    count = len(endpoint.heartbeats)

    with pytest.raises(ControlClaimRejected):
        asyncio.run(worker.heartbeat())

    assert local.state.pending_heartbeat == pending
    assert len(endpoint.heartbeats) == count and endpoint.revision == 1


def test_stop_during_recovery_prevents_a_new_input_or_http_exchange(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker, local, endpoint, calls = context()
    supervisor = worker.heartbeat_supervisor()
    original = local.state

    async def scenario() -> None:
        reached, release = asyncio.Event(), asyncio.Event()

        async def recover() -> WorkerRecoveryState:
            reached.set()
            await release.wait()
            return original

        monkeypatch.setattr(local, "load_state", recover)
        exchange = asyncio.create_task(worker.heartbeat())
        await reached.wait()
        supervisor.request_stop()
        release.set()
        with pytest.raises(AssignmentUnavailable):
            await exchange

    asyncio.run(scenario())

    assert local.state is original and calls == []
    assert endpoint.heartbeats == []


def test_stop_during_input_commit_retains_pending_without_sending_http(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker, local, endpoint, calls = context()
    supervisor = worker.heartbeat_supervisor()
    original_prepare = local.get_or_create_heartbeat

    async def scenario() -> None:
        committed, release = asyncio.Event(), asyncio.Event()

        async def prepare(
            request: WorkerClaimRequest, intent: WorkerHeartbeatIntent
        ) -> WorkerHeartbeatIntent:
            result = await original_prepare(request, intent)
            committed.set()
            await release.wait()
            return result

        monkeypatch.setattr(local, "get_or_create_heartbeat", prepare)
        exchange = asyncio.create_task(worker.heartbeat())
        await committed.wait()
        supervisor.request_stop()
        release.set()
        with pytest.raises(ControlClaimRejected):
            await exchange

    asyncio.run(scenario())

    assert local.state.pending_heartbeat is not None
    assert endpoint.heartbeats == [] and "heartbeat_http" not in calls


def test_bootstrap_injects_one_clock_for_exchange_and_supervision_offloading_its_loader(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    original, local, endpoint, calls = context()
    constructor_threads: list[int] = []
    read_threads: list[int] = []
    values = iter(range(100, 200))
    settings = WorkerSettings(
        control_url="https://control.example/",
        auth_token="test_only_worker_token_0123456789abcdefghijklmnop",
        workspace_root=tmp_path / "workspaces",
    )

    def clock() -> float:
        read_threads.append(threading.get_ident())
        return float(next(values))

    def construct_clock() -> object:
        constructor_threads.append(threading.get_ident())
        return clock

    @asynccontextmanager
    async def journal(
        *args: object, **kwargs: object
    ) -> AsyncIterator[PrepareWorkerClaim]:
        yield PrepareWorkerClaim(local, heartbeat_journal=local)

    monkeypatch.setattr(bootstrap, "WindowsLeaseClock", construct_clock)
    monkeypatch.setattr(bootstrap, "HttpControlGateway", lambda client: endpoint)
    monkeypatch.setattr(bootstrap, "open_worker_journal", journal)
    orchestrator_thread = threading.get_ident()

    async def scenario() -> tuple[HeartbeatStopReason, LeaseDeadline | None]:
        async with bootstrap.open_worker_claim_client(
            settings, state_directory=tmp_path / "private"
        ) as worker:
            supervisor = worker.heartbeat_supervisor(
                policy=HeartbeatPolicy(maximum_exchanges=1)
            )
            result = await supervisor.run()
            return result.reason, supervisor.deadline

    reason, deadline = asyncio.run(scenario())

    assert reason is HeartbeatStopReason.BUDGET_EXHAUSTED
    assert deadline is not None and deadline.authority_request_started_at == 101
    assert constructor_threads and all(
        value != orchestrator_thread for value in constructor_threads
    )
    assert read_threads and all(value == orchestrator_thread for value in read_threads)
    assert len(endpoint.heartbeats) == 1
