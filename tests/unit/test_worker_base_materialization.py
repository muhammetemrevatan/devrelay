import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import timedelta
from typing import cast

import pytest

import devrelay.application.worker_heartbeats as supervision
from devrelay.application.contracts.base_sources import (
    BaseMaterialization,
    BaseMaterializationPolicy,
)
from devrelay.application.contracts.claims import WorkerClaimRequest
from devrelay.application.contracts.heartbeats import WorkerHeartbeatExchange
from devrelay.application.contracts.worker_recovery import WorkerRecoveryState
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.contracts.workspace_recovery import (
    WorkerWorkspaceCompletion,
    WorkerWorkspaceIntent,
    WorkspacePreparationDecision,
)
from devrelay.application.lease_deadlines import LeaseDeadlineRejected
from devrelay.application.ports.base_materialization import BaseMaterializationPort
from devrelay.application.ports.base_source import BaseSourcePort, BaseSourceUnavailable
from devrelay.application.ports.worker_journal import (
    WorkerClaimJournal,
    WorkerStateUnavailable,
)
from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from devrelay.application.worker_claims import PrepareWorkerClaim, RequestWorkerClaim
from devrelay.application.worker_heartbeats import HeartbeatSupervisor
from tests.unit.test_materialize_base import Context, context
from tests.unit.test_worker_assignment_fetch import Remote
from tests.unit.test_worker_execution_authority import Clock
from tests.unit.test_worker_heartbeat_exchange import Endpoint, Local


class Journal(Local):
    async def get_or_create_workspace(
        self, request: WorkerClaimRequest, candidate: WorkerWorkspaceIntent
    ) -> WorkspacePreparationDecision:
        raise AssertionError("Materialization cannot create workspace history.")

    async def record_workspace_completion(
        self, request: WorkerClaimRequest, completion: WorkerWorkspaceCompletion
    ) -> WorkerWorkspaceCompletion:
        raise AssertionError("Materialization cannot replace workspace completion.")


class UnavailableResource:
    def __getattr__(self, name: str) -> object:
        raise AssertionError("Unconfigured materialization cannot access resources.")


def worker(
    setup: Context, clock: Clock, *, configured: bool = True
) -> tuple[RequestWorkerClaim, Journal, Endpoint]:
    reference = setup.delivery.outcome.assignment
    assert reference is not None
    authorization = WorkerAuthorization(
        worker_id=reference.worker_id,
        allowed_repository_ids=frozenset({reference.repository_id}),
        credential_expires_at=reference.issued_at + timedelta(hours=1),
    )
    journal = Journal(cast(WorkerRecoveryState, setup.journal.state), setup.calls)
    remote = Remote(authorization, setup.delivery, setup.calls)
    endpoint = Endpoint(authorization, journal, remote, setup.calls)
    preparation = PrepareWorkerClaim(
        journal,
        heartbeat_journal=journal,
        workspace_journal=journal if configured else None,
    )
    return (
        RequestWorkerClaim(
            preparation,
            endpoint,
            authorization,
            heartbeat_gateway=endpoint,
            monotonic_clock=clock,
        ),
        journal,
        endpoint,
    )


@asynccontextmanager
async def active_supervisor(
    client: RequestWorkerClaim, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[HeartbeatSupervisor]:
    ready, stop_wait = asyncio.Event(), asyncio.Event()

    async def tick(
        task: asyncio.Future[WorkerHeartbeatExchange] | None, timeout: float
    ) -> None:
        if task is not None:
            await asyncio.sleep(0)
        else:
            ready.set()
            await stop_wait.wait()

    monkeypatch.setattr(supervision, "_wait_for_tick", tick)
    retained = client.heartbeat_supervisor()
    maintenance = asyncio.create_task(retained.run())
    await ready.wait()
    try:
        yield retained
    finally:
        retained.request_stop()
        stop_wait.set()
        await maintenance


@asynccontextmanager
async def public_materialization(
    setup: Context, facade: str, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[Callable[[], Awaitable[BaseMaterialization]]]:
    client, journal, _ = worker(setup, Clock())
    preparation = PrepareWorkerClaim(journal, workspace_journal=journal)
    async with active_supervisor(client, monkeypatch) as retained:

        async def invoke() -> BaseMaterialization:
            if facade == "prepare":
                return await preparation.materialize_base(
                    setup.delivery,
                    retained,
                    setup.workspace,
                    setup.reservation,
                    setup.source,
                )
            return await client.materialize_base(
                setup.delivery, setup.workspace, setup.reservation, setup.source
            )

        yield invoke


def test_preparation_without_workspace_journal_fails_before_resource_access() -> None:
    setup = context()
    unavailable = UnavailableResource()
    preparation = PrepareWorkerClaim(cast(WorkerClaimJournal, unavailable))

    with pytest.raises(WorkerStateUnavailable, match="Workspace recovery"):
        asyncio.run(
            preparation.materialize_base(
                setup.delivery,
                cast(HeartbeatSupervisor, unavailable),
                cast(BaseMaterializationPort, unavailable),
                setup.reservation,
                cast(BaseSourcePort, unavailable),
            )
        )

    assert setup.calls == []


def test_request_without_workspace_journal_fails_before_resource_access() -> None:
    setup, clock = context(), Clock()
    client, _, endpoint = worker(setup, clock, configured=False)
    unavailable = UnavailableResource()

    with pytest.raises(WorkerStateUnavailable, match="Workspace recovery"):
        asyncio.run(
            client.materialize_base(
                setup.delivery,
                cast(BaseMaterializationPort, unavailable),
                setup.reservation,
                cast(BaseSourcePort, unavailable),
            )
        )

    assert setup.calls == []
    assert endpoint.heartbeats == []


def test_preparation_forwards_exact_live_reservation_and_source() -> None:
    setup = context()
    journal = Journal(setup.journal.state, setup.calls)
    preparation = PrepareWorkerClaim(journal, workspace_journal=journal)
    original = journal.state

    result = asyncio.run(
        preparation.materialize_base(
            setup.delivery,
            cast(HeartbeatSupervisor, setup.guard),
            setup.workspace,
            setup.reservation,
            setup.source,
        )
    )

    assert result.reservation is setup.reservation
    assert setup.source.request == setup.snapshot.request
    assert setup.workspace.written == ["README.md"]
    assert journal.state is original


def test_preparation_cannot_restore_equal_historical_reservation() -> None:
    setup = context()
    journal = Journal(setup.journal.state, setup.calls)
    preparation = PrepareWorkerClaim(journal, workspace_journal=journal)

    with pytest.raises(WorkspaceUnavailable):
        asyncio.run(
            preparation.materialize_base(
                setup.delivery,
                cast(HeartbeatSupervisor, setup.guard),
                setup.workspace,
                replace(setup.reservation),
                setup.source,
            )
        )

    assert setup.source.request is None
    assert not setup.workspace.written


@pytest.mark.parametrize("facade", ["prepare", "request"])
def test_public_materialization_uses_supplied_finite_wait_policy(
    facade: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup, clock = context(), Clock()
    client, journal, _ = worker(setup, clock)
    preparation = PrepareWorkerClaim(journal, workspace_journal=journal)
    policy = BaseMaterializationPolicy(
        operation_timeout_seconds=0.05, total_timeout_seconds=1
    )

    async def scenario() -> None:
        setup.source.release = asyncio.get_running_loop().create_future()
        try:
            with pytest.raises(WorkspaceUnavailable, match="wait budget"):
                if facade == "prepare":
                    await preparation.materialize_base(
                        setup.delivery,
                        cast(HeartbeatSupervisor, setup.guard),
                        setup.workspace,
                        setup.reservation,
                        setup.source,
                        policy=policy,
                    )
                else:
                    async with active_supervisor(client, monkeypatch):
                        await client.materialize_base(
                            setup.delivery,
                            setup.workspace,
                            setup.reservation,
                            setup.source,
                            policy=policy,
                        )
        finally:
            setup.source.release.cancel()

    asyncio.run(scenario())

    assert setup.workspace.sealed
    assert not setup.workspace.written


def test_request_without_live_supervisor_cannot_fetch_source() -> None:
    setup, clock = context(), Clock()
    client, _, endpoint = worker(setup, clock)

    with pytest.raises(LeaseDeadlineRejected):
        asyncio.run(
            client.materialize_base(
                setup.delivery, setup.workspace, setup.reservation, setup.source
            )
        )

    assert setup.source.request is None
    assert endpoint.heartbeats == []
    assert not setup.workspace.written


@pytest.mark.parametrize("phase", ["source", "native"])
def test_request_keeps_heartbeat_exchange_available_during_materialization(
    phase: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup, clock = context(), Clock()
    client, journal, endpoint = worker(setup, clock)

    async def scenario() -> None:
        async with active_supervisor(client, monkeypatch) as retained:
            await export(retained)

    async def export(retained: HeartbeatSupervisor) -> None:
        original_history = journal.state.workspace_completion
        ready = asyncio.Event()
        if phase == "source":
            setup.source.entered = ready
            setup.source.release = asyncio.get_running_loop().create_future()
        else:
            setup.workspace.entered = ready
            setup.workspace.release = asyncio.get_running_loop().create_future()
        exporting = asyncio.create_task(
            client.materialize_base(
                setup.delivery, setup.workspace, setup.reservation, setup.source
            )
        )
        await ready.wait()
        try:
            renewal = await asyncio.wait_for(client.heartbeat(), timeout=1)
            assert renewal.result.renewed
            assert len(endpoint.heartbeats) == 2
            assert journal.state.last_heartbeat == renewal.intent
            assert client.heartbeat_supervisor() is retained
            if phase == "source":
                assert setup.source.release is not None
                setup.source.release.set_result(setup.snapshot)
            else:
                assert setup.workspace.release is not None
                setup.workspace.release.set_result(
                    BaseMaterialization(
                        reservation=setup.reservation,
                        source=setup.snapshot.request,
                        root_tree_sha=setup.snapshot.root_tree_sha,
                        manifest_digest=setup.snapshot.fingerprint(),
                        file_count=1,
                        total_bytes=len(setup.snapshot.files[0].content),
                    )
                )
            result = await exporting
            assert result.reservation is setup.reservation
            assert journal.state.workspace_completion is original_history
        finally:
            if not exporting.done():
                exporting.cancel()
                await asyncio.gather(exporting, return_exceptions=True)

    asyncio.run(scenario())

    assert setup.workspace.written == ["README.md"]
    assert setup.workspace.seals == 0


@pytest.mark.parametrize("facade", ["prepare", "request"])
def test_empty_completed_export_cannot_refetch_through_public_facade(
    facade: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup = context()
    setup.source.snapshot = replace(setup.snapshot, files=())

    async def scenario() -> None:
        async with public_materialization(setup, facade, monkeypatch) as invoke:
            result = await invoke()
            assert result.file_count == result.total_bytes == 0
            with pytest.raises(WorkspaceUnavailable):
                await invoke()

    asyncio.run(scenario())

    assert setup.calls.count("source") == setup.calls.count("write") == 1
    assert setup.workspace.seals == 0


@pytest.mark.parametrize("facade", ["prepare", "request"])
def test_concurrent_public_export_refuses_second_without_sealing_active_scope(
    facade: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup = context()

    async def scenario() -> None:
        setup.source.entered = asyncio.Event()
        setup.source.release = asyncio.get_running_loop().create_future()
        async with public_materialization(setup, facade, monkeypatch) as invoke:
            first = asyncio.ensure_future(invoke())
            await setup.source.entered.wait()
            try:
                with pytest.raises(WorkspaceUnavailable):
                    await invoke()
                assert setup.workspace.seals == 0
                assert setup.calls.count("source") == 1
                setup.source.release.set_result(setup.snapshot)
                result = await first
                assert result.reservation is setup.reservation
            finally:
                if not first.done():
                    first.cancel()
                    await asyncio.gather(first, return_exceptions=True)
                if not setup.source.release.done():
                    setup.source.release.cancel()

    asyncio.run(scenario())

    assert setup.workspace.written == ["README.md"]
    assert setup.workspace.seals == 0


@pytest.mark.parametrize("facade", ["prepare", "request"])
def test_failed_source_read_consumes_public_materialization_admission(
    facade: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup = context()
    setup.source.error = BaseSourceUnavailable("Source fetch failed.")

    async def scenario() -> None:
        async with public_materialization(setup, facade, monkeypatch) as invoke:
            with pytest.raises(BaseSourceUnavailable):
                await invoke()
            setup.source.error = None
            with pytest.raises(WorkspaceUnavailable):
                await invoke()

    asyncio.run(scenario())

    assert setup.calls.count("source") == 1
    assert setup.workspace.seals == 1
    assert not setup.workspace.written
