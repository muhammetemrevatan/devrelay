import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import cast

import pytest

from devrelay.application.contracts.base_sources import (
    BaseMaterialization,
    BaseMaterializationPolicy,
    BaseSourceSnapshot,
)
from devrelay.application.contracts.editable_workspaces import EditableWorkspace
from devrelay.application.contracts.workspaces import WorkspaceReservation
from devrelay.application.lease_deadlines import LeaseDeadlineRejected
from devrelay.application.ports.editable_workspace import EditableWorkspacePort
from devrelay.application.ports.worker_journal import (
    WorkerClaimJournal,
    WorkerStateUnavailable,
)
from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from devrelay.application.worker_claims import PrepareWorkerClaim
from devrelay.application.worker_heartbeats import HeartbeatSupervisor
from tests.unit.test_materialize_base import Context, Workspace, context
from tests.unit.test_worker_base_materialization import (
    UnavailableResource,
    active_supervisor,
    worker,
)
from tests.unit.test_worker_execution_authority import Clock


class EditableScope(Workspace):
    def __init__(self, setup: Context) -> None:
        super().__init__(setup.workspace.binding, setup.calls)
        self.location = setup.workspace.location
        self.reservation = setup.reservation
        self.materialization: BaseMaterialization | None = BaseMaterialization(
            reservation=setup.reservation,
            source=setup.snapshot.request,
            root_tree_sha=setup.snapshot.root_tree_sha,
            manifest_digest=setup.snapshot.fingerprint(),
            file_count=len(setup.snapshot.files),
            total_bytes=sum(len(item.content) for item in setup.snapshot.files),
        )
        self.handoff_entered: asyncio.Event | None = None
        self.handoff_release: asyncio.Future[EditableWorkspace] | None = None
        self.handoff_error: Exception | None = None
        self.handed_off: list[BaseMaterialization] = []

    async def materialize(
        self, reservation: WorkspaceReservation, snapshot: BaseSourceSnapshot
    ) -> BaseMaterialization:
        result = await super().materialize(reservation, snapshot)
        self.materialization = result
        return result

    async def handoff(self, materialization: BaseMaterialization) -> EditableWorkspace:
        assert materialization is self.materialization
        assert not self.sealed
        self.calls.append("handoff")
        self.handed_off.append(materialization)
        if self.handoff_entered is not None:
            self.handoff_entered.set()
        if self.handoff_error is not None:
            raise self.handoff_error
        if self.handoff_release is not None:
            return await asyncio.shield(self.handoff_release)
        return EditableWorkspace(
            materialization=materialization, location=self.location
        )


def editable_context() -> tuple[Context, EditableScope, BaseMaterialization]:
    setup = context()
    workspace = EditableScope(setup)
    assert workspace.materialization is not None
    return setup, workspace, workspace.materialization


@asynccontextmanager
async def public_handoff(
    setup: Context,
    workspace: EditableScope,
    result: BaseMaterialization,
    facade: str,
    monkeypatch: pytest.MonkeyPatch,
    *,
    policy: BaseMaterializationPolicy | None = None,
) -> AsyncIterator[Callable[[], Awaitable[EditableWorkspace]]]:
    client, journal, _ = worker(setup, Clock())
    preparation = PrepareWorkerClaim(journal, workspace_journal=journal)
    async with active_supervisor(client, monkeypatch) as supervisor:

        async def invoke() -> EditableWorkspace:
            chosen = policy or BaseMaterializationPolicy()
            if facade == "prepare":
                return await preparation.handoff_materialized_base(
                    setup.delivery, supervisor, workspace, result, policy=chosen
                )
            return await client.handoff_materialized_base(
                setup.delivery, workspace, result, policy=chosen
            )

        yield invoke


@pytest.mark.parametrize("facade", ["prepare", "request"])
def test_missing_workspace_journal_fails_before_resource_access(facade: str) -> None:
    setup, _, result = editable_context()
    unavailable = UnavailableResource()
    preparation = PrepareWorkerClaim(cast(WorkerClaimJournal, unavailable))
    client, _, endpoint = worker(setup, Clock(), configured=False)

    async def scenario() -> None:
        with pytest.raises(WorkerStateUnavailable, match="Workspace recovery"):
            if facade == "prepare":
                await preparation.handoff_materialized_base(
                    setup.delivery,
                    cast(HeartbeatSupervisor, unavailable),
                    cast(EditableWorkspacePort, unavailable),
                    result,
                )
            else:
                await client.handoff_materialized_base(
                    setup.delivery, cast(EditableWorkspacePort, unavailable), result
                )

    asyncio.run(scenario())

    assert setup.calls == []
    assert endpoint.heartbeats == []


@pytest.mark.parametrize("facade", ["prepare", "request"])
def test_public_handoff_retains_exact_result_and_original_journal_history(
    facade: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup, workspace, result = editable_context()
    original_history = setup.journal.state

    async def scenario() -> None:
        async with public_handoff(
            setup, workspace, result, facade, monkeypatch
        ) as invoke:
            editable = await invoke()
            assert editable.materialization is result
            assert editable.location is workspace.location
            with pytest.raises(WorkspaceUnavailable):
                await invoke()

    asyncio.run(scenario())

    assert workspace.handed_off == [result]
    assert workspace.seals == 0
    assert setup.journal.state is original_history
    assert setup.source.request is None
    assert not workspace.written


@pytest.mark.parametrize("facade", ["prepare", "request"])
def test_equal_reconstructed_result_cannot_handoff_through_public_facade(
    facade: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup, workspace, result = editable_context()

    async def scenario() -> None:
        async with public_handoff(
            setup, workspace, replace(result), facade, monkeypatch
        ) as invoke:
            with pytest.raises(WorkspaceUnavailable):
                await invoke()

    asyncio.run(scenario())

    assert workspace.handed_off == []
    assert workspace.sealed


@pytest.mark.parametrize("facade", ["prepare", "request"])
def test_concurrent_public_handoff_does_not_seal_or_repeat_active_transition(
    facade: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup, workspace, result = editable_context()

    async def scenario() -> None:
        workspace.handoff_entered = asyncio.Event()
        workspace.handoff_release = asyncio.get_running_loop().create_future()
        async with public_handoff(
            setup, workspace, result, facade, monkeypatch
        ) as invoke:
            first = asyncio.ensure_future(invoke())
            await workspace.handoff_entered.wait()
            try:
                with pytest.raises(WorkspaceUnavailable):
                    await invoke()
                assert workspace.seals == 0
                workspace.handoff_release.set_result(
                    EditableWorkspace(
                        materialization=result, location=workspace.location
                    )
                )
                assert (await first).materialization is result
            finally:
                if not first.done():
                    first.cancel()
                    await asyncio.gather(first, return_exceptions=True)

    asyncio.run(scenario())

    assert len(workspace.handed_off) == 1
    assert workspace.seals == 0


@pytest.mark.parametrize("facade", ["prepare", "request"])
def test_failed_public_handoff_consumes_admission_and_seals_original_scope(
    facade: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup, workspace, result = editable_context()
    workspace.handoff_error = WorkspaceUnavailable("Fixture bytes changed.")

    async def scenario() -> None:
        async with public_handoff(
            setup, workspace, result, facade, monkeypatch
        ) as invoke:
            with pytest.raises(WorkspaceUnavailable, match="bytes changed"):
                await invoke()
            workspace.handoff_error = None
            with pytest.raises(WorkspaceUnavailable):
                await invoke()

    asyncio.run(scenario())

    assert workspace.handed_off == [result]
    assert workspace.seals == 1


@pytest.mark.parametrize("facade", ["prepare", "request"])
def test_public_handoff_forwards_finite_wait_policy_and_rejects_late_result(
    facade: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup, workspace, result = editable_context()
    bounded = BaseMaterializationPolicy(
        operation_timeout_seconds=0.05, total_timeout_seconds=1
    )

    async def scenario() -> None:
        workspace.handoff_release = asyncio.get_running_loop().create_future()
        try:
            async with public_handoff(
                setup, workspace, result, facade, monkeypatch, policy=bounded
            ) as invoke:
                with pytest.raises(WorkspaceUnavailable, match="wait budget"):
                    await invoke()
                workspace.handoff_release.set_result(
                    EditableWorkspace(
                        materialization=result, location=workspace.location
                    )
                )
                await asyncio.sleep(0)
                with pytest.raises(WorkspaceUnavailable):
                    await invoke()
        finally:
            if not workspace.handoff_release.done():
                workspace.handoff_release.cancel()

    asyncio.run(scenario())

    assert workspace.sealed
    assert len(workspace.handed_off) == 1


def test_request_without_live_supervisor_cannot_release_base_handles() -> None:
    setup, workspace, result = editable_context()
    client, _, endpoint = worker(setup, Clock())

    with pytest.raises(LeaseDeadlineRejected):
        asyncio.run(client.handoff_materialized_base(setup.delivery, workspace, result))

    assert workspace.handed_off == []
    assert endpoint.heartbeats == []


@pytest.mark.parametrize("facade", ["prepare", "request"])
def test_export_and_handoff_have_separate_public_one_shot_admissions(
    facade: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup, workspace, _ = editable_context()
    workspace.materialization = None
    client, journal, _ = worker(setup, Clock())
    preparation = PrepareWorkerClaim(journal, workspace_journal=journal)

    async def scenario() -> None:
        async with active_supervisor(client, monkeypatch) as supervisor:
            if facade == "prepare":
                materialization = await preparation.materialize_base(
                    setup.delivery,
                    supervisor,
                    workspace,
                    setup.reservation,
                    setup.source,
                )
                editable = await preparation.handoff_materialized_base(
                    setup.delivery, supervisor, workspace, materialization
                )
            else:
                materialization = await client.materialize_base(
                    setup.delivery, workspace, setup.reservation, setup.source
                )
                editable = await client.handoff_materialized_base(
                    setup.delivery, workspace, materialization
                )
            assert editable.materialization is materialization

    asyncio.run(scenario())

    assert setup.calls.count("source") == setup.calls.count("write") == 1
    assert len(workspace.handed_off) == 1
    assert workspace.seals == 0


@pytest.mark.parametrize("facade", ["prepare", "request"])
def test_cancelled_public_handoff_seals_before_late_completion_and_cannot_retry(
    facade: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup, workspace, result = editable_context()

    async def scenario() -> None:
        workspace.handoff_entered = asyncio.Event()
        workspace.handoff_release = asyncio.get_running_loop().create_future()
        async with public_handoff(
            setup, workspace, result, facade, monkeypatch
        ) as invoke:
            handing_off = asyncio.ensure_future(invoke())
            await workspace.handoff_entered.wait()
            handing_off.cancel()
            with pytest.raises(asyncio.CancelledError):
                await handing_off
            assert workspace.sealed
            assert not workspace.handoff_release.done()
            workspace.handoff_release.set_result(
                EditableWorkspace(materialization=result, location=workspace.location)
            )
            await asyncio.sleep(0)
            with pytest.raises(WorkspaceUnavailable):
                await invoke()

    asyncio.run(scenario())

    assert len(workspace.handed_off) == 1


def test_request_keeps_heartbeat_exchange_free_during_handoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup, workspace, result = editable_context()
    client, journal, endpoint = worker(setup, Clock())
    original_history = journal.state.workspace_completion

    async def scenario() -> None:
        workspace.handoff_entered = asyncio.Event()
        workspace.handoff_release = asyncio.get_running_loop().create_future()
        async with active_supervisor(client, monkeypatch) as retained:
            handing_off = asyncio.create_task(
                client.handoff_materialized_base(setup.delivery, workspace, result)
            )
            await workspace.handoff_entered.wait()
            try:
                renewal = await asyncio.wait_for(client.heartbeat(), timeout=1)
                assert renewal.result.renewed
                assert len(endpoint.heartbeats) == 2
                assert journal.state.last_heartbeat == renewal.intent
                assert client.heartbeat_supervisor() is retained
                workspace.handoff_release.set_result(
                    EditableWorkspace(
                        materialization=result, location=workspace.location
                    )
                )
                assert (await handing_off).materialization is result
            finally:
                if not handing_off.done():
                    handing_off.cancel()
                    await asyncio.gather(handing_off, return_exceptions=True)

    asyncio.run(scenario())

    assert journal.state.workspace_completion is original_history
    assert workspace.seals == 0


def test_expired_authority_during_handoff_cannot_accept_late_transition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup, workspace, result = editable_context()
    clock = Clock()
    client, _, _ = worker(setup, clock)

    async def scenario() -> None:
        workspace.handoff_entered = asyncio.Event()
        workspace.handoff_release = asyncio.get_running_loop().create_future()
        try:
            async with active_supervisor(client, monkeypatch):
                handing_off = asyncio.create_task(
                    client.handoff_materialized_base(setup.delivery, workspace, result)
                )
                await workspace.handoff_entered.wait()
                clock.value += 10000
                with pytest.raises(LeaseDeadlineRejected):
                    await asyncio.wait_for(handing_off, 1)
                assert workspace.sealed
                workspace.handoff_release.set_result(
                    EditableWorkspace(
                        materialization=result, location=workspace.location
                    )
                )
                await asyncio.sleep(0)
        finally:
            if not workspace.handoff_release.done():
                workspace.handoff_release.cancel()

    asyncio.run(scenario())

    assert len(workspace.handed_off) == 1
