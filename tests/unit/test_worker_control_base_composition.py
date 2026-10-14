import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from typing import cast
from uuid import UUID

import httpx
import pytest
from pydantic import AnyHttpUrl, SecretStr

from devrelay.application.contracts.assignment_delivery import AssignmentDelivery
from devrelay.application.contracts.base_source_delivery import (
    BaseSourceDelivery,
    encode_base_delivery,
)
from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    WorkerClaimRequest,
)
from devrelay.application.contracts.heartbeats import HeartbeatRequest
from devrelay.application.contracts.worker_recovery import WorkerRecoveryState
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.base_source import BaseSourcePort
from devrelay.application.ports.worker_journal import WorkerStateUnavailable
from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from devrelay.application.worker_claims import (
    BaseSourceBinding,
    PrepareWorkerClaim,
    RequestWorkerClaim,
)
from devrelay.bootstrap import worker as bootstrap
from devrelay.config.worker import WorkerSettings
from devrelay.infrastructure.control.base_sources import HttpControlBaseSource
from devrelay.infrastructure.control.gateway import HttpControlGateway
from devrelay.infrastructure.control.heartbeats import HeartbeatPayload
from tests.assignment_fixtures import NOW
from tests.unit.test_materialize_base import Context, context
from tests.unit.test_worker_base_materialization import (
    Journal,
    active_supervisor,
    worker,
)
from tests.unit.test_worker_execution_authority import Clock
from tests.unit.test_worker_heartbeat_exchange import Endpoint

TOKEN = "test_only_worker_token_0123456789abcdefghijklmnop"


def bound_worker(
    setup: Context, bind: BaseSourceBinding
) -> tuple[RequestWorkerClaim, Journal, Endpoint]:
    clock = Clock()
    _, journal, endpoint = worker(setup, clock)
    preparation = PrepareWorkerClaim(
        journal, heartbeat_journal=journal, workspace_journal=journal
    )
    return (
        RequestWorkerClaim(
            preparation,
            endpoint,
            endpoint.authorization,
            heartbeat_gateway=endpoint,
            monotonic_clock=clock,
            base_source_binding=bind,
        ),
        journal,
        endpoint,
    )


def test_deferred_source_binds_only_protected_original_input_after_live_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: source binding is configured but has not accessed any credentials.
    setup = context()
    seen: list[tuple[WorkerClaimRequest, WorkerAuthorization, AssignmentDelivery]] = []

    async def bind(
        claim: WorkerClaimRequest,
        scope: WorkerAuthorization,
        delivery: AssignmentDelivery,
    ) -> BaseSourcePort:
        assert "verify" in setup.calls
        seen.append((claim, scope, delivery))
        return setup.source

    client, journal, endpoint = bound_worker(setup, bind)

    async def scenario() -> None:
        assert seen == []
        async with active_supervisor(client, monkeypatch):
            before = journal.state
            # Act: omit the explicit source to use the trusted control binder.
            result = await client.materialize_base(
                setup.delivery, setup.workspace, setup.reservation
            )
            # Assert: one exact protected request and worker scope flow to the binder.
            assert result.source == setup.snapshot.request
            assert journal.state is before

    asyncio.run(scenario())
    assert len(seen) == 1
    assert seen[0][0] is journal.state.request
    assert seen[0][1] is endpoint.authorization
    assert seen[0][2] is setup.delivery
    assert setup.workspace.written == ["README.md"]


@pytest.mark.parametrize(
    "failure", ["reconstructed", "pending", "definition", "journal"]
)
def test_invalid_live_scope_or_protected_history_never_binds_source(
    failure: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange: preserve live heartbeat authority while breaking a materialization pin.
    setup = context()
    seen: list[object] = []

    async def bind(
        claim: WorkerClaimRequest,
        scope: WorkerAuthorization,
        delivery: AssignmentDelivery,
    ) -> BaseSourcePort:
        seen.append(claim)
        raise AssertionError("Rejected input must not bind a source.")

    client, journal, _ = bound_worker(setup, bind)

    async def scenario() -> None:
        async with active_supervisor(client, monkeypatch):
            reservation, delivery = setup.reservation, setup.delivery
            if failure == "reconstructed":
                reservation = replace(reservation)
            elif failure == "pending":
                journal.state = replace(journal.state, workspace_completion=None)
            elif failure == "definition":
                delivery = replace(
                    delivery,
                    definition=replace(delivery.definition, target_branch="other"),
                )
            else:

                async def unavailable() -> WorkerRecoveryState:
                    raise WorkerStateUnavailable("Simulated protected read failure.")

                monkeypatch.setattr(journal, "load_state", unavailable)
            # Act / Assert: rejected data supplies no permission to construct transport.
            with pytest.raises(
                (WorkspaceUnavailable, WorkerStateUnavailable, ClaimReplayConflict)
            ):
                await client.materialize_base(delivery, setup.workspace, reservation)

    asyncio.run(scenario())
    assert seen == [] and setup.source.request is None
    assert setup.workspace.sealed and setup.workspace.written == []


def test_second_protected_read_failure_prevents_deferred_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: fail the additional protected read only after native verification.
    setup = context()
    seen: list[object] = []

    async def bind(
        claim: WorkerClaimRequest,
        scope: WorkerAuthorization,
        delivery: AssignmentDelivery,
    ) -> BaseSourcePort:
        seen.append(claim)
        return setup.source

    client, journal, _ = bound_worker(setup, bind)

    async def unavailable() -> WorkerRecoveryState:
        raise WorkerStateUnavailable("Simulated protected read failure.")

    def verified() -> None:
        monkeypatch.setattr(journal, "load_state", unavailable)

    setup.workspace.after_verify = verified

    async def scenario() -> None:
        async with active_supervisor(client, monkeypatch):
            # Act / Assert: a once-valid journal read is insufficient for binding.
            with pytest.raises(WorkerStateUnavailable):
                await client.materialize_base(
                    setup.delivery, setup.workspace, setup.reservation
                )

    asyncio.run(scenario())
    assert "verify" in setup.calls and seen == []
    assert setup.workspace.sealed and setup.workspace.written == []


def test_cancelled_deferred_source_seals_original_scope_and_consumes_one_shot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: block a source response after binding the protected original request.
    setup = context()
    seen: list[object] = []

    async def bind(
        claim: WorkerClaimRequest,
        scope: WorkerAuthorization,
        delivery: AssignmentDelivery,
    ) -> BaseSourcePort:
        seen.append(claim)
        return setup.source

    client, journal, _ = bound_worker(setup, bind)

    async def scenario() -> None:
        setup.source.entered = asyncio.Event()
        setup.source.release = asyncio.get_running_loop().create_future()
        async with active_supervisor(client, monkeypatch):
            before = journal.state
            pending = asyncio.create_task(
                client.materialize_base(
                    setup.delivery, setup.workspace, setup.reservation
                )
            )
            await setup.source.entered.wait()
            # Act: cancel the unresolved source operation.
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
            # Assert: the scope is sealed and a second call cannot reuse this lineage.
            with pytest.raises(WorkspaceUnavailable, match="already"):
                await client.materialize_base(
                    setup.delivery, setup.workspace, setup.reservation
                )
            assert journal.state is before
        setup.source.release.cancel()

    asyncio.run(scenario())
    assert len(seen) == 1
    assert setup.workspace.sealed and setup.workspace.written == []


def test_explicit_source_remains_available_without_constructing_control_binder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: a caller supplies the already trusted explicit source port.
    setup = context()

    async def bind(
        claim: WorkerClaimRequest,
        scope: WorkerAuthorization,
        delivery: AssignmentDelivery,
    ) -> BaseSourcePort:
        raise AssertionError("Explicit source must bypass deferred binding.")

    client, _, _ = bound_worker(setup, bind)

    async def scenario() -> None:
        async with active_supervisor(client, monkeypatch):
            # Act / Assert: prior explicit-source composition remains functional.
            await client.materialize_base(
                setup.delivery, setup.workspace, setup.reservation, setup.source
            )

    asyncio.run(scenario())
    assert setup.workspace.written == ["README.md"]


def test_real_loopback_source_wait_does_not_occupy_heartbeat_connection(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    # Arrange: an actual temporary HTTP server holds source I/O; no provider calls.
    setup, clock = context(), Clock()
    _, journal, endpoint = worker(setup, clock)
    preparation = PrepareWorkerClaim(
        journal, heartbeat_journal=journal, workspace_journal=journal
    )
    clients: list[httpx.AsyncClient] = []
    source_clients: list[httpx.AsyncClient] = []
    events: list[str] = []
    errors: list[Exception] = []

    def gateway(client: httpx.AsyncClient) -> HttpControlGateway:
        clients.append(client)
        return HttpControlGateway(client, now=lambda: NOW)

    def source(
        client: httpx.AsyncClient,
        claim: WorkerClaimRequest,
        authorization: WorkerAuthorization,
        delivery: AssignmentDelivery,
    ) -> HttpControlBaseSource:
        source_clients.append(client)
        assert authorization == endpoint.authorization
        assert claim is journal.state.request
        return HttpControlBaseSource(
            client, claim, authorization, delivery, now=lambda: NOW
        )

    @asynccontextmanager
    async def open_journal(
        settings: WorkerSettings, *, worker_id: UUID, state_directory: Path
    ) -> AsyncIterator[PrepareWorkerClaim]:
        assert worker_id == endpoint.authorization.worker_id
        events.append("journal open")
        try:
            yield preparation
        finally:
            events.append("journal close")

    monkeypatch.setattr(bootstrap, "HttpControlGateway", gateway)
    monkeypatch.setattr(bootstrap, "HttpControlBaseSource", source)
    monkeypatch.setattr(bootstrap, "WindowsLeaseClock", lambda: clock)
    monkeypatch.setattr(bootstrap, "open_worker_journal", open_journal)

    async def scenario() -> None:
        entered, release = asyncio.Event(), asyncio.Event()
        connections: set[asyncio.Task[None]] = set()
        peers: dict[str, object] = {}

        async def handle(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            task = cast(asyncio.Task[None], asyncio.current_task())
            connections.add(task)
            try:
                head = await reader.readuntil(b"\r\n\r\n")
                lines = head.decode("ascii").split("\r\n")
                method, path, _ = lines[0].split(" ")
                headers = {
                    name.lower(): value.strip()
                    for name, value in (
                        line.split(":", 1) for line in lines[1:] if line
                    )
                }
                assert headers["authorization"] == f"Bearer {TOKEN}"
                length = int(headers.get("content-length", "0"))
                assert 0 <= length <= 1024
                body = await reader.readexactly(length)
                if path.endswith("/identity"):
                    assert method == "GET"
                    events.append("identity")
                    response = json.dumps(
                        {
                            "protocol_version": 1,
                            "worker_id": str(endpoint.authorization.worker_id),
                            "allowed_repository_ids": [
                                str(value)
                                for value in endpoint.authorization.allowed_repository_ids
                            ],
                            "credential_expires_at": endpoint.authorization.credential_expires_at.isoformat(),
                        }
                    ).encode()
                else:
                    assert method == "POST"
                    payload = json.loads(body)
                    assert journal.state.request is not None
                    assert payload["claim_request_id"] == str(
                        journal.state.request.claim_request_id
                    )
                    assert payload["proof"] == journal.state.request.proof.reveal()
                    reference = setup.delivery.outcome.assignment
                    assert reference is not None
                    if path.endswith("/base-source"):
                        peers["source"] = writer.get_extra_info("peername")
                        entered.set()
                        await release.wait()
                        response = encode_base_delivery(
                            BaseSourceDelivery(
                                reference=reference,
                                claim_request_id=journal.state.request.claim_request_id,
                                definition_digest=setup.delivery.definition.fingerprint(),
                                snapshot=setup.snapshot,
                            )
                        )
                    else:
                        assert path.endswith("/heartbeats")
                        peers["heartbeat"] = writer.get_extra_info("peername")
                        request = HeartbeatRequest(
                            claim=journal.state.request,
                            heartbeat_request_id=UUID(payload["heartbeat_request_id"]),
                            attempt_id=reference.attempt_id,
                            generation=payload["generation"],
                            expected_lease_revision=payload["expected_lease_revision"],
                            protocol_version=payload["protocol_version"],
                        )
                        result = await endpoint.heartbeat(
                            request, endpoint.authorization, reference=reference
                        )
                        response = (
                            HeartbeatPayload(protocol_version=1, result=result)
                            .model_dump_json()
                            .encode()
                        )
                writer.write(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: "
                    + str(len(response)).encode()
                    + b"\r\n\r\n"
                    + response
                )
                await writer.drain()
            except Exception as error:
                errors.append(error)
            finally:
                writer.close()
                await writer.wait_closed()
                connections.discard(task)

        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        assert server.sockets is not None
        port = server.sockets[0].getsockname()[1]
        settings = WorkerSettings(
            control_url=AnyHttpUrl(f"http://127.0.0.1:{port}/"),
            auth_token=SecretStr(TOKEN),
            workspace_root=tmp_path / "workspace",
        )
        try:
            async with (
                server,
                bootstrap.open_worker_claim_client(
                    settings, state_directory=tmp_path / "private"
                ) as client,
            ):
                assert events == ["identity", "journal open"]
                async with active_supervisor(client, monkeypatch):
                    pending = asyncio.create_task(
                        client.materialize_base(
                            setup.delivery, setup.workspace, setup.reservation
                        )
                    )
                    await asyncio.wait_for(entered.wait(), 2)
                    try:
                        # Act: renew authority while source's real HTTP connection waits.
                        renewal = await asyncio.wait_for(client.heartbeat(), 2)
                        # Assert: renewal completes before source, using another client.
                        assert renewal.result.renewed and not pending.done()
                        assert len(clients) == len(source_clients) == 1
                        assert clients[0] is not source_clients[0]
                        assert peers["source"] != peers["heartbeat"]
                        release.set()
                        result = await asyncio.wait_for(pending, 2)
                        assert result.source == setup.snapshot.request
                    finally:
                        release.set()
                        if not pending.done():
                            pending.cancel()
                        await asyncio.gather(pending, return_exceptions=True)
            assert clients[0].is_closed and source_clients[0].is_closed
            assert events[-1] == "journal close"
        finally:
            release.set()
            server.close()
            await server.wait_closed()
            if connections:
                await asyncio.gather(*tuple(connections), return_exceptions=True)

    asyncio.run(scenario())
    assert errors == []
    assert setup.workspace.written == ["README.md"]
