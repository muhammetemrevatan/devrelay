"""Explicit result composition uses synthetic journals and isolated HTTP channels."""

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from pydantic import AnyHttpUrl, SecretStr

from devrelay.application.contracts.coding_result_receipts import (
    CodingResultUnavailable,
)
from devrelay.application.contracts.coding_result_response_wire import (
    encode_coding_result_status,
)
from devrelay.application.contracts.heartbeats import (
    HeartbeatResult,
    LeaseAuthorityObservation,
)
from devrelay.application.ports.worker_journal import WorkerStateUnavailable
from devrelay.application.worker_claims import PrepareWorkerClaim, RequestWorkerClaim
from devrelay.application.worker_coding_results import WorkerCodingResultExchange
from devrelay.bootstrap import worker as bootstrap
from devrelay.config.worker import WorkerSettings
from devrelay.infrastructure.control.coding_results import (
    open_http_control_coding_results,
)
from devrelay.infrastructure.control.gateway import HttpControlGateway
from devrelay.infrastructure.control.heartbeats import HeartbeatPayload
from tests.unit.test_worker_coding_results import Setup, setup
from tests.unit.test_worker_control_base_composition import TOKEN
from tests.unit.test_worker_heartbeat_exchange import Local


class Channel(httpx.AsyncBaseTransport):
    def __init__(
        self,
        name: str,
        events: list[str],
        handler: Callable[[httpx.Request], Awaitable[httpx.Response]],
    ) -> None:
        self.name, self.events, self.handler = name, events, handler
        self.requests: list[httpx.Request] = []
        self.closed = False

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        assert not self.closed
        self.requests.append(request)
        return await self.handler(request)

    async def aclose(self) -> None:
        if not self.closed:
            self.events.append(self.name + " close")
            self.closed = True


class Composition:
    def __init__(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, *, existing: bool = False
    ) -> None:
        self.value: Setup = setup(existing=existing)
        self.events: list[str] = []
        self.local = Local(self.value.journal.state.original, [])
        self.preparation = PrepareWorkerClaim(
            self.local,
            heartbeat_journal=self.local,
            result_journal=self.value.journal,
        )
        self.primary_clients: list[httpx.AsyncClient] = []
        self.result_opens = 0
        self.entered, self.release = asyncio.Event(), asyncio.Event()
        self.block = False
        self.tmp_path = tmp_path
        reference = self.value.delivery.outcome.assignment
        assert reference is not None
        self.now = reference.issued_at + timedelta(seconds=3)
        self.primary = Channel("primary", self.events, self.control)
        self.source = Channel("source", self.events, self.unused)
        self.results = Channel("results", self.events, self.result)

        def gateway(connection: httpx.AsyncClient) -> HttpControlGateway:
            self.primary_clients.append(connection)
            return HttpControlGateway(connection, now=lambda: self.now)

        @asynccontextmanager
        async def journal(
            settings: WorkerSettings, *, worker_id: UUID, state_directory: Path
        ) -> AsyncIterator[PrepareWorkerClaim]:
            assert worker_id == self.value.authorization.worker_id
            assert state_directory == tmp_path / "private"
            self.events.append("journal open")
            try:
                yield self.preparation
            finally:
                self.events.append("journal close")

        @asynccontextmanager
        async def results(
            control_url: str,
            worker_token: str,
            *,
            transport: httpx.AsyncBaseTransport | None = None,
        ) -> AsyncIterator[object]:
            self.result_opens += 1
            assert transport is self.results
            self.events.append("results open")
            async with open_http_control_coding_results(
                control_url, worker_token, now=lambda: self.now, transport=transport
            ) as endpoint:
                yield endpoint

        monkeypatch.setattr(bootstrap, "HttpControlGateway", gateway)
        monkeypatch.setattr(bootstrap, "WindowsLeaseClock", lambda: lambda: 100.0)
        monkeypatch.setattr(bootstrap, "open_worker_journal", journal)
        monkeypatch.setattr(bootstrap, "open_http_control_coding_results", results)
        monkeypatch.delenv("DEVRELAY_WORKER_CODING_RESULTS_ENABLED", raising=False)

    def settings(self, *, enabled: bool | None = None) -> WorkerSettings:
        options: dict[str, object] = {}
        if enabled is not None:
            options["coding_results_enabled"] = enabled
        return WorkerSettings(
            control_url=AnyHttpUrl("https://control.example/"),
            auth_token=SecretStr(TOKEN),
            workspace_root=self.tmp_path / "workspaces",
            **options,  # type: ignore[arg-type]
        )

    @asynccontextmanager
    async def open(
        self, *, enabled: bool | None = None
    ) -> AsyncIterator[RequestWorkerClaim]:
        async with bootstrap.open_worker_claim_client(
            self.settings(enabled=enabled),
            state_directory=self.tmp_path / "private",
            transport=self.primary,
            source_transport=self.source,
            result_transport=self.results,
        ) as worker:
            yield worker

    async def control(self, request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == "Bearer " + TOKEN
        if request.url.path.endswith("/identity"):
            self.events.append("identity")
            return httpx.Response(
                200,
                json={
                    "protocol_version": 1,
                    "worker_id": str(self.value.authorization.worker_id),
                    "allowed_repository_ids": [
                        str(value)
                        for value in self.value.authorization.allowed_repository_ids
                    ],
                    "credential_expires_at": self.value.authorization.credential_expires_at.isoformat(),
                },
            )
        assert request.url.path.endswith("/heartbeats")
        payload = json.loads(request.content)
        reference = self.value.delivery.outcome.assignment
        assert reference is not None
        self.events.append("heartbeat")
        return httpx.Response(
            200,
            content=HeartbeatPayload(
                protocol_version=1,
                result=HeartbeatResult(
                    heartbeat_request_id=UUID(payload["heartbeat_request_id"]),
                    authority=LeaseAuthorityObservation(
                        reference=reference,
                        lease_revision=2,
                        updated_at=reference.issued_at + timedelta(seconds=2),
                        observed_at=self.now,
                        current_expires_at=reference.expires_at + timedelta(seconds=2),
                    ),
                    renewed=True,
                    stop_required=False,
                ),
            )
            .model_dump_json()
            .encode(),
            headers={"content-type": "application/json"},
        )

    async def unused(self, request: httpx.Request) -> httpx.Response:
        raise AssertionError("Composition must not fetch source on startup.")

    async def result(self, request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/results/recovery")
        self.events.append("result entered")
        self.entered.set()
        if self.block:
            await self.release.wait()
        self.events.append("result returned")
        return httpx.Response(
            200,
            content=encode_coding_result_status(self.value.status(terminal=True)),
            headers={"content-type": "application/json"},
        )


def test_default_disabled_opens_no_result_channel_and_performs_only_identity(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    composed = Composition(monkeypatch, tmp_path)
    assert not composed.settings().coding_results_enabled

    async def scenario() -> None:
        async with composed.open() as worker:
            assert composed.events == ["identity", "journal open"]
            with pytest.raises(WorkerStateUnavailable, match="not enabled"):
                worker.coding_result_exchange()
            with pytest.raises(WorkerStateUnavailable, match="not enabled"):
                await worker.recover_coding_result()
            await worker.close_coding_results()

    asyncio.run(scenario())
    assert composed.result_opens == 0 and composed.results.requests == []
    assert not composed.results.closed
    assert len(composed.primary.requests) == 1 and composed.source.requests == []
    assert composed.value.events == []


def test_enabled_channel_is_lazy_and_facade_retains_one_exchange(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    composed = Composition(monkeypatch, tmp_path)

    async def scenario() -> None:
        async with composed.open(enabled=True) as worker:
            assert composed.result_opens == 1
            assert composed.events == ["identity", "journal open", "results open"]
            first = worker.coding_result_exchange()
            assert worker.coding_result_exchange() is first
            assert composed.results.requests == [] and composed.value.events == []
            assert await worker.recover_coding_result() is None
            assert worker.coding_result_exchange() is first
            assert composed.results.requests == []
        with pytest.raises(CodingResultUnavailable):
            await first.recover()

    asyncio.run(scenario())
    assert composed.value.events == ["load"]
    assert composed.results.closed and composed.primary_clients[0].is_closed


def test_historical_result_recovery_does_not_construct_heartbeat_supervisor(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    composed = Composition(monkeypatch, tmp_path, existing=True)

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("Historical recovery must not create live supervision.")

    monkeypatch.setattr(RequestWorkerClaim, "heartbeat_supervisor", forbidden)

    async def scenario() -> None:
        async with composed.open(enabled=True) as worker:
            recovered = await worker.recover_coding_result()
            assert recovered is not None and recovered.terminal is not None
            assert composed.value.journal.state.terminal_status == recovered
            assert len(composed.primary.requests) == len(composed.results.requests) == 1
            assert composed.local.state is composed.value.journal.state.original

    asyncio.run(scenario())
    assert "heartbeat" not in composed.events


def test_context_close_settles_cancelled_retained_exchange_before_channels_and_journal(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    composed = Composition(monkeypatch, tmp_path, existing=True)
    original_close = WorkerCodingResultExchange.aclose

    async def close(exchange: WorkerCodingResultExchange) -> None:
        composed.events.append("exchange closing")
        await original_close(exchange)
        composed.events.append("exchange closed")

    monkeypatch.setattr(WorkerCodingResultExchange, "aclose", close)

    async def scenario() -> None:
        composed.block = True
        context = composed.open(enabled=True)
        worker = await context.__aenter__()
        pending = asyncio.create_task(worker.recover_coding_result())
        closing: asyncio.Task[bool | None] | None = None
        try:
            await asyncio.wait_for(composed.entered.wait(), 2)
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
            closing = asyncio.create_task(context.__aexit__(None, None, None))
            await asyncio.sleep(0)
            assert "exchange closing" in composed.events and not closing.done()
            assert not composed.results.closed and not composed.source.closed
            assert "journal close" not in composed.events
            composed.release.set()
            await asyncio.wait_for(closing, 2)
            assert composed.value.journal.state.terminal_status is not None
        finally:
            composed.release.set()
            await asyncio.gather(pending, return_exceptions=True)
            if closing is None:
                await context.__aexit__(None, None, None)
            else:
                await closing

    asyncio.run(scenario())
    order = [
        "result returned",
        "exchange closed",
        "results close",
        "source close",
        "journal close",
        "primary close",
    ]
    assert [composed.events.index(event) for event in order] == sorted(
        composed.events.index(event) for event in order
    )


def test_pending_result_http_does_not_block_worker_heartbeat_exchange(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    composed = Composition(monkeypatch, tmp_path, existing=True)

    async def scenario() -> None:
        composed.block = True
        async with composed.open(enabled=True) as worker:
            pending = asyncio.create_task(worker.recover_coding_result())
            try:
                await asyncio.wait_for(composed.entered.wait(), 2)
                renewal = await asyncio.wait_for(worker.heartbeat(), 2)
                assert renewal.result.renewed and not pending.done()
                assert len(composed.results.requests) == 1
                assert len(composed.primary.requests) == 2
                assert composed.source.requests == []
                composed.release.set()
                assert await asyncio.wait_for(pending, 2) is not None
            finally:
                composed.release.set()
                await asyncio.gather(pending, return_exceptions=True)

    asyncio.run(scenario())
    assert composed.events.index("heartbeat") < composed.events.index("result returned")
