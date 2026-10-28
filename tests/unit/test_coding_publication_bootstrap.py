import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import cast

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncEngine

from devrelay.api.dependencies import ClaimOperation, CodingResultOperations
from devrelay.application.contracts.assignments import ClaimResult
from devrelay.application.contracts.claims import WorkerClaimRequest
from devrelay.application.contracts.coding_result_receipts import (
    CodingResultAcceptance,
    CodingResultStatus,
)
from devrelay.application.contracts.coding_result_requests import (
    WorkerCodingResultRequest,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.publish_coding_result import PublishCodingResult
from devrelay.bootstrap import coding_publication as publication_bootstrap
from devrelay.bootstrap import control
from devrelay.bootstrap.control import create_app
from devrelay.config.control import ControlSettings
from tests.unit.test_coding_publication_configuration import TOKEN, enabled


async def claim(
    request: WorkerClaimRequest, authorization: WorkerAuthorization
) -> ClaimResult:
    raise AssertionError("Startup must not dispatch a job.")


async def accept(
    request: WorkerCodingResultRequest, authorization: WorkerAuthorization
) -> CodingResultAcceptance:
    raise AssertionError("Startup must not accept a coding result.")


async def recover(
    request: WorkerCodingResultRequest, authorization: WorkerAuthorization
) -> CodingResultStatus | None:
    raise AssertionError("Startup must not recover a coding result.")


@asynccontextmanager
async def claims() -> AsyncIterator[ClaimOperation]:
    yield claim


@asynccontextmanager
async def results() -> AsyncIterator[CodingResultOperations]:
    yield CodingResultOperations(accept=accept, recover=recover)


def test_disabled_gate_constructs_no_publisher_or_provider_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected(*args: object, **kwargs: object) -> None:
        raise AssertionError("Disabled publication must not construct resources.")

    monkeypatch.setattr(control, "open_control_coding_publisher", unexpected)
    app = create_app(ControlSettings())

    with TestClient(app) as client:
        response = client.get("/health/live")
        publisher = app.state.coding_result_publisher

    assert response.status_code == 200 and publisher is None


def test_real_publication_factory_is_lazy_and_uses_only_separate_write_credential(
    worker_configuration: ControlSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = enabled(worker_configuration)
    clients: list[httpx.AsyncClient] = []
    original = httpx.AsyncClient

    def client(**kwargs: object) -> httpx.AsyncClient:
        # This spy forwards the real factory's keyword arguments unchanged.
        connection = original(**kwargs)  # type: ignore[arg-type]
        clients.append(connection)
        return connection

    async def unexpected(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Opening a publisher must not call GitHub.")

    monkeypatch.setattr(httpx, "AsyncClient", client)

    async def scenario() -> None:
        async with publication_bootstrap.open_coding_publisher(
            settings,
            cast(AsyncEngine, object()),
            transport=httpx.MockTransport(unexpected),
        ) as publisher:
            assert isinstance(publisher, PublishCodingResult)
            assert len(clients) == 1
            assert clients[0].base_url == httpx.URL("https://api.github.com/")
            assert clients[0].headers["Authorization"] == f"Bearer {TOKEN}"
            assert clients[0].headers["X-GitHub-Api-Version"] == "2026-03-10"
            assert not clients[0].follow_redirects and not clients[0].is_closed

    asyncio.run(scenario())

    assert clients[0].is_closed


def test_publication_and_result_resources_share_one_control_database(
    worker_configuration: ControlSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = enabled(worker_configuration)
    engine = cast(AsyncEngine, object())
    publisher = cast(PublishCodingResult, object())
    events: list[str] = []

    @asynccontextmanager
    async def database(configuration: object) -> AsyncIterator[AsyncEngine]:
        events.append("database open")
        try:
            yield engine
        finally:
            events.append("database close")

    @asynccontextmanager
    async def result_resources(
        configuration: ControlSettings, received: AsyncEngine
    ) -> AsyncIterator[CodingResultOperations]:
        assert received is engine
        events.append("results open")
        try:
            yield CodingResultOperations(accept=accept, recover=recover)
        finally:
            events.append("results close")

    @asynccontextmanager
    async def publication_resources(
        configuration: ControlSettings, received: AsyncEngine
    ) -> AsyncIterator[PublishCodingResult]:
        assert received is engine
        events.append("publisher open")
        try:
            yield publisher
        finally:
            events.append("publisher close")

    monkeypatch.setattr(control, "open_database_engine", database)
    monkeypatch.setattr(
        control, "open_control_coding_result_operation", result_resources
    )
    monkeypatch.setattr(control, "open_control_coding_publisher", publication_resources)
    app = create_app(settings, claim_resources=claims)

    with TestClient(app) as client:
        response = client.get("/health/live")
        retained = app.state.coding_result_publisher

    assert response.status_code == 200 and retained is publisher
    assert app.state.coding_result_publisher is None
    assert events == [
        "database open",
        "results open",
        "publisher open",
        "publisher close",
        "results close",
        "database close",
    ]
    assert not any(
        "publication" in route.path for route in app.routes if hasattr(route, "path")
    )


def test_injected_publisher_requires_explicit_gate() -> None:
    @asynccontextmanager
    async def publication_resources() -> AsyncIterator[PublishCodingResult]:
        raise AssertionError("Disabled publisher must not open.")
        yield  # pragma: no cover

    with pytest.raises(ValueError, match="explicitly enabled publication"):
        create_app(ControlSettings(), publication_resources=publication_resources)


def test_linux_publication_clock_uses_suspend_inclusive_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    readings = iter((10.0, 10.0, 85.0))
    identifiers: list[int] = []

    def read(identifier: int) -> float:
        identifiers.append(identifier)
        return next(readings)

    monkeypatch.setattr("sys.platform", "linux")
    monkeypatch.setattr(
        publication_bootstrap,
        "time",
        SimpleNamespace(CLOCK_BOOTTIME=7, clock_gettime=read),
    )

    clock = publication_bootstrap._publication_clock()
    observations = (clock(), clock())

    assert observations == (10.0, 85.0)
    assert identifiers == [7, 7, 7]


@pytest.mark.parametrize("reading", [float("nan"), float("inf"), -1.0, 9.0])
def test_publication_clock_refuses_invalid_or_regressed_readings(
    monkeypatch: pytest.MonkeyPatch, reading: float
) -> None:
    readings = iter((10.0, reading))
    monkeypatch.setattr("sys.platform", "linux")
    monkeypatch.setattr(
        publication_bootstrap,
        "time",
        SimpleNamespace(CLOCK_BOOTTIME=7, clock_gettime=lambda _: next(readings)),
    )
    clock = publication_bootstrap._publication_clock()

    with pytest.raises(ValueError, match="monotonic clock"):
        clock()


@pytest.mark.parametrize("platform", ["linux", "darwin"])
def test_publication_clock_has_no_noninclusive_fallback(
    monkeypatch: pytest.MonkeyPatch, platform: str
) -> None:
    monkeypatch.setattr("sys.platform", platform)
    monkeypatch.setattr(
        publication_bootstrap, "time", SimpleNamespace(monotonic=lambda: 100.0)
    )

    with pytest.raises(ValueError, match="suspend-inclusive"):
        publication_bootstrap._publication_clock()
