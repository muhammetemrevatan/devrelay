from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import cast
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncEngine

from devrelay.api.dependencies import BaseSourceOperations, ClaimOperation
from devrelay.application.contracts.assignments import ClaimResult, NoWorkReason
from devrelay.application.contracts.base_source_delivery import BaseSourceDelivery
from devrelay.application.contracts.claims import WorkerClaimRequest
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.bootstrap import control
from devrelay.bootstrap.control import create_app
from devrelay.config.control import ControlSettings
from tests.assignment_fixtures import NOW


async def claim(
    request: WorkerClaimRequest, authorization: WorkerAuthorization
) -> ClaimResult:
    return ClaimResult(recorded_at=NOW, recovered=True, no_work=NoWorkReason.EMPTY)


async def read(
    attempt_id: UUID, request: WorkerClaimRequest, authorization: WorkerAuthorization
) -> BaseSourceDelivery:
    raise AssertionError("Startup must not fetch provider data.")


async def verify(
    attempt_id: UUID,
    request: WorkerClaimRequest,
    authorization: WorkerAuthorization,
    expected: BaseSourceDelivery,
) -> None:
    raise AssertionError("Startup must not disclose data.")


def enabled(worker_configuration: ControlSettings) -> ControlSettings:
    values = worker_configuration.model_dump()
    values["worker_claims"] = {}
    values["worker_base_sources"] = {}
    return ControlSettings.model_validate(values)


def test_real_source_and_claim_factories_share_one_engine(
    worker_configuration: ControlSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = enabled(worker_configuration)
    engine = cast(AsyncEngine, object())
    events: list[str] = []

    @asynccontextmanager
    async def database(settings: object) -> AsyncIterator[AsyncEngine]:
        events.append("database open")
        try:
            yield engine
        finally:
            events.append("database close")

    @asynccontextmanager
    async def claims(
        settings: ControlSettings, received: AsyncEngine
    ) -> AsyncIterator[ClaimOperation]:
        assert received is engine
        events.append("claims open")
        try:
            yield claim
        finally:
            events.append("claims close")

    @asynccontextmanager
    async def sources(
        settings: ControlSettings, received: AsyncEngine
    ) -> AsyncIterator[BaseSourceOperations]:
        assert received is engine
        events.append("sources open")
        try:
            yield BaseSourceOperations(read_base=read, verify_base=verify)
        finally:
            events.append("sources close")

    monkeypatch.setattr(control, "open_database_engine", database)
    monkeypatch.setattr(control, "open_control_claim_operation", claims)
    monkeypatch.setattr(control, "open_control_base_source_operation", sources)
    app = create_app(settings)

    with TestClient(app):
        assert app.state.worker_base_source_services is not None
        assert app.state.worker_claim_services is not None

    assert events == [
        "database open",
        "claims open",
        "sources open",
        "sources close",
        "claims close",
        "database close",
    ]
    assert app.state.worker_base_source_services is None
    assert app.state.worker_claim_services is None


def test_real_source_opens_engine_even_with_injected_claims(
    worker_configuration: ControlSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = enabled(worker_configuration)
    engine = cast(AsyncEngine, object())
    events: list[str] = []

    @asynccontextmanager
    async def database(settings: object) -> AsyncIterator[AsyncEngine]:
        events.append("database")
        yield engine

    @asynccontextmanager
    async def claims() -> AsyncIterator[ClaimOperation]:
        yield claim

    @asynccontextmanager
    async def sources(
        settings: ControlSettings, received: AsyncEngine
    ) -> AsyncIterator[BaseSourceOperations]:
        assert received is engine
        events.append("sources")
        yield BaseSourceOperations(read_base=read, verify_base=verify)

    monkeypatch.setattr(control, "open_database_engine", database)
    monkeypatch.setattr(control, "open_control_base_source_operation", sources)

    with TestClient(create_app(settings, claim_resources=claims)):
        pass

    assert events == ["database", "sources"]


def test_source_gate_absent_opens_no_source_resources(
    worker_configuration: ControlSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    values = worker_configuration.model_dump()
    values["worker_claims"] = {}
    settings = ControlSettings.model_validate(values)

    @asynccontextmanager
    async def claims() -> AsyncIterator[ClaimOperation]:
        yield claim

    def unexpected(*args: object, **kwargs: object) -> None:
        raise AssertionError("Disabled resources must not open.")

    monkeypatch.setattr(control, "open_database_engine", unexpected)
    monkeypatch.setattr(control, "open_control_base_source_operation", unexpected)

    with TestClient(create_app(settings, claim_resources=claims)) as client:
        assert client.get("/health/live").status_code == 200


def test_injected_source_factory_requires_gate(
    worker_configuration: ControlSettings,
) -> None:
    @asynccontextmanager
    async def sources() -> AsyncIterator[BaseSourceOperations]:
        yield BaseSourceOperations(read_base=read, verify_base=verify)

    with pytest.raises(ValueError, match="explicitly enabled source"):
        create_app(worker_configuration, base_source_resources=sources)


def test_failed_source_startup_unwinds_shared_resources(
    worker_configuration: ControlSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = enabled(worker_configuration)
    events: list[str] = []

    @asynccontextmanager
    async def claims() -> AsyncIterator[ClaimOperation]:
        events.append("claims open")
        try:
            yield claim
        finally:
            events.append("claims close")

    @asynccontextmanager
    async def sources() -> AsyncIterator[BaseSourceOperations]:
        raise RuntimeError("fixture startup failure")
        yield BaseSourceOperations(read_base=read, verify_base=verify)

    app = create_app(settings, claim_resources=claims, base_source_resources=sources)

    with pytest.raises(RuntimeError, match="fixture startup failure"), TestClient(app):
        pass

    assert events == ["claims open", "claims close"]
    assert app.state.worker_base_source_services is None
    assert app.state.worker_claim_services is None
