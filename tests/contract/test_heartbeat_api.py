import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.types import Message

from devrelay.api.dependencies import WorkerClaimOperations
from devrelay.application.contracts.assignment_delivery import AssignmentDelivery
from devrelay.application.contracts.assignments import (
    ClaimPersistenceError,
    ClaimResult,
    ClaimScopeRejected,
)
from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    LeaseProof,
    WorkerClaimRequest,
)
from devrelay.application.contracts.eligibility import WorkerAuthorityExpired
from devrelay.application.contracts.heartbeats import (
    MAXIMUM_LEASE_REVISION,
    HeartbeatAdmissionRejected,
    HeartbeatRejected,
    HeartbeatRequest,
    HeartbeatResult,
    LeaseAuthorityObservation,
)
from devrelay.application.contracts.worker_recovery import AssignmentReference
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.bootstrap import control
from devrelay.bootstrap.control import create_app
from devrelay.bootstrap.persistence import open_database_engine
from devrelay.bootstrap.security import worker_authenticator
from devrelay.config.control import ControlSettings
from devrelay.config.database import DatabaseSettings
from tests.assignment_fixtures import NOW

SECRET = "test-only-heartbeat-error-secret"


class Operation:
    def __init__(self, reference: AssignmentReference) -> None:
        self.reference = reference
        self.calls: list[tuple[HeartbeatRequest, WorkerAuthorization]] = []
        self.error: Exception | None = None
        self.started = asyncio.Event()
        self.release: asyncio.Event | None = None

    async def heartbeat(
        self, request: HeartbeatRequest, authorization: WorkerAuthorization
    ) -> HeartbeatResult:
        self.calls.append((request, authorization))
        self.started.set()
        if self.release is not None:
            await self.release.wait()
        if self.error is not None:
            raise self.error
        return HeartbeatResult(
            heartbeat_request_id=request.heartbeat_request_id,
            authority=LeaseAuthorityObservation(
                reference=self.reference,
                lease_revision=2,
                observed_at=NOW + timedelta(seconds=10),
                updated_at=NOW + timedelta(seconds=10),
                current_expires_at=NOW + timedelta(seconds=130),
            ),
            renewed=True,
            stop_required=False,
        )

    async def claim(
        self, request: WorkerClaimRequest, authorization: WorkerAuthorization
    ) -> ClaimResult:
        raise AssertionError("Heartbeats must not claim new work.")

    async def read(
        self,
        attempt_id: UUID,
        request: WorkerClaimRequest,
        authorization: WorkerAuthorization,
    ) -> AssignmentDelivery:
        raise AssertionError("Heartbeats must not read task payloads.")


@pytest.fixture
def configuration(worker_configuration: ControlSettings) -> ControlSettings:
    values = worker_configuration.model_dump()
    values["worker_claims"] = {
        "max_concurrent_requests": 1,
        "body_timeout_seconds": 0.1,
    }
    return ControlSettings.model_validate(values)


@pytest.fixture
def operation(configuration: ControlSettings) -> Operation:
    return Operation(
        AssignmentReference(
            worker_id=configuration.workers[0].worker_id,
            repository_id=configuration.repositories[0].repository_id,
            job_id=uuid4(),
            attempt_id=uuid4(),
            generation=1,
            issued_at=NOW,
            expires_at=NOW + timedelta(seconds=120),
        )
    )


def application(
    configuration: ControlSettings,
    operation: Operation,
    monkeypatch: pytest.MonkeyPatch,
    *,
    enabled_operation: bool = True,
) -> FastAPI:
    monkeypatch.setattr(
        control,
        "worker_authenticator",
        lambda registrations: worker_authenticator(registrations, now=lambda: NOW),
    )

    @asynccontextmanager
    async def resources() -> AsyncIterator[WorkerClaimOperations]:
        yield WorkerClaimOperations(
            claim=operation.claim,
            read_assignment=operation.read,
            heartbeat=operation.heartbeat if enabled_operation else None,
        )

    return create_app(configuration, claim_resources=resources)


def path(operation: Operation) -> str:
    return f"/api/v1/worker/attempts/{operation.reference.attempt_id}/heartbeats"


def headers(configuration: ControlSettings) -> dict[str, str]:
    return {
        "Authorization": "Bearer "
        + configuration.workers[0].auth_token.get_secret_value()
    }


def payload() -> dict[str, object]:
    return {
        "protocol_version": 1,
        "claim_request_id": str(uuid4()),
        "proof": LeaseProof.generate().reveal(),
        "heartbeat_request_id": str(uuid4()),
        "generation": 1,
        "expected_lease_revision": 1,
    }


def test_heartbeat_default_closed_and_only_worker_identity_is_insufficient(
    worker_configuration: ControlSettings,
    operation: Operation,
) -> None:
    app = create_app(worker_configuration)
    with TestClient(app) as client:
        response = client.post(path(operation), json=payload())
    assert response.status_code == 404
    assert not any("heartbeats" in key for key in app.openapi()["paths"])


def test_heartbeat_path_identity_scoped_authentication_and_secret_free_response(
    configuration: ControlSettings,
    operation: Operation,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = application(configuration, operation, monkeypatch)
    body = payload()
    with TestClient(app) as client:
        response = client.post(
            path(operation), headers=headers(configuration), json=body
        )
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    result = response.json()["result"]
    assert response.json()["protocol_version"] == 1
    assert result["heartbeat_request_id"] == body["heartbeat_request_id"]
    assert result["authority"]["reference"]["attempt_id"] == str(
        operation.reference.attempt_id
    )
    assert result["authority"]["lease_revision"] == 2
    assert result["renewed"] is True and result["stop_required"] is False
    assert len(operation.calls) == 1
    request, authorization = operation.calls[0]
    assert request.attempt_id == operation.reference.attempt_id
    assert str(request.claim.claim_request_id) == body["claim_request_id"]
    assert request.claim.proof.reveal() == body["proof"]
    assert request.expected_lease_revision == 1
    assert authorization.worker_id == configuration.workers[0].worker_id
    assert str(body["proof"]) not in response.text
    assert app.state.worker_claim_services is None


@pytest.mark.parametrize("invalid_auth", [None, "wrong", "cookie"])
def test_heartbeat_authentication_precedes_body_read(
    configuration: ControlSettings,
    operation: Operation,
    monkeypatch: pytest.MonkeyPatch,
    invalid_auth: str | None,
) -> None:
    app = application(configuration, operation, monkeypatch)
    received = 0

    async def receive() -> Message:
        nonlocal received
        received += 1
        raise AssertionError("Unauthorized body must not be consumed.")

    async def run() -> list[Message]:
        messages: list[Message] = []

        async def send(message: Message) -> None:
            messages.append(message)

        async with app.router.lifespan_context(app):
            await app(
                {
                    "type": "http",
                    "asgi": {"version": "3.0"},
                    "http_version": "1.1",
                    "method": "POST",
                    "scheme": "https",
                    "path": path(operation),
                    "raw_path": path(operation).encode(),
                    "query_string": b"",
                    "root_path": "",
                    "headers": []
                    if invalid_auth is None
                    else [
                        (b"cookie", b"token=wrong")
                        if invalid_auth == "cookie"
                        else (b"authorization", b"Bearer wrong")
                    ],
                    "server": ("control.example", 443),
                    "client": ("127.0.0.1", 1234),
                },
                receive,
                send,
            )
        return messages

    messages = asyncio.run(run())
    assert messages[0]["status"] == 401
    assert received == 0 and operation.calls == []


@pytest.mark.parametrize(
    "field,value",
    [
        ("protocol_version", True),
        ("protocol_version", 2),
        ("protocol_version", "1"),
        ("generation", True),
        ("generation", 2),
        ("generation", 0),
        ("expected_lease_revision", True),
        ("expected_lease_revision", 0),
        ("expected_lease_revision", MAXIMUM_LEASE_REVISION + 1),
        ("expected_lease_revision", 1.0),
        ("expected_lease_revision", "1"),
        ("heartbeat_request_id", "bad"),
        ("claim_request_id", 1),
        ("proof", "bad"),
        ("proof", "!" * 43),
        ("worker_id", str(uuid4())),
        ("attempt_id", str(uuid4())),
        ("launch", True),
    ],
)
def test_heartbeat_strict_bounded_inputs(
    configuration: ControlSettings,
    operation: Operation,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: object,
) -> None:
    app = application(configuration, operation, monkeypatch)
    body = {**payload(), field: value}
    with TestClient(app) as client:
        response = client.post(
            path(operation), headers=headers(configuration), json=body
        )
    assert response.status_code == 422
    assert operation.calls == []
    assert str(body["proof"]) not in response.text


@pytest.mark.parametrize(
    "case,status",
    [
        ("large", 413),
        ("media", 415),
        ("encoded", 415),
        ("invalid", 422),
        ("duplicate", 422),
        ("path", 422),
    ],
)
def test_heartbeat_bounded_body_and_path_failures(
    configuration: ControlSettings,
    operation: Operation,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
    status: int,
) -> None:
    app = application(configuration, operation, monkeypatch)
    body = json.dumps(payload()).encode()
    request_headers = {**headers(configuration), "Content-Type": "application/json"}
    if case == "large":
        body += b" " * 1025
    elif case == "media":
        request_headers["Content-Type"] = "text/plain"
    elif case == "encoded":
        request_headers["Content-Encoding"] = "gzip"
    elif case == "invalid":
        body = SECRET.encode()
    elif case == "duplicate":
        body = b'{"protocol_version":1,' + body[1:]
    request_path = (
        path(operation) if case != "path" else "/api/v1/worker/attempts/bad/heartbeats"
    )
    with TestClient(app) as client:
        response = client.post(request_path, headers=request_headers, content=body)
    assert response.status_code == status
    assert operation.calls == []
    assert SECRET not in response.text


@pytest.mark.parametrize(
    "error,status,code",
    [
        (HeartbeatRejected(SECRET), 409, "heartbeat_rejected"),
        (HeartbeatAdmissionRejected(SECRET), 429, "heartbeat_admission_rejected"),
        (WorkerAuthorityExpired(SECRET), 401, "unauthorized"),
        (ClaimScopeRejected(SECRET), 403, "claim_scope_rejected"),
        (ClaimReplayConflict(SECRET), 409, "claim_conflict"),
        (ClaimPersistenceError(SECRET), 503, "storage_unavailable"),
        (ValueError(SECRET), 500, "internal_error"),
    ],
)
def test_heartbeat_errors_are_classified_and_sanitized(
    configuration: ControlSettings,
    operation: Operation,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    error: Exception,
    status: int,
    code: str,
) -> None:
    operation.error = error
    app = application(configuration, operation, monkeypatch)
    with TestClient(app) as client:
        response = client.post(
            path(operation), headers=headers(configuration), json=payload()
        )
    assert response.status_code == status
    assert response.json()["detail"]["code"] == code
    assert response.headers["cache-control"] == "no-store"
    assert SECRET not in response.text + caplog.text
    assert all(record.exc_info is None for record in caplog.records)


def test_heartbeat_missing_injected_operation_fails_closed(
    configuration: ControlSettings,
    operation: Operation,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = application(configuration, operation, monkeypatch, enabled_operation=False)
    with TestClient(app) as client:
        response = client.post(
            path(operation), headers=headers(configuration), json=payload()
        )
    assert response.status_code == 404
    assert operation.calls == []


def test_heartbeat_shares_admission_and_cancellation_releases_it(
    configuration: ControlSettings,
    operation: Operation,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation.release = asyncio.Event()
    app = application(configuration, operation, monkeypatch)

    async def run() -> None:
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app),
                base_url="https://control.example",
                headers=headers(configuration),
            ) as client:
                pending = asyncio.create_task(
                    client.post(path(operation), json=payload())
                )
                await operation.started.wait()
                busy = await client.post("/api/v1/worker/claims", json={})
                assert busy.status_code == 503 and busy.headers["retry-after"] == "1"
                pending.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await pending
                assert operation.release is not None
                operation.release.set()
                assert (
                    await client.post(path(operation), json=payload())
                ).status_code == 200

    asyncio.run(run())
    assert app.state.worker_claim_services is None


def test_heartbeat_body_timeout_releases_admission(
    configuration: ControlSettings,
    operation: Operation,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = application(configuration, operation, monkeypatch)

    async def slow() -> AsyncIterator[bytes]:
        yield b"{"
        await asyncio.Event().wait()

    async def run() -> None:
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app),
                base_url="https://control.example",
                headers={**headers(configuration), "Content-Type": "application/json"},
            ) as client:
                timeout = await client.post(path(operation), content=slow())
                assert timeout.status_code == 408
                assert (
                    await client.post(path(operation), json=payload())
                ).status_code == 200

    asyncio.run(run())
    assert len(operation.calls) == 1


def test_default_heartbeat_composition_shares_pool_and_has_no_startup_io(
    configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = open_database_engine
    events: list[str] = []

    @asynccontextmanager
    async def monitored(settings: DatabaseSettings) -> AsyncIterator[AsyncEngine]:
        events.append("open")
        async with original(settings) as engine:
            yield engine
        events.append("close")

    async def unexpected_read(*args: object, **kwargs: object) -> httpx.Response:
        raise AssertionError(
            "Startup and unauthorized heartbeat must not read providers."
        )

    monkeypatch.setattr(control, "open_database_engine", monitored)
    monkeypatch.setattr(httpx.AsyncClient, "send", unexpected_read)
    app = create_app(configuration)
    with TestClient(app) as client:
        assert app.state.worker_claim_services.heartbeat is not None
        response = client.post(
            f"/api/v1/worker/attempts/{uuid4()}/heartbeats", json=payload()
        )
        assert response.status_code == 401
    assert events == ["open", "close"]
    assert app.state.worker_claim_services is None
