import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.types import Message

from devrelay.api.dependencies import ClaimOperation, SubmissionOperation
from devrelay.application.contracts.assignments import (
    ClaimAdmissionRejected,
    ClaimPersistenceError,
    ClaimResult,
    ClaimScopeRejected,
    NoWorkReason,
)
from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    InvalidLeaseProof,
    LeaseProof,
    WorkerClaimRequest,
)
from devrelay.application.contracts.eligibility import (
    ClaimEligibilityRejected,
    WorkerAuthorityExpired,
)
from devrelay.application.contracts.submissions import (
    SavedJobSubmission,
    SubmissionAuthorization,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.bootstrap import control
from devrelay.bootstrap.control import create_app
from devrelay.bootstrap.persistence import open_database_engine
from devrelay.bootstrap.security import worker_authenticator
from devrelay.config.control import ControlSettings
from devrelay.config.database import DatabaseSettings
from devrelay.config.task_import import TaskImportSettings
from devrelay.config.worker_claims import WorkerClaimsSettings

PATH = "/api/v1/worker/claims"
TOKEN = "test_only_worker_token_0123456789abcdefghijklmnop"
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
NOW = datetime(2026, 10, 10, tzinfo=UTC)


class Operation:
    def __init__(self) -> None:
        self.calls: list[tuple[WorkerClaimRequest, WorkerAuthorization]] = []
        self.error: Exception | asyncio.CancelledError | None = None
        self.started = asyncio.Event()
        self.release: asyncio.Event | None = None

    async def __call__(
        self,
        request: WorkerClaimRequest,
        authorization: WorkerAuthorization,
    ) -> ClaimResult:
        self.calls.append((request, authorization))
        self.started.set()
        if self.release is not None:
            await self.release.wait()
        if self.error is not None:
            raise self.error
        return ClaimResult(recorded_at=NOW, recovered=True, no_work=NoWorkReason.EMPTY)


@pytest.fixture
def configuration(worker_configuration: ControlSettings) -> ControlSettings:
    values = worker_configuration.model_dump()
    values["worker_claims"] = WorkerClaimsSettings(
        max_concurrent_requests=1,
        body_timeout_seconds=0.1,
    )
    return ControlSettings.model_validate(values)


def application(
    configuration: ControlSettings,
    operation: Operation,
    monkeypatch: pytest.MonkeyPatch,
) -> FastAPI:
    monkeypatch.setattr(
        control,
        "worker_authenticator",
        lambda values: worker_authenticator(values, now=lambda: NOW),
    )

    @asynccontextmanager
    async def resources() -> AsyncIterator[ClaimOperation]:
        yield operation

    return create_app(configuration, claim_resources=resources)


def payload() -> dict[str, object]:
    return {
        "claim_request_id": str(uuid4()),
        "proof": LeaseProof.generate().reveal(),
        "protocol_version": 1,
    }


def test_claim_route_requires_explicit_enablement(
    worker_configuration: ControlSettings,
) -> None:
    app = create_app(worker_configuration)

    with TestClient(app) as client:
        response = client.post(PATH, headers=HEADERS, json=payload())

    assert response.status_code == 404
    assert PATH not in app.openapi()["paths"]


def test_claim_uses_only_authenticated_worker_and_returns_no_secrets(
    configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    operation = Operation()
    app = application(configuration, operation, monkeypatch)
    body = payload()

    with TestClient(app) as client:
        response = client.post(PATH, headers=HEADERS, json=body)

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert response.json() == {
        "protocol_version": 1,
        "worker_id": str(configuration.workers[0].worker_id),
        "claim_request_id": body["claim_request_id"],
        "recorded_at": "2026-10-10T00:00:00Z",
        "assignment": None,
        "no_work": "empty",
        "recovered": True,
        "lease_authority_active": False,
    }
    assert len(operation.calls) == 1
    assert operation.calls[0][1].worker_id == configuration.workers[0].worker_id
    assert operation.calls[0][0].proof.reveal() == body["proof"]
    assert TOKEN not in response.text + caplog.text
    assert str(body["proof"]) not in response.text + caplog.text
    assert app.state.worker_claim_services is None


@pytest.mark.parametrize(
    "change", ["worker", "repository", "protocol", "bool", "proof", "missing"]
)
@pytest.mark.parametrize(
    "path",
    [PATH, "/api/v1/worker/attempts/11111111-2222-3333-4444-555555555555/assignment"],
)
def test_claim_input_is_closed_and_bounded(
    configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
    change: str,
    path: str,
) -> None:
    operation = Operation()
    app = application(configuration, operation, monkeypatch)
    body = payload()
    if change in ("worker", "repository"):
        body[f"{change}_id"] = str(uuid4())
    elif change == "protocol":
        body["protocol_version"] = 2
    elif change == "bool":
        body["protocol_version"] = True
    elif change == "proof":
        body["proof"] = "a" * 42 + "b"
    else:
        del body["claim_request_id"]

    with TestClient(app) as client:
        response = client.post(path, headers=HEADERS, json=body)

    assert response.status_code == 422
    assert operation.calls == []
    assert str(body["proof"]) not in response.text


@pytest.mark.parametrize(
    "headers", [{}, {"Authorization": "Bearer wrong"}, {"Cookie": f"token={TOKEN}"}]
)
@pytest.mark.parametrize(
    "path",
    [PATH, "/api/v1/worker/attempts/11111111-2222-3333-4444-555555555555/assignment"],
)
def test_authentication_precedes_body_consumption(
    configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
    headers: dict[str, str],
    path: str,
) -> None:
    operation = Operation()
    app = application(configuration, operation, monkeypatch)
    received = 0

    async def receive() -> Message:
        nonlocal received
        received += 1
        raise AssertionError("Unauthorized body must not be consumed.")

    async def scenario() -> list[Message]:
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
                    "path": path,
                    "raw_path": path.encode(),
                    "query_string": b"",
                    "root_path": "",
                    "headers": [
                        (key.lower().encode(), value.encode())
                        for key, value in headers.items()
                    ],
                    "server": ("control.example", 443),
                    "client": ("127.0.0.1", 1234),
                },
                receive,
                send,
            )
        return messages

    messages = asyncio.run(scenario())

    assert messages[0]["status"] == 401
    assert received == 0 and operation.calls == []


@pytest.mark.parametrize(
    "case,status", [("large", 413), ("media", 415), ("encoded", 415), ("json", 422)]
)
@pytest.mark.parametrize(
    "path",
    [PATH, "/api/v1/worker/attempts/11111111-2222-3333-4444-555555555555/assignment"],
)
def test_claim_body_failures_are_sanitized(
    configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
    status: int,
    path: str,
) -> None:
    operation = Operation()
    app = application(configuration, operation, monkeypatch)
    headers = {**HEADERS, "Content-Type": "application/json"}
    body = TOKEN.encode() if case == "json" else b"x" * 1025
    if case == "media":
        headers["Content-Type"] = "text/plain"
    if case == "encoded":
        headers["Content-Encoding"] = "gzip"

    with TestClient(app) as client:
        response = client.post(path, headers=headers, content=body)

    assert response.status_code == status and operation.calls == []
    assert TOKEN not in response.text


@pytest.mark.parametrize(
    "error,status,code",
    [
        (WorkerAuthorityExpired(TOKEN), 401, "unauthorized"),
        (ClaimEligibilityRejected(TOKEN), 409, "claim_conflict"),
        (ClaimReplayConflict(TOKEN), 409, "claim_conflict"),
        (InvalidLeaseProof(TOKEN), 422, "invalid_request"),
        (ClaimScopeRejected(TOKEN), 403, "claim_scope_rejected"),
        (ClaimAdmissionRejected(TOKEN), 429, "claim_admission_rejected"),
        (ClaimPersistenceError(TOKEN), 503, "storage_unavailable"),
        (ValueError(TOKEN), 500, "internal_error"),
    ],
)
def test_claim_failures_use_central_safe_translation(
    configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    error: Exception,
    status: int,
    code: str,
) -> None:
    operation = Operation()
    operation.error = error
    app = application(configuration, operation, monkeypatch)

    with TestClient(app) as client:
        response = client.post(PATH, headers=HEADERS, json=payload())

    assert response.status_code == status
    assert response.json()["detail"]["code"] == code
    assert TOKEN not in response.text + caplog.text
    assert all(record.exc_info is None for record in caplog.records)


def test_cancellation_releases_claim_admission(
    configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation = Operation()
    operation.release = asyncio.Event()
    app = application(configuration, operation, monkeypatch)

    async def scenario() -> tuple[int, int]:
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app), base_url="https://control.example"
            ) as client:
                active = asyncio.create_task(
                    client.post(PATH, headers=HEADERS, json=payload())
                )
                await operation.started.wait()
                busy = await client.post(PATH, headers=HEADERS, json=payload())
                active.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await active
                assert operation.release is not None
                operation.release.set()
                ready = await client.post(PATH, headers=HEADERS, json=payload())
                return busy.status_code, ready.status_code

    statuses = asyncio.run(scenario())

    assert statuses == (503, 200)


@pytest.mark.parametrize(
    "path,ready_status,call_count",
    [
        (PATH, 200, 1),
        (
            "/api/v1/worker/attempts/11111111-2222-3333-4444-555555555555/assignment",
            404,
            0,
        ),
    ],
)
def test_slow_body_times_out_and_admission_recovers(
    configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
    path: str,
    ready_status: int,
    call_count: int,
) -> None:
    operation = Operation()
    app = application(configuration, operation, monkeypatch)

    async def slow() -> AsyncIterator[bytes]:
        yield b"{"
        await asyncio.Event().wait()

    async def scenario() -> tuple[int, int]:
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app), base_url="https://control.example"
            ) as client:
                timeout = await client.post(
                    path,
                    headers={**HEADERS, "Content-Type": "application/json"},
                    content=slow(),
                )
                ready = await client.post(path, headers=HEADERS, json=payload())
                return timeout.status_code, ready.status_code

    statuses = asyncio.run(scenario())

    assert statuses == (408, ready_status) and len(operation.calls) == call_count


def test_default_claim_import_and_submission_resources_share_pool_without_io(
    configuration: ControlSettings,
    submission_configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    values = configuration.model_dump()
    values["task_import"] = TaskImportSettings(
        auth_token=SecretStr("test_only_import_token_0123456789abcdefghijklmnop")
    )
    values["job_submission"] = submission_configuration.job_submission
    original = open_database_engine
    events: list[str] = []

    @asynccontextmanager
    async def monitored(settings: DatabaseSettings) -> AsyncIterator[AsyncEngine]:
        events.append("open")
        async with original(settings) as engine:
            yield engine
        events.append("close")

    async def unexpected_read(*args: object, **kwargs: object) -> httpx.Response:
        raise AssertionError("Startup and unauthorized claims must not read providers.")

    monkeypatch.setattr(control, "open_database_engine", monitored)
    monkeypatch.setattr(httpx.AsyncClient, "send", unexpected_read)
    app = create_app(ControlSettings.model_validate(values))

    with TestClient(app) as client:
        health = client.get("/health/live")
        rejected = client.post(
            PATH, headers={"Authorization": "Bearer invalid"}, json=payload()
        )

    assert health.status_code == 200 and rejected.status_code == 401
    assert events == ["open", "close"]
    assert app.state.worker_claim_services is None
    assert app.state.job_submission_services is None
    assert app.state.task_import_services is None


def test_claim_startup_failure_closes_previous_resources_and_clears_services(
    configuration: ControlSettings,
    submission_configuration: ControlSettings,
) -> None:
    values = configuration.model_dump()
    values["job_submission"] = submission_configuration.job_submission
    events: list[str] = []

    @asynccontextmanager
    async def submission() -> AsyncIterator[SubmissionOperation]:
        events.append("open")
        try:

            async def unused(
                *,
                snapshot_id: UUID,
                repository_id: UUID,
                authorization: SubmissionAuthorization,
            ) -> SavedJobSubmission:
                raise AssertionError("Startup must not submit work.")

            yield unused
        finally:
            events.append("close")

    @asynccontextmanager
    async def failing_claims() -> AsyncIterator[ClaimOperation]:
        raise RuntimeError("Claim setup failed.")
        yield

    app = create_app(
        ControlSettings.model_validate(values),
        submission_resources=submission,
        claim_resources=failing_claims,
    )

    with pytest.raises(RuntimeError, match="Claim setup failed"):
        with TestClient(app):
            pytest.fail("Failed startup cannot serve requests.")

    assert events == ["open", "close"]
    assert app.state.worker_claim_services is None
    assert app.state.worker_authentication_services is None
    assert app.state.job_submission_services is None
