import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.types import Message

from devrelay.api.dependencies import ImportOperation, SubmissionOperation
from devrelay.application.contracts.submissions import (
    PreparedJobSubmission,
    RepositoryHead,
    SavedJobSubmission,
    StoredJobSubmission,
    SubmissionAuthorization,
)
from devrelay.application.contracts.tasks import StoredTaskSnapshot
from devrelay.application.jobs import (
    JobSubmissionConflict,
    JobSubmissionRejected,
    TaskNotReady,
    TaskRequirementsChanged,
)
from devrelay.application.ports.job_persistence import JobPersistenceError
from devrelay.application.ports.repository_source import (
    InvalidRepositorySourceData,
    RepositorySourceAccessDenied,
    RepositorySourceError,
    RepositorySourceUnavailable,
)
from devrelay.application.ports.task_persistence import TaskSnapshotNotFound
from devrelay.application.tasks import TaskImportResult
from devrelay.bootstrap import control
from devrelay.bootstrap.control import create_app
from devrelay.bootstrap.jobs import repository_policy
from devrelay.bootstrap.persistence import open_database_engine
from devrelay.config.control import ControlSettings
from devrelay.config.database import DatabaseSettings
from devrelay.config.task_import import TaskImportSettings
from devrelay.domain.tasks import TaskIdentity, TaskSnapshot

TOKEN = "test_only_submission_token_0123456789abcdefghijklmnop"
IMPORT_TOKEN = "test_only_import_token_0123456789abcdefghijklmnop"
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
PATH = "/api/v1/jobs"


class Operation:
    def __init__(self, configuration: ControlSettings) -> None:
        assert configuration.jira is not None
        policy = repository_policy(configuration.repositories[0], configuration.jira)
        snapshot = TaskSnapshot(
            id=uuid4(),
            identity=TaskIdentity(
                provider="jira",
                site_id=str(configuration.jira.cloud_id),
                external_id="10005",
            ),
            display_key="DRY-5",
            title="Submit a task",
            description="Preserve the selected requirements.",
            acceptance_criteria=("Record one queued Job.",),
            captured_at=datetime.now(UTC),
        )
        intent = PreparedJobSubmission(
            stored=StoredTaskSnapshot(source_task_id=uuid4(), snapshot=snapshot),
            policy=policy,
            actor_id="test-operator",
        )
        job = intent.build_candidate(
            job_id=uuid4(),
            head=RepositoryHead(
                repository_id=policy.repository_id,
                target_branch=policy.target_branch,
                revision="a" * 40,
            ),
            now=datetime.now(UTC),
        )
        self.result = SavedJobSubmission(
            stored=StoredJobSubmission(intent=intent, job=job), created=True
        )
        self.calls: list[tuple[UUID, UUID, SubmissionAuthorization]] = []
        self.events: list[str] = []
        self.error: BaseException | None = None
        self.entered: asyncio.Event | None = None
        self.release: asyncio.Event | None = None

    @property
    def payload(self) -> dict[str, str]:
        return {
            "snapshot_id": str(self.result.stored.job.snapshot_id),
            "repository_id": str(self.result.stored.job.repository_id),
        }

    async def __call__(
        self,
        *,
        snapshot_id: UUID,
        repository_id: UUID,
        authorization: SubmissionAuthorization,
    ) -> SavedJobSubmission:
        self.calls.append((snapshot_id, repository_id, authorization))
        if self.entered is not None and self.release is not None:
            self.entered.set()
            await self.release.wait()
        if self.error is not None:
            raise self.error
        return self.result

    @asynccontextmanager
    async def resource(self) -> AsyncIterator[SubmissionOperation]:
        self.events.append("open")
        try:
            yield self
        finally:
            self.events.append("close")


def app(operation: Operation, configuration: ControlSettings) -> FastAPI:
    return create_app(configuration, submission_resources=operation.resource)


@pytest.mark.parametrize(
    "header",
    [
        None,
        "Basic abc",
        "Bearer",
        "Bearer wrong-token",
        f"Bearer {IMPORT_TOKEN}",
        "Bearer " + "x" * 129,
    ],
)
def test_invalid_credentials_never_submit(
    submission_configuration: ControlSettings, header: str | None
) -> None:
    operation = Operation(submission_configuration)

    with TestClient(app(operation, submission_configuration)) as client:
        response = client.post(
            PATH,
            headers={} if header is None else {"Authorization": header},
            json=operation.payload,
        )

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"
    assert operation.calls == []
    assert operation.events == ["open", "close"]
    assert TOKEN not in response.text


def test_duplicate_authorization_is_rejected(
    submission_configuration: ControlSettings,
) -> None:
    operation = Operation(submission_configuration)

    with TestClient(app(operation, submission_configuration)) as client:
        response = client.post(
            PATH,
            headers=[("Authorization", f"Bearer {TOKEN}")] * 2,
            json=operation.payload,
        )

    assert response.status_code == 401
    assert operation.calls == []


def test_query_cookie_and_body_do_not_authenticate(
    submission_configuration: ControlSettings,
) -> None:
    operation = Operation(submission_configuration)
    payload = {**operation.payload, "token": TOKEN}

    with TestClient(app(operation, submission_configuration)) as client:
        client.cookies.set("token", TOKEN)
        response = client.post(PATH + f"?token={TOKEN}", json=payload)

    assert response.status_code == 401
    assert operation.calls == []
    assert TOKEN not in response.text


@pytest.mark.parametrize("created, status", [(True, 201), (False, 200)])
def test_success_and_replay_return_only_materialized_job_identity(
    submission_configuration: ControlSettings, created: bool, status: int
) -> None:
    operation = Operation(submission_configuration)
    operation.result = replace(operation.result, created=created)
    application = app(operation, submission_configuration)
    expected = operation.result.stored.job

    with TestClient(application) as client:
        response = client.post(PATH, headers=HEADERS, json=operation.payload)

    assert response.status_code == status
    assert response.headers["cache-control"] == "no-store"
    assert response.json() == {
        "job_id": str(expected.id),
        "source_task_id": str(expected.source_task_id),
        "snapshot_id": str(expected.snapshot_id),
        "repository_id": str(expected.repository_id),
        "status": "queued",
        "created": created,
    }
    assert operation.calls == [
        (
            expected.snapshot_id,
            expected.repository_id,
            SubmissionAuthorization(
                actor_id="test-operator",
                allowed_repository_ids=frozenset({expected.repository_id}),
            ),
        )
    ]
    assert application.state.job_submission_services is None


@pytest.mark.parametrize(
    "updates",
    [
        {"snapshot_id": "bad-id"},
        {"repository_id": 123},
        {"actor_id": "spoofed"},
        {"allowed_repository_ids": []},
        {"base_revision": "b" * 40},
        {"target_branch": "other"},
        {"checks": []},
        {"secret": TOKEN},
    ],
)
def test_untrusted_identity_and_policy_fields_are_rejected(
    submission_configuration: ControlSettings, updates: dict[str, object]
) -> None:
    operation = Operation(submission_configuration)
    payload = {**operation.payload, **updates}

    with TestClient(app(operation, submission_configuration)) as client:
        response = client.post(PATH, headers=HEADERS, json=payload)

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "invalid_request"
    assert TOKEN not in response.text
    assert "spoofed" not in response.text
    assert operation.calls == []


@pytest.mark.parametrize(
    "error, status, code",
    [
        (JobSubmissionRejected(TOKEN), 403, "submission_rejected"),
        (JobSubmissionConflict(TOKEN), 409, "submission_conflict"),
        (TaskRequirementsChanged(TOKEN), 409, "task_requirements_changed"),
        (TaskNotReady(()), 409, "task_not_ready"),
        (TaskSnapshotNotFound(TOKEN), 404, "snapshot_not_found"),
        (RepositorySourceAccessDenied(TOKEN), 502, "repository_access_denied"),
        (RepositorySourceUnavailable(TOKEN), 503, "repository_unavailable"),
        (InvalidRepositorySourceData(TOKEN), 502, "invalid_repository_data"),
        (RepositorySourceError(TOKEN), 502, "invalid_repository_data"),
        (JobPersistenceError(TOKEN), 503, "storage_unavailable"),
        (RuntimeError(TOKEN), 500, "internal_error"),
    ],
)
def test_failures_use_central_sanitized_handlers(
    submission_configuration: ControlSettings,
    error: Exception,
    status: int,
    code: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    operation = Operation(submission_configuration)
    operation.error = error

    with TestClient(app(operation, submission_configuration)) as client:
        response = client.post(PATH, headers=HEADERS, json=operation.payload)

    assert response.status_code == status
    assert response.json()["detail"]["code"] == code
    assert TOKEN not in response.text
    assert TOKEN not in caplog.text
    assert all(record.exc_info is None for record in caplog.records)


@pytest.mark.parametrize(
    "body, headers, status",
    [
        (b'{"snapshot_id":', {"Content-Type": "application/json"}, 422),
        (b"x" * 1025, {"Content-Type": "application/json"}, 413),
        (b"{}", {"Content-Type": "text/plain"}, 415),
        (b"{}", {"Content-Type": "application/json", "Content-Encoding": "gzip"}, 415),
    ],
)
def test_invalid_transport_bodies_are_not_submitted(
    submission_configuration: ControlSettings,
    body: bytes,
    headers: dict[str, str],
    status: int,
) -> None:
    operation = Operation(submission_configuration)

    with TestClient(app(operation, submission_configuration)) as client:
        response = client.post(PATH, headers={**HEADERS, **headers}, content=body)

    assert response.status_code == status
    assert operation.calls == []


def test_openapi_has_separate_authentication_and_bounded_request_schema(
    submission_configuration: ControlSettings,
) -> None:
    operation = Operation(submission_configuration)

    schema = app(operation, submission_configuration).openapi()

    endpoint = schema["paths"][PATH]["post"]
    assert endpoint["security"] == [{"JobSubmissionBearer": []}]
    request = endpoint["requestBody"]["content"]["application/json"]["schema"]
    assert request["required"] == ["snapshot_id", "repository_id"]
    assert request["additionalProperties"] is False
    for status in (200, 201, 401, 403, 404, 408, 409, 413, 415, 422, 500, 502, 503):
        assert str(status) in endpoint["responses"]
    assert TOKEN not in str(schema)


@pytest.mark.parametrize(
    "mode, status",
    [("unauthorized", 401), ("chunked", 413), ("timeout", 408), ("disconnect", 400)],
)
def test_stream_limits_and_authentication_precede_submission(
    submission_configuration: ControlSettings, mode: str, status: int
) -> None:
    values = submission_configuration.model_dump()
    values["job_submission"]["body_timeout_seconds"] = 0.1
    values["job_submission"]["max_concurrent_requests"] = 1
    operation = Operation(submission_configuration)
    application = app(operation, ControlSettings.model_validate(values))
    messages: list[Message] = []
    reads = 0
    recovered: list[httpx.Response] = []

    async def receive() -> Message:
        nonlocal reads
        reads += 1
        if mode == "unauthorized":
            raise AssertionError("Authentication must precede body reads.")
        if mode == "chunked":
            return {"type": "http.request", "body": b"x" * 700, "more_body": True}
        if mode == "timeout":
            await asyncio.Event().wait()
        return {"type": "http.disconnect"}

    async def send(message: Message) -> None:
        messages.append(message)

    async def scenario() -> None:
        headers = [(b"content-type", b"application/json")]
        if mode != "unauthorized":
            headers.append((b"authorization", f"Bearer {TOKEN}".encode()))
        async with application.router.lifespan_context(application):
            await application(
                {
                    "type": "http",
                    "asgi": {"version": "3.0"},
                    "http_version": "1.1",
                    "method": "POST",
                    "scheme": "http",
                    "path": PATH,
                    "raw_path": PATH.encode(),
                    "query_string": b"",
                    "root_path": "",
                    "headers": headers,
                    "client": ("127.0.0.1", 50000),
                    "server": ("testserver", 80),
                },
                receive,
                send,
            )
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=application),
                base_url="http://testserver",
            ) as client:
                recovered.append(
                    await client.post(PATH, headers=HEADERS, json=operation.payload)
                )

    asyncio.run(scenario())

    assert messages[0]["status"] == status
    assert len(operation.calls) == 1
    assert recovered[0].status_code == 201
    assert reads == (0 if mode == "unauthorized" else 2 if mode == "chunked" else 1)


def test_cancelled_submission_releases_admission(
    submission_configuration: ControlSettings,
) -> None:
    operation = Operation(submission_configuration)
    operation.error = asyncio.CancelledError()
    application = app(operation, submission_configuration)

    async def scenario() -> None:
        async with application.router.lifespan_context(application):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=application),
                base_url="http://testserver",
            ) as client:
                with pytest.raises(asyncio.CancelledError):
                    await client.post(PATH, headers=HEADERS, json=operation.payload)
                operation.error = None
                recovered = await client.post(
                    PATH, headers=HEADERS, json=operation.payload
                )
        assert recovered.status_code == 201
        assert len(operation.calls) == 2

    asyncio.run(scenario())


def test_default_resources_share_one_pool_and_credentials_remain_separate(
    submission_configuration: ControlSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    values = submission_configuration.model_dump()
    values["task_import"] = TaskImportSettings(auth_token=SecretStr(IMPORT_TOKEN))
    configuration = ControlSettings.model_validate(values)
    original_database = open_database_engine
    events: list[str] = []

    @asynccontextmanager
    async def monitored_database(
        settings: DatabaseSettings,
    ) -> AsyncIterator[AsyncEngine]:
        events.append("open")
        async with original_database(settings) as engine:
            yield engine
        events.append("close")

    async def unexpected_provider_read(
        *args: object, **kwargs: object
    ) -> httpx.Response:
        raise AssertionError(
            "Startup and unauthorized requests must not read providers."
        )

    monkeypatch.setattr(control, "open_database_engine", monitored_database)
    monkeypatch.setattr(httpx.AsyncClient, "send", unexpected_provider_read)
    application = create_app(configuration)

    with TestClient(application) as client:
        health = client.get("/health/live")
        rejected_submission = client.post(
            PATH, headers={"Authorization": f"Bearer {IMPORT_TOKEN}"}, json={}
        )
        rejected_import = client.post("/api/v1/tasks/import", headers=HEADERS, json={})

    assert health.status_code == 200
    assert rejected_submission.status_code == rejected_import.status_code == 401
    assert events == ["open", "close"]
    assert application.state.job_submission_services is None
    assert application.state.task_import_services is None


def test_partial_startup_failure_closes_resources_and_clears_services(
    submission_configuration: ControlSettings,
) -> None:
    values = submission_configuration.model_dump()
    values["task_import"] = TaskImportSettings(auth_token=SecretStr(IMPORT_TOKEN))
    events: list[str] = []

    async def unused_import(key: str) -> TaskImportResult:
        raise AssertionError("Startup must not import tasks.")

    @asynccontextmanager
    async def import_resource() -> AsyncIterator[ImportOperation]:
        events.append("open")
        try:
            yield unused_import
        finally:
            events.append("close")

    @asynccontextmanager
    async def failing_submission() -> AsyncIterator[SubmissionOperation]:
        raise RuntimeError("Test startup failure.")
        yield

    application = create_app(
        ControlSettings.model_validate(values),
        import_resources=import_resource,
        submission_resources=failing_submission,
    )

    with pytest.raises(RuntimeError, match="Test startup failure"):
        with TestClient(application):
            pass

    assert events == ["open", "close"]
    assert application.state.task_import_services is None
    assert application.state.job_submission_services is None


def test_concurrent_submission_admission_is_bounded(
    submission_configuration: ControlSettings,
) -> None:
    values = submission_configuration.model_dump()
    values["job_submission"]["max_concurrent_requests"] = 1
    operation = Operation(submission_configuration)
    application = app(operation, ControlSettings.model_validate(values))

    async def scenario() -> None:
        operation.entered, operation.release = asyncio.Event(), asyncio.Event()
        async with application.router.lifespan_context(application):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=application),
                base_url="http://testserver",
            ) as client:
                first = asyncio.create_task(
                    client.post(PATH, headers=HEADERS, json=operation.payload)
                )
                await asyncio.wait_for(operation.entered.wait(), timeout=2)
                try:
                    busy = await client.post(
                        PATH, headers=HEADERS, json=operation.payload
                    )
                finally:
                    operation.release.set()
                    accepted = await first
                recovered = await client.post(
                    PATH, headers=HEADERS, json=operation.payload
                )
        assert busy.status_code == 503
        assert busy.json()["detail"]["code"] == "submission_busy"
        assert busy.headers["retry-after"] == "1"
        assert accepted.status_code == recovered.status_code == 201
        assert len(operation.calls) == 2

    asyncio.run(scenario())
