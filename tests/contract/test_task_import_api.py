import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr, ValidationError
from starlette.types import Message, Receive

from devrelay.api.dependencies import ImportOperation
from devrelay.application.contracts.tasks import SavedTaskSnapshot
from devrelay.application.ports.task_persistence import TaskPersistenceError
from devrelay.application.ports.task_source import (
    InvalidTaskSourceData,
    TaskNotFound,
    TaskSourceAccessDenied,
    TaskSourceError,
    TaskSourceScopeDenied,
    TaskSourceUnavailable,
)
from devrelay.application.tasks import (
    RequirementGap,
    RequirementsStatus,
    TaskImportRejected,
    TaskImportResult,
)
from devrelay.bootstrap.control import create_app
from devrelay.config.control import ControlSettings
from devrelay.config.database import DatabaseSettings
from devrelay.config.jira import JiraSettings
from devrelay.config.task_import import TaskImportSettings
from devrelay.domain.tasks import TaskIdentity, TaskSnapshot

TOKEN = "test_only_import_token_0123456789abcdefghijklmnop"
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
PATH = "/api/v1/tasks/import"


def settings(*, max_concurrent: int = 4, body_timeout: float = 5) -> ControlSettings:
    return ControlSettings(
        jira=JiraSettings(
            site_url="https://example.atlassian.net",
            cloud_id=UUID("11111111-2222-3333-4444-555555555555"),
            email="reader@example.test",
            api_token=SecretStr("test-only-jira-token"),
            allowed_project_keys=frozenset({"DRY"}),
        ),
        database=DatabaseSettings(
            host="127.0.0.1",
            database="devrelay_test",
            username="devrelay_test",
            password=SecretStr("test-only-database-password"),
        ),
        task_import=TaskImportSettings(
            auth_token=SecretStr(TOKEN),
            max_concurrent_requests=max_concurrent,
            body_timeout_seconds=body_timeout,
        ),
    )


def result(*, created: bool = True) -> TaskImportResult:
    snapshot = TaskSnapshot(
        id=uuid4(),
        identity=TaskIdentity(
            provider="jira", site_id="test-site", external_id="10005"
        ),
        display_key="DRY-5",
        title="Import a task",
        description="",
        acceptance_criteria=(),
        captured_at=datetime.now(UTC),
    )
    return TaskImportResult(
        saved=SavedTaskSnapshot(
            source_task_id=uuid4(), snapshot=snapshot, created=created
        ),
        observed_labels=frozenset({"backend"}),
        requirements_status=RequirementsStatus.INCOMPLETE,
        missing_requirements=tuple(RequirementGap),
    )


class Operation:
    def __init__(self, *, created: bool = True, error: Exception | None = None) -> None:
        self.result = result(created=created)
        self.error = error
        self.calls: list[str] = []
        self.events: list[str] = []

    async def __call__(self, key: str) -> TaskImportResult:
        self.calls.append(key)
        if self.error is not None:
            raise self.error
        return self.result

    @asynccontextmanager
    async def resource(self) -> AsyncIterator[ImportOperation]:
        self.events.append("open")
        try:
            yield self
        finally:
            self.events.append("close")


def app(operation: Operation, configuration: ControlSettings | None = None) -> FastAPI:
    return create_app(configuration or settings(), import_resources=operation.resource)


@pytest.mark.parametrize(
    "header",
    [
        None,
        "",
        "Basic abc",
        "Bearer",
        "Bearer wrong-token",
        "Bearer " + "x" * 129,
        "Bearer ünicode",
    ],
)
def test_invalid_credentials_never_call_import(header: str | None) -> None:
    operation = Operation()
    with TestClient(app(operation)) as client:
        response = client.post(
            PATH,
            headers={}
            if header is None
            else {b"Authorization": header.encode("utf-8")},
            json={"task_key": "DRY-5"},
        )
    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"
    assert response.json()["detail"]["code"] == "unauthorized"
    assert operation.calls == []
    assert operation.events == ["open", "close"]


def test_tokens_in_query_cookie_or_body_are_not_credentials() -> None:
    operation = Operation()
    with TestClient(app(operation)) as client:
        client.cookies.set("token", TOKEN)
        response = client.post(
            PATH + f"?token={TOKEN}", json={"task_key": "DRY-5", "token": TOKEN}
        )
    assert response.status_code == 401
    assert TOKEN not in response.text
    assert operation.calls == []


def test_duplicate_authorization_headers_are_rejected() -> None:
    operation = Operation()
    with TestClient(app(operation)) as client:
        response = client.post(
            PATH,
            headers=[
                ("Authorization", f"Bearer {TOKEN}"),
                ("Authorization", f"Bearer {TOKEN}"),
            ],
            json={"task_key": "DRY-5"},
        )
    assert response.status_code == 401
    assert operation.calls == []


@pytest.mark.parametrize("created, status", [(True, 201), (False, 200)])
def test_success_and_duplicate_return_materialized_diagnostics(
    created: bool, status: int
) -> None:
    operation = Operation(created=created)
    application = app(operation)
    with TestClient(application) as client:
        response = client.post(PATH, headers=HEADERS, json={"task_key": "DRY-5"})
    assert response.status_code == status
    assert response.headers["cache-control"] == "no-store"
    body = response.json()
    assert body["snapshot_id"] == str(operation.result.saved.snapshot.id)
    assert body["created"] == created
    assert body["requirements_status"] == "incomplete"
    assert body["missing_requirements"] == [item.value for item in RequirementGap]
    assert "description" not in body
    assert TOKEN not in response.text
    assert operation.calls == ["DRY-5"]
    assert application.state.task_import_services is None


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"task_key": ""},
        {"task_key": " DRY-5"},
        {"task_key": "https://example.test"},
        {"task_key": 5},
        {"task_key": "DRY-5", "url": "https://example.test"},
        {"task_key": "DRY-5", "secret": TOKEN},
    ],
)
def test_invalid_input_is_not_reflected_or_imported(payload: dict[str, object]) -> None:
    operation = Operation()
    with TestClient(app(operation)) as client:
        response = client.post(PATH, headers=HEADERS, json=payload)
    assert response.status_code == 422
    assert response.json() == {
        "detail": {
            "code": "invalid_request",
            "message": "Provide only a valid task_key.",
        }
    }
    assert TOKEN not in response.text
    assert "example.test" not in response.text
    assert operation.calls == []


@pytest.mark.parametrize(
    "body, content_type, status, code, message",
    [
        (
            b'{"task_key":',
            "application/json",
            422,
            "invalid_request",
            "Provide only a valid task_key.",
        ),
        (
            b"x" * 1025,
            "application/json",
            413,
            "body_too_large",
            "Request exceeds the size limit.",
        ),
        (
            b"x",
            "text/plain",
            415,
            "unsupported_media_type",
            "Uncompressed application/json is required.",
        ),
        (
            b"",
            "application/json",
            422,
            "invalid_request",
            "Provide only a valid task_key.",
        ),
    ],
)
def test_body_validation_is_bounded(
    body: bytes, content_type: str, status: int, code: str, message: str
) -> None:
    operation = Operation()
    with TestClient(app(operation)) as client:
        response = client.post(
            PATH, headers={**HEADERS, "Content-Type": content_type}, content=body
        )
    assert response.status_code == status
    assert response.json() == {"detail": {"code": code, "message": message}}
    assert operation.calls == []


def test_compressed_input_is_rejected() -> None:
    operation = Operation()
    with TestClient(app(operation)) as client:
        response = client.post(
            PATH,
            headers={**HEADERS, "Content-Encoding": "gzip"},
            json={"task_key": "DRY-5"},
        )
    assert response.status_code == 415
    assert operation.calls == []


@pytest.mark.parametrize(
    "error, status, code",
    [
        (TaskImportRejected(TOKEN), 403, "import_rejected"),
        (TaskSourceScopeDenied(TOKEN), 403, "import_rejected"),
        (TaskNotFound(TOKEN), 404, "task_not_found"),
        (TaskSourceAccessDenied(TOKEN), 502, "source_access_denied"),
        (TaskSourceUnavailable(TOKEN), 503, "source_unavailable"),
        (InvalidTaskSourceData(TOKEN), 502, "invalid_source_data"),
        (TaskSourceError(TOKEN), 502, "invalid_source_data"),
        (TaskPersistenceError(TOKEN), 503, "storage_unavailable"),
        (RuntimeError(TOKEN), 500, "internal_error"),
        (TimeoutError(TOKEN), 500, "internal_error"),
        (ValueError(TOKEN), 500, "internal_error"),
        (
            ValidationError.from_exception_data(
                "InternalData",
                [{"type": "missing", "loc": ("secret",), "input": TOKEN}],
            ),
            500,
            "internal_error",
        ),
    ],
)
def test_errors_are_classified_without_secret_disclosure(
    error: Exception, status: int, code: str, caplog: pytest.LogCaptureFixture
) -> None:
    operation = Operation(error=error)
    with TestClient(app(operation)) as client:
        response = client.post(PATH, headers=HEADERS, json={"task_key": "DRY-5"})
        operation.error = None
        assert (
            client.post(PATH, headers=HEADERS, json={"task_key": "DRY-5"}).status_code
            == 201
        )
    assert response.status_code == status
    assert response.json()["detail"]["code"] == code
    assert TOKEN not in response.text
    assert TOKEN not in caplog.text
    assert all(record.exc_info is None for record in caplog.records)
    if status == 500:
        assert len(caplog.records) == 1
        assert caplog.records[0].__dict__["error_type"] == type(error).__name__


def test_disabled_import_is_not_registered() -> None:
    with TestClient(create_app()) as client:
        assert (
            client.post(PATH, headers=HEADERS, json={"task_key": "DRY-5"}).status_code
            == 404
        )
        assert client.get("/health/live").status_code == 200


def test_cancellation_escapes_local_catch_all_and_releases_admission() -> None:
    async def scenario() -> None:
        calls = 0

        async def operation(key: str) -> TaskImportResult:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise asyncio.CancelledError
            raise TaskNotFound(TOKEN)

        @asynccontextmanager
        async def resource() -> AsyncIterator[ImportOperation]:
            yield operation

        application = create_app(settings(max_concurrent=1), import_resources=resource)
        async with application.router.lifespan_context(application):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=application),
                base_url="http://testserver",
            ) as client:
                with pytest.raises(asyncio.CancelledError):
                    await client.post(PATH, headers=HEADERS, json={"task_key": "DRY-5"})
                response = await client.post(
                    PATH, headers=HEADERS, json={"task_key": "DRY-5"}
                )
                assert response.status_code == 404
                assert calls == 2

    asyncio.run(scenario())


def test_openapi_declares_authentication_and_request_schema() -> None:
    operation = Operation()
    schema = app(operation).openapi()
    endpoint = schema["paths"][PATH]["post"]
    assert endpoint["security"] == [{"TaskImportBearer": []}]
    assert endpoint["requestBody"]["content"]["application/json"]["schema"][
        "required"
    ] == ["task_key"]
    for status in (401, 403, 404, 408, 413, 415, 422, 500, 502, 503):
        assert endpoint["responses"][str(status)]["content"]["application/json"][
            "schema"
        ] == {"$ref": "#/components/schemas/ApiErrorResponse"}
    assert schema["components"]["schemas"]["ApiErrorResponse"]["required"] == ["detail"]
    assert schema["components"]["schemas"]["ApiErrorDetail"]["required"] == [
        "code",
        "message",
    ]
    assert TOKEN not in str(schema)


def test_concurrent_import_admission_is_bounded_and_recovers() -> None:
    async def scenario() -> None:
        entered = asyncio.Event()
        release = asyncio.Event()
        captured = result()

        async def operation(key: str) -> TaskImportResult:
            entered.set()
            await release.wait()
            return captured

        @asynccontextmanager
        async def resource() -> AsyncIterator[ImportOperation]:
            yield operation

        application = create_app(settings(max_concurrent=1), import_resources=resource)
        async with application.router.lifespan_context(application):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=application),
                base_url="http://testserver",
            ) as client:
                first = asyncio.create_task(
                    client.post(PATH, headers=HEADERS, json={"task_key": "DRY-5"})
                )
                await asyncio.wait_for(entered.wait(), timeout=2)
                try:
                    busy = await client.post(
                        PATH, headers=HEADERS, json={"task_key": "DRY-6"}
                    )
                    assert busy.status_code == 503
                    assert busy.json()["detail"]["code"] == "import_busy"
                    assert busy.headers["retry-after"] == "1"
                    assert (await client.get("/health/live")).status_code == 200
                finally:
                    release.set()
                    assert (await first).status_code == 201
                assert (
                    await client.post(PATH, headers=HEADERS, json={"task_key": "DRY-7"})
                ).status_code == 201

    asyncio.run(scenario())


async def exchange(
    application: FastAPI, receive: Receive, *, authorized: bool = True
) -> list[Message]:
    messages: list[Message] = []

    async def send(message: Message) -> None:
        messages.append(message)

    headers = [(b"content-type", b"application/json")]
    if authorized:
        headers.append((b"authorization", f"Bearer {TOKEN}".encode()))
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
    return messages


def test_unauthorized_request_body_is_never_consumed() -> None:
    async def scenario() -> None:
        operation = Operation()
        application = app(operation)

        async def receive() -> Message:
            raise AssertionError("Authentication must precede body consumption.")

        async with application.router.lifespan_context(application):
            messages = await exchange(application, receive, authorized=False)
        assert messages[0]["status"] == 401
        assert operation.calls == []

    asyncio.run(scenario())


def test_chunked_body_is_limited_without_content_length() -> None:
    async def scenario() -> None:
        operation = Operation()
        application = app(operation)
        reads = 0

        async def receive() -> Message:
            nonlocal reads
            reads += 1
            return {"type": "http.request", "body": b"x" * 700, "more_body": True}

        async with application.router.lifespan_context(application):
            messages = await exchange(application, receive)
        assert messages[0]["status"] == 413
        assert reads == 2
        assert operation.calls == []

    asyncio.run(scenario())


def test_body_timeout_releases_admission() -> None:
    async def scenario() -> None:
        operation = Operation()
        application = app(operation, settings(max_concurrent=1, body_timeout=0.1))

        async def receive() -> Message:
            await asyncio.Event().wait()
            raise AssertionError("Unreachable.")

        async with application.router.lifespan_context(application):
            messages = await exchange(application, receive)
            assert messages[0]["status"] == 408
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=application),
                base_url="http://testserver",
            ) as client:
                assert (
                    await client.post(PATH, headers=HEADERS, json={"task_key": "DRY-5"})
                ).status_code == 201
        assert operation.calls == ["DRY-5"]

    asyncio.run(scenario())


def test_disconnected_body_is_not_imported() -> None:
    async def scenario() -> None:
        operation = Operation()
        application = app(operation)

        async def receive() -> Message:
            return {"type": "http.disconnect"}

        async with application.router.lifespan_context(application):
            messages = await exchange(application, receive)
        assert messages[0]["status"] == 400
        assert operation.calls == []

    asyncio.run(scenario())
