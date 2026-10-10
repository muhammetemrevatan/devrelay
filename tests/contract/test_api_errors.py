import asyncio
import traceback
from collections.abc import AsyncIterator

import pytest
from fastapi.testclient import TestClient
from starlette.background import BackgroundTask
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.types import Message, Receive, Scope, Send

from devrelay.api.middleware import ResponseAbortedError, UnexpectedErrorMiddleware
from devrelay.api.schemas.tasks import TaskImportRequest
from devrelay.application.ports.task_persistence import TaskPersistenceError
from devrelay.application.ports.task_source import (
    TaskNotFound,
    TaskSourceError,
    TaskSourceScopeDenied,
)
from devrelay.bootstrap.control import create_app

SECRET = "arbitrary-integration-secret"


class RefinedScopeDenied(TaskSourceScopeDenied):
    """An unmapped subtype should inherit its closest registered handler."""


@pytest.mark.parametrize(
    "error, status, code, message",
    [
        (
            TaskNotFound(SECRET),
            404,
            "task_not_found",
            "Task was not found in the configured scope.",
        ),
        (
            TaskPersistenceError(SECRET),
            503,
            "storage_unavailable",
            "Task storage is unavailable.",
        ),
        (
            TaskSourceError(SECRET),
            502,
            "invalid_source_data",
            "Task source read failed.",
        ),
        (
            RefinedScopeDenied(SECRET),
            403,
            "import_rejected",
            "Task is outside the import scope.",
        ),
    ],
)
def test_handlers_apply_to_other_routes_and_choose_specific_ancestors(
    error: Exception,
    status: int,
    code: str,
    message: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    application = create_app()

    @application.get("/test/error")
    async def endpoint() -> None:
        try:
            raise RuntimeError(SECRET)
        except RuntimeError as cause:
            raise error from cause

    # Default TestClient propagates unhandled failures: this also checks that
    # mapped exceptions do not reach ServerErrorMiddleware's re-raise path.
    with TestClient(application) as client:
        response = client.get("/test/error")
    assert response.status_code == status
    assert response.json() == {"detail": {"code": code, "message": message}}
    assert SECRET not in response.text
    assert SECRET not in caplog.text
    assert all(record.exc_info is None for record in caplog.records)


def test_unexpected_errors_in_other_routes_are_sanitized(
    caplog: pytest.LogCaptureFixture,
) -> None:
    application = create_app()

    @application.get("/test/unexpected")
    async def endpoint() -> None:
        try:
            raise ValueError(SECRET)
        except ValueError as cause:
            raise RuntimeError(SECRET) from cause

    with TestClient(application) as client:
        response = client.get(
            "/test/unexpected", params={"secret": SECRET}, headers={"X-Secret": SECRET}
        )
        assert client.get("/health/live").status_code == 200
    assert response.status_code == 500
    assert response.json() == {
        "detail": {"code": "internal_error", "message": "Internal error."}
    }
    assert SECRET not in response.text
    assert SECRET not in caplog.text
    assert len(caplog.records) == 1
    assert caplog.records[0].__dict__["error_type"] == "RuntimeError"
    assert caplog.records[0].exc_info is None


def test_response_validation_failure_is_an_internal_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    application = create_app()

    @application.get("/test/invalid-response", response_model=TaskImportRequest)
    async def endpoint() -> dict[str, str]:
        return {"task_key": SECRET}

    with TestClient(application) as client:
        response = client.get("/test/invalid-response")
    assert response.status_code == 500
    assert response.json()["detail"]["code"] == "internal_error"
    assert SECRET not in response.text
    assert SECRET not in caplog.text
    assert caplog.records[0].__dict__["error_type"] == "ResponseValidationError"
    assert caplog.records[0].exc_info is None


def http_scope() -> Scope:
    return {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.4"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/test/late-error",
        "raw_path": b"/test/late-error",
        "query_string": b"",
        "root_path": "",
        "headers": [],
        "client": ("127.0.0.1", 50000),
        "server": ("testserver", 80),
    }


async def unused_receive() -> Message:
    raise AssertionError("Error handling must not read the request body.")


@pytest.mark.parametrize("background", [False, True])
def test_late_failures_abort_without_second_response_or_sensitive_traceback(
    background: bool,
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def fail() -> None:
        raise RuntimeError(SECRET)

    async def stream() -> AsyncIterator[bytes]:
        yield b"partial"
        await fail()

    application = create_app()

    @application.get("/test/late-error")
    async def endpoint() -> Response:
        if background:
            return JSONResponse({"ok": True}, background=BackgroundTask(fail))
        return StreamingResponse(stream())

    async def scenario() -> None:
        messages: list[Message] = []

        async def send(message: Message) -> None:
            messages.append(message)

        with pytest.raises(ResponseAbortedError) as captured:
            await application(http_scope(), unused_receive, send)
        assert SECRET not in "".join(traceback.format_exception(captured.value))
        assert captured.value.__suppress_context__
        assert [
            m["status"] for m in messages if m["type"] == "http.response.start"
        ] == [200]
        assert len(messages) == 2
        assert messages[1]["type"] == "http.response.body"
        assert messages[1].get("more_body", False) is (not background)

    asyncio.run(scenario())
    assert SECRET not in caplog.text
    assert all(record.exc_info is None for record in caplog.records)


def test_failure_sending_error_response_is_also_sanitized(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def failed_app(scope: Scope, receive: Receive, send: Send) -> None:
        raise RuntimeError(SECRET)

    async def failed_send(message: Message) -> None:
        raise OSError(SECRET)

    async def scenario() -> None:
        with pytest.raises(ResponseAbortedError) as captured:
            await UnexpectedErrorMiddleware(failed_app)(
                http_scope(), unused_receive, failed_send
            )
        assert SECRET not in "".join(traceback.format_exception(captured.value))

    asyncio.run(scenario())
    assert SECRET not in caplog.text
    assert all(record.exc_info is None for record in caplog.records)


@pytest.mark.parametrize("scope_type", ["websocket", "lifespan"])
def test_non_http_scopes_pass_through_without_http_error_handling(
    scope_type: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def failed_app(scope: Scope, receive: Receive, send: Send) -> None:
        assert scope["type"] == scope_type
        raise RuntimeError(SECRET)

    async def unused_send(message: Message) -> None:
        raise AssertionError("Non-HTTP scopes must not receive HTTP responses.")

    async def scenario() -> None:
        with pytest.raises(RuntimeError, match=SECRET):
            await UnexpectedErrorMiddleware(failed_app)(
                {"type": scope_type}, unused_receive, unused_send
            )

    asyncio.run(scenario())
    assert caplog.records == []
