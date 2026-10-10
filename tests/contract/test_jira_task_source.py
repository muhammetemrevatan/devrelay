import asyncio
import base64
import traceback
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from pydantic import JsonValue, SecretStr, TypeAdapter

from devrelay.application.contracts.tasks import TaskRead
from devrelay.application.ports.task_source import (
    InvalidTaskSourceData,
    TaskNotFound,
    TaskSourceAccessDenied,
    TaskSourceError,
    TaskSourceUnavailable,
)
from devrelay.bootstrap.jira import open_jira_task_source
from devrelay.config.jira import JiraSettings, JiraTokenMode
from devrelay.domain.tasks import TaskIdentity, TaskSnapshot

_SITE_ID = UUID("11111111-2222-3333-4444-555555555555")
_TOKEN = "test-only-jira-credential-never-print"
_JSON = TypeAdapter(dict[str, JsonValue])


@pytest.fixture
def payload() -> dict[str, JsonValue]:
    file = Path(__file__).parent / "fixtures" / "jira" / "issue.json"
    return _JSON.validate_json(file.read_bytes())


@pytest.fixture
def settings() -> JiraSettings:
    return JiraSettings(
        site_url="https://example.atlassian.net",
        cloud_id=_SITE_ID,
        email="reader@example.test",
        api_token=SecretStr(_TOKEN),
        allowed_project_keys=frozenset({"DRY"}),
    )


def _fields(payload: dict[str, JsonValue]) -> dict[str, JsonValue]:
    fields = payload["fields"]
    assert isinstance(fields, dict)
    return fields


def _read(
    settings: JiraSettings,
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    key: str = "DRY-7",
) -> TaskRead:
    async def execute() -> TaskRead:
        async with open_jira_task_source(
            settings, transport=httpx.MockTransport(handler)
        ) as source:
            return await source.read_by_key(key)

    return asyncio.run(execute())


@pytest.mark.parametrize("correct_identity", [True, False])
def test_readiness_by_identity_returns_labels_after_key_rename(
    settings: JiraSettings,
    payload: dict[str, JsonValue],
    correct_identity: bool,
) -> None:
    payload["key"] = "DRY-99"
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=payload)

    async def scenario() -> None:
        async with open_jira_task_source(
            settings, transport=httpx.MockTransport(handle)
        ) as source:
            identity = TaskIdentity(
                provider="jira",
                site_id=str(_SITE_ID),
                external_id="10007" if correct_identity else "10008",
            )
            if correct_identity:
                read = await source.read_by_identity(identity)
                assert read.snapshot.identity == identity
                assert read.snapshot.display_key == "DRY-99"
                assert read.labels == frozenset({"agent-ready", "backend"})
            else:
                with pytest.raises(InvalidTaskSourceData):
                    await source.read_by_identity(identity)

    asyncio.run(scenario())
    assert len(requests) == 1
    assert requests[0].url.path.endswith(
        "/issue/10007" if correct_identity else "/issue/10008"
    )


def test_selected_task_is_mapped_and_only_requested_fields_are_read(
    settings: JiraSettings, payload: dict[str, JsonValue]
) -> None:
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=payload)

    result = _read(settings, handle)
    assert result.snapshot.identity == TaskIdentity(
        provider="jira", site_id=str(_SITE_ID), external_id="10007"
    )
    assert result.snapshot.acceptance_criteria == (
        "Use the stable task ID.",
        "Keep credentials out of errors.",
    )
    assert result.snapshot.source_updated_at == datetime(
        2026, 10, 9, 10, 45, tzinfo=UTC
    )
    assert result.snapshot.captured_at.tzinfo is UTC
    assert result.labels == frozenset({"agent-ready", "backend"})
    assert result.scope_key == "DRY"
    assert len(requests) == 1
    request = requests[0]
    assert request.method == "GET"
    assert request.url.params["updateHistory"] == "false"
    assert str(request.url).startswith(f"https://api.atlassian.com/ex/jira/{_SITE_ID}/")
    assert request.url.path.endswith("/rest/api/3/issue/DRY-7")
    assert set(request.url.params["fields"].split(",")) == {
        "summary",
        "description",
        "updated",
        "project",
        "labels",
    }
    assert (
        request.headers["authorization"]
        == "Basic "
        + base64.b64encode(f"reader@example.test:{_TOKEN}".encode()).decode()
    )


def test_unscoped_token_uses_the_configured_site(
    settings: JiraSettings, payload: dict[str, JsonValue]
) -> None:
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=payload)

    _read(settings.model_copy(update={"token_mode": JiraTokenMode.UNSCOPED}), handle)
    assert requests[0].url.host == "example.atlassian.net"
    assert requests[0].url.path == "/rest/api/3/issue/DRY-7"


def test_stable_id_read_accepts_a_renamed_display_key(
    settings: JiraSettings, payload: dict[str, JsonValue]
) -> None:
    payload["key"] = "DRY-99"
    identity = TaskIdentity(provider="jira", site_id=str(_SITE_ID), external_id="10007")

    async def execute() -> TaskSnapshot:
        async with open_jira_task_source(
            settings,
            transport=httpx.MockTransport(lambda _: httpx.Response(200, json=payload)),
        ) as source:
            return await source.fetch_snapshot(identity)

    assert asyncio.run(execute()).display_key == "DRY-99"


def test_wrong_stable_identity_is_rejected(
    settings: JiraSettings, payload: dict[str, JsonValue]
) -> None:
    async def execute() -> TaskSnapshot:
        async with open_jira_task_source(
            settings,
            transport=httpx.MockTransport(lambda _: httpx.Response(200, json=payload)),
        ) as source:
            return await source.fetch_snapshot(
                TaskIdentity(
                    provider="jira", site_id=str(_SITE_ID), external_id="99999"
                )
            )

    with pytest.raises(InvalidTaskSourceData, match="different task identity"):
        asyncio.run(execute())


@pytest.mark.parametrize(
    "key", ["OTHER-7", "DRY-7/../../", "https://other.example/DRY-7"]
)
def test_lookup_scope_and_paths_are_checked_before_network(
    settings: JiraSettings, key: str
) -> None:
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(500)

    with pytest.raises(TaskSourceError):
        _read(settings, handle, key=key)
    assert not requests


@pytest.mark.parametrize(
    ("status", "error_type"),
    [
        (401, TaskSourceAccessDenied),
        (403, TaskSourceAccessDenied),
        (404, TaskNotFound),
        (408, TaskSourceUnavailable),
        (429, TaskSourceUnavailable),
        (500, TaskSourceUnavailable),
        (503, TaskSourceUnavailable),
        (302, InvalidTaskSourceData),
        (400, InvalidTaskSourceData),
    ],
)
def test_status_errors_are_classified_without_retries_or_secret_body_logging(
    settings: JiraSettings, status: int, error_type: type[TaskSourceError]
) -> None:
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            status, text=_TOKEN, headers={"location": "https://other.example"}
        )

    with pytest.raises(error_type) as error:
        _read(settings, handle)
    assert _TOKEN not in "".join(traceback.format_exception(error.value))
    assert len(requests) == 1


@pytest.mark.parametrize("error_type", [httpx.ConnectError, httpx.ReadTimeout])
def test_transport_errors_are_redacted(
    settings: JiraSettings, error_type: type[httpx.RequestError]
) -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        raise error_type(f"Untrusted error contains {_TOKEN}", request=request)

    with pytest.raises(TaskSourceUnavailable) as error:
        _read(settings, handle)
    assert _TOKEN not in "".join(traceback.format_exception(error.value))


@pytest.mark.parametrize(
    "body, content_type",
    [(b"<html>login</html>", "text/html"), (b"{", "application/json")],
)
def test_invalid_transport_data_is_rejected(
    settings: JiraSettings, body: bytes, content_type: str
) -> None:
    with pytest.raises(InvalidTaskSourceData):
        _read(
            settings,
            lambda _: httpx.Response(
                200, content=body, headers={"content-type": content_type}
            ),
        )


@pytest.mark.parametrize(
    "field, value",
    [
        ("summary", 12),
        ("summary", " " * 3),
        ("updated", "2026-10-09T13:00:00"),
        ("project", {"key": "OTHER"}),
        ("project", {"key": "DRY2"}),
        ("labels", ["x" * 129]),
        ("description", {"type": "doc", "version": 2, "content": []}),
        (
            "description",
            {"type": "doc", "version": 1, "content": [{"type": "mediaSingle"}]},
        ),
    ],
)
def test_invalid_or_out_of_scope_provider_data_is_rejected(
    settings: JiraSettings, payload: dict[str, JsonValue], field: str, value: JsonValue
) -> None:
    _fields(payload)[field] = value
    with pytest.raises(TaskSourceError):
        _read(settings, lambda _: httpx.Response(200, json=payload))


def test_custom_criteria_are_explicit_and_do_not_guess_provider_fields(
    settings: JiraSettings, payload: dict[str, JsonValue]
) -> None:
    fields = _fields(payload)
    fields["customfield_12345"] = (
        "First explicit criterion.\nSecond explicit criterion."
    )
    configured = settings.model_copy(
        update={"acceptance_criteria_field": "customfield_12345"}
    )
    result = _read(configured, lambda _: httpx.Response(200, json=payload))
    assert result.snapshot.acceptance_criteria == (
        "First explicit criterion.",
        "Second explicit criterion.",
    )
    del fields["customfield_12345"]
    with pytest.raises(InvalidTaskSourceData, match="field is missing"):
        _read(configured, lambda _: httpx.Response(200, json=payload))


class _Stream(httpx.AsyncByteStream):
    def __init__(self, *, slow: bool = False) -> None:
        self.closed = False
        self.slow = slow

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b"x" * 800
        if self.slow:
            await asyncio.sleep(1)
        yield b"x" * 800

    async def aclose(self) -> None:
        self.closed = True


@pytest.mark.parametrize("slow", [False, True])
def test_stream_limits_and_total_deadline_close_responses(
    settings: JiraSettings, slow: bool
) -> None:
    stream = _Stream(slow=slow)
    configured = settings.model_copy(
        update={"max_response_bytes": 1024, "request_timeout_seconds": 0.02}
    )
    expected = TaskSourceUnavailable if slow else InvalidTaskSourceData
    with pytest.raises(expected):
        _read(
            configured,
            lambda _: httpx.Response(
                200, stream=stream, headers={"content-type": "application/json"}
            ),
        )
    assert stream.closed


def test_unrequested_compression_is_rejected_before_reading_the_body(
    settings: JiraSettings,
) -> None:
    stream = _Stream()
    with pytest.raises(InvalidTaskSourceData, match="identity encoding"):
        _read(
            settings,
            lambda _: httpx.Response(
                200,
                stream=stream,
                headers={
                    "content-type": "application/json",
                    "content-encoding": "gzip",
                },
            ),
        )
    assert stream.closed


def test_wrong_site_is_rejected_without_a_request(settings: JiraSettings) -> None:
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(500)

    async def execute() -> None:
        async with open_jira_task_source(
            settings, transport=httpx.MockTransport(handle)
        ) as source:
            await source.fetch_snapshot(
                TaskIdentity(
                    provider="jira", site_id="another-site", external_id="10007"
                )
            )

    with pytest.raises(TaskSourceAccessDenied):
        asyncio.run(execute())
    assert not requests


def test_null_description_and_absent_ready_label_are_observed_without_eligibility_decisions(
    settings: JiraSettings,
    payload: dict[str, JsonValue],
) -> None:
    _fields(payload)["description"] = None
    _fields(payload)["labels"] = []
    result = _read(settings, lambda _: httpx.Response(200, json=payload))
    assert result.snapshot.description == ""
    assert result.snapshot.acceptance_criteria == ()
    assert result.labels == frozenset()
