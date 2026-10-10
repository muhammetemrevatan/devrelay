import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.types import Message

from devrelay.api.dependencies import ImportOperation, WorkerAuthenticationServices
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.bootstrap import control
from devrelay.bootstrap.control import create_app
from devrelay.bootstrap.security import worker_authenticator
from devrelay.config.control import ControlSettings, RuntimeEnvironment
from devrelay.config.task_import import TaskImportSettings

TOKEN = "test_only_worker_token_0123456789abcdefghijklmnop"
OTHER_TOKEN = "test_only_other_worker_token_0123456789abcdefghijklmnop"
PATH = "/api/v1/worker/identity"
NOW = datetime(2026, 10, 10, tzinfo=UTC)


def test_query_token_is_not_worker_authentication(
    worker_configuration: ControlSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = application(worker_configuration, monkeypatch)

    with TestClient(app) as client:
        response = client.get(PATH, params={"token": TOKEN})

    assert response.status_code == 401
    assert TOKEN not in response.text


def test_explicitly_empty_registrations_remove_worker_route(
    worker_configuration: ControlSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    values = worker_configuration.model_dump()
    values["workers"] = ()
    app = application(ControlSettings.model_validate(values), monkeypatch)

    with TestClient(app) as client:
        response = client.get(PATH, headers={"Authorization": f"Bearer {TOKEN}"})

    assert response.status_code == 404


def test_rotation_after_restart_rejects_the_previous_token(
    worker_configuration: ControlSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    values = worker_configuration.model_dump()
    values["workers"][0]["auth_token"] = OTHER_TOKEN
    app = application(ControlSettings.model_validate(values), monkeypatch)

    with TestClient(app) as client:
        old = client.get(PATH, headers={"Authorization": f"Bearer {TOKEN}"})
        new = client.get(PATH, headers={"Authorization": f"Bearer {OTHER_TOKEN}"})

    assert old.status_code == 401
    assert new.status_code == 200


def test_each_worker_receives_only_its_own_repository_scope(
    worker_configuration: ControlSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    values = worker_configuration.model_dump()
    repo_id = uuid4()
    repo = dict(values["repositories"][0])
    repo.update(repository_id=repo_id, slug="example/other")
    values["repositories"] = (*values["repositories"], repo)
    values["github"]["repository_ids"] = (
        *values["github"]["repository_ids"],
        {"repository_id": repo_id, "github_id": 456},
    )
    second = dict(values["workers"][0])
    second.update(
        worker_id=uuid4(),
        auth_token=OTHER_TOKEN,
        allowed_repository_ids=frozenset({repo_id}),
    )
    values["workers"] = (*values["workers"], second)
    app = application(ControlSettings.model_validate(values), monkeypatch)

    with TestClient(app) as client:
        response = client.get(PATH, headers={"Authorization": f"Bearer {OTHER_TOKEN}"})

    assert response.status_code == 200
    assert response.json()["worker_id"] == str(second["worker_id"])
    assert response.json()["allowed_repository_ids"] == [str(repo_id)]
    assert TOKEN not in response.text


@pytest.mark.parametrize("path", ["/api/v1/tasks/import", "/api/v1/jobs"])
def test_worker_token_cannot_use_operator_apis(
    worker_configuration: ControlSettings,
    submission_configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
    path: str,
) -> None:
    values = worker_configuration.model_dump()
    values["task_import"] = TaskImportSettings(
        auth_token="test_only_import_token_0123456789abcdefghijklmnop"
    )
    values["job_submission"] = submission_configuration.job_submission
    app = application(ControlSettings.model_validate(values), monkeypatch)

    with TestClient(app) as client:
        response = client.post(path, headers={"Authorization": f"Bearer {TOKEN}"})

    assert response.status_code == 401
    assert TOKEN not in response.text


def test_partial_lifespan_failure_clears_worker_authentication(
    worker_configuration: ControlSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    values = worker_configuration.model_dump()
    values["task_import"] = TaskImportSettings(
        auth_token="test_only_import_token_0123456789abcdefghijklmnop"
    )

    @asynccontextmanager
    async def broken_import() -> AsyncIterator[ImportOperation]:
        raise RuntimeError("Startup failed.")
        yield

    app = create_app(
        ControlSettings.model_validate(values), import_resources=broken_import
    )

    with pytest.raises(RuntimeError, match="Startup failed"):
        with TestClient(app):
            pytest.fail("Startup must fail before serving requests.")

    assert app.state.worker_authentication_services is None


def application(
    configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
    *,
    now: Callable[[], datetime] = lambda: NOW,
) -> FastAPI:
    monkeypatch.setattr(
        control,
        "worker_authenticator",
        lambda values: worker_authenticator(values, now=now),
    )
    return create_app(configuration)


def test_worker_route_is_absent_by_default() -> None:
    app = create_app()

    with TestClient(app) as client:
        response = client.get(PATH)

    assert response.status_code == 404
    assert PATH not in app.openapi()["paths"]


def test_identity_returns_only_authenticated_scope(
    worker_configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    app = application(worker_configuration, monkeypatch)
    registration = worker_configuration.workers[0]

    with TestClient(app) as client:
        response = client.get(PATH, headers={"Authorization": f"Bearer {TOKEN}"})

    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "no-store"
    assert response.json() == {
        "worker_id": str(registration.worker_id),
        "allowed_repository_ids": [
            str(value) for value in sorted(registration.allowed_repository_ids)
        ],
        "credential_expires_at": "2026-10-10T01:00:00Z",
        "protocol_version": 1,
    }
    assert TOKEN not in response.text
    assert TOKEN not in caplog.text
    assert all(record.exc_info is None for record in caplog.records)
    assert app.state.worker_authentication_services is None


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"Authorization": "Basic whatever"},
        {"Authorization": "Bearer"},
        {"Authorization": "Bearer wrong"},
        {"Authorization": "Bearer " + "x" * 129},
        [("Authorization", f"Bearer {TOKEN}"), ("Authorization", f"Bearer {TOKEN}")],
        {"Cookie": f"token={TOKEN}"},
    ],
)
def test_invalid_or_ambiguous_credentials_have_one_safe_failure(
    worker_configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
    headers: dict[str, str] | list[tuple[str, str]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    app = application(worker_configuration, monkeypatch)

    with TestClient(app) as client:
        response = client.get(PATH, headers=headers)

    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"] == "Bearer"
    assert response.headers["Cache-Control"] == "no-store"
    assert response.json() == {
        "detail": {
            "code": "unauthorized",
            "message": "Valid worker authentication is required.",
        }
    }
    assert TOKEN not in response.text
    assert TOKEN not in caplog.text


@pytest.mark.parametrize("state", ["disabled", "expired", "expiry_boundary"])
def test_expired_and_disabled_registrations_cannot_authenticate(
    worker_configuration: ControlSettings, monkeypatch: pytest.MonkeyPatch, state: str
) -> None:
    values = worker_configuration.model_dump()
    if state == "disabled":
        values["workers"][0]["enabled"] = False
    else:
        values["workers"][0]["expires_at"] = NOW - timedelta(
            seconds=1 if state == "expired" else 0
        )
    app = application(ControlSettings.model_validate(values), monkeypatch)

    with TestClient(app) as client:
        response = client.get(PATH, headers={"Authorization": f"Bearer {TOKEN}"})

    assert response.status_code == 401


def test_expiry_is_not_cached_for_the_process_lifetime(
    worker_configuration: ControlSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = [NOW]
    app = application(worker_configuration, monkeypatch, now=lambda: clock[0])

    with TestClient(app) as client:
        before = client.get(PATH, headers={"Authorization": f"Bearer {TOKEN}"})
        clock[0] = worker_configuration.workers[0].expires_at
        after = client.get(PATH, headers={"Authorization": f"Bearer {TOKEN}"})

    assert before.status_code == 200
    assert after.status_code == 401


@pytest.mark.parametrize(
    "source", ["jira", "database", "github", "submission", "import"]
)
def test_control_credentials_do_not_authenticate_as_workers(
    worker_configuration: ControlSettings,
    submission_configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
    source: str,
) -> None:
    assert worker_configuration.jira is not None
    assert worker_configuration.database is not None
    assert worker_configuration.github is not None
    assert submission_configuration.job_submission is not None
    candidates = {
        "jira": worker_configuration.jira.api_token,
        "database": worker_configuration.database.password,
        "github": worker_configuration.github.api_token,
        "submission": submission_configuration.job_submission.auth_token,
    }
    token = (
        candidates[source].get_secret_value()
        if source != "import"
        else "test_only_import_token_0123456789abcdefghijklmnop"
    )
    app = application(worker_configuration, monkeypatch)

    with TestClient(app) as client:
        response = client.get(PATH, headers={"Authorization": f"Bearer {token}"})

    assert response.status_code == 401


def test_foreign_worker_id_in_request_cannot_select_another_identity(
    worker_configuration: ControlSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    values = worker_configuration.model_dump()
    second = dict(values["workers"][0])
    second.update(worker_id=uuid4(), auth_token=OTHER_TOKEN)
    values["workers"] = (values["workers"][0], second)
    app = application(ControlSettings.model_validate(values), monkeypatch)

    with TestClient(app) as client:
        response = client.request(
            "GET",
            PATH,
            headers={
                "Authorization": f"Bearer {TOKEN}",
                "X-Worker-ID": str(second["worker_id"]),
            },
            params={"worker_id": str(second["worker_id"])},
            json={"worker_id": str(second["worker_id"])},
        )

    assert response.status_code == 200
    assert response.json()["worker_id"] == str(
        worker_configuration.workers[0].worker_id
    )


def test_worker_identity_opens_no_database_or_provider_resources(
    worker_configuration: ControlSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("Worker identity must not open control integration resources.")

    monkeypatch.setattr(control, "open_database_engine", forbidden)
    monkeypatch.setattr(control, "open_control_import_operation", forbidden)
    monkeypatch.setattr(control, "open_control_submission_operation", forbidden)
    app = application(worker_configuration, monkeypatch)

    with TestClient(app) as client:
        response = client.get(PATH, headers={"Authorization": f"Bearer {TOKEN}"})

    assert response.status_code == 200
    assert app.state.worker_authentication_services is None


def test_bearer_identity_check_never_reads_request_body(
    worker_configuration: ControlSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = application(worker_configuration, monkeypatch)

    async def send_request() -> list[Message]:
        messages: list[Message] = []

        async def receive() -> Message:
            pytest.fail("Worker identity must not consume a request body.")

        async def send(message: Message) -> None:
            messages.append(message)

        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": PATH,
            "raw_path": PATH.encode(),
            "query_string": b"",
            "headers": [(b"authorization", f"Bearer {TOKEN}".encode())],
            "client": ("127.0.0.1", 1234),
            "server": ("test", 80),
        }
        await app(scope, receive, send)
        return messages

    with TestClient(app):
        messages = asyncio.run(send_request())

    assert messages[0]["status"] == 200


def test_missing_lifespan_authentication_service_is_unavailable(
    worker_configuration: ControlSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = application(worker_configuration, monkeypatch)

    with TestClient(app) as client:
        app.state.worker_authentication_services = None
        response = client.get(PATH, headers={"Authorization": f"Bearer {TOKEN}"})

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "worker_unavailable"


def test_unexpected_authentication_failure_uses_the_shared_boundary(
    worker_configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    app = application(worker_configuration, monkeypatch)

    def broken(token: str) -> WorkerAuthorization | None:
        raise RuntimeError(TOKEN)

    with TestClient(app) as client:
        app.state.worker_authentication_services = WorkerAuthenticationServices(
            authenticate=broken
        )
        response = client.get(PATH, headers={"Authorization": f"Bearer {TOKEN}"})

    assert response.status_code == 500
    assert response.json()["detail"]["code"] == "internal_error"
    assert TOKEN not in response.text
    assert TOKEN not in caplog.text
    assert all(record.exc_info is None for record in caplog.records)


def test_identity_openapi_documents_separate_worker_authentication(
    worker_configuration: ControlSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = application(worker_configuration, monkeypatch)

    schema = app.openapi()

    operation = schema["paths"][PATH]["get"]
    assert operation["security"] == [{"WorkerBearer": []}]
    assert "requestBody" not in operation
    assert {"200", "401", "503", "500"} <= operation["responses"].keys()


def test_production_identity_route_keeps_api_docs_disabled(
    worker_configuration: ControlSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    values = worker_configuration.model_dump()
    values["environment"] = RuntimeEnvironment.PRODUCTION
    app = application(ControlSettings.model_validate(values), monkeypatch)

    with TestClient(app) as client:
        response = client.get("/openapi.json")

    assert response.status_code == 404
