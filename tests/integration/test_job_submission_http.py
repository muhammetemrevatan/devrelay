import asyncio
import json
import sys
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.pool import AsyncAdaptedQueuePool

from devrelay.api.dependencies import ImportOperation, SubmissionOperation
from devrelay.bootstrap.control import create_app
from devrelay.bootstrap.jobs import open_github_job_submitter
from devrelay.bootstrap.tasks import open_task_importer
from devrelay.config.control import ControlSettings
from devrelay.config.task_import import TaskImportSettings

pytestmark = pytest.mark.postgres
DatabaseFactory = Callable[[], AbstractAsyncContextManager[AsyncEngine]]
IMPORT_TOKEN = "test_only_import_token_0123456789abcdefghijklmnop"
SUBMISSION_TOKEN = "test_only_submission_token_0123456789abcdefghijklmnop"


def test_http_submission_persists_once_with_scoped_auth_and_offline_replay(
    database: DatabaseFactory, submission_configuration: ControlSettings
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            values = submission_configuration.model_dump()
            values["task_import"] = TaskImportSettings(
                auth_token=SecretStr(IMPORT_TOKEN)
            )
            configuration = ControlSettings.model_validate(values)
            assert configuration.jira is not None and configuration.github is not None
            jira, github = configuration.jira, configuration.github
            repository = configuration.repositories[0]
            payload = json.loads(
                (
                    Path(__file__).resolve().parents[1]
                    / "contract/fixtures/jira/issue.json"
                ).read_text()
            )
            payload["fields"]["labels"] = ["agent-ready"]
            calls: list[str] = []

            def provider_response(request: httpx.Request) -> httpx.Response:
                assert isinstance(engine.sync_engine.pool, AsyncAdaptedQueuePool)
                assert engine.sync_engine.pool.checkedout() == 0
                assert request.method == "GET"
                if request.url.host == "api.atlassian.com":
                    calls.append("jira")
                    return httpx.Response(200, json=payload)
                calls.append("github")
                if request.url.path.endswith("/pulls"):
                    return httpx.Response(200, json=[])
                if "/git/ref/" in request.url.path:
                    return httpx.Response(
                        200,
                        json={
                            "ref": "refs/heads/master",
                            "object": {"type": "commit", "sha": "a" * 40},
                        },
                    )
                return httpx.Response(
                    200,
                    json={
                        "id": 123,
                        "full_name": "example/devrelay",
                        "archived": False,
                        "disabled": False,
                    },
                )

            transport = httpx.MockTransport(provider_response)

            @asynccontextmanager
            async def import_resource() -> AsyncIterator[ImportOperation]:
                async with open_task_importer(
                    jira, engine, transport=transport
                ) as importer:
                    yield importer.execute

            @asynccontextmanager
            async def submission_resource() -> AsyncIterator[SubmissionOperation]:
                async with open_github_job_submitter(
                    jira,
                    github,
                    configuration.repositories,
                    engine,
                    jira_transport=transport,
                    github_transport=transport,
                ) as submitter:
                    yield submitter.execute

            application = create_app(
                configuration,
                import_resources=import_resource,
                submission_resources=submission_resource,
            )
            async with application.router.lifespan_context(application):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=application),
                    base_url="http://testserver",
                ) as client:
                    imported = await client.post(
                        "/api/v1/tasks/import",
                        headers={"Authorization": f"Bearer {IMPORT_TOKEN}"},
                        json={"task_key": "DRY-7"},
                    )
                    assert imported.status_code == 201
                    selection = {
                        "snapshot_id": imported.json()["snapshot_id"],
                        "repository_id": str(repository.repository_id),
                    }
                    calls.clear()
                    wrong_token = await client.post(
                        "/api/v1/jobs",
                        headers={"Authorization": f"Bearer {IMPORT_TOKEN}"},
                        json=selection,
                    )
                    headers = {"Authorization": f"Bearer {SUBMISSION_TOKEN}"}
                    unauthorized_repository = await client.post(
                        "/api/v1/jobs",
                        headers=headers,
                        json={**selection, "repository_id": str(uuid4())},
                    )
                    assert calls == []
                    first = await client.post(
                        "/api/v1/jobs", headers=headers, json=selection
                    )
                    observed_reads = tuple(calls)
                    payload["fields"]["labels"] = []
                    calls.clear()
                    replay = await client.post(
                        "/api/v1/jobs", headers=headers, json=selection
                    )

            assert wrong_token.status_code == 401
            assert unauthorized_repository.status_code == 403
            assert first.status_code == 201
            assert replay.status_code == 200
            assert first.json()["job_id"] == replay.json()["job_id"]
            assert first.json()["status"] == replay.json()["status"] == "queued"
            assert observed_reads == ("jira", "github", "github", "github", "github")
            assert calls == []
            async with engine.connect() as connection:
                assert (
                    await connection.execute(text("SELECT count(*) FROM jobs"))
                ).scalar_one() == 1
                assert (
                    await connection.execute(text("SELECT count(*) FROM job_checks"))
                ).scalar_one() == 1
                assert (
                    await connection.execute(text("SELECT actor_id FROM jobs"))
                ).scalar_one() == "test-operator"
            assert application.state.task_import_services is None
            assert application.state.job_submission_services is None

    asyncio.run(
        scenario(),
        loop_factory=asyncio.SelectorEventLoop if sys.platform == "win32" else None,
    )
