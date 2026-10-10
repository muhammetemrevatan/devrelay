import os
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncEngine

from devrelay.bootstrap.persistence import open_database_engine
from devrelay.config.control import ControlSettings
from devrelay.config.database import DatabaseSettings
from devrelay.config.github import GitHubRepositoryId, GitHubSettings
from devrelay.config.jira import JiraSettings
from devrelay.config.job_submission import JobSubmissionSettings
from devrelay.config.repositories import CheckSettings, RepositorySettings


@pytest.fixture(autouse=True)
def isolate_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in tuple(os.environ):
        if name.startswith(("DEVRELAY_CONTROL_", "DEVRELAY_WORKER_")):
            monkeypatch.delenv(name)


@pytest.fixture
def submission_configuration() -> ControlSettings:
    repository_id = UUID("11111111-2222-3333-4444-555555555555")
    return ControlSettings(
        jira=JiraSettings(
            site_url="https://example.atlassian.net",
            cloud_id=UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
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
        github=GitHubSettings(
            api_token=SecretStr("test-only-github-token"),
            repository_ids=(
                GitHubRepositoryId(repository_id=repository_id, github_id=123),
            ),
        ),
        repositories=(
            RepositorySettings(
                repository_id=repository_id,
                slug="example/devrelay",
                enabled=True,
                target_branch="master",
                coding_branch_prefix="devrelay/jobs",
                policy_version="v1",
                allowed_project_keys=frozenset({"DRY"}),
                checks=(
                    CheckSettings(
                        id="pytest",
                        argv=("uv", "run", "--locked", "pytest"),
                        timeout_seconds=300,
                    ),
                ),
                max_repair_cycles=2,
            ),
        ),
        job_submission=JobSubmissionSettings(
            auth_token=SecretStr(
                "test_only_submission_token_0123456789abcdefghijklmnop"
            ),
            actor_id="test-operator",
            allowed_repository_ids=frozenset({repository_id}),
        ),
    )


@pytest.fixture
def database() -> Callable[[], AbstractAsyncContextManager[AsyncEngine]]:
    """Migrate/remove only generated schemas on an explicitly selected test DB."""
    port = os.environ.get("DEVRELAY_TEST_POSTGRES_PORT")
    if port is None:
        pytest.skip("Set DEVRELAY_TEST_POSTGRES_PORT for disposable PostgreSQL tests.")
    settings = DatabaseSettings(
        host="127.0.0.1",
        port=int(port),
        database="devrelay_test",
        username="devrelay_test",
        password=SecretStr("devrelay-test-only-password"),
        sslmode="disable",
    )

    def upgrade(connection: Connection) -> None:
        config = Config(str(Path(__file__).resolve().parents[1] / "alembic.ini"))
        config.attributes["connection"] = connection
        command.upgrade(config, "head")

    @asynccontextmanager
    async def fresh_database() -> AsyncIterator[AsyncEngine]:
        schema = "test_" + uuid4().hex
        async with open_database_engine(settings) as admin:
            async with admin.begin() as connection:
                identity = (
                    await connection.execute(
                        text("SELECT current_database(), current_user")
                    )
                ).one()
                assert tuple(identity) == ("devrelay_test", "devrelay_test")
                # Only this generated identifier is interpolated, never user text.
                await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            try:
                async with open_database_engine(
                    settings.model_copy(update={"schema_name": schema})
                ) as engine:
                    async with engine.begin() as connection:
                        await connection.run_sync(upgrade)
                    yield engine
            finally:
                async with admin.begin() as connection:
                    await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))

    return fresh_database
