import os
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncEngine

from devrelay.bootstrap.persistence import open_database_engine
from devrelay.config.database import DatabaseSettings


@pytest.fixture(autouse=True)
def isolate_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in tuple(os.environ):
        if name.startswith(("DEVRELAY_CONTROL_", "DEVRELAY_WORKER_")):
            monkeypatch.delenv(name)


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
