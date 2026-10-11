"""Alembic driver using a bootstrap-owned engine or an injected test connection."""

import asyncio
import sys
from pathlib import Path

from alembic import context
from sqlalchemy.engine import Connection

from devrelay.bootstrap.persistence import open_database_engine
from devrelay.config.control import ControlSettings
from devrelay.infrastructure.persistence.claim_models import ClaimReceiptRecord
from devrelay.infrastructure.persistence.job_models import JobRecord
from devrelay.infrastructure.persistence.workflow_models import JobWorkflowRecord

assert JobWorkflowRecord.metadata is JobRecord.metadata
assert ClaimReceiptRecord.metadata is JobRecord.metadata


def migrate(connection: Connection) -> None:
    context.configure(
        connection=connection, target_metadata=JobRecord.metadata, compare_type=True
    )
    with context.begin_transaction():
        context.run_migrations()


async def migrate_online() -> None:
    env_file = context.get_x_argument(as_dictionary=True).get("env_file")
    settings = ControlSettings(_env_file=Path(env_file) if env_file else None).database
    if settings is None:
        raise RuntimeError("Database settings are required for an explicit migration.")
    async with open_database_engine(settings) as engine:
        async with engine.begin() as connection:
            await connection.run_sync(migrate)


if context.is_offline_mode():
    context.configure(
        dialect_name="postgresql",
        literal_binds=True,
        target_metadata=JobRecord.metadata,
    )
    with context.begin_transaction():
        context.run_migrations()
else:
    supplied = context.config.attributes.get("connection")
    if supplied is not None:
        if not isinstance(supplied, Connection):
            raise TypeError("Migration connection must be a SQLAlchemy connection.")
        migrate(supplied)
    else:
        asyncio.run(
            migrate_online(),
            loop_factory=asyncio.SelectorEventLoop if sys.platform == "win32" else None,
        )
