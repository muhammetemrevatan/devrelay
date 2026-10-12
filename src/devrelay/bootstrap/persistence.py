from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from devrelay.application.contracts.assignments import ClaimLimits
from devrelay.application.contracts.heartbeats import HeartbeatLimits
from devrelay.application.contracts.submissions import RepositoryPolicy
from devrelay.application.ports.acknowledgement_persistence import AcknowledgementStore
from devrelay.application.ports.claim_eligibility import QueuedJobReader
from devrelay.application.ports.claim_persistence import ClaimStore
from devrelay.application.ports.heartbeat_persistence import HeartbeatStore
from devrelay.application.ports.job_persistence import (
    JobBranchReader,
    JobSubmissionReader,
    JobSubmissionUnitOfWork,
)
from devrelay.application.ports.task_persistence import (
    TaskSnapshotReader,
    TaskUnitOfWork,
)
from devrelay.config.database import DatabaseSettings
from devrelay.infrastructure.persistence.claim_store import SqlAlchemyClaimStore
from devrelay.infrastructure.persistence.job_repository import (
    SqlAlchemyJobBranchReader,
    SqlAlchemyJobSubmissionReader,
)
from devrelay.infrastructure.persistence.job_unit_of_work import (
    SqlAlchemyJobSubmissionUnitOfWork,
)
from devrelay.infrastructure.persistence.queued_job_reader import (
    SqlAlchemyQueuedJobReader,
)
from devrelay.infrastructure.persistence.task_reader import SqlAlchemyTaskSnapshotReader
from devrelay.infrastructure.persistence.unit_of_work import SqlAlchemyTaskUnitOfWork


def database_url(settings: DatabaseSettings) -> URL:
    return URL.create(
        "postgresql+psycopg",
        username=settings.username,
        password=settings.password.get_secret_value(),
        host=settings.host,
        port=settings.port,
        database=settings.database,
    )


@asynccontextmanager
async def open_database_engine(
    settings: DatabaseSettings,
) -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(
        database_url(settings),
        pool_size=settings.pool_size,
        max_overflow=0,
        pool_timeout=settings.connect_timeout_seconds,
        pool_pre_ping=True,
        hide_parameters=True,
        isolation_level="READ COMMITTED",
        connect_args={
            "connect_timeout": settings.connect_timeout_seconds,
            "sslmode": settings.sslmode,
            "options": (
                f"-c statement_timeout={settings.statement_timeout_ms} "
                f"-c lock_timeout={settings.lock_timeout_ms} "
                f"-c search_path={settings.schema_name} -c timezone=UTC"
            ),
        },
    )
    try:
        yield engine
    finally:
        await engine.dispose()


def task_unit_of_work_factory(engine: AsyncEngine) -> Callable[[], TaskUnitOfWork]:
    sessions = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
    return lambda: SqlAlchemyTaskUnitOfWork(sessions)


def task_snapshot_reader(engine: AsyncEngine) -> TaskSnapshotReader:
    return SqlAlchemyTaskSnapshotReader(
        async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
    )


def job_submission_reader(engine: AsyncEngine) -> JobSubmissionReader:
    return SqlAlchemyJobSubmissionReader(
        async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
    )


def queued_job_reader(engine: AsyncEngine) -> QueuedJobReader:
    return SqlAlchemyQueuedJobReader(
        async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
    )


def claim_store(
    engine: AsyncEngine,
    policies: tuple[RepositoryPolicy, ...],
    limits: ClaimLimits | None = None,
) -> ClaimStore:
    return SqlAlchemyClaimStore(
        async_sessionmaker(engine, expire_on_commit=False, autoflush=False),
        policies,
        limits,
    )


def acknowledgement_store(
    engine: AsyncEngine, policies: tuple[RepositoryPolicy, ...]
) -> AcknowledgementStore:
    return SqlAlchemyClaimStore(
        async_sessionmaker(engine, expire_on_commit=False, autoflush=False), policies
    )


def heartbeat_store(
    engine: AsyncEngine,
    policies: tuple[RepositoryPolicy, ...],
    limits: HeartbeatLimits | None = None,
) -> HeartbeatStore:
    return SqlAlchemyClaimStore(
        async_sessionmaker(engine, expire_on_commit=False, autoflush=False),
        policies,
        heartbeat_limits=limits,
    )


def job_branch_reader(engine: AsyncEngine) -> JobBranchReader:
    return SqlAlchemyJobBranchReader(
        async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
    )


def job_submission_unit_of_work_factory(
    engine: AsyncEngine,
) -> Callable[[], JobSubmissionUnitOfWork]:
    sessions = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
    return lambda: SqlAlchemyJobSubmissionUnitOfWork(sessions)
