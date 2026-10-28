from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.ext.asyncio import async_sessionmaker

from devrelay.application.accepted_coding_results import DispatchAcceptedCodingResult
from devrelay.application.contracts.coding_result_receipts import (
    CodingResultStatus,
    TerminalCodingResult,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.bootstrap.claims import _claim_configuration
from devrelay.bootstrap.coding_publication import open_coding_publisher
from devrelay.bootstrap.persistence import coding_result_store, open_database_engine
from devrelay.config.control import ControlSettings
from devrelay.infrastructure.persistence.accepted_result_reader import (
    SqlAlchemyAcceptedCodingResultReader,
)


def publication_authorization(
    settings: ControlSettings, worker_id: UUID, *, now: datetime
) -> WorkerAuthorization:
    """Scope an explicit trusted control command to one current enrollment."""
    if (
        not isinstance(worker_id, UUID)
        or not isinstance(now, datetime)
        or now.utcoffset() is None
        or settings.database is None
        or settings.worker_claims is None
        or settings.worker_coding_results is None
        or settings.coding_publication is None
    ):
        raise ValueError("Publication requires explicit current broker configuration.")
    registration = next(
        (item for item in settings.workers if item.worker_id == worker_id), None
    )
    if (
        registration is None
        or not registration.enabled
        or now >= registration.expires_at
    ):
        raise ValueError("Publication requires a current enabled worker enrollment.")
    enabled = frozenset(
        item.repository_id for item in settings.repositories if item.enabled
    )
    allowed = (
        registration.allowed_repository_ids
        & settings.coding_publication.allowed_repository_ids
        & enabled
    )
    if not allowed:
        raise ValueError("Publication requires an enabled shared repository scope.")
    return WorkerAuthorization(
        worker_id=registration.worker_id,
        allowed_repository_ids=allowed,
        credential_expires_at=registration.expires_at,
    )


@asynccontextmanager
async def _accepted_result_dispatch(
    settings: ControlSettings,
    result_id: UUID,
    worker_id: UUID,
    *,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> AsyncIterator[tuple[DispatchAcceptedCodingResult, WorkerAuthorization]]:
    if not isinstance(result_id, UUID):
        raise ValueError("Publication requires a known accepted result UUID.")
    authorization = publication_authorization(settings, worker_id, now=now())
    policies, _ = _claim_configuration(settings)
    policies = tuple(
        item
        for item in policies
        if item.repository_id in authorization.allowed_repository_ids
    )
    assert settings.database is not None
    async with open_database_engine(settings.database) as engine:
        store = coding_result_store(engine, policies)
        reader = SqlAlchemyAcceptedCodingResultReader(
            async_sessionmaker(engine, expire_on_commit=False, autoflush=False),
            policies,
            store,
        )
        async with open_coding_publisher(settings, engine) as publisher:
            # Opening resources can await. Recheck expiry before loading evidence;
            # the store independently rechecks DB-time scope at every operation.
            current = publication_authorization(settings, worker_id, now=now())
            yield DispatchAcceptedCodingResult(reader, publisher), current


async def publish_accepted_result(
    settings: ControlSettings,
    result_id: UUID,
    worker_id: UUID,
    *,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> TerminalCodingResult:
    """Publish one accepted UUID; opening the ordinary server never calls this."""
    async with _accepted_result_dispatch(settings, result_id, worker_id, now=now) as (
        dispatcher,
        authorization,
    ):
        return await dispatcher.execute(result_id, authorization)


async def reconcile_accepted_result(
    settings: ControlSettings,
    result_id: UUID,
    worker_id: UUID,
    *,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> CodingResultStatus | None:
    """Observe retained provider effects without another publication write."""
    async with _accepted_result_dispatch(settings, result_id, worker_id, now=now) as (
        dispatcher,
        authorization,
    ):
        return await dispatcher.reconcile(result_id, authorization)
