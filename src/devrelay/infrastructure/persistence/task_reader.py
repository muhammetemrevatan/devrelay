from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from devrelay.application.contracts.tasks import StoredTaskSnapshot
from devrelay.application.ports.task_persistence import (
    TaskPersistenceError,
    TaskSnapshotNotFound,
)
from devrelay.domain.tasks import TaskIdentity
from devrelay.infrastructure.persistence.models import (
    SourceTaskRecord,
    TaskSnapshotRecord,
)
from devrelay.infrastructure.persistence.task_mapping import materialize_snapshot


class SqlAlchemyTaskSnapshotReader:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def get_by_id(self, snapshot_id: UUID) -> StoredTaskSnapshot:
        try:
            async with self._sessions() as session:
                row = (
                    await session.execute(
                        select(TaskSnapshotRecord, SourceTaskRecord)
                        .join(
                            SourceTaskRecord,
                            TaskSnapshotRecord.source_task_id == SourceTaskRecord.id,
                        )
                        .where(TaskSnapshotRecord.id == snapshot_id)
                    )
                ).one_or_none()
                if row is None:
                    raise TaskSnapshotNotFound("Stored task snapshot was not found.")
                record, source = row
                return StoredTaskSnapshot(
                    source_task_id=source.id,
                    snapshot=materialize_snapshot(
                        record,
                        TaskIdentity(
                            provider=source.source_provider,
                            site_id=source.source_site_id,
                            external_id=source.external_task_id,
                        ),
                    ),
                )
        except SQLAlchemyError, ValueError, TypeError:
            raise TaskPersistenceError("Task snapshot read failed.") from None
