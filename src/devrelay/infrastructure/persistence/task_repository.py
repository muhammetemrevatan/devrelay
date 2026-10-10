from uuid import UUID, uuid4

from sqlalchemy import case, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from devrelay.application.contracts.tasks import SavedTaskSnapshot
from devrelay.application.ports.task_persistence import TaskPersistenceError
from devrelay.domain.tasks import TaskSnapshot
from devrelay.infrastructure.persistence.models import (
    SourceTaskRecord,
    TaskSnapshotRecord,
)
from devrelay.infrastructure.persistence.task_mapping import materialize_snapshot


class SqlAlchemyTaskRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save_snapshot(self, snapshot: TaskSnapshot) -> SavedTaskSnapshot:
        try:
            source = insert(SourceTaskRecord).values(
                id=uuid4(),
                source_provider=snapshot.identity.provider,
                source_site_id=snapshot.identity.site_id,
                external_task_id=snapshot.identity.external_id,
                display_key=snapshot.display_key,
                last_seen_at=snapshot.captured_at,
            )
            source_statement = source.on_conflict_do_update(
                constraint="uq_source_tasks_identity",
                set_={
                    "display_key": case(
                        (
                            source.excluded.last_seen_at
                            >= SourceTaskRecord.last_seen_at,
                            source.excluded.display_key,
                        ),
                        else_=SourceTaskRecord.display_key,
                    ),
                    "last_seen_at": func.greatest(
                        SourceTaskRecord.last_seen_at, source.excluded.last_seen_at
                    ),
                },
            ).returning(SourceTaskRecord.id)
            source_task_id: UUID = (
                await self._session.execute(source_statement)
            ).scalar_one()
            statement = (
                insert(TaskSnapshotRecord)
                .values(
                    id=snapshot.id,
                    source_task_id=source_task_id,
                    content_fingerprint=snapshot.content_fingerprint,
                    fingerprint_version=snapshot.fingerprint_version,
                    title=snapshot.title,
                    description=snapshot.description,
                    acceptance_criteria=list(snapshot.acceptance_criteria),
                    display_key=snapshot.display_key,
                    source_updated_at=snapshot.source_updated_at,
                    captured_at=snapshot.captured_at,
                )
                .on_conflict_do_nothing(constraint="uq_task_snapshots_content")
                .returning(TaskSnapshotRecord)
            )
            record: TaskSnapshotRecord | None = (
                await self._session.execute(statement)
            ).scalar_one_or_none()
            created = record is not None
            if record is None:
                record = (
                    await self._session.execute(
                        select(TaskSnapshotRecord).where(
                            TaskSnapshotRecord.source_task_id == source_task_id,
                            TaskSnapshotRecord.fingerprint_version
                            == snapshot.fingerprint_version,
                            TaskSnapshotRecord.content_fingerprint
                            == snapshot.content_fingerprint,
                        )
                    )
                ).scalar_one()
            stored = materialize_snapshot(record, snapshot.identity)
            return SavedTaskSnapshot(
                source_task_id=source_task_id, snapshot=stored, created=created
            )
        except SQLAlchemyError, ValueError, TypeError:
            raise TaskPersistenceError("Task snapshot storage failed.") from None
