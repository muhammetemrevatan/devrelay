from uuid import UUID

from sqlalchemy import Select, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from devrelay.application.contracts.submissions import StoredJobSubmission
from devrelay.application.jobs import JobSubmissionConflict
from devrelay.application.ports.job_persistence import JobPersistenceError
from devrelay.application.ports.task_persistence import TaskPersistenceError
from devrelay.domain.jobs.values import JobStatus
from devrelay.infrastructure.persistence.job_models import JobRecord
from devrelay.infrastructure.persistence.job_repository import read_submission
from devrelay.infrastructure.persistence.workflow_models import JobWorkflowRecord


def queued_candidate_query(repository_ids: frozenset[UUID]) -> Select[str]:
    return (
        select(JobRecord.submission_key)
        .join(JobWorkflowRecord, JobWorkflowRecord.job_id == JobRecord.id)
        .where(
            JobWorkflowRecord.repository_id.in_(repository_ids),
            JobWorkflowRecord.status == JobStatus.QUEUED.value,
            JobWorkflowRecord.workflow_version == 0,
            JobWorkflowRecord.blocks_new_submission.is_(True),
            JobRecord.definition_version == 2,
            JobRecord.provider_repository_id.is_not(None),
        )
        .order_by(JobWorkflowRecord.created_at, JobWorkflowRecord.job_id)
        .limit(1)
    )


class SqlAlchemyQueuedJobReader:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def find_candidate(
        self, *, repository_ids: frozenset[UUID]
    ) -> StoredJobSubmission | None:
        if (
            not isinstance(repository_ids, frozenset)
            or not 1 <= len(repository_ids) <= 32
            or any(not isinstance(value, UUID) for value in repository_ids)
        ):
            raise JobPersistenceError("Queue read requires bounded repository scope.")
        try:
            async with self._sessions() as session:
                # One read snapshot spans selection, definition and ordered checks.
                await session.connection(
                    execution_options={"isolation_level": "REPEATABLE READ"}
                )
                key = (
                    await session.scalars(queued_candidate_query(repository_ids))
                ).one_or_none()
                if key is None:
                    return None
                result = await read_submission(session, key)
                if result is None:
                    raise JobPersistenceError("Selected Job definition is unavailable.")
                return result
        except (
            SQLAlchemyError,
            ValueError,
            TypeError,
            JobSubmissionConflict,
            TaskPersistenceError,
        ):
            raise JobPersistenceError("Queued Job read failed.") from None
