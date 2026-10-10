from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from devrelay.application.contracts.submissions import (
    JobBranchAssociation,
    PreparedJobSubmission,
    RepositoryHead,
    SavedJobSubmission,
    StoredJobSubmission,
)
from devrelay.application.jobs import (
    JobSubmissionConflict,
    SubmissionState,
    decide_submission,
    recorded_submission,
)
from devrelay.application.ports.job_persistence import JobPersistenceError
from devrelay.application.ports.task_persistence import TaskPersistenceError
from devrelay.domain.jobs.job import Job
from devrelay.infrastructure.persistence.job_mapping import materialize_submission
from devrelay.infrastructure.persistence.job_models import (
    JobCheckRecord,
    JobRecord,
    RepositoryBindingRecord,
    RepositoryRecord,
)
from devrelay.infrastructure.persistence.models import (
    SourceTaskRecord,
    TaskSnapshotRecord,
)
from devrelay.infrastructure.persistence.task_mapping import materialize_snapshot


async def read_submission(
    session: AsyncSession,
    submission_key: str,
) -> StoredJobSubmission | None:
    row = (
        await session.execute(
            select(
                JobRecord,
                TaskSnapshotRecord,
                SourceTaskRecord,
                RepositoryRecord,
                RepositoryBindingRecord,
            )
            .join(TaskSnapshotRecord, JobRecord.snapshot_id == TaskSnapshotRecord.id)
            .join(SourceTaskRecord, JobRecord.source_task_id == SourceTaskRecord.id)
            .join(RepositoryRecord, JobRecord.repository_id == RepositoryRecord.id)
            .outerjoin(
                RepositoryBindingRecord,
                JobRecord.repository_id == RepositoryBindingRecord.repository_id,
            )
            .where(JobRecord.submission_key == submission_key)
        )
    ).one_or_none()
    if row is None:
        return None
    record, snapshot, source, repository, binding = row
    checks = tuple(
        (
            await session.scalars(
                select(JobCheckRecord)
                .where(JobCheckRecord.job_id == record.id)
                .order_by(JobCheckRecord.position)
                .limit(33)
            )
        ).all()
    )
    return materialize_submission(record, snapshot, source, repository, checks, binding)


class SqlAlchemyJobSubmissionReader:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def find_by_key(self, submission_key: str) -> StoredJobSubmission | None:
        try:
            async with self._sessions() as session:
                return await read_submission(session, submission_key)
        except (
            SQLAlchemyError,
            ValueError,
            TypeError,
            JobSubmissionConflict,
            TaskPersistenceError,
        ):
            raise JobPersistenceError("Job submission read failed.") from None


class SqlAlchemyJobBranchReader:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def find_by_ids(
        self, *, repository_id: UUID, job_ids: tuple[UUID, ...]
    ) -> tuple[JobBranchAssociation, ...]:
        if (
            not isinstance(repository_id, UUID)
            or not isinstance(job_ids, tuple)
            or len(job_ids) > 100
            or any(not isinstance(item, UUID) for item in job_ids)
            or len(set(job_ids)) != len(job_ids)
        ):
            raise JobPersistenceError(
                "Job branch lookup requires bounded unique identities."
            )
        if not job_ids:
            return ()
        try:
            async with self._sessions() as session:
                rows = (
                    await session.execute(
                        select(
                            JobRecord.id,
                            JobRecord.repository_id,
                            JobRecord.provider_repository_id,
                            JobRecord.source_task_id,
                            JobRecord.authorized_branch,
                            JobRecord.coding_branch_prefix,
                        )
                        .where(
                            JobRecord.repository_id == repository_id,
                            JobRecord.id.in_(job_ids),
                        )
                        .order_by(JobRecord.id)
                        .limit(100)
                    )
                ).all()
                result: list[JobBranchAssociation] = []
                for job_id, repo_id, provider_id, source_id, branch, prefix in rows:
                    if branch != f"{prefix}/{job_id.hex}":
                        raise JobPersistenceError(
                            "Stored Job branch binding is inconsistent."
                        )
                    result.append(
                        JobBranchAssociation(
                            job_id=job_id,
                            repository_id=repo_id,
                            provider_repository_id=provider_id,
                            source_task_id=source_id,
                            authorized_branch=branch,
                        )
                    )
                return tuple(result)
        except SQLAlchemyError, ValueError, TypeError:
            raise JobPersistenceError("Job branch association read failed.") from None


class SqlAlchemyJobSubmissionRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def admit(
        self, intent: PreparedJobSubmission, candidate: Job
    ) -> SavedJobSubmission:
        try:
            if intent.policy.provider_repository_id is None:
                raise JobSubmissionConflict(
                    "New submissions require a provider binding."
                )
            expected = intent.build_candidate(
                job_id=candidate.id,
                head=RepositoryHead(
                    repository_id=candidate.repository_id,
                    provider_repository_id=intent.policy.provider_repository_id,
                    target_branch=candidate.target_branch,
                    revision=candidate.base_revision,
                ),
                now=candidate.created_at,
            )
            if candidate != expected or not intent.policy.enabled:
                raise JobSubmissionConflict(
                    "Candidate does not match authorized intent."
                )
            # One task row serializes competing definitions across snapshots/repos.
            # No provider request is allowed between this lock and commit.
            source_row = (
                await self._session.execute(
                    select(SourceTaskRecord, TaskSnapshotRecord)
                    .join(
                        TaskSnapshotRecord,
                        TaskSnapshotRecord.source_task_id == SourceTaskRecord.id,
                    )
                    .where(
                        SourceTaskRecord.id == intent.stored.source_task_id,
                        TaskSnapshotRecord.id == intent.stored.snapshot.id,
                    )
                    .with_for_update(of=SourceTaskRecord)
                )
            ).one_or_none()
            if source_row is None:
                raise JobSubmissionConflict(
                    "Submission snapshot/source binding does not match storage."
                )
            source, snapshot = source_row
            identity = intent.stored.snapshot.identity
            if (
                source.source_provider,
                source.source_site_id,
                source.external_task_id,
            ) != (identity.provider, identity.site_id, identity.external_id):
                raise JobSubmissionConflict(
                    "Submission source identity does not match storage."
                )
            if materialize_snapshot(snapshot, identity) != intent.stored.snapshot:
                raise JobSubmissionConflict(
                    "Submission content does not match the stored snapshot."
                )
            existing = await read_submission(self._session, intent.submission_key)
            if existing is not None:
                decide_submission(
                    intent, SubmissionState(existing=recorded_submission(existing))
                )
                return SavedJobSubmission(stored=existing, created=False)
            blocking: UUID | None = (
                await self._session.execute(
                    select(JobRecord.id)
                    .where(
                        JobRecord.source_task_id == source.id,
                        JobRecord.repository_id == intent.policy.repository_id,
                        JobRecord.blocks_new_submission.is_(True),
                    )
                    .limit(1)
                )
            ).scalar_one_or_none()
            if blocking is not None:
                raise JobSubmissionConflict("Task already has an unresolved delivery.")
            await self._session.execute(
                insert(RepositoryRecord)
                .values(
                    id=intent.policy.repository_id,
                    provider="github",
                    slug=intent.policy.slug.lower(),
                )
                .on_conflict_do_nothing(index_elements=[RepositoryRecord.id])
            )
            repository = (
                await self._session.scalars(
                    select(RepositoryRecord).where(
                        RepositoryRecord.id == intent.policy.repository_id,
                    )
                )
            ).one()
            if repository.slug != intent.policy.slug.lower():
                raise JobSubmissionConflict(
                    "Repository identity is already bound to another slug."
                )
            policy = intent.policy
            await self._session.execute(
                insert(RepositoryBindingRecord)
                .values(
                    repository_id=policy.repository_id,
                    provider_repository_id=policy.provider_repository_id,
                    bound_at=candidate.created_at,
                )
                .on_conflict_do_nothing(
                    index_elements=[RepositoryBindingRecord.repository_id]
                )
            )
            await self._session.execute(
                insert(JobRecord).values(
                    id=candidate.id,
                    source_task_id=candidate.source_task_id,
                    snapshot_id=candidate.snapshot_id,
                    repository_id=candidate.repository_id,
                    provider_repository_id=policy.provider_repository_id,
                    submission_key=intent.submission_key,
                    submission_fingerprint=intent.submission_fingerprint,
                    submission_generation=1,
                    definition_version=2,
                    status=candidate.status.value,
                    workflow_version=0,
                    blocks_new_submission=True,
                    repository_slug=policy.slug,
                    target_branch=candidate.target_branch,
                    authorized_branch=candidate.authorized_branch,
                    coding_branch_prefix=policy.coding_branch_prefix,
                    base_revision=candidate.base_revision,
                    policy_version=policy.version,
                    policy_source_provider=policy.task_scope.provider,
                    policy_source_site_id=policy.task_scope.site_id,
                    allowed_scope_keys=sorted(policy.task_scope.allowed_scope_keys),
                    readiness_label=policy.task_scope.readiness_label,
                    max_repair_cycles=policy.max_repair_cycles,
                    required_check_count=len(policy.checks),
                    actor_id=intent.actor_id,
                    created_at=candidate.created_at,
                    updated_at=candidate.updated_at,
                )
            )
            # One bounded executemany insert, never one statement/commit per check.
            await self._session.execute(
                insert(JobCheckRecord),
                [
                    {
                        "job_id": candidate.id,
                        "check_id": check.id,
                        "check_count": len(policy.checks),
                        "position": index,
                        "argv": list(check.argv),
                        "working_directory": check.working_directory,
                        "timeout_seconds": check.timeout_seconds,
                    }
                    for index, check in enumerate(policy.checks)
                ],
            )
            return SavedJobSubmission(
                stored=StoredJobSubmission(intent=intent, job=candidate), created=True
            )
        except IntegrityError:
            raise JobSubmissionConflict(
                "Submission conflicts with stored identity or policy constraints."
            ) from None
        except SQLAlchemyError, ValueError, TypeError, TaskPersistenceError:
            raise JobPersistenceError("Job submission storage failed.") from None
