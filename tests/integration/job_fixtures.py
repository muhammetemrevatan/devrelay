from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncEngine

from devrelay.application.contracts.submissions import (
    CheckPolicy,
    PreparedJobSubmission,
    RepositoryHead,
    RepositoryPolicy,
    StoredJobSubmission,
)
from devrelay.application.contracts.tasks import StoredTaskSnapshot
from devrelay.application.tasks import TaskImportPolicy
from devrelay.bootstrap.persistence import (
    job_submission_unit_of_work_factory,
    task_unit_of_work_factory,
)
from devrelay.domain.tasks import TaskIdentity, TaskSnapshot


async def save_job(
    engine: AsyncEngine,
    *,
    check_count: int = 1,
    repository: RepositoryPolicy | None = None,
    created_at: datetime | None = None,
    job_id: UUID | None = None,
) -> StoredJobSubmission:
    now = created_at or datetime.now(UTC)
    task = TaskSnapshot(
        id=uuid4(),
        identity=TaskIdentity(
            provider="jira", site_id="test-site", external_id=uuid4().hex
        ),
        display_key="DRY-5",
        title="Implement a feature",
        description="Keep requirements stable.",
        acceptance_criteria=("Pass configured checks.",),
        captured_at=now,
    )
    async with task_unit_of_work_factory(engine)() as unit:
        saved = await unit.tasks.save_snapshot(task)
        await unit.commit()
    policy = repository or RepositoryPolicy(
        repository_id=uuid4(),
        provider_repository_id=123,
        slug="example/devrelay",
        enabled=True,
        target_branch="master",
        coding_branch_prefix="devrelay/jobs",
        version="v1",
        task_scope=TaskImportPolicy(
            provider="jira", site_id="test-site", allowed_scope_keys=frozenset({"DRY"})
        ),
        checks=tuple(
            CheckPolicy(
                id=f"check_{index}",
                argv=("pytest",),
                working_directory=".",
                timeout_seconds=300,
            )
            for index in range(check_count)
        ),
        max_repair_cycles=2,
    )
    intent = PreparedJobSubmission(
        stored=StoredTaskSnapshot(
            source_task_id=saved.source_task_id, snapshot=saved.snapshot
        ),
        policy=policy,
        actor_id="test-operator",
    )
    assert policy.provider_repository_id is not None
    candidate = intent.build_candidate(
        job_id=job_id or uuid4(),
        head=RepositoryHead(
            repository_id=policy.repository_id,
            provider_repository_id=policy.provider_repository_id,
            target_branch=policy.target_branch,
            revision="a" * 40,
        ),
        now=now,
    )
    async with job_submission_unit_of_work_factory(engine)() as unit:
        result = await unit.jobs.admit(intent, candidate)
        await unit.commit()
    return result.stored
