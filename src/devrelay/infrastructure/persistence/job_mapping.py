from devrelay.application.contracts.submissions import (
    CheckPolicy,
    PreparedJobSubmission,
    RepositoryPolicy,
    StoredJobSubmission,
)
from devrelay.application.contracts.tasks import StoredTaskSnapshot
from devrelay.application.jobs import (
    SubmissionState,
    decide_submission,
    recorded_submission,
)
from devrelay.application.ports.job_persistence import JobPersistenceError
from devrelay.application.tasks import TaskImportPolicy
from devrelay.domain.jobs.job import Job
from devrelay.domain.jobs.values import JobStatus
from devrelay.domain.tasks import TaskIdentity
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


def materialize_submission(
    record: JobRecord,
    snapshot: TaskSnapshotRecord,
    source: SourceTaskRecord,
    repository: RepositoryRecord,
    checks: tuple[JobCheckRecord, ...],
    binding: RepositoryBindingRecord | None,
) -> StoredJobSubmission:
    if (
        JobStatus(record.status) is not JobStatus.QUEUED
        or record.workflow_version != 0
        or not record.blocks_new_submission
        or record.definition_version not in (1, 2)
        or (
            record.definition_version == 1 and record.provider_repository_id is not None
        )
        or (
            record.definition_version == 2
            and (
                binding is None
                or record.provider_repository_id != binding.provider_repository_id
                or record.repository_id != binding.repository_id
            )
        )
        or record.submission_generation != 1
        or len(checks) != record.required_check_count
        or any(
            check.position != index or check.check_count != len(checks)
            for index, check in enumerate(checks)
        )
        or record.repository_slug.lower() != repository.slug
        or record.source_task_id != source.id
        or snapshot.source_task_id != source.id
    ):
        raise JobPersistenceError(
            "Stored submission definition failed integrity validation."
        )
    task = materialize_snapshot(
        snapshot,
        TaskIdentity(
            provider=source.source_provider,
            site_id=source.source_site_id,
            external_id=source.external_task_id,
        ),
    )
    policy = RepositoryPolicy(
        repository_id=repository.id,
        provider_repository_id=record.provider_repository_id,
        slug=record.repository_slug,
        enabled=True,
        target_branch=record.target_branch,
        coding_branch_prefix=record.coding_branch_prefix,
        version=record.policy_version,
        task_scope=TaskImportPolicy(
            provider=record.policy_source_provider,
            site_id=record.policy_source_site_id,
            allowed_scope_keys=frozenset(record.allowed_scope_keys),
            readiness_label=record.readiness_label,
        ),
        checks=tuple(
            CheckPolicy(
                id=check.check_id,
                argv=tuple(check.argv),
                working_directory=check.working_directory,
                timeout_seconds=check.timeout_seconds,
            )
            for check in checks
        ),
        max_repair_cycles=record.max_repair_cycles,
    )
    if (
        task.identity.provider != policy.task_scope.provider
        or task.identity.site_id != policy.task_scope.site_id
    ):
        raise JobPersistenceError(
            "Stored submission source policy failed integrity validation."
        )
    intent = PreparedJobSubmission(
        stored=StoredTaskSnapshot(source_task_id=source.id, snapshot=task),
        policy=policy,
        actor_id=record.actor_id,
    )
    if (
        intent.submission_key != record.submission_key
        or intent.submission_fingerprint != record.submission_fingerprint
    ):
        raise JobPersistenceError(
            "Stored submission fingerprint failed integrity validation."
        )
    job = Job(
        id=record.id,
        source_task_id=source.id,
        snapshot_id=task.id,
        repository_id=repository.id,
        authorized_branch=record.authorized_branch,
        target_branch=record.target_branch,
        base_revision=record.base_revision,
        policy_version=record.policy_version,
        required_check_ids=tuple(check.id for check in policy.checks),
        max_repair_cycles=record.max_repair_cycles,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )
    stored = StoredJobSubmission(intent=intent, job=job)
    decide_submission(intent, SubmissionState(existing=recorded_submission(stored)))
    return stored
