import re
from uuid import UUID

from devrelay.application.contracts.heartbeats import (
    MAXIMUM_LEASE_REVISION,
    LeaseAuthorityObservation,
)
from devrelay.application.contracts.submissions import (
    CheckPolicy,
    PreparedJobSubmission,
    RepositoryPolicy,
    StoredJobSubmission,
)
from devrelay.application.contracts.tasks import StoredTaskSnapshot
from devrelay.application.contracts.worker_recovery import AssignmentReference
from devrelay.application.contracts.workflows import JobWorkflowState, WorkflowEventType
from devrelay.application.jobs import (
    SubmissionState,
    decide_submission,
    recorded_submission,
)
from devrelay.application.ports.job_persistence import JobPersistenceError
from devrelay.application.tasks import TaskImportPolicy
from devrelay.domain.jobs.job import Job
from devrelay.domain.jobs.values import AttemptStatus, JobStatus
from devrelay.domain.tasks import TaskIdentity
from devrelay.infrastructure.persistence.acknowledgement_models import (
    AcknowledgementReceiptRecord,
)
from devrelay.infrastructure.persistence.claim_models import ExecutionAttemptRecord
from devrelay.infrastructure.persistence.heartbeat_models import (
    AttemptLeaseAuthorityRecord,
    HeartbeatReceiptRecord,
)
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
from devrelay.infrastructure.persistence.workflow_models import (
    JobWorkflowEventRecord,
    JobWorkflowRecord,
)


def _validate_lease_authority(
    attempt: ExecutionAttemptRecord,
    authority: AttemptLeaseAuthorityRecord | None,
    heartbeat: HeartbeatReceiptRecord | None,
    *,
    repository_id: UUID,
    claimed: bool,
) -> None:
    """Validate joined current lease without rewriting historical assignment pins."""
    if authority is None:
        raise JobPersistenceError("Current assignment lease authority is missing.")
    ownership = (
        attempt.id,
        attempt.job_id,
        attempt.worker_id,
        attempt.claim_request_id,
        attempt.generation,
    )
    if (
        (
            authority.attempt_id,
            authority.job_id,
            authority.worker_id,
            authority.claim_request_id,
            authority.generation,
        )
        != ownership
        or type(authority.generation) is not int
        or type(authority.lease_revision) is not int
        or not 1 <= authority.lease_revision <= MAXIMUM_LEASE_REVISION
        or type(authority.receipt_count) is not int
        or not 0 <= authority.receipt_count <= MAXIMUM_LEASE_REVISION
        or authority.lease_revision > authority.receipt_count + 1
        or authority.revoked_at is not None
        or (claimed and (authority.lease_revision != 1 or authority.receipt_count != 0))
    ):
        raise JobPersistenceError("Current assignment lease ownership is inconsistent.")
    try:
        LeaseAuthorityObservation(
            reference=AssignmentReference(
                worker_id=attempt.worker_id,
                job_id=attempt.job_id,
                repository_id=repository_id,
                attempt_id=attempt.id,
                generation=attempt.generation,
                issued_at=attempt.issued_at,
                expires_at=attempt.expires_at,
            ),
            lease_revision=authority.lease_revision,
            observed_at=authority.updated_at,
            updated_at=authority.updated_at,
            current_expires_at=authority.current_expires_at,
            revoked_at=authority.revoked_at,
        )
        if authority.receipt_count == 0:
            if (
                heartbeat is not None
                or authority.lease_revision != 1
                or authority.next_heartbeat_at != attempt.issued_at
            ):
                raise ValueError("Initial lease has unsupported heartbeat history.")
            return
        if heartbeat is None:
            raise ValueError("Latest heartbeat history is missing.")
        if (
            (
                heartbeat.attempt_id,
                heartbeat.job_id,
                heartbeat.worker_id,
                heartbeat.claim_request_id,
                heartbeat.generation,
            )
            != ownership
            or heartbeat.repository_id != repository_id
            or type(heartbeat.protocol_version) is not int
            or heartbeat.protocol_version != 1
            or type(heartbeat.fingerprint_version) is not int
            or heartbeat.fingerprint_version != 1
            or type(heartbeat.renewed) is not bool
            or re.fullmatch(r"[0-9a-f]{64}", heartbeat.fingerprint) is None
            or heartbeat.attempt_ordinal != authority.receipt_count
            or heartbeat.lease_revision != authority.lease_revision
            or heartbeat.updated_at != authority.updated_at
            or heartbeat.current_expires_at != authority.current_expires_at
            or not heartbeat.updated_at
            <= heartbeat.recorded_at
            < heartbeat.current_expires_at
            or attempt.acknowledged_at is None
            or heartbeat.recorded_at < attempt.acknowledged_at
            or authority.next_heartbeat_at <= heartbeat.recorded_at
            or heartbeat.lease_revision
            != heartbeat.expected_lease_revision + int(heartbeat.renewed)
            or (heartbeat.renewed and heartbeat.updated_at != heartbeat.recorded_at)
            or any(
                type(value) is not int or not 1 <= value <= MAXIMUM_LEASE_REVISION
                for value in (
                    heartbeat.generation,
                    heartbeat.lease_revision,
                    heartbeat.expected_lease_revision,
                    heartbeat.attempt_ordinal,
                    heartbeat.worker_ordinal,
                    heartbeat.global_ordinal,
                )
            )
        ):
            raise ValueError("Latest heartbeat history is inconsistent.")
    except ValueError, TypeError:
        raise JobPersistenceError(
            "Current assignment lease history failed validation."
        ) from None


def materialize_submission(
    record: JobRecord,
    snapshot: TaskSnapshotRecord,
    source: SourceTaskRecord,
    repository: RepositoryRecord,
    checks: tuple[JobCheckRecord, ...],
    binding: RepositoryBindingRecord | None,
    workflow: JobWorkflowRecord | None,
    workflow_event: JobWorkflowEventRecord | None,
    submitted: JobWorkflowEventRecord | None,
    attempt: ExecutionAttemptRecord | None,
    claimed_event: JobWorkflowEventRecord | None,
    acknowledgement: AcknowledgementReceiptRecord | None,
    lease_authority: AttemptLeaseAuthorityRecord | None,
    heartbeat: HeartbeatReceiptRecord | None,
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
    if (
        workflow is None
        or workflow_event is None
        or submitted is None
        or submitted.job_id != record.id
        or submitted.workflow_version != 0
        or submitted.event_type != WorkflowEventType.SUBMITTED.value
        or submitted.status != JobStatus.QUEUED.value
        or submitted.occurred_at != record.created_at
        or (workflow.job_id, workflow.source_task_id, workflow.repository_id)
        != (record.id, record.source_task_id, record.repository_id)
        or not workflow.blocks_new_submission
        or workflow.created_at != record.created_at
        or workflow.updated_at < record.created_at
        or workflow_event.job_id != workflow.job_id
        or workflow_event.workflow_version != workflow.workflow_version
        or workflow_event.status != workflow.status
        or workflow_event.occurred_at != workflow.updated_at
    ):
        raise JobPersistenceError(
            "Stored workflow/history failed integrity validation."
        )
    queued = (
        workflow.status == JobStatus.QUEUED.value
        and workflow.workflow_version == 0
        and workflow.updated_at == record.created_at
        and workflow.active_attempt_id is None
        and workflow.last_attempt_id is None
        and workflow.last_attempt_number == 0
        and workflow_event.event_type == WorkflowEventType.SUBMITTED.value
        and workflow_event.attempt_id is None
        and workflow_event.previous_version is None
        and workflow_event.previous_status is None
        and attempt is None
    )
    claimed = (
        workflow.status == JobStatus.CLAIMED.value
        and workflow.workflow_version == 1
        and workflow.active_attempt_id is not None
        and workflow.last_attempt_id == workflow.active_attempt_id
        and workflow.last_attempt_number == 1
        and workflow_event.event_type == WorkflowEventType.CLAIMED.value
        and workflow_event.attempt_id == workflow.active_attempt_id
        and workflow_event.previous_version == 0
        and workflow_event.previous_status == JobStatus.QUEUED.value
        and attempt is not None
        and attempt.id == workflow.active_attempt_id
        and attempt.job_id == record.id
        and attempt.attempt_number == workflow.last_attempt_number
        and attempt.generation == 1
        and attempt.status == AttemptStatus.CLAIMED.value
        and attempt.issued_at == workflow.updated_at
        and attempt.updated_at == attempt.issued_at
        and attempt.expires_at > attempt.issued_at
        and attempt.acknowledged_at is None
        and attempt.revoked_at is None
        and attempt.ended_at is None
    )
    running = (
        workflow.status == JobStatus.RUNNING.value
        and workflow.workflow_version == 2
        and workflow.active_attempt_id is not None
        and workflow.last_attempt_id == workflow.active_attempt_id
        and workflow.last_attempt_number == 1
        and workflow_event.event_type == WorkflowEventType.ACKNOWLEDGED.value
        and workflow_event.attempt_id == workflow.active_attempt_id
        and workflow_event.previous_version == 1
        and workflow_event.previous_status == JobStatus.CLAIMED.value
        and attempt is not None
        and attempt.id == workflow.active_attempt_id
        and attempt.job_id == record.id
        and attempt.attempt_number == 1
        and attempt.generation == 1
        and attempt.status == AttemptStatus.RUNNING.value
        and attempt.acknowledged_at == workflow.updated_at
        and attempt.updated_at == workflow.updated_at
        and attempt.issued_at <= workflow.updated_at < attempt.expires_at
        and attempt.revoked_at is None
        and attempt.ended_at is None
        and claimed_event is not None
        and claimed_event.status == JobStatus.CLAIMED.value
        and claimed_event.event_type == WorkflowEventType.CLAIMED.value
        and claimed_event.workflow_version == 1
        and claimed_event.job_id == record.id
        and claimed_event.attempt_id == attempt.id
        and claimed_event.occurred_at == attempt.issued_at
        and claimed_event.previous_version == 0
        and claimed_event.previous_status == JobStatus.QUEUED.value
        and acknowledgement is not None
        and acknowledgement.attempt_id == attempt.id
        and acknowledgement.job_id == record.id
        and acknowledgement.worker_id == attempt.worker_id
        and acknowledgement.claim_request_id == attempt.claim_request_id
        and acknowledgement.generation == 1
        and acknowledgement.repository_id == record.repository_id
        and acknowledgement.snapshot_id == record.snapshot_id
        and acknowledgement.base_revision == record.base_revision
        and acknowledgement.acknowledged_at == attempt.acknowledged_at
        and acknowledgement.workflow_version == 2
        and acknowledgement.expected_workflow_version == 1
        and acknowledgement.protocol_version == 1
        and acknowledgement.definition_digest_version == 1
        and acknowledgement.fingerprint_version == 1
    )
    if (queued or claimed) and acknowledgement is not None:
        raise JobPersistenceError(
            "Unacknowledged workflow contains acknowledgement history."
        )
    if not (queued or claimed or running):
        raise JobPersistenceError("Stored workflow transition is unsupported.")
    if queued:
        if lease_authority is not None or heartbeat is not None:
            raise JobPersistenceError(
                "Queued workflow contains lease authority history."
            )
    elif attempt is not None:
        _validate_lease_authority(
            attempt,
            lease_authority,
            heartbeat,
            repository_id=record.repository_id,
            claimed=claimed,
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
    state = JobWorkflowState(
        job_id=workflow.job_id,
        status=JobStatus(workflow.status),
        version=workflow.workflow_version,
        blocks_new_submission=workflow.blocks_new_submission,
        created_at=workflow.created_at,
        updated_at=workflow.updated_at,
    )
    stored = StoredJobSubmission(intent=intent, job=job, workflow=state)
    decide_submission(intent, SubmissionState(existing=recorded_submission(stored)))
    return stored
