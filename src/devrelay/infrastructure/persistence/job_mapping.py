import re
from datetime import datetime
from typing import NamedTuple
from uuid import UUID

from devrelay.application.contracts.assignment_delivery import AssignedJobDefinition
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
from devrelay.infrastructure.persistence.result_models import (
    DeliveryOperationEventRecord,
)
from devrelay.infrastructure.persistence.task_mapping import materialize_snapshot
from devrelay.infrastructure.persistence.workflow_models import (
    JobWorkflowEventRecord,
    JobWorkflowRecord,
)


class AcceptedResultHistory(NamedTuple):
    """Scalar historical pins only; generic readers never load result payloads."""

    id: UUID
    request_id: UUID
    worker_id: UUID
    attempt_id: UUID
    job_id: UUID
    claim_request_id: UUID
    repository_id: UUID
    snapshot_id: UUID
    execution_id: UUID
    generation: int
    definition_digest: str
    claim_fingerprint: str
    fingerprint: str
    protocol_version: int
    fingerprint_version: int
    expected_workflow_version: int
    publishing_workflow_version: int
    candidate_revision: str
    repair_cycles: int
    recorded_at: datetime
    payload_sha256: str


class DeliveryOperationHistory(NamedTuple):
    result_id: UUID
    version: int
    state: str
    invocation_id: UUID | None
    event_count: int
    pr_number: int | None
    pr_repository_id: UUID | None
    pr_provider_repository_id: int | None
    pr_branch: str | None
    pr_target_branch: str | None
    pr_head_revision: str | None
    completed_at: datetime | None
    final_workflow_version: int | None
    release_ordinal: int | None


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
        or authority.revoked_at != attempt.revoked_at
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
            observed_at=authority.revoked_at or authority.updated_at,
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
            or (
                authority.revoked_at is not None
                and heartbeat.recorded_at > authority.revoked_at
            )
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


def _acknowledged_history(
    record: JobRecord,
    attempt: ExecutionAttemptRecord | None,
    claimed: JobWorkflowEventRecord | None,
    acknowledged: JobWorkflowEventRecord | None,
    receipt: AcknowledgementReceiptRecord | None,
) -> bool:
    if attempt is None or claimed is None or acknowledged is None or receipt is None:
        return False
    return (
        attempt.job_id == record.id
        and attempt.attempt_number == attempt.generation == 1
        and record.created_at <= attempt.issued_at
        and attempt.acknowledged_at is not None
        and attempt.issued_at <= attempt.acknowledged_at < attempt.expires_at
        and attempt.updated_at >= attempt.acknowledged_at
        and (
            claimed.job_id,
            claimed.attempt_id,
            claimed.workflow_version,
            claimed.event_type,
            claimed.status,
            claimed.previous_version,
            claimed.previous_status,
            claimed.occurred_at,
        )
        == (
            record.id,
            attempt.id,
            1,
            WorkflowEventType.CLAIMED.value,
            JobStatus.CLAIMED.value,
            0,
            JobStatus.QUEUED.value,
            attempt.issued_at,
        )
        and (
            acknowledged.job_id,
            acknowledged.attempt_id,
            acknowledged.workflow_version,
            acknowledged.event_type,
            acknowledged.status,
            acknowledged.previous_version,
            acknowledged.previous_status,
            acknowledged.occurred_at,
        )
        == (
            record.id,
            attempt.id,
            2,
            WorkflowEventType.ACKNOWLEDGED.value,
            JobStatus.RUNNING.value,
            1,
            JobStatus.CLAIMED.value,
            attempt.acknowledged_at,
        )
        and (
            receipt.attempt_id,
            receipt.job_id,
            receipt.worker_id,
            receipt.claim_request_id,
            receipt.generation,
            receipt.repository_id,
            receipt.snapshot_id,
            receipt.base_revision,
            receipt.acknowledged_at,
            receipt.workflow_version,
            receipt.expected_workflow_version,
            receipt.protocol_version,
            receipt.definition_digest_version,
            receipt.fingerprint_version,
        )
        == (
            attempt.id,
            record.id,
            attempt.worker_id,
            attempt.claim_request_id,
            1,
            record.repository_id,
            record.snapshot_id,
            record.base_revision,
            attempt.acknowledged_at,
            2,
            1,
            1,
            1,
            1,
        )
        and re.fullmatch(r"[0-9a-f]{64}", receipt.definition_digest) is not None
        and re.fullmatch(r"[0-9a-f]{64}", receipt.fingerprint) is not None
        and all(
            event.result_id is None
            and event.cycle is None
            and event.candidate_revision is None
            for event in (claimed, acknowledged)
        )
    )


def _publication_history(
    record: JobRecord,
    workflow: JobWorkflowRecord,
    event: JobWorkflowEventRecord,
    attempt: ExecutionAttemptRecord | None,
    acknowledgement: AcknowledgementReceiptRecord | None,
    result: AcceptedResultHistory | None,
    operation: DeliveryOperationHistory | None,
    operation_event: DeliveryOperationEventRecord | None,
) -> bool:
    if (
        attempt is None
        or acknowledgement is None
        or result is None
        or operation is None
        or operation_event is None
    ):
        return False
    step = (
        operation_event.step_id,
        operation_event.step_kind,
        operation_event.input_digest,
        operation_event.observation_digest,
    )
    if operation_event.kind in ("accepted", "publishing_started", "finalized"):
        if any(value is not None for value in step):
            return False
    elif operation_event.kind in ("write_planned", "write_observed", "uncertain"):
        if (
            not isinstance(operation_event.step_id, UUID)
            or type(operation_event.step_kind) is not str
            or not 1 <= len(operation_event.step_kind) <= 128
            or type(operation_event.input_digest) is not str
            or re.fullmatch(r"[0-9a-f]{64}", operation_event.input_digest) is None
            or (
                operation_event.kind == "write_observed"
                and (
                    type(operation_event.observation_digest) is not str
                    or re.fullmatch(r"[0-9a-f]{64}", operation_event.observation_digest)
                    is None
                )
            )
            or (
                operation_event.kind != "write_observed"
                and operation_event.observation_digest is not None
            )
        ):
            return False
    else:
        return False
    if (
        any(
            not isinstance(value, UUID)
            for value in (result.id, result.request_id, result.execution_id)
        )
        or (
            result.attempt_id,
            result.job_id,
            result.worker_id,
            result.claim_request_id,
            result.repository_id,
            result.snapshot_id,
            result.generation,
            result.definition_digest,
        )
        != (
            attempt.id,
            record.id,
            attempt.worker_id,
            attempt.claim_request_id,
            record.repository_id,
            record.snapshot_id,
            1,
            acknowledgement.definition_digest,
        )
        or any(
            re.fullmatch(r"[0-9a-f]{64}", value) is None
            for value in (
                result.definition_digest,
                result.claim_fingerprint,
                result.fingerprint,
                result.payload_sha256,
            )
        )
        or re.fullmatch(r"[0-9a-f]{40}", result.candidate_revision) is None
        or (
            result.protocol_version,
            result.fingerprint_version,
            result.expected_workflow_version,
        )
        != (1, 1, 2)
        or type(result.repair_cycles) is not int
        or not 0 <= result.repair_cycles <= record.max_repair_cycles
        or result.publishing_workflow_version != 4 + 2 * result.repair_cycles
        or attempt.acknowledged_at is None
        or result.recorded_at < attempt.acknowledged_at
        or workflow.repair_cycles != result.repair_cycles
        or workflow.candidate_revision != result.candidate_revision
        or workflow.accepted_result_id != result.id
        or workflow.last_attempt_id != attempt.id
        or workflow.last_attempt_number != 1
        or (event.attempt_id, event.result_id, event.cycle, event.candidate_revision)
        != (attempt.id, result.id, result.repair_cycles, result.candidate_revision)
        or operation.result_id != result.id
        or type(operation.version) is not int
        or not 0 <= operation.version < 2048
        or operation.event_count != operation.version + 1
        or operation_event.result_id != result.id
        or operation_event.sequence != operation.version
        or operation_event.invocation_id != operation.invocation_id
        or operation_event.occurred_at < result.recorded_at
    ):
        return False
    terminal = (
        operation.pr_number,
        operation.pr_repository_id,
        operation.pr_provider_repository_id,
        operation.pr_branch,
        operation.pr_target_branch,
        operation.pr_head_revision,
        operation.completed_at,
        operation.final_workflow_version,
        operation.release_ordinal,
    )
    if workflow.status == JobStatus.PUBLISHING_PR.value:
        stage_valid = (
            operation.state == "accepted"
            and operation.version == 0
            and operation.invocation_id is None
            and operation_event.kind == "accepted"
            and operation_event.occurred_at == result.recorded_at
        ) or (
            operation.state in ("publishing", "uncertain")
            and operation.version >= 1
            and isinstance(operation.invocation_id, UUID)
            and operation_event.kind
            in (
                ("uncertain", "write_observed")
                if operation.state == "uncertain"
                else ("publishing_started", "write_planned", "write_observed")
            )
        )
        return (
            stage_valid
            and all(value is None for value in terminal)
            and workflow.workflow_version == result.publishing_workflow_version
            and workflow.active_attempt_id == attempt.id
            and (event.event_type, event.previous_version, event.previous_status)
            == (
                WorkflowEventType.CHECKS_ACCEPTED.value,
                result.publishing_workflow_version - 1,
                JobStatus.TESTING.value,
            )
            and workflow.updated_at == result.recorded_at == attempt.updated_at
            and attempt.status == AttemptStatus.PUBLISHING_PR.value
            and attempt.revoked_at is None
            and attempt.ended_at is None
        )
    return (
        workflow.status == JobStatus.AWAITING_REVIEW.value
        and operation.state == "finalized"
        and operation_event.kind == "finalized"
        and operation.version >= 2
        and isinstance(operation.invocation_id, UUID)
        and all(value is not None for value in terminal)
        and type(operation.pr_number) is int
        and operation.pr_number > 0
        and operation.pr_repository_id == record.repository_id
        and operation.pr_provider_repository_id == record.provider_repository_id
        and operation.pr_branch == record.authorized_branch
        and operation.pr_target_branch == record.target_branch
        and operation.pr_head_revision == result.candidate_revision
        and operation.final_workflow_version == result.publishing_workflow_version + 1
        and workflow.workflow_version == operation.final_workflow_version
        and workflow.active_attempt_id is None
        and (event.event_type, event.previous_version, event.previous_status)
        == (
            WorkflowEventType.PUBLISHED.value,
            result.publishing_workflow_version,
            JobStatus.PUBLISHING_PR.value,
        )
        and operation.completed_at
        == workflow.updated_at
        == attempt.updated_at
        == attempt.revoked_at
        == attempt.ended_at
        == operation_event.occurred_at
        and operation.completed_at is not None
        and operation.completed_at >= result.recorded_at
        and type(operation.release_ordinal) is int
        and operation.release_ordinal > 0
        and attempt.status == AttemptStatus.SUCCEEDED.value
    )


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
    acknowledged_event: JobWorkflowEventRecord | None = None,
    accepted_result: AcceptedResultHistory | None = None,
    operation: DeliveryOperationHistory | None = None,
    operation_event: DeliveryOperationEventRecord | None = None,
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
    history = _acknowledged_history(
        record, attempt, claimed_event, acknowledged_event, acknowledgement
    )
    running = running and history
    publication = history and _publication_history(
        record,
        workflow,
        workflow_event,
        attempt,
        acknowledgement,
        accepted_result,
        operation,
        operation_event,
    )
    if not publication and (
        workflow.repair_cycles != 0
        or workflow.candidate_revision is not None
        or workflow.accepted_result_id is not None
        or accepted_result is not None
        or operation is not None
        or operation_event is not None
    ):
        raise JobPersistenceError("Unsupported workflow contains result history.")
    if (queued or claimed) and (
        acknowledgement is not None or acknowledged_event is not None
    ):
        raise JobPersistenceError(
            "Unacknowledged workflow contains acknowledgement history."
        )
    if not (queued or claimed or running or publication):
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
        if publication and (
            lease_authority is None
            or accepted_result is None
            or accepted_result.recorded_at >= lease_authority.current_expires_at
            or (
                operation is not None
                and operation.completed_at is not None
                and operation.completed_at >= lease_authority.current_expires_at
            )
        ):
            raise JobPersistenceError(
                "Publication acceptance exceeded retained lease authority."
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
    if acknowledgement is not None:
        if policy.provider_repository_id is None:
            raise JobPersistenceError(
                "Acknowledged definition has no provider identity."
            )
        definition = AssignedJobDefinition(
            job_id=job.id,
            source_task_id=job.source_task_id,
            repository_id=job.repository_id,
            provider_repository_id=policy.provider_repository_id,
            repository_slug=policy.slug,
            snapshot=task,
            authorized_branch=job.authorized_branch,
            target_branch=job.target_branch,
            base_revision=job.base_revision,
            policy_version=job.policy_version,
            checks=policy.checks,
            max_repair_cycles=job.max_repair_cycles,
            created_at=job.created_at,
        )
        if acknowledgement.definition_digest != definition.fingerprint():
            raise JobPersistenceError(
                "Acknowledgement changed immutable definition pins."
            )
    decide_submission(intent, SubmissionState(existing=recorded_submission(stored)))
    return stored
