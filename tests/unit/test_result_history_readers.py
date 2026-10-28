"""Historical SQL reader contracts without a live database or provider."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, cast
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from devrelay.application.contracts.acknowledgements import (
    WorkerAcknowledgementIntent,
    WorkerAcknowledgementRequest,
)
from devrelay.application.contracts.assignment_delivery import AssignedJobDefinition
from devrelay.application.contracts.claims import LeaseProof, WorkerClaimRequest
from devrelay.application.contracts.heartbeats import (
    HeartbeatRejected,
    HeartbeatRequest,
)
from devrelay.application.contracts.submissions import (
    RepositoryHead,
    StoredJobSubmission,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.contracts.workflows import JobWorkflowState
from devrelay.application.ports.job_persistence import JobPersistenceError
from devrelay.domain.jobs.values import JobStatus
from devrelay.infrastructure.persistence.acknowledgement_models import (
    AcknowledgementReceiptRecord,
)
from devrelay.infrastructure.persistence.claim_models import (
    ClaimReceiptRecord,
    DispatchSlotRecord,
    ExecutionAttemptRecord,
    WorkerCoordinationRecord,
)
from devrelay.infrastructure.persistence.claim_store import SqlAlchemyClaimStore
from devrelay.infrastructure.persistence.heartbeat_models import (
    AttemptLeaseAuthorityRecord,
    HeartbeatReceiptRecord,
)
from devrelay.infrastructure.persistence.job_mapping import (
    AcceptedResultHistory,
    DeliveryOperationHistory,
    _acknowledged_history,
    _publication_history,
    _validate_lease_authority,
    materialize_submission,
)
from devrelay.infrastructure.persistence.job_models import (
    JobCheckRecord,
    JobRecord,
    RepositoryBindingRecord,
    RepositoryRecord,
)
from devrelay.infrastructure.persistence.job_repository import read_submission
from devrelay.infrastructure.persistence.models import (
    SourceTaskRecord,
    TaskSnapshotRecord,
)
from devrelay.infrastructure.persistence.result_models import (
    DeliveryOperationEventRecord,
)
from devrelay.infrastructure.persistence.workflow_models import (
    JobWorkflowEventRecord,
    JobWorkflowRecord,
)
from tests.unit.test_job_submission import NOW, prepare, repository, stored_task


@dataclass
class History:
    stored: StoredJobSubmission
    definition: AssignedJobDefinition
    authorization: WorkerAuthorization
    claim: WorkerClaimRequest
    acknowledgement: WorkerAcknowledgementRequest
    record: JobRecord
    attempt: ExecutionAttemptRecord
    workflow: JobWorkflowRecord
    claimed: JobWorkflowEventRecord
    acknowledged: JobWorkflowEventRecord
    current: JobWorkflowEventRecord
    receipt: AcknowledgementReceiptRecord
    claim_receipt: ClaimReceiptRecord
    authority: AttemptLeaseAuthorityRecord
    result: AcceptedResultHistory
    operation: DeliveryOperationHistory
    operation_event: DeliveryOperationEventRecord
    slot: DispatchSlotRecord
    latest_heartbeat: HeartbeatReceiptRecord | None = None


def history(*, terminal: bool = False, repairs: int = 0) -> History:
    intent = prepare(stored_task(), repository())
    policy = intent.policy
    job = intent.build_candidate(
        job_id=uuid4(),
        head=RepositoryHead(
            repository_id=policy.repository_id,
            provider_repository_id=123,
            target_branch=policy.target_branch,
            revision="a" * 40,
        ),
        now=NOW,
    )
    worker, attempt_id, result_id = uuid4(), uuid4(), uuid4()
    claim = WorkerClaimRequest(claim_request_id=uuid4(), proof=LeaseProof.generate())
    authorization = WorkerAuthorization(
        worker_id=worker,
        credential_expires_at=NOW + timedelta(hours=1),
        allowed_repository_ids=frozenset({job.repository_id}),
    )
    identity = claim.identify(authorization)
    definition = AssignedJobDefinition(
        job_id=job.id,
        source_task_id=job.source_task_id,
        repository_id=job.repository_id,
        provider_repository_id=123,
        repository_slug=policy.slug,
        snapshot=intent.stored.snapshot,
        authorized_branch=job.authorized_branch,
        target_branch=job.target_branch,
        base_revision=job.base_revision,
        policy_version=job.policy_version,
        checks=policy.checks,
        max_repair_cycles=job.max_repair_cycles,
        created_at=job.created_at,
    )
    ack = WorkerAcknowledgementRequest(
        claim=claim,
        intent=WorkerAcknowledgementIntent(
            acknowledgement_request_id=uuid4(),
            claim_request_id=claim.claim_request_id,
            attempt_id=attempt_id,
            generation=1,
            snapshot_id=job.snapshot_id,
            base_revision=job.base_revision,
            definition_digest=definition.fingerprint(),
        ),
    )
    accepted, finished = NOW + timedelta(seconds=90), NOW + timedelta(seconds=95)
    current_time = finished if terminal else accepted
    status = "awaiting_review" if terminal else "publishing_pr"
    version = 4 + 2 * repairs + int(terminal)
    stored = StoredJobSubmission(
        intent=intent,
        job=job,
        workflow=JobWorkflowState(
            job_id=job.id,
            status=JobStatus(status),
            version=version,
            blocks_new_submission=True,
            created_at=NOW,
            updated_at=current_time,
        ),
    )
    record = JobRecord(
        id=job.id,
        repository_id=job.repository_id,
        provider_repository_id=123,
        snapshot_id=job.snapshot_id,
        authorized_branch=job.authorized_branch,
        target_branch=job.target_branch,
        base_revision=job.base_revision,
        max_repair_cycles=2,
        created_at=NOW,
    )
    attempt = ExecutionAttemptRecord(
        id=attempt_id,
        job_id=job.id,
        worker_id=worker,
        claim_request_id=claim.claim_request_id,
        attempt_number=1,
        generation=1,
        status="succeeded" if terminal else "publishing_pr",
        issued_at=NOW,
        expires_at=NOW + timedelta(seconds=30),
        acknowledged_at=NOW + timedelta(seconds=1),
        updated_at=current_time,
        revoked_at=finished if terminal else None,
        ended_at=finished if terminal else None,
    )
    workflow = JobWorkflowRecord(
        job_id=job.id,
        status=status,
        workflow_version=version,
        active_attempt_id=None if terminal else attempt_id,
        last_attempt_id=attempt_id,
        last_attempt_number=1,
        repair_cycles=repairs,
        candidate_revision="b" * 40,
        accepted_result_id=result_id,
        updated_at=current_time,
    )
    claimed = JobWorkflowEventRecord(
        job_id=job.id,
        workflow_version=1,
        status="claimed",
        event_type="claimed",
        attempt_id=attempt_id,
        previous_version=0,
        previous_status="queued",
        occurred_at=NOW,
    )
    acknowledged = JobWorkflowEventRecord(
        job_id=job.id,
        workflow_version=2,
        status="running",
        event_type="acknowledged",
        attempt_id=attempt_id,
        previous_version=1,
        previous_status="claimed",
        occurred_at=attempt.acknowledged_at,
    )
    current = JobWorkflowEventRecord(
        job_id=job.id,
        workflow_version=version,
        status=status,
        event_type="published" if terminal else "checks_accepted",
        attempt_id=attempt_id,
        previous_version=version - 1,
        previous_status="publishing_pr" if terminal else "testing",
        occurred_at=current_time,
        result_id=result_id,
        cycle=repairs,
        candidate_revision="b" * 40,
    )
    receipt = AcknowledgementReceiptRecord(
        worker_id=worker,
        request_id=ack.intent.acknowledgement_request_id,
        attempt_id=attempt_id,
        job_id=job.id,
        repository_id=job.repository_id,
        claim_request_id=claim.claim_request_id,
        generation=1,
        snapshot_id=job.snapshot_id,
        base_revision=job.base_revision,
        definition_digest=definition.fingerprint(),
        acknowledged_at=attempt.acknowledged_at,
        workflow_version=2,
        expected_workflow_version=1,
        protocol_version=1,
        definition_digest_version=1,
        fingerprint_version=1,
        fingerprint=ack.identify(authorization).fingerprint,
    )
    claim_receipt = ClaimReceiptRecord(
        worker_id=worker,
        claim_request_id=claim.claim_request_id,
        protocol_version=1,
        fingerprint_version=1,
        verifier_version=1,
        fingerprint=identity.fingerprint,
        verifier=identity.verifier.digest,
        outcome="assigned",
        job_id=job.id,
        attempt_id=attempt_id,
        no_work_reason=None,
        recorded_at=NOW,
    )
    authority = AttemptLeaseAuthorityRecord(
        attempt_id=attempt_id,
        job_id=job.id,
        worker_id=worker,
        claim_request_id=claim.claim_request_id,
        generation=1,
        lease_revision=2,
        receipt_count=1,
        updated_at=NOW + timedelta(seconds=20),
        current_expires_at=NOW + timedelta(seconds=120),
        next_heartbeat_at=NOW + timedelta(seconds=26),
        revoked_at=finished if terminal else None,
    )
    result = AcceptedResultHistory(
        result_id,
        uuid4(),
        worker,
        attempt_id,
        job.id,
        claim.claim_request_id,
        job.repository_id,
        job.snapshot_id,
        uuid4(),
        1,
        definition.fingerprint(),
        identity.fingerprint,
        "c" * 64,
        1,
        1,
        2,
        4 + 2 * repairs,
        "b" * 40,
        repairs,
        accepted,
        "d" * 64,
    )
    invocation = uuid4() if terminal else None
    operation = DeliveryOperationHistory(
        result_id,
        2 if terminal else 0,
        "finalized" if terminal else "accepted",
        invocation,
        3 if terminal else 1,
        12 if terminal else None,
        job.repository_id if terminal else None,
        123 if terminal else None,
        job.authorized_branch if terminal else None,
        job.target_branch if terminal else None,
        "b" * 40 if terminal else None,
        finished if terminal else None,
        version if terminal else None,
        1 if terminal else None,
    )
    operation_event = DeliveryOperationEventRecord(
        result_id=result_id,
        sequence=operation.version,
        kind="finalized" if terminal else "accepted",
        invocation_id=invocation,
        occurred_at=current_time,
    )
    slot = DispatchSlotRecord(
        name="coding",
        reserved_attempt_id=uuid4() if terminal else attempt_id,
        heartbeat_receipt_count=5,
    )
    latest = HeartbeatReceiptRecord(
        worker_id=worker,
        request_id=uuid4(),
        attempt_id=attempt_id,
        job_id=job.id,
        repository_id=job.repository_id,
        claim_request_id=claim.claim_request_id,
        generation=1,
        protocol_version=1,
        fingerprint_version=1,
        fingerprint="e" * 64,
        expected_lease_revision=1,
        lease_revision=2,
        updated_at=authority.updated_at,
        current_expires_at=authority.current_expires_at,
        recorded_at=authority.updated_at,
        renewed=True,
        attempt_ordinal=1,
        worker_ordinal=1,
        global_ordinal=1,
    )
    return History(
        stored,
        definition,
        authorization,
        claim,
        ack,
        record,
        attempt,
        workflow,
        claimed,
        acknowledged,
        current,
        receipt,
        claim_receipt,
        authority,
        result,
        operation,
        operation_event,
        slot,
        latest,
    )


def valid(setup: History) -> bool:
    return _acknowledged_history(
        setup.record, setup.attempt, setup.claimed, setup.acknowledged, setup.receipt
    ) and _publication_history(
        setup.record,
        setup.workflow,
        setup.current,
        setup.attempt,
        setup.receipt,
        setup.result,
        setup.operation,
        setup.operation_event,
    )


def mapping_arguments(setup: History) -> list[Any]:
    """Represent complete joined rows, including the immutable queued definition."""
    job, intent = setup.stored.job, setup.stored.intent
    policy, task = intent.policy, intent.stored.snapshot
    record = setup.record
    for name, value in {
        "source_task_id": job.source_task_id,
        "status": "queued",
        "workflow_version": 0,
        "blocks_new_submission": True,
        "definition_version": 2,
        "submission_generation": 1,
        "required_check_count": len(policy.checks),
        "repository_slug": policy.slug,
        "created_at": job.created_at,
        "updated_at": job.created_at,
        "coding_branch_prefix": policy.coding_branch_prefix,
        "policy_version": policy.version,
        "policy_source_provider": policy.task_scope.provider,
        "policy_source_site_id": policy.task_scope.site_id,
        "allowed_scope_keys": sorted(policy.task_scope.allowed_scope_keys),
        "readiness_label": policy.task_scope.readiness_label,
        "actor_id": intent.actor_id,
        "submission_key": intent.submission_key,
        "submission_fingerprint": intent.submission_fingerprint,
    }.items():
        setattr(record, name, value)
    setup.workflow.source_task_id = job.source_task_id
    setup.workflow.repository_id = job.repository_id
    setup.workflow.created_at = job.created_at
    setup.workflow.blocks_new_submission = True
    snapshot = TaskSnapshotRecord(
        id=task.id,
        source_task_id=job.source_task_id,
        content_fingerprint=task.content_fingerprint,
        fingerprint_version=task.fingerprint_version,
        title=task.title,
        description=task.description,
        acceptance_criteria=list(task.acceptance_criteria),
        display_key=task.display_key,
        source_updated_at=task.source_updated_at,
        captured_at=task.captured_at,
    )
    source = SourceTaskRecord(
        id=job.source_task_id,
        source_provider=task.identity.provider,
        source_site_id=task.identity.site_id,
        external_task_id=task.identity.external_id,
    )
    checks = tuple(
        JobCheckRecord(
            job_id=job.id,
            check_id=check.id,
            check_count=len(policy.checks),
            position=index,
            argv=list(check.argv),
            working_directory=check.working_directory,
            timeout_seconds=check.timeout_seconds,
        )
        for index, check in enumerate(policy.checks)
    )
    submitted = JobWorkflowEventRecord(
        job_id=job.id,
        workflow_version=0,
        status="queued",
        event_type="submitted",
        occurred_at=job.created_at,
    )
    return [
        record,
        snapshot,
        source,
        RepositoryRecord(id=job.repository_id, slug=policy.slug),
        checks,
        RepositoryBindingRecord(
            repository_id=job.repository_id, provider_repository_id=123
        ),
        setup.workflow,
        setup.current,
        submitted,
        setup.attempt,
        setup.claimed,
        setup.receipt,
        setup.authority,
        setup.latest_heartbeat,
        setup.acknowledged,
        setup.result,
        setup.operation,
        setup.operation_event,
    ]


@pytest.mark.parametrize(
    "state", ["queued", "running", "publishing_pr", "awaiting_review"]
)
def test_full_materializer_keeps_immutable_definition_separate_from_current_state(
    state: str,
) -> None:
    setup = history(terminal=state == "awaiting_review")
    arguments = mapping_arguments(setup)
    acknowledged_at = setup.attempt.acknowledged_at
    assert acknowledged_at is not None
    if state in ("queued", "running"):
        setup.workflow.status = state
        setup.workflow.workflow_version = 0 if state == "queued" else 2
        setup.workflow.updated_at = NOW if state == "queued" else acknowledged_at
        setup.workflow.repair_cycles = 0
        setup.workflow.candidate_revision = setup.workflow.accepted_result_id = None
        arguments[15:] = [None, None, None]
        if state == "queued":
            setup.workflow.active_attempt_id = setup.workflow.last_attempt_id = None
            setup.workflow.last_attempt_number = 0
            arguments[7] = arguments[8]
            arguments[9:15] = [None] * 6
        else:
            setup.attempt.status = "running"
            setup.attempt.updated_at = acknowledged_at
            arguments[7] = setup.acknowledged
    stored = materialize_submission(*arguments)
    assert stored.job.status is JobStatus.QUEUED
    assert stored.job.workflow_version == 0
    assert stored.job.candidate_revision is None
    assert stored.workflow.status.value == state
    assert (
        stored.intent.submission_fingerprint
        == setup.stored.intent.submission_fingerprint
    )


@pytest.mark.parametrize("position", [6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 17])
def test_full_materializer_rejects_missing_advanced_join(position: int) -> None:
    setup = history(terminal=True)
    arguments = mapping_arguments(setup)
    arguments[position] = None
    with pytest.raises(JobPersistenceError):
        materialize_submission(*arguments)


@pytest.mark.parametrize("terminal", [False, True])
def test_acceptance_and_normal_finalization_must_precede_retained_current_expiry(
    terminal: bool,
) -> None:
    setup = history(terminal=terminal)
    expiry = setup.authority.current_expires_at
    setup.workflow.updated_at = setup.attempt.updated_at = setup.current.occurred_at = (
        expiry
    )
    setup.operation_event.occurred_at = expiry
    if terminal:
        setup.attempt.revoked_at = setup.attempt.ended_at = (
            setup.authority.revoked_at
        ) = expiry
        setup.operation = setup.operation._replace(completed_at=expiry)
    else:
        setup.result = setup.result._replace(recorded_at=expiry)
    with pytest.raises(JobPersistenceError, match="retained lease authority"):
        materialize_submission(*mapping_arguments(setup))


@pytest.mark.parametrize("field", ["step_id", "input_digest", "observation_digest"])
def test_current_operation_event_cannot_carry_foreign_stage_evidence(
    field: str,
) -> None:
    setup = history()
    setattr(setup.operation_event, field, uuid4() if field == "step_id" else "e" * 64)
    assert not valid(setup)


@pytest.mark.parametrize("terminal", [False, True])
@pytest.mark.parametrize("repairs", [0, 1, 2])
def test_advanced_history_preserves_original_ack_and_original_expiry(
    terminal: bool, repairs: int
) -> None:
    setup = history(terminal=terminal, repairs=repairs)
    assert setup.attempt.updated_at > setup.attempt.expires_at
    assert setup.receipt.workflow_version == 2
    assert valid(setup)
    _validate_lease_authority(
        setup.attempt,
        setup.authority,
        setup.latest_heartbeat,
        repository_id=setup.record.repository_id,
        claimed=False,
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("candidate_revision", "e" * 40),
        ("repair_cycles", 2),
        ("definition_digest", "e" * 64),
        ("attempt_id", uuid4()),
        ("job_id", uuid4()),
        ("repository_id", uuid4()),
        ("snapshot_id", uuid4()),
        ("generation", 2),
        ("publishing_workflow_version", 10),
        ("payload_sha256", "bad"),
    ],
)
def test_changed_result_scalar_pins_fail_closed(field: str, value: object) -> None:
    setup = history()
    setup.result = setup.result._replace(**{field: cast(Any, value)})
    assert not valid(setup)


@pytest.mark.parametrize(
    "component,field,value",
    [
        ("acknowledged", "workflow_version", 3),
        ("acknowledged", "occurred_at", NOW),
        ("receipt", "definition_digest", "e" * 64),
        ("current", "result_id", uuid4()),
        ("current", "cycle", 1),
        ("workflow", "active_attempt_id", uuid4()),
        ("operation_event", "sequence", 1),
        ("operation_event", "result_id", uuid4()),
    ],
)
def test_missing_or_changed_original_and_current_event_links_fail_closed(
    component: str, field: str, value: object
) -> None:
    setup = history()
    setattr(getattr(setup, component), field, value)
    assert not valid(setup)


@pytest.mark.parametrize(
    "missing", ["attempt", "receipt", "result", "operation", "operation_event"]
)
def test_missing_advanced_join_is_not_accepted(missing: str) -> None:
    setup = history()
    values = {
        key: getattr(setup, key)
        for key in ("attempt", "receipt", "result", "operation", "operation_event")
    }
    values[missing] = None
    assert not _publication_history(
        setup.record,
        setup.workflow,
        setup.current,
        values["attempt"],
        values["receipt"],
        values["result"],
        values["operation"],
        values["operation_event"],
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("pr_head_revision", "e" * 40),
        ("pr_provider_repository_id", 124),
        ("pr_repository_id", uuid4()),
        ("pr_branch", "foreign"),
        ("pr_target_branch", "foreign"),
        ("release_ordinal", None),
        ("completed_at", NOW),
        ("final_workflow_version", 10),
    ],
)
def test_terminal_association_and_release_are_exact(field: str, value: object) -> None:
    setup = history(terminal=True)
    setup.operation = setup.operation._replace(**{field: cast(Any, value)})
    assert not valid(setup)


def test_late_observation_preserves_readable_sticky_uncertainty() -> None:
    setup = history()
    invocation = uuid4()
    setup.operation = setup.operation._replace(
        state="uncertain", version=4, event_count=5, invocation_id=invocation
    )
    setup.operation_event.sequence = 4
    setup.operation_event.kind = "write_observed"
    setup.operation_event.invocation_id = invocation
    setup.operation_event.step_id = uuid4()
    setup.operation_event.step_kind = "create_pr"
    setup.operation_event.input_digest = "e" * 64
    setup.operation_event.observation_digest = "f" * 64
    setup.operation_event.occurred_at += timedelta(seconds=200)
    # A late observation is historical bookkeeping, not renewed work authority.
    assert setup.operation_event.occurred_at > setup.authority.current_expires_at
    assert valid(setup)
    stored = materialize_submission(*mapping_arguments(setup))
    assert stored.workflow.status is JobStatus.PUBLISHING_PR
    assert setup.operation.completed_at is None


class Rows:
    def __init__(self, value: Any) -> None:
        self.value = value

    def one(self) -> Any:
        return self.value

    def one_or_none(self) -> Any:
        return self.value


class Session:
    def __init__(self, setup: History) -> None:
        self.setup = setup
        self.heartbeat: HeartbeatReceiptRecord | None = None
        self.statements: list[Any] = []

    async def scalars(self, statement: Any) -> Rows:
        self.statements.append(statement)
        model = statement.column_descriptions[0]["entity"]
        values = {
            ClaimReceiptRecord: self.setup.claim_receipt,
            JobWorkflowRecord: self.setup.workflow,
            ExecutionAttemptRecord: self.setup.attempt,
            AttemptLeaseAuthorityRecord: self.setup.authority,
            AcknowledgementReceiptRecord: self.setup.receipt,
            HeartbeatReceiptRecord: self.heartbeat,
        }
        return Rows(values[model])

    async def execute(self, statement: Any) -> Rows:
        self.statements.append(statement)
        if (
            hasattr(statement, "column_descriptions")
            and statement.column_descriptions[0]["entity"] is ExecutionAttemptRecord
        ):
            return Rows((self.setup.attempt, self.setup.authority))
        return Rows(None)


class Sessions:
    def __init__(self, session: Session) -> None:
        self.session = session

    @asynccontextmanager
    async def begin(self) -> Any:
        yield self.session


class Store(SqlAlchemyClaimStore):
    def __init__(self, setup: History, session: Session) -> None:
        super().__init__(
            cast(async_sessionmaker[AsyncSession], Sessions(session)),
            (setup.stored.intent.policy,),
        )
        self.setup = setup
        self.now = setup.attempt.updated_at + timedelta(seconds=1)

    async def _clock(self, session: AsyncSession) -> Any:
        return self.now

    async def _timeouts(self, session: AsyncSession) -> None:
        pass

    async def _slot(self, session: AsyncSession, *, lock: bool) -> DispatchSlotRecord:
        return self.setup.slot

    async def _worker(
        self, session: AsyncSession, worker_id: Any, *, lock: bool
    ) -> WorkerCoordinationRecord:
        return WorkerCoordinationRecord(
            worker_id=self.setup.authorization.worker_id, heartbeat_receipt_count=5
        )

    async def _stored(self, session: AsyncSession, job_id: Any) -> StoredJobSubmission:
        return self.setup.stored


@pytest.mark.parametrize("terminal", [False, True])
def test_claim_and_ack_replay_do_not_use_advanced_phase_time_as_original_lease_update(
    terminal: bool,
) -> None:
    setup = history(terminal=terminal)
    store = Store(setup, Session(setup))
    result = asyncio.run(
        store.recover(setup.claim.identify(setup.authorization), setup.authorization)
    )
    assert result is not None and result.attempt is not None
    assert result.attempt.lease.expires_at == setup.attempt.expires_at
    assert result.attempt.lease.updated_at == (
        setup.attempt.revoked_at or setup.attempt.acknowledged_at
    )
    assert result.lease_authority_active is not terminal
    ack = asyncio.run(
        store.acknowledge(
            setup.acknowledgement.identify(setup.authorization),
            setup.authorization,
            definition_digest=setup.definition.fingerprint(),
        )
    )
    assert ack.recovered and ack.workflow_version == 2
    assert ack.acknowledged_at == setup.attempt.acknowledged_at
    assert ack.lease_authority_active is not terminal


def test_late_heartbeat_replay_after_terminal_slot_reuse_returns_current_revoked_authority() -> (
    None
):
    setup = history(terminal=True)
    session = Session(setup)
    store = Store(setup, session)
    pending = HeartbeatRequest(
        claim=setup.claim,
        heartbeat_request_id=uuid4(),
        attempt_id=setup.attempt.id,
        generation=1,
        expected_lease_revision=1,
    )
    identity = pending.identify(setup.authorization)
    session.heartbeat = HeartbeatReceiptRecord(
        worker_id=setup.authorization.worker_id,
        request_id=pending.heartbeat_request_id,
        attempt_id=setup.attempt.id,
        job_id=setup.record.id,
        repository_id=setup.record.repository_id,
        claim_request_id=setup.claim.claim_request_id,
        generation=1,
        fingerprint=identity.fingerprint,
        protocol_version=1,
        fingerprint_version=1,
        expected_lease_revision=1,
        lease_revision=1,
        updated_at=NOW,
        current_expires_at=setup.attempt.expires_at,
        recorded_at=NOW + timedelta(seconds=2),
        renewed=False,
        attempt_ordinal=1,
        worker_ordinal=1,
        global_ordinal=1,
    )
    setup.authority.receipt_count = 2
    result = asyncio.run(store.heartbeat(identity, setup.authorization))
    assert result.stop_required and not result.renewed
    assert result.authority.revoked_at == setup.attempt.revoked_at
    assert result.authority.updated_at == NOW + timedelta(seconds=20)
    assert result.authority.lease_revision == 2
    session.heartbeat = None
    with pytest.raises(HeartbeatRejected):
        asyncio.run(store.heartbeat(identity, setup.authorization))


@pytest.mark.parametrize("slot_matches", [False, True])
def test_new_heartbeat_requires_current_publishing_ownership(
    slot_matches: bool,
) -> None:
    setup = history(repairs=1)
    session = Session(setup)
    store = Store(setup, session)
    setup.authority.lease_revision = 2
    setup.authority.receipt_count = 1
    setup.authority.updated_at = NOW + timedelta(seconds=20)
    setup.authority.current_expires_at = NOW + timedelta(seconds=200)
    if not slot_matches:
        setup.slot.reserved_attempt_id = uuid4()
    pending = HeartbeatRequest(
        claim=setup.claim,
        heartbeat_request_id=uuid4(),
        attempt_id=setup.attempt.id,
        generation=1,
        expected_lease_revision=2,
    )
    if not slot_matches:
        with pytest.raises(HeartbeatRejected):
            asyncio.run(
                store.heartbeat(
                    pending.identify(setup.authorization), setup.authorization
                )
            )
    else:
        result = asyncio.run(
            store.heartbeat(pending.identify(setup.authorization), setup.authorization)
        )
        assert not result.stop_required
        assert result.authority.reference.expires_at == setup.attempt.expires_at
        assert result.authority.current_expires_at == NOW + timedelta(seconds=200)
        assert any(
            str(statement).startswith("INSERT INTO worker_heartbeat_receipts")
            for statement in session.statements
        )


@pytest.mark.parametrize("changed", ["expired", "scope", "proof"])
def test_historical_recovery_keeps_current_authentication_scope_and_original_proof(
    changed: str,
) -> None:
    from dataclasses import replace

    from devrelay.application.contracts.assignments import ClaimScopeRejected
    from devrelay.application.contracts.claims import ClaimReplayConflict
    from devrelay.application.contracts.eligibility import WorkerAuthorityExpired

    setup = history(terminal=True)
    session, authorization, claim = Session(setup), setup.authorization, setup.claim
    store = Store(setup, session)
    expected: type[Exception]
    if changed == "expired":
        authorization = replace(authorization, credential_expires_at=store.now)
        expected = WorkerAuthorityExpired
    elif changed == "scope":
        authorization = replace(
            authorization, allowed_repository_ids=frozenset({uuid4()})
        )
        expected = ClaimScopeRejected
    else:
        claim = replace(claim, proof=LeaseProof.generate())
        expected = ClaimReplayConflict
    with pytest.raises(expected):
        asyncio.run(store.recover(claim.identify(authorization), authorization))


def test_generic_reader_query_keeps_last_attempt_and_ack_anchor_without_large_payload() -> (
    None
):
    class EmptySession:
        statement: Any = None

        async def execute(self, statement: Any) -> Rows:
            self.statement = statement
            return Rows(None)

    session = EmptySession()
    assert asyncio.run(read_submission(cast(AsyncSession, session), "a" * 64)) is None
    query = str(session.statement)
    assert "job_workflows.last_attempt_id = execution_attempts.id" in query
    assert "attempt_results.payload_bytes" not in query
    assert "attempt_results.payload_sha256" in query
    assert "delivery_operation_events.sequence = delivery_operations.version" in query
    assert query.count("LEFT OUTER JOIN job_workflow_events AS") == 3
