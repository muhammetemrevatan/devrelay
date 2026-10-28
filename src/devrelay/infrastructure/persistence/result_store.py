import asyncio
import hashlib
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from secrets import compare_digest
from uuid import UUID, uuid4

from sqlalchemy import and_, or_, select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased, defer

from devrelay.application.contracts.assignment_delivery import AssignedJobDefinition
from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    ClaimRequestIdentity,
    LeaseProofVerifier,
)
from devrelay.application.contracts.coding_publications import PublicationHistory
from devrelay.application.contracts.coding_result_acceptance import PreparedCodingResult
from devrelay.application.contracts.coding_result_receipts import (
    CodingPublicationState,
    CodingPublicationStep,
    CodingResultAcceptance,
    CodingResultReceipt,
    CodingResultRejected,
    CodingResultStatus,
    CodingResultUnavailable,
    CodingWritePermit,
    TerminalCodingResult,
    VerifiedCodingPublication,
)
from devrelay.application.contracts.coding_result_requests import CodingResultIdentity
from devrelay.application.contracts.coding_result_wire import (
    decode_coding_result_intent,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.contracts.workflows import WorkflowEventType
from devrelay.domain.jobs.attempt import ExecutionAttempt, Lease
from devrelay.domain.jobs.job import Job
from devrelay.domain.jobs.values import (
    AttemptStatus,
    CheckResult,
    JobStatus,
    PullRequestReference,
    VerificationOutcome,
    _uuid,
)
from devrelay.infrastructure.persistence.acknowledgement_models import (
    AcknowledgementReceiptRecord,
)
from devrelay.infrastructure.persistence.claim_models import (
    ClaimReceiptRecord,
    DispatchSlotRecord,
    ExecutionAttemptRecord,
)
from devrelay.infrastructure.persistence.claim_store import SqlAlchemyClaimStore
from devrelay.infrastructure.persistence.heartbeat_models import (
    AttemptLeaseAuthorityRecord,
    HeartbeatReceiptRecord,
)
from devrelay.infrastructure.persistence.job_mapping import _validate_lease_authority
from devrelay.infrastructure.persistence.job_models import JobRecord
from devrelay.infrastructure.persistence.models import SourceTaskRecord
from devrelay.infrastructure.persistence.result_models import (
    AttemptResultRecord,
    DeliveryOperationEventRecord,
    DeliveryOperationRecord,
)
from devrelay.infrastructure.persistence.workflow_models import (
    JobWorkflowEventRecord,
    JobWorkflowRecord,
)

_FAILURE = "Coding result coordination is unavailable."


@dataclass(frozen=True, slots=True, repr=False)
class _ResultInput:
    payload_bytes: bytes
    payload_sha256: str
    fingerprint: str
    definition: AssignedJobDefinition
    claim: ClaimRequestIdentity
    cycle_checks: tuple[tuple[CheckResult, ...], ...]


def _snapshot(prepared: PreparedCodingResult) -> _ResultInput:
    """Own bounded metadata before the first await; source bytes are immutable.

    Do not copy the report's full source graph: its canonical byte payload will
    be decoded into owned objects by the off-loop preflight.
    """
    if not isinstance(prepared, PreparedCodingResult):
        raise CodingResultRejected("A prepared original coding result is required.")
    return _ResultInput(
        payload_bytes=prepared.payload_bytes,
        payload_sha256=prepared.payload_sha256,
        fingerprint=prepared.identity.fingerprint,
        definition=deepcopy(prepared.definition),
        claim=deepcopy(prepared.identity.claim),
        cycle_checks=deepcopy(prepared.cycle_checks),
    )


def _capture(source: _ResultInput) -> PreparedCodingResult:
    """Rebind complete immutable bytes before opening a transaction, off-loop."""
    payload = source.payload_bytes
    if type(payload) is not bytes or not compare_digest(
        hashlib.sha256(payload).hexdigest(), source.payload_sha256
    ):
        raise CodingResultRejected("Coding result payload changed after preparation.")
    intent = decode_coding_result_intent(payload)
    intent.result.validate(source.definition)
    identity = CodingResultIdentity(intent=intent, claim=source.claim)
    identity.require_replay(source.fingerprint)
    return PreparedCodingResult(
        identity=identity,
        definition=source.definition,
        payload_bytes=payload,
        payload_sha256=source.payload_sha256,
        cycle_checks=source.cycle_checks,
    )


@dataclass(frozen=True, slots=True)
class _LockedResult:
    slot: DispatchSlotRecord
    job: JobRecord
    workflow: JobWorkflowRecord
    attempt: ExecutionAttemptRecord
    authority: AttemptLeaseAuthorityRecord
    result: AttemptResultRecord | None


def _receipt(record: AttemptResultRecord) -> CodingResultReceipt:
    return CodingResultReceipt(
        result_id=record.id,
        worker_id=record.worker_id,
        request_id=record.request_id,
        claim_request_id=record.claim_request_id,
        job_id=record.job_id,
        attempt_id=record.attempt_id,
        repository_id=record.repository_id,
        execution_id=record.execution_id,
        definition_digest=record.definition_digest,
        request_fingerprint=record.fingerprint,
        candidate_revision=record.candidate_revision,
        repair_cycles=record.repair_cycles,
        publishing_workflow_version=record.publishing_workflow_version,
        recorded_at=record.recorded_at,
        protocol_version=record.protocol_version,
    )


def _runtime_attempt(locked: _LockedResult) -> ExecutionAttempt:
    record, authority = locked.attempt, locked.authority
    return ExecutionAttempt(
        id=record.id,
        job_id=record.job_id,
        number=record.attempt_number,
        status=AttemptStatus(record.status),
        acknowledged_at=record.acknowledged_at,
        ended_at=record.ended_at,
        updated_at=max(
            record.updated_at,
            authority.updated_at,
            authority.revoked_at or authority.updated_at,
        ),
        lease=Lease(
            attempt_id=record.id,
            worker_id=record.worker_id,
            generation=record.generation,
            issued_at=record.issued_at,
            updated_at=authority.revoked_at or authority.updated_at,
            expires_at=authority.current_expires_at,
            revoked_at=authority.revoked_at,
        ),
    )


def _running_job(prepared: PreparedCodingResult, locked: _LockedResult) -> Job:
    definition, record = prepared.definition, locked.attempt
    if record.acknowledged_at is None:
        raise CodingResultUnavailable(_FAILURE)
    return (
        Job(
            id=definition.job_id,
            source_task_id=definition.source_task_id,
            snapshot_id=definition.snapshot.id,
            repository_id=definition.repository_id,
            authorized_branch=definition.authorized_branch,
            target_branch=definition.target_branch,
            base_revision=definition.base_revision,
            policy_version=definition.policy_version,
            required_check_ids=tuple(check.id for check in definition.checks),
            max_repair_cycles=definition.max_repair_cycles,
            created_at=definition.created_at,
            updated_at=definition.created_at,
        )
        .claim(record.id, now=record.issued_at)
        .begin_execution(record.id, now=record.acknowledged_at)
    )


def _accepted_states(
    prepared: PreparedCodingResult, locked: _LockedResult, now: datetime
) -> tuple[Job, ...]:
    current = _running_job(prepared, locked)
    states: list[Job] = []
    for cycle, checks in zip(
        prepared.identity.intent.result.cycles, prepared.cycle_checks, strict=True
    ):
        current = current.begin_testing(locked.attempt.id, cycle.revision, now=now)
        states.append(current)
        current = (
            current.accept_checks(locked.attempt.id, checks, now=now)
            if all(check.outcome is VerificationOutcome.PASSED for check in checks)
            else current.request_repair(locked.attempt.id, now=now)
        )
        states.append(current)
    return tuple(states)


class SqlAlchemyResultStore(SqlAlchemyClaimStore):
    """Use the existing coordination locks and clock, with no provider I/O.

    Full source decoding and hashing finish before the write transaction. Every
    mutation uses slot -> worker -> source -> workflow -> Attempt -> authority ->
    result/operation. Historical replay still authenticates the original proof
    and current scope; it never needs the old slot to remain occupied.
    """

    async def _lock_result(
        self,
        session: AsyncSession,
        prepared: PreparedCodingResult,
        authorization: WorkerAuthorization,
        *,
        metadata_only: bool = False,
    ) -> _LockedResult:
        identity, definition = prepared.identity, prepared.definition
        if identity.claim.worker_id != authorization.worker_id:
            raise CodingResultRejected(
                "Coding result requires the authenticated worker."
            )
        now = await self._clock(session)
        if not metadata_only:
            scope = self._authorize(authorization, now)
        else:
            scope = frozenset(
                key
                for key, policy in self._policies.items()
                if policy.enabled and key in authorization.allowed_repository_ids
            )
        if definition.repository_id not in scope:
            raise CodingResultRejected(
                "Coding result is outside current repository scope."
            )
        slot = await self._slot(session, lock=True)
        worker = await self._worker(session, authorization.worker_id, lock=True)
        claim = (
            await session.scalars(
                select(ClaimReceiptRecord).where(
                    ClaimReceiptRecord.worker_id == authorization.worker_id,
                    ClaimReceiptRecord.claim_request_id
                    == identity.claim.claim_request_id,
                )
            )
        ).one_or_none()
        if worker is None or claim is None or claim.outcome != "assigned":
            raise CodingResultRejected("The original assigned claim is required.")
        if (
            claim.protocol_version,
            claim.fingerprint_version,
            claim.verifier_version,
        ) != (1, 1, 1):
            raise CodingResultUnavailable(_FAILURE)
        ClaimRequestIdentity(
            worker_id=claim.worker_id,
            claim_request_id=claim.claim_request_id,
            protocol_version=1,
            fingerprint_version=1,
            fingerprint=claim.fingerprint,
            verifier=LeaseProofVerifier(
                worker_id=claim.worker_id,
                claim_request_id=claim.claim_request_id,
                digest=claim.verifier,
            ),
        ).require_replay(identity.claim)
        if (
            claim.job_id != definition.job_id
            or claim.attempt_id != identity.intent.execution.acknowledgement.attempt_id
        ):
            raise CodingResultRejected("Coding result changed its original assignment.")
        job = (
            await session.scalars(select(JobRecord).where(JobRecord.id == claim.job_id))
        ).one_or_none()
        if job is None:
            raise CodingResultUnavailable(_FAILURE)
        await session.execute(
            select(SourceTaskRecord.id)
            .where(SourceTaskRecord.id == job.source_task_id)
            .with_for_update()
        )
        workflow = (
            await session.scalars(
                select(JobWorkflowRecord)
                .where(JobWorkflowRecord.job_id == job.id)
                .with_for_update()
            )
        ).one_or_none()
        attempt = (
            await session.scalars(
                select(ExecutionAttemptRecord)
                .where(ExecutionAttemptRecord.id == claim.attempt_id)
                .with_for_update()
            )
        ).one_or_none()
        authority = (
            await session.scalars(
                select(AttemptLeaseAuthorityRecord)
                .where(AttemptLeaseAuthorityRecord.attempt_id == claim.attempt_id)
                .with_for_update()
            )
        ).one_or_none()
        acknowledgement = (
            await session.scalars(
                select(AcknowledgementReceiptRecord).where(
                    AcknowledgementReceiptRecord.attempt_id == claim.attempt_id
                )
            )
        ).one_or_none()
        if (
            workflow is None
            or attempt is None
            or authority is None
            or acknowledgement is None
        ):
            raise CodingResultUnavailable(_FAILURE)
        intent = identity.intent.execution.acknowledgement
        if (
            (
                job.source_task_id,
                job.snapshot_id,
                job.repository_id,
                job.provider_repository_id,
                job.repository_slug,
                job.base_revision,
                job.authorized_branch,
                job.target_branch,
                job.policy_version,
                job.max_repair_cycles,
                job.required_check_count,
                job.created_at,
            )
            != (
                definition.source_task_id,
                definition.snapshot.id,
                definition.repository_id,
                definition.provider_repository_id,
                definition.repository_slug,
                definition.base_revision,
                definition.authorized_branch,
                definition.target_branch,
                definition.policy_version,
                definition.max_repair_cycles,
                len(definition.checks),
                definition.created_at,
            )
            or (
                attempt.job_id,
                attempt.worker_id,
                attempt.claim_request_id,
                attempt.generation,
                attempt.issued_at,
            )
            != (
                job.id,
                authorization.worker_id,
                identity.claim.claim_request_id,
                intent.generation,
                claim.recorded_at,
            )
            or (
                acknowledgement.worker_id,
                acknowledgement.request_id,
                acknowledgement.job_id,
                acknowledgement.repository_id,
                acknowledgement.claim_request_id,
                acknowledgement.snapshot_id,
                acknowledgement.base_revision,
                acknowledgement.definition_digest,
                acknowledgement.generation,
                acknowledgement.protocol_version,
                acknowledgement.definition_digest_version,
                acknowledgement.expected_workflow_version,
                acknowledgement.workflow_version,
                acknowledgement.acknowledged_at,
            )
            != (
                authorization.worker_id,
                intent.acknowledgement_request_id,
                job.id,
                definition.repository_id,
                intent.claim_request_id,
                intent.snapshot_id,
                intent.base_revision,
                intent.definition_digest,
                intent.generation,
                intent.protocol_version,
                intent.definition_digest_version,
                1,
                2,
                attempt.acknowledged_at,
            )
        ):
            raise CodingResultRejected(
                "Coding result changed accepted definition or acknowledgement pins."
            )
        latest = (
            await session.scalars(
                select(HeartbeatReceiptRecord).where(
                    HeartbeatReceiptRecord.attempt_id == attempt.id,
                    HeartbeatReceiptRecord.attempt_ordinal == authority.receipt_count,
                )
            )
        ).one_or_none()
        _validate_lease_authority(
            attempt, authority, latest, repository_id=job.repository_id, claimed=False
        )
        results = (
            await session.scalars(
                select(AttemptResultRecord)
                .options(defer(AttemptResultRecord.payload_bytes, raiseload=True))
                .where(
                    or_(
                        AttemptResultRecord.attempt_id == attempt.id,
                        and_(
                            AttemptResultRecord.worker_id == authorization.worker_id,
                            AttemptResultRecord.request_id
                            == identity.intent.result_request_id,
                        ),
                    )
                )
                .with_for_update()
            )
        ).all()
        if len(results) > 1:
            raise ClaimReplayConflict(
                "Coding result UUID already belongs to different accepted input."
            )
        result = results[0] if results else None
        if not metadata_only:
            self._authorize(authorization, await self._clock(session))
        return _LockedResult(slot, job, workflow, attempt, authority, result)

    def _match(
        self, prepared: PreparedCodingResult, record: AttemptResultRecord
    ) -> CodingResultReceipt:
        prepared.identity.require_replay(record.fingerprint)
        receipt = _receipt(record)
        receipt.require_input(prepared.identity, job_id=prepared.definition.job_id)
        if (
            record.request_id != prepared.identity.intent.result_request_id
            or record.snapshot_id != prepared.definition.snapshot.id
            or record.claim_fingerprint != prepared.identity.claim.fingerprint
            or record.payload_sha256 != prepared.payload_sha256
            or (
                record.generation,
                record.expected_workflow_version,
                record.fingerprint_version,
            )
            != (1, 2, 1)
        ):
            raise ClaimReplayConflict(
                "Coding result conflicts with its accepted input."
            )
        return receipt

    def _live(
        self, locked: _LockedResult, authorization: WorkerAuthorization, now: datetime
    ) -> ExecutionAttempt:
        scope = self._authorize(authorization, now)
        if (
            locked.job.repository_id not in scope
            or locked.slot.reserved_attempt_id != locked.attempt.id
        ):
            raise CodingResultRejected("Current coding reservation is unavailable.")
        attempt = _runtime_attempt(locked)
        attempt.lease.require_authority(
            worker_id=authorization.worker_id,
            generation=locked.attempt.generation,
            now=now,
        )
        return attempt

    async def accept(
        self,
        prepared: PreparedCodingResult,
        authorization: WorkerAuthorization,
    ) -> CodingResultAcceptance:
        captured = await asyncio.to_thread(_capture, _snapshot(prepared))
        try:
            async with self._sessions() as session, session.begin():
                await self._timeouts(session)
                locked = await self._lock_result(session, captured, authorization)
                if locked.result is not None:
                    receipt = self._match(captured, locked.result)
                    await self._status(session, locked, receipt)
                    self._authorize(authorization, await self._clock(session))
                    return CodingResultAcceptance(receipt=receipt, recovered=True)
                if (
                    locked.workflow.status != JobStatus.RUNNING.value
                    or locked.workflow.workflow_version != 2
                    or locked.workflow.active_attempt_id != locked.attempt.id
                    or locked.workflow.last_attempt_id != locked.attempt.id
                    or locked.workflow.accepted_result_id is not None
                    or locked.attempt.status != AttemptStatus.RUNNING.value
                ):
                    raise CodingResultRejected(
                        "New results require the original running assignment."
                    )
                now = await self._clock(session)
                runtime = self._live(locked, authorization, now)
                publishing = runtime.accept_coding_result(
                    worker_id=authorization.worker_id,
                    generation=runtime.lease.generation,
                    result_attempt_id=runtime.id,
                    now=now,
                )
                states = _accepted_states(captured, locked, now)
                identity, definition = captured.identity, captured.definition
                report = identity.intent.result
                record = AttemptResultRecord(
                    id=uuid4(),
                    request_id=identity.intent.result_request_id,
                    worker_id=authorization.worker_id,
                    attempt_id=runtime.id,
                    job_id=locked.job.id,
                    claim_request_id=identity.claim.claim_request_id,
                    repository_id=definition.repository_id,
                    snapshot_id=definition.snapshot.id,
                    execution_id=identity.intent.execution.execution_id,
                    generation=runtime.lease.generation,
                    definition_digest=report.definition_digest,
                    claim_fingerprint=identity.claim.fingerprint,
                    fingerprint=identity.fingerprint,
                    protocol_version=1,
                    fingerprint_version=1,
                    expected_workflow_version=2,
                    publishing_workflow_version=states[-1].workflow_version,
                    candidate_revision=report.final_candidate.revision,
                    repair_cycles=len(report.cycles) - 1,
                    recorded_at=now,
                    payload_bytes=captured.payload_bytes,
                    payload_sha256=captured.payload_sha256,
                )
                session.add(record)
                session.add(
                    DeliveryOperationRecord(
                        result_id=record.id, version=0, event_count=1, state="accepted"
                    )
                )
                session.add(
                    DeliveryOperationEventRecord(
                        result_id=record.id,
                        sequence=0,
                        kind="accepted",
                        occurred_at=now,
                    )
                )
                await session.flush()
                previous_status, previous_version = JobStatus.RUNNING.value, 2
                for state in states:
                    event_type = {
                        JobStatus.TESTING: WorkflowEventType.TESTED,
                        JobStatus.RUNNING: WorkflowEventType.REPAIR_REQUESTED,
                        JobStatus.PUBLISHING_PR: WorkflowEventType.CHECKS_ACCEPTED,
                    }[state.status]
                    session.add(
                        JobWorkflowEventRecord(
                            job_id=state.id,
                            workflow_version=state.workflow_version,
                            status=state.status.value,
                            event_type=event_type.value,
                            occurred_at=now,
                            attempt_id=runtime.id,
                            previous_version=previous_version,
                            previous_status=previous_status,
                            result_id=record.id,
                            cycle=state.repair_cycles,
                            candidate_revision=state.candidate_revision,
                        )
                    )
                    await session.execute(
                        update(JobWorkflowRecord)
                        .where(
                            JobWorkflowRecord.job_id == state.id,
                            JobWorkflowRecord.workflow_version == previous_version,
                        )
                        .values(
                            status=state.status.value,
                            workflow_version=state.workflow_version,
                            updated_at=now,
                            repair_cycles=state.repair_cycles,
                            candidate_revision=state.candidate_revision,
                            accepted_result_id=record.id,
                        )
                    )
                    previous_status, previous_version = (
                        state.status.value,
                        state.workflow_version,
                    )
                await session.execute(
                    update(ExecutionAttemptRecord)
                    .where(ExecutionAttemptRecord.id == runtime.id)
                    .values(status=publishing.status.value, updated_at=now)
                )
                self._live(locked, authorization, await self._clock(session))
                receipt = _receipt(record)
            return CodingResultAcceptance(receipt=receipt, recovered=False)
        except SQLAlchemyError:
            raise CodingResultUnavailable(_FAILURE) from None

    async def _status(
        self,
        session: AsyncSession,
        locked: _LockedResult,
        receipt: CodingResultReceipt,
    ) -> CodingResultStatus:
        operation = (
            await session.scalars(
                select(DeliveryOperationRecord)
                .where(DeliveryOperationRecord.result_id == receipt.result_id)
                .with_for_update()
            )
        ).one_or_none()
        if operation is None:
            raise CodingResultUnavailable(_FAILURE)
        event = (
            await session.scalars(
                select(DeliveryOperationEventRecord).where(
                    DeliveryOperationEventRecord.result_id == receipt.result_id,
                    DeliveryOperationEventRecord.sequence == operation.version,
                )
            )
        ).one_or_none()
        if (
            event is None
            or operation.event_count != operation.version + 1
            or event.invocation_id != operation.invocation_id
            or event.occurred_at < receipt.recorded_at
            or locked.attempt.acknowledged_at is None
            or not locked.attempt.acknowledged_at
            <= receipt.recorded_at
            < locked.authority.current_expires_at
        ):
            raise CodingResultUnavailable(_FAILURE)
        if (
            locked.workflow.accepted_result_id != receipt.result_id
            or locked.workflow.last_attempt_id != receipt.attempt_id
            or locked.workflow.repair_cycles != receipt.repair_cycles
            or locked.workflow.candidate_revision != receipt.candidate_revision
        ):
            raise CodingResultUnavailable(_FAILURE)
        terminal = None
        if operation.state == "finalized":
            if (
                operation.pr_number is None
                or operation.pr_repository_id is None
                or operation.pr_branch is None
                or operation.pr_target_branch is None
                or operation.pr_head_revision is None
                or operation.completed_at is None
                or operation.final_workflow_version is None
                or operation.release_ordinal is None
                or operation.pr_provider_repository_id
                != locked.job.provider_repository_id
                or operation.pr_repository_id != receipt.repository_id
                or operation.pr_branch != locked.job.authorized_branch
                or operation.pr_target_branch != locked.job.target_branch
                or operation.pr_head_revision != receipt.candidate_revision
                or operation.final_workflow_version
                != receipt.publishing_workflow_version + 1
                or not receipt.recorded_at
                <= operation.completed_at
                < locked.authority.current_expires_at
                or event.occurred_at != operation.completed_at
                or locked.attempt.ended_at != operation.completed_at
                or locked.attempt.revoked_at != operation.completed_at
                or locked.authority.revoked_at != operation.completed_at
                or locked.slot.reserved_attempt_id == locked.attempt.id
                or locked.slot.release_count < operation.release_ordinal
                or (
                    locked.slot.release_count == operation.release_ordinal
                    and locked.slot.last_released_result_id != receipt.result_id
                )
                or locked.workflow.status != JobStatus.AWAITING_REVIEW.value
                or locked.workflow.workflow_version != operation.final_workflow_version
                or locked.workflow.updated_at != operation.completed_at
                or locked.workflow.active_attempt_id is not None
                or locked.attempt.status != AttemptStatus.SUCCEEDED.value
                or locked.attempt.updated_at != operation.completed_at
                or event.kind != "finalized"
            ):
                raise CodingResultUnavailable(_FAILURE)
            terminal = TerminalCodingResult(
                receipt=receipt,
                reference=PullRequestReference(
                    repository_id=operation.pr_repository_id,
                    number=operation.pr_number,
                    source_branch=operation.pr_branch,
                    target_branch=operation.pr_target_branch,
                    head_revision=operation.pr_head_revision,
                ),
                finalized_at=operation.completed_at,
                workflow_version=operation.final_workflow_version,
                release_ordinal=operation.release_ordinal,
            )
        elif (
            locked.workflow.status != JobStatus.PUBLISHING_PR.value
            or locked.workflow.workflow_version != receipt.publishing_workflow_version
            or locked.workflow.updated_at != receipt.recorded_at
            or locked.workflow.active_attempt_id != locked.attempt.id
            or locked.attempt.status != AttemptStatus.PUBLISHING_PR.value
            or locked.attempt.updated_at != receipt.recorded_at
            or locked.slot.reserved_attempt_id != locked.attempt.id
            or locked.authority.revoked_at is not None
            or locked.attempt.ended_at is not None
            or (
                operation.state == "accepted"
                and (
                    event.kind != "accepted"
                    or operation.version != 0
                    or operation.invocation_id is not None
                    or event.occurred_at != receipt.recorded_at
                )
            )
            or (
                operation.state == "publishing"
                and (
                    operation.invocation_id is None
                    or event.kind
                    not in ("publishing_started", "write_planned", "write_observed")
                )
            )
            or (
                operation.state == "uncertain"
                and (
                    operation.invocation_id is None
                    or event.kind not in ("uncertain", "write_observed")
                )
            )
        ):
            raise CodingResultUnavailable(_FAILURE)
        return CodingResultStatus(
            receipt=receipt,
            state=CodingPublicationState(operation.state),
            operation_version=operation.version,
            terminal=terminal,
        )

    async def recover_result(
        self,
        prepared: PreparedCodingResult,
        authorization: WorkerAuthorization,
    ) -> CodingResultStatus | None:
        captured = await asyncio.to_thread(_capture, _snapshot(prepared))
        try:
            async with self._sessions() as session, session.begin():
                await self._timeouts(session)
                locked = await self._lock_result(session, captured, authorization)
                if locked.result is None:
                    self._authorize(authorization, await self._clock(session))
                    return None
                status = await self._status(
                    session, locked, self._match(captured, locked.result)
                )
                self._authorize(authorization, await self._clock(session))
                return status
        except SQLAlchemyError:
            raise CodingResultUnavailable(_FAILURE) from None

    async def read_publication(
        self,
        prepared: PreparedCodingResult,
        authorization: WorkerAuthorization,
    ) -> PublicationHistory | None:
        """Return bounded durable history, without renewing or granting write rights."""
        captured = await asyncio.to_thread(_capture, _snapshot(prepared))
        try:
            async with self._sessions() as session, session.begin():
                await self._timeouts(session)
                locked = await self._lock_result(session, captured, authorization)
                if locked.result is None:
                    self._authorize(authorization, await self._clock(session))
                    return None
                receipt = self._match(captured, locked.result)
                status = await self._status(session, locked, receipt)
                planned = aliased(DeliveryOperationEventRecord)
                latest = DeliveryOperationEventRecord
                row = (
                    await session.execute(
                        select(DeliveryOperationRecord.invocation_id, latest, planned)
                        .join(
                            latest,
                            and_(
                                latest.result_id == DeliveryOperationRecord.result_id,
                                latest.sequence == DeliveryOperationRecord.version,
                            ),
                        )
                        .outerjoin(
                            planned,
                            and_(
                                planned.result_id == latest.result_id,
                                planned.step_id == latest.step_id,
                                planned.kind == "write_planned",
                            ),
                        )
                        .where(
                            DeliveryOperationRecord.result_id == receipt.result_id,
                            DeliveryOperationRecord.version == status.operation_version,
                        )
                    )
                ).one_or_none()
                if row is None:
                    raise CodingResultUnavailable(_FAILURE)
                invocation_id, event, plan = row
                step = None
                if event.kind in ("write_planned", "write_observed", "uncertain"):
                    if (
                        plan is None
                        or invocation_id is None
                        or plan.step_id is None
                        or plan.step_kind is None
                        or plan.input_digest is None
                        or event.invocation_id != invocation_id
                        or (
                            plan.result_id,
                            plan.invocation_id,
                            plan.step_id,
                            plan.step_kind,
                            plan.input_digest,
                        )
                        != (
                            receipt.result_id,
                            invocation_id,
                            event.step_id,
                            event.step_kind,
                            event.input_digest,
                        )
                        or plan.occurred_at < receipt.recorded_at
                        or event.occurred_at < plan.occurred_at
                        or event.sequence - plan.sequence
                        != (
                            0
                            if event.kind == "write_planned"
                            else 2
                            if event.kind == "write_observed"
                            and status.state is CodingPublicationState.UNCERTAIN
                            else 1
                        )
                        or plan.observation_digest is not None
                        or (event.observation_digest is not None)
                        != (event.kind == "write_observed")
                    ):
                        raise CodingResultUnavailable(_FAILURE)
                    try:
                        step = CodingPublicationStep(
                            result_id=receipt.result_id,
                            invocation_id=invocation_id,
                            expected_operation_version=plan.sequence - 1,
                            step_id=plan.step_id,
                            step_kind=plan.step_kind,
                            input_digest=plan.input_digest,
                        )
                    except ValueError:
                        raise CodingResultUnavailable(_FAILURE) from None
                elif (
                    plan is not None
                    or event.step_id is not None
                    or event.step_kind is not None
                    or event.input_digest is not None
                    or event.observation_digest is not None
                ):
                    raise CodingResultUnavailable(_FAILURE)
                try:
                    history = PublicationHistory(
                        status=status,
                        invocation_id=invocation_id,
                        last_step=step,
                        last_observation_digest=event.observation_digest,
                        last_event_kind=event.kind,
                    )
                except ValueError:
                    raise CodingResultUnavailable(_FAILURE) from None
                self._authorize(authorization, await self._clock(session))
                return history
        except SQLAlchemyError:
            raise CodingResultUnavailable(_FAILURE) from None

    async def _operation(
        self,
        session: AsyncSession,
        locked: _LockedResult,
        prepared: PreparedCodingResult,
    ) -> tuple[
        CodingResultReceipt, DeliveryOperationRecord, DeliveryOperationEventRecord
    ]:
        if locked.result is None:
            raise CodingResultRejected(
                "Publication requires an accepted coding result."
            )
        receipt = self._match(prepared, locked.result)
        await self._status(session, locked, receipt)
        operation = (
            await session.scalars(
                select(DeliveryOperationRecord)
                .where(DeliveryOperationRecord.result_id == receipt.result_id)
                .with_for_update()
            )
        ).one()
        event = (
            await session.scalars(
                select(DeliveryOperationEventRecord).where(
                    DeliveryOperationEventRecord.result_id == receipt.result_id,
                    DeliveryOperationEventRecord.sequence == operation.version,
                )
            )
        ).one()
        return receipt, operation, event

    @staticmethod
    def _advance(
        session: AsyncSession,
        operation: DeliveryOperationRecord,
        *,
        kind: str,
        state: str,
        now: datetime,
        step: CodingPublicationStep | None = None,
        observation_digest: str | None = None,
    ) -> None:
        if operation.version >= 2047:
            raise CodingResultRejected("Publication history budget is exhausted.")
        operation.version += 1
        operation.event_count += 1
        operation.state = state
        session.add(
            DeliveryOperationEventRecord(
                result_id=operation.result_id,
                sequence=operation.version,
                kind=kind,
                invocation_id=operation.invocation_id,
                occurred_at=now,
                step_id=step.step_id if step else None,
                step_kind=step.step_kind if step else None,
                input_digest=step.input_digest if step else None,
                observation_digest=observation_digest,
            )
        )

    async def begin_publication(
        self,
        prepared: PreparedCodingResult,
        authorization: WorkerAuthorization,
        *,
        invocation_id: UUID,
    ) -> CodingResultStatus:
        _uuid(invocation_id)
        captured = await asyncio.to_thread(_capture, _snapshot(prepared))
        try:
            async with self._sessions() as session, session.begin():
                await self._timeouts(session)
                locked = await self._lock_result(session, captured, authorization)
                receipt, operation, event = await self._operation(
                    session, locked, captured
                )
                if operation.state != "accepted":
                    if operation.invocation_id != invocation_id:
                        raise ClaimReplayConflict(
                            "Publication already has a different owner invocation."
                        )
                    status = await self._status(session, locked, receipt)
                    self._authorize(authorization, await self._clock(session))
                    return status
                now = await self._clock(session)
                self._live(locked, authorization, now)
                if event.kind != "accepted":
                    raise CodingResultUnavailable(_FAILURE)
                operation.invocation_id = invocation_id
                self._advance(
                    session,
                    operation,
                    kind="publishing_started",
                    state="publishing",
                    now=now,
                )
                await session.flush()
                self._live(locked, authorization, await self._clock(session))
                return CodingResultStatus(
                    receipt=receipt,
                    state=CodingPublicationState.PUBLISHING,
                    operation_version=operation.version,
                )
        except SQLAlchemyError:
            raise CodingResultUnavailable(_FAILURE) from None

    async def plan_write(
        self,
        prepared: PreparedCodingResult,
        authorization: WorkerAuthorization,
        *,
        step: CodingPublicationStep,
    ) -> CodingWritePermit:
        if not isinstance(step, CodingPublicationStep):
            raise CodingResultRejected("A typed broker publication step is required.")
        step = replace(step)
        captured = await asyncio.to_thread(_capture, _snapshot(prepared))
        try:
            async with self._sessions() as session, session.begin():
                await self._timeouts(session)
                locked = await self._lock_result(session, captured, authorization)
                receipt, operation, previous = await self._operation(
                    session, locked, captured
                )
                if (step.result_id, step.invocation_id) != (
                    receipt.result_id,
                    operation.invocation_id,
                ):
                    raise ClaimReplayConflict(
                        "Publication step changed the accepted owner."
                    )
                existing = (
                    await session.scalars(
                        select(DeliveryOperationEventRecord).where(
                            DeliveryOperationEventRecord.result_id == step.result_id,
                            DeliveryOperationEventRecord.step_id == step.step_id,
                            DeliveryOperationEventRecord.kind == "write_planned",
                        )
                    )
                ).one_or_none()
                if existing is not None:
                    self._match_step(
                        existing, step, sequence=step.expected_operation_version + 1
                    )
                    self._authorize(authorization, await self._clock(session))
                    return CodingWritePermit(
                        step=step, operation_version=existing.sequence, created=False
                    )
                now = await self._clock(session)
                self._live(locked, authorization, now)
                if (
                    operation.state != "publishing"
                    or operation.version != step.expected_operation_version
                    or previous.kind not in ("publishing_started", "write_observed")
                ):
                    raise CodingResultRejected(
                        "A new write requires settled current publication history."
                    )
                self._advance(
                    session,
                    operation,
                    kind="write_planned",
                    state="publishing",
                    now=now,
                    step=step,
                )
                await session.flush()
                authorized_at = await self._clock(session)
                self._live(locked, authorization, authorized_at)
                permit = CodingWritePermit(
                    step=step,
                    operation_version=operation.version,
                    created=True,
                    authorized_at=authorized_at,
                    authorized_until=min(
                        locked.authority.current_expires_at,
                        authorization.credential_expires_at,
                    ),
                )
            return permit
        except SQLAlchemyError:
            raise CodingResultUnavailable(_FAILURE) from None

    @staticmethod
    def _match_step(
        event: DeliveryOperationEventRecord,
        step: CodingPublicationStep,
        *,
        sequence: int | None = None,
    ) -> None:
        if (
            event.result_id,
            event.invocation_id,
            event.step_id,
            event.step_kind,
            event.input_digest,
        ) != (
            step.result_id,
            step.invocation_id,
            step.step_id,
            step.step_kind,
            step.input_digest,
        ) or (sequence is not None and event.sequence != sequence):
            raise ClaimReplayConflict(
                "Publication step conflicts with its durable input."
            )

    async def observe_write(
        self,
        prepared: PreparedCodingResult,
        authorization: WorkerAuthorization,
        *,
        step: CodingPublicationStep,
        observation_digest: str,
    ) -> CodingResultStatus:
        """Record a trusted broker outcome for an already committed write plan.

        This bookkeeping may finish after lease or credential expiry. It creates
        no next-write right and cannot finalize an uncertain operation.
        """
        return await self._settle_write(
            prepared, authorization, step=step, observation_digest=observation_digest
        )

    async def mark_uncertain(
        self,
        prepared: PreparedCodingResult,
        authorization: WorkerAuthorization,
        *,
        step: CodingPublicationStep,
    ) -> CodingResultStatus:
        return await self._settle_write(
            prepared, authorization, step=step, observation_digest=None
        )

    async def _settle_write(
        self,
        prepared: PreparedCodingResult,
        authorization: WorkerAuthorization,
        *,
        step: CodingPublicationStep,
        observation_digest: str | None,
    ) -> CodingResultStatus:
        if not isinstance(step, CodingPublicationStep):
            raise CodingResultRejected("A typed broker publication step is required.")
        step = replace(step)
        if observation_digest is not None:
            from devrelay.application.contracts.coding_result_receipts import _digest

            _digest(observation_digest)
        captured = await asyncio.to_thread(_capture, _snapshot(prepared))
        kind = "write_observed" if observation_digest is not None else "uncertain"
        try:
            async with self._sessions() as session, session.begin():
                await self._timeouts(session)
                locked = await self._lock_result(
                    session, captured, authorization, metadata_only=True
                )
                receipt, operation, previous = await self._operation(
                    session, locked, captured
                )
                if (receipt.result_id, operation.invocation_id) != (
                    step.result_id,
                    step.invocation_id,
                ):
                    raise ClaimReplayConflict(
                        "Write outcome changed its accepted owner."
                    )
                planned = (
                    await session.scalars(
                        select(DeliveryOperationEventRecord).where(
                            DeliveryOperationEventRecord.result_id == step.result_id,
                            DeliveryOperationEventRecord.kind == "write_planned",
                            DeliveryOperationEventRecord.step_id == step.step_id,
                        )
                    )
                ).one_or_none()
                if planned is None:
                    raise CodingResultRejected(
                        "Write outcome requires an earlier durable plan."
                    )
                self._match_step(
                    planned, step, sequence=step.expected_operation_version + 1
                )
                existing = (
                    await session.scalars(
                        select(DeliveryOperationEventRecord).where(
                            DeliveryOperationEventRecord.result_id == step.result_id,
                            DeliveryOperationEventRecord.kind == kind,
                            DeliveryOperationEventRecord.step_id == step.step_id,
                        )
                    )
                ).one_or_none()
                if existing is not None:
                    self._match_step(existing, step)
                    if existing.observation_digest != observation_digest:
                        raise ClaimReplayConflict(
                            "Write observation changed after recording."
                        )
                    return await self._status(session, locked, receipt)
                if operation.state not in (
                    "publishing",
                    "uncertain",
                ) or previous.kind not in (
                    ("write_planned", "uncertain")
                    if observation_digest
                    else ("write_planned",)
                ):
                    raise CodingResultRejected(
                        "Write outcome is not the unresolved current step."
                    )
                self._match_step(previous, step)
                now = await self._clock(session)
                self._advance(
                    session,
                    operation,
                    kind=kind,
                    state="uncertain" if kind == "uncertain" else operation.state,
                    now=now,
                    step=step,
                    observation_digest=observation_digest,
                )
                await session.flush()
                return await self._status(session, locked, receipt)
        except SQLAlchemyError:
            raise CodingResultUnavailable(_FAILURE) from None

    async def finalize_publication(
        self,
        prepared: PreparedCodingResult,
        authorization: WorkerAuthorization,
        *,
        publication: VerifiedCodingPublication,
    ) -> TerminalCodingResult:
        if not isinstance(publication, VerifiedCodingPublication):
            raise CodingResultRejected(
                "A trusted broker publication observation is required."
            )
        publication = replace(publication, reference=replace(publication.reference))
        captured = await asyncio.to_thread(_capture, _snapshot(prepared))
        try:
            async with self._sessions() as session, session.begin():
                await self._timeouts(session)
                locked = await self._lock_result(session, captured, authorization)
                receipt, operation, previous = await self._operation(
                    session, locked, captured
                )
                reference = publication.reference
                if (
                    publication.result_id != receipt.result_id
                    or publication.invocation_id != operation.invocation_id
                    or publication.provider_repository_id
                    != locked.job.provider_repository_id
                    or (
                        reference.repository_id,
                        reference.source_branch,
                        reference.target_branch,
                        reference.head_revision,
                    )
                    != (
                        receipt.repository_id,
                        locked.job.authorized_branch,
                        locked.job.target_branch,
                        receipt.candidate_revision,
                    )
                ):
                    raise ClaimReplayConflict(
                        "Publication changed its accepted repository, branches or checked head."
                    )
                if operation.state == "finalized":
                    status = await self._status(session, locked, receipt)
                    if (
                        status.terminal is None
                        or status.terminal.reference != reference
                        or publication.expected_operation_version
                        != operation.version - 1
                    ):
                        raise ClaimReplayConflict(
                            "Publication conflicts with its terminal receipt."
                        )
                    self._authorize(authorization, await self._clock(session))
                    return status.terminal
                now = await self._clock(session)
                runtime = self._live(locked, authorization, now)
                if (
                    operation.state != "publishing"
                    or previous.kind != "write_observed"
                    or operation.version != publication.expected_operation_version
                    or not previous.occurred_at <= publication.observed_at <= now
                    or now - publication.observed_at > timedelta(seconds=30)
                ):
                    raise CodingResultRejected(
                        "Normal finalization requires fresh settled broker publication evidence."
                    )
                completed = runtime.complete_publication(
                    worker_id=authorization.worker_id,
                    generation=runtime.lease.generation,
                    published_attempt_id=runtime.id,
                    stopped_attempt_id=runtime.id,
                    now=now,
                )
                state = _accepted_states(captured, locked, receipt.recorded_at)[
                    -1
                ].record_publication(runtime.id, reference, now=now)
                release_ordinal = locked.slot.release_count + 1
                session.add(
                    JobWorkflowEventRecord(
                        job_id=state.id,
                        workflow_version=state.workflow_version,
                        status=state.status.value,
                        event_type=WorkflowEventType.PUBLISHED.value,
                        occurred_at=now,
                        attempt_id=runtime.id,
                        previous_version=receipt.publishing_workflow_version,
                        previous_status=JobStatus.PUBLISHING_PR.value,
                        result_id=receipt.result_id,
                        cycle=receipt.repair_cycles,
                        candidate_revision=receipt.candidate_revision,
                    )
                )
                locked.workflow.status = state.status.value
                locked.workflow.workflow_version = state.workflow_version
                locked.workflow.updated_at = now
                locked.workflow.active_attempt_id = None
                locked.attempt.status = completed.status.value
                locked.attempt.updated_at = now
                locked.attempt.ended_at = now
                locked.attempt.revoked_at = now
                locked.authority.revoked_at = now
                locked.slot.reserved_attempt_id = None
                locked.slot.release_count = release_ordinal
                locked.slot.last_released_result_id = receipt.result_id
                locked.slot.version += 1
                operation.pr_number = reference.number
                operation.pr_repository_id = reference.repository_id
                operation.pr_provider_repository_id = publication.provider_repository_id
                operation.pr_branch = reference.source_branch
                operation.pr_target_branch = reference.target_branch
                operation.pr_head_revision = reference.head_revision
                operation.completed_at = now
                operation.final_workflow_version = state.workflow_version
                operation.release_ordinal = release_ordinal
                self._advance(
                    session, operation, kind="finalized", state="finalized", now=now
                )
                await session.flush()
                # Revocation now closes the lease; retain its previous deadline
                # and check that the transaction did not cross it before commit.
                final_now = await self._clock(session)
                self._authorize(authorization, final_now)
                if final_now >= locked.authority.current_expires_at:
                    raise CodingResultRejected(
                        "Lease expired before publication could commit."
                    )
                terminal = TerminalCodingResult(
                    receipt=receipt,
                    reference=reference,
                    finalized_at=now,
                    workflow_version=state.workflow_version,
                    release_ordinal=release_ordinal,
                )
            return terminal
        except SQLAlchemyError:
            raise CodingResultUnavailable(_FAILURE) from None
