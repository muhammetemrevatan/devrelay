from dataclasses import replace
from datetime import datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import func, insert, select, text, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from devrelay.application.contracts.acknowledgements import (
    AcknowledgementIdentity,
    AcknowledgementRejected,
    AcknowledgementResult,
)
from devrelay.application.contracts.assignments import (
    ClaimAdmissionRejected,
    ClaimLimits,
    ClaimPersistenceError,
    ClaimResult,
    ClaimScopeRejected,
    NoWorkReason,
)
from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    ClaimRequestIdentity,
    LeaseProofVerifier,
)
from devrelay.application.contracts.eligibility import (
    ClaimEligibilityRejected,
    PreparedJobClaim,
    WorkerAuthorityExpired,
)
from devrelay.application.contracts.heartbeats import (
    HeartbeatAdmissionRejected,
    HeartbeatIdentity,
    HeartbeatLimits,
    HeartbeatRejected,
    HeartbeatResult,
    LeaseAuthorityObservation,
)
from devrelay.application.contracts.submissions import (
    PreparedJobSubmission,
    RepositoryPolicy,
    StoredJobSubmission,
    repository_policy_map,
)
from devrelay.application.contracts.worker_recovery import AssignmentReference
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.contracts.workflows import JobWorkflowState, WorkflowEventType
from devrelay.application.ports.job_persistence import JobPersistenceError
from devrelay.domain.jobs.attempt import ExecutionAttempt, Lease
from devrelay.domain.jobs.values import AttemptStatus, JobStatus
from devrelay.infrastructure.persistence.acknowledgement_models import (
    AcknowledgementReceiptRecord,
)
from devrelay.infrastructure.persistence.claim_models import (
    ClaimReceiptRecord,
    DispatchSlotRecord,
    ExecutionAttemptRecord,
    WorkerCoordinationRecord,
)
from devrelay.infrastructure.persistence.heartbeat_models import (
    AttemptLeaseAuthorityRecord,
    HeartbeatReceiptRecord,
)
from devrelay.infrastructure.persistence.job_models import JobRecord
from devrelay.infrastructure.persistence.job_repository import read_submission
from devrelay.infrastructure.persistence.models import SourceTaskRecord
from devrelay.infrastructure.persistence.workflow_models import (
    JobWorkflowEventRecord,
    JobWorkflowRecord,
)


class SqlAlchemyClaimStore:
    """Singleton serialization, bounded SQL and commit-before-disclosure.

    Initial claims and acknowledgements retain the same reservation. Expiry never
    releases the slot; later transitions require their own evidence and containment.
    """

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        policies: tuple[RepositoryPolicy, ...],
        limits: ClaimLimits | None = None,
        *,
        heartbeat_limits: HeartbeatLimits | None = None,
    ) -> None:
        self._sessions = sessions
        self._policies = repository_policy_map(policies)
        self._limits = limits or ClaimLimits()
        self._heartbeat_limits = heartbeat_limits or HeartbeatLimits()

    def _authorize(
        self, authorization: WorkerAuthorization, now: datetime
    ) -> frozenset[UUID]:
        if now >= authorization.credential_expires_at:
            raise WorkerAuthorityExpired("Worker authority has expired.")
        scope = frozenset(
            identity
            for identity, policy in self._policies.items()
            if policy.enabled and identity in authorization.allowed_repository_ids
        )
        if not scope:
            raise ClaimScopeRejected("Worker has no enabled repository scope.")
        return scope

    async def _timeouts(self, session: AsyncSession) -> None:
        await session.execute(
            text(
                "SELECT set_config('lock_timeout', '2000', true), "
                "set_config('statement_timeout', '5000', true)"
            )
        )

    async def _clock(self, session: AsyncSession) -> datetime:
        value = (await session.execute(select(func.clock_timestamp()))).scalar_one()
        if not isinstance(value, datetime) or value.utcoffset() != timedelta(0):
            raise ClaimPersistenceError("Coordination clock must be UTC.")
        return value

    async def _slot(self, session: AsyncSession, *, lock: bool) -> DispatchSlotRecord:
        query = select(DispatchSlotRecord).where(DispatchSlotRecord.name == "coding")
        if lock:
            query = query.with_for_update()
        slot = (await session.scalars(query)).one_or_none()
        if slot is None:
            raise ClaimPersistenceError("Dispatch coordination is unavailable.")
        return slot

    async def _worker(
        self, session: AsyncSession, worker_id: UUID, *, lock: bool
    ) -> WorkerCoordinationRecord | None:
        query = select(WorkerCoordinationRecord).where(
            WorkerCoordinationRecord.worker_id == worker_id
        )
        if lock:
            query = query.with_for_update()
        return (await session.scalars(query)).one_or_none()

    def _admit(
        self,
        slot: DispatchSlotRecord,
        worker: WorkerCoordinationRecord | None,
        now: datetime,
    ) -> None:
        if (
            slot.receipt_count >= self._limits.global_receipts
            or (worker is None and slot.worker_count >= self._limits.historical_workers)
            or (
                worker is not None
                and (
                    worker.receipt_count >= self._limits.worker_receipts
                    or now < worker.next_poll_at
                )
            )
        ):
            raise ClaimAdmissionRejected("New claim exceeds polling or storage limits.")

    async def _stored(self, session: AsyncSession, job_id: UUID) -> StoredJobSubmission:
        key = (
            await session.execute(
                select(JobRecord.submission_key).where(JobRecord.id == job_id)
            )
        ).scalar_one()
        stored = await read_submission(session, key)
        if stored is None:
            raise ClaimPersistenceError("Claim definition is unavailable.")
        return stored

    async def _replay(
        self,
        session: AsyncSession,
        identity: ClaimRequestIdentity,
        authorization: WorkerAuthorization,
        now: datetime,
        *,
        slot: DispatchSlotRecord,
    ) -> ClaimResult | None:
        scope = self._authorize(authorization, now)
        if identity.worker_id != authorization.worker_id:
            raise ClaimScopeRejected(
                "Claim identity requires the authenticated worker."
            )
        receipt = (
            await session.scalars(
                select(ClaimReceiptRecord).where(
                    ClaimReceiptRecord.worker_id == identity.worker_id,
                    ClaimReceiptRecord.claim_request_id == identity.claim_request_id,
                )
            )
        ).one_or_none()
        if receipt is None:
            return None
        if (
            receipt.protocol_version,
            receipt.fingerprint_version,
            receipt.verifier_version,
        ) != (1, 1, 1):
            raise ClaimPersistenceError("Claim receipt version is unsupported.")
        accepted = ClaimRequestIdentity(
            worker_id=receipt.worker_id,
            claim_request_id=receipt.claim_request_id,
            protocol_version=1,
            fingerprint_version=1,
            fingerprint=receipt.fingerprint,
            verifier=LeaseProofVerifier(
                worker_id=receipt.worker_id,
                claim_request_id=receipt.claim_request_id,
                digest=receipt.verifier,
            ),
        )
        accepted.require_replay(identity)
        if receipt.outcome == "no_work":
            if (
                receipt.job_id is not None
                or receipt.attempt_id is not None
                or receipt.no_work_reason is None
            ):
                raise ClaimPersistenceError("No-work receipt is inconsistent.")
            self._authorize(authorization, await self._clock(session))
            return ClaimResult(
                recorded_at=receipt.recorded_at,
                recovered=True,
                no_work=NoWorkReason(receipt.no_work_reason),
            )
        if (
            receipt.outcome != "assigned"
            or receipt.job_id is None
            or receipt.attempt_id is None
            or receipt.no_work_reason is not None
        ):
            raise ClaimPersistenceError("Assignment receipt is inconsistent.")
        record, authority = (
            await session.execute(
                select(ExecutionAttemptRecord, AttemptLeaseAuthorityRecord)
                .outerjoin(
                    AttemptLeaseAuthorityRecord,
                    AttemptLeaseAuthorityRecord.attempt_id == ExecutionAttemptRecord.id,
                )
                .where(ExecutionAttemptRecord.id == receipt.attempt_id)
            )
        ).one()
        if authority is None:
            raise ClaimPersistenceError("Current assignment authority is unavailable.")
        if (
            record.worker_id,
            record.job_id,
            record.claim_request_id,
            record.issued_at,
        ) != (
            receipt.worker_id,
            receipt.job_id,
            receipt.claim_request_id,
            receipt.recorded_at,
        ):
            raise ClaimPersistenceError("Assignment ownership is inconsistent.")
        stored = await self._stored(session, receipt.job_id)
        if stored.job.repository_id not in scope:
            raise ClaimScopeRejected("Assignment is outside current worker scope.")
        attempt = ExecutionAttempt(
            id=record.id,
            job_id=record.job_id,
            number=record.attempt_number,
            status=AttemptStatus(record.status),
            updated_at=record.updated_at,
            acknowledged_at=record.acknowledged_at,
            ended_at=record.ended_at,
            lease=Lease(
                attempt_id=record.id,
                worker_id=record.worker_id,
                generation=record.generation,
                issued_at=record.issued_at,
                # Phase timestamps advance beyond the original lease deadline.
                # Preserve the immutable claim/ACK anchors on historical replay.
                updated_at=record.revoked_at
                or record.acknowledged_at
                or record.issued_at,
                expires_at=record.expires_at,
                revoked_at=record.revoked_at,
            ),
        )
        now = await self._clock(session)
        self._authorize(authorization, now)
        return ClaimResult(
            recorded_at=receipt.recorded_at,
            recovered=True,
            stored=stored,
            attempt=attempt,
            lease_authority_active=now < authority.current_expires_at
            and authority.revoked_at is None
            and attempt.reserves_execution
            and slot.reserved_attempt_id == attempt.id,
        )

    async def recover(
        self, identity: ClaimRequestIdentity, authorization: WorkerAuthorization
    ) -> ClaimResult | None:
        try:
            async with self._sessions.begin() as session:
                await self._timeouts(session)
                slot = await self._slot(session, lock=True)
                await self._worker(session, authorization.worker_id, lock=True)
                now = await self._clock(session)
                result = await self._replay(
                    session, identity, authorization, now, slot=slot
                )
            return result
        except ClaimReplayConflict:
            raise
        except SQLAlchemyError, ValueError, TypeError, JobPersistenceError:
            raise ClaimPersistenceError("Claim recovery storage failed.") from None

    async def check_admission(self, authorization: WorkerAuthorization) -> None:
        try:
            async with self._sessions() as session:
                slot = await self._slot(session, lock=False)
                worker = await self._worker(
                    session, authorization.worker_id, lock=False
                )
                now = await self._clock(session)
                self._authorize(authorization, now)
                self._admit(slot, worker, now)
        except SQLAlchemyError, ValueError, TypeError:
            raise ClaimPersistenceError("Claim admission storage failed.") from None

    async def acknowledge(
        self,
        identity: AcknowledgementIdentity,
        authorization: WorkerAuthorization,
        *,
        definition_digest: str,
    ) -> AcknowledgementResult:
        try:
            async with self._sessions.begin() as session:
                await self._timeouts(session)
                slot = await self._slot(session, lock=True)
                await self._worker(session, authorization.worker_id, lock=True)
                recovered = await self._replay(
                    session,
                    identity.claim,
                    authorization,
                    await self._clock(session),
                    slot=slot,
                )
                if (
                    recovered is None
                    or recovered.stored is None
                    or recovered.attempt is None
                ):
                    raise AcknowledgementRejected("Original assignment is required.")
                stored, attempt, intent = (
                    recovered.stored,
                    recovered.attempt,
                    identity.intent,
                )
                if (
                    attempt.id != intent.attempt_id
                    or stored.job.snapshot_id != intent.snapshot_id
                    or stored.job.base_revision != intent.base_revision
                    or definition_digest != intent.definition_digest
                ):
                    raise AcknowledgementRejected(
                        "Acknowledgement differs from original assignment."
                    )
                await session.execute(
                    select(SourceTaskRecord.id)
                    .where(SourceTaskRecord.id == stored.job.source_task_id)
                    .with_for_update()
                )
                workflow = (
                    await session.scalars(
                        select(JobWorkflowRecord)
                        .where(JobWorkflowRecord.job_id == stored.job.id)
                        .with_for_update()
                    )
                ).one()
                record, authority = (
                    await session.execute(
                        select(ExecutionAttemptRecord, AttemptLeaseAuthorityRecord)
                        .join(
                            AttemptLeaseAuthorityRecord,
                            AttemptLeaseAuthorityRecord.attempt_id
                            == ExecutionAttemptRecord.id,
                        )
                        .where(ExecutionAttemptRecord.id == attempt.id)
                        .with_for_update(of=ExecutionAttemptRecord)
                        .execution_options(populate_existing=True)
                    )
                ).one()
                now = await self._clock(session)
                scope = self._authorize(authorization, now)
                if stored.job.repository_id not in scope:
                    raise ClaimScopeRejected(
                        "Acknowledgement is outside current scope."
                    )
                receipt = (
                    await session.scalars(
                        select(AcknowledgementReceiptRecord).where(
                            AcknowledgementReceiptRecord.worker_id
                            == authorization.worker_id,
                            AcknowledgementReceiptRecord.request_id
                            == intent.acknowledgement_request_id,
                        )
                    )
                ).one_or_none()
                if receipt is not None:
                    identity.require_replay(receipt.fingerprint)
                    if (
                        receipt.attempt_id != attempt.id
                        or receipt.job_id != stored.job.id
                        or receipt.claim_request_id != intent.claim_request_id
                        or receipt.snapshot_id != intent.snapshot_id
                        or receipt.base_revision != intent.base_revision
                        or receipt.definition_digest != intent.definition_digest
                        or receipt.repository_id != stored.job.repository_id
                        or record.acknowledged_at != receipt.acknowledged_at
                        or receipt.workflow_version != 2
                        or receipt.expected_workflow_version != 1
                        or receipt.generation != intent.generation
                        or receipt.protocol_version != intent.protocol_version
                        or receipt.definition_digest_version
                        != intent.definition_digest_version
                        or receipt.fingerprint_version != 1
                    ):
                        raise ClaimPersistenceError(
                            "Acknowledgement history is inconsistent."
                        )
                    result = AcknowledgementResult(
                        worker_id=authorization.worker_id,
                        job_id=stored.job.id,
                        repository_id=stored.job.repository_id,
                        intent=intent,
                        acknowledged_at=receipt.acknowledged_at,
                        expires_at=record.expires_at,
                        recovered=True,
                        lease_authority_active=now < authority.current_expires_at
                        and authority.revoked_at is None,
                    )
                else:
                    if (
                        slot.reserved_attempt_id != attempt.id
                        or record.status != AttemptStatus.CLAIMED.value
                        or record.acknowledged_at is not None
                        or record.worker_id != authorization.worker_id
                        or record.claim_request_id != intent.claim_request_id
                        or record.generation != intent.generation
                        or now >= record.expires_at
                        or workflow.workflow_version != intent.expected_workflow_version
                        or workflow.status != JobStatus.CLAIMED.value
                        or workflow.active_attempt_id != attempt.id
                        or workflow.updated_at != record.issued_at
                    ):
                        raise AcknowledgementRejected(
                            "Initial acknowledgement authority is unavailable."
                        )
                    acknowledged = attempt.acknowledge(
                        worker_id=authorization.worker_id,
                        generation=intent.generation,
                        now=now,
                    )
                    job = stored.job.claim(
                        attempt.id, now=attempt.lease.issued_at
                    ).begin_execution(attempt.id, now=now)
                    await session.execute(
                        update(ExecutionAttemptRecord)
                        .where(
                            ExecutionAttemptRecord.id == attempt.id,
                            ExecutionAttemptRecord.status
                            == AttemptStatus.CLAIMED.value,
                        )
                        .values(
                            status=acknowledged.status.value,
                            updated_at=now,
                            acknowledged_at=now,
                        )
                    )
                    await session.execute(
                        update(JobWorkflowRecord)
                        .where(
                            JobWorkflowRecord.job_id == stored.job.id,
                            JobWorkflowRecord.workflow_version
                            == intent.expected_workflow_version,
                        )
                        .values(
                            status=job.status.value,
                            workflow_version=job.workflow_version,
                            updated_at=now,
                        )
                    )
                    await session.execute(
                        insert(JobWorkflowEventRecord).values(
                            job_id=stored.job.id,
                            workflow_version=2,
                            status=JobStatus.RUNNING.value,
                            event_type=WorkflowEventType.ACKNOWLEDGED.value,
                            occurred_at=now,
                            attempt_id=attempt.id,
                            previous_version=1,
                            previous_status=JobStatus.CLAIMED.value,
                        )
                    )
                    await session.execute(
                        insert(AcknowledgementReceiptRecord).values(
                            worker_id=authorization.worker_id,
                            request_id=intent.acknowledgement_request_id,
                            attempt_id=attempt.id,
                            job_id=stored.job.id,
                            repository_id=stored.job.repository_id,
                            claim_request_id=intent.claim_request_id,
                            generation=intent.generation,
                            snapshot_id=intent.snapshot_id,
                            base_revision=intent.base_revision,
                            definition_digest=intent.definition_digest,
                            definition_digest_version=1,
                            protocol_version=1,
                            fingerprint_version=1,
                            fingerprint=identity.fingerprint,
                            expected_workflow_version=1,
                            workflow_version=2,
                            acknowledged_at=now,
                        )
                    )
                    result = AcknowledgementResult(
                        worker_id=authorization.worker_id,
                        job_id=stored.job.id,
                        repository_id=stored.job.repository_id,
                        intent=intent,
                        acknowledged_at=now,
                        expires_at=record.expires_at,
                        recovered=False,
                        lease_authority_active=True,
                    )
                disclosure_time = await self._clock(session)
                self._authorize(authorization, disclosure_time)
                result = replace(
                    result,
                    lease_authority_active=disclosure_time
                    < authority.current_expires_at
                    and authority.revoked_at is None
                    and slot.reserved_attempt_id == attempt.id
                    and record.ended_at is None,
                )
            return result
        except ClaimReplayConflict:
            raise
        except SQLAlchemyError, ValueError, TypeError, JobPersistenceError:
            raise ClaimPersistenceError("Acknowledgement storage failed.") from None

    async def heartbeat(
        self, identity: HeartbeatIdentity, authorization: WorkerAuthorization
    ) -> HeartbeatResult:
        """Serialize current authority and retained identity without changing anchors."""
        try:
            async with self._sessions.begin() as session:
                await self._timeouts(session)
                slot = await self._slot(session, lock=True)
                worker = await self._worker(session, authorization.worker_id, lock=True)
                self._authorize(authorization, await self._clock(session))
                if identity.claim.worker_id != authorization.worker_id:
                    raise ClaimScopeRejected(
                        "Heartbeat requires the authenticated worker."
                    )
                receipt = (
                    await session.scalars(
                        select(HeartbeatReceiptRecord).where(
                            HeartbeatReceiptRecord.worker_id == authorization.worker_id,
                            HeartbeatReceiptRecord.request_id
                            == identity.heartbeat_request_id,
                        )
                    )
                ).one_or_none()
                if receipt is not None:
                    identity.require_replay(receipt.fingerprint)
                recovered = await self._replay(
                    session,
                    identity.claim,
                    authorization,
                    await self._clock(session),
                    slot=slot,
                )
                if (
                    recovered is None
                    or recovered.attempt is None
                    or recovered.stored is None
                ):
                    raise HeartbeatRejected(
                        "Heartbeat requires an original assignment."
                    )
                stored, attempt = recovered.stored, recovered.attempt
                if (
                    worker is None
                    or identity.attempt_id != attempt.id
                    or identity.generation != attempt.lease.generation
                ):
                    raise HeartbeatRejected(
                        "Heartbeat differs from original ownership."
                    )
                await session.execute(
                    select(SourceTaskRecord.id)
                    .where(SourceTaskRecord.id == stored.job.source_task_id)
                    .with_for_update()
                )
                workflow = (
                    await session.scalars(
                        select(JobWorkflowRecord)
                        .where(JobWorkflowRecord.job_id == stored.job.id)
                        .with_for_update()
                    )
                ).one()
                record = (
                    await session.scalars(
                        select(ExecutionAttemptRecord)
                        .where(ExecutionAttemptRecord.id == attempt.id)
                        .with_for_update()
                        .execution_options(populate_existing=True)
                    )
                ).one()
                authority = (
                    await session.scalars(
                        select(AttemptLeaseAuthorityRecord)
                        .where(AttemptLeaseAuthorityRecord.attempt_id == attempt.id)
                        .with_for_update()
                        .execution_options(populate_existing=True)
                    )
                ).one()
                now = await self._clock(session)
                scope = self._authorize(authorization, now)
                if stored.job.repository_id not in scope:
                    raise ClaimScopeRejected("Heartbeat is outside current scope.")
                reference = AssignmentReference(
                    worker_id=record.worker_id,
                    job_id=record.job_id,
                    repository_id=stored.job.repository_id,
                    attempt_id=record.id,
                    generation=record.generation,
                    issued_at=record.issued_at,
                    expires_at=record.expires_at,
                )
                renewed = False
                if receipt is not None:
                    if (
                        (
                            receipt.worker_id,
                            receipt.attempt_id,
                            receipt.job_id,
                            receipt.repository_id,
                            receipt.claim_request_id,
                            receipt.generation,
                        )
                        != (
                            authorization.worker_id,
                            record.id,
                            record.job_id,
                            stored.job.repository_id,
                            identity.claim.claim_request_id,
                            record.generation,
                        )
                        or (receipt.protocol_version, receipt.fingerprint_version)
                        != (1, 1)
                        or receipt.expected_lease_revision
                        != identity.expected_lease_revision
                        or receipt.lease_revision
                        != receipt.expected_lease_revision + int(receipt.renewed)
                        or receipt.attempt_ordinal > authority.receipt_count
                        or receipt.worker_ordinal > worker.heartbeat_receipt_count
                        or receipt.global_ordinal > slot.heartbeat_receipt_count
                        or type(receipt.renewed) is not bool
                        or record.acknowledged_at is None
                        or receipt.recorded_at < record.acknowledged_at
                        or any(
                            type(value) is not int or not 1 <= value < 1 << 63
                            for value in (
                                receipt.attempt_ordinal,
                                receipt.worker_ordinal,
                                receipt.global_ordinal,
                                receipt.lease_revision,
                                receipt.expected_lease_revision,
                            )
                        )
                    ):
                        raise ClaimPersistenceError(
                            "Heartbeat recovery history is inconsistent."
                        )
                    LeaseAuthorityObservation(
                        reference=reference,
                        lease_revision=receipt.lease_revision,
                        observed_at=receipt.recorded_at,
                        updated_at=receipt.updated_at,
                        current_expires_at=receipt.current_expires_at,
                    )
                else:
                    running = (
                        record.status == AttemptStatus.RUNNING.value
                        and workflow.status == JobStatus.RUNNING.value
                        and workflow.workflow_version == 2
                    )
                    publishing = (
                        record.status == AttemptStatus.PUBLISHING_PR.value
                        and workflow.status == JobStatus.PUBLISHING_PR.value
                        and workflow.workflow_version == 4 + 2 * workflow.repair_cycles
                    )
                    if (
                        slot.reserved_attempt_id != attempt.id
                        or not (running or publishing)
                        or workflow.active_attempt_id != attempt.id
                        or record.acknowledged_at is None
                    ):
                        raise HeartbeatRejected(
                            "Heartbeat requires active acknowledged work."
                        )
                    if (
                        now >= authority.current_expires_at
                        or authority.revoked_at is not None
                        or identity.expected_lease_revision != authority.lease_revision
                    ):
                        raise HeartbeatRejected(
                            "Current heartbeat authority is unavailable."
                        )
                    limits = self._heartbeat_limits
                    if (
                        now < authority.next_heartbeat_at
                        or authority.receipt_count >= limits.attempt_receipts
                        or worker.heartbeat_receipt_count >= limits.worker_receipts
                        or slot.heartbeat_receipt_count >= limits.global_receipts
                    ):
                        raise HeartbeatAdmissionRejected(
                            "New heartbeat exceeds rate or storage limits."
                        )
                    expiry = min(
                        now + timedelta(seconds=limits.lease_seconds),
                        authorization.credential_expires_at,
                    )
                    renewed = expiry > authority.current_expires_at
                    revision = authority.lease_revision + int(renewed)
                    if revision >= 1 << 63:
                        raise HeartbeatRejected("Lease revision has reached its bound.")
                    updated = now if renewed else authority.updated_at
                    expiry = expiry if renewed else authority.current_expires_at
                    attempt_ordinal = authority.receipt_count + 1
                    worker_ordinal = worker.heartbeat_receipt_count + 1
                    global_ordinal = slot.heartbeat_receipt_count + 1
                    await session.execute(
                        update(AttemptLeaseAuthorityRecord)
                        .where(
                            AttemptLeaseAuthorityRecord.attempt_id == attempt.id,
                            AttemptLeaseAuthorityRecord.lease_revision
                            == identity.expected_lease_revision,
                            AttemptLeaseAuthorityRecord.receipt_count
                            == authority.receipt_count,
                        )
                        .values(
                            lease_revision=revision,
                            updated_at=updated,
                            current_expires_at=expiry,
                            receipt_count=attempt_ordinal,
                            next_heartbeat_at=now
                            + timedelta(seconds=limits.minimum_interval_seconds),
                        )
                    )
                    await session.execute(
                        update(WorkerCoordinationRecord)
                        .where(
                            WorkerCoordinationRecord.worker_id
                            == authorization.worker_id
                        )
                        .values(heartbeat_receipt_count=worker_ordinal)
                    )
                    await session.execute(
                        update(DispatchSlotRecord)
                        .where(DispatchSlotRecord.name == "coding")
                        .values(heartbeat_receipt_count=global_ordinal)
                    )
                    await session.execute(
                        insert(HeartbeatReceiptRecord).values(
                            worker_id=authorization.worker_id,
                            request_id=identity.heartbeat_request_id,
                            attempt_id=record.id,
                            job_id=record.job_id,
                            repository_id=stored.job.repository_id,
                            claim_request_id=record.claim_request_id,
                            generation=record.generation,
                            protocol_version=1,
                            fingerprint_version=1,
                            fingerprint=identity.fingerprint,
                            expected_lease_revision=identity.expected_lease_revision,
                            lease_revision=revision,
                            updated_at=updated,
                            current_expires_at=expiry,
                            renewed=renewed,
                            recorded_at=now,
                            attempt_ordinal=attempt_ordinal,
                            worker_ordinal=worker_ordinal,
                            global_ordinal=global_ordinal,
                        )
                    )
                disclosure = await self._clock(session)
                self._authorize(authorization, disclosure)
                observation = LeaseAuthorityObservation(
                    reference=reference,
                    lease_revision=authority.lease_revision,
                    observed_at=disclosure,
                    updated_at=authority.updated_at,
                    current_expires_at=authority.current_expires_at,
                    revoked_at=authority.revoked_at,
                )
                stop = (
                    observation.revoked_at is not None
                    or disclosure >= observation.current_expires_at
                    or slot.reserved_attempt_id != attempt.id
                    or record.ended_at is not None
                )
                result = HeartbeatResult(
                    heartbeat_request_id=identity.heartbeat_request_id,
                    authority=observation,
                    renewed=renewed and not stop,
                    stop_required=stop,
                )
            return result
        except ClaimReplayConflict:
            raise
        except SQLAlchemyError, ValueError, TypeError, JobPersistenceError:
            raise ClaimPersistenceError("Heartbeat storage failed.") from None

    async def claim(
        self,
        identity: ClaimRequestIdentity,
        authorization: WorkerAuthorization,
        observation: PreparedJobClaim | None,
    ) -> ClaimResult:
        try:
            async with self._sessions.begin() as session:
                await self._timeouts(session)
                slot = await self._slot(session, lock=True)
                worker = await self._worker(session, authorization.worker_id, lock=True)
                now = await self._clock(session)
                recovered = await self._replay(
                    session, identity, authorization, now, slot=slot
                )
                if recovered is not None:
                    return recovered
                self._admit(slot, worker, now)
                stored: StoredJobSubmission | None = None
                if observation is not None:
                    if observation.worker_id != authorization.worker_id:
                        raise ClaimScopeRejected(
                            "Observation belongs to another worker."
                        )
                    job = observation.stored.job
                    await session.execute(
                        select(SourceTaskRecord.id)
                        .where(SourceTaskRecord.id == job.source_task_id)
                        .with_for_update()
                    )
                    await session.execute(
                        select(JobWorkflowRecord.job_id)
                        .where(JobWorkflowRecord.job_id == job.id)
                        .with_for_update()
                    )
                    stored = await self._stored(session, job.id)
                # These separate statements run after every lock wait and bounded read.
                now = await self._clock(session)
                scope = self._authorize(authorization, now)
                self._admit(slot, worker, now)
                reason: NoWorkReason | None = NoWorkReason.EMPTY
                if stored is not None and observation is not None:
                    if stored.job.repository_id not in scope:
                        raise ClaimScopeRejected(
                            "Candidate is outside current worker scope."
                        )
                    policy = self._policies[stored.job.repository_id]
                    current = PreparedJobSubmission(
                        stored=stored.intent.stored,
                        policy=policy,
                        actor_id=stored.intent.actor_id,
                    )
                    if (
                        current.submission_fingerprint
                        != stored.intent.submission_fingerprint
                    ):
                        raise ClaimEligibilityRejected(
                            "Repository policy changed after observation."
                        )
                    reason = NoWorkReason.CHANGED
                    if stored == observation.stored:
                        try:
                            observation.require_fresh(now=now)
                        except ClaimEligibilityRejected:
                            reason = NoWorkReason.STALE
                        else:
                            reason = (
                                NoWorkReason.OCCUPIED
                                if slot.reserved_attempt_id is not None
                                else None
                            )
                elif slot.reserved_attempt_id is not None:
                    reason = NoWorkReason.OCCUPIED
                result = await self._record(
                    session,
                    identity,
                    authorization,
                    slot,
                    worker,
                    stored if reason is None else None,
                    reason,
                    now,
                )
            return result
        except ClaimReplayConflict:
            raise
        except SQLAlchemyError, ValueError, TypeError, JobPersistenceError:
            raise ClaimPersistenceError(
                "Claim transaction failed; recover the original request."
            ) from None

    async def _record(
        self,
        session: AsyncSession,
        identity: ClaimRequestIdentity,
        authorization: WorkerAuthorization,
        slot: DispatchSlotRecord,
        worker: WorkerCoordinationRecord | None,
        stored: StoredJobSubmission | None,
        reason: NoWorkReason | None,
        now: datetime,
    ) -> ClaimResult:
        if worker is None:
            await session.execute(
                insert(WorkerCoordinationRecord).values(
                    worker_id=identity.worker_id,
                    enrolled_at=now,
                    next_poll_at=now + timedelta(seconds=30),
                    receipt_count=1,
                    version=1,
                )
            )
        else:
            await session.execute(
                update(WorkerCoordinationRecord)
                .where(WorkerCoordinationRecord.worker_id == identity.worker_id)
                .values(
                    next_poll_at=now + timedelta(seconds=30),
                    receipt_count=worker.receipt_count + 1,
                    version=worker.version + 1,
                )
            )
        attempt: ExecutionAttempt | None = None
        if stored is not None:
            attempt_id = uuid4()
            claimed = stored.job.claim(attempt_id, now=now)
            lease = Lease(
                attempt_id=attempt_id,
                worker_id=identity.worker_id,
                generation=1,
                issued_at=now,
                updated_at=now,
                expires_at=min(
                    now + timedelta(seconds=self._limits.lease_seconds),
                    authorization.credential_expires_at,
                ),
            )
            attempt = ExecutionAttempt(
                id=attempt_id, job_id=claimed.id, number=1, lease=lease, updated_at=now
            )
            await session.execute(
                insert(ExecutionAttemptRecord).values(
                    id=attempt.id,
                    job_id=attempt.job_id,
                    worker_id=lease.worker_id,
                    claim_request_id=identity.claim_request_id,
                    attempt_number=1,
                    generation=1,
                    status=attempt.status.value,
                    issued_at=now,
                    updated_at=now,
                    expires_at=lease.expires_at,
                )
            )
            await session.execute(
                insert(AttemptLeaseAuthorityRecord).values(
                    attempt_id=attempt.id,
                    job_id=attempt.job_id,
                    worker_id=lease.worker_id,
                    claim_request_id=identity.claim_request_id,
                    generation=lease.generation,
                    lease_revision=1,
                    updated_at=now,
                    current_expires_at=lease.expires_at,
                    revoked_at=None,
                    receipt_count=0,
                    next_heartbeat_at=now,
                )
            )
            await session.execute(
                update(JobWorkflowRecord)
                .where(
                    JobWorkflowRecord.job_id == claimed.id,
                    JobWorkflowRecord.workflow_version == stored.workflow.version,
                )
                .values(
                    status=claimed.status.value,
                    workflow_version=claimed.workflow_version,
                    updated_at=now,
                    active_attempt_id=attempt.id,
                    last_attempt_id=attempt.id,
                    last_attempt_number=1,
                )
            )
            await session.execute(
                insert(JobWorkflowEventRecord).values(
                    job_id=claimed.id,
                    workflow_version=claimed.workflow_version,
                    status=claimed.status.value,
                    event_type=WorkflowEventType.CLAIMED.value,
                    occurred_at=now,
                    attempt_id=attempt.id,
                    previous_version=0,
                    previous_status=JobStatus.QUEUED.value,
                )
            )
            stored = StoredJobSubmission(
                intent=stored.intent,
                job=stored.job,
                workflow=JobWorkflowState(
                    job_id=claimed.id,
                    status=claimed.status,
                    version=claimed.workflow_version,
                    blocks_new_submission=True,
                    created_at=claimed.created_at,
                    updated_at=now,
                ),
            )
        await session.execute(
            update(DispatchSlotRecord)
            .where(DispatchSlotRecord.name == "coding")
            .values(
                reserved_attempt_id=attempt.id if attempt else slot.reserved_attempt_id,
                receipt_count=slot.receipt_count + 1,
                worker_count=slot.worker_count + (worker is None),
                version=slot.version + 1,
            )
        )
        await session.execute(
            insert(ClaimReceiptRecord).values(
                worker_id=identity.worker_id,
                claim_request_id=identity.claim_request_id,
                protocol_version=1,
                fingerprint_version=1,
                verifier_version=1,
                fingerprint=identity.fingerprint,
                verifier=identity.verifier.digest,
                outcome="assigned" if attempt else "no_work",
                no_work_reason=reason.value if reason else None,
                job_id=attempt.job_id if attempt else None,
                attempt_id=attempt.id if attempt else None,
                recorded_at=now,
            )
        )
        return ClaimResult(
            recorded_at=now,
            recovered=False,
            stored=stored,
            attempt=attempt,
            no_work=reason,
            lease_authority_active=attempt is not None,
        )
