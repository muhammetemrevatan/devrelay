from datetime import datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import func, insert, select, text, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

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
from devrelay.application.contracts.submissions import (
    PreparedJobSubmission,
    RepositoryPolicy,
    StoredJobSubmission,
    repository_policy_map,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.contracts.workflows import JobWorkflowState, WorkflowEventType
from devrelay.application.ports.job_persistence import JobPersistenceError
from devrelay.domain.jobs.attempt import ExecutionAttempt, Lease
from devrelay.domain.jobs.values import AttemptStatus, JobStatus
from devrelay.infrastructure.persistence.claim_models import (
    ClaimReceiptRecord,
    DispatchSlotRecord,
    ExecutionAttemptRecord,
    WorkerCoordinationRecord,
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

    This supports only initial claims. Expiry never releases the slot; subsequent
    transitions need their own domain/evidence migration and trusted containment.
    """

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        policies: tuple[RepositoryPolicy, ...],
        limits: ClaimLimits | None = None,
    ) -> None:
        self._sessions = sessions
        self._policies = repository_policy_map(policies)
        self._limits = limits or ClaimLimits()

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
        record = (
            await session.scalars(
                select(ExecutionAttemptRecord).where(
                    ExecutionAttemptRecord.id == receipt.attempt_id
                )
            )
        ).one()
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
                updated_at=record.updated_at,
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
            lease_authority_active=now < attempt.lease.expires_at
            and attempt.lease.revoked_at is None
            and attempt.reserves_execution,
        )

    async def recover(
        self, identity: ClaimRequestIdentity, authorization: WorkerAuthorization
    ) -> ClaimResult | None:
        try:
            async with self._sessions.begin() as session:
                await self._timeouts(session)
                await self._slot(session, lock=True)
                await self._worker(session, authorization.worker_id, lock=True)
                now = await self._clock(session)
                result = await self._replay(session, identity, authorization, now)
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
                recovered = await self._replay(session, identity, authorization, now)
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
