from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKeyConstraint,
    Index,
    LargeBinary,
    SmallInteger,
    String,
    UniqueConstraint,
    Uuid,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from devrelay.infrastructure.persistence.models import Base


class WorkerCoordinationRecord(Base):
    __tablename__ = "worker_coordination"
    __table_args__ = (
        CheckConstraint("receipt_count > 0"),
        CheckConstraint("version = receipt_count"),
        CheckConstraint("next_poll_at >= enrolled_at"),
        CheckConstraint("heartbeat_receipt_count >= 0"),
    )
    worker_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    enrolled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    next_poll_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    receipt_count: Mapped[int] = mapped_column(BigInteger)
    version: Mapped[int] = mapped_column(BigInteger)
    heartbeat_receipt_count: Mapped[int] = mapped_column(BigInteger, server_default="0")


class DispatchSlotRecord(Base):
    __tablename__ = "dispatch_slots"
    __table_args__ = (
        CheckConstraint("name = 'coding'"),
        CheckConstraint("receipt_count >= 0"),
        CheckConstraint("worker_count BETWEEN 0 AND 256"),
        CheckConstraint(
            "release_count >= 0 AND release_count <= receipt_count",
            name="ck_dispatch_release_count",
        ),
        CheckConstraint(
            "version = receipt_count + release_count", name="ck_dispatch_slots_version"
        ),
        CheckConstraint(
            "(release_count = 0 AND last_released_result_id IS NULL) OR (release_count > 0 AND last_released_result_id IS NOT NULL)",
            name="ck_dispatch_slots_release",
        ),
        ForeignKeyConstraint(
            ["last_released_result_id", "release_count"],
            ["delivery_operations.result_id", "delivery_operations.release_ordinal"],
            name="fk_dispatch_last_release",
            deferrable=True,
            initially="DEFERRED",
            use_alter=True,
        ),
        CheckConstraint("worker_count <= receipt_count"),
        CheckConstraint("heartbeat_receipt_count >= 0"),
        ForeignKeyConstraint(["reserved_attempt_id"], ["execution_attempts.id"]),
    )
    name: Mapped[str] = mapped_column(String(16), primary_key=True)
    reserved_attempt_id: Mapped[UUID | None] = mapped_column(Uuid)
    receipt_count: Mapped[int] = mapped_column(BigInteger)
    worker_count: Mapped[int] = mapped_column(BigInteger)
    version: Mapped[int] = mapped_column(BigInteger)
    heartbeat_receipt_count: Mapped[int] = mapped_column(BigInteger, server_default="0")

    release_count: Mapped[int] = mapped_column(BigInteger, server_default="0")
    last_released_result_id: Mapped[UUID | None] = mapped_column(Uuid)


class ExecutionAttemptRecord(Base):
    __tablename__ = "execution_attempts"
    __table_args__ = (
        ForeignKeyConstraint(["job_id"], ["job_workflows.job_id"]),
        ForeignKeyConstraint(["worker_id"], ["worker_coordination.worker_id"]),
        ForeignKeyConstraint(
            ["worker_id", "claim_request_id"],
            [
                "worker_claim_receipts.worker_id",
                "worker_claim_receipts.claim_request_id",
            ],
            name="fk_attempt_original_claim",
            deferrable=True,
            initially="DEFERRED",
            use_alter=True,
        ),
        UniqueConstraint("job_id", "attempt_number"),
        UniqueConstraint("id", "job_id"),
        UniqueConstraint("id", "job_id", "worker_id", "claim_request_id"),
        CheckConstraint("attempt_number = 1"),
        CheckConstraint("generation = 1"),
        CheckConstraint(
            "expires_at > issued_at AND "
            "((status = 'claimed' AND updated_at = issued_at AND acknowledged_at IS NULL AND revoked_at IS NULL AND ended_at IS NULL) "
            "OR (acknowledged_at IS NOT NULL AND acknowledged_at >= issued_at AND acknowledged_at < expires_at "
            "AND updated_at >= acknowledged_at AND ((status IN ('running', 'publishing_pr') AND revoked_at IS NULL AND ended_at IS NULL) "
            "OR (status = 'succeeded' AND revoked_at IS NOT NULL AND ended_at IS NOT NULL AND revoked_at = ended_at "
            "AND ended_at = updated_at AND ended_at >= acknowledged_at))))",
            name="ck_execution_attempts_ack_state",
        ),
        Index(
            "uq_execution_attempts_open_job",
            "job_id",
            unique=True,
            postgresql_where=text("ended_at IS NULL"),
        ),
        Index(
            "uq_execution_attempts_open_worker",
            "worker_id",
            unique=True,
            postgresql_where=text("ended_at IS NULL"),
        ),
    )
    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    job_id: Mapped[UUID] = mapped_column(Uuid)
    worker_id: Mapped[UUID] = mapped_column(Uuid)
    claim_request_id: Mapped[UUID] = mapped_column(Uuid)
    attempt_number: Mapped[int] = mapped_column(BigInteger)
    generation: Mapped[int] = mapped_column(BigInteger)
    status: Mapped[str] = mapped_column(String(32))
    issued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    acknowledged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ClaimReceiptRecord(Base):
    __tablename__ = "worker_claim_receipts"
    __table_args__ = (
        ForeignKeyConstraint(["worker_id"], ["worker_coordination.worker_id"]),
        ForeignKeyConstraint(
            ["attempt_id", "job_id", "worker_id", "claim_request_id"],
            [
                "execution_attempts.id",
                "execution_attempts.job_id",
                "execution_attempts.worker_id",
                "execution_attempts.claim_request_id",
            ],
            deferrable=True,
            initially="DEFERRED",
        ),
        CheckConstraint("protocol_version = 1"),
        CheckConstraint("fingerprint_version = 1"),
        CheckConstraint("verifier_version = 1"),
        CheckConstraint("fingerprint ~ '^[0-9a-f]{64}$'"),
        CheckConstraint("octet_length(verifier) = 32"),
        CheckConstraint("outcome IN ('assigned', 'no_work')"),
        CheckConstraint(
            "(outcome = 'no_work' AND job_id IS NULL AND attempt_id IS NULL "
            "AND no_work_reason IS NOT NULL AND no_work_reason IN ('empty', 'occupied', 'changed', 'stale')) "
            "OR (outcome = 'assigned' AND job_id IS NOT NULL AND attempt_id IS NOT NULL AND no_work_reason IS NULL)"
        ),
    )
    worker_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    claim_request_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    protocol_version: Mapped[int] = mapped_column(SmallInteger)
    fingerprint_version: Mapped[int] = mapped_column(SmallInteger)
    verifier_version: Mapped[int] = mapped_column(SmallInteger)
    fingerprint: Mapped[str] = mapped_column(String(64))
    verifier: Mapped[bytes] = mapped_column(LargeBinary)
    outcome: Mapped[str] = mapped_column(String(16))
    no_work_reason: Mapped[str | None] = mapped_column(String(16))
    job_id: Mapped[UUID | None] = mapped_column(Uuid)
    attempt_id: Mapped[UUID | None] = mapped_column(Uuid)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
