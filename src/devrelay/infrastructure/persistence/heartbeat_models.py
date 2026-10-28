from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKeyConstraint,
    SmallInteger,
    String,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.orm import Mapped, mapped_column

from devrelay.infrastructure.persistence.models import Base


class AttemptLeaseAuthorityRecord(Base):
    __tablename__ = "attempt_lease_authorities"
    __table_args__ = (
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
        CheckConstraint("generation = 1 AND lease_revision > 0 AND receipt_count >= 0"),
        CheckConstraint(
            "revoked_at IS NULL OR revoked_at >= updated_at",
            name="ck_lease_authority_revocation",
        ),
        CheckConstraint(
            "current_expires_at > updated_at AND "
            "current_expires_at <= updated_at + interval '300 seconds' AND "
            "next_heartbeat_at >= updated_at AND lease_revision <= receipt_count + 1"
        ),
    )
    attempt_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    job_id: Mapped[UUID] = mapped_column(Uuid)
    worker_id: Mapped[UUID] = mapped_column(Uuid)
    claim_request_id: Mapped[UUID] = mapped_column(Uuid)
    generation: Mapped[int] = mapped_column(BigInteger)
    lease_revision: Mapped[int] = mapped_column(BigInteger)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    current_expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    receipt_count: Mapped[int] = mapped_column(BigInteger)
    next_heartbeat_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class HeartbeatReceiptRecord(Base):
    __tablename__ = "worker_heartbeat_receipts"
    __table_args__ = (
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
        UniqueConstraint("attempt_id", "attempt_ordinal"),
        UniqueConstraint("worker_id", "worker_ordinal"),
        UniqueConstraint("global_ordinal"),
        CheckConstraint(
            "generation = 1 AND protocol_version = 1 AND fingerprint_version = 1 "
            "AND fingerprint ~ '^[0-9a-f]{64}$'"
        ),
        CheckConstraint(
            "expected_lease_revision > 0 AND lease_revision > 0 AND "
            "attempt_ordinal > 0 AND worker_ordinal > 0 AND global_ordinal > 0 AND "
            "((renewed AND lease_revision = expected_lease_revision + 1 "
            "AND updated_at = recorded_at) OR "
            "(NOT renewed AND lease_revision = expected_lease_revision))"
        ),
        CheckConstraint(
            "updated_at <= recorded_at AND current_expires_at > recorded_at AND "
            "current_expires_at <= updated_at + interval '300 seconds'"
        ),
    )
    worker_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    request_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    attempt_id: Mapped[UUID] = mapped_column(Uuid)
    job_id: Mapped[UUID] = mapped_column(Uuid)
    repository_id: Mapped[UUID] = mapped_column(Uuid)
    claim_request_id: Mapped[UUID] = mapped_column(Uuid)
    generation: Mapped[int] = mapped_column(BigInteger)
    protocol_version: Mapped[int] = mapped_column(SmallInteger)
    fingerprint_version: Mapped[int] = mapped_column(SmallInteger)
    fingerprint: Mapped[str] = mapped_column(String(64))
    expected_lease_revision: Mapped[int] = mapped_column(BigInteger)
    lease_revision: Mapped[int] = mapped_column(BigInteger)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    current_expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    renewed: Mapped[bool] = mapped_column(Boolean)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    attempt_ordinal: Mapped[int] = mapped_column(BigInteger)
    worker_ordinal: Mapped[int] = mapped_column(BigInteger)
    global_ordinal: Mapped[int] = mapped_column(BigInteger)
