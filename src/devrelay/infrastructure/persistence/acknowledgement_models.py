from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    BigInteger,
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


class AcknowledgementReceiptRecord(Base):
    __tablename__ = "worker_acknowledgement_receipts"
    __table_args__ = (
        UniqueConstraint("attempt_id"),
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
        ForeignKeyConstraint(
            ["job_id", "workflow_version"],
            ["job_workflow_events.job_id", "job_workflow_events.workflow_version"],
            deferrable=True,
            initially="DEFERRED",
        ),
        CheckConstraint(
            "generation = 1 AND expected_workflow_version = 1 AND workflow_version = 2"
        ),
        CheckConstraint(
            "definition_digest_version = 1 AND protocol_version = 1 AND fingerprint_version = 1"
        ),
        CheckConstraint(
            "base_revision ~ '^([0-9a-f]{40}|[0-9a-f]{64})$' AND definition_digest ~ '^[0-9a-f]{64}$' AND fingerprint ~ '^[0-9a-f]{64}$'"
        ),
    )
    worker_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    request_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    attempt_id: Mapped[UUID] = mapped_column(Uuid)
    job_id: Mapped[UUID] = mapped_column(Uuid)
    repository_id: Mapped[UUID] = mapped_column(Uuid)
    claim_request_id: Mapped[UUID] = mapped_column(Uuid)
    generation: Mapped[int] = mapped_column(BigInteger)
    snapshot_id: Mapped[UUID] = mapped_column(Uuid)
    base_revision: Mapped[str] = mapped_column(String(64))
    definition_digest: Mapped[str] = mapped_column(String(64))
    definition_digest_version: Mapped[int] = mapped_column(SmallInteger)
    protocol_version: Mapped[int] = mapped_column(SmallInteger)
    fingerprint_version: Mapped[int] = mapped_column(SmallInteger)
    fingerprint: Mapped[str] = mapped_column(String(64))
    expected_workflow_version: Mapped[int] = mapped_column(BigInteger)
    workflow_version: Mapped[int] = mapped_column(BigInteger)
    acknowledged_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
