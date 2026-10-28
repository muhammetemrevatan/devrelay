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


class AttemptResultRecord(Base):
    """Immutable successful coding result; canonical payload contains no proof."""

    __tablename__ = "attempt_results"
    __table_args__ = (
        UniqueConstraint("worker_id", "request_id", name="uq_attempt_results_request"),
        UniqueConstraint("attempt_id", name="uq_attempt_results_attempt"),
        UniqueConstraint("id", "job_id", name="uq_attempt_results_job"),
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
        CheckConstraint(
            "generation = 1 AND protocol_version = 1 AND fingerprint_version = 1 "
            "AND expected_workflow_version = 2 AND repair_cycles BETWEEN 0 AND 5 "
            "AND publishing_workflow_version = 4 + 2 * repair_cycles",
            name="ck_attempt_results_versions",
        ),
        CheckConstraint(
            "definition_digest ~ '^[0-9a-f]{64}$' AND claim_fingerprint ~ '^[0-9a-f]{64}$' "
            "AND fingerprint ~ '^[0-9a-f]{64}$' AND candidate_revision ~ '^[0-9a-f]{40}$' "
            "AND payload_sha256 ~ '^[0-9a-f]{64}$' "
            "AND octet_length(payload_bytes) BETWEEN 1 AND 16777216 "
            "AND encode(sha256(payload_bytes), 'hex') = payload_sha256",
            name="ck_attempt_results_payload",
        ),
    )
    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    request_id: Mapped[UUID] = mapped_column(Uuid)
    worker_id: Mapped[UUID] = mapped_column(Uuid)
    attempt_id: Mapped[UUID] = mapped_column(Uuid)
    job_id: Mapped[UUID] = mapped_column(Uuid)
    claim_request_id: Mapped[UUID] = mapped_column(Uuid)
    repository_id: Mapped[UUID] = mapped_column(Uuid)
    snapshot_id: Mapped[UUID] = mapped_column(Uuid)
    execution_id: Mapped[UUID] = mapped_column(Uuid)
    generation: Mapped[int] = mapped_column(BigInteger)
    definition_digest: Mapped[str] = mapped_column(String(64))
    claim_fingerprint: Mapped[str] = mapped_column(String(64))
    fingerprint: Mapped[str] = mapped_column(String(64))
    protocol_version: Mapped[int] = mapped_column(SmallInteger)
    fingerprint_version: Mapped[int] = mapped_column(SmallInteger)
    expected_workflow_version: Mapped[int] = mapped_column(BigInteger)
    publishing_workflow_version: Mapped[int] = mapped_column(BigInteger)
    candidate_revision: Mapped[str] = mapped_column(String(40))
    repair_cycles: Mapped[int] = mapped_column(BigInteger)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    payload_bytes: Mapped[bytes] = mapped_column(LargeBinary)
    payload_sha256: Mapped[str] = mapped_column(String(64))


class DeliveryOperationRecord(Base):
    __tablename__ = "delivery_operations"
    __table_args__ = (
        ForeignKeyConstraint(["result_id"], ["attempt_results.id"]),
        ForeignKeyConstraint(
            ["result_id", "version"],
            [
                "delivery_operation_events.result_id",
                "delivery_operation_events.sequence",
            ],
            deferrable=True,
            initially="DEFERRED",
            use_alter=True,
        ),
        UniqueConstraint("release_ordinal", name="uq_delivery_operations_release"),
        UniqueConstraint(
            "result_id", "release_ordinal", name="uq_delivery_operations_release_pair"
        ),
        CheckConstraint(
            "version BETWEEN 0 AND 2047 AND event_count = version + 1 "
            "AND state IN ('accepted', 'publishing', 'uncertain', 'finalized') "
            "AND ((state = 'accepted' AND version = 0 AND invocation_id IS NULL) "
            "OR (state <> 'accepted' AND version > 0 AND invocation_id IS NOT NULL))",
            name="ck_delivery_operations_stage",
        ),
        CheckConstraint(
            "(state <> 'finalized' AND pr_number IS NULL AND pr_repository_id IS NULL "
            "AND pr_provider_repository_id IS NULL AND pr_branch IS NULL "
            "AND pr_target_branch IS NULL AND pr_head_revision IS NULL "
            "AND completed_at IS NULL AND final_workflow_version IS NULL AND release_ordinal IS NULL) "
            "OR (state = 'finalized' AND pr_number IS NOT NULL AND pr_number > 0 "
            "AND pr_repository_id IS NOT NULL AND pr_provider_repository_id IS NOT NULL "
            "AND pr_provider_repository_id > 0 AND pr_branch IS NOT NULL AND length(pr_branch) BETWEEN 1 AND 256 "
            "AND pr_target_branch IS NOT NULL AND length(pr_target_branch) BETWEEN 1 AND 256 "
            "AND pr_branch <> pr_target_branch AND pr_head_revision IS NOT NULL "
            "AND pr_head_revision ~ '^[0-9a-f]{40}$' AND completed_at IS NOT NULL "
            "AND final_workflow_version IS NOT NULL AND final_workflow_version BETWEEN 5 AND 15 "
            "AND release_ordinal IS NOT NULL AND release_ordinal > 0)",
            name="ck_delivery_operations_terminal",
        ),
    )
    result_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    version: Mapped[int] = mapped_column(BigInteger)
    state: Mapped[str] = mapped_column(String(16))
    invocation_id: Mapped[UUID | None] = mapped_column(Uuid)
    event_count: Mapped[int] = mapped_column(BigInteger)
    pr_number: Mapped[int | None] = mapped_column(BigInteger)
    pr_repository_id: Mapped[UUID | None] = mapped_column(Uuid)
    pr_provider_repository_id: Mapped[int | None] = mapped_column(BigInteger)
    pr_branch: Mapped[str | None] = mapped_column(String(256))
    pr_target_branch: Mapped[str | None] = mapped_column(String(256))
    pr_head_revision: Mapped[str | None] = mapped_column(String(40))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    final_workflow_version: Mapped[int | None] = mapped_column(BigInteger)
    release_ordinal: Mapped[int | None] = mapped_column(BigInteger)


class DeliveryOperationEventRecord(Base):
    __tablename__ = "delivery_operation_events"
    __table_args__ = (
        ForeignKeyConstraint(
            ["result_id"],
            ["delivery_operations.result_id"],
            deferrable=True,
            initially="DEFERRED",
        ),
        CheckConstraint(
            "sequence BETWEEN 0 AND 2047 AND kind IN "
            "('accepted', 'publishing_started', 'write_planned', 'write_observed', 'uncertain', 'finalized') "
            "AND ((sequence = 0 AND kind = 'accepted' AND invocation_id IS NULL) "
            "OR (sequence > 0 AND kind <> 'accepted' AND invocation_id IS NOT NULL))",
            name="ck_delivery_operation_events_stage",
        ),
        CheckConstraint(
            "(kind IN ('accepted', 'publishing_started', 'finalized') AND step_id IS NULL "
            "AND step_kind IS NULL AND input_digest IS NULL AND observation_digest IS NULL) "
            "OR (kind IN ('write_planned', 'write_observed') AND step_id IS NOT NULL "
            "AND step_kind IS NOT NULL AND length(step_kind) BETWEEN 1 AND 128 "
            "AND input_digest IS NOT NULL AND input_digest ~ '^[0-9a-f]{64}$' "
            "AND ((kind = 'write_planned' AND observation_digest IS NULL) "
            "OR (kind = 'write_observed' AND observation_digest IS NOT NULL AND observation_digest ~ '^[0-9a-f]{64}$'))) "
            "OR (kind = 'uncertain' AND step_id IS NOT NULL AND step_kind IS NOT NULL "
            "AND length(step_kind) BETWEEN 1 AND 128 AND input_digest IS NOT NULL "
            "AND input_digest ~ '^[0-9a-f]{64}$' AND observation_digest IS NULL)",
            name="ck_delivery_operation_events_evidence",
        ),
        Index(
            "uq_delivery_events_planned_step",
            "result_id",
            "step_id",
            unique=True,
            postgresql_where=text("kind = 'write_planned'"),
        ),
        Index(
            "uq_delivery_events_observed_step",
            "result_id",
            "step_id",
            unique=True,
            postgresql_where=text("kind = 'write_observed'"),
        ),
    )
    result_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    sequence: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    kind: Mapped[str] = mapped_column(String(32))
    invocation_id: Mapped[UUID | None] = mapped_column(Uuid)
    step_id: Mapped[UUID | None] = mapped_column(Uuid)
    step_kind: Mapped[str | None] = mapped_column(String(128))
    input_digest: Mapped[str | None] = mapped_column(String(64))
    observation_digest: Mapped[str | None] = mapped_column(String(64))
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
