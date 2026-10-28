from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKeyConstraint,
    Index,
    String,
    UniqueConstraint,
    Uuid,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from devrelay.infrastructure.persistence.models import Base


class JobWorkflowRecord(Base):
    __tablename__ = "job_workflows"
    __table_args__ = (
        ForeignKeyConstraint(
            ["active_attempt_id", "job_id"],
            ["execution_attempts.id", "execution_attempts.job_id"],
            name="fk_workflow_active_attempt",
            deferrable=True,
            initially="DEFERRED",
            use_alter=True,
        ),
        ForeignKeyConstraint(
            ["last_attempt_id", "job_id"],
            ["execution_attempts.id", "execution_attempts.job_id"],
            name="fk_workflow_last_attempt",
            deferrable=True,
            initially="DEFERRED",
            use_alter=True,
        ),
        ForeignKeyConstraint(
            ["job_id", "source_task_id", "repository_id"],
            ["jobs.id", "jobs.source_task_id", "jobs.repository_id"],
            name="fk_job_workflows_definition",
            ondelete="RESTRICT",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["job_id", "workflow_version", "status"],
            [
                "job_workflow_events.job_id",
                "job_workflow_events.workflow_version",
                "job_workflow_events.status",
            ],
            name="fk_job_workflows_current_event",
            deferrable=True,
            initially="DEFERRED",
            use_alter=True,
        ),
        ForeignKeyConstraint(
            ["accepted_result_id", "job_id"],
            ["attempt_results.id", "attempt_results.job_id"],
            name="fk_workflow_result",
            deferrable=True,
            initially="DEFERRED",
            use_alter=True,
        ),
        CheckConstraint(
            "blocks_new_submission IS TRUE AND updated_at >= created_at AND repair_cycles BETWEEN 0 AND 5 AND "
            "((status = 'queued' AND workflow_version = 0 AND updated_at = created_at "
            "AND active_attempt_id IS NULL AND last_attempt_id IS NULL AND last_attempt_number = 0 "
            "AND repair_cycles = 0 AND candidate_revision IS NULL AND accepted_result_id IS NULL) "
            "OR (last_attempt_id IS NOT NULL AND last_attempt_number = 1 AND "
            "((status = 'claimed' AND workflow_version = 1 AND active_attempt_id IS NOT NULL "
            "AND active_attempt_id = last_attempt_id AND repair_cycles = 0 AND candidate_revision IS NULL AND accepted_result_id IS NULL) "
            "OR (status = 'running' AND workflow_version = 2 + 2 * repair_cycles AND active_attempt_id IS NOT NULL "
            "AND active_attempt_id = last_attempt_id AND candidate_revision IS NULL "
            "AND ((repair_cycles = 0 AND accepted_result_id IS NULL) OR (repair_cycles > 0 AND accepted_result_id IS NOT NULL))) "
            "OR (status IN ('testing', 'publishing_pr', 'awaiting_review') AND accepted_result_id IS NOT NULL "
            "AND candidate_revision IS NOT NULL AND candidate_revision ~ '^[0-9a-f]{40}$' "
            "AND ((status = 'testing' AND workflow_version = 3 + 2 * repair_cycles AND active_attempt_id IS NOT NULL AND active_attempt_id = last_attempt_id) "
            "OR (status = 'publishing_pr' AND workflow_version = 4 + 2 * repair_cycles AND active_attempt_id IS NOT NULL AND active_attempt_id = last_attempt_id) "
            "OR (status = 'awaiting_review' AND workflow_version = 5 + 2 * repair_cycles AND active_attempt_id IS NULL))))))",
            name="ck_job_workflows_claim_state",
        ),
        Index(
            "uq_job_workflows_unresolved_task_repository",
            "source_task_id",
            "repository_id",
            unique=True,
            postgresql_where=text("blocks_new_submission IS TRUE"),
        ),
        Index(
            "ix_job_workflows_queued_candidate",
            "repository_id",
            "created_at",
            "job_id",
            postgresql_where=text(
                "status = 'queued' AND workflow_version = 0 AND blocks_new_submission IS TRUE"
            ),
        ),
    )
    job_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    source_task_id: Mapped[UUID] = mapped_column(Uuid)
    repository_id: Mapped[UUID] = mapped_column(Uuid)
    status: Mapped[str] = mapped_column(String(32))
    workflow_version: Mapped[int] = mapped_column(BigInteger)
    blocks_new_submission: Mapped[bool] = mapped_column(Boolean)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    active_attempt_id: Mapped[UUID | None] = mapped_column(Uuid)
    last_attempt_id: Mapped[UUID | None] = mapped_column(Uuid)
    last_attempt_number: Mapped[int] = mapped_column(BigInteger, server_default="0")
    repair_cycles: Mapped[int] = mapped_column(BigInteger, server_default="0")
    candidate_revision: Mapped[str | None] = mapped_column(String(40))
    accepted_result_id: Mapped[UUID | None] = mapped_column(Uuid)


class JobWorkflowEventRecord(Base):
    __tablename__ = "job_workflow_events"
    __table_args__ = (
        ForeignKeyConstraint(
            ["attempt_id", "job_id"],
            ["execution_attempts.id", "execution_attempts.job_id"],
            name="fk_workflow_event_attempt",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["job_id"],
            ["job_workflows.job_id"],
            name="fk_job_workflow_events_workflow",
            ondelete="RESTRICT",
            deferrable=True,
            initially="DEFERRED",
        ),
        UniqueConstraint(
            "job_id", "workflow_version", "status", name="uq_job_workflow_events_state"
        ),
        ForeignKeyConstraint(
            ["result_id", "job_id"],
            ["attempt_results.id", "attempt_results.job_id"],
            name="fk_workflow_event_result",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["job_id", "previous_version", "previous_status"],
            [
                "job_workflow_events.job_id",
                "job_workflow_events.workflow_version",
                "job_workflow_events.status",
            ],
            name="fk_workflow_event_previous",
            deferrable=True,
            initially="DEFERRED",
        ),
        CheckConstraint(
            "((workflow_version = 0 AND status = 'queued' AND event_type = 'submitted' "
            "AND attempt_id IS NULL AND previous_version IS NULL AND previous_status IS NULL) "
            "OR (workflow_version = 1 AND status = 'claimed' AND event_type = 'claimed' "
            "AND attempt_id IS NOT NULL AND previous_version = 0 AND previous_version IS NOT NULL "
            "AND previous_status = 'queued' AND previous_status IS NOT NULL) "
            "OR (workflow_version = 2 AND status = 'running' AND event_type = 'acknowledged' "
            "AND attempt_id IS NOT NULL AND previous_version = 1 AND previous_version IS NOT NULL "
            "AND previous_status = 'claimed' AND previous_status IS NOT NULL)) "
            "AND result_id IS NULL AND cycle IS NULL AND candidate_revision IS NULL "
            "OR (workflow_version > 2 AND attempt_id IS NOT NULL AND result_id IS NOT NULL "
            "AND cycle IS NOT NULL AND cycle BETWEEN 0 AND 5 AND previous_version IS NOT NULL "
            "AND previous_version = workflow_version - 1 AND previous_status IS NOT NULL "
            "AND ((status = 'testing' AND workflow_version = 3 + 2 * cycle AND event_type = 'tested' "
            "AND previous_status = 'running' AND candidate_revision IS NOT NULL AND candidate_revision ~ '^[0-9a-f]{40}$') "
            "OR (status = 'running' AND workflow_version = 2 + 2 * cycle AND cycle > 0 AND event_type = 'repair_requested' "
            "AND previous_status = 'testing' AND candidate_revision IS NULL) "
            "OR (status = 'publishing_pr' AND workflow_version = 4 + 2 * cycle AND event_type = 'checks_accepted' "
            "AND previous_status = 'testing' AND candidate_revision IS NOT NULL AND candidate_revision ~ '^[0-9a-f]{40}$') "
            "OR (status = 'awaiting_review' AND workflow_version = 5 + 2 * cycle AND event_type = 'published' "
            "AND previous_status = 'publishing_pr' AND candidate_revision IS NOT NULL AND candidate_revision ~ '^[0-9a-f]{40}$')))",
            name="ck_job_workflow_events_claim_state",
        ),
    )
    job_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    workflow_version: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    status: Mapped[str] = mapped_column(String(32))
    event_type: Mapped[str] = mapped_column(String(32))
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    attempt_id: Mapped[UUID | None] = mapped_column(Uuid)
    previous_version: Mapped[int | None] = mapped_column(BigInteger)
    previous_status: Mapped[str | None] = mapped_column(String(32))
    result_id: Mapped[UUID | None] = mapped_column(Uuid)
    cycle: Mapped[int | None] = mapped_column(BigInteger)
    candidate_revision: Mapped[str | None] = mapped_column(String(40))
