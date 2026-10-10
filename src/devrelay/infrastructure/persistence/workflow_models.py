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
        CheckConstraint(
            "status = 'queued' AND workflow_version = 0 AND blocks_new_submission IS TRUE",
            name="ck_job_workflows_initial_state",
        ),
        CheckConstraint(
            "created_at = updated_at", name="ck_job_workflows_initial_chronology"
        ),
        Index(
            "uq_job_workflows_unresolved_task_repository",
            "source_task_id",
            "repository_id",
            unique=True,
            postgresql_where=text("blocks_new_submission IS TRUE"),
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


class JobWorkflowEventRecord(Base):
    __tablename__ = "job_workflow_events"
    __table_args__ = (
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
        CheckConstraint(
            "workflow_version = 0 AND status = 'queued' AND event_type = 'submitted'",
            name="ck_job_workflow_events_initial_state",
        ),
    )
    job_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    workflow_version: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    status: Mapped[str] = mapped_column(String(32))
    event_type: Mapped[str] = mapped_column(String(32))
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
