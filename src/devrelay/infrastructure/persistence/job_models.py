from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column

from devrelay.infrastructure.persistence.models import Base


class RepositoryRecord(Base):
    """Configuration identity/slug binding, not proof of remote numeric identity."""

    __tablename__ = "repositories"
    __table_args__ = (
        UniqueConstraint("slug", name="uq_repositories_slug"),
        CheckConstraint(
            "provider = 'github' AND slug = lower(slug) AND slug ~ '^[a-z0-9][a-z0-9-]*/[a-z0-9_][a-z0-9_.-]*$'",
            name="ck_repositories_identity",
        ),
    )
    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    provider: Mapped[str] = mapped_column(String(32))
    slug: Mapped[str] = mapped_column(String(256))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class RepositoryBindingRecord(Base):
    __tablename__ = "repository_bindings"
    __table_args__ = (
        UniqueConstraint(
            "provider_repository_id", name="uq_repository_bindings_provider_id"
        ),
        UniqueConstraint(
            "repository_id",
            "provider_repository_id",
            name="uq_repository_bindings_pair",
        ),
        CheckConstraint(
            "provider_repository_id > 0", name="ck_repository_bindings_positive_id"
        ),
    )
    repository_id: Mapped[UUID] = mapped_column(
        ForeignKey("repositories.id", ondelete="RESTRICT"), primary_key=True
    )
    provider_repository_id: Mapped[int] = mapped_column(BigInteger)
    bound_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class JobRecord(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        ForeignKeyConstraint(
            ["repository_id", "provider_repository_id"],
            [
                "repository_bindings.repository_id",
                "repository_bindings.provider_repository_id",
            ],
            name="fk_jobs_repository_binding",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["snapshot_id", "source_task_id"],
            ["task_snapshots.id", "task_snapshots.source_task_id"],
            name="fk_jobs_snapshot_source",
            ondelete="RESTRICT",
        ),
        UniqueConstraint("submission_key", name="uq_jobs_submission_key"),
        UniqueConstraint(
            "snapshot_id",
            "repository_id",
            "submission_generation",
            name="uq_jobs_snapshot_repository_generation",
        ),
        UniqueConstraint("id", "required_check_count", name="uq_jobs_id_check_count"),
        UniqueConstraint(
            "id", "source_task_id", "repository_id", name="uq_jobs_definition_identity"
        ),
        ForeignKeyConstraint(
            ["id"],
            ["job_workflows.job_id"],
            name="fk_jobs_workflow",
            deferrable=True,
            initially="DEFERRED",
            use_alter=True,
        ),
        CheckConstraint(
            "submission_key ~ '^[0-9a-f]{64}$' AND submission_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_jobs_submission_hashes",
        ),
        CheckConstraint(
            "submission_generation = 1 AND ((definition_version = 1 AND provider_repository_id IS NULL) OR (definition_version = 2 AND provider_repository_id IS NOT NULL AND provider_repository_id > 0))",
            name="ck_jobs_definition_version",
        ),
        CheckConstraint(
            "status = 'queued' AND workflow_version = 0 AND blocks_new_submission IS TRUE",
            name="ck_jobs_submission_state",
        ),
        CheckConstraint(
            "base_revision ~ '^([0-9a-f]{40}|[0-9a-f]{64})$' AND authorized_branch <> target_branch",
            name="ck_jobs_git_pin",
        ),
        CheckConstraint(
            "required_check_count BETWEEN 1 AND 32 AND max_repair_cycles BETWEEN 0 AND 5",
            name="ck_jobs_policy_limits",
        ),
        CheckConstraint("updated_at = created_at", name="ck_jobs_initial_chronology"),
        CheckConstraint(
            "devrelay_valid_policy_array_v1(allowed_scope_keys, 1, 32, 64)",
            name="ck_jobs_scope_bounds",
        ),
        CheckConstraint(
            "policy_version !~ '^[[:space:]]*$' AND actor_id !~ '^[[:space:]]*$' AND readiness_label !~ '^[[:space:]]*$' AND policy_source_provider !~ '^[[:space:]]*$' AND policy_source_site_id !~ '^[[:space:]]*$' AND target_branch !~ '^[[:space:]]*$' AND authorized_branch !~ '^[[:space:]]*$' AND coding_branch_prefix !~ '^[[:space:]]*$'",
            name="ck_jobs_nonblank_policy",
        ),
    )
    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    source_task_id: Mapped[UUID] = mapped_column(Uuid)
    snapshot_id: Mapped[UUID] = mapped_column(Uuid)
    repository_id: Mapped[UUID] = mapped_column(
        ForeignKey("repositories.id", ondelete="RESTRICT")
    )
    provider_repository_id: Mapped[int | None] = mapped_column(BigInteger)
    submission_key: Mapped[str] = mapped_column(String(64))
    submission_fingerprint: Mapped[str] = mapped_column(String(64))
    submission_generation: Mapped[int] = mapped_column(Integer)
    definition_version: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(32))
    workflow_version: Mapped[int] = mapped_column(Integer)
    blocks_new_submission: Mapped[bool] = mapped_column(Boolean)
    repository_slug: Mapped[str] = mapped_column(String(256))
    target_branch: Mapped[str] = mapped_column(String(256))
    authorized_branch: Mapped[str] = mapped_column(String(256))
    coding_branch_prefix: Mapped[str] = mapped_column(String(180))
    base_revision: Mapped[str] = mapped_column(String(64))
    policy_version: Mapped[str] = mapped_column(String(64))
    policy_source_provider: Mapped[str] = mapped_column(String(64))
    policy_source_site_id: Mapped[str] = mapped_column(String(256))
    allowed_scope_keys: Mapped[list[str]] = mapped_column(ARRAY(Text, dimensions=1))
    readiness_label: Mapped[str] = mapped_column(String(128))
    max_repair_cycles: Mapped[int] = mapped_column(Integer)
    required_check_count: Mapped[int] = mapped_column(Integer)
    actor_id: Mapped[str] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class JobCheckRecord(Base):
    __tablename__ = "job_checks"
    __table_args__ = (
        ForeignKeyConstraint(
            ["job_id", "check_count"],
            ["jobs.id", "jobs.required_check_count"],
            name="fk_job_checks_job_count",
            ondelete="RESTRICT",
        ),
        UniqueConstraint("job_id", "position", name="uq_job_checks_position"),
        CheckConstraint(
            "check_count BETWEEN 1 AND 32 AND position >= 0 AND position < check_count",
            name="ck_job_checks_position",
        ),
        CheckConstraint(
            "check_id ~ '^[a-z][a-z0-9_-]*$' AND timeout_seconds BETWEEN 1 AND 3600",
            name="ck_job_checks_limits",
        ),
        CheckConstraint(
            "devrelay_valid_policy_array_v1(argv, 1, 64, 4096)",
            name="ck_job_checks_argv",
        ),
        CheckConstraint(
            "working_directory = '.' OR working_directory ~ '^[A-Za-z0-9_-]+(/[A-Za-z0-9_-]+)*$'",
            name="ck_job_checks_directory",
        ),
    )
    job_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    check_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    check_count: Mapped[int] = mapped_column(Integer)
    position: Mapped[int] = mapped_column(Integer)
    argv: Mapped[list[str]] = mapped_column(ARRAY(Text, dimensions=1))
    working_directory: Mapped[str] = mapped_column(String(256))
    timeout_seconds: Mapped[int] = mapped_column(Integer)
