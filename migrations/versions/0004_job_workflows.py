"""Separate current workflow and initial history from accepted definitions."""

import sqlalchemy as sa
from alembic import op

revision = "0004_job_workflows"
down_revision = "0003_repository_bindings"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("LOCK TABLE jobs IN ACCESS EXCLUSIVE MODE")
    op.create_unique_constraint(
        "uq_jobs_definition_identity", "jobs", ["id", "source_task_id", "repository_id"]
    )
    op.create_table(
        "job_workflows",
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("source_task_id", sa.Uuid(), nullable=False),
        sa.Column("repository_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("workflow_version", sa.BigInteger(), nullable=False),
        sa.Column("blocks_new_submission", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("job_id", name="pk_job_workflows"),
        sa.ForeignKeyConstraint(
            ["job_id", "source_task_id", "repository_id"],
            ["jobs.id", "jobs.source_task_id", "jobs.repository_id"],
            name="fk_job_workflows_definition",
            ondelete="RESTRICT",
            deferrable=True,
            initially="DEFERRED",
        ),
        sa.CheckConstraint(
            "status = 'queued' AND workflow_version = 0 AND blocks_new_submission IS TRUE",
            name="ck_job_workflows_initial_state",
        ),
        sa.CheckConstraint(
            "created_at = updated_at", name="ck_job_workflows_initial_chronology"
        ),
    )
    op.create_table(
        "job_workflow_events",
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("workflow_version", sa.BigInteger(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("event_type", sa.String(32), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint(
            "job_id", "workflow_version", name="pk_job_workflow_events"
        ),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["job_workflows.job_id"],
            name="fk_job_workflow_events_workflow",
            ondelete="RESTRICT",
            deferrable=True,
            initially="DEFERRED",
        ),
        sa.UniqueConstraint(
            "job_id", "workflow_version", "status", name="uq_job_workflow_events_state"
        ),
        sa.CheckConstraint(
            "workflow_version = 0 AND status = 'queued' AND event_type = 'submitted'",
            name="ck_job_workflow_events_initial_state",
        ),
    )
    op.execute("""
        INSERT INTO job_workflows
            (job_id, source_task_id, repository_id, status, workflow_version,
             blocks_new_submission, created_at, updated_at)
        SELECT id, source_task_id, repository_id, status, workflow_version,
               blocks_new_submission, created_at, updated_at FROM jobs
    """)
    op.execute("""
        INSERT INTO job_workflow_events
            (job_id, workflow_version, status, event_type, occurred_at)
        SELECT job_id, workflow_version, status, 'submitted', updated_at
        FROM job_workflows
    """)
    # Flush backfill FK events before ALTER TABLE; restore deferred runtime checks.
    op.execute(
        "SET CONSTRAINTS fk_job_workflows_definition, fk_job_workflow_events_workflow IMMEDIATE"
    )
    op.create_foreign_key(
        "fk_jobs_workflow",
        "jobs",
        "job_workflows",
        ["id"],
        ["job_id"],
        deferrable=True,
        initially="DEFERRED",
    )
    op.create_foreign_key(
        "fk_job_workflows_current_event",
        "job_workflows",
        "job_workflow_events",
        ["job_id", "workflow_version", "status"],
        ["job_id", "workflow_version", "status"],
        deferrable=True,
        initially="DEFERRED",
    )
    op.execute(
        "SET CONSTRAINTS fk_job_workflows_definition, fk_job_workflow_events_workflow DEFERRED"
    )
    op.create_index(
        "uq_job_workflows_unresolved_task_repository",
        "job_workflows",
        ["source_task_id", "repository_id"],
        unique=True,
        postgresql_where=sa.text("blocks_new_submission IS TRUE"),
    )
    op.drop_index("uq_jobs_unresolved_task_repository", table_name="jobs")
    op.execute("""
        CREATE FUNCTION devrelay_validate_initial_workflow()
        RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
        DECLARE identity uuid;
        BEGIN
          IF TG_TABLE_NAME = 'jobs' THEN identity := NEW.id;
          ELSE identity := NEW.job_id; END IF;
          IF NOT EXISTS (
            SELECT 1 FROM jobs j
            JOIN job_workflows w ON w.job_id = j.id
            JOIN job_workflow_events e ON e.job_id = w.job_id
              AND e.workflow_version = w.workflow_version AND e.status = w.status
            WHERE j.id = identity AND w.created_at = j.created_at
              AND w.updated_at = j.created_at AND e.occurred_at = w.updated_at
          ) THEN
            RAISE EXCEPTION 'Initial workflow and history must match the definition.'
              USING ERRCODE = '23514';
          END IF;
          RETURN NULL;
        END $$
    """)
    for table in ("jobs", "job_workflows", "job_workflow_events"):
        op.execute(
            f"CREATE CONSTRAINT TRIGGER {table}_initial_workflow AFTER INSERT ON {table} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION devrelay_validate_initial_workflow()"
        )
    # No transition use case is exposed yet; later slices extend workflow guards.
    for table in ("job_workflows", "job_workflow_events"):
        op.execute(
            f"CREATE TRIGGER {table}_no_mutation BEFORE UPDATE OR DELETE ON {table} FOR EACH ROW EXECUTE FUNCTION devrelay_reject_submission_mutation()"
        )


def downgrade() -> None:
    op.execute(
        "LOCK TABLE jobs, job_workflows, job_workflow_events IN ACCESS EXCLUSIVE MODE"
    )
    op.execute("""
        DO $$ BEGIN
          IF EXISTS (
            SELECT 1 FROM jobs j LEFT JOIN job_workflows w ON w.job_id = j.id
            LEFT JOIN job_workflow_events e ON e.job_id = w.job_id
              AND e.workflow_version = w.workflow_version
            WHERE w.job_id IS NULL OR e.job_id IS NULL OR w.status <> 'queued'
              OR w.workflow_version <> 0 OR NOT w.blocks_new_submission
              OR w.created_at <> j.created_at OR w.updated_at <> j.created_at
              OR e.event_type <> 'submitted' OR e.status <> 'queued'
              OR e.occurred_at <> w.updated_at
          ) OR EXISTS (SELECT 1 FROM job_workflow_events WHERE workflow_version <> 0) THEN
            RAISE EXCEPTION 'Cannot discard advanced or inconsistent workflow history.'
              USING ERRCODE = '23514';
          END IF;
        END $$
    """)
    op.create_index(
        "uq_jobs_unresolved_task_repository",
        "jobs",
        ["source_task_id", "repository_id"],
        unique=True,
        postgresql_where=sa.text("blocks_new_submission IS TRUE"),
    )
    for table in ("jobs", "job_workflows", "job_workflow_events"):
        op.execute(f"DROP TRIGGER {table}_initial_workflow ON {table}")
    op.execute("DROP FUNCTION devrelay_validate_initial_workflow()")
    op.drop_constraint("fk_jobs_workflow", "jobs", type_="foreignkey")
    op.drop_constraint(
        "fk_job_workflows_current_event", "job_workflows", type_="foreignkey"
    )
    op.drop_table("job_workflow_events")
    op.drop_table("job_workflows")
    op.drop_constraint("uq_jobs_definition_identity", "jobs", type_="unique")
