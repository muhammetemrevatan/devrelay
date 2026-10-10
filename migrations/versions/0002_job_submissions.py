"""Store immutable queued submissions and complete ordered check policies."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0002_job_submissions"
down_revision = "0001_task_snapshots"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE FUNCTION devrelay_valid_policy_array_v1(items text[], minimum integer, maximum integer, item_limit integer)
        RETURNS boolean LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
          SELECT items IS NOT NULL AND array_ndims(items) = 1
            AND cardinality(items) BETWEEN minimum AND maximum
            AND NOT EXISTS (SELECT 1 FROM unnest(items) item
              WHERE item IS NULL OR length(item) NOT BETWEEN 1 AND item_limit
                OR item ~ '[[:cntrl:]]')
        $$
    """)
    op.create_unique_constraint(
        "uq_task_snapshots_id_source", "task_snapshots", ["id", "source_task_id"]
    )
    op.create_table(
        "repositories",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("provider", sa.String(32), nullable=False),
        sa.Column("slug", sa.String(256), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name="pk_repositories"),
        sa.UniqueConstraint("slug", name="uq_repositories_slug"),
        sa.CheckConstraint(
            "provider = 'github' AND slug = lower(slug) AND slug ~ '^[a-z0-9][a-z0-9-]*/[a-z0-9_][a-z0-9_.-]*$'",
            name="ck_repositories_identity",
        ),
    )
    op.create_table(
        "jobs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("source_task_id", sa.Uuid(), nullable=False),
        sa.Column("snapshot_id", sa.Uuid(), nullable=False),
        sa.Column("repository_id", sa.Uuid(), nullable=False),
        sa.Column("submission_key", sa.String(64), nullable=False),
        sa.Column("submission_fingerprint", sa.String(64), nullable=False),
        sa.Column("submission_generation", sa.Integer(), nullable=False),
        sa.Column("definition_version", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("workflow_version", sa.Integer(), nullable=False),
        sa.Column("blocks_new_submission", sa.Boolean(), nullable=False),
        sa.Column("repository_slug", sa.String(256), nullable=False),
        sa.Column("target_branch", sa.String(256), nullable=False),
        sa.Column("authorized_branch", sa.String(256), nullable=False),
        sa.Column("coding_branch_prefix", sa.String(180), nullable=False),
        sa.Column("base_revision", sa.String(64), nullable=False),
        sa.Column("policy_version", sa.String(64), nullable=False),
        sa.Column("policy_source_provider", sa.String(64), nullable=False),
        sa.Column("policy_source_site_id", sa.String(256), nullable=False),
        sa.Column(
            "allowed_scope_keys",
            postgresql.ARRAY(sa.Text(), dimensions=1),
            nullable=False,
        ),
        sa.Column("readiness_label", sa.String(128), nullable=False),
        sa.Column("max_repair_cycles", sa.Integer(), nullable=False),
        sa.Column("required_check_count", sa.Integer(), nullable=False),
        sa.Column("actor_id", sa.String(128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_jobs"),
        sa.ForeignKeyConstraint(
            ["repository_id"],
            ["repositories.id"],
            name="fk_jobs_repository_id_repositories",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["snapshot_id", "source_task_id"],
            ["task_snapshots.id", "task_snapshots.source_task_id"],
            name="fk_jobs_snapshot_source",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("submission_key", name="uq_jobs_submission_key"),
        sa.UniqueConstraint(
            "snapshot_id",
            "repository_id",
            "submission_generation",
            name="uq_jobs_snapshot_repository_generation",
        ),
        sa.UniqueConstraint(
            "id", "required_check_count", name="uq_jobs_id_check_count"
        ),
        sa.CheckConstraint(
            "submission_key ~ '^[0-9a-f]{64}$' AND submission_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_jobs_submission_hashes",
        ),
        sa.CheckConstraint(
            "submission_generation = 1 AND definition_version = 1",
            name="ck_jobs_definition_version",
        ),
        sa.CheckConstraint(
            "status = 'queued' AND workflow_version = 0 AND blocks_new_submission IS TRUE",
            name="ck_jobs_submission_state",
        ),
        sa.CheckConstraint(
            "base_revision ~ '^([0-9a-f]{40}|[0-9a-f]{64})$' AND authorized_branch <> target_branch",
            name="ck_jobs_git_pin",
        ),
        sa.CheckConstraint(
            "required_check_count BETWEEN 1 AND 32 AND max_repair_cycles BETWEEN 0 AND 5",
            name="ck_jobs_policy_limits",
        ),
        sa.CheckConstraint(
            "updated_at = created_at", name="ck_jobs_initial_chronology"
        ),
        sa.CheckConstraint(
            "devrelay_valid_policy_array_v1(allowed_scope_keys, 1, 32, 64)",
            name="ck_jobs_scope_bounds",
        ),
        sa.CheckConstraint(
            "policy_version !~ '^[[:space:]]*$' AND actor_id !~ '^[[:space:]]*$' AND readiness_label !~ '^[[:space:]]*$' AND policy_source_provider !~ '^[[:space:]]*$' AND policy_source_site_id !~ '^[[:space:]]*$' AND target_branch !~ '^[[:space:]]*$' AND authorized_branch !~ '^[[:space:]]*$' AND coding_branch_prefix !~ '^[[:space:]]*$'",
            name="ck_jobs_nonblank_policy",
        ),
    )
    op.create_index(
        "uq_jobs_unresolved_task_repository",
        "jobs",
        ["source_task_id", "repository_id"],
        unique=True,
        postgresql_where=sa.text("blocks_new_submission IS TRUE"),
    )
    op.create_table(
        "job_checks",
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("check_id", sa.String(128), nullable=False),
        sa.Column("check_count", sa.Integer(), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("argv", postgresql.ARRAY(sa.Text(), dimensions=1), nullable=False),
        sa.Column("working_directory", sa.String(256), nullable=False),
        sa.Column("timeout_seconds", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("job_id", "check_id", name="pk_job_checks"),
        sa.ForeignKeyConstraint(
            ["job_id", "check_count"],
            ["jobs.id", "jobs.required_check_count"],
            name="fk_job_checks_job_count",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("job_id", "position", name="uq_job_checks_position"),
        sa.CheckConstraint(
            "check_count BETWEEN 1 AND 32 AND position >= 0 AND position < check_count",
            name="ck_job_checks_position",
        ),
        sa.CheckConstraint(
            "check_id ~ '^[a-z][a-z0-9_-]*$' AND timeout_seconds BETWEEN 1 AND 3600",
            name="ck_job_checks_limits",
        ),
        sa.CheckConstraint(
            "devrelay_valid_policy_array_v1(argv, 1, 64, 4096)",
            name="ck_job_checks_argv",
        ),
        sa.CheckConstraint(
            "working_directory = '.' OR working_directory ~ '^[A-Za-z0-9_-]+(/[A-Za-z0-9_-]+)*$'",
            name="ck_job_checks_directory",
        ),
    )
    op.execute("""
        CREATE FUNCTION devrelay_reject_submission_mutation()
        RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
          RAISE EXCEPTION 'Submission definitions are immutable.' USING ERRCODE = '23514';
        END $$
    """)
    # No execution mutation or retention path exists yet. Future migrations must
    # add the corresponding versioned state/history contract before allowing it.
    for table in ("repositories", "jobs", "job_checks"):
        op.execute(
            f"CREATE TRIGGER {table}_no_mutation BEFORE UPDATE OR DELETE ON {table} FOR EACH ROW EXECUTE FUNCTION devrelay_reject_submission_mutation()"
        )
    op.execute("""
        CREATE FUNCTION devrelay_validate_submission_checks()
        RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$ BEGIN
          IF (SELECT count(*) FROM job_checks WHERE job_id = NEW.id) <> NEW.required_check_count THEN
            RAISE EXCEPTION 'Submission checks must be complete.' USING ERRCODE = '23514';
          END IF;
          RETURN NULL;
        END $$
    """)
    op.execute("""
        CREATE CONSTRAINT TRIGGER jobs_complete_checks AFTER INSERT ON jobs
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
        EXECUTE FUNCTION devrelay_validate_submission_checks()
    """)


def downgrade() -> None:
    op.drop_table("job_checks")
    op.drop_table("jobs")
    op.drop_table("repositories")
    op.drop_constraint("uq_task_snapshots_id_source", "task_snapshots", type_="unique")
    op.execute("DROP FUNCTION devrelay_validate_submission_checks()")
    op.execute("DROP FUNCTION devrelay_reject_submission_mutation()")
    op.execute(
        "DROP FUNCTION devrelay_valid_policy_array_v1(text[], integer, integer, integer)"
    )
