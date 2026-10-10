"""Pin new queued definitions to immutable GitHub numeric identities."""

import sqlalchemy as sa
from alembic import op

revision = "0003_repository_bindings"
down_revision = "0002_job_submissions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "repository_bindings",
        sa.Column("repository_id", sa.Uuid(), nullable=False),
        sa.Column("provider_repository_id", sa.BigInteger(), nullable=False),
        sa.Column("bound_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("repository_id", name="pk_repository_bindings"),
        sa.ForeignKeyConstraint(
            ["repository_id"],
            ["repositories.id"],
            name="fk_repository_bindings_repository",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "provider_repository_id", name="uq_repository_bindings_provider_id"
        ),
        sa.UniqueConstraint(
            "repository_id",
            "provider_repository_id",
            name="uq_repository_bindings_pair",
        ),
        sa.CheckConstraint(
            "provider_repository_id > 0", name="ck_repository_bindings_positive_id"
        ),
    )
    op.execute("""
        CREATE TRIGGER repository_bindings_no_mutation
        BEFORE UPDATE OR DELETE ON repository_bindings FOR EACH ROW
        EXECUTE FUNCTION devrelay_reject_submission_mutation()
    """)
    op.add_column(
        "jobs", sa.Column("provider_repository_id", sa.BigInteger(), nullable=True)
    )
    op.drop_constraint("ck_jobs_definition_version", "jobs", type_="check")
    op.create_check_constraint(
        "ck_jobs_definition_version",
        "jobs",
        "submission_generation = 1 AND ((definition_version = 1 AND provider_repository_id IS NULL) OR (definition_version = 2 AND provider_repository_id IS NOT NULL AND provider_repository_id > 0))",
    )
    op.create_foreign_key(
        "fk_jobs_repository_binding",
        "jobs",
        "repository_bindings",
        ["repository_id", "provider_repository_id"],
        ["repository_id", "provider_repository_id"],
        ondelete="RESTRICT",
    )
    # Preserve old rows without inventing identity; prohibit new unbound work.
    op.execute("""
        CREATE FUNCTION devrelay_require_repository_binding()
        RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
          IF NEW.definition_version IS DISTINCT FROM 2 OR NEW.provider_repository_id IS NULL THEN
            RAISE EXCEPTION 'New submissions require a provider binding.' USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END $$
    """)
    op.execute("""
        CREATE TRIGGER jobs_require_repository_binding BEFORE INSERT ON jobs
        FOR EACH ROW EXECUTE FUNCTION devrelay_require_repository_binding()
    """)


def downgrade() -> None:
    op.execute("LOCK TABLE jobs, repository_bindings IN ACCESS EXCLUSIVE MODE")
    op.execute("""
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM repository_bindings) OR EXISTS (SELECT 1 FROM jobs WHERE definition_version = 2) THEN
            RAISE EXCEPTION 'Cannot discard accepted repository bindings.' USING ERRCODE = '23514';
          END IF;
        END $$
    """)
    op.execute("DROP TRIGGER jobs_require_repository_binding ON jobs")
    op.execute("DROP FUNCTION devrelay_require_repository_binding()")
    op.drop_constraint("fk_jobs_repository_binding", "jobs", type_="foreignkey")
    op.drop_constraint("ck_jobs_definition_version", "jobs", type_="check")
    op.drop_column("jobs", "provider_repository_id")
    op.create_check_constraint(
        "ck_jobs_definition_version",
        "jobs",
        "submission_generation = 1 AND definition_version = 1",
    )
    op.drop_table("repository_bindings")
