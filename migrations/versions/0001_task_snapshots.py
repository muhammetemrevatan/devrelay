"""Create stable task identities and immutable content snapshots."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001_task_snapshots"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE FUNCTION devrelay_valid_task_criteria_v1(items text[])
        RETURNS boolean LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
          SELECT items IS NOT NULL
            AND COALESCE(array_ndims(items), 1) = 1
            AND cardinality(items) <= 32
            AND NOT EXISTS (
              SELECT 1 FROM unnest(items) AS criterion
              WHERE criterion IS NULL OR length(criterion) > 4096
                OR criterion ~ '^[[:space:]]*$'
            )
        $$
    """)
    op.create_table(
        "source_tasks",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("source_provider", sa.String(64), nullable=False),
        sa.Column("source_site_id", sa.String(256), nullable=False),
        sa.Column("external_task_id", sa.String(256), nullable=False),
        sa.Column("display_key", sa.String(128), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_source_tasks"),
        sa.UniqueConstraint(
            "source_provider",
            "source_site_id",
            "external_task_id",
            name="uq_source_tasks_identity",
        ),
        sa.CheckConstraint(
            "source_provider !~ '^[[:space:]]*$' AND source_site_id !~ '^[[:space:]]*$' AND external_task_id !~ '^[[:space:]]*$' AND display_key !~ '^[[:space:]]*$'",
            name="ck_source_tasks_nonblank",
        ),
    )
    op.create_table(
        "task_snapshots",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("source_task_id", sa.Uuid(), nullable=False),
        sa.Column("content_fingerprint", sa.String(64), nullable=False),
        sa.Column("fingerprint_version", sa.Integer(), nullable=False),
        sa.Column("title", sa.String(512), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column(
            "acceptance_criteria",
            postgresql.ARRAY(sa.Text(), dimensions=1),
            nullable=False,
        ),
        sa.Column("display_key", sa.String(128), nullable=False),
        sa.Column("source_updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("captured_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_task_snapshots"),
        sa.ForeignKeyConstraint(
            ["source_task_id"],
            ["source_tasks.id"],
            name="fk_task_snapshots_source_task_id_source_tasks",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "source_task_id",
            "fingerprint_version",
            "content_fingerprint",
            name="uq_task_snapshots_content",
        ),
        sa.CheckConstraint(
            "fingerprint_version = 1", name="ck_task_snapshots_fingerprint_version"
        ),
        sa.CheckConstraint(
            "content_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_task_snapshots_fingerprint",
        ),
        sa.CheckConstraint(
            "title !~ '^[[:space:]]*$' AND display_key !~ '^[[:space:]]*$'",
            name="ck_task_snapshots_nonblank",
        ),
        sa.CheckConstraint(
            "length(description) <= 65536", name="ck_task_snapshots_description_size"
        ),
        sa.CheckConstraint(
            "devrelay_valid_task_criteria_v1(acceptance_criteria)",
            name="ck_task_snapshots_criteria",
        ),
    )
    op.execute("""
        CREATE FUNCTION devrelay_reject_snapshot_update()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          RAISE EXCEPTION 'Task snapshots are immutable.' USING ERRCODE = '23514';
        END
        $$
    """)
    op.execute("""
        CREATE TRIGGER task_snapshots_no_update BEFORE UPDATE ON task_snapshots
        FOR EACH ROW EXECUTE FUNCTION devrelay_reject_snapshot_update()
    """)


def downgrade() -> None:
    op.drop_table("task_snapshots")
    op.drop_table("source_tasks")
    op.execute("DROP FUNCTION devrelay_reject_snapshot_update()")
    op.execute("DROP FUNCTION devrelay_valid_task_criteria_v1(text[])")
