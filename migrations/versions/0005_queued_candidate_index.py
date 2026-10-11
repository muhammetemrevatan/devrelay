"""Index bounded scoped queue observations without changing Job state."""

import sqlalchemy as sa
from alembic import op

revision = "0005_queued_candidate_index"
down_revision = "0004_job_workflows"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_job_workflows_queued_candidate",
        "job_workflows",
        ["repository_id", "created_at", "job_id"],
        postgresql_where=sa.text(
            "status = 'queued' AND workflow_version = 0 AND blocks_new_submission IS TRUE"
        ),
    )


def downgrade() -> None:
    op.drop_index("ix_job_workflows_queued_candidate", table_name="job_workflows")
