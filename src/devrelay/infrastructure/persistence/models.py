from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    metadata = MetaData(
        naming_convention={
            "pk": "pk_%(table_name)s",
            "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
        }
    )


class SourceTaskRecord(Base):
    __tablename__ = "source_tasks"
    __table_args__ = (
        UniqueConstraint(
            "source_provider",
            "source_site_id",
            "external_task_id",
            name="uq_source_tasks_identity",
        ),
        CheckConstraint(
            "source_provider !~ '^[[:space:]]*$' AND source_site_id !~ '^[[:space:]]*$' AND external_task_id !~ '^[[:space:]]*$' AND display_key !~ '^[[:space:]]*$'",
            name="ck_source_tasks_nonblank",
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    source_provider: Mapped[str] = mapped_column(String(64))
    source_site_id: Mapped[str] = mapped_column(String(256))
    external_task_id: Mapped[str] = mapped_column(String(256))
    display_key: Mapped[str] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class TaskSnapshotRecord(Base):
    __tablename__ = "task_snapshots"
    __table_args__ = (
        UniqueConstraint(
            "source_task_id",
            "fingerprint_version",
            "content_fingerprint",
            name="uq_task_snapshots_content",
        ),
        UniqueConstraint("id", "source_task_id", name="uq_task_snapshots_id_source"),
        CheckConstraint(
            "fingerprint_version = 1", name="ck_task_snapshots_fingerprint_version"
        ),
        CheckConstraint(
            "content_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_task_snapshots_fingerprint",
        ),
        CheckConstraint(
            "title !~ '^[[:space:]]*$' AND display_key !~ '^[[:space:]]*$'",
            name="ck_task_snapshots_nonblank",
        ),
        CheckConstraint(
            "length(description) <= 65536", name="ck_task_snapshots_description_size"
        ),
        CheckConstraint(
            "devrelay_valid_task_criteria_v1(acceptance_criteria)",
            name="ck_task_snapshots_criteria",
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    source_task_id: Mapped[UUID] = mapped_column(
        ForeignKey("source_tasks.id", ondelete="RESTRICT")
    )
    content_fingerprint: Mapped[str] = mapped_column(String(64))
    fingerprint_version: Mapped[int] = mapped_column(Integer)
    title: Mapped[str] = mapped_column(String(512))
    description: Mapped[str] = mapped_column(Text)
    acceptance_criteria: Mapped[list[str]] = mapped_column(ARRAY(Text, dimensions=1))
    display_key: Mapped[str] = mapped_column(String(128))
    source_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
