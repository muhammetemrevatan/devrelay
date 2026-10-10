from dataclasses import dataclass
from uuid import UUID

from devrelay.domain.tasks import TaskSnapshot


@dataclass(frozen=True, slots=True, kw_only=True)
class TaskRead:
    """A single provider observation; labels do not authorize task execution."""

    snapshot: TaskSnapshot
    scope_key: str
    labels: frozenset[str]


@dataclass(frozen=True, slots=True, kw_only=True)
class SavedTaskSnapshot:
    """Fully materialized canonical storage result; never an ORM entity."""

    source_task_id: UUID
    snapshot: TaskSnapshot
    created: bool


@dataclass(frozen=True, slots=True, kw_only=True)
class StoredTaskSnapshot:
    """Canonical content plus stable storage identity for submission reads."""

    source_task_id: UUID
    snapshot: TaskSnapshot
