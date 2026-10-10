from types import TracebackType
from typing import Protocol, Self
from uuid import UUID

from devrelay.application.contracts.tasks import SavedTaskSnapshot, StoredTaskSnapshot
from devrelay.domain.tasks import TaskSnapshot


class TaskPersistenceError(Exception):
    """Storage failed; expose no SQL, parameters or database credentials."""


class TaskSnapshotNotFound(TaskPersistenceError):
    """The selected stored snapshot does not exist."""


class TaskSnapshotReader(Protocol):
    async def get_by_id(self, snapshot_id: UUID) -> StoredTaskSnapshot:
        """Materialize one snapshot and release its session before returning."""
        ...


class TaskRepository(Protocol):
    async def save_snapshot(self, snapshot: TaskSnapshot) -> SavedTaskSnapshot:
        """Atomically deduplicate identity/content inside the owner's transaction."""
        ...


class TaskUnitOfWork(Protocol):
    @property
    def tasks(self) -> TaskRepository: ...

    async def __aenter__(self) -> Self: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...

    async def commit(self) -> None:
        """Explicitly commit; leaving without commit must roll back."""
        ...
