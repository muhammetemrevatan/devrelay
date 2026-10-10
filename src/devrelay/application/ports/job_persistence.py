from types import TracebackType
from typing import Protocol, Self
from uuid import UUID

from devrelay.application.contracts.submissions import (
    JobBranchAssociation,
    PreparedJobSubmission,
    SavedJobSubmission,
    StoredJobSubmission,
)
from devrelay.domain.jobs.job import Job


class JobPersistenceError(Exception):
    """Sanitized storage failure without SQL, arguments or credentials."""


class JobSubmissionReader(Protocol):
    async def find_by_key(self, submission_key: str) -> StoredJobSubmission | None:
        """Materialize one complete definition and release the read session."""
        ...


class JobBranchReader(Protocol):
    async def find_by_ids(
        self, *, repository_id: UUID, job_ids: tuple[UUID, ...]
    ) -> tuple[JobBranchAssociation, ...]:
        """Read at most 100 scoped Job branches in one query; release the session.

        Include all persisted statuses: cancellation does not close remote PRs.
        No task text, vendor payload or ORM object may substitute for this binding.
        """
        ...


class JobSubmissionRepository(Protocol):
    async def admit(
        self,
        intent: PreparedJobSubmission,
        candidate: Job,
    ) -> SavedJobSubmission:
        """Lock task scope, repeat replay/conflict checks and save full definition.

        Never commit independently or perform provider/network work here.
        """
        ...


class JobSubmissionUnitOfWork(Protocol):
    @property
    def jobs(self) -> JobSubmissionRepository: ...

    async def __aenter__(self) -> Self: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...

    async def commit(self) -> None: ...
