from typing import Protocol
from uuid import UUID

from devrelay.application.contracts.submissions import RepositorySubmissionObservation


class RepositorySourceError(Exception):
    """Classified, credential-free failure of repository observation."""


class RepositorySourceAccessDenied(RepositorySourceError):
    """The repository is inaccessible, missing or outside configured identity."""


class RepositorySourceUnavailable(RepositorySourceError):
    """Observation failed transiently; retry policy belongs to the caller."""


class InvalidRepositorySourceData(RepositorySourceError):
    """Observation or task/PR association could not be verified completely."""


class RepositorySubmissionSource(Protocol):
    async def observe(
        self,
        *,
        repository_id: UUID,
        target_branch: str,
        source_task_id: UUID,
    ) -> RepositorySubmissionObservation:
        """Observe matching head and task-associated unresolved PRs with bounded I/O.

        Implementations must verify remote repository identity and association,
        never trust an arbitrary user body or assume absent local PR means none
        remotely. Observation is not an atomic remote snapshot or dispatch authority.
        """
        ...
