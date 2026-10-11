from typing import Protocol
from uuid import UUID

from devrelay.application.contracts.submissions import StoredJobSubmission


class QueuedJobReader(Protocol):
    async def find_candidate(
        self, *, repository_ids: frozenset[UUID]
    ) -> StoredJobSubmission | None:
        """Materialize at most one bound queued Job and release its read session.

        Scope contains at most 32 enabled, authenticated repository identities.
        Order by creation time then Job UUID. Never lock or assign the candidate;
        the later claim transaction revalidates its version and current scope.
        """
        ...
