from typing import Protocol

from devrelay.application.contracts.tasks import TaskRead
from devrelay.domain.tasks import TaskIdentity, TaskSnapshot


class TaskSourceError(Exception):
    """A classified task read failure with a bounded, credential-free message."""


class TaskNotFound(TaskSourceError):
    """The requested task does not exist in the configured scope."""


class TaskSourceAccessDenied(TaskSourceError):
    """The configured credentials cannot read the task; do not blindly retry."""


class TaskSourceScopeDenied(TaskSourceAccessDenied):
    """The requested or returned task is outside the configured source scope."""


class TaskSourceUnavailable(TaskSourceError):
    """The provider is temporarily unavailable; retries require bounded policy."""


class InvalidTaskSourceData(TaskSourceError):
    """The provider response cannot be mapped to a valid task snapshot."""


class TaskSource(Protocol):
    async def read_by_key(self, display_key: str) -> TaskRead:
        """Resolve a display key in configured scope and capture identity/metadata.

        Display keys are lookup inputs, never persistence identity. The caller
        evaluates readiness labels and repository policy without vendor clients.
        """
        ...

    async def fetch_snapshot(self, identity: TaskIdentity) -> TaskSnapshot:
        """Read matching immutable content or raise a classified TaskSourceError.

        Implementations own bounded I/O timeouts, response sizes and provider
        mapping. They must not select a repository, enqueue work, change the
        provider task or store a snapshot. Scope, eligibility and persistence
        belong to the calling use case. Missing criteria may be captured as an
        empty tuple; that is not permission to execute the task.
        """
        ...


class TaskReadinessSource(Protocol):
    async def read_by_identity(self, identity: TaskIdentity) -> TaskRead:
        """Re-read content and readiness by stable identity, including renamed keys."""
        ...
