from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import UUID


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerAuthorization:
    """Credential-free scope supplied by trusted authentication, not worker input."""

    worker_id: UUID
    allowed_repository_ids: frozenset[UUID]
    credential_expires_at: datetime

    def __post_init__(self) -> None:
        if (
            not isinstance(self.worker_id, UUID)
            or not isinstance(self.allowed_repository_ids, frozenset)
            or not 1 <= len(self.allowed_repository_ids) <= 32
            or any(not isinstance(value, UUID) for value in self.allowed_repository_ids)
            or self.credential_expires_at.utcoffset() != timedelta(0)
        ):
            raise ValueError("Worker authority requires typed scope and UTC expiry.")
