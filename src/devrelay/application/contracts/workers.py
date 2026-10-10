from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256
from urllib.parse import urlsplit
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


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerJournalScope:
    """Local namespace, not authentication or permission to execute work."""

    worker_id: UUID
    control_url: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.worker_id, UUID)
            or not isinstance(self.control_url, str)
            or not 1 <= len(self.control_url) <= 2048
        ):
            raise ValueError("Worker journal requires bounded control identity.")
        parsed = urlsplit(self.control_url)
        if (
            parsed.scheme not in ("http", "https")
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("Worker journal requires a credential-free control URL.")

    @property
    def key(self) -> bytes:
        return sha256(
            b"devrelay.worker-journal.v1\x00"
            + self.worker_id.bytes
            + self.control_url.encode("utf-8")
        ).digest()
