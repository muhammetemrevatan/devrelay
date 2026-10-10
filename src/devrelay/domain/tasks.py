import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import UUID


def _require_text(value: str, name: str, limit: int, *, required: bool = True) -> None:
    if required and not value.strip():
        raise ValueError(f"{name} must not be blank.")
    if len(value) > limit:
        raise ValueError(f"{name} exceeds its character limit of {limit}.")


def _utc_timestamp(value: datetime, name: str) -> datetime:
    if value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware.")
    return value.astimezone(UTC)


def _normalize_newlines(value: str) -> str:
    return value.replace("\r\n", "\n").replace("\r", "\n")


@dataclass(frozen=True, slots=True, kw_only=True)
class TaskIdentity:
    """Opaque provider/site/task IDs; display keys are not stable identities."""

    provider: str
    site_id: str
    external_id: str

    def __post_init__(self) -> None:
        for name, value, limit in (
            ("provider", self.provider, 64),
            ("site_id", self.site_id, 256),
            ("external_id", self.external_id, 256),
        ):
            _require_text(value, name, limit)
            if value != value.strip():
                raise ValueError(f"{name} must not contain surrounding whitespace.")


@dataclass(frozen=True, slots=True, kw_only=True)
class TaskSnapshot:
    """Bounded captured requirements; repository authorization belongs to a Job.

    Fingerprint version 1 hashes title, description and ordered criteria as UTF-8
    canonical JSON with normalized newlines. Display keys, IDs and timestamps
    are metadata. Deduplication must also include the TaskIdentity and version.
    """

    id: UUID
    identity: TaskIdentity
    display_key: str
    title: str
    description: str
    acceptance_criteria: tuple[str, ...]
    captured_at: datetime
    source_updated_at: datetime | None = None
    fingerprint_version: int = field(init=False, default=1)
    content_fingerprint: str = field(init=False)

    def __post_init__(self) -> None:
        _require_text(self.display_key, "display_key", 128)
        _require_text(self.title, "title", 512)
        _require_text(self.description, "description", 65_536, required=False)
        if not isinstance(self.acceptance_criteria, tuple):
            raise TypeError("acceptance_criteria must be an immutable tuple.")
        if len(self.acceptance_criteria) > 32:
            raise ValueError("acceptance_criteria must contain at most 32 items.")
        for criterion in self.acceptance_criteria:
            _require_text(criterion, "acceptance criterion", 4_096)
        object.__setattr__(
            self, "captured_at", _utc_timestamp(self.captured_at, "captured_at")
        )
        if self.source_updated_at is not None:
            object.__setattr__(
                self,
                "source_updated_at",
                _utc_timestamp(self.source_updated_at, "source_updated_at"),
            )
        content = {
            "version": self.fingerprint_version,
            "title": _normalize_newlines(self.title),
            "description": _normalize_newlines(self.description),
            "acceptance_criteria": [
                _normalize_newlines(item) for item in self.acceptance_criteria
            ],
        }
        encoded = json.dumps(
            content, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        object.__setattr__(
            self, "content_fingerprint", hashlib.sha256(encoded).hexdigest()
        )
