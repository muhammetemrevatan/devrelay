from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from devrelay.application.contracts.submissions import StoredJobSubmission
from devrelay.domain.jobs.attempt import ExecutionAttempt
from devrelay.domain.jobs.values import _utc


class ClaimAdmissionRejected(Exception):
    """New requests exceed a durable admission boundary."""


class ClaimScopeRejected(Exception):
    """Current authenticated scope cannot disclose or claim work."""


class ClaimPersistenceError(Exception):
    """Coordination storage failed; retry only the original request identity."""


class NoWorkReason(StrEnum):
    EMPTY = "empty"
    OCCUPIED = "occupied"
    CHANGED = "changed"
    STALE = "stale"


@dataclass(frozen=True, slots=True, kw_only=True)
class ClaimLimits:
    lease_seconds: int = 120
    worker_receipts: int = 100_000
    global_receipts: int = 1_000_000
    historical_workers: int = 256

    def __post_init__(self) -> None:
        for name, maximum in (
            ("lease_seconds", 300),
            ("worker_receipts", 1_000_000),
            ("global_receipts", 10_000_000),
            ("historical_workers", 256),
        ):
            value = getattr(self, name)
            if type(value) is not int or not 1 <= value <= maximum:
                raise ValueError("Claim limits require bounded positive integers.")
        if self.lease_seconds < 30:
            raise ValueError("Lease TTL must be between 30 and 300 seconds.")


@dataclass(frozen=True, slots=True, kw_only=True)
class ClaimResult:
    """Materialized recovery view; active lease metadata never authorizes launch."""

    recorded_at: datetime
    recovered: bool
    stored: StoredJobSubmission | None = None
    attempt: ExecutionAttempt | None = None
    no_work: NoWorkReason | None = None
    lease_authority_active: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "recorded_at", _utc(self.recorded_at))
        if (
            type(self.recovered) is not bool
            or type(self.lease_authority_active) is not bool
            or (self.no_work is not None and not isinstance(self.no_work, NoWorkReason))
        ):
            raise ValueError("Claim result requires typed outcome metadata.")
        if (self.stored is None) != (self.attempt is None):
            raise ValueError("Assignment references must be complete.")
        if self.stored is None:
            if self.no_work is None or self.lease_authority_active:
                raise ValueError("No-work requires a reason and no lease authority.")
        elif (
            self.no_work is not None
            or self.attempt is None
            or self.attempt.job_id != self.stored.job.id
            or self.attempt.lease.issued_at != self.recorded_at
        ):
            raise ValueError("Assignment must match the recorded claim.")
