from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import UUID

from devrelay.application.contracts.submissions import StoredJobSubmission
from devrelay.domain.jobs.values import JobStatus, _utc, _uuid


class ClaimEligibilityRejected(Exception):
    """Queued work no longer matches the trusted pre-claim requirements."""


class WorkerAuthorityExpired(ClaimEligibilityRejected):
    """Worker credentials cannot authorize a new claim observation."""


@dataclass(frozen=True, slots=True, kw_only=True)
class PreparedJobClaim:
    """Trusted observation for a later locked claim, never execution authority.

    observed_at precedes provider I/O, so a slow second read cannot refresh an
    old Jira observation. The claim transaction must recheck age, worker scope,
    credential expiry, repository pins and workflow version using fresh DB time.
    """

    worker_id: UUID
    stored: StoredJobSubmission
    observed_at: datetime

    def __post_init__(self) -> None:
        _uuid(self.worker_id)
        object.__setattr__(self, "observed_at", _utc(self.observed_at))
        if (
            self.stored.intent.policy.provider_repository_id is None
            or self.stored.workflow.status is not JobStatus.QUEUED
            or self.stored.workflow.version != 0
            or not self.stored.workflow.blocks_new_submission
            or self.observed_at < self.stored.workflow.updated_at
        ):
            raise ClaimEligibilityRejected("Claim requires bound queued work.")

    def require_fresh(self, *, now: datetime) -> None:
        timestamp = _utc(now)
        if not timedelta(0) <= timestamp - self.observed_at < timedelta(seconds=30):
            raise ClaimEligibilityRejected("Claim eligibility observation is stale.")
