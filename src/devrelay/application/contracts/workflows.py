from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from uuid import UUID

from devrelay.domain.jobs.job import Job
from devrelay.domain.jobs.values import JobStatus, _utc, _uuid


class WorkflowEventType(StrEnum):
    SUBMITTED = "submitted"
    CLAIMED = "claimed"
    ACKNOWLEDGED = "acknowledged"
    TESTED = "tested"
    REPAIR_REQUESTED = "repair_requested"
    CHECKS_ACCEPTED = "checks_accepted"
    PUBLISHED = "published"


@dataclass(frozen=True, slots=True, kw_only=True)
class JobWorkflowState:
    """Materialized current-state projection, separate from accepted policy."""

    job_id: UUID
    status: JobStatus
    version: int
    blocks_new_submission: bool
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        _uuid(self.job_id)
        if (
            not isinstance(self.status, JobStatus)
            or type(self.version) is not int
            or not 0 <= self.version < 2**63
            or type(self.blocks_new_submission) is not bool
            or self.blocks_new_submission
            != (self.status not in {JobStatus.SUCCEEDED, JobStatus.CANCELLED})
        ):
            raise ValueError("Workflow projection is inconsistent.")
        object.__setattr__(self, "created_at", _utc(self.created_at))
        object.__setattr__(self, "updated_at", _utc(self.updated_at))
        if self.updated_at < self.created_at:
            raise ValueError("Workflow chronology is inconsistent.")

    @classmethod
    def initial(cls, job: Job) -> JobWorkflowState:
        if (
            job.status is not JobStatus.QUEUED
            or job.workflow_version != 0
            or job.created_at != job.updated_at
        ):
            raise ValueError("Initial workflow requires an unassigned definition.")
        return cls(
            job_id=job.id,
            status=JobStatus.QUEUED,
            version=0,
            blocks_new_submission=True,
            created_at=job.created_at,
            updated_at=job.updated_at,
        )
