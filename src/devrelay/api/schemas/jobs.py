from uuid import UUID

from pydantic import BaseModel, ConfigDict

from devrelay.application.contracts.submissions import SavedJobSubmission
from devrelay.domain.jobs.values import JobStatus


class JobSubmissionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    snapshot_id: UUID
    repository_id: UUID


class JobSubmissionResponse(BaseModel):
    job_id: UUID
    source_task_id: UUID
    snapshot_id: UUID
    repository_id: UUID
    status: JobStatus
    created: bool

    @classmethod
    def from_result(cls, result: SavedJobSubmission) -> JobSubmissionResponse:
        job = result.stored.job
        return cls(
            job_id=job.id,
            source_task_id=job.source_task_id,
            snapshot_id=job.snapshot_id,
            repository_id=job.repository_id,
            status=job.status,
            created=result.created,
        )
