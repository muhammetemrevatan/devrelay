from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from devrelay.application.tasks import (
    RequirementGap,
    RequirementsStatus,
    TaskImportResult,
)


class TaskImportRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)
    task_key: str = Field(
        strict=True,
        min_length=3,
        max_length=85,
        pattern=r"^[A-Z][A-Z0-9_]{0,63}-[1-9][0-9]{0,19}$",
    )


class TaskImportResponse(BaseModel):
    source_task_id: UUID
    snapshot_id: UUID
    display_key: str
    content_fingerprint: str
    fingerprint_version: int
    created: bool
    requirements_status: RequirementsStatus
    missing_requirements: tuple[RequirementGap, ...]
    observed_labels: tuple[str, ...]

    @classmethod
    def from_result(cls, result: TaskImportResult) -> TaskImportResponse:
        snapshot = result.saved.snapshot
        return cls(
            source_task_id=result.saved.source_task_id,
            snapshot_id=snapshot.id,
            display_key=snapshot.display_key,
            content_fingerprint=snapshot.content_fingerprint,
            fingerprint_version=snapshot.fingerprint_version,
            created=result.saved.created,
            requirements_status=result.requirements_status,
            missing_requirements=result.missing_requirements,
            observed_labels=tuple(sorted(result.observed_labels)),
        )
