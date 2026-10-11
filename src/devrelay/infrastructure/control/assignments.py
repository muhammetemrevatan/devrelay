from datetime import datetime
from secrets import compare_digest
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from devrelay.application.contracts.assignment_delivery import (
    AssignedJobDefinition,
    AssignmentDelivery,
)
from devrelay.application.contracts.assignments import NoWorkReason
from devrelay.application.contracts.submissions import CheckPolicy
from devrelay.application.contracts.worker_recovery import (
    AssignmentReference,
    WorkerClaimOutcome,
)
from devrelay.domain.tasks import TaskIdentity, TaskSnapshot


class SnapshotPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)

    id: UUID
    identity: TaskIdentity
    display_key: str = Field(max_length=128)
    title: str = Field(max_length=512)
    description: str = Field(max_length=65_536)
    acceptance_criteria: tuple[str, ...] = Field(max_length=32)
    captured_at: datetime
    source_updated_at: datetime | None
    fingerprint_version: int = Field(ge=1, le=1)

    def to_snapshot(self) -> TaskSnapshot:
        return TaskSnapshot(
            **self.model_dump(exclude={"identity", "fingerprint_version"}),
            identity=self.identity,
        )


class DefinitionPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)

    job_id: UUID
    source_task_id: UUID
    repository_id: UUID
    provider_repository_id: int = Field(ge=1, lt=2**63)
    repository_slug: str = Field(max_length=256)
    snapshot: SnapshotPayload
    authorized_branch: str = Field(max_length=256)
    target_branch: str = Field(max_length=256)
    base_revision: str = Field(max_length=64)
    policy_version: str = Field(max_length=64)
    checks: tuple[CheckPolicy, ...] = Field(min_length=1, max_length=32)
    max_repair_cycles: int = Field(ge=0, le=5)
    created_at: datetime

    def to_definition(self) -> AssignedJobDefinition:
        return AssignedJobDefinition(
            **self.model_dump(exclude={"snapshot", "checks"}),
            snapshot=self.snapshot.to_snapshot(),
            checks=self.checks,
        )


class OutcomePayload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)

    protocol_version: int = Field(ge=1, le=1)
    worker_id: UUID
    claim_request_id: UUID
    recorded_at: datetime
    assignment: AssignmentReference | None
    no_work: NoWorkReason | None

    def to_outcome(self) -> WorkerClaimOutcome:
        return WorkerClaimOutcome(
            **self.model_dump(exclude={"assignment", "protocol_version"}),
            assignment=self.assignment,
        )


class DeliveryPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)

    outcome: OutcomePayload
    definition: DefinitionPayload
    definition_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    lease_authority_active: bool

    def to_delivery(self) -> AssignmentDelivery:
        definition = self.definition.to_definition()
        if not compare_digest(definition.fingerprint(), self.definition_fingerprint):
            raise ValueError("Assignment definition fingerprint is inconsistent.")
        return AssignmentDelivery(
            outcome=self.outcome.to_outcome(),
            definition=definition,
            lease_authority_active=self.lease_authority_active,
        )
