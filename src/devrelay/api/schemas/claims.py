from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, SecretStr
from starlette.responses import Response

from devrelay.application.contracts.assignment_delivery import (
    MAX_ASSIGNMENT_BYTES,
    AssignedJobDefinition,
    AssignmentDelivery,
    InvalidAssignmentDefinition,
)
from devrelay.application.contracts.assignments import ClaimResult, NoWorkReason
from devrelay.application.contracts.claims import LeaseProof, WorkerClaimRequest
from devrelay.application.contracts.worker_recovery import (
    AssignmentReference,
    WorkerClaimOutcome,
)


class WorkerClaimPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    claim_request_id: UUID
    proof: SecretStr = Field(min_length=43, max_length=43)
    protocol_version: int = Field(strict=True, ge=1, le=1)

    def to_request(self) -> WorkerClaimRequest:
        return WorkerClaimRequest(
            claim_request_id=self.claim_request_id,
            proof=LeaseProof(self.proof.get_secret_value()),
        )


class WorkerClaimResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    protocol_version: Literal[1] = 1
    worker_id: UUID
    claim_request_id: UUID
    recorded_at: datetime
    assignment: AssignmentReference | None
    no_work: NoWorkReason | None
    recovered: bool
    lease_authority_active: bool

    @classmethod
    def from_result(
        cls, result: ClaimResult, *, worker_id: UUID, claim_request_id: UUID
    ) -> WorkerClaimResponse:
        outcome = WorkerClaimOutcome.from_result(
            result, worker_id=worker_id, claim_request_id=claim_request_id
        )
        return cls(
            worker_id=outcome.worker_id,
            claim_request_id=outcome.claim_request_id,
            recorded_at=outcome.recorded_at,
            assignment=outcome.assignment,
            no_work=outcome.no_work,
            recovered=result.recovered,
            lease_authority_active=result.lease_authority_active,
        )


class AssignmentDeliveryResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    outcome: WorkerClaimOutcome
    definition: AssignedJobDefinition
    definition_fingerprint: str
    lease_authority_active: bool

    @classmethod
    def render(cls, delivery: AssignmentDelivery) -> Response:
        body = (
            cls(
                outcome=delivery.outcome,
                definition=delivery.definition,
                definition_fingerprint=delivery.definition.fingerprint(),
                lease_authority_active=delivery.lease_authority_active,
            )
            .model_dump_json()
            .encode("utf-8")
        )
        if len(body) > MAX_ASSIGNMENT_BYTES:
            raise InvalidAssignmentDefinition("Assignment exceeds delivery bounds.")
        return Response(
            body, media_type="application/json", headers={"Cache-Control": "no-store"}
        )
