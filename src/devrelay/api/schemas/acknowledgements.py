from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, SecretStr

from devrelay.api.errors import InvalidWorkerClaimRequest
from devrelay.application.contracts.acknowledgements import (
    AcknowledgementResult,
    WorkerAcknowledgementIntent,
    WorkerAcknowledgementRequest,
)
from devrelay.application.contracts.claims import LeaseProof, WorkerClaimRequest


class WorkerAcknowledgementPayload(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    intent: WorkerAcknowledgementIntent
    proof: SecretStr = Field(min_length=43, max_length=43)

    def to_request(self, attempt_id: UUID) -> WorkerAcknowledgementRequest:
        if self.intent.attempt_id != attempt_id:
            raise InvalidWorkerClaimRequest
        return WorkerAcknowledgementRequest(
            intent=self.intent,
            claim=WorkerClaimRequest(
                claim_request_id=self.intent.claim_request_id,
                proof=LeaseProof(self.proof.get_secret_value()),
            ),
        )


class WorkerAcknowledgementResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    result: AcknowledgementResult
