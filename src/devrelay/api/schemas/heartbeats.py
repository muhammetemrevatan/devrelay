from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, SecretStr

from devrelay.application.contracts.claims import LeaseProof, WorkerClaimRequest
from devrelay.application.contracts.heartbeats import (
    MAXIMUM_LEASE_REVISION,
    HeartbeatRequest,
    HeartbeatResult,
)


class WorkerHeartbeatPayload(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    protocol_version: int = Field(strict=True, ge=1, le=1)
    claim_request_id: UUID
    proof: SecretStr = Field(min_length=43, max_length=43)
    heartbeat_request_id: UUID
    generation: int = Field(strict=True, ge=1, le=1)
    expected_lease_revision: int = Field(strict=True, ge=1, le=MAXIMUM_LEASE_REVISION)

    def to_request(self, attempt_id: UUID) -> HeartbeatRequest:
        return HeartbeatRequest(
            claim=WorkerClaimRequest(
                claim_request_id=self.claim_request_id,
                proof=LeaseProof(self.proof.get_secret_value()),
            ),
            heartbeat_request_id=self.heartbeat_request_id,
            attempt_id=attempt_id,
            generation=self.generation,
            expected_lease_revision=self.expected_lease_revision,
            protocol_version=self.protocol_version,
        )


class WorkerHeartbeatResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    protocol_version: Literal[1] = 1
    result: HeartbeatResult
