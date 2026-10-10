from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from devrelay.application.contracts.workers import WorkerAuthorization


class WorkerIdentityResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    worker_id: UUID
    allowed_repository_ids: tuple[UUID, ...]
    credential_expires_at: datetime
    protocol_version: Literal[1] = 1

    @classmethod
    def from_authorization(
        cls, authorization: WorkerAuthorization
    ) -> WorkerIdentityResponse:
        return cls(
            worker_id=authorization.worker_id,
            allowed_repository_ids=tuple(sorted(authorization.allowed_repository_ids)),
            credential_expires_at=authorization.credential_expires_at,
        )
