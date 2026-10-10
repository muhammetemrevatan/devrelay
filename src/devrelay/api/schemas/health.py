from enum import StrEnum

from pydantic import BaseModel, ConfigDict


class HealthStatus(StrEnum):
    OK = "ok"


class LivenessResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    status: HealthStatus = HealthStatus.OK
