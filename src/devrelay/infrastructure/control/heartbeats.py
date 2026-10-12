from pydantic import BaseModel, ConfigDict, Field

from devrelay.application.contracts.heartbeats import HeartbeatResult


class HeartbeatPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)

    protocol_version: int = Field(ge=1, le=1)
    result: HeartbeatResult
