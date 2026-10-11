from pydantic import BaseModel, ConfigDict, Field


class WorkerClaimsSettings(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    max_concurrent_requests: int = Field(default=4, strict=True, ge=1, le=4)
    body_timeout_seconds: float = Field(default=5, ge=0.1, le=15)
