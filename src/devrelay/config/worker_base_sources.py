from pydantic import BaseModel, ConfigDict, Field


class WorkerBaseSourcesSettings(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    max_concurrent_requests: int = Field(default=1, strict=True, ge=1, le=1)
    body_timeout_seconds: float = Field(default=5, strict=True, ge=0.1, le=15)
    request_timeout_seconds: float = Field(default=90, strict=True, ge=1, le=90)
