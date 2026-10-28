from pydantic import BaseModel, ConfigDict, Field, model_validator


class WorkerCodingResultsSettings(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    max_concurrent_requests: int = Field(default=1, strict=True, ge=1, le=1)
    body_timeout_seconds: float = Field(default=5, strict=True, ge=0.1, le=15)
    request_timeout_seconds: float = Field(default=30, strict=True, ge=1, le=60)

    @model_validator(mode="after")
    def require_body_deadline_within_request(self) -> WorkerCodingResultsSettings:
        if self.body_timeout_seconds > self.request_timeout_seconds:
            raise ValueError(
                "Result body deadline must fit the overall request deadline."
            )
        return self
