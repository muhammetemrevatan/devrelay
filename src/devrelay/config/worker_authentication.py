from datetime import UTC, datetime
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    field_validator,
)

from devrelay.config.security import validate_bearer_token


class WorkerRegistrationSettings(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    worker_id: UUID
    auth_token: SecretStr = Field(min_length=43, max_length=128)
    expires_at: AwareDatetime
    enabled: bool = Field(default=False, strict=True)
    allowed_repository_ids: frozenset[UUID] = Field(min_length=1, max_length=32)

    @field_validator("auth_token")
    @classmethod
    def require_generated_token(cls, value: SecretStr) -> SecretStr:
        return validate_bearer_token(value)

    @field_validator("expires_at")
    @classmethod
    def normalize_expiry(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)
