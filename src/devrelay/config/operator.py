from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator

from devrelay.config.security import validate_bearer_token


class OperatorSettings(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    auth_token: SecretStr = Field(min_length=43, max_length=128)
    max_concurrent_requests: int = Field(default=4, ge=1, le=4)
    body_timeout_seconds: float = Field(default=5, ge=0.1, le=15)

    @field_validator("auth_token")
    @classmethod
    def require_url_safe_token(cls, value: SecretStr) -> SecretStr:
        return validate_bearer_token(value)
