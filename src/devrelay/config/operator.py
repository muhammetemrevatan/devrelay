import re

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator


class OperatorSettings(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    auth_token: SecretStr = Field(min_length=43, max_length=128)
    max_concurrent_requests: int = Field(default=4, ge=1, le=4)
    body_timeout_seconds: float = Field(default=5, ge=0.1, le=15)

    @field_validator("auth_token")
    @classmethod
    def require_url_safe_token(cls, value: SecretStr) -> SecretStr:
        token = value.get_secret_value()
        if re.fullmatch(r"[A-Za-z0-9_-]{43,128}", token) is None or token.startswith(
            "replace-with-"
        ):
            raise ValueError(
                "Operator authentication requires a generated URL-safe token."
            )
        return value
