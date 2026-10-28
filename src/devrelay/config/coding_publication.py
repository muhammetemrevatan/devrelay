from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    field_validator,
    model_validator,
)


class CodingPublicationSettings(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    api_token: SecretStr = Field(min_length=1, max_length=512)
    allowed_repository_ids: frozenset[UUID] = Field(min_length=1, max_length=32)
    max_writes: int = Field(default=128, ge=3, le=512, strict=True)
    max_requests: int = Field(default=1024, ge=16, le=4096, strict=True)
    max_request_bytes: int = Field(
        default=16 * 1024**2, ge=1024, le=32 * 1024**2, strict=True
    )
    max_response_bytes: int = Field(
        default=4 * 1024**2, ge=1024, le=4 * 1024**2, strict=True
    )
    max_total_response_bytes: int = Field(
        default=32 * 1024**2, ge=4096, le=64 * 1024**2, strict=True
    )
    total_timeout_seconds: float = Field(default=90, ge=1, le=120, allow_inf_nan=False)
    request_timeout_seconds: float = Field(
        default=15, ge=0.1, le=60, allow_inf_nan=False
    )
    dispatch_margin_seconds: float = Field(
        default=0.25, ge=0.01, le=5, allow_inf_nan=False
    )

    @field_validator("api_token")
    @classmethod
    def valid_token(cls, value: SecretStr) -> SecretStr:
        token = value.get_secret_value()
        if not token.isascii() or any(
            ord(char) <= 32 or ord(char) == 127 for char in token
        ):
            raise ValueError("Publication requires a nonblank ASCII credential.")
        return value

    @model_validator(mode="after")
    def bounded_time(self) -> CodingPublicationSettings:
        if self.request_timeout_seconds > self.total_timeout_seconds:
            raise ValueError("Publication request timeout exceeds its total budget.")
        return self
