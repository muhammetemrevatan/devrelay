from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    field_validator,
    model_validator,
)


class GitHubRepositoryId(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    repository_id: UUID
    github_id: int = Field(gt=0, le=2**63 - 1, strict=True)


class GitHubSettings(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    api_token: SecretStr = Field(min_length=1, max_length=512)
    repository_ids: tuple[GitHubRepositoryId, ...] = Field(min_length=1, max_length=32)
    request_timeout_seconds: float = Field(default=15, gt=0, le=60)
    observation_timeout_seconds: float = Field(default=30, gt=0, le=120)
    max_response_bytes: int = Field(default=2_097_152, ge=1024, le=4_194_304)
    max_pull_request_pages: int = Field(default=5, ge=1, le=10, strict=True)

    @field_validator("api_token")
    @classmethod
    def valid_token(cls, value: SecretStr) -> SecretStr:
        text = value.get_secret_value()
        if not text.isascii() or any(
            ord(char) <= 32 or ord(char) == 127 for char in text
        ):
            raise ValueError(
                "GitHub credential must be nonblank ASCII without whitespace."
            )
        return value

    @model_validator(mode="after")
    def unique_bindings(self) -> GitHubSettings:
        if len({item.repository_id for item in self.repository_ids}) != len(
            self.repository_ids
        ) or len({item.github_id for item in self.repository_ids}) != len(
            self.repository_ids
        ):
            raise ValueError("GitHub repository identities must be unique.")
        return self
