import re
from enum import StrEnum
from uuid import UUID

from pydantic import (
    AnyHttpUrl,
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    field_validator,
)


class JiraTokenMode(StrEnum):
    SCOPED = "scoped"
    UNSCOPED = "unscoped"


class JiraSettings(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    site_url: AnyHttpUrl
    cloud_id: UUID
    email: str = Field(min_length=3, max_length=320)
    api_token: SecretStr = Field(min_length=1)
    token_mode: JiraTokenMode = JiraTokenMode.SCOPED
    allowed_project_keys: frozenset[str] = Field(min_length=1, max_length=32)
    acceptance_criteria_field: str | None = None
    request_timeout_seconds: float = Field(default=15, gt=0, le=60)
    max_response_bytes: int = Field(default=1_048_576, ge=1_024, le=2_097_152)

    @field_validator("site_url")
    @classmethod
    def require_cloud_site(cls, value: AnyHttpUrl) -> AnyHttpUrl:
        if (
            value.scheme != "https"
            or not (value.host or "").endswith(".atlassian.net")
            or value.port not in (None, 443)
            or value.path not in (None, "/")
            or value.username is not None
            or value.password is not None
            or value.query is not None
            or value.fragment is not None
        ):
            raise ValueError("Jira site must be an HTTPS Atlassian Cloud site root.")
        return value

    @field_validator("email", "api_token")
    @classmethod
    def reject_credential_whitespace(cls, value: str | SecretStr) -> str | SecretStr:
        text = value.get_secret_value() if isinstance(value, SecretStr) else value
        if not text or text != text.strip() or "\n" in text or "\r" in text:
            raise ValueError(
                "Jira credentials must not contain surrounding whitespace."
            )
        return value

    @field_validator("allowed_project_keys")
    @classmethod
    def validate_project_keys(cls, values: frozenset[str]) -> frozenset[str]:
        if any(re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", item) is None for item in values):
            raise ValueError(
                "Jira project keys must be explicit uppercase identifiers."
            )
        return values

    @field_validator("acceptance_criteria_field")
    @classmethod
    def validate_custom_field(cls, value: str | None) -> str | None:
        if (
            value is not None
            and re.fullmatch(r"customfield_[0-9]{1,10}", value) is None
        ):
            raise ValueError(
                "Acceptance criteria field must be a customfield identifier."
            )
        return value
