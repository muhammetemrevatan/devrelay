from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator


class JiraProject(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore", hide_input_in_errors=True)
    key: str = Field(min_length=1, max_length=64)


class JiraFields(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore", hide_input_in_errors=True)
    summary: str = Field(min_length=1, max_length=512)
    description: JsonValue
    updated: str | None
    project: JiraProject
    labels: list[str] = Field(max_length=64)

    @field_validator("labels")
    @classmethod
    def validate_labels(cls, values: list[str]) -> list[str]:
        if any(not label or len(label) > 128 for label in values):
            raise ValueError("Jira labels must be bounded nonempty strings.")
        return values

    @field_validator("updated")
    @classmethod
    def validate_timestamp(cls, value: str | None) -> str | None:
        if value is not None:
            timestamp = datetime.fromisoformat(value)
            if timestamp.utcoffset() is None:
                raise ValueError("Jira updated timestamp must be timezone-aware.")
        return value


class JiraIssue(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore", hide_input_in_errors=True)
    id: str = Field(pattern=r"^[0-9]+$", max_length=256)
    key: str = Field(pattern=r"^[A-Z][A-Z0-9_]*-[0-9]+$", max_length=128)
    fields: JiraFields
