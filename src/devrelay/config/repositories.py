import re
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class CheckSettings(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    id: str = Field(min_length=1, max_length=128, pattern=r"^[a-z][a-z0-9_-]*$")
    argv: tuple[str, ...] = Field(min_length=1, max_length=64)
    working_directory: str = Field(default=".", min_length=1, max_length=256)
    timeout_seconds: int = Field(ge=1, le=3600, strict=True)

    @field_validator("argv")
    @classmethod
    def bounded_arguments(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(
            not item
            or len(item) > 4096
            or any(ord(char) < 32 or ord(char) == 127 for char in item)
            for item in value
        ):
            raise ValueError("Check arguments must be bounded and contain no controls.")
        if not value[0].strip() or value[0] != value[0].strip():
            raise ValueError("Check executable must be explicit and unpadded.")
        return value

    @field_validator("working_directory")
    @classmethod
    def relative_directory(cls, value: str) -> str:
        # Syntax guard only: the future runner must resolve and contain the path,
        # reject symlink escapes, and supply a restricted environment/process scope.
        if (
            value != "."
            and re.fullmatch(r"[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+)*", value) is None
        ):
            raise ValueError(
                "Check working directory must be a relative portable path."
            )
        return value


class RepositorySettings(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    repository_id: UUID
    slug: str = Field(min_length=3, max_length=256)
    enabled: bool = False
    target_branch: str = Field(min_length=1, max_length=256)
    coding_branch_prefix: str = Field(min_length=1, max_length=180)
    policy_version: str = Field(
        min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_-]+$"
    )
    allowed_project_keys: frozenset[str] = Field(min_length=1, max_length=32)
    readiness_label: str = Field(
        default="agent-ready", min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_-]+$"
    )
    checks: tuple[CheckSettings, ...] = Field(min_length=1, max_length=32)
    max_repair_cycles: int = Field(ge=0, le=5, strict=True)

    @field_validator("slug")
    @classmethod
    def explicit_github_slug(cls, value: str) -> str:
        if re.fullmatch(
            r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,38})/[A-Za-z0-9_][A-Za-z0-9_.-]{0,99}", value
        ) is None or value.endswith(".git"):
            raise ValueError("Repository must be an explicit GitHub owner/name slug.")
        return value

    @field_validator("target_branch", "coding_branch_prefix")
    @classmethod
    def literal_branch(cls, value: str) -> str:
        # Literal branch rules from git-check-ref-format; never expand @{-1}.
        if (
            value.startswith(("-", "/", "refs/"))
            or value.endswith(("/", "."))
            or value == "@"
            or any(part in value for part in ("..", "//", "@{"))
            or any(
                ord(char) < 33 or ord(char) == 127 or char in "~^:?*[\\"
                for char in value
            )
            or any(
                part.startswith(".") or part.endswith(".lock")
                for part in value.split("/")
            )
        ):
            raise ValueError("Repository policy requires a valid literal branch name.")
        return value

    @field_validator("allowed_project_keys")
    @classmethod
    def explicit_project_keys(cls, value: frozenset[str]) -> frozenset[str]:
        if any(re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", key) is None for key in value):
            raise ValueError("Repository source scope requires uppercase project keys.")
        return value

    @model_validator(mode="after")
    def unique_checks(self) -> RepositorySettings:
        if len({check.id for check in self.checks}) != len(self.checks):
            raise ValueError("Repository check identifiers must be unique.")
        return self
