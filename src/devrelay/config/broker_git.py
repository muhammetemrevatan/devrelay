from pathlib import PurePosixPath

from pydantic import BaseModel, ConfigDict, Field, field_validator


class BrokerGitSettings(BaseModel):
    """Explicit installed Linux Git identity; disabled unless separately supplied."""

    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    executable: str
    executable_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    version: str = Field(pattern=r"^git version [A-Za-z0-9. -]{1,80}$")
    exec_directory: str
    https_helper_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    private_directory: str
    operation_timeout_seconds: float = Field(default=10, strict=True, ge=0.05, le=30)
    total_timeout_seconds: float = Field(default=30, strict=True, ge=0.05, le=60)
    stop_timeout_seconds: float = Field(default=2, strict=True, ge=0.05, le=5)
    maximum_disk_bytes: int = Field(
        default=16 * 1024**2, strict=True, ge=1024**2, le=32 * 1024**2
    )
    maximum_retained_bytes: int = Field(
        default=64 * 1024**2, strict=True, ge=1024**2, le=256 * 1024**2
    )
    maximum_processes: int = Field(default=8, strict=True, ge=1, le=8)

    @field_validator("executable", "exec_directory", "private_directory")
    @classmethod
    def require_explicit_linux_path(cls, value: str) -> str:
        if (
            not value.startswith("/")
            or value.startswith("//")
            or "\\" in value
            or "\x00" in value
            or str(PurePosixPath(value)) != value
            or any(part in (".", "..") for part in value.split("/"))
            or len(value) > 4096
        ):
            raise ValueError("Broker Git requires canonical absolute Linux paths.")
        return value
