"""Operator-owned Linux runtime pins; no Docker client or application imports."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DockerRuntimeSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    image_id: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    endpoint: Literal[
        "npipe:////./pipe/dockerDesktopLinuxEngine",
        "npipe:////./pipe/docker_engine",
        "unix:///var/run/docker.sock",
    ] = "npipe:////./pipe/dockerDesktopLinuxEngine"
    auth_volume: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_-]{0,62}$")
    model: str | None = Field(
        default=None, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$"
    )
    model_calls_enabled: bool = Field(default=False, strict=True)
    coding_timeout_seconds: int = Field(default=900, ge=1, le=3600, strict=True)
    maximum_runtime_seconds: int = Field(default=3600, ge=1, le=21_600, strict=True)

    @model_validator(mode="after")
    def require_dedicated_authentication(self) -> DockerRuntimeSettings:
        if self.model_calls_enabled and self.auth_volume is None:
            raise ValueError("Model work requires a dedicated authentication volume.")
        if self.coding_timeout_seconds > self.maximum_runtime_seconds:
            raise ValueError("Coding time must fit the total execution budget.")
        return self


class WorkerRuntimeSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="DEVRELAY_WORKER_",
        extra="ignore",
        frozen=True,
        hide_input_in_errors=True,
    )

    docker_runtime: DockerRuntimeSettings | None = None
