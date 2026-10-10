from ipaddress import ip_address
from pathlib import Path

from pydantic import AnyHttpUrl, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class WorkerSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="DEVRELAY_WORKER_",
        extra="ignore",
        frozen=True,
        hide_input_in_errors=True,
    )

    control_url: AnyHttpUrl
    auth_token: SecretStr = Field(min_length=32)
    workspace_root: Path = Path("workspaces")
    poll_interval_seconds: int = Field(default=5, ge=1, le=300)

    @field_validator("control_url")
    @classmethod
    def require_protected_transport(cls, value: AnyHttpUrl) -> AnyHttpUrl:
        if value.username is not None or value.password is not None:
            raise ValueError("Control URL must not contain credentials.")
        if value.query is not None or value.fragment is not None:
            raise ValueError("Control URL must not contain a query or fragment.")
        if value.scheme == "https":
            return value
        host = (value.host or "").strip("[]")
        try:
            loopback = ip_address(host).is_loopback
        except ValueError:
            loopback = host == "localhost"
        if not loopback:
            raise ValueError("Plain HTTP is permitted only for a loopback tunnel.")
        return value
