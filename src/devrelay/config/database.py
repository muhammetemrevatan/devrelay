from pydantic import BaseModel, ConfigDict, Field, SecretStr


class DatabaseSettings(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    host: str = Field(min_length=1, max_length=253)
    port: int = Field(default=5432, ge=1, le=65535)
    database: str = Field(min_length=1, max_length=63)
    username: str = Field(min_length=1, max_length=63)
    password: SecretStr = Field(min_length=1)
    schema_name: str = Field(default="public", pattern=r"^[a-z_][a-z0-9_]{0,62}$")
    pool_size: int = Field(default=4, ge=1, le=10)
    connect_timeout_seconds: int = Field(default=5, ge=1, le=30)
    statement_timeout_ms: int = Field(default=5000, ge=100, le=60000)
    lock_timeout_ms: int = Field(default=2000, ge=100, le=30000)
    sslmode: str = Field(
        default="prefer", pattern=r"^(disable|prefer|require|verify-ca|verify-full)$"
    )
