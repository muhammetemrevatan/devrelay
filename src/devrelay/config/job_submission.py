from uuid import UUID

from pydantic import Field

from devrelay.config.operator import OperatorSettings


class JobSubmissionSettings(OperatorSettings):
    actor_id: str = Field(
        strict=True,
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:@-]*$",
    )
    allowed_repository_ids: frozenset[UUID] = Field(min_length=1, max_length=32)
