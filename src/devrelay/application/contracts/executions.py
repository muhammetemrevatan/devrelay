import re
from dataclasses import dataclass, replace
from enum import StrEnum
from uuid import UUID

from devrelay.application.contracts.acknowledgements import WorkerAcknowledgementIntent
from devrelay.application.contracts.claims import ClaimReplayConflict


class ExecutionRuntime(StrEnum):
    CODEX = "codex"


class ExecutionIsolation(StrEnum):
    WINDOWS_RESTRICTED = "windows_restricted"
    DOCKER_LINUX = "docker_linux"


@dataclass(frozen=True, slots=True, kw_only=True)
class ExecutionPolicy:
    """Pinned configuration, not proof of runtime permissions or containment."""

    runtime: ExecutionRuntime
    isolation: ExecutionIsolation
    policy_digest: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.runtime, ExecutionRuntime)
            or not isinstance(self.isolation, ExecutionIsolation)
            or not isinstance(self.policy_digest, str)
            or re.fullmatch(r"[0-9a-f]{64}", self.policy_digest) is None
        ):
            raise ValueError(
                "Execution policy requires typed runtime and canonical digest."
            )


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerExecutionIntent:
    """Durable preparation only, never permission or evidence of process launch."""

    execution_id: UUID
    acknowledgement: WorkerAcknowledgementIntent
    policy: ExecutionPolicy

    def __post_init__(self) -> None:
        if (
            not isinstance(self.execution_id, UUID)
            or not isinstance(self.acknowledgement, WorkerAcknowledgementIntent)
            or not isinstance(self.policy, ExecutionPolicy)
        ):
            raise ValueError(
                "Execution intent requires typed original pins and policy."
            )

    def require_same_input(self, candidate: WorkerExecutionIntent) -> None:
        if not isinstance(candidate, WorkerExecutionIntent) or self != replace(
            candidate, execution_id=self.execution_id
        ):
            raise ClaimReplayConflict("Execution intent conflicts with original input.")
