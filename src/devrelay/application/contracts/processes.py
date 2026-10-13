from dataclasses import dataclass
from enum import StrEnum
from math import isfinite
from uuid import UUID

from devrelay.application.contracts.executions import WorkerExecutionIntent
from devrelay.application.contracts.worker_recovery import AssignmentReference


@dataclass(frozen=True, slots=True, kw_only=True)
class ExecutionProcessBinding:
    """Trusted association to a live process scope, never launch permission."""

    intent: WorkerExecutionIntent
    reference: AssignmentReference

    def __post_init__(self) -> None:
        if (
            not isinstance(self.intent, WorkerExecutionIntent)
            or not isinstance(self.reference, AssignmentReference)
            or self.intent.acknowledgement.attempt_id != self.reference.attempt_id
            or self.intent.acknowledgement.generation != self.reference.generation
        ):
            raise ValueError("Process scope requires matching original execution pins.")


@dataclass(frozen=True, slots=True, kw_only=True)
class ProcessResourceLimits:
    cpu_percent: int
    committed_memory_bytes: int
    maximum_processes: int

    def __post_init__(self) -> None:
        for name, minimum, maximum in (
            ("cpu_percent", 1, 100),
            ("committed_memory_bytes", 64 * 1024**2, 16 * 1024**3),
            ("maximum_processes", 1, 128),
        ):
            value = getattr(self, name)
            if type(value) is not int or not minimum <= value <= maximum:
                raise ValueError("Process resource limits require bounded integers.")


@dataclass(frozen=True, slots=True, kw_only=True)
class ProcessManagementPolicy:
    maximum_runtime_seconds: float = 3600
    monitor_interval_seconds: float = 0.1
    operation_timeout_seconds: float = 1
    stop_timeout_seconds: float = 3
    maximum_observations: int = 100_000

    def __post_init__(self) -> None:
        for name, minimum, maximum in (
            ("maximum_runtime_seconds", 1, 3600),
            ("monitor_interval_seconds", 0.05, 1),
            ("operation_timeout_seconds", 0.05, 5),
            ("stop_timeout_seconds", 0.1, 10),
        ):
            value = getattr(self, name)
            if (
                type(value) not in (int, float)
                or not minimum <= value <= maximum
                or not isfinite(value)
            ):
                raise ValueError(
                    "Process management timing must be finite and bounded."
                )
        if (
            type(self.maximum_observations) is not int
            or not 1 <= self.maximum_observations <= 100_000
        ):
            raise ValueError("Process observation budget requires a bounded integer.")


class ProcessStopStatus(StrEnum):
    EMPTY_VERIFIED = "empty_verified"
    UNKNOWN = "unknown"


class ProcessStopReason(StrEnum):
    EMPTY = "empty"
    AUTHORITY_LOST = "authority_lost"
    REQUESTED = "requested"
    CANCELLED = "cancelled"
    RUNTIME_LIMIT = "runtime_limit"
    OBSERVATION_FAILED = "observation_failed"
    OBSERVATION_LIMIT = "observation_limit"
    CLOCK_FAILED = "clock_failed"


@dataclass(frozen=True, slots=True, kw_only=True)
class ProcessScopeResult:
    """Fresh empty evidence for associated processes only; no reservation release.

    It proves neither secret isolation, escaped-process termination nor restart
    reconciliation. It must not be restored as current containment from a journal.
    """

    execution_id: UUID
    status: ProcessStopStatus
    reason: ProcessStopReason

    def __post_init__(self) -> None:
        if (
            not isinstance(self.execution_id, UUID)
            or not isinstance(self.status, ProcessStopStatus)
            or not isinstance(self.reason, ProcessStopReason)
        ):
            raise ValueError("Process result requires typed scoped observation.")
