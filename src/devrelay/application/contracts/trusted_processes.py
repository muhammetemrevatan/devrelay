import subprocess
from dataclasses import dataclass, field
from enum import StrEnum
from math import isfinite
from pathlib import PureWindowsPath
from uuid import UUID

from devrelay.application.contracts.processes import ProcessStopStatus

TRUSTED_PROCESS_READ_SIZE = 8192
_MAXIMUM_OUTPUT = 1024 * 1024


def _path(value: str) -> None:
    if type(value) is not str or not value or "\x00" in value:
        raise ValueError("Trusted process paths require explicit local Windows paths.")
    path = PureWindowsPath(value)
    if (
        not path.is_absolute()
        or len(path.drive) != 2
        or path.drive[1] != ":"
        or not path.drive[0].isascii()
        or not path.drive[0].isalpha()
        or any(character in value[2:] for character in ':"<>|?*')
        or any(part in (".", "..") for part in value.replace("\\", "/").split("/"))
        or len(value.encode("utf-16-le")) > 32760
    ):
        raise ValueError("Trusted process paths require explicit local Windows paths.")


@dataclass(frozen=True, slots=True, kw_only=True, repr=False)
class TrustedProcessSpec:
    """Host-owned command, never executable authority derived from task text.

    Paths and environment are explicit, not executable provenance, identity or
    sandbox evidence. The caller must retain its trusted workspace and policy.
    This bridge never loads a user profile or inherits the coordinator environment.
    """

    executable: str
    arguments: tuple[str, ...]
    working_directory: str
    environment: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        _path(self.executable)
        _path(self.working_directory)
        if type(self.arguments) is not tuple or len(self.arguments) > 64:
            raise ValueError(
                "Trusted process arguments require finite explicit values."
            )
        if any(
            type(item) is not str or "\x00" in item or len(item) > 8192
            for item in self.arguments
        ):
            raise ValueError(
                "Trusted process arguments require finite explicit values."
            )
        command = subprocess.list2cmdline((self.executable, *self.arguments))
        if len(command.encode("utf-16-le")) // 2 + 1 > 32767:
            raise ValueError("Trusted process command exceeds the Windows limit.")
        if type(self.environment) is not tuple or len(self.environment) > 64:
            raise ValueError(
                "Trusted process environment must be explicit and bounded."
            )
        names: set[str] = set()
        size = 1
        for item in self.environment:
            if type(item) is not tuple or len(item) != 2:
                raise ValueError(
                    "Trusted process environment requires name/value pairs."
                )
            name, value = item
            if (
                type(name) is not str
                or not name
                or "=" in name
                or "\x00" in name
                or type(value) is not str
                or "\x00" in value
                or name.casefold() in names
            ):
                raise ValueError("Trusted process environment requires unique names.")
            names.add(name.casefold())
            size += len((name + "=" + value + "\x00").encode("utf-16-le")) // 2
        if size > 32767:
            raise ValueError("Trusted process environment exceeds its finite budget.")


@dataclass(frozen=True, slots=True, kw_only=True)
class TrustedProcessObservation:
    exit_code: int | None
    stdout: bytes = field(repr=False)
    stderr: bytes = field(repr=False)
    stdout_eof: bool
    stderr_eof: bool

    def __post_init__(self) -> None:
        if self.exit_code is not None and (
            type(self.exit_code) is not int or not 0 <= self.exit_code <= 0xFFFFFFFF
        ):
            raise ValueError("Trusted process observation requires a native exit code.")
        if (
            type(self.stdout) is not bytes
            or type(self.stderr) is not bytes
            or len(self.stdout) > TRUSTED_PROCESS_READ_SIZE
            or len(self.stderr) > TRUSTED_PROCESS_READ_SIZE
            or type(self.stdout_eof) is not bool
            or type(self.stderr_eof) is not bool
        ):
            raise ValueError("Trusted process observation exceeds its finite budget.")


@dataclass(frozen=True, slots=True, kw_only=True)
class TrustedProcessPolicy:
    maximum_runtime_seconds: float = 60
    operation_timeout_seconds: float = 1
    stop_timeout_seconds: float = 3
    monitor_interval_seconds: float = 0.05
    maximum_observations: int = 100_000
    maximum_output_bytes: int = _MAXIMUM_OUTPUT

    def __post_init__(self) -> None:
        for name, minimum, maximum in (
            ("maximum_runtime_seconds", 0.1, 3600),
            ("operation_timeout_seconds", 0.05, 5),
            ("stop_timeout_seconds", 0.1, 10),
            ("monitor_interval_seconds", 0.01, 1),
        ):
            value = getattr(self, name)
            if (
                type(value) not in (int, float)
                or not minimum <= value <= maximum
                or not isfinite(value)
            ):
                raise ValueError("Trusted process timing requires finite limits.")
        for name, maximum in (
            ("maximum_observations", 100_000),
            ("maximum_output_bytes", _MAXIMUM_OUTPUT),
        ):
            value = getattr(self, name)
            if type(value) is not int or not 1 <= value <= maximum:
                raise ValueError("Trusted process budgets require bounded integers.")


class TrustedProcessStopReason(StrEnum):
    EXITED = "exited"
    AUTHORITY_LOST = "authority_lost"
    CANCELLED = "cancelled"
    RUNTIME_LIMIT = "runtime_limit"
    OUTPUT_LIMIT = "output_limit"
    OPERATION_FAILED = "operation_failed"
    CLOCK_FAILED = "clock_failed"
    OBSERVATION_LIMIT = "observation_limit"
    REQUESTED = "requested"


@dataclass(frozen=True, slots=True, kw_only=True)
class TrustedProcessResult:
    """Process-local fixture/host command evidence, never sandbox or lease release."""

    execution_id: UUID
    reason: TrustedProcessStopReason
    exit_code: int | None
    stdout: bytes = field(repr=False)
    stderr: bytes = field(repr=False)
    stop_status: ProcessStopStatus

    def __post_init__(self) -> None:
        if (
            not isinstance(self.execution_id, UUID)
            or not isinstance(self.reason, TrustedProcessStopReason)
            or not isinstance(self.stop_status, ProcessStopStatus)
            or type(self.stdout) is not bytes
            or type(self.stderr) is not bytes
            or len(self.stdout) + len(self.stderr) > _MAXIMUM_OUTPUT
            or (
                self.exit_code is not None
                and (
                    type(self.exit_code) is not int
                    or not 0 <= self.exit_code <= 0xFFFFFFFF
                )
            )
        ):
            raise ValueError("Trusted process result requires bounded typed evidence.")
