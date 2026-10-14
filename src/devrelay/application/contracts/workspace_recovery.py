import re
from dataclasses import dataclass, field, replace
from uuid import UUID

from devrelay.application.contracts.claims import ClaimReplayConflict
from devrelay.application.contracts.executions import WorkerExecutionIntent

_RESERVED = {"CON", "PRN", "AUX", "NUL", "CLOCK$", "CONIN$", "CONOUT$"} | {
    prefix + suffix for prefix in ("COM", "LPT") for suffix in "123456789¹²³"
}


def canonical_workspace_path(value: object, *, allow_volume_root: bool = False) -> str:
    """Validate literal local DOS names; physical identity needs native observation."""
    if (
        type(value) is not str
        or not 3 <= len(value) <= 200
        or re.match(r"^[A-Za-z]:\\", value) is None
    ):
        raise ValueError("Workspace location requires a canonical local path.")
    if value == value[:3]:
        if not allow_volume_root:
            raise ValueError("Workspace location requires a canonical local path.")
        return value
    for part in value[3:].split("\\"):
        if (
            not part
            or part in (".", "..")
            or part[-1] in ". "
            or part.split(".", 1)[0].rstrip(" .").upper() in _RESERVED
            or any(
                ord(char) < 32
                or 127 <= ord(char) <= 159
                or 0xD800 <= ord(char) <= 0xDFFF
                or char in '/:*?"<>|~'
                for char in part
            )
        ):
            raise ValueError("Workspace location requires a canonical local path.")
    if len(value.encode("utf-16-le")) // 2 > 200:
        raise ValueError("Workspace location requires a canonical local path.")
    return value


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkspaceDirectoryIdentity:
    volume_serial_number: int
    file_index: int

    def __post_init__(self) -> None:
        if (
            type(self.volume_serial_number) is not int
            or not 0 <= self.volume_serial_number < 2**32
            or type(self.file_index) is not int
            or not 0 < self.file_index < 2**64
        ):
            raise ValueError("Workspace directory requires a native identity.")


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkspaceLocation:
    """Observed trusted configuration, not a filesystem capability or sandbox."""

    root_directory: str = field(repr=False)
    policy_digest: str
    identity: WorkspaceDirectoryIdentity

    def __post_init__(self) -> None:
        canonical_workspace_path(self.root_directory)
        if (
            type(self.policy_digest) is not str
            or re.fullmatch(r"[0-9a-f]{64}", self.policy_digest) is None
            or not isinstance(self.identity, WorkspaceDirectoryIdentity)
        ):
            raise ValueError("Workspace location requires bounded observed pins.")


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerWorkspaceIntent:
    """Commit before creation; historical input never authorizes another mkdir."""

    workspace_request_id: UUID
    execution: WorkerExecutionIntent
    location: WorkspaceLocation

    def __post_init__(self) -> None:
        if (
            not isinstance(self.workspace_request_id, UUID)
            or not isinstance(self.execution, WorkerExecutionIntent)
            or not isinstance(self.location, WorkspaceLocation)
        ):
            raise ValueError("Workspace intent requires original execution pins.")

    def require_same_input(self, candidate: WorkerWorkspaceIntent) -> None:
        if not isinstance(candidate, WorkerWorkspaceIntent) or self != replace(
            candidate, workspace_request_id=self.workspace_request_id
        ):
            raise ClaimReplayConflict("Workspace intent conflicts with original input.")


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkspacePreparationDecision:
    intent: WorkerWorkspaceIntent
    created: bool

    def __post_init__(self) -> None:
        if (
            not isinstance(self.intent, WorkerWorkspaceIntent)
            or type(self.created) is not bool
        ):
            raise ValueError("Workspace preparation requires a typed commit decision.")


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerWorkspaceCompletion:
    """Immutable directory observation, not evidence of checkout or stopped work."""

    intent: WorkerWorkspaceIntent
    identity: WorkspaceDirectoryIdentity

    def __post_init__(self) -> None:
        if (
            not isinstance(self.intent, WorkerWorkspaceIntent)
            or not isinstance(self.identity, WorkspaceDirectoryIdentity)
            or self.identity.volume_serial_number
            != self.intent.location.identity.volume_serial_number
            or self.identity == self.intent.location.identity
        ):
            raise ValueError(
                "Workspace completion requires a distinct same-volume child."
            )


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkspaceRecovery:
    """Historical input/completion only; never a usable workspace reservation."""

    intent: WorkerWorkspaceIntent | None
    completion: WorkerWorkspaceCompletion | None
    history_unknown: bool = False

    def __post_init__(self) -> None:
        if (
            type(self.history_unknown) is not bool
            or (
                self.history_unknown
                and (self.intent is not None or self.completion is not None)
            )
            or (self.intent is None and not self.history_unknown)
            or (
                self.intent is not None
                and not isinstance(self.intent, WorkerWorkspaceIntent)
            )
        ) or (
            self.completion is not None
            and (
                not isinstance(self.completion, WorkerWorkspaceCompletion)
                or self.completion.intent != self.intent
            )
        ):
            raise ValueError("Workspace recovery requires matching historical pins.")
