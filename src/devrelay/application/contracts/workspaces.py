from dataclasses import dataclass, field

from devrelay.application.contracts.assignment_delivery import AssignedJobDefinition
from devrelay.application.contracts.processes import ExecutionProcessBinding


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkspaceBinding:
    """Full accepted input for directory reservation, never checkout authority."""

    execution: ExecutionProcessBinding
    definition: AssignedJobDefinition = field(repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.execution, ExecutionProcessBinding) or not isinstance(
            self.definition, AssignedJobDefinition
        ):
            raise ValueError("Workspace binding requires typed execution inputs.")
        reference = self.execution.reference
        acknowledgement = self.execution.intent.acknowledgement
        if (
            self.definition.job_id != reference.job_id
            or self.definition.repository_id != reference.repository_id
            or self.definition.created_at > reference.issued_at
            or self.definition.snapshot.id != acknowledgement.snapshot_id
            or self.definition.base_revision != acknowledgement.base_revision
            or self.definition.fingerprint() != acknowledgement.definition_digest
        ):
            raise ValueError("Workspace binding changed accepted definition pins.")


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
class WorkspaceReservation:
    """Process-local directory observation, with no launch or recovery evidence.

    It proves neither Git provenance nor access isolation. It must not be restored
    as permission to reuse a workspace or release a coding reservation.
    """

    binding: WorkspaceBinding
    directory_name: str
    identity: WorkspaceDirectoryIdentity

    def __post_init__(self) -> None:
        if (
            not isinstance(self.binding, WorkspaceBinding)
            or self.directory_name != str(self.binding.execution.intent.execution_id)
            or not isinstance(self.identity, WorkspaceDirectoryIdentity)
        ):
            raise ValueError("Workspace reservation requires exact execution pins.")
