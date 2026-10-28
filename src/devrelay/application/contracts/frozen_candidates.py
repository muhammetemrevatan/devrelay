from dataclasses import dataclass, field
from uuid import UUID

from devrelay.application.contracts.candidates import CandidateSnapshot
from devrelay.application.contracts.editable_workspaces import EditableWorkspace
from devrelay.application.contracts.git_sources import GitWorkspacePreparation
from devrelay.application.contracts.workspace_recovery import (
    WorkspaceDirectoryIdentity,
    canonical_workspace_path,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class FrozenCandidate:
    """Process-local observation; its owning live handles remain indispensable.

    This value is never a serialized workspace, sandbox or restart capability.
    Read-only check policy and the original native owner must also remain live.
    """

    capture_id: UUID
    editable: EditableWorkspace = field(repr=False)
    preparation: GitWorkspacePreparation = field(repr=False)
    snapshot: CandidateSnapshot = field(repr=False)
    git_directory: str = field(repr=False)
    git_identity: WorkspaceDirectoryIdentity

    def __post_init__(self) -> None:
        if (
            not isinstance(self.capture_id, UUID)
            or not isinstance(self.editable, EditableWorkspace)
            or not isinstance(self.preparation, GitWorkspacePreparation)
            or not isinstance(self.snapshot, CandidateSnapshot)
            or not isinstance(self.git_identity, WorkspaceDirectoryIdentity)
        ):
            raise ValueError("Frozen candidate requires exact original observations.")
        self.editable.__post_init__()
        self.preparation.__post_init__()
        self.snapshot.validate()
        self.git_identity.__post_init__()
        materialization = self.editable.materialization
        reservation = materialization.reservation
        definition = reservation.binding.definition
        if (
            self.preparation.materialization is not materialization
            or self.snapshot.execution_id
            != reservation.binding.execution.intent.execution_id
            or self.snapshot.request != materialization.source
            or self.snapshot.authorized_branch != definition.authorized_branch
            or self.preparation.branch != definition.authorized_branch
            or self.snapshot.cycle > definition.max_repair_cycles
            or self.snapshot.root_tree_sha == materialization.root_tree_sha
            or self.git_identity.volume_serial_number
            != reservation.identity.volume_serial_number
            or self.git_identity
            in (reservation.identity, self.editable.location.identity)
        ):
            raise ValueError("Frozen candidate changed original execution pins.")
        canonical_workspace_path(self.worktree)
        canonical_workspace_path(self.git_directory)
        if self.git_directory != self.worktree + "\\.git\\devrelay\\" + str(
            self.capture_id
        ):
            raise ValueError("Frozen candidate changed its private Git namespace.")

    @property
    def worktree(self) -> str:
        return (
            self.editable.location.root_directory
            + "\\"
            + self.editable.materialization.reservation.directory_name
        )
