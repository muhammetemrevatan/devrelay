from typing import Protocol
from uuid import UUID

from devrelay.application.contracts.candidates import CandidateCommitSpec
from devrelay.application.contracts.editable_workspaces import EditableWorkspace
from devrelay.application.contracts.frozen_candidates import FrozenCandidate
from devrelay.application.contracts.git_sources import (
    GitBaseSourceSnapshot,
    GitWorkspacePreparation,
)
from devrelay.application.ports.git_workspace import GitWorkspacePort


class CandidateWorkspacePort(GitWorkspacePort, Protocol):
    @property
    def git_preparation(self) -> GitWorkspacePreparation | None: ...

    @property
    def editable(self) -> EditableWorkspace | None: ...

    @property
    def candidate_capture_preparation(self) -> GitWorkspacePreparation | None: ...

    async def prepare_candidate_capture(
        self, source: GitBaseSourceSnapshot
    ) -> GitWorkspacePreparation: ...

    async def capture_candidate(
        self,
        preparation: GitWorkspacePreparation,
        editable: EditableWorkspace,
        *,
        capture_id: UUID,
        cycle: int,
        commit: CandidateCommitSpec,
    ) -> FrozenCandidate: ...

    async def verify_candidate(self, candidate: FrozenCandidate) -> None: ...

    async def release_candidate(
        self, candidate: FrozenCandidate
    ) -> EditableWorkspace: ...
