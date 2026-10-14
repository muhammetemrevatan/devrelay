from typing import Protocol

from devrelay.application.contracts.claims import WorkerClaimRequest
from devrelay.application.contracts.worker_recovery import WorkerRecoveryState
from devrelay.application.contracts.workspace_recovery import (
    WorkerWorkspaceCompletion,
    WorkerWorkspaceIntent,
    WorkspacePreparationDecision,
)


class WorkerWorkspaceJournal(Protocol):
    async def load_state(self) -> WorkerRecoveryState: ...

    async def get_or_create_workspace(
        self, request: WorkerClaimRequest, candidate: WorkerWorkspaceIntent
    ) -> WorkspacePreparationDecision:
        """Only created=True from this new commit may precede one creation."""
        ...

    async def record_workspace_completion(
        self, request: WorkerClaimRequest, completion: WorkerWorkspaceCompletion
    ) -> WorkerWorkspaceCompletion:
        """Record matching historical observation without renewing authority."""
        ...
