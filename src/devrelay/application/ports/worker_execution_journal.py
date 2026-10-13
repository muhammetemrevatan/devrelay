from typing import Protocol

from devrelay.application.contracts.claims import WorkerClaimRequest
from devrelay.application.contracts.executions import WorkerExecutionIntent
from devrelay.application.contracts.worker_recovery import WorkerRecoveryState


class WorkerExecutionJournal(Protocol):
    async def load_state(self) -> WorkerRecoveryState: ...

    async def get_or_create_execution(
        self, request: WorkerClaimRequest, candidate: WorkerExecutionIntent
    ) -> WorkerExecutionIntent:
        """Commit original pinned preparation before return, with no live authority."""
        ...
