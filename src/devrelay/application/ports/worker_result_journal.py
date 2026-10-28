from typing import Protocol

from devrelay.application.contracts.claims import WorkerClaimRequest
from devrelay.application.contracts.coding_result_receipts import (
    CodingResultReceipt,
    CodingResultStatus,
)
from devrelay.application.contracts.coding_result_requests import (
    WorkerCodingResultIntent,
)
from devrelay.application.contracts.worker_result_recovery import (
    WorkerCodingResultState,
)


class WorkerResultJournal(Protocol):
    async def load_result_state(self) -> WorkerCodingResultState: ...

    async def get_or_create_result(
        self,
        request: WorkerClaimRequest,
        candidate: WorkerCodingResultIntent,
        *,
        target_branch: str,
    ) -> WorkerCodingResultIntent:
        """Commit exact input before HTTP; existing original UUID wins on replay."""
        ...

    async def record_result_acceptance(
        self,
        request: WorkerClaimRequest,
        receipt: CodingResultReceipt,
    ) -> WorkerCodingResultState: ...

    async def record_result_terminal(
        self,
        request: WorkerClaimRequest,
        status: CodingResultStatus,
    ) -> WorkerCodingResultState: ...
