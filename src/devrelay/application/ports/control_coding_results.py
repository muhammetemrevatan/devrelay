from collections.abc import Callable
from typing import Protocol
from uuid import UUID

from devrelay.application.contracts.coding_result_receipts import (
    CodingResultAcceptance,
    CodingResultStatus,
)
from devrelay.application.contracts.coding_result_requests import (
    WorkerCodingResultRequest,
)
from devrelay.application.contracts.workers import WorkerAuthorization


class ControlCodingResultGateway(Protocol):
    async def submit(
        self,
        request: WorkerCodingResultRequest,
        authorization: WorkerAuthorization,
        *,
        job_id: UUID,
        target_branch: str,
        before_submit: Callable[[], None] | None = None,
    ) -> CodingResultAcceptance: ...

    async def recover(
        self,
        request: WorkerCodingResultRequest,
        authorization: WorkerAuthorization,
        *,
        job_id: UUID,
        target_branch: str,
    ) -> CodingResultStatus | None: ...
