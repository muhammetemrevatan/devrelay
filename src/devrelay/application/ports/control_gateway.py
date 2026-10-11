from typing import Protocol
from uuid import UUID

from devrelay.application.contracts.acknowledgements import (
    AcknowledgementResult,
    WorkerAcknowledgementRequest,
)
from devrelay.application.contracts.assignment_delivery import AssignmentDelivery
from devrelay.application.contracts.claims import WorkerClaimRequest
from devrelay.application.contracts.worker_recovery import ClaimExchange
from devrelay.application.contracts.workers import WorkerAuthorization


class ControlAccessDenied(Exception):
    """Control rejected current credentials or scope; retain pending recovery."""


class ControlUnavailable(Exception):
    """A response may have been lost; retry only the durable original request."""


class InvalidControlResponse(Exception):
    """Control returned an unsupported or uncorrelated response."""


class ControlClaimRejected(Exception):
    """Control rejected the claim; operator reconciliation may be needed."""


class ControlGateway(Protocol):
    async def acknowledge(
        self, request: WorkerAcknowledgementRequest, authorization: WorkerAuthorization
    ) -> AcknowledgementResult: ...

    async def identify(self) -> WorkerAuthorization: ...

    async def claim(
        self, request: WorkerClaimRequest, authorization: WorkerAuthorization
    ) -> ClaimExchange: ...

    async def read_assignment(
        self,
        attempt_id: UUID,
        request: WorkerClaimRequest,
        authorization: WorkerAuthorization,
    ) -> AssignmentDelivery: ...
