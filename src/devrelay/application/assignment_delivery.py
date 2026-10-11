import asyncio
from collections.abc import Callable
from uuid import UUID

from devrelay.application.contracts.assignment_delivery import (
    AssignmentDelivery,
    AssignmentUnavailable,
    InvalidAssignmentDefinition,
)
from devrelay.application.contracts.claims import LeaseProof, WorkerClaimRequest
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.claim_persistence import ClaimRecoveryStore


class ReadAssignedJob:
    """Recover a known receipt only; never admit, reconcile or claim fresh work."""

    def __init__(
        self, store: ClaimRecoveryStore, proof_guard: Callable[[LeaseProof], None]
    ) -> None:
        self._store = store
        self._proof_guard = proof_guard

    async def execute(
        self,
        attempt_id: UUID,
        request: WorkerClaimRequest,
        authorization: WorkerAuthorization,
    ) -> AssignmentDelivery:
        self._proof_guard(request.proof)
        result = await self._store.recover(
            request.identify(authorization), authorization
        )
        if result is None or result.attempt is None or result.attempt.id != attempt_id:
            raise AssignmentUnavailable("Assigned receipt is unavailable.")
        try:
            return await asyncio.to_thread(
                AssignmentDelivery.from_result,
                result,
                worker_id=authorization.worker_id,
                claim_request_id=request.claim_request_id,
            )
        except ValueError, TypeError:
            raise InvalidAssignmentDefinition(
                "Stored assignment delivery failed validation."
            ) from None
