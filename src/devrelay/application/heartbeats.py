from collections.abc import Callable

from devrelay.application.contracts.claims import LeaseProof
from devrelay.application.contracts.heartbeats import HeartbeatRequest, HeartbeatResult
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.heartbeat_persistence import HeartbeatStore


class HeartbeatAssignedJob:
    """Guard original proof; persistence checks current authority under its locks."""

    def __init__(
        self, store: HeartbeatStore, proof_guard: Callable[[LeaseProof], None]
    ) -> None:
        self._store = store
        self._proof_guard = proof_guard

    async def execute(
        self, request: HeartbeatRequest, authorization: WorkerAuthorization
    ) -> HeartbeatResult:
        self._proof_guard(request.claim.proof)
        result = await self._store.heartbeat(
            request.identify(authorization), authorization
        )
        result.require_correlation(request, authorization)
        return result
