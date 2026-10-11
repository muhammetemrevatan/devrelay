from collections.abc import Callable

from devrelay.application.assignment_delivery import ReadAssignedJob
from devrelay.application.contracts.assignments import ClaimResult
from devrelay.application.contracts.claims import LeaseProof, WorkerClaimRequest
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.claim_persistence import ClaimPreparer, ClaimStore


class ClaimJob:
    """Resolve recovery before provider reads; storage commits before returning."""

    def __init__(
        self,
        store: ClaimStore,
        preparer: ClaimPreparer,
        proof_guard: Callable[[LeaseProof], None],
    ) -> None:
        self._store = store
        self._preparer = preparer
        self._proof_guard = proof_guard
        self.assignment_reader = ReadAssignedJob(store, proof_guard)

    async def execute(
        self, request: WorkerClaimRequest, authorization: WorkerAuthorization
    ) -> ClaimResult:
        self._proof_guard(request.proof)
        identity = request.identify(authorization)
        recovered = await self._store.recover(identity, authorization)
        if recovered is not None:
            return recovered
        await self._store.check_admission(authorization)
        observation = await self._preparer.execute(authorization)
        return await self._store.claim(identity, authorization, observation)
