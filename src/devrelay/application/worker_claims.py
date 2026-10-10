from uuid import uuid4

from devrelay.application.contracts.claims import LeaseProof, WorkerClaimRequest
from devrelay.application.ports.worker_journal import WorkerClaimJournal


class PrepareWorkerClaim:
    """Prepare recoverable local input; no HTTP request or assignment is made."""

    def __init__(self, journal: WorkerClaimJournal) -> None:
        self._journal = journal

    async def prepare(self) -> WorkerClaimRequest:
        pending = await self._journal.load_pending()
        if pending is not None:
            return pending
        candidate = WorkerClaimRequest(
            claim_request_id=uuid4(), proof=LeaseProof.generate()
        )
        return await self._journal.get_or_create_pending(candidate)
