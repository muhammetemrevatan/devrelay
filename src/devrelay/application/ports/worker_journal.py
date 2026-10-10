from typing import Protocol

from devrelay.application.contracts.claims import LeaseProof, WorkerClaimRequest


class WorkerStateUnavailable(RuntimeError):
    """Local recovery state cannot be trusted or committed."""


class WorkerStateBusy(WorkerStateUnavailable):
    """Another process already owns this worker's local state."""


class ProofProtector(Protocol):
    def protect(self, proof: LeaseProof, *, context: bytes) -> bytes: ...

    def unprotect(self, ciphertext: bytes, *, context: bytes) -> LeaseProof: ...


class WorkerClaimJournal(Protocol):
    async def load_pending(self) -> WorkerClaimRequest | None: ...

    async def get_or_create_pending(
        self, candidate: WorkerClaimRequest
    ) -> WorkerClaimRequest:
        """Commit before returning; existing pending input wins over a new poll."""
        ...
