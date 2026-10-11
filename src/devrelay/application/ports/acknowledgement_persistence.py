from typing import Protocol

from devrelay.application.contracts.acknowledgements import (
    AcknowledgementIdentity,
    AcknowledgementResult,
)
from devrelay.application.contracts.workers import WorkerAuthorization


class AcknowledgementStore(Protocol):
    async def acknowledge(
        self,
        identity: AcknowledgementIdentity,
        authorization: WorkerAuthorization,
        *,
        definition_digest: str,
    ) -> AcknowledgementResult:
        """Commit state/history/receipt; exact replay never renews authority."""
        ...
