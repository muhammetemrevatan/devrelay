import asyncio

from devrelay.application.assignment_delivery import ReadAssignedJob
from devrelay.application.contracts.acknowledgements import (
    AcknowledgementRejected,
    AcknowledgementResult,
    WorkerAcknowledgementRequest,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.acknowledgement_persistence import AcknowledgementStore


class AcknowledgeAssignedJob:
    """Validate accepted pins off-transaction; storage fences the atomic mutation."""

    def __init__(self, reader: ReadAssignedJob, store: AcknowledgementStore) -> None:
        self._reader = reader
        self._store = store

    async def execute(
        self, request: WorkerAcknowledgementRequest, authorization: WorkerAuthorization
    ) -> AcknowledgementResult:
        delivery = await self._reader.execute(
            request.intent.attempt_id, request.claim, authorization
        )
        digest = await asyncio.to_thread(delivery.definition.fingerprint)
        if (
            delivery.definition.snapshot.id != request.intent.snapshot_id
            or delivery.definition.base_revision != request.intent.base_revision
            or digest != request.intent.definition_digest
        ):
            raise AcknowledgementRejected(
                "Acknowledgement differs from accepted definition."
            )
        return await self._store.acknowledge(
            request.identify(authorization), authorization, definition_digest=digest
        )
