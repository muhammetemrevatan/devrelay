"""Explicit broker dispatch of one durable result; no polling or execution rights."""

from dataclasses import replace
from typing import Protocol
from uuid import UUID

from devrelay.application.contracts.coding_result_acceptance import PreparedCodingResult
from devrelay.application.contracts.coding_result_receipts import (
    CodingResultRejected,
    CodingResultStatus,
    TerminalCodingResult,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.publish_coding_result import PublishCodingResult


class AcceptedCodingResultReader(Protocol):
    async def read(
        self, result_id: UUID, authorization: WorkerAuthorization
    ) -> PreparedCodingResult | None: ...


class DispatchAcceptedCodingResult:
    """Consume supplied current control authority and a known accepted UUID once.

    The existing publisher owns durable invocation/replay rules. Loading retained
    evidence never creates another publication owner or grants worker launch rights.
    """

    def __init__(
        self, reader: AcceptedCodingResultReader, publisher: PublishCodingResult
    ) -> None:
        self._reader = reader
        self._publisher = publisher

    async def execute(
        self, result_id: UUID, authorization: WorkerAuthorization
    ) -> TerminalCodingResult:
        if not isinstance(result_id, UUID) or not isinstance(
            authorization, WorkerAuthorization
        ):
            raise CodingResultRejected(
                "An accepted result and current worker scope are required."
            )
        captured = replace(authorization)
        prepared = await self._reader.read(result_id, captured)
        if prepared is None:
            raise CodingResultRejected("The accepted coding result is unavailable.")
        return await self._publisher.execute(prepared, captured)

    async def reconcile(
        self, result_id: UUID, authorization: WorkerAuthorization
    ) -> CodingResultStatus | None:
        """Observe the original durable owner; never issue another provider write."""
        if not isinstance(result_id, UUID) or not isinstance(
            authorization, WorkerAuthorization
        ):
            raise CodingResultRejected(
                "An accepted result and current worker scope are required."
            )
        captured = replace(authorization)
        prepared = await self._reader.read(result_id, captured)
        if prepared is None:
            return None
        return await self._publisher.reconcile(prepared, captured)
