from typing import Protocol
from uuid import UUID

from devrelay.application.contracts.coding_publications import PublicationHistory
from devrelay.application.contracts.coding_result_acceptance import PreparedCodingResult
from devrelay.application.contracts.coding_result_receipts import (
    CodingPublicationStep,
    CodingResultAcceptance,
    CodingResultStatus,
    CodingWritePermit,
    TerminalCodingResult,
    VerifiedCodingPublication,
)
from devrelay.application.contracts.workers import WorkerAuthorization


class CodingResultStore(Protocol):
    async def accept(
        self,
        prepared: PreparedCodingResult,
        authorization: WorkerAuthorization,
    ) -> CodingResultAcceptance: ...

    async def recover_result(
        self,
        prepared: PreparedCodingResult,
        authorization: WorkerAuthorization,
    ) -> CodingResultStatus | None: ...


class CodingPublicationStore(CodingResultStore, Protocol):
    """Broker-only coordination; these methods are never worker endpoints."""

    async def read_publication(
        self,
        prepared: PreparedCodingResult,
        authorization: WorkerAuthorization,
    ) -> PublicationHistory | None: ...

    async def begin_publication(
        self,
        prepared: PreparedCodingResult,
        authorization: WorkerAuthorization,
        *,
        invocation_id: UUID,
    ) -> CodingResultStatus: ...

    async def plan_write(
        self,
        prepared: PreparedCodingResult,
        authorization: WorkerAuthorization,
        *,
        step: CodingPublicationStep,
    ) -> CodingWritePermit: ...

    async def observe_write(
        self,
        prepared: PreparedCodingResult,
        authorization: WorkerAuthorization,
        *,
        step: CodingPublicationStep,
        observation_digest: str,
    ) -> CodingResultStatus: ...

    async def mark_uncertain(
        self,
        prepared: PreparedCodingResult,
        authorization: WorkerAuthorization,
        *,
        step: CodingPublicationStep,
    ) -> CodingResultStatus: ...

    async def finalize_publication(
        self,
        prepared: PreparedCodingResult,
        authorization: WorkerAuthorization,
        *,
        publication: VerifiedCodingPublication,
    ) -> TerminalCodingResult: ...
