from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncEngine

from devrelay.application.assignment_delivery import ReadAssignedJob
from devrelay.application.coding_result_delivery import AcceptWorkerCodingResult
from devrelay.bootstrap.claims import _claim_configuration
from devrelay.bootstrap.persistence import claim_store, coding_result_store
from devrelay.config.control import ControlSettings


@asynccontextmanager
async def open_worker_coding_results(
    settings: ControlSettings,
    engine: AsyncEngine,
) -> AsyncIterator[AcceptWorkerCodingResult]:
    """Server-owned result acceptance/recovery; no publisher or provider I/O."""
    if settings.worker_coding_results is None or settings.worker_claims is None:
        raise ValueError(
            "Coding results require explicitly enabled results and claims."
        )
    policies, proof_guard = _claim_configuration(settings)
    service = AcceptWorkerCodingResult(
        ReadAssignedJob(claim_store(engine, policies), proof_guard),
        coding_result_store(engine, policies),
    )
    try:
        yield service
    finally:
        await service.aclose()
