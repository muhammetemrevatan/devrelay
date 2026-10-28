import asyncio

from devrelay.application.assignment_delivery import ReadAssignedJob
from devrelay.application.coding_result_preparation import prepare_coding_result
from devrelay.application.contracts.coding_result_acceptance import PreparedCodingResult
from devrelay.application.contracts.coding_result_receipts import (
    CodingResultAcceptance,
    CodingResultRejected,
    CodingResultStatus,
    CodingResultUnavailable,
)
from devrelay.application.contracts.coding_result_requests import (
    WorkerCodingResultRequest,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.coding_result_persistence import CodingResultStore
from devrelay.domain.jobs.values import StaleAttempt


class AcceptWorkerCodingResult:
    """Consume the original assignment, prepare off-loop, then atomically accept.

    Acceptance retains the coding reservation for trusted broker publication.
    The successful report is historical process/check evidence, never permission
    to launch a process or repeat a provider write. Cancellation may leave an
    accepted result; exact recovery observes it without accepting different input.
    """

    def __init__(self, reader: ReadAssignedJob, store: CodingResultStore) -> None:
        self._reader = reader
        self._store = store
        self._preparing: asyncio.Task[PreparedCodingResult] | None = None
        self._storing: (
            asyncio.Task[CodingResultAcceptance | CodingResultStatus | None] | None
        ) = None
        self._closed = False

    def require_ready(self) -> None:
        """Fail before a new body/encoder while canceled original work settles."""
        if self._closed or any(
            pending is not None and not pending.done()
            for pending in (self._preparing, self._storing)
        ):
            raise CodingResultUnavailable("Coding result delivery is busy or closed.")

    async def _prepare(
        self,
        request: WorkerCodingResultRequest,
        authorization: WorkerAuthorization,
    ) -> PreparedCodingResult:
        self.require_ready()
        if self._storing is not None:
            if not self._storing.cancelled():
                self._storing.exception()
            self._storing = None
        if self._preparing is not None:
            if not self._preparing.done():
                raise CodingResultUnavailable(
                    "A previous result preparation is still settling."
                )
            # Retrieve the previous outcome without treating a late value as
            # this call's prepared input. Never cancel the actual encoder.
            if not self._preparing.cancelled():
                self._preparing.exception()
            self._preparing = None
        delivery = await self._reader.execute(
            request.intent.execution.acknowledgement.attempt_id,
            request.claim,
            authorization,
        )
        # Another request may have finished its assignment read while this call
        # awaited. Admit exactly one encoder before its first scheduling point.
        if self._closed or self._preparing is not None:
            raise CodingResultUnavailable(
                "A previous result preparation is still settling."
            )
        pending = asyncio.create_task(
            asyncio.to_thread(
                prepare_coding_result,
                request,
                authorization,
                delivery.definition,
            )
        )
        self._preparing = pending
        try:
            return await asyncio.shield(pending)
        finally:
            if pending.done() and self._preparing is pending:
                self._preparing = None

    async def execute(
        self,
        request: WorkerCodingResultRequest,
        authorization: WorkerAuthorization,
    ) -> CodingResultAcceptance:
        prepared = await self._prepare(request, authorization)
        try:
            result = await self._invoke_store(prepared, authorization, recover=False)
            if not isinstance(result, CodingResultAcceptance):
                raise CodingResultUnavailable(
                    "Coding acceptance returned invalid metadata."
                )
            return result
        except StaleAttempt:
            raise CodingResultRejected(
                "The original assignment no longer has acceptance authority."
            ) from None

    async def recover(
        self,
        request: WorkerCodingResultRequest,
        authorization: WorkerAuthorization,
    ) -> CodingResultStatus | None:
        prepared = await self._prepare(request, authorization)
        result = await self._invoke_store(prepared, authorization, recover=True)
        if result is not None and not isinstance(result, CodingResultStatus):
            raise CodingResultUnavailable("Coding recovery returned invalid metadata.")
        return result

    async def _perform_store(
        self,
        prepared: PreparedCodingResult,
        authorization: WorkerAuthorization,
        *,
        recover: bool,
    ) -> CodingResultAcceptance | CodingResultStatus | None:
        if recover:
            return await self._store.recover_result(prepared, authorization)
        return await self._store.accept(prepared, authorization)

    async def _invoke_store(
        self,
        prepared: PreparedCodingResult,
        authorization: WorkerAuthorization,
        *,
        recover: bool,
    ) -> CodingResultAcceptance | CodingResultStatus | None:
        self.require_ready()
        pending = asyncio.create_task(
            self._perform_store(prepared, authorization, recover=recover)
        )
        self._storing = pending
        pending.add_done_callback(
            lambda done: None if done.cancelled() else done.exception()
        )
        try:
            return await asyncio.shield(pending)
        finally:
            if pending.done() and self._storing is pending:
                self._storing = None

    async def aclose(self) -> None:
        self._closed = True
        for pending in (self._preparing, self._storing):
            if pending is not None:
                try:
                    await asyncio.shield(pending)
                except Exception:
                    # Its original request owns error classification. Shutdown
                    # only settles actual encoder/transaction work, with no replay.
                    pass
        if self._preparing is not None and self._preparing.done():
            self._preparing = None
        if self._storing is not None and self._storing.done():
            self._storing = None
