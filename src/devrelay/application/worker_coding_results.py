"""One original result exchange; retained history never restores execution rights."""

import asyncio
from collections.abc import Coroutine
from copy import deepcopy
from dataclasses import fields, is_dataclass, replace
from datetime import datetime
from enum import Enum
from typing import Any, cast
from uuid import UUID

from devrelay.application.coding_result_preparation import prepare_coding_result
from devrelay.application.contracts.assignment_delivery import (
    MAX_ASSIGNMENT_BYTES,
    AssignmentDelivery,
)
from devrelay.application.contracts.claims import ClaimReplayConflict, LeaseProof
from devrelay.application.contracts.coding_result_acceptance import PreparedCodingResult
from devrelay.application.contracts.coding_result_receipts import (
    CodingResultAcceptance,
    CodingResultStatus,
    CodingResultUnavailable,
)
from devrelay.application.contracts.coding_result_requests import (
    MAX_CODING_RESULT_REQUEST_BYTES,
    WorkerCodingResultIntent,
    WorkerCodingResultRequest,
)
from devrelay.application.contracts.worker_recovery import WorkerRecoveryPhase
from devrelay.application.contracts.worker_result_recovery import (
    WorkerCodingResultState,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.control_coding_results import ControlCodingResultGateway
from devrelay.application.ports.worker_result_journal import WorkerResultJournal
from devrelay.application.worker_executions import ExecutionAuthorityGuard

_UNAVAILABLE = "Coding result exchange is unavailable."


def _own[T](value: T) -> T:
    """Bound graph traversal before copying; byte hashing stays on the executor."""
    remaining = MAX_CODING_RESULT_REQUEST_BYTES + MAX_ASSIGNMENT_BYTES
    pending: list[tuple[object, int]] = [(value, 0)]
    visited: set[int] = set()
    memo: dict[int, object] = {}
    while pending:
        item, depth = pending.pop()
        if id(item) in visited:
            continue
        visited.add(id(item))
        if depth > 32 or len(visited) > 32768:
            raise CodingResultUnavailable(_UNAVAILABLE)
        if isinstance(item, LeaseProof):
            memo[id(item)] = LeaseProof(item.reveal())
        elif type(item) is bytes:
            remaining -= len(item)
        elif type(item) is str:
            remaining -= 4 * len(item)
        elif item is None or isinstance(item, (int, UUID, datetime, Enum)):
            pass
        elif isinstance(item, (tuple, frozenset)):
            if len(item) > 4096:
                raise CodingResultUnavailable(_UNAVAILABLE)
            pending.extend((part, depth + 1) for part in item)
        elif is_dataclass(item) and not isinstance(item, type):
            pending.extend(
                (getattr(item, field.name), depth + 1) for field in fields(item)
            )
        else:
            raise CodingResultUnavailable(_UNAVAILABLE)
        if remaining < 0:
            raise CodingResultUnavailable(_UNAVAILABLE)
    return deepcopy(value, memo)


def _require_state(
    state: WorkerCodingResultState, authorization: WorkerAuthorization
) -> None:
    if not isinstance(state, WorkerCodingResultState):
        raise CodingResultUnavailable(_UNAVAILABLE)
    state.__post_init__()
    if state.history_unknown:
        raise CodingResultUnavailable("Historical result requires reconciliation.")
    reference = (
        state.original.assignment.assignment if state.original.assignment else None
    )
    if reference is not None and (
        reference.worker_id != authorization.worker_id
        or reference.repository_id not in authorization.allowed_repository_ids
    ):
        raise ClaimReplayConflict("Result history changed the configured worker scope.")


def _status_state(
    state: WorkerCodingResultState, response: CodingResultStatus
) -> WorkerCodingResultState:
    response.__post_init__()
    if state.receipt is not None and state.receipt != response.receipt:
        raise ClaimReplayConflict("Recovery changed the retained acceptance.")
    if state.terminal_status is not None and state.terminal_status != response:
        raise ClaimReplayConflict("Recovery regressed terminal result history.")
    return replace(
        state,
        receipt=response.receipt,
        terminal_status=response if response.terminal is not None else None,
    )


def _acceptance_state(
    state: WorkerCodingResultState, response: CodingResultAcceptance
) -> WorkerCodingResultState:
    response.__post_init__()
    if state.receipt is not None and state.receipt != response.receipt:
        raise ClaimReplayConflict("Acceptance changed retained result history.")
    return replace(state, receipt=response.receipt)


def _request(state: WorkerCodingResultState) -> WorkerCodingResultRequest:
    original = state.original.request
    if original is None or state.intent is None:
        raise CodingResultUnavailable(_UNAVAILABLE)
    return WorkerCodingResultRequest(intent=state.intent, claim=original)


def _retained(
    state: WorkerCodingResultState, intent: WorkerCodingResultIntent, target_branch: str
) -> tuple[WorkerCodingResultState, WorkerCodingResultRequest]:
    updated = replace(state, intent=intent, target_branch=target_branch)
    return updated, _request(updated)


def _prepare(
    state: WorkerCodingResultState,
    delivery: AssignmentDelivery,
    candidate: WorkerCodingResultIntent,
    authorization: WorkerAuthorization,
) -> PreparedCodingResult:
    _require_state(state, authorization)
    original = state.original
    delivery.__post_init__()
    reference = delivery.outcome.assignment
    if (
        original.phase is not WorkerRecoveryPhase.ACKNOWLEDGED
        or original.request is None
        or original.assignment != delivery.outcome
        or original.execution != candidate.execution
        or original.acknowledgement != candidate.execution.acknowledgement
        or original.completion is None
        or original.completion.intent != candidate.execution.acknowledgement
        or original.workspace_history_unknown
        or original.workspace_intent is None
        or original.workspace_completion is None
        or original.workspace_intent.execution != candidate.execution
        or original.workspace_completion.intent != original.workspace_intent
        or reference is None
        or reference.worker_id != authorization.worker_id
        or reference.repository_id not in authorization.allowed_repository_ids
    ):
        raise ClaimReplayConflict("Coding result changed original execution pins.")
    original.workspace_intent.location.__post_init__()
    original.workspace_intent.location.identity.__post_init__()
    original.workspace_completion.identity.__post_init__()
    if state.intent is not None:
        state.intent.require_same_input(candidate)
        if state.target_branch != delivery.definition.target_branch:
            raise ClaimReplayConflict("Coding result changed its accepted target.")
    return prepare_coding_result(
        WorkerCodingResultRequest(intent=candidate, claim=original.request),
        authorization,
        delivery.definition,
    )


class WorkerCodingResultExchange:
    """Independent result admission; never holds the heartbeat admission lock.

    A canceled CPU wait keeps its actual work tracked. Once HTTP starts, its
    correlated response and journal write settle together even if the caller
    stops waiting. Neither outcome admits a second submit on this instance.
    """

    def __init__(
        self,
        journal: WorkerResultJournal,
        gateway: ControlCodingResultGateway,
        authorization: WorkerAuthorization,
    ) -> None:
        self._journal, self._gateway = journal, gateway
        self._authorization = _own(authorization)
        self._submitted = self._busy = self._closed = False
        self._submit_cancelled = False
        self._submit_owner: asyncio.Task[object] | None = None
        self._pending: set[asyncio.Task[object]] = set()
        self._closing: asyncio.Task[None] | None = None

    def _admit(self) -> None:
        if self._closed or self._busy or any(not task.done() for task in self._pending):
            raise CodingResultUnavailable(_UNAVAILABLE)
        self._busy = True

    async def _tracked[T](self, operation: Coroutine[Any, Any, T]) -> T:
        if self._closed:
            operation.close()
            raise CodingResultUnavailable(_UNAVAILABLE)
        task = asyncio.create_task(operation)
        tracked = cast(asyncio.Task[object], task)
        self._pending.add(tracked)

        def settled(done: asyncio.Task[object]) -> None:
            if not done.cancelled():
                done.exception()
            self._pending.discard(done)

        tracked.add_done_callback(settled)
        return await asyncio.shield(task)

    async def _known(
        self, state: WorkerCodingResultState, request: WorkerCodingResultRequest
    ) -> CodingResultStatus | None:
        reference = (
            state.original.assignment.assignment if state.original.assignment else None
        )
        if reference is None or state.target_branch is None:
            raise CodingResultUnavailable(_UNAVAILABLE)
        response = await self._gateway.recover(
            request,
            self._authorization,
            job_id=reference.job_id,
            target_branch=state.target_branch,
        )
        if response is None:
            if state.receipt is not None:
                raise CodingResultUnavailable("Accepted result history disappeared.")
            return None
        response = _own(response)
        if not isinstance(response, CodingResultStatus):
            raise CodingResultUnavailable(_UNAVAILABLE)
        proposed = await asyncio.to_thread(_status_state, state, response)
        assert proposed.receipt is not None
        recorded = await self._journal.record_result_acceptance(
            request.claim, proposed.receipt
        )
        if (
            not isinstance(recorded, WorkerCodingResultState)
            or recorded.receipt != response.receipt
        ):
            raise CodingResultUnavailable("Result acceptance was not retained.")
        if response.terminal is not None:
            recorded = await self._journal.record_result_terminal(
                request.claim, response
            )
            if (
                not isinstance(recorded, WorkerCodingResultState)
                or recorded.terminal_status != response
            ):
                raise CodingResultUnavailable("Terminal result was not retained.")
        return response

    async def _post(
        self,
        state: WorkerCodingResultState,
        request: WorkerCodingResultRequest,
        guard: ExecutionAuthorityGuard,
    ) -> CodingResultAcceptance:
        reference = (
            state.original.assignment.assignment if state.original.assignment else None
        )
        if reference is None or state.target_branch is None:
            raise CodingResultUnavailable(_UNAVAILABLE)

        def before_submit() -> None:
            if (
                self._closed
                or self._submit_cancelled
                or (self._submit_owner is not None and self._submit_owner.cancelling())
            ):
                raise CodingResultUnavailable("Result submit admission ended.")
            guard.require_current_assignment(reference)

        before_submit()
        response = await self._gateway.submit(
            request,
            self._authorization,
            job_id=reference.job_id,
            target_branch=state.target_branch,
            before_submit=before_submit,
        )
        response = _own(response)
        if not isinstance(response, CodingResultAcceptance):
            raise CodingResultUnavailable(_UNAVAILABLE)
        await asyncio.to_thread(_acceptance_state, state, response)
        recorded = await self._journal.record_result_acceptance(
            request.claim, response.receipt
        )
        if (
            not isinstance(recorded, WorkerCodingResultState)
            or recorded.receipt != response.receipt
        ):
            raise CodingResultUnavailable("Result acceptance was not retained.")
        return response

    async def submit(
        self,
        delivery: AssignmentDelivery,
        candidate: WorkerCodingResultIntent,
        *,
        guard: ExecutionAuthorityGuard,
    ) -> CodingResultAcceptance:
        if self._submitted:
            raise CodingResultUnavailable(
                "Original result submit was already requested."
            )
        self._submitted = True
        self._admit()
        self._submit_owner = cast(asyncio.Task[object] | None, asyncio.current_task())
        try:
            delivery, candidate = _own((delivery, candidate))
            reference = delivery.outcome.assignment
            if reference is None:
                raise CodingResultUnavailable(_UNAVAILABLE)
            guard.require_current_assignment(reference)
            state = _own(await self._journal.load_result_state())
            guard.require_current_assignment(reference)
            prepared = await self._tracked(
                asyncio.to_thread(
                    _prepare, state, delivery, candidate, self._authorization
                )
            )
            guard.require_current_assignment(reference)
            original = state.original.request
            assert original is not None
            if state.intent is not None:
                request = await self._tracked(asyncio.to_thread(_request, state))
                guard.require_current_assignment(reference)
                known = await self._tracked(self._known(state, request))
                guard.require_current_assignment(reference)
                if known is not None:
                    return CodingResultAcceptance(receipt=known.receipt, recovered=True)
                retained = state.intent
            else:
                guard.require_current_assignment(reference)
                retained = await self._journal.get_or_create_result(
                    original,
                    prepared.identity.intent,
                    target_branch=delivery.definition.target_branch,
                )
                retained = _own(retained)
                guard.require_current_assignment(reference)
                await self._tracked(
                    asyncio.to_thread(
                        prepared.identity.intent.require_same_input, retained
                    )
                )
                guard.require_current_assignment(reference)
            state, request = await self._tracked(
                asyncio.to_thread(
                    _retained, state, retained, delivery.definition.target_branch
                )
            )
            guard.require_current_assignment(reference)
            result = await self._tracked(self._post(state, request, guard))
            guard.require_current_assignment(reference)
            return result
        except asyncio.CancelledError:
            self._submit_cancelled = True
            raise
        finally:
            self._busy = False

    async def recover(self) -> CodingResultStatus | None:
        self._admit()
        try:
            state = _own(await self._journal.load_result_state())
            await self._tracked(
                asyncio.to_thread(_require_state, state, self._authorization)
            )
            if state.intent is None:
                return None
            request = await self._tracked(asyncio.to_thread(_request, state))
            return await self._tracked(self._known(state, request))
        finally:
            self._busy = False

    async def _settle(self) -> None:
        while self._pending:
            for task in tuple(self._pending):
                if task.done():
                    if not task.cancelled():
                        task.exception()
                    self._pending.discard(task)
            pending = tuple(self._pending)
            if not pending:
                return
            await asyncio.gather(
                *(asyncio.shield(task) for task in pending), return_exceptions=True
            )

    async def aclose(self) -> None:
        # Resource owners may close the gateway/journal as soon as this returns.
        # Delay even repeated cancellation until response bookkeeping settles.
        self._closed = True
        if self._closing is None:
            self._closing = asyncio.create_task(self._settle())
        cancelled = False
        while not self._closing.done():
            try:
                await asyncio.shield(self._closing)
            except asyncio.CancelledError:
                cancelled = True
        self._closing.result()
        if cancelled:
            raise asyncio.CancelledError
