"""One durable broker owner; provider uncertainty never authorizes another write."""

import asyncio
import hashlib
import hmac
import re
from collections.abc import Callable, Coroutine
from copy import deepcopy
from dataclasses import dataclass, fields, is_dataclass, replace
from datetime import UTC, datetime
from enum import Enum
from math import isfinite
from typing import Any, cast
from uuid import UUID, uuid4

from devrelay.application.coding_result_preparation import compile_coding_checks
from devrelay.application.contracts.assignment_delivery import AssignedJobDefinition
from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    ClaimRequestIdentity,
)
from devrelay.application.contracts.coding_publications import (
    ProviderPublication,
    PublicationHistory,
    PublicationObservation,
    PublicationPlan,
    PublicationPolicy,
    PublicationWrite,
    PublicationWriteKind,
)
from devrelay.application.contracts.coding_result_acceptance import PreparedCodingResult
from devrelay.application.contracts.coding_result_receipts import (
    CodingPublicationState,
    CodingPublicationStep,
    CodingResultReceipt,
    CodingResultStatus,
    CodingResultUnavailable,
    CodingWritePermit,
    TerminalCodingResult,
    VerifiedCodingPublication,
)
from devrelay.application.contracts.coding_result_requests import (
    MAX_CODING_RESULT_REQUEST_BYTES,
    CodingResultIdentity,
)
from devrelay.application.contracts.coding_result_wire import (
    decode_coding_result_intent,
    encode_coding_result_intent,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.plan_coding_publication import build_publication_plan
from devrelay.application.ports.coding_publication_provider import (
    CodingPublicationProvider,
)
from devrelay.application.ports.coding_result_persistence import CodingPublicationStore
from devrelay.domain.jobs.values import CheckResult, _utc

_ERROR = "Coding publication requires reconciliation."
_DEFAULT_POLICY = PublicationPolicy()


def _metadata[T](value: T) -> T:
    """Own bounded metadata without touching a caller's potentially huge report graph."""
    remaining = 2 * 1024**2
    visited: set[int] = set()
    stack: list[tuple[object, int]] = [(value, 0)]
    while stack:
        item, depth = stack.pop()
        if id(item) in visited:
            continue
        visited.add(id(item))
        if depth > 32 or len(visited) > 8192:
            raise CodingResultUnavailable(_ERROR)
        if type(item) is str:
            remaining -= 4 * len(item)
        elif type(item) is bytes:
            remaining -= len(item)
        elif item is None or isinstance(item, (int, UUID, datetime, Enum)):
            pass
        elif isinstance(item, (tuple, frozenset)):
            if len(item) > 4096:
                raise CodingResultUnavailable(_ERROR)
            stack.extend((part, depth + 1) for part in item)
        elif is_dataclass(item) and not isinstance(item, type):
            stack.extend(
                (getattr(item, field.name), depth + 1) for field in fields(item)
            )
        else:
            raise CodingResultUnavailable(_ERROR)
        if remaining < 0:
            raise CodingResultUnavailable(_ERROR)
    return deepcopy(value)


@dataclass(frozen=True, slots=True)
class _Input:
    payload: bytes
    payload_sha256: str
    fingerprint: str
    claim: ClaimRequestIdentity
    definition: AssignedJobDefinition
    checks: tuple[tuple[CheckResult, ...], ...]


@dataclass(slots=True)
class _Dispatch:
    owner: asyncio.Task[object] | None
    cancelled: bool = False


def _snapshot(prepared: PreparedCodingResult) -> _Input:
    if (
        not isinstance(prepared, PreparedCodingResult)
        or type(prepared.payload_bytes) is not bytes
        or not 1 <= len(prepared.payload_bytes) <= MAX_CODING_RESULT_REQUEST_BYTES
        or not isinstance(prepared.identity, CodingResultIdentity)
        or type(prepared.payload_sha256) is not str
        or re.fullmatch(r"[0-9a-f]{64}", prepared.payload_sha256) is None
        or type(prepared.identity.fingerprint) is not str
        or re.fullmatch(r"[0-9a-f]{64}", prepared.identity.fingerprint) is None
    ):
        raise CodingResultUnavailable(_ERROR)
    return _Input(
        prepared.payload_bytes,
        prepared.payload_sha256,
        prepared.identity.fingerprint,
        _metadata(prepared.identity.claim),
        _metadata(prepared.definition),
        _metadata(prepared.cycle_checks),
    )


def _capture(source: _Input) -> PreparedCodingResult:
    if not hmac.compare_digest(
        hashlib.sha256(source.payload).hexdigest(), source.payload_sha256
    ):
        raise CodingResultUnavailable(_ERROR)
    intent = decode_coding_result_intent(source.payload)
    if encode_coding_result_intent(intent) != source.payload:
        raise CodingResultUnavailable(_ERROR)
    identity = CodingResultIdentity(intent=intent, claim=source.claim)
    identity.require_replay(source.fingerprint)
    definition = replace(
        source.definition,
        snapshot=replace(
            source.definition.snapshot,
            identity=replace(source.definition.snapshot.identity),
        ),
        checks=tuple(replace(check) for check in source.definition.checks),
    )
    intent.result.validate(definition)
    compiled = compile_coding_checks(intent.result)
    if compiled != source.checks:
        raise ClaimReplayConflict("Publication changed accepted check evidence.")
    return PreparedCodingResult(
        identity=identity,
        definition=definition,
        payload_bytes=source.payload,
        payload_sha256=source.payload_sha256,
        cycle_checks=compiled,
    )


def _status(
    status: CodingResultStatus,
    prepared: PreparedCodingResult,
    *,
    receipt: CodingResultReceipt | None = None,
) -> None:
    if not isinstance(status, CodingResultStatus):
        raise CodingResultUnavailable(_ERROR)
    status.__post_init__()
    status.receipt.require_input(prepared.identity, job_id=prepared.definition.job_id)
    if receipt is not None and status.receipt != receipt:
        raise ClaimReplayConflict("Publication changed its accepted result receipt.")
    if status.terminal is not None:
        reference = status.terminal.reference
        if (
            reference.repository_id,
            reference.source_branch,
            reference.target_branch,
            reference.head_revision,
        ) != (
            prepared.definition.repository_id,
            prepared.definition.authorized_branch,
            prepared.definition.target_branch,
            status.receipt.candidate_revision,
        ):
            raise ClaimReplayConflict(
                "Publication changed its terminal branch association."
            )


def _history(history: PublicationHistory, prepared: PreparedCodingResult) -> None:
    if not isinstance(history, PublicationHistory):
        raise CodingResultUnavailable(_ERROR)
    history.__post_init__()
    _status(history.status, prepared)
    if history.status.state is CodingPublicationState.ACCEPTED and (
        history.invocation_id is not None
        or history.last_step is not None
        or history.last_event_kind != "accepted"
    ):
        raise CodingResultUnavailable(_ERROR)
    if (
        history.status.state is not CodingPublicationState.ACCEPTED
        and history.invocation_id is None
    ):
        raise CodingResultUnavailable(_ERROR)


class PublishCodingResult:
    def __init__(
        self,
        store: CodingPublicationStore,
        provider: CodingPublicationProvider,
        *,
        allowed_repository_ids: frozenset[UUID],
        policy: PublicationPolicy = _DEFAULT_POLICY,
        monotonic: Callable[[], float],
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if (
            type(allowed_repository_ids) is not frozenset
            or not 1 <= len(allowed_repository_ids) <= 32
            or any(
                not isinstance(identity, UUID) for identity in allowed_repository_ids
            )
        ):
            raise ValueError("Publisher requires explicit bounded repository scope.")
        self._store, self._provider = store, provider
        self._allowed = allowed_repository_ids
        self._policy = replace(policy)
        self._monotonic, self._now = monotonic, now
        self._last_time: float | None = None
        self._active = self._closed = False
        self._pending: set[asyncio.Task[object]] = set()
        self._closing: asyncio.Task[None] | None = None

    def _time(self) -> float:
        value = self._monotonic()
        if (
            type(value) not in (int, float)
            or not isfinite(value)
            or value < 0
            or (self._last_time is not None and value < self._last_time)
        ):
            raise CodingResultUnavailable(_ERROR)
        self._last_time = float(value)
        return float(value)

    def _admit(
        self, prepared: PreparedCodingResult, authorization: WorkerAuthorization
    ) -> tuple[_Input, WorkerAuthorization]:
        if (
            self._closed
            or self._active
            or any(not task.done() for task in self._pending)
        ):
            raise CodingResultUnavailable(_ERROR)
        source, current = _snapshot(prepared), _metadata(authorization)
        current.__post_init__()
        if (
            source.definition.repository_id not in self._allowed
            or source.definition.repository_id not in current.allowed_repository_ids
            or source.claim.worker_id != current.worker_id
        ):
            raise ClaimReplayConflict("Publisher repository or worker scope changed.")
        self._active = True
        return source, current

    async def _tracked[T](self, operation: Coroutine[Any, Any, T]) -> T:
        task = asyncio.create_task(operation)
        tracked = cast(asyncio.Task[object], task)
        self._pending.add(tracked)

        def settled(done: asyncio.Task[object]) -> None:
            if not done.cancelled():
                done.exception()
            self._pending.discard(done)

        tracked.add_done_callback(settled)
        return await asyncio.shield(task)

    def _live(self, cancelled: _Dispatch, deadline: float) -> None:
        if (
            self._closed
            or cancelled.cancelled
            or (cancelled.owner is not None and cancelled.owner.cancelling())
            or self._time() >= deadline
        ):
            raise CodingResultUnavailable("Publication dispatch admission ended.")

    async def _read(
        self, prepared: PreparedCodingResult, authorization: WorkerAuthorization
    ) -> PublicationHistory | None:
        result = await self._store.read_publication(prepared, authorization)
        if result is not None:
            result = _metadata(result)
            _history(result, prepared)
        return result

    async def _uncertain(
        self,
        prepared: PreparedCodingResult,
        authorization: WorkerAuthorization,
        step: CodingPublicationStep,
    ) -> None:
        try:
            status = await self._store.mark_uncertain(
                prepared, authorization, step=step
            )
            _status(status, prepared)
            if status.state is not CodingPublicationState.UNCERTAIN:
                raise CodingResultUnavailable(_ERROR)
        except Exception:
            # The original planned event still retains the reservation if this
            # acknowledgement is unavailable; never retry a provider write.
            pass

    async def _write(
        self,
        prepared: PreparedCodingResult,
        authorization: WorkerAuthorization,
        plan: PublicationPlan,
        write: PublicationWrite,
        step: CodingPublicationStep,
        receipt: CodingResultReceipt,
        cancelled: _Dispatch,
        deadline: float,
    ) -> tuple[CodingResultStatus, PublicationObservation]:
        started = self._time()
        permit = _metadata(
            await self._store.plan_write(prepared, authorization, step=step)
        )
        if not isinstance(permit, CodingWritePermit):
            raise CodingResultUnavailable(_ERROR)
        permit.__post_init__()
        if (
            permit.step != step
            or permit.operation_version != step.expected_operation_version + 1
            or not permit.created
        ):
            raise CodingResultUnavailable(
                "Recovered publication permits cannot dispatch writes."
            )
        dispatched = False
        try:
            if permit.authorized_at is None or permit.authorized_until is None:
                raise CodingResultUnavailable(_ERROR)
            authority_deadline = (
                started
                + (permit.authorized_until - permit.authorized_at).total_seconds()
                - self._policy.dispatch_margin_seconds
            )

            def before_write() -> None:
                nonlocal dispatched
                self._live(cancelled, min(deadline, authority_deadline))
                if dispatched:
                    raise CodingResultUnavailable(
                        "Publication permit was already consumed."
                    )
                dispatched = True

            observation = _metadata(
                await self._provider.write(plan, write, before_write=before_write)
            )
            if not isinstance(observation, PublicationObservation):
                raise CodingResultUnavailable(_ERROR)
            observation.__post_init__()
            if (
                not dispatched
                or observation.write_id != write.write_id
                or observation.input_digest != write.input_digest
                or (observation.pull_request_number is not None)
                != (write.kind is PublicationWriteKind.PULL_REQUEST)
            ):
                raise ClaimReplayConflict(
                    "Provider observation changed its planned write."
                )
            status = _metadata(
                await self._store.observe_write(
                    prepared,
                    authorization,
                    step=step,
                    observation_digest=observation.observation_digest,
                )
            )
            _status(status, prepared, receipt=receipt)
            if (
                status.state is not CodingPublicationState.PUBLISHING
                or status.operation_version != permit.operation_version + 1
            ):
                raise CodingResultUnavailable(_ERROR)
            return status, observation
        except Exception, asyncio.CancelledError:
            await self._uncertain(prepared, authorization, step)
            raise CodingResultUnavailable(_ERROR) from None

    async def _execute(
        self, source: _Input, authorization: WorkerAuthorization, cancelled: _Dispatch
    ) -> TerminalCodingResult:
        deadline = self._time() + self._policy.total_timeout_seconds
        prepared = await asyncio.to_thread(_capture, source)
        self._live(cancelled, deadline)
        history = await self._read(prepared, authorization)
        if history is None:
            raise CodingResultUnavailable("An accepted coding result is required.")
        if history.status.terminal is not None:
            return history.status.terminal
        if history.status.state is not CodingPublicationState.ACCEPTED:
            raise CodingResultUnavailable(
                "An existing publisher requires read-only reconciliation."
            )
        plan = await asyncio.to_thread(
            build_publication_plan, prepared, history.status.receipt, self._policy
        )
        self._live(cancelled, deadline)
        invocation_id = uuid4()
        status = _metadata(
            await self._store.begin_publication(
                prepared, authorization, invocation_id=invocation_id
            )
        )
        _status(status, prepared, receipt=history.status.receipt)
        if (
            status.state is not CodingPublicationState.PUBLISHING
            or status.operation_version != 1
        ):
            raise CodingResultUnavailable(
                "Publication begin did not create a new owner."
            )
        owner = await self._read(prepared, authorization)
        if (
            owner is None
            or owner.invocation_id != invocation_id
            or owner.status != status
            or owner.last_event_kind != "publishing_started"
            or owner.last_step is not None
        ):
            raise CodingResultUnavailable("Publication owner association changed.")
        observation: PublicationObservation | None = None
        for write in plan.writes:
            self._live(cancelled, deadline)
            step = CodingPublicationStep(
                result_id=plan.result_id,
                invocation_id=invocation_id,
                expected_operation_version=status.operation_version,
                step_id=write.write_id,
                step_kind=write.kind.value,
                input_digest=write.input_digest,
            )
            status, observation = await self._write(
                prepared,
                authorization,
                plan,
                write,
                step,
                history.status.receipt,
                cancelled,
                deadline,
            )
        assert observation is not None and observation.pull_request_number is not None
        self._live(cancelled, deadline)
        try:
            verified = _metadata(
                await self._provider.verify(
                    plan, pull_request_number=observation.pull_request_number
                )
            )
        except Exception:
            raise CodingResultUnavailable(_ERROR) from None
        if not isinstance(verified, ProviderPublication):
            raise CodingResultUnavailable(_ERROR)
        verified.__post_init__()
        reference = verified.reference
        if verified.provider_repository_id != plan.provider_repository_id or (
            reference.repository_id,
            reference.number,
            reference.source_branch,
            reference.target_branch,
            reference.head_revision,
        ) != (
            plan.repository_id,
            observation.pull_request_number,
            plan.source_branch,
            plan.target_branch,
            plan.head_revision,
        ):
            raise ClaimReplayConflict(
                "Verified publication changed its accepted candidate."
            )
        now = _utc(self._now())
        if not 0 <= (now - verified.observed_at).total_seconds() <= 30:
            raise CodingResultUnavailable("Publication observation is not fresh.")
        publication = VerifiedCodingPublication(
            result_id=plan.result_id,
            invocation_id=invocation_id,
            expected_operation_version=status.operation_version,
            reference=reference,
            provider_repository_id=verified.provider_repository_id,
            observed_at=verified.observed_at,
        )
        self._live(cancelled, deadline)
        terminal = _metadata(
            await self._store.finalize_publication(
                prepared, authorization, publication=publication
            )
        )
        if not isinstance(terminal, TerminalCodingResult):
            raise CodingResultUnavailable(_ERROR)
        _status(
            CodingResultStatus(
                receipt=terminal.receipt,
                state=CodingPublicationState.FINALIZED,
                operation_version=status.operation_version + 1,
                terminal=terminal,
            ),
            prepared,
            receipt=status.receipt,
        )
        if terminal.reference != reference:
            raise ClaimReplayConflict("Terminal publication changed its verified PR.")
        return terminal

    async def execute(
        self, prepared: PreparedCodingResult, authorization: WorkerAuthorization
    ) -> TerminalCodingResult:
        source, current = self._admit(prepared, authorization)
        cancelled = _Dispatch(cast(asyncio.Task[object] | None, asyncio.current_task()))
        try:
            return await self._tracked(self._execute(source, current, cancelled))
        except asyncio.CancelledError:
            cancelled.cancelled = True
            raise
        finally:
            self._active = False

    async def _reconcile(
        self, source: _Input, authorization: WorkerAuthorization
    ) -> CodingResultStatus | None:
        prepared = await asyncio.to_thread(_capture, source)
        history = await self._read(prepared, authorization)
        if (
            history is None
            or history.status.terminal is not None
            or history.last_step is None
        ):
            return history.status if history is not None else None
        plan = await asyncio.to_thread(
            build_publication_plan, prepared, history.status.receipt, self._policy
        )
        step = history.last_step
        matches = [
            (index, write)
            for index, write in enumerate(plan.writes)
            if write.write_id == step.step_id
        ]
        if len(matches) != 1:
            raise ClaimReplayConflict("Historical publication step changed its plan.")
        index, write = matches[0]
        if (step.step_kind, step.input_digest, step.expected_operation_version) != (
            write.kind.value,
            write.input_digest,
            1 + 2 * index,
        ):
            raise ClaimReplayConflict("Historical publication step changed its input.")
        try:
            observation = _metadata(await self._provider.observe(plan, write))
        except Exception:
            raise CodingResultUnavailable(_ERROR) from None
        if observation is None:
            return await self._fresh_status(
                prepared, authorization, history, history.status
            )
        if not isinstance(observation, PublicationObservation):
            raise CodingResultUnavailable(_ERROR)
        observation.__post_init__()
        if (observation.write_id, observation.input_digest) != (
            write.write_id,
            write.input_digest,
        ) or (observation.pull_request_number is not None) != (
            write.kind is PublicationWriteKind.PULL_REQUEST
        ):
            raise ClaimReplayConflict("Reconciliation changed the original write.")
        if history.last_observation_digest is not None:
            if history.last_observation_digest != observation.observation_digest:
                raise ClaimReplayConflict(
                    "Reconciliation changed a recorded observation."
                )
            return await self._fresh_status(
                prepared, authorization, history, history.status
            )
        result = _metadata(
            await self._store.observe_write(
                prepared,
                authorization,
                step=step,
                observation_digest=observation.observation_digest,
            )
        )
        _status(result, prepared, receipt=history.status.receipt)
        if (
            result.state is not history.status.state
            or result.operation_version != history.status.operation_version + 1
        ):
            raise CodingResultUnavailable(_ERROR)
        return await self._fresh_status(prepared, authorization, history, result)

    async def _fresh_status(
        self,
        prepared: PreparedCodingResult,
        authorization: WorkerAuthorization,
        original: PublicationHistory,
        expected: CodingResultStatus,
    ) -> CodingResultStatus:
        # Provider observation may cross credential expiry. Historical bookkeeping
        # can settle, but disclosure still needs fresh store authentication.
        current = await self._read(prepared, authorization)
        if (
            current is None
            or current.invocation_id != original.invocation_id
            or current.status.receipt != expected.receipt
            or current.status.operation_version < expected.operation_version
            or (
                expected.state is CodingPublicationState.UNCERTAIN
                and current.status.state is not CodingPublicationState.UNCERTAIN
            )
        ):
            raise CodingResultUnavailable(_ERROR)
        return current.status

    async def reconcile(
        self, prepared: PreparedCodingResult, authorization: WorkerAuthorization
    ) -> CodingResultStatus | None:
        source, current = self._admit(prepared, authorization)
        try:
            return await self._tracked(self._reconcile(source, current))
        finally:
            self._active = False

    async def _settle(self) -> None:
        while self._pending:
            for task in tuple(self._pending):
                if task.done():
                    if not task.cancelled():
                        task.exception()
                    self._pending.discard(task)
            pending = tuple(self._pending)
            if pending:
                await asyncio.gather(
                    *(asyncio.shield(task) for task in pending), return_exceptions=True
                )

    async def aclose(self) -> None:
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
