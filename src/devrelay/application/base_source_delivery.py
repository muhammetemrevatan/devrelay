import asyncio
from collections.abc import Callable
from uuid import UUID

from devrelay.application.contracts.assignment_delivery import (
    AssignmentDelivery,
    AssignmentUnavailable,
    InvalidAssignmentDefinition,
)
from devrelay.application.contracts.assignments import ClaimResult
from devrelay.application.contracts.base_source_delivery import (
    BASE_DELIVERY_LIMITS,
    BaseSourceDelivery,
)
from devrelay.application.contracts.base_sources import (
    BaseSourceRequest,
    BaseSourceSnapshot,
)
from devrelay.application.contracts.claims import LeaseProof, WorkerClaimRequest
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.base_source import (
    BaseSourcePort,
    BaseSourceUnavailable,
    InvalidBaseSourceData,
)
from devrelay.application.ports.claim_persistence import ClaimRecoveryStore
from devrelay.domain.jobs.values import AttemptStatus


def _source_request(delivery: AssignmentDelivery) -> BaseSourceRequest:
    definition = delivery.definition
    return BaseSourceRequest(
        repository_id=definition.repository_id,
        provider_repository_id=definition.provider_repository_id,
        repository_slug=definition.repository_slug,
        base_revision=definition.base_revision,
    )


def _inputs(delivery: AssignmentDelivery) -> tuple[BaseSourceRequest, str]:
    try:
        return _source_request(delivery), delivery.definition.fingerprint()
    except TypeError, ValueError:
        raise InvalidAssignmentDefinition(
            "Stored assignment delivery failed validation."
        ) from None


def _export(
    delivery: AssignmentDelivery,
    source_request: BaseSourceRequest,
    definition_digest: str,
    snapshot: BaseSourceSnapshot,
) -> BaseSourceDelivery:
    """Validate provider bytes on a worker thread, after recovery has closed."""
    if not isinstance(snapshot, BaseSourceSnapshot):
        raise InvalidBaseSourceData("Base source failed delivery validation.")
    try:
        if (
            snapshot.request != source_request
            or type(snapshot.files) is not tuple
            or len(snapshot.files) > BASE_DELIVERY_LIMITS.max_files
            or any(
                type(item.content) is not bytes
                or len(item.content) > BASE_DELIVERY_LIMITS.max_file_bytes
                for item in snapshot.files
            )
            or sum(len(item.content) for item in snapshot.files)
            > BASE_DELIVERY_LIMITS.max_total_bytes
        ):
            raise ValueError("Base source exceeds delivery pins or limits.")
        snapshot.validate()
        reference = delivery.outcome.assignment
        if reference is None:
            raise ValueError("Base source requires assignment anchors.")
        return BaseSourceDelivery(
            reference=reference,
            claim_request_id=delivery.outcome.claim_request_id,
            definition_digest=definition_digest,
            snapshot=snapshot,
        )
    except AttributeError, TypeError, ValueError:
        raise InvalidBaseSourceData("Base source failed delivery validation.") from None


class ReadAssignedBase:
    """Export one pinned base under fresh authority; never renew or mutate work."""

    def __init__(
        self,
        store: ClaimRecoveryStore,
        source: BaseSourcePort,
        proof_guard: Callable[[LeaseProof], None],
    ) -> None:
        self._store = store
        self._source = source
        self._proof_guard = proof_guard

    async def _recover(
        self,
        attempt_id: UUID,
        request: WorkerClaimRequest,
        authorization: WorkerAuthorization,
    ) -> ClaimResult:
        self._proof_guard(request.proof)
        result = await self._store.recover(
            request.identify(authorization), authorization
        )
        if (
            result is None
            or result.attempt is None
            or result.stored is None
            or result.attempt.id != attempt_id
            or result.attempt.status is not AttemptStatus.RUNNING
            or result.attempt.acknowledged_at is None
            or not result.lease_authority_active
            or result.attempt.lease.worker_id != authorization.worker_id
            or result.stored.job.repository_id
            not in authorization.allowed_repository_ids
        ):
            raise AssignmentUnavailable("Running assignment is unavailable.")
        return result

    async def _current(
        self,
        attempt_id: UUID,
        request: WorkerClaimRequest,
        authorization: WorkerAuthorization,
    ) -> tuple[ClaimResult, AssignmentDelivery]:
        result = await self._recover(attempt_id, request, authorization)
        try:
            delivery = await asyncio.to_thread(
                AssignmentDelivery.from_result,
                result,
                worker_id=authorization.worker_id,
                claim_request_id=request.claim_request_id,
            )
        except ValueError, TypeError:
            raise InvalidAssignmentDefinition(
                "Stored assignment delivery failed validation."
            ) from None
        if (
            delivery.definition.repository_id
            not in authorization.allowed_repository_ids
        ):
            raise AssignmentUnavailable("Running assignment is unavailable.")
        return result, delivery

    async def _require_unchanged(
        self,
        attempt_id: UUID,
        request: WorkerClaimRequest,
        authorization: WorkerAuthorization,
        validated: ClaimResult,
    ) -> None:
        # This final recovery samples fresh DB authority after CPU work. Nothing
        # awaits after it; compare already-materialized bounded immutable pins.
        final = await self._recover(attempt_id, request, authorization)
        original, current = validated.attempt, final.attempt
        if (
            original is None
            or current is None
            or final.stored != validated.stored
            or final.recorded_at != validated.recorded_at
            or current.id != original.id
            or current.job_id != original.job_id
            or current.number != original.number
            or current.lease != original.lease
            or current.acknowledged_at != original.acknowledged_at
        ):
            raise AssignmentUnavailable("Running assignment changed during delivery.")

    async def execute(
        self,
        attempt_id: UUID,
        request: WorkerClaimRequest,
        authorization: WorkerAuthorization,
    ) -> BaseSourceDelivery:
        recovered, current = await self._current(attempt_id, request, authorization)
        source_request, digest = await asyncio.to_thread(_inputs, current)
        await self._require_unchanged(attempt_id, request, authorization, recovered)
        try:
            async with asyncio.timeout(BASE_DELIVERY_LIMITS.total_timeout_seconds):
                snapshot = await self._source.read(source_request)
                delivery = await asyncio.to_thread(
                    _export, current, source_request, digest, snapshot
                )
        except TimeoutError:
            raise BaseSourceUnavailable("Base source delivery timed out.") from None
        await self.require_current(attempt_id, request, authorization, delivery)
        return delivery

    async def require_current(
        self,
        attempt_id: UUID,
        request: WorkerClaimRequest,
        authorization: WorkerAuthorization,
        expected: BaseSourceDelivery,
    ) -> None:
        """Recheck after provider waits and again after response serialization."""
        recovered, current = await self._current(attempt_id, request, authorization)
        source_request, digest = await asyncio.to_thread(_inputs, current)
        if (
            not isinstance(expected, BaseSourceDelivery)
            or current.outcome.assignment != expected.reference
            or current.outcome.claim_request_id != expected.claim_request_id
            or digest != expected.definition_digest
            or source_request != expected.snapshot.request
        ):
            raise AssignmentUnavailable("Running assignment changed during delivery.")
        await self._require_unchanged(attempt_id, request, authorization, recovered)
