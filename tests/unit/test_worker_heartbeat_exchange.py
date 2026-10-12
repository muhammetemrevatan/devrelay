import asyncio
from collections.abc import Callable
from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

import pytest

from devrelay.application.contracts.assignment_delivery import AssignmentUnavailable
from devrelay.application.contracts.claims import WorkerClaimRequest
from devrelay.application.contracts.heartbeats import (
    HeartbeatRequest,
    HeartbeatResult,
    LeaseAuthorityObservation,
)
from devrelay.application.contracts.worker_recovery import (
    AcknowledgementCompletion,
    AssignmentReference,
    WorkerHeartbeatIntent,
    WorkerRecoveryPhase,
    WorkerRecoveryState,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.control_gateway import (
    ControlClaimRejected,
    ControlUnavailable,
    InvalidControlResponse,
)
from devrelay.application.ports.worker_journal import (
    WorkerStateBusy,
    WorkerStateUnavailable,
)
from devrelay.application.worker_claims import PrepareWorkerClaim, RequestWorkerClaim
from tests.acknowledgement_fixtures import acknowledgement_intent
from tests.unit.test_worker_acknowledgement import Journal
from tests.unit.test_worker_assignment_fetch import Remote, prepare


class Local(Journal):
    def __init__(self, state: WorkerRecoveryState, calls: list[str]) -> None:
        super().__init__(state, calls)
        self.intent_failure = False
        self.completion_failure = False
        self.cancel_after_commit = False

    async def get_or_create_heartbeat(
        self, request: WorkerClaimRequest, candidate: WorkerHeartbeatIntent
    ) -> WorkerHeartbeatIntent:
        self.calls.append("heartbeat_intent")
        if self.intent_failure:
            raise WorkerStateUnavailable("Simulated input write failure.")
        if self.state.pending_heartbeat is not None:
            self.state.pending_heartbeat.require_same_input(candidate)
            return self.state.pending_heartbeat
        self.state = replace(self.state, pending_heartbeat=candidate)
        return candidate

    async def record_heartbeat(
        self, request: WorkerClaimRequest, intent: WorkerHeartbeatIntent
    ) -> WorkerRecoveryState:
        self.calls.append("heartbeat_complete")
        if self.completion_failure:
            raise WorkerStateUnavailable("Simulated completion write failure.")
        self.state = replace(self.state, pending_heartbeat=None, last_heartbeat=intent)
        if self.cancel_after_commit:
            raise asyncio.CancelledError()
        return self.state


class Endpoint(Remote):
    def __init__(
        self,
        authorization: WorkerAuthorization,
        local: Local,
        original: Remote,
        calls: list[str],
    ) -> None:
        super().__init__(authorization, original.delivery, calls)
        self.local = local
        self.heartbeats: list[HeartbeatRequest] = []
        self.seen: set[object] = set()
        self.revision = 1
        self.stop = False
        self.mutate: Callable[[HeartbeatResult], HeartbeatResult] = lambda value: value

    async def heartbeat(
        self,
        request: HeartbeatRequest,
        authorization: WorkerAuthorization,
        *,
        reference: AssignmentReference,
    ) -> HeartbeatResult:
        self.calls.append("heartbeat_http")
        self.heartbeats.append(request)
        self.started.set()
        if self.release is not None:
            await self.release.wait()
        if self.error is not None:
            raise self.error
        replay = request.heartbeat_request_id in self.seen
        if not replay and not self.stop:
            self.revision += 1
        self.seen.add(request.heartbeat_request_id)
        updated = reference.issued_at + timedelta(seconds=self.revision)
        expiry = reference.expires_at + timedelta(seconds=self.revision)
        return self.mutate(
            HeartbeatResult(
                heartbeat_request_id=request.heartbeat_request_id,
                authority=LeaseAuthorityObservation(
                    reference=reference,
                    lease_revision=self.revision,
                    updated_at=updated,
                    observed_at=expiry if self.stop else updated,
                    current_expires_at=expiry,
                ),
                renewed=not replay and not self.stop,
                stop_required=self.stop,
            )
        )


def context() -> tuple[RequestWorkerClaim, Local, Endpoint, list[str]]:
    _, original, remote, delivery, calls = prepare()
    intent = acknowledgement_intent(delivery)
    reference = delivery.outcome.assignment
    assert reference is not None
    local = Local(
        replace(
            original.state,
            phase=WorkerRecoveryPhase.ACKNOWLEDGED,
            acknowledgement=intent,
            completion=AcknowledgementCompletion(
                intent=intent,
                acknowledged_at=reference.issued_at + timedelta(seconds=1),
            ),
        ),
        calls,
    )
    endpoint = Endpoint(remote.authorization, local, remote, calls)
    return client(local, endpoint), local, endpoint, calls


def client(local: Local, endpoint: Endpoint) -> RequestWorkerClaim:
    readings = iter(range(100, 200))

    def clock() -> float:
        local.calls.append("clock")
        return float(next(readings))

    return RequestWorkerClaim(
        PrepareWorkerClaim(local, heartbeat_journal=local),
        endpoint,
        endpoint.authorization,
        heartbeat_gateway=endpoint,
        monotonic_clock=clock,
    )


def test_durable_input_and_completion_bracket_http_and_process_local_timing() -> None:
    worker, local, endpoint, calls = context()
    original = local.state

    exchange = asyncio.run(worker.heartbeat())

    assert calls == [
        "heartbeat_intent",
        "clock",
        "heartbeat_http",
        "heartbeat_complete",
        "clock",
    ]
    assert exchange.request_started_at == 100 and exchange.received_at == 101
    assert local.state.last_heartbeat == exchange.intent
    assert local.state.pending_heartbeat is None
    assert local.state.assignment is original.assignment
    assert local.state.request is original.request
    assert local.state.completion is original.completion
    assert local.state.phase is WorkerRecoveryPhase.ACKNOWLEDGED
    assert endpoint.heartbeats[0].claim is original.request


def test_next_explicit_renewal_uses_fresh_revision_with_new_uuid() -> None:
    worker, local, endpoint, calls = context()
    first = asyncio.run(worker.heartbeat())
    calls.clear()

    second = asyncio.run(worker.heartbeat())

    assert second.intent.heartbeat_request_id != first.intent.heartbeat_request_id
    assert (
        second.intent.expected_lease_revision == first.result.authority.lease_revision
    )
    assert second.result.authority.lease_revision == 3
    assert local.state.last_heartbeat == second.intent


@pytest.mark.parametrize("renew", [True, False])
def test_restart_replays_completed_input_before_any_new_identity(renew: bool) -> None:
    worker, local, endpoint, calls = context()
    first = asyncio.run(worker.heartbeat())
    restarted = client(local, endpoint)
    calls.clear()

    recovered = asyncio.run(restarted.heartbeat(renew=renew))

    assert recovered.intent == first.intent
    assert not recovered.result.renewed
    assert recovered.result.authority.lease_revision == 2
    assert "heartbeat_intent" not in calls


def test_observation_only_without_prior_input_fails_before_http_or_local_write() -> (
    None
):
    worker, local, endpoint, calls = context()

    with pytest.raises(AssignmentUnavailable):
        asyncio.run(worker.heartbeat(renew=False))

    assert calls == [] and local.state.last_heartbeat is None


@pytest.mark.parametrize("failure", ["input", "network", "completion", "cancel"])
def test_failure_preserves_original_input_and_never_reports_completion(
    failure: str,
) -> None:
    worker, local, endpoint, calls = context()
    local.intent_failure = failure == "input"
    local.completion_failure = failure == "completion"
    endpoint.error = (
        ControlUnavailable("Response lost.")
        if failure == "network"
        else asyncio.CancelledError()
        if failure == "cancel"
        else None
    )

    with pytest.raises(
        (WorkerStateUnavailable, ControlUnavailable, asyncio.CancelledError)
    ):
        asyncio.run(worker.heartbeat())

    assert local.state.last_heartbeat is None
    assert (local.state.pending_heartbeat is None) == (failure == "input")
    assert ("heartbeat_http" not in calls) == (failure == "input")


@pytest.mark.parametrize("failure", ["network", "completion", "cancel"])
def test_ambiguous_input_recovers_same_uuid_and_proof_after_restart(
    failure: str,
) -> None:
    worker, local, endpoint, calls = context()
    local.completion_failure = failure == "completion"
    endpoint.error = (
        ControlUnavailable("Response lost.")
        if failure == "network"
        else asyncio.CancelledError()
        if failure == "cancel"
        else None
    )
    with pytest.raises(
        (WorkerStateUnavailable, ControlUnavailable, asyncio.CancelledError)
    ):
        asyncio.run(worker.heartbeat())
    pending = local.state.pending_heartbeat
    endpoint.error = None
    local.completion_failure = False
    restarted = client(local, endpoint)
    calls.clear()

    recovered = asyncio.run(restarted.heartbeat())

    assert recovered.intent == pending and local.state.last_heartbeat == pending
    assert endpoint.heartbeats[-1].claim is endpoint.heartbeats[0].claim
    assert "heartbeat_intent" not in calls


def test_cancelled_completion_commit_invalidates_older_in_memory_revision() -> None:
    worker, local, endpoint, calls = context()
    asyncio.run(worker.heartbeat())
    local.cancel_after_commit = True
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(worker.heartbeat())
    committed = local.state.last_heartbeat
    local.cancel_after_commit = False
    calls.clear()

    recovered = asyncio.run(worker.heartbeat())

    assert recovered.intent == committed and not recovered.result.renewed
    assert recovered.result.authority.lease_revision == 3
    assert "heartbeat_intent" not in calls


@pytest.mark.parametrize(
    "field", ["job_id", "issued_at", "expires_at", "heartbeat_request_id"]
)
def test_injected_gateway_cannot_change_original_pins_or_complete_journal(
    field: str,
) -> None:
    worker, local, endpoint, calls = context()

    def mutate(result: HeartbeatResult) -> HeartbeatResult:
        if field == "heartbeat_request_id":
            return replace(result, heartbeat_request_id=uuid4())
        reference = result.authority.reference
        if field == "job_id":
            changed = replace(reference, job_id=uuid4())
        elif field == "issued_at":
            changed = replace(
                reference, issued_at=reference.issued_at + timedelta(microseconds=1)
            )
        else:
            changed = replace(
                reference, expires_at=reference.expires_at + timedelta(microseconds=1)
            )
        return replace(result, authority=replace(result.authority, reference=changed))

    endpoint.mutate = mutate

    with pytest.raises(InvalidControlResponse):
        asyncio.run(worker.heartbeat())

    assert local.state.pending_heartbeat is not None
    assert local.state.last_heartbeat is None and "heartbeat_complete" not in calls


@pytest.mark.parametrize(
    "phase", [WorkerRecoveryPhase.ASSIGNED, WorkerRecoveryPhase.ACKNOWLEDGEMENT_PENDING]
)
def test_only_durable_acknowledged_phase_can_heartbeat(
    phase: WorkerRecoveryPhase,
) -> None:
    worker, local, endpoint, calls = context()
    local.state = replace(
        local.state,
        phase=phase,
        completion=None,
        acknowledgement=local.state.acknowledgement
        if phase is WorkerRecoveryPhase.ACKNOWLEDGEMENT_PENDING
        else None,
    )

    with pytest.raises(AssignmentUnavailable):
        asyncio.run(worker.heartbeat())

    assert calls == [] and endpoint.heartbeats == []


def test_observed_stop_blocks_new_identity_but_allows_exact_observation() -> None:
    worker, local, endpoint, calls = context()
    first = asyncio.run(worker.heartbeat())
    endpoint.stop = True
    asyncio.run(worker.heartbeat(renew=False))
    calls.clear()

    with pytest.raises(ControlClaimRejected):
        asyncio.run(worker.heartbeat())

    assert calls == [] and local.state.last_heartbeat == first.intent


@pytest.mark.parametrize(
    "operation", ["heartbeat", "claim", "fetch", "ack", "prepare_ack"]
)
def test_all_worker_exchanges_share_one_busy_boundary(operation: str) -> None:
    worker, local, endpoint, calls = context()

    async def scenario() -> None:
        endpoint.release = asyncio.Event()
        active = asyncio.create_task(worker.heartbeat())
        await endpoint.started.wait()
        try:
            with pytest.raises(WorkerStateBusy):
                if operation == "heartbeat":
                    await worker.heartbeat()
                elif operation == "claim":
                    await worker.execute()
                elif operation == "fetch":
                    await worker.fetch_assignment()
                elif operation == "ack":
                    await worker.acknowledge()
                else:
                    await worker.prepare_acknowledgement()
        finally:
            endpoint.release.set()
            await active

    asyncio.run(scenario())

    assert len(endpoint.heartbeats) == 1


@pytest.mark.parametrize("field", ["request_started_at", "received_at"])
@pytest.mark.parametrize(
    "value", [True, -1, float("nan"), float("inf"), 2**53, 10**1000]
)
def test_exchange_timing_rejects_untrusted_nonfinite_or_unbounded_readings(
    field: str, value: float
) -> None:
    worker, local, endpoint, calls = context()
    exchange = asyncio.run(worker.heartbeat())

    with pytest.raises(ValueError):
        if field == "request_started_at":
            replace(exchange, request_started_at=value)
        else:
            replace(exchange, received_at=value)


def test_exchange_timing_rejects_receipt_before_start() -> None:
    worker, local, endpoint, calls = context()
    exchange = asyncio.run(worker.heartbeat())

    with pytest.raises(ValueError):
        replace(exchange, received_at=exchange.request_started_at - 1)
