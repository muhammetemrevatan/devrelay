import asyncio
from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

import pytest

from devrelay.application.contracts.acknowledgements import (
    AcknowledgementResult,
    WorkerAcknowledgementRequest,
)
from devrelay.application.contracts.claims import WorkerClaimRequest
from devrelay.application.contracts.worker_recovery import (
    AcknowledgementCompletion,
    WorkerRecoveryPhase,
    WorkerRecoveryState,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.control_gateway import (
    ControlUnavailable,
    InvalidControlResponse,
)
from devrelay.application.ports.worker_journal import (
    WorkerStateBusy,
    WorkerStateUnavailable,
)
from devrelay.application.worker_claims import PrepareWorkerClaim, RequestWorkerClaim
from tests.unit.test_worker_acknowledgement import Journal
from tests.unit.test_worker_assignment_fetch import Remote, prepare


class Local(Journal):
    def __init__(self, state: WorkerRecoveryState, calls: list[str]) -> None:
        super().__init__(state, calls)
        self.completion_failure = False

    async def record_acknowledgement(
        self, request: WorkerClaimRequest, completion: AcknowledgementCompletion
    ) -> WorkerRecoveryState:
        self.calls.append("complete_ack")
        if self.completion_failure:
            raise WorkerStateUnavailable("Simulated local write failure.")
        self.state = replace(
            self.state, phase=WorkerRecoveryPhase.ACKNOWLEDGED, completion=completion
        )
        return self.state


class Endpoint(Remote):
    async def acknowledge(
        self, request: WorkerAcknowledgementRequest, authorization: WorkerAuthorization
    ) -> AcknowledgementResult:
        self.calls.append("ack_http")
        self.started.set()
        if self.release is not None:
            await self.release.wait()
        if self.error is not None:
            raise self.error
        reference = self.delivery.outcome.assignment
        assert reference is not None
        return AcknowledgementResult(
            worker_id=reference.worker_id,
            job_id=reference.job_id,
            repository_id=reference.repository_id,
            intent=request.intent,
            acknowledged_at=reference.issued_at + timedelta(seconds=1),
            expires_at=reference.expires_at,
            recovered=False,
            lease_authority_active=True,
        )


def context() -> tuple[RequestWorkerClaim, Local, Endpoint, list[str]]:
    _, original, remote, delivery, calls = prepare()
    local = Local(original.state, calls)
    endpoint = Endpoint(remote.authorization, delivery, calls)
    return (
        RequestWorkerClaim(PrepareWorkerClaim(local), endpoint, remote.authorization),
        local,
        endpoint,
        calls,
    )


def test_intent_precedes_http_and_completion_precedes_return_without_repinning() -> (
    None
):
    worker, local, endpoint, calls = context()
    original = local.state
    result = asyncio.run(worker.acknowledge())
    assert calls == ["read", "intent", "ack_http", "complete_ack"]
    assert local.state.phase is WorkerRecoveryPhase.ACKNOWLEDGED
    assert (
        local.state.assignment is original.assignment
        and local.state.request is original.request
    )
    assert local.state.acknowledgement == result.intent


def test_completed_acknowledgement_replays_without_fetching_or_repinning() -> None:
    worker, local, endpoint, calls = context()
    original = asyncio.run(worker.acknowledge())
    calls.clear()

    repeated = asyncio.run(worker.acknowledge())

    assert calls == ["ack_http", "complete_ack"] and repeated.intent == original.intent


@pytest.mark.parametrize("failure", ["intent", "network", "completion", "cancel"])
def test_ambiguous_failure_retains_input(
    failure: str,
) -> None:
    worker, local, endpoint, calls = context()
    if failure == "intent":
        local.failure = True
    elif failure == "completion":
        local.completion_failure = True
    else:
        asyncio.run(worker.prepare_acknowledgement())
        endpoint.error = (
            asyncio.CancelledError()
            if failure == "cancel"
            else ControlUnavailable("Lost response.")
        )
    with pytest.raises(
        (WorkerStateUnavailable, ControlUnavailable, asyncio.CancelledError)
    ):
        asyncio.run(worker.acknowledge())
    if failure == "intent":
        assert "ack_http" not in calls and local.state.acknowledgement is None
    else:
        intent = local.state.acknowledgement
        assert intent is not None and local.state.completion is None


@pytest.mark.parametrize("failure", ["network", "completion", "cancel"])
def test_ambiguous_exchange_recovers_same_identity_without_another_payload_read(
    failure: str,
) -> None:
    worker, local, endpoint, calls = context()
    intent = asyncio.run(worker.prepare_acknowledgement())
    endpoint.error = (
        ControlUnavailable("Lost response.")
        if failure == "network"
        else asyncio.CancelledError()
        if failure == "cancel"
        else None
    )
    local.completion_failure = failure == "completion"
    with pytest.raises(
        (ControlUnavailable, WorkerStateUnavailable, asyncio.CancelledError)
    ):
        asyncio.run(worker.acknowledge())
    endpoint.error = None
    local.completion_failure = False

    recovered = asyncio.run(worker.acknowledge())

    assert recovered.intent == intent and calls.count("read") == 1
    assert local.state.phase is WorkerRecoveryPhase.ACKNOWLEDGED


@pytest.mark.parametrize("change", ["job", "expiry", "time"])
def test_worker_rejects_changed_original_response_anchors_before_completion(
    change: str,
) -> None:
    worker, local, endpoint, calls = context()

    class Changed(Endpoint):
        async def acknowledge(
            self,
            request: WorkerAcknowledgementRequest,
            authorization: WorkerAuthorization,
        ) -> AcknowledgementResult:
            value = await super().acknowledge(request, authorization)
            return replace(
                value,
                **(
                    {"job_id": uuid4()}
                    if change == "job"
                    else {"expires_at": value.expires_at + timedelta(seconds=1)}
                    if change == "expiry"
                    else {
                        "acknowledged_at": self.delivery.outcome.recorded_at
                        - timedelta(seconds=1)
                    }
                ),
            )

    changed = Changed(endpoint.authorization, endpoint.delivery, calls)
    worker = RequestWorkerClaim(
        PrepareWorkerClaim(local), changed, endpoint.authorization
    )
    with pytest.raises(InvalidControlResponse):
        asyncio.run(worker.acknowledge())
    assert local.state.completion is None and "complete_ack" not in calls


def test_concurrent_exchange_is_rejected_and_cancellation_preserves_pending_intent() -> (
    None
):
    worker, local, endpoint, calls = context()

    async def scenario() -> None:
        endpoint.release = asyncio.Event()
        task = asyncio.create_task(worker.acknowledge())
        await endpoint.started.wait()
        for operation in (
            worker.acknowledge,
            worker.execute,
            worker.fetch_assignment,
            worker.prepare_acknowledgement,
        ):
            with pytest.raises(WorkerStateBusy):
                await operation()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert local.state.phase is WorkerRecoveryPhase.ACKNOWLEDGEMENT_PENDING
        endpoint.release.set()
        await worker.acknowledge()

    asyncio.run(scenario())
