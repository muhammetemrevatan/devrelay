import asyncio
import threading
from dataclasses import replace
from uuid import UUID, uuid4

import pytest

from devrelay.application.contracts.acknowledgements import WorkerAcknowledgementIntent
from devrelay.application.contracts.assignment_delivery import (
    AssignedJobDefinition,
    AssignmentDelivery,
    AssignmentUnavailable,
)
from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    WorkerClaimRequest,
)
from devrelay.application.contracts.worker_recovery import (
    WorkerRecoveryPhase,
    WorkerRecoveryState,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.worker_journal import (
    WorkerStateBusy,
    WorkerStateUnavailable,
)
from devrelay.application.worker_claims import PrepareWorkerClaim, RequestWorkerClaim
from tests.acknowledgement_fixtures import acknowledgement_intent
from tests.unit.test_worker_assignment_fetch import Local, Remote, prepare


class Journal(Local):
    def __init__(self, state: WorkerRecoveryState, calls: list[str]) -> None:
        super().__init__(state, calls)
        self.intent: WorkerAcknowledgementIntent | None = None
        self.failure = False

    async def get_or_create_acknowledgement(
        self, request: WorkerClaimRequest, candidate: WorkerAcknowledgementIntent
    ) -> WorkerAcknowledgementIntent:
        self.calls.append("intent")
        if self.failure:
            raise WorkerStateUnavailable("Cannot commit.")
        if self.intent is None:
            self.intent = candidate
        else:
            self.intent.require_same_input(candidate)
        self.state = replace(
            self.state,
            phase=WorkerRecoveryPhase.ACKNOWLEDGEMENT_PENDING,
            acknowledgement=self.intent,
        )
        return self.intent


def context() -> tuple[RequestWorkerClaim, Journal, Remote, list[str]]:
    _, original, remote, delivery, calls = prepare()
    local = Journal(original.state, calls)
    return (
        RequestWorkerClaim(PrepareWorkerClaim(local), remote, remote.authorization),
        local,
        remote,
        calls,
    )


def test_preparation_fetches_and_durably_pins_definition_before_returning() -> None:
    worker, local, remote, calls = context()
    original = local.state
    assert (
        original.assignment is not None and original.assignment.assignment is not None
    )
    assert original.request is not None

    intent = asyncio.run(worker.prepare_acknowledgement())

    assert calls == ["read", "intent"] and local.state.acknowledgement is intent
    assert intent.snapshot_id == remote.delivery.definition.snapshot.id
    assert intent.base_revision == remote.delivery.definition.base_revision
    assert intent.definition_digest == remote.delivery.definition.fingerprint()
    assert intent.attempt_id == original.assignment.assignment.attempt_id
    assert local.state.request is original.request
    assert local.state.assignment is original.assignment
    assert local.state.phase is WorkerRecoveryPhase.ACKNOWLEDGEMENT_PENDING
    assert original.request.proof.reveal() not in repr(intent)


def test_recovered_intent_returns_without_http_or_regenerating_identity() -> None:
    worker, local, remote, calls = context()
    recorded = acknowledgement_intent(remote.delivery)
    local.state = replace(
        local.state,
        phase=WorkerRecoveryPhase.ACKNOWLEDGEMENT_PENDING,
        acknowledgement=recorded,
    )
    remote.error = ValueError("HTTP must not be called.")

    recovered = asyncio.run(worker.prepare_acknowledgement())

    assert recovered is recorded and calls == []


def test_failed_local_commit_preserves_assigned_state() -> None:
    worker, local, remote, calls = context()
    original = local.state
    local.failure = True

    with pytest.raises(WorkerStateUnavailable):
        asyncio.run(worker.prepare_acknowledgement())

    assert calls == ["read", "intent"] and local.state is original


def test_inactive_delivery_cannot_prepare_new_acknowledgement() -> None:
    worker, local, remote, calls = context()
    original = local.state
    remote.delivery = replace(remote.delivery, lease_authority_active=False)

    with pytest.raises(AssignmentUnavailable):
        asyncio.run(worker.prepare_acknowledgement())

    assert calls == ["read"] and local.state is original


@pytest.mark.parametrize(
    "phase", [WorkerRecoveryPhase.IDLE, WorkerRecoveryPhase.PENDING]
)
def test_unassigned_worker_never_fetches_or_prepares_intent(
    phase: WorkerRecoveryPhase,
) -> None:
    worker, local, remote, calls = context()
    request = local.state.request if phase is WorkerRecoveryPhase.PENDING else None
    local.state = WorkerRecoveryState(phase=phase, request=request)

    with pytest.raises(AssignmentUnavailable):
        asyncio.run(worker.prepare_acknowledgement())

    assert calls == [] and local.state.acknowledgement is None


@pytest.mark.parametrize("changed", ["snapshot", "base", "checks"])
def test_later_fetch_rejects_changed_pinned_acknowledgement(changed: str) -> None:
    worker, local, remote, calls = context()
    recorded = acknowledgement_intent(remote.delivery)
    local.state = replace(
        local.state,
        phase=WorkerRecoveryPhase.ACKNOWLEDGEMENT_PENDING,
        acknowledgement=recorded,
    )
    definition = remote.delivery.definition
    if changed == "snapshot":
        definition = replace(
            definition, snapshot=replace(definition.snapshot, id=uuid4())
        )
    elif changed == "base":
        definition = replace(definition, base_revision="b" * 40)
    else:
        definition = replace(
            definition, checks=(replace(definition.checks[0], argv=("other",)),)
        )
    remote.delivery = replace(remote.delivery, definition=definition)
    original = local.state

    with pytest.raises(ClaimReplayConflict):
        asyncio.run(worker.fetch_assignment())

    assert calls == ["read"] and local.state is original


def test_pinned_fetch_remains_recoverable_when_authority_is_inactive() -> None:
    worker, local, remote, calls = context()
    recorded = acknowledgement_intent(remote.delivery)
    local.state = replace(
        local.state,
        phase=WorkerRecoveryPhase.ACKNOWLEDGEMENT_PENDING,
        acknowledgement=recorded,
    )
    remote.delivery = replace(remote.delivery, lease_authority_active=False)
    original = local.state

    delivery = asyncio.run(worker.fetch_assignment())

    assert not delivery.lease_authority_active and calls == ["read"]
    assert local.state is original and local.state.acknowledgement is recorded


def test_digest_work_runs_off_the_event_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    worker, local, remote, calls = context()
    fingerprint = AssignedJobDefinition.fingerprint
    owner = threading.get_ident()
    threads: list[int] = []

    def calculate(definition: AssignedJobDefinition) -> str:
        threads.append(threading.get_ident())
        return fingerprint(definition)

    monkeypatch.setattr(AssignedJobDefinition, "fingerprint", calculate)

    asyncio.run(worker.prepare_acknowledgement())

    assert threads and all(identity != owner for identity in threads)


def test_acknowledgement_preparation_rejects_concurrent_exchanges(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker, local, remote, calls = context()
    original = local.state

    async def exercise() -> None:
        started, release = asyncio.Event(), asyncio.Event()
        reader = remote.read_assignment

        async def blocked(
            attempt_id: UUID,
            request: WorkerClaimRequest,
            authorization: WorkerAuthorization,
        ) -> AssignmentDelivery:
            started.set()
            await release.wait()
            return await reader(attempt_id, request, authorization)

        monkeypatch.setattr(remote, "read_assignment", blocked)
        active = asyncio.create_task(worker.prepare_acknowledgement())
        await started.wait()
        try:
            for operation in (
                worker.execute,
                worker.fetch_assignment,
                worker.prepare_acknowledgement,
            ):
                with pytest.raises(WorkerStateBusy):
                    await operation()
        finally:
            active.cancel()
            with pytest.raises(asyncio.CancelledError):
                await active

    asyncio.run(exercise())

    assert calls == [] and local.state is original
