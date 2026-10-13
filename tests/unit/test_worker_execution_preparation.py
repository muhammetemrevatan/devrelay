import asyncio
import threading
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import Any, cast
from uuid import UUID

import pytest

import devrelay.application.worker_executions as execution_preparation
from devrelay.application.contracts.assignment_delivery import (
    AssignedJobDefinition,
    AssignmentDelivery,
    AssignmentUnavailable,
)
from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    WorkerClaimRequest,
)
from devrelay.application.contracts.executions import (
    ExecutionIsolation,
    ExecutionPolicy,
    ExecutionRuntime,
    WorkerExecutionIntent,
)
from devrelay.application.contracts.worker_recovery import (
    AcknowledgementCompletion,
    AssignmentReference,
    WorkerRecoveryPhase,
    WorkerRecoveryState,
)
from devrelay.application.lease_deadlines import LeaseDeadlineRejected
from devrelay.application.ports.worker_journal import (
    WorkerStateBusy,
    WorkerStateUnavailable,
)
from devrelay.application.worker_claims import PrepareWorkerClaim, RequestWorkerClaim
from devrelay.application.worker_executions import PrepareAssignedExecution
from tests.acknowledgement_fixtures import (
    acknowledgement_inputs,
    acknowledgement_intent,
)
from tests.unit.test_worker_heartbeat_exchange import context as heartbeat_context


def policy() -> ExecutionPolicy:
    return ExecutionPolicy(
        runtime=ExecutionRuntime.CODEX,
        isolation=ExecutionIsolation.WINDOWS_RESTRICTED,
        policy_digest="a" * 64,
    )


class Guard:
    def __init__(self, reference: AssignmentReference, calls: list[str]) -> None:
        self.reference = reference
        self.calls = calls
        self.stopped = False

    def require_current_assignment(self, reference: AssignmentReference) -> None:
        self.calls.append("authority")
        if self.stopped or reference != self.reference:
            raise LeaseDeadlineRejected("Current assignment authority unavailable.")


class Journal:
    def __init__(self, state: WorkerRecoveryState, calls: list[str]) -> None:
        self.state = state
        self.calls = calls
        self.received: tuple[WorkerClaimRequest, WorkerExecutionIntent] | None = None
        self.after_commit: Callable[[], None] = lambda: None
        self.error: Exception | None = None
        self.cancel_after_commit = False
        self.returned: Any = None

    async def load_state(self) -> WorkerRecoveryState:
        self.calls.append("recover")
        return self.state

    async def get_or_create_execution(
        self, request: WorkerClaimRequest, candidate: WorkerExecutionIntent
    ) -> WorkerExecutionIntent:
        self.calls.append("commit")
        self.received = request, candidate
        if self.error is not None:
            raise self.error
        if self.state.execution is None:
            self.state = replace(self.state, execution=candidate)
        else:
            self.state.execution.require_same_input(candidate)
        self.after_commit()
        if self.cancel_after_commit:
            raise asyncio.CancelledError
        if self.returned is not None:
            return cast(WorkerExecutionIntent, self.returned)
        assert self.state.execution is not None
        return self.state.execution


@dataclass
class Context:
    service: PrepareAssignedExecution
    journal: Journal
    guard: Guard
    delivery: AssignmentDelivery
    calls: list[str]


def context() -> Context:
    request, delivery = acknowledgement_inputs()
    acknowledgement = acknowledgement_intent(delivery)
    reference = delivery.outcome.assignment
    assert reference is not None
    state = WorkerRecoveryState(
        phase=WorkerRecoveryPhase.ACKNOWLEDGED,
        request=request,
        assignment=delivery.outcome,
        acknowledgement=acknowledgement,
        completion=AcknowledgementCompletion(
            intent=acknowledgement,
            acknowledged_at=reference.issued_at + timedelta(seconds=1),
        ),
    )
    calls: list[str] = []
    journal, guard = Journal(state, calls), Guard(reference, calls)
    return Context(
        PrepareAssignedExecution(journal, guard, policy()),
        journal,
        guard,
        delivery,
        calls,
    )


def test_new_execution_pins_original_input_between_fresh_authority_checks() -> None:
    setup = context()
    original = setup.journal.state

    intent = asyncio.run(setup.service.execute(setup.delivery))

    assert setup.calls == ["recover", "authority", "commit", "authority"]
    assert setup.journal.state.execution is intent
    assert intent.acknowledgement == original.acknowledgement
    assert intent.policy == policy()
    assert setup.journal.received is not None
    assert setup.journal.received[0] is original.request
    assert setup.journal.state.completion == original.completion
    assert setup.journal.state.assignment == original.assignment
    assert original.request is not None
    assert original.request.proof.reveal() not in repr(intent)


def test_existing_preparation_recovers_under_stopped_authority_without_new_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup = context()
    acknowledgement = setup.journal.state.acknowledgement
    assert acknowledgement is not None
    existing = WorkerExecutionIntent(
        execution_id=UUID(int=99), acknowledgement=acknowledgement, policy=policy()
    )
    setup.journal.state = replace(setup.journal.state, execution=existing)
    setup.guard.stopped = True

    def forbidden_identity() -> UUID:
        raise AssertionError("Historical recovery must not generate an identity.")

    monkeypatch.setattr(execution_preparation, "uuid4", forbidden_identity)

    recovered = asyncio.run(setup.service.execute(setup.delivery))

    assert recovered is existing
    assert setup.calls == ["recover"]


def test_stopped_authority_rejects_new_preparation_without_a_durable_write() -> None:
    setup = context()
    original = setup.journal.state
    setup.guard.stopped = True

    with pytest.raises(LeaseDeadlineRejected):
        asyncio.run(setup.service.execute(setup.delivery))

    assert setup.calls == ["recover", "authority"]
    assert setup.journal.state is original


def test_post_commit_authority_loss_preserves_historical_execution_identity() -> None:
    setup = context()
    setup.journal.after_commit = lambda: setattr(setup.guard, "stopped", True)

    with pytest.raises(LeaseDeadlineRejected):
        asyncio.run(setup.service.execute(setup.delivery))

    assert setup.calls == ["recover", "authority", "commit", "authority"]
    assert setup.journal.state.execution is not None
    assert setup.journal.received is not None
    assert setup.journal.state.execution is setup.journal.received[1]


def test_cancelled_commit_retains_original_preparation_for_later_recovery() -> None:
    setup = context()
    setup.journal.cancel_after_commit = True

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(setup.service.execute(setup.delivery))

    assert setup.journal.state.execution is not None
    assert setup.calls == ["recover", "authority", "commit"]


def test_failed_commit_does_not_create_preparation() -> None:
    setup = context()
    original = setup.journal.state
    setup.journal.error = WorkerStateUnavailable("Cannot commit preparation.")

    with pytest.raises(WorkerStateUnavailable):
        asyncio.run(setup.service.execute(setup.delivery))

    assert setup.journal.state is original
    assert setup.calls == ["recover", "authority", "commit"]


def test_changed_execution_policy_cannot_replace_existing_preparation() -> None:
    setup = context()
    acknowledgement = setup.journal.state.acknowledgement
    assert acknowledgement is not None
    existing = WorkerExecutionIntent(
        execution_id=UUID(int=99),
        acknowledgement=acknowledgement,
        policy=replace(policy(), policy_digest="b" * 64),
    )
    setup.journal.state = replace(setup.journal.state, execution=existing)

    with pytest.raises(ClaimReplayConflict):
        asyncio.run(setup.service.execute(setup.delivery))

    assert setup.journal.state.execution is existing
    assert setup.calls == ["recover"]


@pytest.mark.parametrize(
    "field",
    [
        "snapshot",
        "base_revision",
        "checks",
        "provider_repository_id",
        "authorized_branch",
        "target_branch",
        "policy_version",
        "max_repair_cycles",
    ],
)
def test_complete_definition_changes_cannot_prepare_execution(field: str) -> None:
    setup = context()
    definition = setup.delivery.definition
    changes: dict[str, Any] = {
        "snapshot": replace(definition.snapshot, id=UUID(int=99)),
        "base_revision": "b" * 40,
        "checks": (
            replace(
                definition.checks[0],
                timeout_seconds=definition.checks[0].timeout_seconds + 1,
            ),
            *definition.checks[1:],
        ),
        "provider_repository_id": definition.provider_repository_id + 1,
        "authorized_branch": "task/changed",
        "target_branch": "changed-base",
        "policy_version": "changed-policy",
        "max_repair_cycles": definition.max_repair_cycles + 1,
    }
    delivery = replace(
        setup.delivery, definition=replace(definition, **{field: changes[field]})
    )

    with pytest.raises(ClaimReplayConflict):
        asyncio.run(setup.service.execute(delivery))

    assert setup.calls == ["recover"]
    assert setup.journal.state.execution is None


def test_delivery_cannot_change_original_lease_anchors() -> None:
    setup = context()
    reference = setup.delivery.outcome.assignment
    assert reference is not None
    changed_reference = replace(
        reference, expires_at=reference.expires_at + timedelta(seconds=1)
    )
    delivery = replace(
        setup.delivery,
        outcome=replace(setup.delivery.outcome, assignment=changed_reference),
    )

    with pytest.raises(ClaimReplayConflict):
        asyncio.run(setup.service.execute(delivery))

    assert setup.calls == ["recover"]


def test_completed_durable_acknowledgement_is_required() -> None:
    setup = context()
    setup.journal.state = replace(
        setup.journal.state,
        phase=WorkerRecoveryPhase.ACKNOWLEDGEMENT_PENDING,
        completion=None,
    )

    with pytest.raises(AssignmentUnavailable):
        asyncio.run(setup.service.execute(setup.delivery))

    assert setup.calls == ["recover"]


def test_journal_result_with_changed_policy_fails_closed_after_commit() -> None:
    setup = context()
    acknowledgement = setup.journal.state.acknowledgement
    assert acknowledgement is not None
    setup.journal.returned = WorkerExecutionIntent(
        execution_id=UUID(int=99),
        acknowledgement=acknowledgement,
        policy=replace(policy(), policy_digest="b" * 64),
    )

    with pytest.raises(ClaimReplayConflict):
        asyncio.run(setup.service.execute(setup.delivery))

    assert setup.journal.state.execution is not None
    assert setup.calls == ["recover", "authority", "commit"]


def test_malformed_journal_result_fails_closed_after_commit() -> None:
    setup = context()
    setup.journal.returned = "untrusted-result"

    with pytest.raises(ClaimReplayConflict):
        asyncio.run(setup.service.execute(setup.delivery))

    assert setup.journal.state.execution is not None
    assert setup.calls == ["recover", "authority", "commit"]


def test_digest_work_is_off_loop_and_stop_during_it_prevents_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup = context()
    original_fingerprint = AssignedJobDefinition.fingerprint
    entered, release = threading.Event(), threading.Event()
    threads: list[int] = []

    def blocked_digest(definition: AssignedJobDefinition) -> str:
        threads.append(threading.get_ident())
        entered.set()
        assert release.wait(timeout=5)
        return original_fingerprint(definition)

    monkeypatch.setattr(AssignedJobDefinition, "fingerprint", blocked_digest)

    async def scenario() -> None:
        pending = asyncio.create_task(setup.service.execute(setup.delivery))
        await asyncio.to_thread(entered.wait, 5)
        setup.guard.stopped = True
        release.set()
        with pytest.raises(LeaseDeadlineRejected):
            await pending

    asyncio.run(scenario())

    assert threads and threads[0] != threading.get_ident()
    assert setup.calls == ["recover", "authority"]
    assert setup.journal.state.execution is None


def test_composed_client_requires_explicit_execution_journal_without_http() -> None:
    worker, local, endpoint, calls = heartbeat_context()
    original = local.state

    with pytest.raises(WorkerStateUnavailable, match="not configured"):
        asyncio.run(worker.prepare_execution(endpoint.delivery, policy()))

    assert calls == []
    assert local.state is original
    assert endpoint.heartbeats == []


def test_composed_client_recovers_historical_execution_through_stopped_lineage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, local, endpoint, calls = heartbeat_context()
    acknowledgement = local.state.acknowledgement
    assert acknowledgement is not None
    existing = WorkerExecutionIntent(
        execution_id=UUID(int=99), acknowledgement=acknowledgement, policy=policy()
    )
    execution_journal = Journal(replace(local.state, execution=existing), calls)
    worker = RequestWorkerClaim(
        PrepareWorkerClaim(
            local, heartbeat_journal=local, execution_journal=execution_journal
        ),
        endpoint,
        endpoint.authorization,
        heartbeat_gateway=endpoint,
        monotonic_clock=lambda: 100,
    )
    worker.heartbeat_supervisor().request_stop()

    def forbidden_identity() -> UUID:
        raise AssertionError("Historical recovery must not invent an identity.")

    monkeypatch.setattr(execution_preparation, "uuid4", forbidden_identity)

    recovered = asyncio.run(worker.prepare_execution(endpoint.delivery, policy()))

    assert recovered is existing
    assert calls == ["recover"]
    assert endpoint.heartbeats == []
    assert worker.heartbeat_supervisor().stop_required


def test_composed_preparation_shares_inflight_worker_exchange_lock() -> None:
    async def scenario() -> tuple[Journal, list[str]]:
        _, local, endpoint, calls = heartbeat_context()
        execution_journal = Journal(local.state, calls)
        worker = RequestWorkerClaim(
            PrepareWorkerClaim(
                local, heartbeat_journal=local, execution_journal=execution_journal
            ),
            endpoint,
            endpoint.authorization,
            heartbeat_gateway=endpoint,
            monotonic_clock=lambda: 100,
        )
        endpoint.release = asyncio.Event()
        heartbeat = asyncio.create_task(worker.heartbeat())
        await endpoint.started.wait()

        with pytest.raises(WorkerStateBusy):
            await worker.prepare_execution(endpoint.delivery, policy())

        endpoint.release.set()
        await heartbeat
        return execution_journal, calls

    execution_journal, calls = asyncio.run(scenario())

    assert execution_journal.state.execution is None
    assert "recover" not in calls
    assert calls.count("heartbeat_http") == 1
