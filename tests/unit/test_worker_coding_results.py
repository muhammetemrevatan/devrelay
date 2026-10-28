"""Result exchange fault ordering; no SQLite, native process or account I/O."""

import asyncio
import threading
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import timedelta
from uuid import UUID, uuid4

import pytest

import devrelay.application.worker_coding_results as exchange_module
from devrelay.application.coding_result_preparation import prepare_coding_result
from devrelay.application.contracts.assignment_delivery import (
    AssignedJobDefinition,
    AssignmentDelivery,
)
from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    WorkerClaimRequest,
)
from devrelay.application.contracts.coding_result_acceptance import PreparedCodingResult
from devrelay.application.contracts.coding_result_receipts import (
    CodingPublicationState,
    CodingResultAcceptance,
    CodingResultReceipt,
    CodingResultStatus,
    CodingResultUnavailable,
    TerminalCodingResult,
)
from devrelay.application.contracts.coding_result_requests import (
    WorkerCodingResultIntent,
    WorkerCodingResultRequest,
)
from devrelay.application.contracts.worker_recovery import AssignmentReference
from devrelay.application.contracts.worker_result_recovery import (
    WorkerCodingResultState,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.contracts.workspace_recovery import (
    WorkerWorkspaceCompletion,
    WorkerWorkspaceIntent,
    WorkspaceDirectoryIdentity,
    WorkspaceLocation,
)
from devrelay.application.worker_coding_results import WorkerCodingResultExchange
from devrelay.domain.jobs.values import PullRequestReference
from tests.git_source_fixtures import git_workspace_inputs
from tests.unit.test_coding_results import context as result_context


class Expired(RuntimeError):
    pass


class Guard:
    def __init__(self, reference: AssignmentReference, events: list[str]) -> None:
        self.reference, self.events = reference, events
        self.active = True

    def require_current_assignment(self, reference: AssignmentReference) -> None:
        assert reference == self.reference
        self.events.append("guard")
        if not self.active:
            raise Expired("Current authority ended.")


class Journal:
    def __init__(self, state: WorkerCodingResultState, events: list[str]) -> None:
        self.state, self.events = state, events
        self.hook: Callable[[str], None] = lambda _: None
        self.accepted = asyncio.Event()

    async def load_result_state(self) -> WorkerCodingResultState:
        self.events.append("load")
        self.hook("load")
        await asyncio.sleep(0)
        return self.state

    async def get_or_create_result(
        self,
        request: WorkerClaimRequest,
        candidate: WorkerCodingResultIntent,
        *,
        target_branch: str,
    ) -> WorkerCodingResultIntent:
        self.events.append("commit")
        assert self.state.original.request is not None
        assert request.proof.reveal() == self.state.original.request.proof.reveal()
        if self.state.intent is not None:
            self.state.intent.require_same_input(candidate)
        else:
            self.state = replace(
                self.state, intent=candidate, target_branch=target_branch
            )
        self.hook("commit")
        assert self.state.intent is not None
        return self.state.intent

    async def record_result_acceptance(
        self, request: WorkerClaimRequest, receipt: CodingResultReceipt
    ) -> WorkerCodingResultState:
        self.events.append("receipt")
        self.state = replace(self.state, receipt=receipt)
        self.hook("receipt")
        self.accepted.set()
        return self.state

    async def record_result_terminal(
        self, request: WorkerClaimRequest, status: CodingResultStatus
    ) -> WorkerCodingResultState:
        self.events.append("terminal")
        self.state = replace(self.state, receipt=status.receipt, terminal_status=status)
        return self.state


def receipt(
    request: WorkerCodingResultRequest,
    authorization: WorkerAuthorization,
    definition: AssignedJobDefinition,
) -> CodingResultReceipt:
    intent = request.intent
    return CodingResultReceipt(
        result_id=UUID(int=999),
        worker_id=authorization.worker_id,
        request_id=intent.result_request_id,
        claim_request_id=request.claim.claim_request_id,
        job_id=definition.job_id,
        attempt_id=intent.execution.acknowledgement.attempt_id,
        repository_id=definition.repository_id,
        execution_id=intent.execution.execution_id,
        definition_digest=definition.fingerprint(),
        request_fingerprint=request.identify(authorization).fingerprint,
        candidate_revision=intent.result.final_candidate.revision,
        repair_cycles=len(intent.result.cycles) - 1,
        publishing_workflow_version=4 + 2 * (len(intent.result.cycles) - 1),
        recorded_at=definition.created_at + timedelta(seconds=5),
    )


class Gateway:
    def __init__(self, definition: AssignedJobDefinition, events: list[str]) -> None:
        self.definition, self.events = definition, events
        self.status: CodingResultStatus | None = None
        self.submitted: list[WorkerCodingResultRequest] = []
        self.recovered: list[WorkerCodingResultRequest] = []
        self.hook: Callable[[str], None] = lambda _: None
        self.change: Callable[[CodingResultReceipt], CodingResultReceipt] = (
            lambda value: value
        )
        self.entered, self.release = asyncio.Event(), asyncio.Event()
        self.block = False
        self.encoding, self.encoded = asyncio.Event(), asyncio.Event()
        self.block_encoding = False

    async def submit(
        self,
        request: WorkerCodingResultRequest,
        authorization: WorkerAuthorization,
        *,
        job_id: UUID,
        target_branch: str,
        before_submit: Callable[[], None] | None = None,
    ) -> CodingResultAcceptance:
        self.encoding.set()
        if self.block_encoding:
            await self.encoded.wait()
        if before_submit is not None:
            before_submit()
        self.events.append("post")
        self.submitted.append(request)
        assert (
            job_id == self.definition.job_id
            and target_branch == self.definition.target_branch
        )
        self.entered.set()
        if self.block:
            await self.release.wait()
        self.hook("response")
        return CodingResultAcceptance(
            receipt=self.change(receipt(request, authorization, self.definition)),
            recovered=False,
        )

    async def recover(
        self,
        request: WorkerCodingResultRequest,
        authorization: WorkerAuthorization,
        *,
        job_id: UUID,
        target_branch: str,
    ) -> CodingResultStatus | None:
        self.events.append("get")
        self.recovered.append(request)
        assert (
            job_id == self.definition.job_id
            and target_branch == self.definition.target_branch
        )
        self.hook("get")
        return self.status


@dataclass
class Setup:
    delivery: AssignmentDelivery
    candidate: WorkerCodingResultIntent
    authorization: WorkerAuthorization
    journal: Journal
    gateway: Gateway
    guard: Guard
    events: list[str]

    def exchange(self) -> WorkerCodingResultExchange:
        return WorkerCodingResultExchange(
            self.journal, self.gateway, self.authorization
        )

    def status(self, *, terminal: bool = False) -> CodingResultStatus:
        original = self.journal.state.original.request
        assert original is not None
        accepted = receipt(
            WorkerCodingResultRequest(
                intent=self.journal.state.intent or self.candidate, claim=original
            ),
            self.authorization,
            self.delivery.definition,
        )
        final = (
            TerminalCodingResult(
                receipt=accepted,
                reference=PullRequestReference(
                    repository_id=accepted.repository_id,
                    number=8,
                    source_branch=self.delivery.definition.authorized_branch,
                    target_branch=self.delivery.definition.target_branch,
                    head_revision=accepted.candidate_revision,
                ),
                finalized_at=accepted.recorded_at + timedelta(seconds=2),
                workflow_version=accepted.publishing_workflow_version + 1,
                release_ordinal=1,
            )
            if terminal
            else None
        )
        return CodingResultStatus(
            receipt=accepted,
            state=CodingPublicationState.FINALIZED
            if terminal
            else CodingPublicationState.ACCEPTED,
            operation_version=4 if terminal else 0,
            terminal=final,
        )


def setup(*, existing: bool = False) -> Setup:
    state, delivery, binding, _ = git_workspace_inputs()
    execution = binding.execution.intent
    report = result_context(
        definition=delivery.definition, execution_id=execution.execution_id
    ).result
    candidate = WorkerCodingResultIntent(
        result_request_id=uuid4(), execution=execution, result=report
    )
    workspace = WorkerWorkspaceIntent(
        workspace_request_id=uuid4(),
        execution=execution,
        location=WorkspaceLocation(
            root_directory="C:\\fixture-root",
            policy_digest="b" * 64,
            identity=WorkspaceDirectoryIdentity(volume_serial_number=1, file_index=2),
        ),
    )
    state = replace(
        state,
        workspace_intent=workspace,
        workspace_completion=WorkerWorkspaceCompletion(
            intent=workspace,
            identity=WorkspaceDirectoryIdentity(volume_serial_number=1, file_index=3),
        ),
    )
    reference = delivery.outcome.assignment
    assert reference is not None
    authorization = WorkerAuthorization(
        worker_id=reference.worker_id,
        allowed_repository_ids=frozenset({reference.repository_id}),
        credential_expires_at=reference.expires_at + timedelta(hours=1),
    )
    events: list[str] = []
    journal = Journal(
        WorkerCodingResultState(
            original=state,
            intent=candidate if existing else None,
            target_branch=delivery.definition.target_branch if existing else None,
        ),
        events,
    )
    return Setup(
        delivery,
        candidate,
        authorization,
        journal,
        Gateway(delivery.definition, events),
        Guard(reference, events),
        events,
    )


def test_submit_commits_before_post_and_never_releases_original_claim() -> None:
    async def scenario() -> None:
        value = setup()
        exchange = value.exchange()
        result = await exchange.submit(
            value.delivery, value.candidate, guard=value.guard
        )
        assert not result.recovered
        assert (
            value.events.index("commit")
            < value.events.index("post")
            < value.events.index("receipt")
        )
        assert value.journal.state.original.request is not None
        assert value.journal.state.receipt == result.receipt
        with pytest.raises(CodingResultUnavailable):
            await exchange.submit(value.delivery, value.candidate, guard=value.guard)
        assert len(value.gateway.submitted) == 1
        await exchange.aclose()

    asyncio.run(scenario())


@pytest.mark.parametrize("terminal", [False, True])
def test_existing_input_recovers_original_uuid_before_any_post(terminal: bool) -> None:
    async def scenario() -> None:
        value = setup(existing=True)
        value.gateway.status = value.status(terminal=terminal)
        original_id = value.candidate.result_request_id
        changed = replace(value.candidate, result_request_id=uuid4())
        result = await value.exchange().submit(
            value.delivery, changed, guard=value.guard
        )
        assert result.recovered and result.receipt.request_id == original_id
        assert value.gateway.recovered[0].intent.result_request_id == original_id
        assert not value.gateway.submitted and "commit" not in value.events
        assert (value.journal.state.terminal_status is not None) is terminal

    asyncio.run(scenario())


def test_existing_unaccepted_input_only_posts_original_after_not_found() -> None:
    async def scenario() -> None:
        value = setup(existing=True)
        result = await value.exchange().submit(
            value.delivery,
            replace(value.candidate, result_request_id=uuid4()),
            guard=value.guard,
        )
        assert value.events.index("get") < value.events.index("post")
        assert result.receipt.request_id == value.candidate.result_request_id

    asyncio.run(scenario())


@pytest.mark.parametrize("boundary", ["load", "commit", "get"])
def test_authority_loss_at_async_boundaries_prevents_post(boundary: str) -> None:
    async def scenario() -> None:
        value = setup(existing=boundary == "get")

        def expire(event: str) -> None:
            if event == boundary:
                value.guard.active = False

        value.journal.hook = expire
        value.gateway.hook = expire
        with pytest.raises(Expired):
            await value.exchange().submit(
                value.delivery, value.candidate, guard=value.guard
            )
        assert not value.gateway.submitted

    asyncio.run(scenario())


def test_expiry_after_http_does_not_lose_correlated_acceptance() -> None:
    async def scenario() -> None:
        value = setup()

        def expire(_: str) -> None:
            value.guard.active = False

        value.gateway.hook = expire
        with pytest.raises(Expired):
            await value.exchange().submit(
                value.delivery, value.candidate, guard=value.guard
            )
        assert value.journal.state.receipt is not None
        assert value.events.index("receipt") < len(value.events) - 1

    asyncio.run(scenario())


@pytest.mark.parametrize("pin", ["job", "candidate", "request", "definition"])
def test_uncorrelated_http_receipts_never_reach_journal(pin: str) -> None:
    async def scenario() -> None:
        value = setup()

        def change(item: CodingResultReceipt) -> CodingResultReceipt:
            if pin == "job":
                return replace(item, job_id=uuid4())
            if pin == "candidate":
                return replace(item, candidate_revision="f" * 40)
            if pin == "request":
                return replace(item, request_id=uuid4())
            return replace(item, definition_digest="f" * 64)

        value.gateway.change = change
        with pytest.raises(ValueError):
            await value.exchange().submit(
                value.delivery, value.candidate, guard=value.guard
            )
        assert value.journal.state.receipt is None and "receipt" not in value.events

    asyncio.run(scenario())


@pytest.mark.parametrize("pin", ["policy", "target", "bytes", "workspace"])
def test_original_input_mismatch_fails_before_journal_commit(pin: str) -> None:
    async def scenario() -> None:
        value = setup()
        if pin == "policy":
            value.candidate = replace(
                value.candidate,
                execution=replace(
                    value.candidate.execution,
                    policy=replace(
                        value.candidate.execution.policy, policy_digest="f" * 64
                    ),
                ),
            )
        elif pin == "target":
            value.delivery = replace(
                value.delivery,
                definition=replace(
                    value.delivery.definition, target_branch="other-target"
                ),
            )
        elif pin == "bytes":
            object.__setattr__(
                value.candidate.result.final_candidate, "raw_commit", b"invalid"
            )
        else:
            value.journal.state = replace(
                value.journal.state,
                original=replace(
                    value.journal.state.original, workspace_completion=None
                ),
            )
        with pytest.raises(ValueError):
            await value.exchange().submit(
                value.delivery, value.candidate, guard=value.guard
            )
        assert "commit" not in value.events and not value.gateway.submitted

    asyncio.run(scenario())


def test_caller_mutation_during_load_cannot_change_captured_input() -> None:
    async def scenario() -> None:
        value = setup()
        expected = value.candidate.result_request_id

        def mutate(_: str) -> None:
            object.__setattr__(value.candidate, "result_request_id", uuid4())
            object.__setattr__(
                value.delivery.definition.snapshot, "title", "Changed caller metadata"
            )

        # Gateway independently owns the accepted fixture definition too.
        value.gateway.definition = exchange_module._own(value.delivery.definition)
        value.journal.hook = mutate
        result = await value.exchange().submit(
            value.delivery, value.candidate, guard=value.guard
        )
        assert result.receipt.request_id == expected

    asyncio.run(scenario())


def test_recover_is_historical_even_when_local_authority_expired() -> None:
    async def scenario() -> None:
        value = setup(existing=True)
        value.guard.active = False
        value.gateway.status = value.status(terminal=True)
        result = await value.exchange().recover()
        assert result == value.gateway.status
        assert "guard" not in value.events and not value.gateway.submitted
        assert value.events[-2:] == ["receipt", "terminal"]

    asyncio.run(scenario())


def test_recover_without_input_or_with_unknown_legacy_history_never_posts() -> None:
    async def scenario() -> None:
        value = setup()
        assert await value.exchange().recover() is None
        value.journal.state = replace(value.journal.state, history_unknown=True)
        with pytest.raises(CodingResultUnavailable):
            await value.exchange().recover()
        assert not value.gateway.recovered and not value.gateway.submitted

    asyncio.run(scenario())


def test_accepted_history_cannot_be_resubmitted_if_recovery_returns_not_found() -> None:
    async def scenario() -> None:
        value = setup(existing=True)
        value.journal.state = replace(
            value.journal.state, receipt=value.status().receipt
        )
        with pytest.raises(CodingResultUnavailable):
            await value.exchange().submit(
                value.delivery, value.candidate, guard=value.guard
            )
        assert not value.gateway.submitted

    asyncio.run(scenario())


def test_cancelled_http_wait_still_records_receipt_and_close_waits_for_it() -> None:
    async def scenario() -> None:
        value = setup()
        value.gateway.block = True
        exchange = value.exchange()
        pending = asyncio.create_task(
            exchange.submit(value.delivery, value.candidate, guard=value.guard)
        )
        await asyncio.wait_for(value.gateway.entered.wait(), 2)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        with pytest.raises(CodingResultUnavailable):
            await exchange.recover()
        closing = asyncio.create_task(exchange.aclose())
        await asyncio.sleep(0)
        assert not closing.done()
        value.gateway.release.set()
        await asyncio.wait_for(closing, 2)
        assert value.journal.state.receipt is not None
        with pytest.raises(CodingResultUnavailable):
            await exchange.recover()

    asyncio.run(scenario())


def test_cancelled_preflight_keeps_cpu_owned_and_can_never_progress_to_post(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = prepare_coding_result
    entered, release = threading.Event(), threading.Event()

    def blocked(
        request: WorkerCodingResultRequest,
        authorization: WorkerAuthorization,
        definition: AssignedJobDefinition,
    ) -> PreparedCodingResult:
        entered.set()
        assert release.wait(3)
        return original(request, authorization, definition)

    monkeypatch.setattr(exchange_module, "prepare_coding_result", blocked)

    async def scenario() -> None:
        value = setup()
        exchange = value.exchange()
        pending = asyncio.create_task(
            exchange.submit(value.delivery, value.candidate, guard=value.guard)
        )
        assert await asyncio.to_thread(entered.wait, 2)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        with pytest.raises(CodingResultUnavailable):
            await exchange.recover()
        closing = asyncio.create_task(exchange.aclose())
        await asyncio.sleep(0)
        assert not closing.done()
        release.set()
        await asyncio.wait_for(closing, 2)
        assert "commit" not in value.events and not value.gateway.submitted

    try:
        asyncio.run(scenario())
    finally:
        release.set()


@pytest.mark.parametrize("reason", ["cancel", "close", "expiry"])
def test_final_transport_boundary_blocks_post_after_encoder_wait(reason: str) -> None:
    async def scenario() -> None:
        value = setup()
        value.gateway.block_encoding = True
        exchange = value.exchange()
        pending = asyncio.create_task(
            exchange.submit(value.delivery, value.candidate, guard=value.guard)
        )
        await asyncio.wait_for(value.gateway.encoding.wait(), 2)
        closing: asyncio.Task[None] | None = None
        if reason == "cancel":
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
        elif reason == "close":
            closing = asyncio.create_task(exchange.aclose())
            await asyncio.sleep(0)
            assert not closing.done()
        else:
            value.guard.active = False
        value.gateway.encoded.set()
        if reason != "cancel":
            with pytest.raises(
                Expired if reason == "expiry" else CodingResultUnavailable
            ):
                await pending
        if closing is not None:
            await closing
        await exchange.aclose()
        assert "post" not in value.events and value.journal.state.receipt is None

    asyncio.run(scenario())


def test_second_submit_cannot_overtake_first_assignment_read() -> None:
    async def scenario() -> None:
        value = setup()
        exchange = value.exchange()
        first = asyncio.create_task(
            exchange.submit(value.delivery, value.candidate, guard=value.guard)
        )
        await asyncio.sleep(0)
        with pytest.raises(CodingResultUnavailable):
            await exchange.submit(value.delivery, value.candidate, guard=value.guard)
        await first
        assert value.events.count("load") == 1 and value.events.count("post") == 1

    asyncio.run(scenario())


def test_changed_existing_target_cannot_recover_or_create_another_report() -> None:
    async def scenario() -> None:
        value = setup(existing=True)
        value.journal.state = replace(value.journal.state, target_branch="wrong-target")
        with pytest.raises(ClaimReplayConflict):
            await value.exchange().submit(
                value.delivery, value.candidate, guard=value.guard
            )
        assert "commit" not in value.events and not value.gateway.recovered

    asyncio.run(scenario())


@pytest.mark.parametrize("branch", ["source", "target"])
def test_recovery_independently_rejects_a_terminal_with_changed_branch(
    branch: str,
) -> None:
    async def scenario() -> None:
        value = setup(existing=True)
        status = value.status(terminal=True)
        assert status.terminal is not None
        reference = (
            replace(status.terminal.reference, source_branch="unexpected-branch")
            if branch == "source"
            else replace(status.terminal.reference, target_branch="unexpected-branch")
        )
        value.gateway.status = replace(
            status, terminal=replace(status.terminal, reference=reference)
        )
        with pytest.raises(ValueError):
            await value.exchange().recover()
        assert "receipt" not in value.events and "terminal" not in value.events

    asyncio.run(scenario())


def test_repeated_close_cancellation_waits_for_http_receipt_before_reraising() -> None:
    async def scenario() -> None:
        value = setup()
        value.gateway.block = True
        exchange = value.exchange()
        submitting = asyncio.create_task(
            exchange.submit(value.delivery, value.candidate, guard=value.guard)
        )
        await asyncio.wait_for(value.gateway.entered.wait(), 2)
        closing = asyncio.create_task(exchange.aclose())
        await asyncio.sleep(0)
        closing.cancel()
        await asyncio.sleep(0)
        assert not closing.done()
        closing.cancel()
        await asyncio.sleep(0)
        assert not closing.done()
        with pytest.raises(CodingResultUnavailable):
            await exchange.recover()
        value.gateway.release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(closing, 2)
        result = await submitting
        assert value.journal.state.receipt == result.receipt
        assert value.events.count("receipt") == 1
        # A subsequent closer observes the retained settled task; no retry.
        await exchange.aclose()
        assert len(value.gateway.submitted) == 1

    asyncio.run(scenario())


def test_submit_cancel_request_fences_post_before_outer_cancel_handler_runs() -> None:
    async def scenario() -> None:
        value = setup()
        value.gateway.block_encoding = True
        exchange = value.exchange()
        submitting = asyncio.create_task(
            exchange.submit(value.delivery, value.candidate, guard=value.guard)
        )
        await asyncio.wait_for(value.gateway.encoding.wait(), 2)

        # Wake the provider continuation before scheduling the canceled waiter.
        value.gateway.encoded.set()
        submitting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await submitting
        await exchange.aclose()

        assert value.gateway.submitted == []
        assert value.journal.state.intent is not None
        assert value.journal.state.receipt is None

    asyncio.run(scenario())
