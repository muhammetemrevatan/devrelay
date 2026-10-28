"""Protected result history fixtures; no account, model, provider or live DB I/O."""

import asyncio
import sqlite3
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import timedelta
from pathlib import Path
from typing import Never
from uuid import uuid4

import pytest

import devrelay.infrastructure.worker_state.results as result_storage
from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    LeaseProof,
    WorkerClaimRequest,
)
from devrelay.application.contracts.coding_result_receipts import (
    CodingPublicationState,
    CodingResultReceipt,
    CodingResultStatus,
    TerminalCodingResult,
)
from devrelay.application.contracts.coding_result_requests import (
    WorkerCodingResultIntent,
)
from devrelay.application.contracts.coding_result_wire import (
    encode_coding_result_intent,
)
from devrelay.application.contracts.executions import (
    ExecutionIsolation,
    ExecutionPolicy,
    ExecutionRuntime,
    WorkerExecutionIntent,
)
from devrelay.application.contracts.worker_recovery import AcknowledgementCompletion
from devrelay.application.contracts.worker_result_recovery import (
    WorkerCodingResultState,
)
from devrelay.application.contracts.workers import (
    WorkerAuthorization,
    WorkerJournalScope,
)
from devrelay.application.ports.worker_journal import WorkerStateUnavailable
from devrelay.domain.jobs.values import PullRequestReference
from devrelay.infrastructure.worker_state.journal import (
    AsyncClaimJournal,
    SqliteClaimStore,
)
from devrelay.infrastructure.worker_state.workspaces import workspace_integrity
from tests.acknowledgement_fixtures import (
    acknowledgement_inputs,
    acknowledgement_intent,
)
from tests.integration.test_worker_heartbeat_journal import acknowledged_store
from tests.integration.test_worker_heartbeat_journal import inputs as heartbeat_inputs
from tests.integration.test_worker_journal import TestProtector, TestStorage
from tests.integration.test_worker_workspace_journal import inputs as workspace_inputs
from tests.unit.test_coding_results import context as result_context


@dataclass(frozen=True)
class Setup:
    store: SqliteClaimStore
    request: WorkerClaimRequest
    intent: WorkerCodingResultIntent
    target_branch: str
    receipt: CodingResultReceipt
    terminal: CodingResultStatus


def create(path: Path, *, workspace_completed: bool = True) -> Setup:
    request, delivery = acknowledgement_inputs()
    reference = delivery.outcome.assignment
    assert reference is not None
    scope = WorkerJournalScope(
        worker_id=reference.worker_id, control_url="https://control.example/"
    )
    local = SqliteClaimStore(path, scope, TestProtector(), TestStorage())
    local.get_or_create_pending(request)
    local.record_outcome(request, delivery.outcome)
    ack = acknowledgement_intent(delivery)
    local.get_or_create_acknowledgement(request, ack)
    local.record_acknowledgement(
        request,
        AcknowledgementCompletion(
            intent=ack,
            acknowledged_at=reference.issued_at + timedelta(seconds=1),
        ),
    )
    execution = WorkerExecutionIntent(
        execution_id=uuid4(),
        acknowledgement=ack,
        policy=ExecutionPolicy(
            runtime=ExecutionRuntime.CODEX,
            isolation=ExecutionIsolation.WINDOWS_RESTRICTED,
            policy_digest="a" * 64,
        ),
    )
    local.get_or_create_execution(request, execution)
    _, workspace, completion = workspace_inputs(local)
    local.get_or_create_workspace(request, workspace)
    if workspace_completed:
        local.record_workspace_completion(request, completion)
    report = result_context(
        definition=delivery.definition, execution_id=execution.execution_id
    ).result
    intent = WorkerCodingResultIntent(
        result_request_id=uuid4(), execution=execution, result=report
    )
    authorization = WorkerAuthorization(
        worker_id=reference.worker_id,
        allowed_repository_ids=frozenset({reference.repository_id}),
        credential_expires_at=reference.expires_at,
    )
    from devrelay.application.contracts.coding_result_requests import (
        WorkerCodingResultRequest,
    )

    identity = WorkerCodingResultRequest(intent=intent, claim=request).identify(
        authorization
    )
    receipt = CodingResultReceipt(
        result_id=uuid4(),
        worker_id=reference.worker_id,
        request_id=intent.result_request_id,
        claim_request_id=request.claim_request_id,
        job_id=reference.job_id,
        attempt_id=reference.attempt_id,
        repository_id=reference.repository_id,
        execution_id=execution.execution_id,
        definition_digest=report.definition_digest,
        request_fingerprint=identity.fingerprint,
        candidate_revision=report.final_candidate.revision,
        repair_cycles=0,
        publishing_workflow_version=4,
        recorded_at=reference.issued_at + timedelta(seconds=2),
    )
    terminal = CodingResultStatus(
        receipt=receipt,
        state=CodingPublicationState.FINALIZED,
        operation_version=4,
        terminal=TerminalCodingResult(
            receipt=receipt,
            reference=PullRequestReference(
                repository_id=reference.repository_id,
                number=7,
                source_branch=delivery.definition.authorized_branch,
                target_branch=delivery.definition.target_branch,
                head_revision=report.final_candidate.revision,
            ),
            finalized_at=receipt.recorded_at + timedelta(seconds=1),
            workflow_version=5,
            release_ordinal=1,
        ),
    )
    return Setup(
        local, request, intent, delivery.definition.target_branch, receipt, terminal
    )


@pytest.fixture
def setup(tmp_path: Path) -> Iterator[Setup]:
    value = create(tmp_path)
    try:
        yield value
    finally:
        value.store.close()


def retain(setup: Setup) -> WorkerCodingResultIntent:
    return setup.store.get_or_create_result(
        setup.request, setup.intent, target_branch=setup.target_branch
    )


def normalized(
    state: WorkerCodingResultState,
    previous: WorkerCodingResultState,
) -> WorkerCodingResultState:
    # Secret-bearing claim requests intentionally have identity equality. Compare
    # durable public history while retaining the original protected proof check.
    assert state.original.request and previous.original.request
    assert (
        state.original.request.proof.reveal()
        == previous.original.request.proof.reveal()
    )
    return replace(
        state, original=replace(state.original, request=previous.original.request)
    )


@pytest.mark.parametrize("stage", ["pending", "accepted", "terminal"])
def test_complete_result_history_reopens_with_protected_proof_and_original_assignment(
    setup: Setup,
    tmp_path: Path,
    stage: str,
) -> None:
    before = setup.store.load_state()
    assert retain(setup) == setup.intent
    if stage != "pending":
        setup.store.record_result_acceptance(setup.request, setup.receipt)
    if stage == "terminal":
        setup.store.record_result_terminal(setup.request, setup.terminal)
    setup.store.close()
    reopened = SqliteClaimStore(
        tmp_path, setup.store._scope, setup.store._protector, TestStorage()
    )
    try:
        state = reopened.load_result_state()
        assert state.intent == setup.intent
        assert state.target_branch == setup.target_branch
        assert state.receipt == (None if stage == "pending" else setup.receipt)
        assert state.terminal_status == (
            setup.terminal if stage == "terminal" else None
        )
        assert not state.history_unknown
        assert state.original.request is not None
        assert state.original.request.proof.reveal() == setup.request.proof.reveal()
        assert replace(state.original, request=before.request) == before
        assert setup.request.proof.reveal() not in repr(state)
        assert setup.request.proof.reveal().encode() not in encode_coding_result_intent(
            state.intent
        )
        assert all(
            setup.request.proof.reveal().encode() not in path.read_bytes()
            for path in tmp_path.iterdir()
        )
    finally:
        reopened.close()


def test_first_input_is_durable_and_fresh_uuid_replays_original_without_repin(
    setup: Setup,
) -> None:
    original = retain(setup)
    proposed = replace(setup.intent, result_request_id=uuid4())
    assert (
        setup.store.get_or_create_result(
            setup.request, proposed, target_branch=setup.target_branch
        )
        == original
    )
    assert setup.store.load_result_state().intent == original


def test_changed_target_branch_conflicts_without_replacing_original(
    setup: Setup,
) -> None:
    retain(setup)
    with pytest.raises(ClaimReplayConflict):
        setup.store.get_or_create_result(
            setup.request, setup.intent, target_branch="different-base"
        )
    assert setup.store.load_result_state().target_branch == setup.target_branch


def test_changed_result_content_conflicts_under_same_or_fresh_request_uuid(
    setup: Setup,
) -> None:
    retain(setup)
    original = setup.intent
    changed_check = replace(
        original.result.cycles[0].checks[0], diagnostic_tail="different sanitized tail"
    )
    changed_cycle = replace(
        original.result.cycles[0],
        checks=(changed_check, *original.result.cycles[0].checks[1:]),
    )
    changed_report = replace(original.result, cycles=(changed_cycle,))
    for request_id in (original.result_request_id, uuid4()):
        with pytest.raises(ClaimReplayConflict):
            setup.store.get_or_create_result(
                setup.request,
                replace(original, result_request_id=request_id, result=changed_report),
                target_branch=setup.target_branch,
            )
    assert setup.store.load_result_state().intent == original


@pytest.mark.parametrize("operation", ["input", "acceptance", "terminal"])
def test_wrong_original_proof_cannot_create_or_change_result_history(
    setup: Setup, operation: str
) -> None:
    retain(setup)
    forged = replace(setup.request, proof=LeaseProof.generate())
    with pytest.raises(ClaimReplayConflict):
        if operation == "input":
            setup.store.get_or_create_result(
                forged, setup.intent, target_branch=setup.target_branch
            )
        elif operation == "acceptance":
            setup.store.record_result_acceptance(forged, setup.receipt)
        else:
            setup.store.record_result_terminal(forged, setup.terminal)
    assert setup.store.load_result_state().receipt is None


def test_result_requires_known_completed_protected_workspace(tmp_path: Path) -> None:
    value = create(tmp_path, workspace_completed=False)
    try:
        with pytest.raises(ClaimReplayConflict):
            retain(value)
        assert value.store.load_result_state().intent is None
    finally:
        value.store.close()


@pytest.mark.parametrize(
    "field",
    [
        "worker_id",
        "attempt_id",
        "job_id",
        "execution_id",
        "request_id",
        "definition_digest",
        "request_fingerprint",
        "candidate_revision",
    ],
)
def test_acceptance_receipt_must_match_the_complete_retained_input(
    setup: Setup, field: str
) -> None:
    retain(setup)
    changed = {
        "worker_id": replace(setup.receipt, worker_id=uuid4()),
        "attempt_id": replace(setup.receipt, attempt_id=uuid4()),
        "job_id": replace(setup.receipt, job_id=uuid4()),
        "execution_id": replace(setup.receipt, execution_id=uuid4()),
        "request_id": replace(setup.receipt, request_id=uuid4()),
        "definition_digest": replace(setup.receipt, definition_digest="e" * 64),
        "request_fingerprint": replace(setup.receipt, request_fingerprint="e" * 64),
        "candidate_revision": replace(setup.receipt, candidate_revision="e" * 40),
    }[field]
    with pytest.raises(ClaimReplayConflict):
        setup.store.record_result_acceptance(setup.request, changed)
    assert setup.store.load_result_state().receipt is None


def test_acceptance_and_terminal_replays_preserve_exact_original_receipts(
    setup: Setup,
) -> None:
    retain(setup)
    accepted = setup.store.record_result_acceptance(setup.request, setup.receipt)
    assert accepted.receipt == setup.receipt
    assert (
        normalized(
            setup.store.record_result_acceptance(setup.request, setup.receipt), accepted
        )
        == accepted
    )
    terminal = setup.store.record_result_terminal(setup.request, setup.terminal)
    assert terminal.terminal_status == setup.terminal
    assert (
        normalized(
            setup.store.record_result_terminal(setup.request, setup.terminal), terminal
        )
        == terminal
    )
    assert (
        normalized(
            setup.store.record_result_acceptance(setup.request, setup.receipt), terminal
        )
        == terminal
    )


def test_acceptance_before_original_acknowledgement_is_rejected_without_journal_changes(
    setup: Setup,
) -> None:
    retain(setup)
    before = setup.store.load_result_state()
    completion = before.original.completion
    assert completion is not None
    stale = replace(
        setup.receipt,
        recorded_at=completion.acknowledged_at - timedelta(microseconds=1),
    )
    with pytest.raises(ClaimReplayConflict):
        setup.store.record_result_acceptance(setup.request, stale)
    assert normalized(setup.store.load_result_state(), before) == before


def test_nonterminal_pending_receipt_never_becomes_terminal_acknowledgement(
    setup: Setup,
) -> None:
    retain(setup)
    setup.store.record_result_acceptance(setup.request, setup.receipt)
    pending = CodingResultStatus(
        receipt=setup.receipt,
        state=CodingPublicationState.ACCEPTED,
        operation_version=0,
    )
    with pytest.raises(ClaimReplayConflict):
        setup.store.record_result_terminal(setup.request, pending)
    assert setup.store.load_result_state().terminal_status is None


@pytest.mark.parametrize("field", ["source_branch", "target_branch"])
def test_terminal_publication_branch_must_match_the_retained_coding_and_target_pins(
    setup: Setup,
    field: str,
) -> None:
    retain(setup)
    terminal = setup.terminal.terminal
    assert terminal is not None
    reference = (
        replace(terminal.reference, source_branch="other-coding-branch")
        if field == "source_branch"
        else replace(terminal.reference, target_branch="other-target")
    )
    changed = replace(setup.terminal, terminal=replace(terminal, reference=reference))
    with pytest.raises(ClaimReplayConflict):
        setup.store.record_result_terminal(setup.request, changed)
    assert setup.store.load_result_state().terminal_status is None


def test_terminal_record_can_persist_both_receipts_when_acceptance_response_was_lost(
    setup: Setup,
) -> None:
    retain(setup)
    state = setup.store.record_result_terminal(setup.request, setup.terminal)
    assert state.receipt == setup.receipt
    assert state.terminal_status == setup.terminal


def test_changed_terminal_publication_number_cannot_replace_durable_acknowledgement(
    setup: Setup,
) -> None:
    retain(setup)
    setup.store.record_result_terminal(setup.request, setup.terminal)
    terminal = setup.terminal.terminal
    assert terminal is not None
    changed = replace(
        setup.terminal,
        terminal=replace(
            terminal,
            reference=replace(terminal.reference, number=terminal.reference.number + 1),
        ),
    )
    with pytest.raises(ClaimReplayConflict):
        setup.store.record_result_terminal(setup.request, changed)
    assert setup.store.load_result_state().terminal_status == setup.terminal


def test_ack_claim_and_heartbeat_replays_preserve_all_pending_result_bytes(
    setup: Setup,
) -> None:
    retain(setup)
    setup.store.record_result_acceptance(setup.request, setup.receipt)
    before = setup.store.load_result_state()
    original = before.original
    assert (
        original.assignment
        and original.acknowledgement
        and original.completion
        and original.execution
    )
    setup.store.get_or_create_pending(setup.request)
    setup.store.record_outcome(setup.request, original.assignment)
    setup.store.get_or_create_acknowledgement(setup.request, original.acknowledgement)
    setup.store.record_acknowledgement(setup.request, original.completion)
    setup.store.get_or_create_execution(setup.request, original.execution)
    request, heartbeat = heartbeat_inputs(setup.store)
    setup.store.get_or_create_heartbeat(request, heartbeat)
    setup.store.record_heartbeat(request, heartbeat)
    current = setup.store.load_result_state()
    assert current.intent == before.intent and current.receipt == before.receipt
    assert current.terminal_status is None and not current.history_unknown
    assert current.original.last_heartbeat == heartbeat


@pytest.mark.parametrize(
    "damage",
    [
        "DELETE FROM coding_result_input",
        "DELETE FROM coding_result_receipts WHERE slot=1",
        "DELETE FROM coding_result_receipts WHERE slot=2",
        "DELETE FROM coding_result_receipts; DELETE FROM coding_result_input",
        "UPDATE coding_result_input SET target_branch='forged-target'",
        "UPDATE coding_result_input SET payload_sha256='eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'",
        "UPDATE coding_result_input SET payload_bytes=CAST(payload_bytes||' ' AS BLOB)",
        "UPDATE coding_result_receipts SET payload_bytes=X'7B7D' WHERE slot=1",
        "UPDATE metadata SET result_history_unknown=1",
        "DROP TABLE coding_result_input",
        "DROP TABLE coding_result_receipts",
    ],
)
def test_result_payload_receipt_presence_and_target_tampering_fail_closed(
    setup: Setup,
    tmp_path: Path,
    damage: str,
) -> None:
    retain(setup)
    setup.store.record_result_acceptance(setup.request, setup.receipt)
    setup.store.record_result_terminal(setup.request, setup.terminal)
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        connection.executescript(damage)
    with pytest.raises(WorkerStateUnavailable) as error:
        setup.store.load_result_state()
    assert setup.request.proof.reveal() not in str(error.value)


def make_v7(local: SqliteClaimStore, path: Path) -> None:
    state = local.load_state()
    assert state.request and state.assignment and state.completion
    marker = workspace_integrity(
        local._scope,
        state.request,
        state.assignment,
        state.completion,
        state.pending_heartbeat,
        state.last_heartbeat,
        state.execution,
        state.workspace_intent,
        state.workspace_completion,
        state.workspace_history_unknown,
    )
    local.close()
    with sqlite3.connect(path / "journal.sqlite3") as connection:
        header = connection.execute(
            "SELECT scope,prepared,phase,completed,workspace_history_unknown FROM metadata"
        ).fetchone()
        connection.execute("DROP TABLE metadata")
        connection.execute("DROP TABLE coding_result_input")
        connection.execute("DROP TABLE coding_result_receipts")
        connection.execute("""CREATE TABLE metadata (
          slot INTEGER PRIMARY KEY CHECK(slot=1),version INTEGER NOT NULL CHECK(version=7),
          scope BLOB NOT NULL,prepared INTEGER NOT NULL,phase TEXT NOT NULL,
          completed INTEGER NOT NULL,workspace_history_unknown INTEGER NOT NULL
        )""")
        connection.execute("INSERT INTO metadata VALUES(1,7,?,?,?,?,?)", header)
        connection.execute("UPDATE assignment SET integrity=?", (marker,))


@pytest.mark.parametrize("execution_present", [False, True])
def test_valid_v7_upgrade_preserves_originals_and_marks_only_execution_history_unknown(
    tmp_path: Path,
    execution_present: bool,
) -> None:
    local = (
        create(tmp_path).store if execution_present else acknowledged_store(tmp_path)
    )
    before = local.load_state()
    make_v7(local, tmp_path)
    reopened = SqliteClaimStore(tmp_path, local._scope, local._protector, TestStorage())
    try:
        state = reopened.load_result_state()
        assert replace(state.original, request=before.request) == before
        assert state.history_unknown is execution_present
        assert state.intent is None
        assert state.receipt is None
        assert state.terminal_status is None
        assert (
            state.original.workspace_history_unknown == before.workspace_history_unknown
        )
        with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
            assert connection.execute("SELECT version FROM metadata").fetchone() == (9,)
    finally:
        reopened.close()


def test_v7_upgrade_never_authorizes_a_new_report_for_an_old_execution(
    setup: Setup,
    tmp_path: Path,
) -> None:
    make_v7(setup.store, tmp_path)
    reopened = SqliteClaimStore(
        tmp_path, setup.store._scope, setup.store._protector, TestStorage()
    )
    try:
        assert reopened.load_result_state().history_unknown
        with pytest.raises(WorkerStateUnavailable):
            reopened.get_or_create_result(
                setup.request, setup.intent, target_branch=setup.target_branch
            )
        with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
            connection.execute("UPDATE metadata SET result_history_unknown=0")
        with pytest.raises(WorkerStateUnavailable):
            reopened.load_result_state()
    finally:
        reopened.close()


def test_failed_legacy_upgrade_rolls_back_original_version_rows_and_partial_schema(
    setup: Setup,
    tmp_path: Path,
) -> None:
    make_v7(setup.store, tmp_path)
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        original = connection.execute("SELECT * FROM assignment").fetchall()
        connection.execute("""CREATE TRIGGER reject_result_upgrade BEFORE UPDATE ON assignment
          BEGIN SELECT RAISE(ABORT,'fixture upgrade marker failure'); END""")
    with pytest.raises(WorkerStateUnavailable):
        SqliteClaimStore(
            tmp_path, setup.store._scope, setup.store._protector, TestStorage()
        )
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        assert connection.execute("SELECT version FROM metadata").fetchone() == (7,)
        assert connection.execute("SELECT * FROM assignment").fetchall() == original
        assert (
            connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'coding_result_%'"
            ).fetchall()
            == []
        )


def test_cancelled_result_commit_is_recovered_on_the_original_owning_executor(
    tmp_path: Path,
) -> None:
    executor = ThreadPoolExecutor(max_workers=1)
    entered, release = threading.Event(), threading.Event()
    owners: list[int] = []

    class DelayedStore(SqliteClaimStore):
        def get_or_create_result(
            self,
            request: WorkerClaimRequest,
            candidate: WorkerCodingResultIntent,
            *,
            target_branch: str,
        ) -> WorkerCodingResultIntent:
            owners.append(threading.get_ident())
            entered.set()
            if not release.wait(5):
                raise WorkerStateUnavailable("Fixture result commit was not released.")
            return super().get_or_create_result(
                request, candidate, target_branch=target_branch
            )

    def initialize() -> Setup:
        original = create(tmp_path)
        original.store.close()
        local = DelayedStore(
            tmp_path, original.store._scope, original.store._protector, TestStorage()
        )
        return replace(original, store=local)

    async def scenario() -> None:
        loop = asyncio.get_running_loop()
        original = await loop.run_in_executor(executor, initialize)
        journal = AsyncClaimJournal(original.store, executor)
        try:
            task = asyncio.create_task(
                journal.get_or_create_result(
                    original.request,
                    original.intent,
                    target_branch=original.target_branch,
                )
            )
            assert await asyncio.to_thread(entered.wait, 2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            release.set()
            state = await journal.load_result_state()
            assert state.intent == original.intent
            assert owners[0] != threading.get_ident()
            journal.stop_admission()
            with pytest.raises(WorkerStateUnavailable):
                await journal.get_or_create_result(
                    original.request,
                    original.intent,
                    target_branch=original.target_branch,
                )
        finally:
            release.set()
            await loop.run_in_executor(executor, original.store.close)

    try:
        asyncio.run(scenario())
    finally:
        release.set()
        executor.shutdown(wait=True)


def test_v8_downgrade_and_erasure_cannot_recreate_an_empty_result_history(
    setup: Setup,
    tmp_path: Path,
) -> None:
    retain(setup)
    setup.store.record_result_terminal(setup.request, setup.terminal)
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        protected_v8_marker = connection.execute(
            "SELECT integrity FROM assignment"
        ).fetchone()[0]
    make_v7(setup.store, tmp_path)
    # Erasing v8 tables/metadata cannot supply a valid old-version HMAC. Restore
    # the actual protected marker rather than the trusted test helper's v7 one.
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        connection.execute("UPDATE assignment SET integrity=?", (protected_v8_marker,))
    with pytest.raises(WorkerStateUnavailable):
        SqliteClaimStore(
            tmp_path, setup.store._scope, setup.store._protector, TestStorage()
        )


@pytest.mark.parametrize("operation", ["input", "acceptance", "terminal"])
def test_result_bytes_and_presence_marker_roll_back_atomically_on_storage_failure(
    setup: Setup,
    tmp_path: Path,
    operation: str,
) -> None:
    if operation != "input":
        retain(setup)
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        connection.execute("""CREATE TRIGGER reject_result_marker BEFORE UPDATE OF integrity ON assignment
          BEGIN SELECT RAISE(ABORT,'fixture protected marker failure'); END""")
    with pytest.raises(WorkerStateUnavailable):
        if operation == "input":
            retain(setup)
        elif operation == "acceptance":
            setup.store.record_result_acceptance(setup.request, setup.receipt)
        else:
            setup.store.record_result_terminal(setup.request, setup.terminal)
    state = setup.store.load_result_state()
    assert state.intent == (None if operation == "input" else setup.intent)
    assert state.receipt is None and state.terminal_status is None
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        assert connection.execute(
            "SELECT count(*) FROM coding_result_receipts"
        ).fetchone() == (0,)


def test_cancelled_terminal_write_preserves_durable_ack_on_owning_executor(
    tmp_path: Path,
) -> None:
    executor = ThreadPoolExecutor(max_workers=1)
    entered, release = threading.Event(), threading.Event()
    owners: list[int] = []

    class DelayedStore(SqliteClaimStore):
        def record_result_terminal(
            self,
            request: WorkerClaimRequest,
            status: CodingResultStatus,
        ) -> WorkerCodingResultState:
            owners.append(threading.get_ident())
            entered.set()
            if not release.wait(5):
                raise WorkerStateUnavailable(
                    "Fixture terminal commit was not released."
                )
            return super().record_result_terminal(request, status)

    def initialize() -> Setup:
        original = create(tmp_path)
        retain(original)
        original.store.record_result_acceptance(original.request, original.receipt)
        original.store.close()
        local = DelayedStore(
            tmp_path, original.store._scope, original.store._protector, TestStorage()
        )
        return replace(original, store=local)

    async def scenario() -> None:
        loop = asyncio.get_running_loop()
        original = await loop.run_in_executor(executor, initialize)
        journal = AsyncClaimJournal(original.store, executor)
        try:
            task = asyncio.create_task(
                journal.record_result_terminal(original.request, original.terminal)
            )
            assert await asyncio.to_thread(entered.wait, 2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            release.set()
            state = await journal.load_result_state()
            assert state.terminal_status == original.terminal
            assert state.receipt == original.receipt
            assert state.intent == original.intent
            assert owners[0] != threading.get_ident()
            journal.stop_admission()
            with pytest.raises(WorkerStateUnavailable):
                await journal.record_result_terminal(
                    original.request, original.terminal
                )
        finally:
            release.set()
            await loop.run_in_executor(executor, original.store.close)

    try:
        asyncio.run(scenario())
    finally:
        release.set()
        executor.shutdown(wait=True)


@pytest.mark.parametrize(
    "damage",
    [
        "unicode_digest",
        "oversized_report",
        "oversized_acceptance",
        "oversized_terminal",
        "oversized_target",
    ],
)
def test_corrupt_storage_types_and_sizes_are_rejected_before_any_result_decoder(
    setup: Setup,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    damage: str,
) -> None:
    retain(setup)
    setup.store.record_result_terminal(setup.request, setup.terminal)
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        connection.execute("PRAGMA ignore_check_constraints=ON")
        if damage == "unicode_digest":
            connection.execute(
                "UPDATE coding_result_input SET payload_sha256=?", ("é" * 64,)
            )
        elif damage == "oversized_report":
            connection.execute(
                "UPDATE coding_result_input SET payload_bytes=zeroblob(16777217)"
            )
        elif damage == "oversized_acceptance":
            connection.execute(
                "UPDATE coding_result_receipts SET payload_bytes=zeroblob(65537) WHERE slot=1"
            )
        elif damage == "oversized_terminal":
            connection.execute(
                "UPDATE coding_result_receipts SET payload_bytes=zeroblob(65537) WHERE slot=2"
            )
        else:
            connection.execute(
                "UPDATE coding_result_input SET target_branch=?", ("t" * 257,)
            )

    def unexpected_decode(*args: object, **kwargs: object) -> Never:
        raise AssertionError(
            "Corrupt oversized or noncanonical storage reached a decoder."
        )

    for decoder in (
        "decode_coding_result_intent",
        "decode_coding_result_acceptance",
        "decode_coding_result_status",
    ):
        monkeypatch.setattr(result_storage, decoder, unexpected_decode)
    with pytest.raises(WorkerStateUnavailable) as error:
        setup.store.load_result_state()
    assert setup.request.proof.reveal() not in str(error.value)


@pytest.mark.parametrize("operation", ["input", "acceptance", "terminal"])
@pytest.mark.parametrize("mutation", ["claim_id", "proof"])
def test_result_write_captures_original_claim_before_waiting_for_async_admission(
    tmp_path: Path,
    operation: str,
    mutation: str,
) -> None:
    executor = ThreadPoolExecutor(max_workers=1)

    def initialize() -> Setup:
        value = create(tmp_path)
        if operation != "input":
            retain(value)
        return value

    async def scenario() -> None:
        loop = asyncio.get_running_loop()
        value = await loop.run_in_executor(executor, initialize)
        journal = AsyncClaimJournal(value.store, executor)
        supplied = WorkerClaimRequest(
            claim_request_id=value.request.claim_request_id,
            proof=LeaseProof(value.request.proof.reveal()),
        )
        await journal._admission.acquire()
        held = True

        async def write() -> None:
            if operation == "input":
                await journal.get_or_create_result(
                    supplied, value.intent, target_branch=value.target_branch
                )
            elif operation == "acceptance":
                await journal.record_result_acceptance(supplied, value.receipt)
            else:
                await journal.record_result_terminal(supplied, value.terminal)

        task = asyncio.create_task(write())
        try:
            # Run through the synchronous snapshot until the held admission
            # boundary. The supplied secret-bearing object stays caller-owned.
            await asyncio.sleep(0)
            assert not task.done()
            if mutation == "claim_id":
                object.__setattr__(supplied, "claim_request_id", uuid4())
            else:
                object.__setattr__(
                    supplied.proof, "_encoded", LeaseProof.generate().reveal()
                )
            journal._admission.release()
            held = False
            await task
            state = await journal.load_result_state()
            assert state.intent == value.intent
            assert state.original.request is not None
            assert (
                state.original.request.claim_request_id
                == value.request.claim_request_id
            )
            assert state.original.request.proof.reveal() == value.request.proof.reveal()
            assert state.receipt == (None if operation == "input" else value.receipt)
            assert state.terminal_status == (
                value.terminal if operation == "terminal" else None
            )
        finally:
            if held:
                journal._admission.release()
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await loop.run_in_executor(executor, value.store.close)

    try:
        asyncio.run(scenario())
    finally:
        executor.shutdown(wait=True)
