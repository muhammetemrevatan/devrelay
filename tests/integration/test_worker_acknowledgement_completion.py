import asyncio
import sqlite3
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest

from devrelay.application.contracts.assignments import NoWorkReason
from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    LeaseProof,
    WorkerClaimRequest,
)
from devrelay.application.contracts.worker_recovery import (
    AcknowledgementCompletion,
    WorkerClaimOutcome,
    WorkerRecoveryPhase,
    WorkerRecoveryState,
)
from devrelay.application.contracts.workers import WorkerJournalScope
from devrelay.application.ports.worker_journal import WorkerStateUnavailable
from devrelay.infrastructure.worker_state import journal
from devrelay.infrastructure.worker_state.completions import save_completion
from devrelay.infrastructure.worker_state.journal import (
    AsyncClaimJournal,
    SqliteClaimStore,
)
from tests.acknowledgement_fixtures import (
    acknowledgement_inputs,
    acknowledgement_intent,
)
from tests.integration.test_worker_journal import TestProtector, TestStorage


@pytest.fixture
def store(tmp_path: Path) -> Iterator[SqliteClaimStore]:
    request, delivery = acknowledgement_inputs()
    scope = WorkerJournalScope(
        worker_id=delivery.outcome.worker_id, control_url="https://control.example/"
    )
    local = SqliteClaimStore(tmp_path, scope, TestProtector(), TestStorage())
    local.get_or_create_pending(request)
    local.record_outcome(request, delivery.outcome)
    local.get_or_create_acknowledgement(request, acknowledgement_intent(delivery))
    try:
        yield local
    finally:
        local.close()


def evidence(
    store: SqliteClaimStore,
) -> tuple[WorkerClaimRequest, AcknowledgementCompletion]:
    state = store.load_state()
    assert state.request and state.acknowledgement and state.assignment
    return state.request, AcknowledgementCompletion(
        intent=state.acknowledgement,
        acknowledged_at=state.assignment.recorded_at + timedelta(seconds=1),
    )


def test_completion_reopen_duplicate_and_claim_replay_preserve_original_state(
    store: SqliteClaimStore, tmp_path: Path
) -> None:
    request, completion = evidence(store)
    before = store.load_state()
    state = store.record_acknowledgement(request, completion)
    assert (
        state.phase is WorkerRecoveryPhase.ACKNOWLEDGED
        and state.completion == completion
    )
    assert state.assignment == before.assignment and state.request is not None
    assert store.record_acknowledgement(request, completion).completion == completion
    assert before.assignment is not None
    assert store.record_outcome(request, before.assignment).completion == completion
    with pytest.raises(ClaimReplayConflict):
        store.record_acknowledgement(
            request,
            replace(
                completion,
                acknowledged_at=completion.acknowledged_at + timedelta(seconds=1),
            ),
        )
    store.close()
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        assert connection.execute(
            "SELECT count(*) FROM acknowledgement_completion"
        ).fetchone() == (1,)
        assert "lease_authority_active" not in str(
            connection.execute("SELECT sql FROM sqlite_master").fetchall()
        )
    reopened = SqliteClaimStore(tmp_path, store._scope, store._protector, TestStorage())
    try:
        restored = reopened.load_state()
        assert (
            restored.completion == completion
            and restored.assignment == before.assignment
        )
        assert (
            restored.request is not None
            and restored.request.proof.reveal() == request.proof.reveal()
        )
    finally:
        reopened.close()


@pytest.mark.parametrize("change", ["proof", "intent", "before", "expired"])
def test_invalid_completion_keeps_original_pending_input(
    store: SqliteClaimStore, change: str
) -> None:
    request, completion = evidence(store)
    before = store.load_state()
    assert before.assignment and before.assignment.assignment
    if change == "proof":
        request = WorkerClaimRequest(
            claim_request_id=request.claim_request_id, proof=LeaseProof.generate()
        )
    elif change == "intent":
        completion = replace(
            completion, intent=replace(completion.intent, definition_digest="f" * 64)
        )
    else:
        completion = replace(
            completion,
            acknowledged_at=before.assignment.recorded_at - timedelta(seconds=1)
            if change == "before"
            else before.assignment.assignment.expires_at,
        )
    with pytest.raises(ClaimReplayConflict):
        store.record_acknowledgement(request, completion)
    assert store.load_state().phase is WorkerRecoveryPhase.ACKNOWLEDGEMENT_PENDING
    assert store.load_state().acknowledgement == before.acknowledgement


def test_completion_write_failure_rolls_back_all_rows(
    store: SqliteClaimStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    request, completion = evidence(store)
    before = store.load_state()
    original = save_completion

    def broken(
        connection: sqlite3.Connection,
        scope: WorkerJournalScope,
        request: WorkerClaimRequest,
        outcome: object,
        result: AcknowledgementCompletion,
    ) -> None:
        assert before.assignment is not None
        original(connection, scope, request, before.assignment, result)
        raise sqlite3.OperationalError("Simulated failure after writes.")

    monkeypatch.setattr(journal, "save_completion", broken)
    with pytest.raises(WorkerStateUnavailable):
        store.record_acknowledgement(request, completion)
    assert store.load_state().completion is None
    assert store.load_state().acknowledgement == before.acknowledgement


@pytest.mark.parametrize(
    "damage",
    [
        "UPDATE acknowledgement_completion SET integrity=zeroblob(32)",
        "UPDATE acknowledgement_completion SET acknowledged_at='2026-10-10T00:00:30.000000+00:00'",
        "DELETE FROM acknowledgement_completion",
        "DELETE FROM acknowledgement_completion; UPDATE metadata SET phase='acknowledgement_pending'",
        "DELETE FROM acknowledgement_completion; DELETE FROM acknowledgement_intent; UPDATE metadata SET phase='assigned'",
    ],
)
def test_damaged_completion_and_phase_regression_fail_closed(
    store: SqliteClaimStore, tmp_path: Path, damage: str
) -> None:
    request, completion = evidence(store)
    store.record_acknowledgement(request, completion)
    store.close()
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        connection.executescript(damage)
    with pytest.raises(WorkerStateUnavailable):
        SqliteClaimStore(tmp_path, store._scope, store._protector, TestStorage())


@pytest.mark.parametrize("damage", [False, True])
def test_v3_intent_upgrade_is_atomic_and_preserves_ciphertext_and_pins(
    store: SqliteClaimStore, tmp_path: Path, damage: bool
) -> None:
    before = store.load_state()
    store.close()
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        scope, prepared, phase, completed = connection.execute(
            "SELECT scope, prepared, phase, completed FROM metadata"
        ).fetchone()
        connection.execute("DROP TABLE metadata")
        connection.execute("DROP TABLE acknowledgement_completion")
        connection.execute("DROP TABLE heartbeat_input")
        connection.execute("DROP TABLE execution_input")
        connection.execute("DROP TABLE workspace_intent")
        connection.execute("DROP TABLE workspace_completion")
        connection.execute(
            "CREATE TABLE metadata(slot INTEGER PRIMARY KEY, version INTEGER CHECK(version=3), scope BLOB, prepared INTEGER, phase TEXT, completed INTEGER)"
        )
        connection.execute(
            "INSERT INTO metadata VALUES(1,3,?,?,?,?)",
            (scope, prepared, phase, completed),
        )
        rows = tuple(
            connection.execute("SELECT * FROM " + table).fetchall()
            for table in ("pending_claim", "assignment", "acknowledgement_intent")
        )
        if damage:
            connection.execute("CREATE TABLE acknowledgement_completion(slot INTEGER)")
    if damage:
        with pytest.raises(WorkerStateUnavailable):
            SqliteClaimStore(tmp_path, store._scope, store._protector, TestStorage())
    else:
        upgraded = SqliteClaimStore(
            tmp_path, store._scope, store._protector, TestStorage()
        )
        try:
            assert upgraded.load_state().acknowledgement == before.acknowledgement
            assert upgraded.load_state().phase is before.phase
        finally:
            upgraded.close()
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        assert connection.execute("SELECT version FROM metadata").fetchone() == (
            (3,) if damage else (7,)
        )
        assert (
            tuple(
                connection.execute("SELECT * FROM " + table).fetchall()
                for table in ("pending_claim", "assignment", "acknowledgement_intent")
            )
            == rows
        )


def test_cancelled_completion_wait_recovers_committed_evidence_on_owner_thread(
    tmp_path: Path,
) -> None:
    request, delivery = acknowledgement_inputs()
    intent = acknowledgement_intent(delivery)
    scope = WorkerJournalScope(
        worker_id=delivery.outcome.worker_id, control_url="https://control.example/"
    )
    completed = AcknowledgementCompletion(
        intent=intent,
        acknowledged_at=delivery.outcome.recorded_at + timedelta(seconds=1),
    )
    started, release = threading.Event(), threading.Event()
    threads: list[int] = []

    class Delayed(SqliteClaimStore):
        def record_acknowledgement(
            self, request: WorkerClaimRequest, completion: AcknowledgementCompletion
        ) -> WorkerRecoveryState:
            threads.append(threading.get_ident())
            started.set()
            if not release.wait(5):
                raise WorkerStateUnavailable("Test timeout.")
            return super().record_acknowledgement(request, completion)

    async def run(executor: ThreadPoolExecutor) -> None:
        loop = asyncio.get_running_loop()
        owner = threading.get_ident()
        local = await loop.run_in_executor(
            executor, lambda: Delayed(tmp_path, scope, TestProtector(), TestStorage())
        )
        adapter = AsyncClaimJournal(local, executor)
        try:
            await adapter.get_or_create_pending(request)
            await adapter.record_outcome(request, delivery.outcome)
            await adapter.get_or_create_acknowledgement(request, intent)
            task = asyncio.create_task(
                adapter.record_acknowledgement(request, completed)
            )
            assert await asyncio.to_thread(started.wait, 5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            release.set()
            state = await adapter.load_state()
            assert (
                state.completion == completed
                and state.phase is WorkerRecoveryPhase.ACKNOWLEDGED
            )
            assert threads and threads[0] != owner
        finally:
            release.set()
            adapter.stop_admission()
            await loop.run_in_executor(executor, local.close)

    with ThreadPoolExecutor(max_workers=1) as executor:
        asyncio.run(run(executor))


@pytest.mark.parametrize("phase", ["idle", "pending", "assigned", "no_work"])
def test_v3_upgrade_preserves_each_existing_recovery_phase(
    tmp_path: Path, phase: str
) -> None:
    request, delivery = acknowledgement_inputs()
    scope = WorkerJournalScope(
        worker_id=delivery.outcome.worker_id, control_url="https://control.example/"
    )
    protector = TestProtector()
    local = SqliteClaimStore(tmp_path, scope, protector, TestStorage())
    if phase != "idle":
        local.get_or_create_pending(request)
    if phase == "assigned":
        local.record_outcome(request, delivery.outcome)
    elif phase == "no_work":
        local.record_outcome(
            request,
            WorkerClaimOutcome(
                worker_id=scope.worker_id,
                claim_request_id=request.claim_request_id,
                recorded_at=delivery.outcome.recorded_at,
                no_work=NoWorkReason.EMPTY,
            ),
        )
    before = local.load_state()
    local.close()
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        row = connection.execute(
            "SELECT scope, prepared, phase, completed FROM metadata"
        ).fetchone()
        connection.execute("DROP TABLE metadata")
        connection.execute("DROP TABLE acknowledgement_completion")
        connection.execute("DROP TABLE heartbeat_input")
        connection.execute("DROP TABLE execution_input")
        connection.execute("DROP TABLE workspace_intent")
        connection.execute("DROP TABLE workspace_completion")
        connection.execute(
            "CREATE TABLE metadata(slot INTEGER PRIMARY KEY, version INTEGER CHECK(version=3), scope BLOB, prepared INTEGER, phase TEXT, completed INTEGER)"
        )
        connection.execute("INSERT INTO metadata VALUES(1,3,?,?,?,?)", row)
        rows = tuple(
            connection.execute("SELECT * FROM " + table).fetchall()
            for table in (
                "pending_claim",
                "assignment",
                "completed_no_work",
                "acknowledgement_intent",
            )
        )

    upgraded = SqliteClaimStore(tmp_path, scope, protector, TestStorage())

    try:
        state = upgraded.load_state()
        assert state.phase is before.phase and state.assignment == before.assignment
        assert state.last_no_work == before.last_no_work and state.completion is None
        if before.request is not None:
            assert (
                state.request is not None
                and state.request.claim_request_id == before.request.claim_request_id
            )
            assert state.request.proof.reveal() == before.request.proof.reveal()
        with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
            assert (
                tuple(
                    connection.execute("SELECT * FROM " + table).fetchall()
                    for table in (
                        "pending_claim",
                        "assignment",
                        "completed_no_work",
                        "acknowledgement_intent",
                    )
                )
                == rows
            )
    finally:
        upgraded.close()
