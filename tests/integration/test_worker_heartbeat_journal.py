import asyncio
import sqlite3
import sys
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import SecretStr

from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    LeaseProof,
    WorkerClaimRequest,
)
from devrelay.application.contracts.worker_recovery import (
    AcknowledgementCompletion,
    WorkerHeartbeatIntent,
    WorkerRecoveryPhase,
    WorkerRecoveryState,
)
from devrelay.application.contracts.workers import WorkerJournalScope
from devrelay.application.ports.worker_journal import WorkerStateUnavailable
from devrelay.bootstrap.worker import open_worker_journal
from devrelay.config.worker import WorkerSettings
from devrelay.infrastructure.worker_state.completions import completion_integrity
from devrelay.infrastructure.worker_state.journal import (
    AsyncClaimJournal,
    SqliteClaimStore,
)
from tests.acknowledgement_fixtures import (
    acknowledgement_inputs,
    acknowledgement_intent,
)
from tests.integration.test_worker_journal import TOKEN, TestProtector, TestStorage


def acknowledged_store(tmp_path: Path) -> SqliteClaimStore:
    request, delivery = acknowledgement_inputs()
    scope = WorkerJournalScope(
        worker_id=delivery.outcome.worker_id, control_url="https://control.example/"
    )
    store = SqliteClaimStore(tmp_path, scope, TestProtector(), TestStorage())
    store.get_or_create_pending(request)
    store.record_outcome(request, delivery.outcome)
    intent = acknowledgement_intent(delivery)
    store.get_or_create_acknowledgement(request, intent)
    store.record_acknowledgement(
        request,
        AcknowledgementCompletion(
            intent=intent,
            acknowledged_at=delivery.outcome.recorded_at + timedelta(seconds=1),
        ),
    )
    return store


@pytest.fixture
def store(tmp_path: Path) -> Iterator[SqliteClaimStore]:
    value = acknowledged_store(tmp_path)
    try:
        yield value
    finally:
        value.close()


def inputs(store: SqliteClaimStore) -> tuple[WorkerClaimRequest, WorkerHeartbeatIntent]:
    state = store.load_state()
    assert state.request and state.assignment and state.assignment.assignment
    return state.request, WorkerHeartbeatIntent(
        heartbeat_request_id=uuid4(),
        claim_request_id=state.request.claim_request_id,
        attempt_id=state.assignment.assignment.attempt_id,
        generation=1,
        expected_lease_revision=1,
    )


@pytest.mark.parametrize("completed", [False, True])
def test_heartbeat_input_reopens_with_original_evidence_only(
    store: SqliteClaimStore, tmp_path: Path, completed: bool
) -> None:
    request, intent = inputs(store)
    original = store.load_state()
    store.get_or_create_heartbeat(request, intent)
    if completed:
        store.record_heartbeat(request, intent)
    store.close()
    reopened = SqliteClaimStore(tmp_path, store._scope, store._protector, TestStorage())
    try:
        state = reopened.load_state()

        assert state.phase is WorkerRecoveryPhase.ACKNOWLEDGED
        assert state.assignment == original.assignment
        assert state.acknowledgement == original.acknowledgement
        assert state.completion == original.completion
        assert state.pending_heartbeat == (None if completed else intent)
        assert state.last_heartbeat == (intent if completed else None)
        assert state.request and state.request.proof.reveal() == request.proof.reveal()
        assert request.proof.reveal() not in repr(state)
        assert all(
            request.proof.reveal().encode() not in p.read_bytes()
            for p in tmp_path.iterdir()
        )
        with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
            columns = {
                row[1]
                for row in connection.execute("PRAGMA table_info(heartbeat_input)")
            }
        assert columns == {
            "slot",
            "heartbeat_request_id",
            "claim_request_id",
            "attempt_id",
            "generation",
            "expected_lease_revision",
            "protocol_version",
        }
    finally:
        reopened.close()


def test_pending_input_reuses_exact_uuid_for_fresh_identical_candidate(
    store: SqliteClaimStore,
) -> None:
    request, intent = inputs(store)
    store.get_or_create_heartbeat(request, intent)
    candidate = replace(intent, heartbeat_request_id=uuid4())

    recovered = store.get_or_create_heartbeat(request, candidate)

    assert recovered == intent
    assert store.load_state().pending_heartbeat == intent


@pytest.mark.parametrize(
    "field", ["claim_request_id", "attempt_id", "expected_lease_revision"]
)
def test_changed_pending_input_conflicts_without_repin(
    store: SqliteClaimStore, field: str
) -> None:
    request, intent = inputs(store)
    store.get_or_create_heartbeat(request, intent)
    candidate = (
        replace(intent, expected_lease_revision=2)
        if field == "expected_lease_revision"
        else replace(intent, claim_request_id=uuid4())
        if field == "claim_request_id"
        else replace(intent, attempt_id=uuid4())
    )

    with pytest.raises(ClaimReplayConflict):
        store.get_or_create_heartbeat(request, candidate)

    assert store.load_state().pending_heartbeat == intent


@pytest.mark.parametrize("operation", ["prepare", "complete"])
def test_wrong_proof_cannot_change_heartbeat_input(
    store: SqliteClaimStore, operation: str
) -> None:
    request, intent = inputs(store)
    store.get_or_create_heartbeat(request, intent)
    changed = replace(request, proof=LeaseProof.generate())

    with pytest.raises(ClaimReplayConflict):
        if operation == "prepare":
            store.get_or_create_heartbeat(changed, intent)
        else:
            store.record_heartbeat(changed, intent)

    assert store.load_state().pending_heartbeat == intent


def test_old_completion_preserves_newer_pending_input(store: SqliteClaimStore) -> None:
    request, original = inputs(store)
    store.get_or_create_heartbeat(request, original)
    store.record_heartbeat(request, original)
    newer = replace(original, heartbeat_request_id=uuid4(), expected_lease_revision=2)
    store.get_or_create_heartbeat(request, newer)

    repeated = store.record_heartbeat(request, original)

    assert repeated.pending_heartbeat == newer
    assert repeated.last_heartbeat == original


@pytest.mark.parametrize("changed", ["uuid", "revision"])
def test_completion_requires_exact_pending_input(
    store: SqliteClaimStore, changed: str
) -> None:
    request, intent = inputs(store)
    store.get_or_create_heartbeat(request, intent)
    candidate = (
        replace(intent, heartbeat_request_id=uuid4())
        if changed == "uuid"
        else replace(intent, expected_lease_revision=2)
    )

    with pytest.raises(ClaimReplayConflict):
        store.record_heartbeat(request, candidate)

    assert store.load_state().pending_heartbeat == intent


def test_completion_without_pending_input_is_rejected(store: SqliteClaimStore) -> None:
    request, intent = inputs(store)

    with pytest.raises(ClaimReplayConflict):
        store.record_heartbeat(request, intent)

    assert store.load_state().last_heartbeat is None


@pytest.mark.parametrize(
    "phase", ["idle", "pending", "assigned", "acknowledgement_pending"]
)
@pytest.mark.parametrize("operation", ["prepare", "complete"])
def test_heartbeat_requires_completed_acknowledgement(
    tmp_path: Path, phase: str, operation: str
) -> None:
    request, delivery = acknowledgement_inputs()
    scope = WorkerJournalScope(
        worker_id=delivery.outcome.worker_id, control_url="https://control.example/"
    )
    local = SqliteClaimStore(tmp_path, scope, TestProtector(), TestStorage())
    if phase != "idle":
        local.get_or_create_pending(request)
    if phase in ("assigned", "acknowledgement_pending"):
        local.record_outcome(request, delivery.outcome)
    acknowledgement = acknowledgement_intent(delivery)
    if phase == "acknowledgement_pending":
        local.get_or_create_acknowledgement(request, acknowledgement)
    candidate = WorkerHeartbeatIntent(
        heartbeat_request_id=uuid4(),
        claim_request_id=request.claim_request_id,
        attempt_id=acknowledgement.attempt_id,
        generation=1,
        expected_lease_revision=1,
    )
    try:
        with pytest.raises(ClaimReplayConflict):
            if operation == "prepare":
                local.get_or_create_heartbeat(request, candidate)
            else:
                local.record_heartbeat(request, candidate)

        state = local.load_state()
        assert state.phase.value == phase
        assert state.pending_heartbeat is None and state.last_heartbeat is None
    finally:
        local.close()


def test_claim_and_acknowledgement_replay_preserve_both_heartbeat_inputs(
    store: SqliteClaimStore,
) -> None:
    request, first = inputs(store)
    store.get_or_create_heartbeat(request, first)
    store.record_heartbeat(request, first)
    second = replace(first, heartbeat_request_id=uuid4(), expected_lease_revision=2)
    store.get_or_create_heartbeat(request, second)
    before = store.load_state()
    assert before.assignment and before.completion

    store.record_outcome(request, before.assignment)
    result = store.record_acknowledgement(request, before.completion)

    assert result.pending_heartbeat == second and result.last_heartbeat == first
    assert (
        result.assignment == before.assignment
        and result.completion == before.completion
    )


def test_successive_heartbeats_keep_only_one_last_input_and_one_pending(
    store: SqliteClaimStore, tmp_path: Path
) -> None:
    request, first = inputs(store)
    store.get_or_create_heartbeat(request, first)
    store.record_heartbeat(request, first)
    second = replace(first, heartbeat_request_id=uuid4(), expected_lease_revision=2)
    store.get_or_create_heartbeat(request, second)
    store.record_heartbeat(request, second)
    third = replace(first, heartbeat_request_id=uuid4(), expected_lease_revision=3)

    store.get_or_create_heartbeat(request, third)

    state = store.load_state()
    assert state.pending_heartbeat == third and state.last_heartbeat == second
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        rows = connection.execute(
            "SELECT slot,heartbeat_request_id FROM heartbeat_input ORDER BY slot"
        ).fetchall()
    assert rows == [
        (1, str(third.heartbeat_request_id)),
        (2, str(second.heartbeat_request_id)),
    ]


def test_completed_historical_input_cannot_clear_state_after_it_leaves_last_slot(
    store: SqliteClaimStore,
) -> None:
    request, first = inputs(store)
    store.get_or_create_heartbeat(request, first)
    store.record_heartbeat(request, first)
    second = replace(first, heartbeat_request_id=uuid4(), expected_lease_revision=2)
    store.get_or_create_heartbeat(request, second)
    store.record_heartbeat(request, second)

    with pytest.raises(ClaimReplayConflict):
        store.record_heartbeat(request, first)

    assert store.load_state().last_heartbeat == second


@pytest.mark.parametrize("changed", ["same", "same_uuid_changed_revision", "regressed"])
def test_completed_uuid_and_revision_regression_are_rejected(
    store: SqliteClaimStore, changed: str
) -> None:
    request, initial = inputs(store)
    intent = replace(initial, expected_lease_revision=2)
    store.get_or_create_heartbeat(request, intent)
    store.record_heartbeat(request, intent)
    candidate = (
        intent
        if changed == "same"
        else replace(intent, expected_lease_revision=3)
        if changed == "same_uuid_changed_revision"
        else replace(intent, heartbeat_request_id=uuid4(), expected_lease_revision=1)
    )

    with pytest.raises(ClaimReplayConflict):
        store.get_or_create_heartbeat(request, candidate)

    assert store.load_state().pending_heartbeat is None
    assert store.load_state().last_heartbeat == intent


@pytest.mark.parametrize("completed", [False, True])
@pytest.mark.parametrize(
    "damage",
    [
        "DELETE FROM heartbeat_input",
        "UPDATE heartbeat_input SET expected_lease_revision=2",
        "UPDATE heartbeat_input SET heartbeat_request_id='22222222-2222-4222-8222-222222222222'",
        "UPDATE heartbeat_input SET claim_request_id='22222222-2222-4222-8222-222222222222'",
        "UPDATE heartbeat_input SET attempt_id='22222222-2222-4222-8222-222222222222'",
        "UPDATE heartbeat_input SET slot=3-slot",
        "UPDATE assignment SET integrity=zeroblob(32)",
        "DELETE FROM heartbeat_input; DELETE FROM acknowledgement_completion; UPDATE metadata SET phase='acknowledgement_pending'",
    ],
)
def test_input_erasure_mutation_or_phase_regression_fails_closed(
    store: SqliteClaimStore, tmp_path: Path, completed: bool, damage: str
) -> None:
    request, intent = inputs(store)
    store.get_or_create_heartbeat(request, intent)
    if completed:
        store.record_heartbeat(request, intent)
    store.close()
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        connection.executescript(damage)

    with pytest.raises(WorkerStateUnavailable):
        SqliteClaimStore(tmp_path, store._scope, store._protector, TestStorage())


@pytest.mark.parametrize("operation", ["prepare", "complete"])
def test_failed_input_transition_rolls_back_all_changes(
    store: SqliteClaimStore, tmp_path: Path, operation: str
) -> None:
    request, intent = inputs(store)
    if operation == "complete":
        store.get_or_create_heartbeat(request, intent)
    before = store.load_state()
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        connection.execute(
            "CREATE TRIGGER reject_marker BEFORE UPDATE ON assignment BEGIN SELECT RAISE(ABORT, 'test failure'); END"
        )

    with pytest.raises(WorkerStateUnavailable):
        if operation == "prepare":
            store.get_or_create_heartbeat(request, intent)
        else:
            store.record_heartbeat(request, intent)

    after = store.load_state()
    assert replace(after, request=before.request) == before
    assert after.request and before.request
    assert after.request.proof.reveal() == before.request.proof.reveal()


@pytest.mark.parametrize("damaged", [False, True])
def test_v4_upgrade_preserves_ciphertext_original_anchors_and_completion_atomically(
    store: SqliteClaimStore, tmp_path: Path, damaged: bool
) -> None:
    state = store.load_state()
    assert state.request and state.assignment and state.completion
    integrity = completion_integrity(
        store._scope, state.request, state.assignment, state.completion
    )
    store.close()
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        header = connection.execute(
            "SELECT scope,prepared,phase,completed FROM metadata"
        ).fetchone()
        connection.execute("DROP TABLE metadata")
        connection.execute("DROP TABLE heartbeat_input")
        connection.execute("DROP TABLE execution_input")
        connection.execute(
            "CREATE TABLE metadata(slot INTEGER PRIMARY KEY,version INTEGER CHECK(version=4),scope BLOB,prepared INTEGER,phase TEXT,completed INTEGER)"
        )
        connection.execute("INSERT INTO metadata VALUES(1,4,?,?,?,?)", header)
        connection.execute("UPDATE assignment SET integrity=?", (integrity,))
        preserved = tuple(
            connection.execute("SELECT * FROM " + table).fetchall()
            for table in (
                "pending_claim",
                "acknowledgement_intent",
                "acknowledgement_completion",
            )
        )
        if damaged:
            connection.execute("CREATE TABLE heartbeat_input(slot INTEGER)")
    if damaged:
        with pytest.raises(WorkerStateUnavailable):
            SqliteClaimStore(tmp_path, store._scope, store._protector, TestStorage())
    else:
        reopened = SqliteClaimStore(
            tmp_path, store._scope, store._protector, TestStorage()
        )
        try:
            restored = reopened.load_state()
            assert replace(restored, request=state.request) == state
            assert restored.request
            assert restored.request.proof.reveal() == state.request.proof.reveal()
        finally:
            reopened.close()

    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        assert connection.execute("SELECT version FROM metadata").fetchone() == (
            (4,) if damaged else (6,)
        )
        assert (
            tuple(
                connection.execute("SELECT * FROM " + table).fetchall()
                for table in (
                    "pending_claim",
                    "acknowledgement_intent",
                    "acknowledgement_completion",
                )
            )
            == preserved
        )
        assert (
            connection.execute("SELECT integrity FROM assignment").fetchone()[0]
            == integrity
            if damaged
            else connection.execute("SELECT integrity FROM assignment").fetchone()[0]
            != integrity
        )


def test_cancelled_completion_recovers_on_sqlite_owner_thread(tmp_path: Path) -> None:
    executor = ThreadPoolExecutor(max_workers=1)
    entered, release = threading.Event(), threading.Event()
    identities: list[int] = []

    class DelayedStore(SqliteClaimStore):
        def record_heartbeat(
            self, request: WorkerClaimRequest, intent: WorkerHeartbeatIntent
        ) -> WorkerRecoveryState:
            identities.append(threading.get_ident())
            entered.set()
            if not release.wait(5):
                raise WorkerStateUnavailable("Test timeout.")
            return super().record_heartbeat(request, intent)

    def create() -> DelayedStore:
        original = acknowledged_store(tmp_path)
        original.close()
        return DelayedStore(
            tmp_path, original._scope, original._protector, TestStorage()
        )

    async def exercise() -> None:
        loop = asyncio.get_running_loop()
        local = await loop.run_in_executor(executor, create)
        journal = AsyncClaimJournal(local, executor)
        try:
            request, intent = await loop.run_in_executor(executor, inputs, local)
            await journal.get_or_create_heartbeat(request, intent)
            task = asyncio.create_task(journal.record_heartbeat(request, intent))
            assert await asyncio.to_thread(entered.wait, 5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            release.set()
            recovered = await journal.load_state()

            assert (
                recovered.pending_heartbeat is None
                and recovered.last_heartbeat == intent
            )
            assert identities and identities[0] != threading.get_ident()
            journal.stop_admission()
            with pytest.raises(WorkerStateUnavailable):
                await journal.get_or_create_heartbeat(request, intent)
            with pytest.raises(WorkerStateUnavailable):
                await journal.record_heartbeat(request, intent)
        finally:
            release.set()
            await loop.run_in_executor(executor, local.close)

    try:
        asyncio.run(exercise())
    finally:
        release.set()
        executor.shutdown(wait=True)


@pytest.mark.skipif(
    sys.platform != "win32", reason="Native Windows DPAPI and private ACLs."
)
@pytest.mark.parametrize("completed", [False, True])
def test_native_windows_bootstrap_recovers_heartbeat_with_protected_original_proof(
    tmp_path: Path, completed: bool
) -> None:
    _, delivery = acknowledgement_inputs()
    settings = WorkerSettings(
        control_url="https://control.example/",
        auth_token=SecretStr(TOKEN),
        workspace_root=tmp_path / "workspace",
    )
    directory = tmp_path / "private-state"

    async def prepare() -> tuple[WorkerHeartbeatIntent, str]:
        async with open_worker_journal(
            settings, worker_id=delivery.outcome.worker_id, state_directory=directory
        ) as worker:
            request = await worker.prepare()
            outcome = replace(
                delivery.outcome, claim_request_id=request.claim_request_id
            )
            received = replace(delivery, outcome=outcome)
            await worker.record_outcome(request, outcome)
            acknowledgement = await worker.prepare_acknowledgement(received)
            await worker.record_acknowledgement(
                request,
                AcknowledgementCompletion(
                    intent=acknowledgement,
                    acknowledged_at=outcome.recorded_at + timedelta(seconds=1),
                ),
            )
            candidate = WorkerHeartbeatIntent(
                heartbeat_request_id=uuid4(),
                claim_request_id=request.claim_request_id,
                attempt_id=acknowledgement.attempt_id,
                generation=1,
                expected_lease_revision=1,
            )
            intent = await worker.prepare_heartbeat(request, candidate)
            if completed:
                await worker.record_heartbeat(request, intent)
            return intent, request.proof.reveal()

    async def recover() -> WorkerRecoveryState:
        async with open_worker_journal(
            settings, worker_id=delivery.outcome.worker_id, state_directory=directory
        ) as worker:
            return await worker.recover()

    intent, proof = asyncio.run(prepare())

    state = asyncio.run(recover())

    assert state.phase is WorkerRecoveryPhase.ACKNOWLEDGED
    assert state.pending_heartbeat == (None if completed else intent)
    assert state.last_heartbeat == (intent if completed else None)
    assert state.request and state.request.proof.reveal() == proof
    assert all(proof.encode() not in file.read_bytes() for file in directory.iterdir())
