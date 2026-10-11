import asyncio
import sqlite3
import sys
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import SecretStr

from devrelay.application.contracts.acknowledgements import WorkerAcknowledgementIntent
from devrelay.application.contracts.assignments import NoWorkReason
from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    LeaseProof,
    WorkerClaimRequest,
)
from devrelay.application.contracts.worker_recovery import (
    WorkerClaimOutcome,
    WorkerRecoveryPhase,
    WorkerRecoveryState,
)
from devrelay.application.contracts.workers import WorkerJournalScope
from devrelay.application.ports.worker_journal import WorkerStateUnavailable
from devrelay.bootstrap.worker import open_worker_journal
from devrelay.config.worker import WorkerSettings
from devrelay.infrastructure.worker_state.journal import (
    AsyncClaimJournal,
    SqliteClaimStore,
)
from tests.acknowledgement_fixtures import (
    acknowledgement_inputs,
    acknowledgement_intent,
    changed_intent,
)
from tests.integration.test_worker_journal import TOKEN, TestProtector, TestStorage

type AssignedJournal = tuple[
    SqliteClaimStore,
    WorkerClaimRequest,
    WorkerClaimOutcome,
    WorkerAcknowledgementIntent,
    WorkerJournalScope,
    TestProtector,
]


@pytest.fixture
def assigned(
    tmp_path: Path,
) -> Iterator[
    tuple[
        SqliteClaimStore,
        WorkerClaimRequest,
        WorkerClaimOutcome,
        WorkerAcknowledgementIntent,
        WorkerJournalScope,
        TestProtector,
    ]
]:
    request, delivery = acknowledgement_inputs()
    scope = WorkerJournalScope(
        worker_id=delivery.outcome.worker_id, control_url="https://control.example/"
    )
    protector = TestProtector()
    store = SqliteClaimStore(tmp_path, scope, protector, TestStorage())
    store.get_or_create_pending(request)
    store.record_outcome(request, delivery.outcome)
    try:
        yield (
            store,
            request,
            delivery.outcome,
            acknowledgement_intent(delivery),
            scope,
            protector,
        )
    finally:
        store.close()


def test_pinned_intent_survives_reopen_with_original_proof_and_assignment(
    assigned: AssignedJournal, tmp_path: Path
) -> None:
    store, request, outcome, intent, scope, protector = assigned
    store.get_or_create_acknowledgement(request, intent)
    store.close()
    reopened = SqliteClaimStore(tmp_path, scope, protector, TestStorage())
    try:
        state = reopened.load_state()

        assert state.phase is WorkerRecoveryPhase.ACKNOWLEDGEMENT_PENDING
        assert state.acknowledgement == intent and state.assignment == outcome
        assert state.request is not None
        assert state.request.claim_request_id == request.claim_request_id
        assert state.request.proof.reveal() == request.proof.reveal()
        assert request.proof.reveal() not in repr(state)
        assert all(
            request.proof.reveal().encode() not in file.read_bytes()
            for file in tmp_path.iterdir()
        )
        assert len(protector.blobs) == 1
    finally:
        reopened.close()


def test_fresh_candidate_id_cannot_replace_existing_pins(
    assigned: AssignedJournal,
) -> None:
    store, request, outcome, intent, scope, protector = assigned
    store.get_or_create_acknowledgement(request, intent)
    candidate = replace(intent, acknowledgement_request_id=uuid4())

    repeated = store.get_or_create_acknowledgement(request, candidate)

    assert (
        repeated == intent
        and repeated.acknowledgement_request_id != candidate.acknowledgement_request_id
    )
    assert store.load_state().acknowledgement == intent


@pytest.mark.parametrize(
    "field",
    [
        "claim_request_id",
        "attempt_id",
        "snapshot_id",
        "base_revision",
        "definition_digest",
    ],
)
def test_changed_candidate_is_rejected_without_repins(
    assigned: AssignedJournal, field: str
) -> None:
    store, request, outcome, intent, scope, protector = assigned
    store.get_or_create_acknowledgement(request, intent)
    candidate = changed_intent(intent, field)

    with pytest.raises(ClaimReplayConflict):
        store.get_or_create_acknowledgement(request, candidate)

    assert store.load_state().acknowledgement == intent


def test_wrong_original_proof_cannot_prepare_intent(assigned: AssignedJournal) -> None:
    store, request, outcome, intent, scope, protector = assigned
    changed = WorkerClaimRequest(
        claim_request_id=request.claim_request_id, proof=LeaseProof.generate()
    )

    with pytest.raises(ClaimReplayConflict):
        store.get_or_create_acknowledgement(changed, intent)

    assert store.load_state().phase is WorkerRecoveryPhase.ASSIGNED
    assert store.load_state().acknowledgement is None


def test_claim_replay_preserves_pending_acknowledgement(
    assigned: AssignedJournal,
) -> None:
    store, request, outcome, intent, scope, protector = assigned
    store.get_or_create_acknowledgement(request, intent)

    replayed = store.record_outcome(request, outcome)

    assert replayed.phase is WorkerRecoveryPhase.ACKNOWLEDGEMENT_PENDING
    assert replayed.acknowledgement == intent and replayed.assignment == outcome


def test_no_work_response_cannot_clear_pending_acknowledgement(
    assigned: AssignedJournal,
) -> None:
    store, request, outcome, intent, scope, protector = assigned
    store.get_or_create_acknowledgement(request, intent)
    no_work = WorkerClaimOutcome(
        worker_id=scope.worker_id,
        claim_request_id=request.claim_request_id,
        recorded_at=outcome.recorded_at,
        no_work=NoWorkReason.EMPTY,
    )

    with pytest.raises(ClaimReplayConflict):
        store.record_outcome(request, no_work)

    assert store.load_state().acknowledgement == intent


def test_pending_acknowledgement_keeps_original_claim_for_new_poll(
    assigned: AssignedJournal,
) -> None:
    store, request, outcome, intent, scope, protector = assigned
    store.get_or_create_acknowledgement(request, intent)
    candidate = WorkerClaimRequest(
        claim_request_id=uuid4(), proof=LeaseProof.generate()
    )

    pending = store.get_or_create_pending(candidate)

    assert pending.claim_request_id == request.claim_request_id
    assert (
        pending.proof.reveal() == request.proof.reveal() and len(protector.blobs) == 1
    )
    assert store.load_state().acknowledgement == intent


@pytest.mark.parametrize(
    "damage",
    [
        "DELETE FROM acknowledgement_intent",
        "UPDATE acknowledgement_intent SET request_id='33333333-3333-3333-3333-333333333333'",
        "UPDATE acknowledgement_intent SET claim_request_id='33333333-3333-3333-3333-333333333333'",
        "UPDATE acknowledgement_intent SET attempt_id='33333333-3333-3333-3333-333333333333'",
        "UPDATE acknowledgement_intent SET snapshot_id='33333333-3333-3333-3333-333333333333'",
        "UPDATE acknowledgement_intent SET base_revision='bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'",
        "UPDATE acknowledgement_intent SET definition_digest=printf('%064d', 1)",
        "UPDATE acknowledgement_intent SET integrity=zeroblob(32)",
        "UPDATE metadata SET phase='assigned'",
        "DELETE FROM pending_claim",
        "DELETE FROM acknowledgement_intent; UPDATE metadata SET phase='assigned'",
    ],
)
def test_corrupt_intent_fails_closed_on_reopen(
    assigned: AssignedJournal, tmp_path: Path, damage: str
) -> None:
    store, request, outcome, intent, scope, protector = assigned
    store.get_or_create_acknowledgement(request, intent)
    store.close()
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        connection.executescript(damage)

    with pytest.raises(WorkerStateUnavailable):
        SqliteClaimStore(tmp_path, scope, protector, TestStorage())

    assert len(protector.blobs) == 1


def test_partial_intent_write_rolls_back_with_assigned_phase(
    assigned: AssignedJournal, tmp_path: Path
) -> None:
    store, request, outcome, intent, scope, protector = assigned
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        connection.execute(
            "CREATE TRIGGER reject_intent BEFORE UPDATE ON metadata WHEN NEW.phase='acknowledgement_pending' BEGIN SELECT RAISE(ABORT, 'test failure'); END"
        )

    with pytest.raises(WorkerStateUnavailable):
        store.get_or_create_acknowledgement(request, intent)

    state = store.load_state()
    assert state.phase is WorkerRecoveryPhase.ASSIGNED and state.acknowledgement is None
    assert state.request is not None
    assert (
        state.assignment == outcome
        and state.request.claim_request_id == request.claim_request_id
    )
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        assert connection.execute(
            "SELECT count(*) FROM acknowledgement_intent"
        ).fetchone() == (0,)


def make_v2(path: Path) -> None:
    with sqlite3.connect(path / "journal.sqlite3") as connection:
        prepared, phase, completed, scope = connection.execute(
            "SELECT prepared, phase, completed, scope FROM metadata"
        ).fetchone()
        connection.execute("DROP TABLE metadata")
        connection.execute("DROP TABLE acknowledgement_intent")
        connection.execute(
            "CREATE TABLE metadata (slot INTEGER PRIMARY KEY CHECK(slot=1), version INTEGER NOT NULL CHECK(version=2), scope BLOB NOT NULL CHECK(length(scope)=32), prepared INTEGER NOT NULL CHECK(prepared IN(0,1)), phase TEXT NOT NULL CHECK(phase IN('idle','pending','assigned')), completed INTEGER NOT NULL CHECK(completed IN(0,1)))"
        )
        connection.execute(
            "INSERT INTO metadata VALUES(1,2,?,?,?,?)",
            (scope, prepared, phase, completed),
        )


@pytest.mark.parametrize("phase", ["idle", "pending", "assigned", "completed"])
def test_v2_upgrade_preserves_valid_recovery_rows(tmp_path: Path, phase: str) -> None:
    request, delivery = acknowledgement_inputs()
    scope = WorkerJournalScope(
        worker_id=delivery.outcome.worker_id, control_url="https://control.example/"
    )
    protector = TestProtector()
    original = SqliteClaimStore(tmp_path, scope, protector, TestStorage())
    if phase != "idle":
        original.get_or_create_pending(request)
    if phase == "assigned":
        original.record_outcome(request, delivery.outcome)
    elif phase == "completed":
        original.record_outcome(
            request,
            WorkerClaimOutcome(
                worker_id=scope.worker_id,
                claim_request_id=request.claim_request_id,
                recorded_at=delivery.outcome.recorded_at,
                no_work=NoWorkReason.EMPTY,
            ),
        )
    expected = original.load_state()
    original.close()
    make_v2(tmp_path)
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        before = tuple(
            connection.execute("SELECT * FROM " + table).fetchall()
            for table in ("pending_claim", "assignment", "completed_no_work")
        )

    upgraded = SqliteClaimStore(tmp_path, scope, protector, TestStorage())

    try:
        state = upgraded.load_state()
        assert state.phase is expected.phase and state.assignment == expected.assignment
        assert (
            state.last_no_work == expected.last_no_work
            and state.acknowledgement is None
        )
        if expected.request:
            assert state.request is not None
            assert state.request.claim_request_id == expected.request.claim_request_id
            assert state.request.proof.reveal() == expected.request.proof.reveal()
        with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
            after = tuple(
                connection.execute("SELECT * FROM " + table).fetchall()
                for table in ("pending_claim", "assignment", "completed_no_work")
            )
            assert after == before
            assert connection.execute("SELECT version FROM metadata").fetchone() == (3,)
    finally:
        upgraded.close()


@pytest.mark.parametrize(
    "damage",
    [
        "UPDATE assignment SET integrity=zeroblob(32)",
        "CREATE TABLE acknowledgement_intent(slot INTEGER)",
    ],
)
def test_failed_v2_upgrade_preserves_old_schema(
    assigned: AssignedJournal, tmp_path: Path, damage: str
) -> None:
    store, request, outcome, intent, scope, protector = assigned
    store.close()
    make_v2(tmp_path)
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        connection.execute(damage)
        before = connection.execute("SELECT * FROM pending_claim").fetchall()

    with pytest.raises(WorkerStateUnavailable):
        SqliteClaimStore(tmp_path, scope, protector, TestStorage())

    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        assert connection.execute("SELECT version FROM metadata").fetchone() == (2,)
        assert connection.execute("SELECT * FROM pending_claim").fetchall() == before


def test_cancelled_wait_recovers_committed_intent_on_owning_thread(
    tmp_path: Path,
) -> None:
    request, delivery = acknowledgement_inputs()
    intent = acknowledgement_intent(delivery)
    scope = WorkerJournalScope(
        worker_id=delivery.outcome.worker_id, control_url="https://control.example/"
    )
    started, release = threading.Event(), threading.Event()
    executor = ThreadPoolExecutor(max_workers=1)
    threads: list[int] = []

    class DelayedStore(SqliteClaimStore):
        def get_or_create_acknowledgement(
            self, request: WorkerClaimRequest, candidate: WorkerAcknowledgementIntent
        ) -> WorkerAcknowledgementIntent:
            threads.append(threading.get_ident())
            started.set()
            if not release.wait(5):
                raise WorkerStateUnavailable("Test timeout.")
            return super().get_or_create_acknowledgement(request, candidate)

    async def exercise() -> None:
        loop = asyncio.get_running_loop()
        owner = threading.get_ident()
        store = await loop.run_in_executor(
            executor,
            lambda: DelayedStore(tmp_path, scope, TestProtector(), TestStorage()),
        )
        journal = AsyncClaimJournal(store, executor)
        try:
            await journal.get_or_create_pending(request)
            await journal.record_outcome(request, delivery.outcome)
            command = asyncio.create_task(
                journal.get_or_create_acknowledgement(request, intent)
            )
            assert await asyncio.to_thread(started.wait, 5)
            command.cancel()
            with pytest.raises(asyncio.CancelledError):
                await command
            release.set()
            state = await journal.load_state()
            assert (
                state.acknowledgement == intent
                and state.phase is WorkerRecoveryPhase.ACKNOWLEDGEMENT_PENDING
            )
            assert threads and all(identity != owner for identity in threads)
            journal.stop_admission()
            with pytest.raises(WorkerStateUnavailable):
                await journal.get_or_create_acknowledgement(request, intent)
        finally:
            release.set()
            await loop.run_in_executor(executor, store.close)

    try:
        asyncio.run(exercise())
    finally:
        release.set()
        executor.shutdown(wait=True)


@pytest.mark.skipif(
    sys.platform != "win32", reason="Native Windows DPAPI and private ACLs."
)
def test_native_windows_bootstrap_recovers_pinned_acknowledgement(
    tmp_path: Path,
) -> None:
    request, delivery = acknowledgement_inputs()
    settings = WorkerSettings(
        control_url="https://control.example/",
        auth_token=SecretStr(TOKEN),
        workspace_root=tmp_path / "workspace",
    )
    directory = tmp_path / "private-state"

    async def prepare() -> tuple[WorkerAcknowledgementIntent, str]:
        async with open_worker_journal(
            settings, worker_id=delivery.outcome.worker_id, state_directory=directory
        ) as worker:
            pending = await worker.prepare()
            response = replace(
                delivery.outcome, claim_request_id=pending.claim_request_id
            )
            received = replace(delivery, outcome=response)
            await worker.record_outcome(pending, response)
            return await worker.prepare_acknowledgement(
                received
            ), pending.proof.reveal()

    intent, proof = asyncio.run(prepare())

    async def recover() -> WorkerRecoveryState:
        async with open_worker_journal(
            settings, worker_id=delivery.outcome.worker_id, state_directory=directory
        ) as worker:
            return await worker.recover()

    state = asyncio.run(recover())
    assert (
        state.acknowledgement == intent
        and state.phase is WorkerRecoveryPhase.ACKNOWLEDGEMENT_PENDING
    )
    assert state.request is not None
    assert state.request.proof.reveal() == proof
    assert all(proof.encode() not in file.read_bytes() for file in directory.iterdir())
