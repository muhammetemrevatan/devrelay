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
from devrelay.application.contracts.executions import (
    ExecutionIsolation,
    ExecutionPolicy,
    ExecutionRuntime,
    WorkerExecutionIntent,
)
from devrelay.application.contracts.worker_recovery import (
    AcknowledgementCompletion,
    WorkerRecoveryPhase,
)
from devrelay.application.contracts.workers import WorkerJournalScope
from devrelay.application.ports.worker_journal import WorkerStateUnavailable
from devrelay.bootstrap.worker import open_worker_journal
from devrelay.config.worker import WorkerSettings
from devrelay.infrastructure.worker_state.heartbeats import heartbeat_integrity
from devrelay.infrastructure.worker_state.journal import (
    AsyncClaimJournal,
    SqliteClaimStore,
)
from tests.acknowledgement_fixtures import (
    acknowledgement_inputs,
    acknowledgement_intent,
    changed_intent,
)
from tests.integration.test_worker_heartbeat_journal import (
    acknowledged_store,
)
from tests.integration.test_worker_heartbeat_journal import (
    inputs as heartbeat_inputs,
)
from tests.integration.test_worker_journal import TOKEN, TestProtector, TestStorage


@pytest.fixture
def store(tmp_path: Path) -> Iterator[SqliteClaimStore]:
    local = acknowledged_store(tmp_path)
    try:
        yield local
    finally:
        local.close()


def inputs(store: SqliteClaimStore) -> tuple[WorkerClaimRequest, WorkerExecutionIntent]:
    state = store.load_state()
    assert state.request and state.acknowledgement
    return state.request, WorkerExecutionIntent(
        execution_id=uuid4(),
        acknowledgement=state.acknowledgement,
        policy=ExecutionPolicy(
            runtime=ExecutionRuntime.CODEX,
            isolation=ExecutionIsolation.WINDOWS_RESTRICTED,
            policy_digest="a" * 64,
        ),
    )


def test_execution_reopens_with_exact_original_identity_policy_and_protected_proof(
    store: SqliteClaimStore, tmp_path: Path
) -> None:
    request, candidate = inputs(store)
    before = store.load_state()
    intent = store.get_or_create_execution(request, candidate)
    store.close()
    reopened = SqliteClaimStore(tmp_path, store._scope, store._protector, TestStorage())
    try:
        state = reopened.load_state()

        assert state.execution == intent == candidate
        assert state.phase is WorkerRecoveryPhase.ACKNOWLEDGED
        assert (
            state.assignment == before.assignment
            and state.completion == before.completion
        )
        assert state.acknowledgement == before.acknowledgement
        assert state.request and state.request.proof.reveal() == request.proof.reveal()
        assert all(
            request.proof.reveal().encode() not in path.read_bytes()
            for path in tmp_path.iterdir()
        )
        with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
            columns = {
                row[1]
                for row in connection.execute("PRAGMA table_info(execution_input)")
            }
        assert columns == {
            "slot",
            "execution_id",
            "runtime",
            "isolation",
            "policy_digest",
        }
    finally:
        reopened.close()


def test_fresh_candidate_uuid_recovers_original_execution_instead_of_repins(
    store: SqliteClaimStore,
) -> None:
    request, original = inputs(store)
    store.get_or_create_execution(request, original)
    candidate = replace(original, execution_id=uuid4())

    recovered = store.get_or_create_execution(request, candidate)

    assert recovered == original and recovered.execution_id != candidate.execution_id


def test_changed_acknowledgement_uuid_cannot_rebind_execution(
    store: SqliteClaimStore,
) -> None:
    request, original = inputs(store)
    store.get_or_create_execution(request, original)
    candidate = replace(
        original,
        acknowledgement=replace(
            original.acknowledgement, acknowledgement_request_id=uuid4()
        ),
    )

    with pytest.raises(ClaimReplayConflict):
        store.get_or_create_execution(request, candidate)

    assert store.load_state().execution == original


@pytest.mark.parametrize(
    "field",
    [
        "claim_request_id",
        "attempt_id",
        "snapshot_id",
        "base_revision",
        "definition_digest",
        "policy",
    ],
)
def test_changed_execution_pins_or_policy_conflict_without_replacement(
    store: SqliteClaimStore, field: str
) -> None:
    request, original = inputs(store)
    store.get_or_create_execution(request, original)
    candidate = (
        replace(original, policy=replace(original.policy, policy_digest="b" * 64))
        if field == "policy"
        else replace(
            original, acknowledgement=changed_intent(original.acknowledgement, field)
        )
    )

    with pytest.raises(ClaimReplayConflict):
        store.get_or_create_execution(request, candidate)

    assert store.load_state().execution == original


@pytest.mark.parametrize("existing", [False, True])
def test_wrong_proof_cannot_prepare_or_recover_execution(
    store: SqliteClaimStore, existing: bool
) -> None:
    request, candidate = inputs(store)
    if existing:
        store.get_or_create_execution(request, candidate)
    changed = replace(request, proof=LeaseProof.generate())

    with pytest.raises(ClaimReplayConflict):
        store.get_or_create_execution(changed, candidate)

    assert store.load_state().execution == (candidate if existing else None)


@pytest.mark.parametrize("execution_first", [True, False])
def test_execution_survives_heartbeat_prepare_completion_and_original_replays(
    store: SqliteClaimStore, execution_first: bool
) -> None:
    request, execution = inputs(store)
    _, heartbeat = heartbeat_inputs(store)
    if execution_first:
        store.get_or_create_execution(request, execution)
    store.get_or_create_heartbeat(request, heartbeat)
    if not execution_first:
        store.get_or_create_execution(request, execution)
    store.record_heartbeat(request, heartbeat)
    newer = replace(heartbeat, heartbeat_request_id=uuid4(), expected_lease_revision=2)
    store.get_or_create_heartbeat(request, newer)
    before = store.load_state()
    assert before.completion and before.assignment
    store.record_outcome(request, before.assignment)
    store.record_acknowledgement(request, before.completion)
    store.record_heartbeat(request, heartbeat)

    recovered = store.get_or_create_execution(
        request, replace(execution, execution_id=uuid4())
    )

    state = store.load_state()
    assert state.execution == recovered == execution
    assert state.pending_heartbeat == newer and state.last_heartbeat == heartbeat
    assert (
        state.assignment == before.assignment and state.completion == before.completion
    )


@pytest.mark.parametrize(
    "damage",
    [
        "DELETE FROM execution_input",
        "UPDATE execution_input SET execution_id='22222222-2222-4222-8222-222222222222'",
        "UPDATE execution_input SET policy_digest='bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'",
        "UPDATE assignment SET integrity=zeroblob(32)",
        "DELETE FROM execution_input; DELETE FROM acknowledgement_completion; UPDATE metadata SET phase='acknowledgement_pending'",
        "DELETE FROM execution_input; DELETE FROM acknowledgement_completion; DELETE FROM acknowledgement_intent; UPDATE metadata SET phase='assigned'",
    ],
)
def test_execution_erasure_mutation_or_phase_regression_fails_closed(
    store: SqliteClaimStore, tmp_path: Path, damage: str
) -> None:
    request, candidate = inputs(store)
    store.get_or_create_execution(request, candidate)
    store.close()
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        connection.executescript(damage)

    with pytest.raises(WorkerStateUnavailable):
        SqliteClaimStore(tmp_path, store._scope, store._protector, TestStorage())


def test_execution_marker_failure_rolls_back_intent_insertion(
    store: SqliteClaimStore, tmp_path: Path
) -> None:
    request, candidate = inputs(store)
    before = store.load_state()
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        connection.execute(
            "CREATE TRIGGER reject_execution BEFORE UPDATE ON assignment BEGIN SELECT RAISE(ABORT, 'test failure'); END"
        )

    with pytest.raises(WorkerStateUnavailable):
        store.get_or_create_execution(request, candidate)

    state = store.load_state()
    assert state.execution is None
    assert (
        state.assignment == before.assignment and state.completion == before.completion
    )


@pytest.mark.parametrize(
    "damage",
    [
        "UPDATE execution_input SET runtime='other'",
        "UPDATE execution_input SET isolation='unrestricted'",
        "UPDATE execution_input SET policy_digest='AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'",
        "UPDATE execution_input SET execution_id='invalid-identity'",
        "UPDATE execution_input SET slot=2",
        "INSERT INTO execution_input SELECT 2,execution_id,runtime,isolation,policy_digest FROM execution_input",
    ],
)
def test_damaged_execution_storage_shape_or_enum_is_classified_and_sanitized(
    store: SqliteClaimStore, tmp_path: Path, damage: str
) -> None:
    request, candidate = inputs(store)
    store.get_or_create_execution(request, candidate)
    store.close()
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        connection.execute("PRAGMA ignore_check_constraints=ON")
        connection.execute(damage)

    with pytest.raises(WorkerStateUnavailable) as error:
        SqliteClaimStore(tmp_path, store._scope, store._protector, TestStorage())

    assert str(error.value) == "Worker recovery journal is unavailable."


@pytest.mark.parametrize(
    "phase", [WorkerRecoveryPhase.ASSIGNED, WorkerRecoveryPhase.ACKNOWLEDGEMENT_PENDING]
)
def test_execution_requires_durable_completed_acknowledgement(
    tmp_path: Path, phase: WorkerRecoveryPhase
) -> None:
    request, delivery = acknowledgement_inputs()
    scope = WorkerJournalScope(
        worker_id=delivery.outcome.worker_id, control_url="https://control.example/"
    )
    local = SqliteClaimStore(tmp_path, scope, TestProtector(), TestStorage())
    local.get_or_create_pending(request)
    local.record_outcome(request, delivery.outcome)
    acknowledgement = acknowledgement_intent(delivery)
    if phase is WorkerRecoveryPhase.ACKNOWLEDGEMENT_PENDING:
        local.get_or_create_acknowledgement(request, acknowledgement)
    candidate = WorkerExecutionIntent(
        execution_id=uuid4(),
        acknowledgement=acknowledgement,
        policy=ExecutionPolicy(
            runtime=ExecutionRuntime.CODEX,
            isolation=ExecutionIsolation.WINDOWS_RESTRICTED,
            policy_digest="a" * 64,
        ),
    )
    try:
        with pytest.raises(ClaimReplayConflict):
            local.get_or_create_execution(request, candidate)

        assert local.load_state().execution is None
        assert local.load_state().phase is phase
    finally:
        local.close()


def make_v5(store: SqliteClaimStore, path: Path) -> None:
    state = store.load_state()
    assert state.request and state.assignment and state.completion
    integrity = heartbeat_integrity(
        store._scope,
        state.request,
        state.assignment,
        state.completion,
        state.pending_heartbeat,
        state.last_heartbeat,
    )
    store.close()
    with sqlite3.connect(path / "journal.sqlite3") as connection:
        row = connection.execute(
            "SELECT scope,prepared,phase,completed FROM metadata"
        ).fetchone()
        connection.execute("DROP TABLE metadata")
        connection.execute("DROP TABLE execution_input")
        connection.execute("DROP TABLE workspace_intent")
        connection.execute("DROP TABLE workspace_completion")
        connection.execute(
            "CREATE TABLE metadata(slot INTEGER PRIMARY KEY,version INTEGER CHECK(version=5),scope BLOB,prepared INTEGER,phase TEXT,completed INTEGER)"
        )
        connection.execute("INSERT INTO metadata VALUES(1,5,?,?,?,?)", row)
        connection.execute("UPDATE assignment SET integrity=?", (integrity,))


@pytest.mark.parametrize("heartbeat_stage", ["none", "pending", "last", "both"])
def test_valid_v5_upgrade_preserves_ciphertext_ack_and_heartbeat_inputs(
    store: SqliteClaimStore, tmp_path: Path, heartbeat_stage: str
) -> None:
    request, heartbeat = heartbeat_inputs(store)
    if heartbeat_stage != "none":
        store.get_or_create_heartbeat(request, heartbeat)
    if heartbeat_stage in ("last", "both"):
        store.record_heartbeat(request, heartbeat)
    if heartbeat_stage == "both":
        store.get_or_create_heartbeat(
            request,
            replace(heartbeat, heartbeat_request_id=uuid4(), expected_lease_revision=2),
        )
    before = store.load_state()
    make_v5(store, tmp_path)
    tables = (
        "pending_claim",
        "acknowledgement_intent",
        "acknowledgement_completion",
        "heartbeat_input",
    )
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        preserved = tuple(
            connection.execute("SELECT * FROM " + table).fetchall() for table in tables
        )

    upgraded = SqliteClaimStore(tmp_path, store._scope, store._protector, TestStorage())

    try:
        state = upgraded.load_state()
        assert replace(state, request=before.request) == before
        assert (
            state.request
            and before.request
            and state.request.proof.reveal() == before.request.proof.reveal()
        )
    finally:
        upgraded.close()
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        assert connection.execute("SELECT version FROM metadata").fetchone() == (7,)
        assert (
            tuple(
                connection.execute("SELECT * FROM " + table).fetchall()
                for table in tables
            )
            == preserved
        )


@pytest.mark.parametrize("damage", ["marker", "existing_table"])
def test_failed_v5_upgrade_preserves_original_version_and_all_rows(
    store: SqliteClaimStore, tmp_path: Path, damage: str
) -> None:
    make_v5(store, tmp_path)
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        if damage == "marker":
            connection.execute("UPDATE assignment SET integrity=zeroblob(32)")
        else:
            connection.execute("CREATE TABLE execution_input(slot INTEGER)")
        original = connection.execute("SELECT * FROM assignment").fetchall()

    with pytest.raises(WorkerStateUnavailable):
        SqliteClaimStore(tmp_path, store._scope, store._protector, TestStorage())

    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        assert connection.execute("SELECT version FROM metadata").fetchone() == (5,)
        assert connection.execute("SELECT * FROM assignment").fetchall() == original


@pytest.mark.parametrize("execution_present", [False, True])
def test_v6_downgrade_cannot_erase_execution_presence_marker(
    store: SqliteClaimStore, tmp_path: Path, execution_present: bool
) -> None:
    request, candidate = inputs(store)
    if execution_present:
        store.get_or_create_execution(request, candidate)
    store.close()
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        row = connection.execute(
            "SELECT scope,prepared,phase,completed FROM metadata"
        ).fetchone()
        connection.execute("DROP TABLE metadata")
        connection.execute("DROP TABLE execution_input")
        connection.execute("DROP TABLE workspace_intent")
        connection.execute("DROP TABLE workspace_completion")
        connection.execute(
            "CREATE TABLE metadata(slot INTEGER PRIMARY KEY,version INTEGER CHECK(version=5),scope BLOB,prepared INTEGER,phase TEXT,completed INTEGER)"
        )
        connection.execute("INSERT INTO metadata VALUES(1,5,?,?,?,?)", row)

    with pytest.raises(WorkerStateUnavailable):
        SqliteClaimStore(tmp_path, store._scope, store._protector, TestStorage())


def test_cancelled_input_commit_recovers_original_execution_on_owner_thread(
    tmp_path: Path,
) -> None:
    executor = ThreadPoolExecutor(max_workers=1)
    entered, release = threading.Event(), threading.Event()
    owner_threads: list[int] = []

    class DelayedStore(SqliteClaimStore):
        def get_or_create_execution(
            self, request: WorkerClaimRequest, candidate: WorkerExecutionIntent
        ) -> WorkerExecutionIntent:
            owner_threads.append(threading.get_ident())
            entered.set()
            if not release.wait(5):
                raise WorkerStateUnavailable("Test timeout.")
            return super().get_or_create_execution(request, candidate)

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
            request, candidate = await loop.run_in_executor(executor, inputs, local)
            task = asyncio.create_task(
                journal.get_or_create_execution(request, candidate)
            )
            assert await asyncio.to_thread(entered.wait, 5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            release.set()
            state = await journal.load_state()

            assert state.execution == candidate
            assert owner_threads[0] != threading.get_ident()
            journal.stop_admission()
            with pytest.raises(WorkerStateUnavailable):
                await journal.get_or_create_execution(request, candidate)
        finally:
            release.set()
            await loop.run_in_executor(executor, local.close)

    try:
        asyncio.run(exercise())
    finally:
        release.set()
        executor.shutdown(wait=True)


@pytest.mark.skipif(
    sys.platform != "win32", reason="Native Windows DPAPI/private ACLs."
)
def test_native_private_journal_reopens_original_execution_without_process_launch(
    tmp_path: Path,
) -> None:
    _, delivery = acknowledgement_inputs()
    settings = WorkerSettings(
        control_url="https://control.example/",
        auth_token=SecretStr(TOKEN),
        workspace_root=tmp_path / "workspace",
    )
    directory = tmp_path / "private-state"

    async def prepare() -> tuple[WorkerExecutionIntent, str]:
        async with open_worker_journal(
            settings, worker_id=delivery.outcome.worker_id, state_directory=directory
        ) as worker:
            request = await worker.prepare()
            outcome = replace(
                delivery.outcome, claim_request_id=request.claim_request_id
            )
            await worker.record_outcome(request, outcome)
            acknowledgement = await worker.prepare_acknowledgement(
                replace(delivery, outcome=outcome)
            )
            await worker.record_acknowledgement(
                request,
                AcknowledgementCompletion(
                    intent=acknowledgement,
                    acknowledged_at=outcome.recorded_at + timedelta(seconds=1),
                ),
            )
            candidate = WorkerExecutionIntent(
                execution_id=uuid4(),
                acknowledgement=acknowledgement,
                policy=ExecutionPolicy(
                    runtime=ExecutionRuntime.CODEX,
                    isolation=ExecutionIsolation.WINDOWS_RESTRICTED,
                    policy_digest="a" * 64,
                ),
            )
            assert worker._execution_journal
            recorded = await worker._execution_journal.get_or_create_execution(
                request, candidate
            )
            return recorded, request.proof.reveal()

    async def recover() -> WorkerExecutionIntent | None:
        async with open_worker_journal(
            settings, worker_id=delivery.outcome.worker_id, state_directory=directory
        ) as worker:
            return (await worker.recover()).execution

    intent, proof = asyncio.run(prepare())

    restored = asyncio.run(recover())

    assert restored == intent
    assert all(proof.encode() not in path.read_bytes() for path in directory.iterdir())
