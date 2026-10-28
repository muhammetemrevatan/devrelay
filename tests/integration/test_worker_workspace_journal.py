import asyncio
import sqlite3
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest

from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    LeaseProof,
    WorkerClaimRequest,
)
from devrelay.application.contracts.workspace_recovery import (
    WorkerWorkspaceCompletion,
    WorkerWorkspaceIntent,
    WorkspaceDirectoryIdentity,
    WorkspaceLocation,
    WorkspacePreparationDecision,
)
from devrelay.application.ports.worker_journal import WorkerStateUnavailable
from devrelay.infrastructure.worker_state.executions import execution_integrity
from devrelay.infrastructure.worker_state.journal import (
    AsyncClaimJournal,
    SqliteClaimStore,
)
from tests.integration.test_worker_execution_journal import inputs as execution_inputs
from tests.integration.test_worker_heartbeat_journal import acknowledged_store
from tests.integration.test_worker_heartbeat_journal import inputs as heartbeat_inputs
from tests.integration.test_worker_journal import TestStorage


@pytest.fixture
def store(tmp_path: Path) -> Iterator[SqliteClaimStore]:
    local = acknowledged_store(tmp_path)
    request, candidate = execution_inputs(local)
    local.get_or_create_execution(request, candidate)
    try:
        yield local
    finally:
        local.close()


def inputs(
    store: SqliteClaimStore,
) -> tuple[WorkerClaimRequest, WorkerWorkspaceIntent, WorkerWorkspaceCompletion]:
    state = store.load_state()
    assert state.request and state.execution
    intent = WorkerWorkspaceIntent(
        workspace_request_id=uuid4(),
        execution=state.execution,
        location=WorkspaceLocation(
            root_directory="C:\\coordinator\\workspaces",
            policy_digest="c" * 64,
            identity=WorkspaceDirectoryIdentity(
                volume_serial_number=42, file_index=2**63 + 17
            ),
        ),
    )
    completion = WorkerWorkspaceCompletion(
        intent=intent,
        identity=WorkspaceDirectoryIdentity(
            volume_serial_number=42, file_index=2**64 - 1
        ),
    )
    return state.request, intent, completion


@pytest.mark.parametrize("completed", [False, True])
def test_workspace_history_reopens_without_new_creation_permission_or_proof_rotation(
    store: SqliteClaimStore, tmp_path: Path, completed: bool
) -> None:
    request, intent, completion = inputs(store)
    original = store.load_state()
    decision = store.get_or_create_workspace(request, intent)
    if completed:
        store.record_workspace_completion(request, completion)
    store.close()
    reopened = SqliteClaimStore(tmp_path, store._scope, store._protector, TestStorage())
    try:
        state = reopened.load_state()
        repeated = reopened.get_or_create_workspace(
            request, replace(intent, workspace_request_id=uuid4())
        )

        assert decision.created and decision.intent == intent
        assert not repeated.created and repeated.intent == intent
        assert state.workspace_intent == intent
        assert state.workspace_completion == (completion if completed else None)
        assert not state.workspace_history_unknown
        assert (
            state.execution == original.execution
            and state.completion == original.completion
        )
        assert (
            state.assignment == original.assignment
            and state.acknowledgement == original.acknowledgement
        )
        assert state.request and state.request.proof.reveal() == request.proof.reveal()
        assert request.proof.reveal() not in repr(state)
        assert intent.location.root_directory not in repr(state)
        assert all(
            request.proof.reveal().encode() not in path.read_bytes()
            for path in tmp_path.iterdir()
        )
        with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
            root_index = connection.execute(
                "SELECT file_index FROM workspace_intent"
            ).fetchone()[0]
        assert root_index == str(intent.location.identity.file_index)
    finally:
        reopened.close()


@pytest.mark.parametrize("field", ["root", "policy", "root_identity", "execution"])
def test_pending_or_completed_workspace_never_repins_root_or_execution(
    store: SqliteClaimStore, field: str
) -> None:
    request, intent, completion = inputs(store)
    store.get_or_create_workspace(request, intent)
    location = intent.location
    if field == "root":
        candidate = replace(
            intent, location=replace(location, root_directory="D:\\other\\workspaces")
        )
    elif field == "policy":
        candidate = replace(intent, location=replace(location, policy_digest="d" * 64))
    elif field == "root_identity":
        candidate = replace(
            intent,
            location=replace(
                location, identity=replace(location.identity, file_index=123)
            ),
        )
    else:
        candidate = replace(
            intent, execution=replace(intent.execution, execution_id=uuid4())
        )

    with pytest.raises(ClaimReplayConflict):
        store.get_or_create_workspace(request, candidate)

    assert store.load_state().workspace_intent == intent


@pytest.mark.parametrize("operation", ["prepare", "complete"])
def test_wrong_proof_cannot_prepare_or_complete_workspace(
    store: SqliteClaimStore, operation: str
) -> None:
    request, intent, completion = inputs(store)
    store.get_or_create_workspace(request, intent)
    wrong = replace(request, proof=LeaseProof.generate())

    with pytest.raises(ClaimReplayConflict):
        if operation == "prepare":
            store.get_or_create_workspace(wrong, intent)
        else:
            store.record_workspace_completion(wrong, completion)

    assert store.load_state().workspace_completion is None


@pytest.mark.parametrize("changed", ["nonce", "identity"])
def test_completion_requires_exact_original_intent_and_immutable_child_identity(
    store: SqliteClaimStore, changed: str
) -> None:
    request, intent, completion = inputs(store)
    store.get_or_create_workspace(request, intent)
    store.record_workspace_completion(request, completion)
    candidate = (
        replace(completion, intent=replace(intent, workspace_request_id=uuid4()))
        if changed == "nonce"
        else replace(completion, identity=replace(completion.identity, file_index=888))
    )

    with pytest.raises(ClaimReplayConflict):
        store.record_workspace_completion(request, candidate)

    assert store.load_state().workspace_completion == completion


def test_completion_without_durable_intent_is_rejected(store: SqliteClaimStore) -> None:
    request, intent, completion = inputs(store)

    with pytest.raises(ClaimReplayConflict):
        store.record_workspace_completion(request, completion)

    assert store.load_state().workspace_intent is None


@pytest.mark.parametrize(
    "table,value",
    [
        ("workspace_intent", "0001"),
        ("workspace_intent", "0"),
        ("workspace_intent", "18446744073709551616"),
        ("workspace_intent", "-1"),
        ("workspace_completion", "0001"),
        ("workspace_completion", "1.5"),
    ],
)
def test_native_file_index_storage_requires_canonical_positive_uint64_text(
    store: SqliteClaimStore, tmp_path: Path, table: str, value: str
) -> None:
    request, intent, completion = inputs(store)
    store.get_or_create_workspace(request, intent)
    store.record_workspace_completion(request, completion)
    store.close()
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        connection.execute("UPDATE " + table + " SET file_index=?", (value,))

    with pytest.raises(WorkerStateUnavailable):
        SqliteClaimStore(tmp_path, store._scope, store._protector, TestStorage())


def test_workspace_preparation_requires_existing_execution_input(
    tmp_path: Path,
) -> None:
    local = acknowledged_store(tmp_path)
    request, execution = execution_inputs(local)
    candidate = WorkerWorkspaceIntent(
        workspace_request_id=uuid4(),
        execution=execution,
        location=WorkspaceLocation(
            root_directory="C:\\coordinator\\workspaces",
            policy_digest="c" * 64,
            identity=WorkspaceDirectoryIdentity(volume_serial_number=42, file_index=17),
        ),
    )
    try:
        with pytest.raises(ClaimReplayConflict):
            local.get_or_create_workspace(request, candidate)

        assert local.load_state().workspace_intent is None
    finally:
        local.close()


def test_all_original_replays_and_heartbeat_transitions_preserve_workspace_history(
    store: SqliteClaimStore,
) -> None:
    request, intent, completion = inputs(store)
    store.get_or_create_workspace(request, intent)
    store.record_workspace_completion(request, completion)
    state = store.load_state()
    assert state.assignment and state.completion and state.execution
    _, heartbeat = heartbeat_inputs(store)
    store.get_or_create_heartbeat(request, heartbeat)
    store.record_heartbeat(request, heartbeat)
    newer = replace(heartbeat, heartbeat_request_id=uuid4(), expected_lease_revision=2)
    store.get_or_create_heartbeat(request, newer)
    store.record_outcome(request, state.assignment)
    store.record_acknowledgement(request, state.completion)
    store.get_or_create_execution(
        request, replace(state.execution, execution_id=uuid4())
    )
    store.record_heartbeat(request, heartbeat)

    repeated = store.record_workspace_completion(request, completion)

    final = store.load_state()
    assert repeated == completion
    assert final.workspace_intent == intent and final.workspace_completion == completion
    assert final.pending_heartbeat == newer and final.last_heartbeat == heartbeat


@pytest.mark.parametrize(
    "damage",
    [
        "DELETE FROM workspace_intent; DELETE FROM workspace_completion",
        "UPDATE workspace_intent SET root_directory='D:\\other\\workspaces'",
        "UPDATE workspace_intent SET policy_digest='dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd'",
        "UPDATE workspace_intent SET file_index='123'",
        "UPDATE workspace_intent SET workspace_request_id='22222222-2222-4222-8222-222222222222'",
        "UPDATE workspace_completion SET file_index='999'",
        "DELETE FROM workspace_completion",
        "DELETE FROM workspace_intent; DELETE FROM workspace_completion; DELETE FROM execution_input",
        "UPDATE metadata SET workspace_history_unknown=1",
    ],
)
def test_workspace_contents_presence_and_history_marker_tampering_fail_closed(
    store: SqliteClaimStore, tmp_path: Path, damage: str
) -> None:
    request, intent, completion = inputs(store)
    store.get_or_create_workspace(request, intent)
    store.record_workspace_completion(request, completion)
    store.close()
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        connection.executescript(damage)

    with pytest.raises(WorkerStateUnavailable):
        SqliteClaimStore(tmp_path, store._scope, store._protector, TestStorage())


@pytest.mark.parametrize("operation", ["prepare", "complete"])
def test_workspace_write_and_marker_failure_roll_back_together(
    store: SqliteClaimStore, tmp_path: Path, operation: str
) -> None:
    request, intent, completion = inputs(store)
    if operation == "complete":
        store.get_or_create_workspace(request, intent)
    before = store.load_state()
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        connection.execute(
            "CREATE TRIGGER reject_workspace_marker BEFORE UPDATE ON assignment BEGIN SELECT RAISE(ABORT, 'test failure'); END"
        )

    with pytest.raises(WorkerStateUnavailable):
        if operation == "prepare":
            store.get_or_create_workspace(request, intent)
        else:
            store.record_workspace_completion(request, completion)

    after = store.load_state()
    assert replace(after, request=before.request) == before


def make_v6(store: SqliteClaimStore, path: Path) -> None:
    state = store.load_state()
    assert state.request and state.assignment and state.completion
    marker = execution_integrity(
        store._scope,
        state.request,
        state.assignment,
        state.completion,
        state.pending_heartbeat,
        state.last_heartbeat,
        state.execution,
    )
    store.close()
    with sqlite3.connect(path / "journal.sqlite3") as connection:
        row = connection.execute(
            "SELECT scope,prepared,phase,completed FROM metadata"
        ).fetchone()
        connection.execute("DROP TABLE coding_result_input")
        connection.execute("DROP TABLE coding_result_receipts")
        connection.execute("DROP TABLE metadata")
        connection.execute("DROP TABLE workspace_intent")
        connection.execute("DROP TABLE workspace_completion")
        connection.execute(
            "CREATE TABLE metadata(slot INTEGER PRIMARY KEY,version INTEGER CHECK(version=6),scope BLOB,prepared INTEGER,phase TEXT,completed INTEGER)"
        )
        connection.execute("INSERT INTO metadata VALUES(1,6,?,?,?,?)", row)
        connection.execute("UPDATE assignment SET integrity=?", (marker,))


@pytest.mark.parametrize("execution_present", [False, True])
def test_v6_upgrade_preserves_all_input_and_marks_legacy_execution_workspace_ambiguous(
    tmp_path: Path, execution_present: bool
) -> None:
    original = acknowledged_store(tmp_path)
    if execution_present:
        request, execution = execution_inputs(original)
        original.get_or_create_execution(request, execution)
    request, heartbeat = heartbeat_inputs(original)
    original.get_or_create_heartbeat(request, heartbeat)
    original.record_heartbeat(request, heartbeat)
    original.get_or_create_heartbeat(
        request,
        replace(heartbeat, heartbeat_request_id=uuid4(), expected_lease_revision=2),
    )
    before = original.load_state()
    make_v6(original, tmp_path)
    tables = (
        "pending_claim",
        "acknowledgement_intent",
        "acknowledgement_completion",
        "execution_input",
        "heartbeat_input",
    )
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        preserved = tuple(
            connection.execute("SELECT * FROM " + table).fetchall() for table in tables
        )

    upgraded = SqliteClaimStore(
        tmp_path, original._scope, original._protector, TestStorage()
    )

    try:
        state = upgraded.load_state()
        assert (
            replace(state, request=before.request, workspace_history_unknown=False)
            == before
        )
        assert state.workspace_history_unknown is execution_present
        if execution_present:
            _, candidate, completion = inputs(upgraded)
            with pytest.raises(ClaimReplayConflict):
                upgraded.get_or_create_workspace(request, candidate)
    finally:
        upgraded.close()
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        assert connection.execute("SELECT version FROM metadata").fetchone() == (8,)
        assert (
            tuple(
                connection.execute("SELECT * FROM " + table).fetchall()
                for table in tables
            )
            == preserved
        )


def test_legacy_unknown_workspace_flag_cannot_be_cleared_to_allow_new_creation(
    store: SqliteClaimStore, tmp_path: Path
) -> None:
    make_v6(store, tmp_path)
    upgraded = SqliteClaimStore(tmp_path, store._scope, store._protector, TestStorage())
    assert upgraded.load_state().workspace_history_unknown
    upgraded.close()
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        connection.execute("UPDATE metadata SET workspace_history_unknown=0")

    with pytest.raises(WorkerStateUnavailable):
        SqliteClaimStore(tmp_path, store._scope, store._protector, TestStorage())


def test_unknown_workspace_history_survives_heartbeat_and_execution_replays(
    store: SqliteClaimStore, tmp_path: Path
) -> None:
    make_v6(store, tmp_path)
    upgraded = SqliteClaimStore(tmp_path, store._scope, store._protector, TestStorage())
    try:
        request, heartbeat = heartbeat_inputs(upgraded)
        state = upgraded.load_state()
        assert state.execution
        upgraded.get_or_create_heartbeat(request, heartbeat)
        upgraded.record_heartbeat(request, heartbeat)
        upgraded.get_or_create_execution(
            request, replace(state.execution, execution_id=uuid4())
        )

        result = upgraded.load_state()

        assert result.workspace_history_unknown
        assert result.workspace_intent is None and result.workspace_completion is None
    finally:
        upgraded.close()


def test_failed_v6_upgrade_rolls_back_version_original_rows_and_partial_schema(
    store: SqliteClaimStore, tmp_path: Path
) -> None:
    make_v6(store, tmp_path)
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        before = connection.execute("SELECT * FROM assignment").fetchall()
        connection.execute("CREATE TABLE workspace_completion(slot INTEGER)")

    with pytest.raises(WorkerStateUnavailable):
        SqliteClaimStore(tmp_path, store._scope, store._protector, TestStorage())

    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        assert connection.execute("SELECT version FROM metadata").fetchone() == (6,)
        assert connection.execute("SELECT * FROM assignment").fetchall() == before
        assert (
            connection.execute(
                "SELECT name FROM sqlite_master WHERE name='workspace_intent'"
            ).fetchone()
            is None
        )


def test_v7_downgrade_and_workspace_erasure_cannot_regain_creation_permission(
    store: SqliteClaimStore, tmp_path: Path
) -> None:
    request, intent, completion = inputs(store)
    store.get_or_create_workspace(request, intent)
    store.close()
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        row = connection.execute(
            "SELECT scope,prepared,phase,completed FROM metadata"
        ).fetchone()
        connection.execute("DROP TABLE coding_result_input")
        connection.execute("DROP TABLE coding_result_receipts")
        connection.execute("DROP TABLE metadata")
        connection.execute("DROP TABLE workspace_intent")
        connection.execute("DROP TABLE workspace_completion")
        connection.execute(
            "CREATE TABLE metadata(slot INTEGER PRIMARY KEY,version INTEGER CHECK(version=6),scope BLOB,prepared INTEGER,phase TEXT,completed INTEGER)"
        )
        connection.execute("INSERT INTO metadata VALUES(1,6,?,?,?,?)", row)

    with pytest.raises(WorkerStateUnavailable):
        SqliteClaimStore(tmp_path, store._scope, store._protector, TestStorage())


@pytest.mark.parametrize("operation", ["prepare", "complete"])
def test_cancelled_workspace_wait_recovers_committed_input_on_owning_thread(
    tmp_path: Path, operation: str
) -> None:
    executor = ThreadPoolExecutor(max_workers=1)
    entered, release = threading.Event(), threading.Event()
    owner_threads: list[int] = []

    class DelayedStore(SqliteClaimStore):
        def block(self) -> None:
            owner_threads.append(threading.get_ident())
            entered.set()
            if not release.wait(5):
                raise WorkerStateUnavailable("Fixture timeout.")

        def get_or_create_workspace(
            self, request: WorkerClaimRequest, candidate: WorkerWorkspaceIntent
        ) -> WorkspacePreparationDecision:
            if operation == "prepare":
                self.block()
            return super().get_or_create_workspace(request, candidate)

        def record_workspace_completion(
            self, request: WorkerClaimRequest, completion: WorkerWorkspaceCompletion
        ) -> WorkerWorkspaceCompletion:
            if operation == "complete":
                self.block()
            return super().record_workspace_completion(request, completion)

    def create() -> DelayedStore:
        original = acknowledged_store(tmp_path)
        request, candidate = execution_inputs(original)
        original.get_or_create_execution(request, candidate)
        original.close()
        return DelayedStore(
            tmp_path, original._scope, original._protector, TestStorage()
        )

    async def exercise() -> None:
        loop = asyncio.get_running_loop()
        local = await loop.run_in_executor(executor, create)
        journal = AsyncClaimJournal(local, executor)
        try:
            request, intent, completion = await loop.run_in_executor(
                executor, inputs, local
            )
            if operation == "complete":
                await journal.get_or_create_workspace(request, intent)
            task = (
                asyncio.create_task(journal.get_or_create_workspace(request, intent))
                if operation == "prepare"
                else asyncio.create_task(
                    journal.record_workspace_completion(request, completion)
                )
            )
            assert await asyncio.to_thread(entered.wait, 5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            release.set()
            recovered = await journal.load_state()

            assert recovered.workspace_intent == intent
            assert recovered.workspace_completion == (
                completion if operation == "complete" else None
            )
            assert owner_threads[0] != threading.get_ident()
            journal.stop_admission()
            with pytest.raises(WorkerStateUnavailable):
                await journal.get_or_create_workspace(request, intent)
            with pytest.raises(WorkerStateUnavailable):
                await journal.record_workspace_completion(request, completion)
        finally:
            release.set()
            await loop.run_in_executor(executor, local.close)

    try:
        asyncio.run(exercise())
    finally:
        release.set()
        executor.shutdown(wait=True)
