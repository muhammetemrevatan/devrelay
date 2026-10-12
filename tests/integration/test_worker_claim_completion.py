import asyncio
import sqlite3
import sys
import threading
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractAsyncContextManager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncEngine

from devrelay.application.contracts.assignments import NoWorkReason
from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    LeaseProof,
    LeaseProofVerifier,
    WorkerClaimRequest,
)
from devrelay.application.contracts.eligibility import PreparedJobClaim
from devrelay.application.contracts.worker_recovery import (
    AssignmentReference,
    WorkerClaimOutcome,
    WorkerRecoveryPhase,
    WorkerRecoveryState,
)
from devrelay.application.contracts.workers import (
    WorkerAuthorization,
    WorkerJournalScope,
)
from devrelay.application.ports.worker_journal import WorkerStateUnavailable
from devrelay.application.worker_claims import PrepareWorkerClaim
from devrelay.bootstrap.persistence import claim_store
from devrelay.bootstrap.worker import open_worker_journal
from devrelay.config.worker import WorkerSettings
from devrelay.infrastructure.worker_state.journal import (
    AsyncClaimJournal,
    SqliteClaimStore,
)
from tests.integration.job_fixtures import save_job
from tests.integration.test_worker_journal import TOKEN, TestProtector, TestStorage

NOW = datetime(2026, 10, 11, tzinfo=UTC)


@pytest.fixture
def scope() -> WorkerJournalScope:
    return WorkerJournalScope(worker_id=uuid4(), control_url="https://control.example/")


@pytest.fixture
def pending() -> WorkerClaimRequest:
    return WorkerClaimRequest(claim_request_id=uuid4(), proof=LeaseProof(TOKEN))


@pytest.fixture
def protector() -> TestProtector:
    return TestProtector()


@pytest.fixture
def store(
    tmp_path: Path, scope: WorkerJournalScope, protector: TestProtector
) -> Iterator[SqliteClaimStore]:
    value = SqliteClaimStore(tmp_path, scope, protector, TestStorage())
    try:
        yield value
    finally:
        value.close()


def outcome(
    scope: WorkerJournalScope, request: WorkerClaimRequest, *, assigned: bool = True
) -> WorkerClaimOutcome:
    reference = (
        AssignmentReference(
            worker_id=scope.worker_id,
            job_id=uuid4(),
            repository_id=uuid4(),
            attempt_id=uuid4(),
            generation=1,
            issued_at=NOW,
            expires_at=NOW + timedelta(seconds=120),
        )
        if assigned
        else None
    )
    return WorkerClaimOutcome(
        worker_id=scope.worker_id,
        claim_request_id=request.claim_request_id,
        recorded_at=NOW,
        assignment=reference,
        no_work=None if assigned else NoWorkReason.EMPTY,
    )


@pytest.mark.parametrize("assigned", [True, False])
def test_committed_result_survives_reopen(
    store: SqliteClaimStore,
    scope: WorkerJournalScope,
    pending: WorkerClaimRequest,
    protector: TestProtector,
    tmp_path: Path,
    assigned: bool,
) -> None:
    store.get_or_create_pending(pending)
    response = outcome(scope, pending, assigned=assigned)
    store.record_outcome(pending, response)
    store.close()
    reopened = SqliteClaimStore(tmp_path, scope, protector, TestStorage())
    try:
        recovered = reopened.load_state()

        assert recovered.phase is (
            WorkerRecoveryPhase.ASSIGNED if assigned else WorkerRecoveryPhase.IDLE
        )
        if assigned:
            assert recovered.assignment == response and recovered.request is not None
            assert recovered.request.proof.reveal() == pending.proof.reveal()
            assert (
                reopened.get_or_create_pending(
                    WorkerClaimRequest(
                        claim_request_id=uuid4(), proof=LeaseProof.generate()
                    )
                ).claim_request_id
                == pending.claim_request_id
            )
        else:
            assert recovered.request is None and recovered.last_no_work == response
        assert TOKEN not in repr(recovered)
        assert TOKEN.encode() not in (tmp_path / "journal.sqlite3").read_bytes()
        assert all(
            TOKEN.encode() not in file.read_bytes()
            for file in tmp_path.glob("journal.sqlite3-*")
        )
    finally:
        reopened.close()


@pytest.mark.parametrize("assigned", [True, False])
def test_duplicate_result_is_idempotent(
    store: SqliteClaimStore,
    scope: WorkerJournalScope,
    pending: WorkerClaimRequest,
    assigned: bool,
) -> None:
    store.get_or_create_pending(pending)
    response = outcome(scope, pending, assigned=assigned)
    original = store.record_outcome(pending, response)

    repeated = store.record_outcome(pending, response)

    assert (
        repeated.assignment == original.assignment
        and repeated.last_no_work == original.last_no_work
    )
    assert repeated.phase is original.phase


def test_completed_reply_does_not_clear_a_new_pending_request(
    store: SqliteClaimStore, scope: WorkerJournalScope, pending: WorkerClaimRequest
) -> None:
    store.get_or_create_pending(pending)
    response = outcome(scope, pending, assigned=False)
    store.record_outcome(pending, response)
    new = WorkerClaimRequest(claim_request_id=uuid4(), proof=LeaseProof.generate())
    store.get_or_create_pending(new)

    repeated = store.record_outcome(pending, response)

    assert (
        repeated.phase is WorkerRecoveryPhase.PENDING and repeated.request is not None
    )
    assert repeated.request.claim_request_id == new.claim_request_id
    assert repeated.request.proof.reveal() == new.proof.reveal()


@pytest.mark.parametrize("change", ["worker", "request", "proof", "unknown"])
def test_unrelated_reply_cannot_change_recovery_state(
    store: SqliteClaimStore,
    scope: WorkerJournalScope,
    pending: WorkerClaimRequest,
    change: str,
) -> None:
    store.get_or_create_pending(pending)
    response = outcome(scope, pending, assigned=False)
    presented = pending
    if change == "worker":
        response = replace(response, worker_id=uuid4())
    elif change == "request":
        response = replace(response, claim_request_id=uuid4())
    elif change == "proof":
        presented = WorkerClaimRequest(
            claim_request_id=pending.claim_request_id, proof=LeaseProof.generate()
        )
    else:
        presented = WorkerClaimRequest(
            claim_request_id=uuid4(), proof=LeaseProof.generate()
        )
        response = outcome(scope, presented, assigned=False)

    with pytest.raises(ClaimReplayConflict):
        store.record_outcome(presented, response)

    state = store.load_state()
    assert state.phase is WorkerRecoveryPhase.PENDING and state.request is not None
    assert state.request.claim_request_id == pending.claim_request_id


@pytest.mark.parametrize("change", ["job", "attempt", "expiry", "no_work"])
def test_recorded_assignment_cannot_be_replaced_or_renewed(
    store: SqliteClaimStore,
    scope: WorkerJournalScope,
    pending: WorkerClaimRequest,
    change: str,
) -> None:
    store.get_or_create_pending(pending)
    accepted = outcome(scope, pending)
    store.record_outcome(pending, accepted)
    assert accepted.assignment is not None
    reference = accepted.assignment
    if change == "job":
        changed = replace(accepted, assignment=replace(reference, job_id=uuid4()))
    elif change == "attempt":
        changed = replace(accepted, assignment=replace(reference, attempt_id=uuid4()))
    elif change == "expiry":
        changed = replace(
            accepted,
            assignment=replace(
                reference, expires_at=reference.expires_at + timedelta(seconds=1)
            ),
        )
    else:
        changed = outcome(scope, pending, assigned=False)

    with pytest.raises(ClaimReplayConflict):
        store.record_outcome(pending, changed)

    assert store.load_state().assignment == accepted


@pytest.mark.parametrize("assigned", [True, False])
def test_failed_result_commit_preserves_pending_request(
    store: SqliteClaimStore,
    scope: WorkerJournalScope,
    pending: WorkerClaimRequest,
    tmp_path: Path,
    assigned: bool,
) -> None:
    store.get_or_create_pending(pending)
    response = outcome(scope, pending, assigned=assigned)
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        if assigned:
            connection.execute(
                "CREATE TRIGGER fail_result BEFORE INSERT ON assignment BEGIN SELECT RAISE(ABORT, 'Test private failure.'); END"
            )
        else:
            connection.execute(
                "CREATE TRIGGER fail_result BEFORE DELETE ON pending_claim BEGIN SELECT RAISE(ABORT, 'Test private failure.'); END"
            )

    with pytest.raises(WorkerStateUnavailable) as raised:
        store.record_outcome(pending, response)

    assert "private" not in str(raised.value)
    recovered = store.load_state()
    assert (
        recovered.phase is WorkerRecoveryPhase.PENDING and recovered.request is not None
    )
    assert recovered.request.proof.reveal() == pending.proof.reveal()
    assert recovered.last_no_work is None and recovered.assignment is None


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM assignment",
        "DELETE FROM pending_claim",
        "DELETE FROM metadata",
        "UPDATE assignment SET job_id = '11111111-1111-4111-8111-111111111111'",
        "UPDATE assignment SET integrity = zeroblob(32)",
        "UPDATE assignment SET expires_at = '2026-10-11T01:00:00.000000+00:00'",
    ],
)
def test_missing_or_modified_assignment_fails_closed(
    store: SqliteClaimStore,
    scope: WorkerJournalScope,
    pending: WorkerClaimRequest,
    tmp_path: Path,
    sql: str,
) -> None:
    store.get_or_create_pending(pending)
    store.record_outcome(pending, outcome(scope, pending))
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        connection.execute(sql)

    with pytest.raises(WorkerStateUnavailable):
        store.load_state()


def test_missing_completed_marker_fails_closed(
    store: SqliteClaimStore,
    scope: WorkerJournalScope,
    pending: WorkerClaimRequest,
    tmp_path: Path,
) -> None:
    store.get_or_create_pending(pending)
    store.record_outcome(pending, outcome(scope, pending, assigned=False))
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        connection.execute("DELETE FROM completed_no_work")

    with pytest.raises(WorkerStateUnavailable):
        store.load_state()


def test_completed_identity_is_not_prepared_again(
    store: SqliteClaimStore, scope: WorkerJournalScope, pending: WorkerClaimRequest
) -> None:
    store.get_or_create_pending(pending)
    store.record_outcome(pending, outcome(scope, pending, assigned=False))

    with pytest.raises(ClaimReplayConflict):
        store.get_or_create_pending(pending)


def test_repeated_idle_cycles_keep_storage_bounded(
    store: SqliteClaimStore, scope: WorkerJournalScope, tmp_path: Path
) -> None:
    for _ in range(200):
        current = WorkerClaimRequest(
            claim_request_id=uuid4(), proof=LeaseProof.generate()
        )
        store.get_or_create_pending(current)
        store.record_outcome(current, outcome(scope, current, assigned=False))

    state = store.load_state()

    assert state.phase is WorkerRecoveryPhase.IDLE
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        assert connection.execute(
            "SELECT count(*) FROM completed_no_work"
        ).fetchone() == (1,)
        assert connection.execute("SELECT count(*) FROM pending_claim").fetchone() == (
            0,
        )
    assert all(file.stat().st_size < 1024 * 1024 for file in tmp_path.iterdir())


def create_v1(
    tmp_path: Path,
    scope: WorkerJournalScope,
    protector: TestProtector,
    pending: WorkerClaimRequest | None,
) -> tuple[bytes | None, bytes | None]:
    ciphertext = digest = None
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        connection.execute(
            "CREATE TABLE metadata (slot INTEGER PRIMARY KEY CHECK(slot=1), version INTEGER NOT NULL CHECK(version=1), scope BLOB NOT NULL, prepared INTEGER NOT NULL CHECK(prepared IN(0,1)))"
        )
        connection.execute(
            "INSERT INTO metadata VALUES (1,1,?,?)",
            (scope.key, int(pending is not None)),
        )
        connection.execute(
            "CREATE TABLE pending_claim (slot INTEGER PRIMARY KEY CHECK(slot=1), request_id TEXT NOT NULL, protocol_version INTEGER NOT NULL, ciphertext BLOB NOT NULL, verifier BLOB NOT NULL)"
        )
        if pending is not None:
            context = sha256(
                b"devrelay.pending-proof.v1\x00"
                + scope.key
                + pending.claim_request_id.bytes
            ).digest()
            ciphertext = protector.protect(pending.proof, context=context)
            digest = LeaseProofVerifier.from_proof(
                worker_id=scope.worker_id,
                claim_request_id=pending.claim_request_id,
                proof=pending.proof,
            ).digest
            connection.execute(
                "INSERT INTO pending_claim VALUES (1,?,1,?,?)",
                (str(pending.claim_request_id), ciphertext, digest),
            )
    return ciphertext, digest


@pytest.mark.parametrize("prepared", [True, False])
def test_v1_upgrade_preserves_original_proof_and_identity(
    tmp_path: Path,
    scope: WorkerJournalScope,
    protector: TestProtector,
    pending: WorkerClaimRequest,
    prepared: bool,
) -> None:
    original = create_v1(tmp_path, scope, protector, pending if prepared else None)
    migrated = SqliteClaimStore(tmp_path, scope, protector, TestStorage())
    try:
        state = migrated.load_state()

        assert state.phase is (
            WorkerRecoveryPhase.PENDING if prepared else WorkerRecoveryPhase.IDLE
        )
        if prepared:
            assert (
                state.request is not None
                and state.request.claim_request_id == pending.claim_request_id
            )
            assert state.request.proof.reveal() == pending.proof.reveal()
        with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
            assert connection.execute("SELECT version FROM metadata").fetchone() == (5,)
            row = connection.execute(
                "SELECT ciphertext, verifier FROM pending_claim"
            ).fetchone()
            assert row == original if prepared else row is None
    finally:
        migrated.close()


def test_damaged_v1_is_not_upgraded_as_empty(
    tmp_path: Path,
    scope: WorkerJournalScope,
    protector: TestProtector,
    pending: WorkerClaimRequest,
) -> None:
    create_v1(tmp_path, scope, protector, pending)
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        connection.execute("DELETE FROM pending_claim")

    with pytest.raises(WorkerStateUnavailable):
        SqliteClaimStore(tmp_path, scope, protector, TestStorage())

    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        assert connection.execute("SELECT version FROM metadata").fetchone() == (1,)


def test_async_cancelled_result_write_can_be_recovered(
    tmp_path: Path,
    scope: WorkerJournalScope,
    protector: TestProtector,
    pending: WorkerClaimRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered, release = threading.Event(), threading.Event()
    original = SqliteClaimStore.record_outcome
    response = outcome(scope, pending)

    def delayed(
        self: SqliteClaimStore, request: WorkerClaimRequest, result: WorkerClaimOutcome
    ) -> WorkerRecoveryState:
        entered.set()
        if not release.wait(5):
            raise WorkerStateUnavailable("Test write timed out.")
        return original(self, request, result)

    monkeypatch.setattr(SqliteClaimStore, "record_outcome", delayed)

    async def scenario() -> None:
        with ThreadPoolExecutor(max_workers=1) as executor:
            loop = asyncio.get_running_loop()
            value = await loop.run_in_executor(
                executor, SqliteClaimStore, tmp_path, scope, protector, TestStorage()
            )
            journal = AsyncClaimJournal(value, executor)
            try:
                await journal.get_or_create_pending(pending)
                task = asyncio.create_task(journal.record_outcome(pending, response))
                assert await asyncio.to_thread(entered.wait, 5)
                task.cancel()
                release.set()
                with pytest.raises(asyncio.CancelledError):
                    await task
                recovered = await journal.load_state()
                assert (
                    recovered.phase is WorkerRecoveryPhase.ASSIGNED
                    and recovered.assignment == response
                )
            finally:
                release.set()
                journal.stop_admission()
                await loop.run_in_executor(executor, value.close)

    asyncio.run(scenario())


def test_no_work_replay_with_wrong_proof_keeps_new_pending_input(
    store: SqliteClaimStore,
    scope: WorkerJournalScope,
    pending: WorkerClaimRequest,
) -> None:
    store.get_or_create_pending(pending)
    response = outcome(scope, pending, assigned=False)
    store.record_outcome(pending, response)
    new = WorkerClaimRequest(claim_request_id=uuid4(), proof=LeaseProof.generate())
    store.get_or_create_pending(new)
    conflict = WorkerClaimRequest(
        claim_request_id=pending.claim_request_id, proof=LeaseProof.generate()
    )

    with pytest.raises(ClaimReplayConflict):
        store.record_outcome(conflict, response)

    recovered = store.load_state()
    assert (
        recovered.request is not None
        and recovered.request.claim_request_id == new.claim_request_id
    )
    assert recovered.phase is WorkerRecoveryPhase.PENDING


def test_failed_v1_schema_upgrade_preserves_original_ciphertext(
    tmp_path: Path,
    scope: WorkerJournalScope,
    protector: TestProtector,
    pending: WorkerClaimRequest,
) -> None:
    original = create_v1(tmp_path, scope, protector, pending)
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        connection.execute("CREATE TABLE assignment (unexpected TEXT)")

    with pytest.raises(WorkerStateUnavailable):
        SqliteClaimStore(tmp_path, scope, protector, TestStorage())

    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        assert connection.execute("SELECT version FROM metadata").fetchone() == (1,)
        assert (
            connection.execute(
                "SELECT ciphertext, verifier FROM pending_claim"
            ).fetchone()
            == original
        )


@pytest.mark.postgres
@pytest.mark.parametrize("assigned", [True, False])
def test_control_receipt_and_local_completion_recover_same_request(
    database: Callable[[], AbstractAsyncContextManager[AsyncEngine]],
    tmp_path: Path,
    scope: WorkerJournalScope,
    protector: TestProtector,
    assigned: bool,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            stored = await save_job(engine)
            authorization = WorkerAuthorization(
                worker_id=scope.worker_id,
                allowed_repository_ids=frozenset({stored.job.repository_id}),
                credential_expires_at=datetime.now(UTC) + timedelta(hours=1),
            )
            control = claim_store(engine, (stored.intent.policy,))
            with ThreadPoolExecutor(max_workers=1) as executor:
                loop = asyncio.get_running_loop()
                local = await loop.run_in_executor(
                    executor,
                    SqliteClaimStore,
                    tmp_path,
                    scope,
                    protector,
                    TestStorage(),
                )
                journal = AsyncClaimJournal(local, executor)
                worker = PrepareWorkerClaim(journal)
                try:
                    original = await worker.prepare()
                    observation = (
                        PreparedJobClaim(
                            worker_id=scope.worker_id,
                            stored=stored,
                            observed_at=datetime.now(UTC),
                        )
                        if assigned
                        else None
                    )
                    accepted = await control.claim(
                        original.identify(authorization), authorization, observation
                    )
                    response = WorkerClaimOutcome.from_result(
                        accepted,
                        worker_id=scope.worker_id,
                        claim_request_id=original.claim_request_id,
                    )
                    await worker.record_outcome(original, response)
                    recovered = await control.recover(
                        original.identify(authorization), authorization
                    )
                    assert recovered is not None
                    await worker.record_outcome(
                        original,
                        WorkerClaimOutcome.from_result(
                            recovered,
                            worker_id=scope.worker_id,
                            claim_request_id=original.claim_request_id,
                        ),
                    )
                    state = await worker.recover()
                    if assigned:
                        retry = await worker.prepare()
                        assert retry.claim_request_id == original.claim_request_id
                        assert retry.proof.reveal() == original.proof.reveal()
                        assert (
                            state.assignment == response
                            and state.phase is WorkerRecoveryPhase.ASSIGNED
                        )
                        assert accepted.attempt == recovered.attempt
                    else:
                        new = await worker.prepare()
                        assert new.claim_request_id != original.claim_request_id
                        assert (
                            state.last_no_work == response
                            and state.phase is WorkerRecoveryPhase.IDLE
                        )
                finally:
                    journal.stop_admission()
                    await loop.run_in_executor(executor, local.close)

    asyncio.run(
        scenario(),
        loop_factory=asyncio.SelectorEventLoop if sys.platform == "win32" else None,
    )


@pytest.mark.skipif(
    sys.platform != "win32", reason="Requires actual Windows DPAPI and private ACLs."
)
def test_native_windows_bootstrap_recovers_recorded_assignment(tmp_path: Path) -> None:
    worker_id = uuid4()
    settings = WorkerSettings(
        control_url="https://control.example/",
        auth_token=SecretStr("test_only_worker_token_0123456789abcdefghijklmnop"),
        workspace_root=tmp_path / "workspaces",
    )
    scope = WorkerJournalScope(
        worker_id=worker_id, control_url=str(settings.control_url)
    )

    async def scenario() -> None:
        async with open_worker_journal(
            settings, worker_id=worker_id, state_directory=tmp_path / "private"
        ) as service:
            pending = await service.prepare()
            response = outcome(scope, pending)
            await service.record_outcome(pending, response)
        async with open_worker_journal(
            settings, worker_id=worker_id, state_directory=tmp_path / "private"
        ) as service:
            recovered = await service.recover()
            retry = await service.prepare()
            assert (
                recovered.assignment == response
                and recovered.phase is WorkerRecoveryPhase.ASSIGNED
            )
            assert (
                retry.claim_request_id == pending.claim_request_id
                and retry.proof.reveal() == pending.proof.reveal()
            )
            assert (
                pending.proof.reveal().encode()
                not in (tmp_path / "private/journal.sqlite3").read_bytes()
            )

    asyncio.run(scenario())
