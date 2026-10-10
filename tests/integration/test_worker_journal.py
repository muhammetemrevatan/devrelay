import asyncio
import sqlite3
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import SecretStr

from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    LeaseProof,
    WorkerClaimRequest,
)
from devrelay.application.contracts.workers import WorkerJournalScope
from devrelay.application.ports.worker_journal import WorkerStateUnavailable
from devrelay.application.worker_claims import PrepareWorkerClaim
from devrelay.bootstrap.worker import open_worker_journal
from devrelay.config.worker import WorkerSettings
from devrelay.infrastructure.worker_state.journal import (
    AsyncClaimJournal,
    SqliteClaimStore,
)

TOKEN = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"
WORKER_ID = UUID("22222222-2222-4222-8222-222222222222")
REQUEST_ID = UUID("11111111-1111-4111-8111-111111111111")


class TestProtector:
    __test__ = False

    def __init__(self) -> None:
        self.blobs: dict[bytes, tuple[str, bytes]] = {}
        self.fail = False

    def protect(self, proof: LeaseProof, *, context: bytes) -> bytes:
        if self.fail:
            raise ValueError(TOKEN)
        blob = b"opaque-test-ciphertext-" + str(len(self.blobs)).encode()
        self.blobs[blob] = (proof.reveal(), context)
        return blob

    def unprotect(self, ciphertext: bytes, *, context: bytes) -> LeaseProof:
        value, stored_context = self.blobs[ciphertext]
        if context != stored_context:
            raise ValueError(TOKEN)
        return LeaseProof(value)


class TestStorage:
    __test__ = False

    def verify_file(self, path: Path) -> None:
        if not path.is_file() or path.is_symlink():
            raise WorkerStateUnavailable("Test storage is invalid.")


@pytest.fixture
def scope() -> WorkerJournalScope:
    return WorkerJournalScope(
        worker_id=WORKER_ID, control_url="https://control.example/"
    )


@pytest.fixture
def candidate() -> WorkerClaimRequest:
    return WorkerClaimRequest(claim_request_id=REQUEST_ID, proof=LeaseProof(TOKEN))


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


def test_new_claim_is_durable_before_returning(
    store: SqliteClaimStore, candidate: WorkerClaimRequest, tmp_path: Path
) -> None:
    result = store.get_or_create_pending(candidate)

    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        row = connection.execute(
            "SELECT request_id, ciphertext FROM pending_claim"
        ).fetchone()
    assert row is not None and row[0] == str(result.claim_request_id)
    assert TOKEN.encode() not in row[1]
    assert TOKEN.encode() not in (tmp_path / "journal.sqlite3").read_bytes()
    for sidecar in tmp_path.glob("journal.sqlite3-*"):
        assert TOKEN.encode() not in sidecar.read_bytes()


@pytest.mark.parametrize(
    "damage",
    [
        "DELETE FROM metadata",
        "DELETE FROM pending_claim",
        "DROP TABLE metadata",
        "DROP TABLE pending_claim",
    ],
)
def test_damaged_existing_schema_is_not_reinitialized(
    store: SqliteClaimStore,
    candidate: WorkerClaimRequest,
    tmp_path: Path,
    scope: WorkerJournalScope,
    protector: TestProtector,
    damage: str,
) -> None:
    store.get_or_create_pending(candidate)
    store.close()
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        connection.execute(damage)

    with pytest.raises(WorkerStateUnavailable):
        SqliteClaimStore(tmp_path, scope, protector, TestStorage())


def test_reopen_returns_original_pending_claim(
    store: SqliteClaimStore,
    candidate: WorkerClaimRequest,
    tmp_path: Path,
    scope: WorkerJournalScope,
    protector: TestProtector,
) -> None:
    original = store.get_or_create_pending(candidate)
    store.close()
    reopened = SqliteClaimStore(tmp_path, scope, protector, TestStorage())
    try:
        result = reopened.load_pending()

        assert result is not None
        assert result.claim_request_id == original.claim_request_id
        assert result.proof.reveal() == original.proof.reveal()
    finally:
        reopened.close()


def test_new_candidate_cannot_replace_unresolved_claim(
    store: SqliteClaimStore, candidate: WorkerClaimRequest
) -> None:
    original = store.get_or_create_pending(candidate)
    other = WorkerClaimRequest(claim_request_id=UUID(int=3), proof=LeaseProof(TOKEN))

    result = store.get_or_create_pending(other)

    assert result.claim_request_id == original.claim_request_id
    assert result.proof.reveal() == original.proof.reveal()


def test_same_id_different_proof_conflicts(
    store: SqliteClaimStore, candidate: WorkerClaimRequest
) -> None:
    store.get_or_create_pending(candidate)
    other = WorkerClaimRequest(claim_request_id=REQUEST_ID, proof=LeaseProof("A" * 43))

    with pytest.raises(ClaimReplayConflict):
        store.get_or_create_pending(other)

    result = store.load_pending()
    assert result is not None and result.proof.reveal() == TOKEN


def test_protection_failure_commits_no_claim(
    store: SqliteClaimStore, candidate: WorkerClaimRequest, protector: TestProtector
) -> None:
    protector.fail = True

    with pytest.raises(WorkerStateUnavailable) as caught:
        store.get_or_create_pending(candidate)

    assert TOKEN not in str(caught.value)
    assert store.load_pending() is None


def test_failed_sql_commit_does_not_return_prepared_input(
    store: SqliteClaimStore, candidate: WorkerClaimRequest, tmp_path: Path
) -> None:
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        connection.execute(
            "CREATE TRIGGER reject_claim BEFORE INSERT ON pending_claim BEGIN SELECT RAISE(ABORT, 'Injected write failure.'); END"
        )

    with pytest.raises(WorkerStateUnavailable):
        store.get_or_create_pending(candidate)

    assert store.load_pending() is None


@pytest.mark.parametrize(
    "field,value",
    [("request_id", "invalid"), ("ciphertext", b"unknown"), ("verifier", b"x" * 32)],
)
def test_corrupt_record_fails_closed(
    store: SqliteClaimStore,
    candidate: WorkerClaimRequest,
    tmp_path: Path,
    field: str,
    value: str | bytes,
) -> None:
    store.get_or_create_pending(candidate)
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        connection.execute("PRAGMA ignore_check_constraints=ON")
        if field == "request_id":
            connection.execute("UPDATE pending_claim SET request_id = ?", (value,))
        elif field == "ciphertext":
            connection.execute("UPDATE pending_claim SET ciphertext = ?", (value,))
        else:
            connection.execute("UPDATE pending_claim SET verifier = ?", (value,))

    with pytest.raises(WorkerStateUnavailable) as caught:
        store.load_pending()

    assert TOKEN not in str(caught.value)


def test_other_scope_cannot_open_existing_journal(
    store: SqliteClaimStore, tmp_path: Path, protector: TestProtector
) -> None:
    store.close()
    other = WorkerJournalScope(
        worker_id=UUID(int=9), control_url="https://control.example/"
    )

    with pytest.raises(WorkerStateUnavailable):
        SqliteClaimStore(tmp_path, other, protector, TestStorage())


def test_journal_verifies_wal_and_full_sync(
    store: SqliteClaimStore, tmp_path: Path
) -> None:
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        mode = connection.execute("PRAGMA journal_mode").fetchone()

    assert mode == ("wal",)


def test_closed_store_does_not_prepare_new_claim(
    store: SqliteClaimStore, candidate: WorkerClaimRequest
) -> None:
    store.close()

    with pytest.raises(WorkerStateUnavailable):
        store.get_or_create_pending(candidate)


def test_unknown_file_rejects_recovery(store: SqliteClaimStore, tmp_path: Path) -> None:
    (tmp_path / "unexpected.txt").write_text("test", encoding="utf-8")

    with pytest.raises(WorkerStateUnavailable):
        store.load_pending()


def test_async_preparation_keeps_one_pending_request(
    tmp_path: Path,
    scope: WorkerJournalScope,
    protector: TestProtector,
    candidate: WorkerClaimRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> list[WorkerClaimRequest]:
        with ThreadPoolExecutor(max_workers=1) as executor:
            loop = asyncio.get_running_loop()
            store = await loop.run_in_executor(
                executor, SqliteClaimStore, tmp_path, scope, protector, TestStorage()
            )
            journal = AsyncClaimJournal(store, executor)
            service = PrepareWorkerClaim(journal)
            try:
                return await asyncio.gather(*(service.prepare() for _ in range(8)))
            finally:
                journal.stop_admission()
                await loop.run_in_executor(executor, store.close)

    monkeypatch.setattr("devrelay.application.worker_claims.uuid4", lambda: REQUEST_ID)
    monkeypatch.setattr(LeaseProof, "generate", lambda: LeaseProof(TOKEN))

    results = asyncio.run(scenario())

    assert len(results) == 8
    assert {value.claim_request_id for value in results} == {candidate.claim_request_id}
    assert all(value.proof.reveal() == TOKEN for value in results)


@pytest.mark.parametrize(
    "url",
    [
        "",
        "https://name:password@control.example/",
        "https://control.example/?token=secret",
        "https://control.example/#secret",
        "file:///tmp/state",
    ],
)
def test_scope_rejects_credential_or_noncontrol_url(url: str) -> None:
    with pytest.raises(ValueError):
        WorkerJournalScope(worker_id=WORKER_ID, control_url=url)


@pytest.mark.parametrize("cancel_count", [1, 2])
def test_cancelled_initialization_releases_resources_on_owner_thread(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cancel_count: int
) -> None:
    entered, release = threading.Event(), threading.Event()
    calls: list[tuple[str, int]] = []

    class ThreadSecurity(TestProtector, TestStorage):
        def acquire(self, scope: bytes) -> None:
            calls.append(("acquire", threading.get_ident()))

        def ensure_private_directory(self, directory: Path) -> bool:
            entered.set()
            if not release.wait(timeout=5):
                raise WorkerStateUnavailable("Test initialization timed out.")
            directory.mkdir()
            return True

        def close(self) -> None:
            calls.append(("close", threading.get_ident()))

    security = ThreadSecurity()
    monkeypatch.setattr(
        "devrelay.bootstrap.worker.WindowsWorkerSecurity", lambda: security
    )
    settings = WorkerSettings(
        control_url="https://control.example/",
        auth_token=SecretStr("test_only_worker_token_0123456789abcdefghijklmnop"),
        workspace_root=tmp_path / "workspaces",
    )

    async def open_state() -> None:
        async with open_worker_journal(
            settings, worker_id=WORKER_ID, state_directory=tmp_path / "private"
        ):
            pytest.fail("Cancelled setup unexpectedly yielded a service.")

    async def scenario() -> None:
        task = asyncio.create_task(open_state())
        ready = await asyncio.to_thread(entered.wait, 5)
        if not ready:
            release.set()
            await task
            raise AssertionError("Test initialization did not start.")
        task.cancel()
        await asyncio.sleep(0)
        if cancel_count == 2:
            task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())

    assert [name for name, _ in calls] == ["acquire", "close"]
    assert calls[0][1] == calls[1][1]
    assert calls[0][1] != threading.get_ident()
