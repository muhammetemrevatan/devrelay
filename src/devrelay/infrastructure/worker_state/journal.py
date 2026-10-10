import asyncio
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Protocol
from uuid import UUID

from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    LeaseProofVerifier,
    WorkerClaimRequest,
)
from devrelay.application.contracts.workers import WorkerJournalScope
from devrelay.application.ports.worker_journal import (
    ProofProtector,
    WorkerStateUnavailable,
)

_FAILURE = "Worker recovery journal is unavailable."


class PrivateStorage(Protocol):
    def verify_file(self, path: Path) -> None: ...


@dataclass(frozen=True, slots=True)
class _StoredClaim:
    request_id: str
    protocol_version: int
    ciphertext: bytes
    verifier: bytes


class SqliteClaimStore:
    """Synchronous store, owned by a dedicated thread and private OS guard."""

    def __init__(
        self,
        directory: Path,
        scope: WorkerJournalScope,
        protector: ProofProtector,
        storage: PrivateStorage,
    ) -> None:
        self._scope = scope
        self._protector = protector
        self._storage = storage
        self._directory = directory
        self._connection: sqlite3.Connection | None = None
        try:
            path = directory / "journal.sqlite3"
            new = not path.exists()
            if not new:
                self._storage.verify_file(path)
            self._connection = sqlite3.connect(path, timeout=2, isolation_level=None)
            self._storage.verify_file(path)
            connection = self._connection
            mode = connection.execute("PRAGMA journal_mode=WAL").fetchone()
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("PRAGMA trusted_schema=OFF")
            if mode != ("wal",) or connection.execute(
                "PRAGMA synchronous"
            ).fetchone() != (2,):
                raise WorkerStateUnavailable(_FAILURE)
            connection.execute("BEGIN IMMEDIATE")
            if new:
                connection.execute(
                    "CREATE TABLE metadata (slot INTEGER PRIMARY KEY CHECK (slot = 1), version INTEGER NOT NULL CHECK (version = 1), scope BLOB NOT NULL CHECK (length(scope) = 32), prepared INTEGER NOT NULL CHECK (prepared IN (0, 1)))"
                )
                connection.execute(
                    "INSERT INTO metadata VALUES (1, 1, ?, 0)", (scope.key,)
                )
                connection.execute(
                    "CREATE TABLE pending_claim (slot INTEGER PRIMARY KEY CHECK (slot = 1), request_id TEXT NOT NULL CHECK (length(request_id) = 36), protocol_version INTEGER NOT NULL CHECK (protocol_version = 1), ciphertext BLOB NOT NULL CHECK (length(ciphertext) BETWEEN 1 AND 4096), verifier BLOB NOT NULL CHECK (length(verifier) = 32))"
                )
            existing = connection.execute(
                "SELECT version, scope FROM metadata WHERE slot = 1"
            ).fetchone()
            if existing != (1, scope.key):
                raise WorkerStateUnavailable(_FAILURE)
            connection.execute(
                "SELECT request_id, protocol_version, ciphertext, verifier FROM pending_claim WHERE slot = 1"
            )
            connection.execute("COMMIT")
            self._verify_files()
            self._read()
        except sqlite3.Error, OSError, ValueError, WorkerStateUnavailable:
            self.close()
            raise WorkerStateUnavailable(_FAILURE) from None

    def _verify_files(self) -> None:
        for path in self._directory.iterdir():
            if path.name not in {
                "journal.sqlite3",
                "journal.sqlite3-wal",
                "journal.sqlite3-shm",
            }:
                raise WorkerStateUnavailable(_FAILURE)
            self._storage.verify_file(path)

    def _open(self) -> sqlite3.Connection:
        if self._connection is None:
            raise WorkerStateUnavailable(_FAILURE)
        return self._connection

    def _context(self, request_id: UUID) -> bytes:
        return sha256(
            b"devrelay.pending-proof.v1\x00" + self._scope.key + request_id.bytes
        ).digest()

    def _read(self) -> WorkerClaimRequest | None:
        prepared = (
            self._open()
            .execute("SELECT prepared FROM metadata WHERE slot = 1")
            .fetchone()
        )
        if prepared not in ((0,), (1,)):
            raise WorkerStateUnavailable(_FAILURE)
        row = (
            self._open()
            .execute(
                "SELECT request_id, protocol_version, ciphertext, verifier FROM pending_claim WHERE slot = 1"
            )
            .fetchone()
        )
        if row is None:
            if prepared != (0,):
                raise WorkerStateUnavailable(_FAILURE)
            return None
        if prepared != (1,):
            raise WorkerStateUnavailable(_FAILURE)
        stored = _StoredClaim(*row)
        if type(stored.request_id) is not str or len(stored.request_id) != 36:
            raise WorkerStateUnavailable(_FAILURE)
        request_id = UUID(stored.request_id)
        if (
            str(request_id) != stored.request_id
            or type(stored.protocol_version) is not int
            or stored.protocol_version != 1
            or type(stored.ciphertext) is not bytes
            or not 1 <= len(stored.ciphertext) <= 4096
        ):
            raise WorkerStateUnavailable(_FAILURE)
        try:
            proof = self._protector.unprotect(
                stored.ciphertext, context=self._context(request_id)
            )
        except Exception:
            raise WorkerStateUnavailable(_FAILURE) from None
        verifier = LeaseProofVerifier(
            worker_id=self._scope.worker_id,
            claim_request_id=request_id,
            digest=stored.verifier,
        )
        if not verifier.matches(
            proof, worker_id=self._scope.worker_id, claim_request_id=request_id
        ):
            raise WorkerStateUnavailable(_FAILURE)
        return WorkerClaimRequest(claim_request_id=request_id, proof=proof)

    def load_pending(self) -> WorkerClaimRequest | None:
        try:
            self._verify_files()
            return self._read()
        except sqlite3.Error, OSError, ValueError, WorkerStateUnavailable:
            raise WorkerStateUnavailable(_FAILURE) from None

    def get_or_create_pending(
        self, candidate: WorkerClaimRequest
    ) -> WorkerClaimRequest:
        connection = self._open()
        try:
            self._verify_files()
            connection.execute("BEGIN IMMEDIATE")
            pending = self._read()
            if pending is not None:
                if pending.claim_request_id == candidate.claim_request_id:
                    verifier = LeaseProofVerifier.from_proof(
                        worker_id=self._scope.worker_id,
                        claim_request_id=pending.claim_request_id,
                        proof=pending.proof,
                    )
                    if not verifier.matches(
                        candidate.proof,
                        worker_id=self._scope.worker_id,
                        claim_request_id=candidate.claim_request_id,
                    ):
                        raise ClaimReplayConflict(
                            "Pending claim conflicts with the original input."
                        )
                connection.execute("COMMIT")
                return pending
            try:
                ciphertext = self._protector.protect(
                    candidate.proof, context=self._context(candidate.claim_request_id)
                )
            except Exception:
                raise WorkerStateUnavailable(_FAILURE) from None
            if type(ciphertext) is not bytes or not 1 <= len(ciphertext) <= 4096:
                raise WorkerStateUnavailable(_FAILURE)
            verifier = LeaseProofVerifier.from_proof(
                worker_id=self._scope.worker_id,
                claim_request_id=candidate.claim_request_id,
                proof=candidate.proof,
            )
            connection.execute(
                "INSERT INTO pending_claim VALUES (1, ?, ?, ?, ?)",
                (
                    str(candidate.claim_request_id),
                    candidate.protocol_version,
                    ciphertext,
                    verifier.digest,
                ),
            )
            connection.execute("UPDATE metadata SET prepared = 1 WHERE slot = 1")
            connection.execute("COMMIT")
            return candidate
        except ClaimReplayConflict:
            self._rollback()
            raise
        except sqlite3.Error, OSError, ValueError, WorkerStateUnavailable:
            self._rollback()
            raise WorkerStateUnavailable(_FAILURE) from None

    def _rollback(self) -> None:
        connection = self._open()
        if connection.in_transaction:
            try:
                connection.execute("ROLLBACK")
            except sqlite3.Error:
                self.close()
                raise WorkerStateUnavailable(_FAILURE) from None

    def close(self) -> None:
        if self._connection is not None:
            connection, self._connection = self._connection, None
            try:
                connection.close()
            except sqlite3.Error:
                raise WorkerStateUnavailable(_FAILURE) from None


class AsyncClaimJournal:
    """Serializes SQLite/DPAPI on the thread that owns the process mutex."""

    def __init__(self, store: SqliteClaimStore, executor: ThreadPoolExecutor) -> None:
        self._store = store
        self._executor = executor
        self._closed = False
        self._admission = asyncio.Lock()

    async def load_pending(self) -> WorkerClaimRequest | None:
        async with self._admission:
            if self._closed:
                raise WorkerStateUnavailable(_FAILURE)
            return await asyncio.get_running_loop().run_in_executor(
                self._executor, self._store.load_pending
            )

    async def get_or_create_pending(
        self, candidate: WorkerClaimRequest
    ) -> WorkerClaimRequest:
        async with self._admission:
            if self._closed:
                raise WorkerStateUnavailable(_FAILURE)
            return await asyncio.get_running_loop().run_in_executor(
                self._executor, self._store.get_or_create_pending, candidate
            )

    def stop_admission(self) -> None:
        self._closed = True
