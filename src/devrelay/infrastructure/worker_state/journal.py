import asyncio
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import dataclass, replace
from hashlib import sha256
from pathlib import Path
from typing import Protocol
from uuid import UUID

from devrelay.application.contracts.acknowledgements import WorkerAcknowledgementIntent
from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    LeaseProof,
    LeaseProofVerifier,
    WorkerClaimRequest,
)
from devrelay.application.contracts.coding_result_receipts import (
    CodingResultReceipt,
    CodingResultStatus,
)
from devrelay.application.contracts.coding_result_requests import (
    WorkerCodingResultIntent,
)
from devrelay.application.contracts.coding_result_wire import (
    decode_coding_result_intent,
    encode_coding_result_intent,
)
from devrelay.application.contracts.executions import WorkerExecutionIntent
from devrelay.application.contracts.worker_recovery import (
    AcknowledgementCompletion,
    WorkerClaimOutcome,
    WorkerHeartbeatIntent,
    WorkerRecoveryPhase,
    WorkerRecoveryState,
)
from devrelay.application.contracts.worker_result_recovery import (
    WorkerCodingResultState,
)
from devrelay.application.contracts.workers import WorkerJournalScope
from devrelay.application.contracts.workspace_recovery import (
    WorkerWorkspaceCompletion,
    WorkerWorkspaceIntent,
    WorkspacePreparationDecision,
)
from devrelay.application.ports.worker_journal import (
    ProofProtector,
    WorkerStateUnavailable,
)
from devrelay.infrastructure.worker_state.acknowledgements import save_acknowledgement
from devrelay.infrastructure.worker_state.completions import save_completion
from devrelay.infrastructure.worker_state.executions import save_execution
from devrelay.infrastructure.worker_state.heartbeats import (
    save_heartbeat,
)
from devrelay.infrastructure.worker_state.outcomes import (
    create_outcome_schema,
    recovery_state,
    save_assignment,
    save_no_work,
    upgrade_outcome_schema,
)
from devrelay.infrastructure.worker_state.results import (
    read_result_state,
    save_result_receipts,
)
from devrelay.infrastructure.worker_state.workspaces import (
    bind_workspace_state,
    save_workspace_completion,
    save_workspace_intent,
)

_FAILURE = "Worker recovery journal is unavailable."


def _capture_claim(request: WorkerClaimRequest) -> WorkerClaimRequest:
    return WorkerClaimRequest(
        claim_request_id=request.claim_request_id,
        proof=LeaseProof(request.proof.reveal()),
        protocol_version=request.protocol_version,
    )


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
            connection.execute("PRAGMA wal_autocheckpoint=32")
            connection.execute("PRAGMA journal_size_limit=131072")
            if mode != ("wal",) or connection.execute(
                "PRAGMA synchronous"
            ).fetchone() != (2,):
                raise WorkerStateUnavailable(_FAILURE)
            if connection.execute("PRAGMA wal_autocheckpoint").fetchone() != (
                32,
            ) or connection.execute("PRAGMA journal_size_limit").fetchone() != (
                131072,
            ):
                raise WorkerStateUnavailable(_FAILURE)
            connection.execute("BEGIN IMMEDIATE")
            if new:
                create_outcome_schema(connection, scope.key, prepared=0)
                connection.execute(
                    "CREATE TABLE pending_claim (slot INTEGER PRIMARY KEY CHECK (slot = 1), request_id TEXT NOT NULL CHECK (length(request_id) = 36), protocol_version INTEGER NOT NULL CHECK (protocol_version = 1), ciphertext BLOB NOT NULL CHECK (length(ciphertext) BETWEEN 1 AND 4096), verifier BLOB NOT NULL CHECK (length(verifier) = 32))"
                )
            existing = connection.execute(
                "SELECT version, scope FROM metadata WHERE slot = 1"
            ).fetchone()
            if existing == (1, scope.key):
                # Validate the old record/proof before an atomic, lossless upgrade.
                pending = self._read_request()
                connection.execute("DROP TABLE metadata")
                create_outcome_schema(
                    connection, scope.key, prepared=int(pending is not None)
                )
            elif existing in (
                (2, scope.key),
                (3, scope.key),
                (4, scope.key),
                (5, scope.key),
                (6, scope.key),
                (7, scope.key),
            ):
                state = recovery_state(
                    connection, self._scope, self._read_request(), allow_legacy=True
                )
                upgrade_outcome_schema(connection, scope, state)
            elif existing != (8, scope.key):
                raise WorkerStateUnavailable(_FAILURE)
            self._read_state()
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

    def _read_request(self) -> WorkerClaimRequest | None:
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

    def _read_state(self) -> WorkerRecoveryState:
        return recovery_state(self._open(), self._scope, self._read_request())

    def _read(self) -> WorkerClaimRequest | None:
        return self._read_state().request

    def load_state(self) -> WorkerRecoveryState:
        try:
            self._verify_files()
            return self._read_state()
        except sqlite3.Error, OSError, ValueError, WorkerStateUnavailable:
            raise WorkerStateUnavailable(_FAILURE) from None

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
            previous = self._read_state().last_no_work
            if (
                previous is not None
                and previous.claim_request_id == candidate.claim_request_id
            ):
                raise ClaimReplayConflict("Completed claim identity cannot be reused.")
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
            connection.execute(
                "UPDATE metadata SET prepared = 1, phase = 'pending' WHERE slot = 1"
            )
            connection.execute("COMMIT")
            return candidate
        except ClaimReplayConflict:
            self._rollback()
            raise
        except sqlite3.Error, OSError, ValueError, WorkerStateUnavailable:
            self._rollback()
            raise WorkerStateUnavailable(_FAILURE) from None

    def record_outcome(
        self, request: WorkerClaimRequest, outcome: WorkerClaimOutcome
    ) -> WorkerRecoveryState:
        connection = self._open()
        try:
            self._verify_files()
            connection.execute("BEGIN IMMEDIATE")
            if (
                not isinstance(request, WorkerClaimRequest)
                or not isinstance(outcome, WorkerClaimOutcome)
                or outcome.worker_id != self._scope.worker_id
                or outcome.claim_request_id != request.claim_request_id
                or outcome.protocol_version != request.protocol_version
            ):
                raise ClaimReplayConflict(
                    "Claim outcome is outside the original request scope."
                )
            state = self._read_state()
            pending = state.request
            if pending is None or pending.claim_request_id != request.claim_request_id:
                if state.last_no_work != outcome:
                    raise ClaimReplayConflict(
                        "Claim outcome does not match local recovery state."
                    )
                digest = connection.execute(
                    "SELECT verifier FROM completed_no_work WHERE slot = 1"
                ).fetchone()
                if digest is None:
                    raise WorkerStateUnavailable(_FAILURE)
                verifier = LeaseProofVerifier(
                    worker_id=self._scope.worker_id,
                    claim_request_id=request.claim_request_id,
                    digest=digest[0],
                )
            else:
                verifier = LeaseProofVerifier.from_proof(
                    worker_id=self._scope.worker_id,
                    claim_request_id=pending.claim_request_id,
                    proof=pending.proof,
                )
            if not verifier.matches(
                request.proof,
                worker_id=self._scope.worker_id,
                claim_request_id=request.claim_request_id,
            ):
                raise ClaimReplayConflict(
                    "Claim outcome conflicts with the original proof."
                )
            if (
                pending is not None
                and pending.claim_request_id == request.claim_request_id
            ):
                if state.assignment is not None:
                    if state.assignment != outcome:
                        raise ClaimReplayConflict(
                            "Claim outcome conflicts with the recorded assignment."
                        )
                elif outcome.assignment is not None:
                    save_assignment(connection, self._scope, request, outcome)
                else:
                    save_no_work(connection, outcome, verifier.digest)
            result = self._read_state()
            connection.execute("COMMIT")
            return result
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

    def get_or_create_acknowledgement(
        self, request: WorkerClaimRequest, candidate: WorkerAcknowledgementIntent
    ) -> WorkerAcknowledgementIntent:
        connection = self._open()
        try:
            self._verify_files()
            connection.execute("BEGIN IMMEDIATE")
            state = self._read_state()
            if (
                not isinstance(request, WorkerClaimRequest)
                or not isinstance(candidate, WorkerAcknowledgementIntent)
                or state.request is None
                or state.assignment is None
                or state.assignment.assignment is None
                or request.claim_request_id != state.request.claim_request_id
                or candidate.claim_request_id != state.request.claim_request_id
                or candidate.attempt_id != state.assignment.assignment.attempt_id
                or candidate.generation != state.assignment.assignment.generation
            ):
                raise ClaimReplayConflict("Acknowledgement requires original anchors.")
            verifier = LeaseProofVerifier.from_proof(
                worker_id=self._scope.worker_id,
                claim_request_id=state.request.claim_request_id,
                proof=state.request.proof,
            )
            if not verifier.matches(
                request.proof,
                worker_id=self._scope.worker_id,
                claim_request_id=request.claim_request_id,
            ):
                raise ClaimReplayConflict(
                    "Acknowledgement conflicts with original proof."
                )
            if state.acknowledgement is not None:
                state.acknowledgement.require_same_input(candidate)
                result = state.acknowledgement
            else:
                save_acknowledgement(
                    connection, self._scope, state.request, state.assignment, candidate
                )
                saved = self._read_state().acknowledgement
                if saved is None:
                    raise WorkerStateUnavailable(_FAILURE)
                result = saved
            connection.execute("COMMIT")
            return result
        except ClaimReplayConflict:
            self._rollback()
            raise
        except sqlite3.Error, OSError, ValueError, WorkerStateUnavailable:
            self._rollback()
            raise WorkerStateUnavailable(_FAILURE) from None

    def record_acknowledgement(
        self, request: WorkerClaimRequest, completion: AcknowledgementCompletion
    ) -> WorkerRecoveryState:
        connection = self._open()
        try:
            self._verify_files()
            connection.execute("BEGIN IMMEDIATE")
            state = self._read_state()
            if (
                not isinstance(completion, AcknowledgementCompletion)
                or state.request is None
                or state.assignment is None
                or state.acknowledgement != completion.intent
                or request.claim_request_id != state.request.claim_request_id
            ):
                raise ClaimReplayConflict(
                    "Completion requires original acknowledgement input."
                )
            verifier = LeaseProofVerifier.from_proof(
                worker_id=self._scope.worker_id,
                claim_request_id=state.request.claim_request_id,
                proof=state.request.proof,
            )
            if not verifier.matches(
                request.proof,
                worker_id=self._scope.worker_id,
                claim_request_id=request.claim_request_id,
            ):
                raise ClaimReplayConflict("Completion conflicts with original proof.")
            if state.completion is not None:
                if state.completion != completion:
                    raise ClaimReplayConflict(
                        "Completion conflicts with recorded evidence."
                    )
            else:
                reference = state.assignment.assignment
                if (
                    reference is None
                    or not reference.issued_at
                    <= completion.acknowledged_at
                    < reference.expires_at
                ):
                    raise ClaimReplayConflict(
                        "Completion changed original lease bounds."
                    )
                save_completion(
                    connection, self._scope, state.request, state.assignment, completion
                )
                bind_workspace_state(
                    connection, self._scope, state.request, state.assignment, completion
                )
            result = self._read_state()
            connection.execute("COMMIT")
            return result
        except ClaimReplayConflict:
            self._rollback()
            raise
        except sqlite3.Error, OSError, ValueError, WorkerStateUnavailable:
            self._rollback()
            raise WorkerStateUnavailable(_FAILURE) from None

    def _heartbeat_state(
        self, request: WorkerClaimRequest, intent: WorkerHeartbeatIntent
    ) -> WorkerRecoveryState:
        state = self._read_state()
        reference = state.assignment.assignment if state.assignment else None
        if (
            not isinstance(request, WorkerClaimRequest)
            or not isinstance(intent, WorkerHeartbeatIntent)
            or state.phase is not WorkerRecoveryPhase.ACKNOWLEDGED
            or state.request is None
            or state.completion is None
            or reference is None
            or request.claim_request_id != state.request.claim_request_id
            or intent.claim_request_id != state.request.claim_request_id
            or intent.attempt_id != reference.attempt_id
            or intent.generation != reference.generation
            or intent.protocol_version != request.protocol_version
        ):
            raise ClaimReplayConflict("Heartbeat requires completed original anchors.")
        verifier = LeaseProofVerifier.from_proof(
            worker_id=self._scope.worker_id,
            claim_request_id=state.request.claim_request_id,
            proof=state.request.proof,
        )
        if not verifier.matches(
            request.proof,
            worker_id=self._scope.worker_id,
            claim_request_id=request.claim_request_id,
        ):
            raise ClaimReplayConflict("Heartbeat conflicts with original proof.")
        return state

    def get_or_create_execution(
        self, request: WorkerClaimRequest, candidate: WorkerExecutionIntent
    ) -> WorkerExecutionIntent:
        connection = self._open()
        try:
            self._verify_files()
            connection.execute("BEGIN IMMEDIATE")
            state = self._read_state()
            if (
                not isinstance(request, WorkerClaimRequest)
                or not isinstance(candidate, WorkerExecutionIntent)
                or state.phase is not WorkerRecoveryPhase.ACKNOWLEDGED
                or state.request is None
                or state.completion is None
                or request.claim_request_id != state.request.claim_request_id
                or request.protocol_version != state.request.protocol_version
                or candidate.acknowledgement != state.completion.intent
            ):
                raise ClaimReplayConflict(
                    "Execution requires completed original anchors."
                )
            verifier = LeaseProofVerifier.from_proof(
                worker_id=self._scope.worker_id,
                claim_request_id=state.request.claim_request_id,
                proof=state.request.proof,
            )
            if not verifier.matches(
                request.proof,
                worker_id=self._scope.worker_id,
                claim_request_id=request.claim_request_id,
            ):
                raise ClaimReplayConflict("Execution conflicts with original proof.")
            if state.execution is not None:
                state.execution.require_same_input(candidate)
                result = state.execution
            else:
                save_execution(connection, candidate)
                self._bind_heartbeat_state(state)
                saved = self._read_state().execution
                if saved is None:
                    raise WorkerStateUnavailable(_FAILURE)
                result = saved
            connection.execute("COMMIT")
            return result
        except ClaimReplayConflict:
            self._rollback()
            raise
        except sqlite3.Error, OSError, ValueError, WorkerStateUnavailable:
            self._rollback()
            raise WorkerStateUnavailable(_FAILURE) from None

    def _workspace_state(self, request: WorkerClaimRequest) -> WorkerRecoveryState:
        state = self._read_state()
        if (
            not isinstance(request, WorkerClaimRequest)
            or state.phase is not WorkerRecoveryPhase.ACKNOWLEDGED
            or state.request is None
            or state.completion is None
            or state.execution is None
            or request.claim_request_id != state.request.claim_request_id
            or request.protocol_version != state.request.protocol_version
        ):
            raise ClaimReplayConflict(
                "Workspace requires completed original execution."
            )
        verifier = LeaseProofVerifier.from_proof(
            worker_id=self._scope.worker_id,
            claim_request_id=state.request.claim_request_id,
            proof=state.request.proof,
        )
        if not verifier.matches(
            request.proof,
            worker_id=self._scope.worker_id,
            claim_request_id=request.claim_request_id,
        ):
            raise ClaimReplayConflict("Workspace conflicts with original proof.")
        return state

    def get_or_create_workspace(
        self, request: WorkerClaimRequest, candidate: WorkerWorkspaceIntent
    ) -> WorkspacePreparationDecision:
        connection = self._open()
        try:
            self._verify_files()
            connection.execute("BEGIN IMMEDIATE")
            state = self._workspace_state(request)
            if state.workspace_history_unknown:
                raise ClaimReplayConflict("Historical workspace state is uncertain.")
            if (
                not isinstance(candidate, WorkerWorkspaceIntent)
                or candidate.execution != state.execution
            ):
                raise ClaimReplayConflict("Workspace changed original execution input.")
            if state.workspace_intent is not None:
                state.workspace_intent.require_same_input(candidate)
                result = WorkspacePreparationDecision(
                    intent=state.workspace_intent, created=False
                )
            else:
                save_workspace_intent(connection, candidate)
                self._bind_heartbeat_state(state)
                saved = self._read_state().workspace_intent
                if saved is None:
                    raise WorkerStateUnavailable(_FAILURE)
                result = WorkspacePreparationDecision(intent=saved, created=True)
            connection.execute("COMMIT")
            return result
        except ClaimReplayConflict:
            self._rollback()
            raise
        except sqlite3.Error, OSError, ValueError, WorkerStateUnavailable:
            self._rollback()
            raise WorkerStateUnavailable(_FAILURE) from None

    def record_workspace_completion(
        self, request: WorkerClaimRequest, completion: WorkerWorkspaceCompletion
    ) -> WorkerWorkspaceCompletion:
        connection = self._open()
        try:
            self._verify_files()
            connection.execute("BEGIN IMMEDIATE")
            state = self._workspace_state(request)
            if (
                not isinstance(completion, WorkerWorkspaceCompletion)
                or state.workspace_intent != completion.intent
            ):
                raise ClaimReplayConflict(
                    "Workspace completion changed original intent."
                )
            if state.workspace_completion is not None:
                if state.workspace_completion != completion:
                    raise ClaimReplayConflict(
                        "Workspace completion changed recorded identity."
                    )
                result = state.workspace_completion
            else:
                save_workspace_completion(connection, completion)
                self._bind_heartbeat_state(state)
                saved = self._read_state().workspace_completion
                if saved is None:
                    raise WorkerStateUnavailable(_FAILURE)
                result = saved
            connection.execute("COMMIT")
            return result
        except ClaimReplayConflict:
            self._rollback()
            raise
        except sqlite3.Error, OSError, ValueError, WorkerStateUnavailable:
            self._rollback()
            raise WorkerStateUnavailable(_FAILURE) from None

    def _bind_heartbeat_state(self, state: WorkerRecoveryState) -> None:
        if (
            state.request is None
            or state.assignment is None
            or state.completion is None
        ):
            raise WorkerStateUnavailable(_FAILURE)
        bind_workspace_state(
            self._open(), self._scope, state.request, state.assignment, state.completion
        )

    def get_or_create_heartbeat(
        self, request: WorkerClaimRequest, candidate: WorkerHeartbeatIntent
    ) -> WorkerHeartbeatIntent:
        connection = self._open()
        try:
            self._verify_files()
            connection.execute("BEGIN IMMEDIATE")
            state = self._heartbeat_state(request, candidate)
            if state.last_heartbeat is not None and (
                candidate.heartbeat_request_id
                == state.last_heartbeat.heartbeat_request_id
                or candidate.expected_lease_revision
                < state.last_heartbeat.expected_lease_revision
            ):
                raise ClaimReplayConflict(
                    "Completed heartbeat cannot be reused or regressed."
                )
            if state.pending_heartbeat is not None:
                state.pending_heartbeat.require_same_input(candidate)
                result = state.pending_heartbeat
            else:
                save_heartbeat(connection, candidate, slot=1)
                self._bind_heartbeat_state(state)
                saved = self._read_state().pending_heartbeat
                if saved is None:
                    raise WorkerStateUnavailable(_FAILURE)
                result = saved
            connection.execute("COMMIT")
            return result
        except ClaimReplayConflict:
            self._rollback()
            raise
        except sqlite3.Error, OSError, ValueError, WorkerStateUnavailable:
            self._rollback()
            raise WorkerStateUnavailable(_FAILURE) from None

    def record_heartbeat(
        self, request: WorkerClaimRequest, intent: WorkerHeartbeatIntent
    ) -> WorkerRecoveryState:
        connection = self._open()
        try:
            self._verify_files()
            connection.execute("BEGIN IMMEDIATE")
            state = self._heartbeat_state(request, intent)
            if state.last_heartbeat == intent:
                result = state
            elif state.pending_heartbeat != intent:
                raise ClaimReplayConflict(
                    "Heartbeat completion requires original pending input."
                )
            else:
                save_heartbeat(connection, intent, slot=2)
                connection.execute("DELETE FROM heartbeat_input WHERE slot = 1")
                self._bind_heartbeat_state(state)
                result = self._read_state()
            connection.execute("COMMIT")
            return result
        except ClaimReplayConflict:
            self._rollback()
            raise
        except sqlite3.Error, OSError, ValueError, WorkerStateUnavailable:
            self._rollback()
            raise WorkerStateUnavailable(_FAILURE) from None

    def close(self) -> None:
        if self._connection is not None:
            connection, self._connection = self._connection, None
            try:
                connection.close()
            except sqlite3.Error:
                raise WorkerStateUnavailable(_FAILURE) from None

    def load_result_state(self) -> WorkerCodingResultState:
        connection = self._open()
        try:
            self._verify_files()
            connection.execute("BEGIN")
            state = read_result_state(connection, self._read_state())
            connection.execute("COMMIT")
            return state
        except sqlite3.Error, OSError, ValueError, WorkerStateUnavailable:
            self._rollback()
            raise WorkerStateUnavailable(_FAILURE) from None

    def _result_state(self, request: WorkerClaimRequest) -> WorkerCodingResultState:
        state = read_result_state(self._open(), self._read_state())
        original = state.original.request
        if (
            not isinstance(request, WorkerClaimRequest)
            or original is None
            or request.claim_request_id != original.claim_request_id
            or request.protocol_version != original.protocol_version
        ):
            raise ClaimReplayConflict("Coding result requires the original claim.")
        verifier = LeaseProofVerifier.from_proof(
            worker_id=self._scope.worker_id,
            claim_request_id=original.claim_request_id,
            proof=original.proof,
        )
        if not verifier.matches(
            request.proof,
            worker_id=self._scope.worker_id,
            claim_request_id=request.claim_request_id,
        ):
            raise ClaimReplayConflict("Coding result changed its original proof.")
        return state

    def _bind_result(self, state: WorkerCodingResultState) -> None:
        original = state.original
        if (
            original.request is None
            or original.assignment is None
            or original.completion is None
        ):
            raise WorkerStateUnavailable(_FAILURE)
        bind_workspace_state(
            self._open(),
            self._scope,
            original.request,
            original.assignment,
            original.completion,
        )

    def get_or_create_result(
        self,
        request: WorkerClaimRequest,
        candidate: WorkerCodingResultIntent,
        *,
        target_branch: str,
    ) -> WorkerCodingResultIntent:
        connection = self._open()
        try:
            # Full canonicalization precedes the write transaction. The owning
            # executor does all source-byte work away from the asyncio loop.
            payload = encode_coding_result_intent(candidate)
            captured = decode_coding_result_intent(payload)
            self._verify_files()
            connection.execute("BEGIN IMMEDIATE")
            state = self._result_state(request)
            if state.history_unknown:
                raise WorkerStateUnavailable(
                    "Legacy result history requires reconciliation."
                )
            if state.intent is not None:
                state.intent.require_same_input(captured)
                if state.target_branch != target_branch:
                    raise ClaimReplayConflict(
                        "Coding result changed its accepted target branch."
                    )
                result = state.intent
            else:
                try:
                    proposed = replace(
                        state, intent=captured, target_branch=target_branch
                    )
                except ValueError:
                    raise ClaimReplayConflict(
                        "Coding result changed required original execution history."
                    ) from None
                connection.execute(
                    "INSERT INTO coding_result_input VALUES(1,?,?,?)",
                    (payload, sha256(payload).hexdigest(), target_branch),
                )
                self._bind_result(proposed)
                saved = read_result_state(connection, self._read_state()).intent
                if saved is None:
                    raise WorkerStateUnavailable(_FAILURE)
                result = saved
            connection.execute("COMMIT")
            return result
        except ClaimReplayConflict:
            self._rollback()
            raise
        except sqlite3.Error, OSError, ValueError, WorkerStateUnavailable:
            self._rollback()
            raise WorkerStateUnavailable(_FAILURE) from None

    def record_result_acceptance(
        self, request: WorkerClaimRequest, receipt: CodingResultReceipt
    ) -> WorkerCodingResultState:
        connection = self._open()
        try:
            receipt = replace(receipt)
            self._verify_files()
            connection.execute("BEGIN IMMEDIATE")
            state = self._result_state(request)
            if state.receipt is not None and state.receipt != receipt:
                raise ClaimReplayConflict(
                    "Acceptance changed the recorded result receipt."
                )
            try:
                proposed = replace(state, receipt=receipt)
            except ValueError:
                raise ClaimReplayConflict(
                    "Acceptance changed the retained result association."
                ) from None
            save_result_receipts(connection, proposed)
            self._bind_result(proposed)
            result = read_result_state(connection, self._read_state())
            connection.execute("COMMIT")
            return result
        except ClaimReplayConflict:
            self._rollback()
            raise
        except sqlite3.Error, OSError, ValueError, WorkerStateUnavailable:
            self._rollback()
            raise WorkerStateUnavailable(_FAILURE) from None

    def record_result_terminal(
        self, request: WorkerClaimRequest, status: CodingResultStatus
    ) -> WorkerCodingResultState:
        connection = self._open()
        try:
            status = deepcopy(status)
            self._verify_files()
            connection.execute("BEGIN IMMEDIATE")
            state = self._result_state(request)
            if (state.receipt is not None and state.receipt != status.receipt) or (
                state.terminal_status is not None and state.terminal_status != status
            ):
                raise ClaimReplayConflict(
                    "Terminal receipt changed recorded result history."
                )
            try:
                proposed = replace(
                    state, receipt=status.receipt, terminal_status=status
                )
            except ValueError:
                raise ClaimReplayConflict(
                    "Terminal receipt changed the retained publication association."
                ) from None
            save_result_receipts(connection, proposed)
            self._bind_result(proposed)
            result = read_result_state(connection, self._read_state())
            connection.execute("COMMIT")
            return result
        except ClaimReplayConflict:
            self._rollback()
            raise
        except sqlite3.Error, OSError, ValueError, WorkerStateUnavailable:
            self._rollback()
            raise WorkerStateUnavailable(_FAILURE) from None


class AsyncClaimJournal:
    """Serializes SQLite/DPAPI on the thread that owns the process mutex."""

    def __init__(self, store: SqliteClaimStore, executor: ThreadPoolExecutor) -> None:
        self._store = store
        self._executor = executor
        self._closed = False
        self._admission = asyncio.Lock()

    async def load_state(self) -> WorkerRecoveryState:
        async with self._admission:
            if self._closed:
                raise WorkerStateUnavailable(_FAILURE)
            return await asyncio.get_running_loop().run_in_executor(
                self._executor, self._store.load_state
            )

    async def load_result_state(self) -> WorkerCodingResultState:
        async with self._admission:
            if self._closed:
                raise WorkerStateUnavailable(_FAILURE)
            return await asyncio.get_running_loop().run_in_executor(
                self._executor, self._store.load_result_state
            )

    async def get_or_create_result(
        self,
        request: WorkerClaimRequest,
        candidate: WorkerCodingResultIntent,
        *,
        target_branch: str,
    ) -> WorkerCodingResultIntent:
        from functools import partial

        captured = deepcopy(candidate)
        owned = _capture_claim(request)
        async with self._admission:
            if self._closed:
                raise WorkerStateUnavailable(_FAILURE)
            return await asyncio.get_running_loop().run_in_executor(
                self._executor,
                partial(
                    self._store.get_or_create_result,
                    owned,
                    captured,
                    target_branch=target_branch,
                ),
            )

    async def record_result_acceptance(
        self, request: WorkerClaimRequest, receipt: CodingResultReceipt
    ) -> WorkerCodingResultState:
        captured = replace(receipt)
        owned = _capture_claim(request)
        async with self._admission:
            if self._closed:
                raise WorkerStateUnavailable(_FAILURE)
            return await asyncio.get_running_loop().run_in_executor(
                self._executor, self._store.record_result_acceptance, owned, captured
            )

    async def record_result_terminal(
        self, request: WorkerClaimRequest, status: CodingResultStatus
    ) -> WorkerCodingResultState:
        captured = deepcopy(status)
        owned = _capture_claim(request)
        async with self._admission:
            if self._closed:
                raise WorkerStateUnavailable(_FAILURE)
            return await asyncio.get_running_loop().run_in_executor(
                self._executor, self._store.record_result_terminal, owned, captured
            )

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

    async def get_or_create_heartbeat(
        self, request: WorkerClaimRequest, candidate: WorkerHeartbeatIntent
    ) -> WorkerHeartbeatIntent:
        async with self._admission:
            if self._closed:
                raise WorkerStateUnavailable(_FAILURE)
            return await asyncio.get_running_loop().run_in_executor(
                self._executor, self._store.get_or_create_heartbeat, request, candidate
            )

    async def get_or_create_execution(
        self, request: WorkerClaimRequest, candidate: WorkerExecutionIntent
    ) -> WorkerExecutionIntent:
        async with self._admission:
            if self._closed:
                raise WorkerStateUnavailable(_FAILURE)
            return await asyncio.get_running_loop().run_in_executor(
                self._executor, self._store.get_or_create_execution, request, candidate
            )

    async def get_or_create_workspace(
        self, request: WorkerClaimRequest, candidate: WorkerWorkspaceIntent
    ) -> WorkspacePreparationDecision:
        async with self._admission:
            if self._closed:
                raise WorkerStateUnavailable(_FAILURE)
            return await asyncio.get_running_loop().run_in_executor(
                self._executor, self._store.get_or_create_workspace, request, candidate
            )

    async def record_workspace_completion(
        self, request: WorkerClaimRequest, completion: WorkerWorkspaceCompletion
    ) -> WorkerWorkspaceCompletion:
        async with self._admission:
            if self._closed:
                raise WorkerStateUnavailable(_FAILURE)
            return await asyncio.get_running_loop().run_in_executor(
                self._executor,
                self._store.record_workspace_completion,
                request,
                completion,
            )

    async def record_heartbeat(
        self, request: WorkerClaimRequest, intent: WorkerHeartbeatIntent
    ) -> WorkerRecoveryState:
        async with self._admission:
            if self._closed:
                raise WorkerStateUnavailable(_FAILURE)
            return await asyncio.get_running_loop().run_in_executor(
                self._executor, self._store.record_heartbeat, request, intent
            )

    async def record_acknowledgement(
        self, request: WorkerClaimRequest, completion: AcknowledgementCompletion
    ) -> WorkerRecoveryState:
        async with self._admission:
            if self._closed:
                raise WorkerStateUnavailable(_FAILURE)
            return await asyncio.get_running_loop().run_in_executor(
                self._executor, self._store.record_acknowledgement, request, completion
            )

    async def record_outcome(
        self, request: WorkerClaimRequest, outcome: WorkerClaimOutcome
    ) -> WorkerRecoveryState:
        async with self._admission:
            if self._closed:
                raise WorkerStateUnavailable(_FAILURE)
            return await asyncio.get_running_loop().run_in_executor(
                self._executor, self._store.record_outcome, request, outcome
            )

    async def get_or_create_acknowledgement(
        self, request: WorkerClaimRequest, candidate: WorkerAcknowledgementIntent
    ) -> WorkerAcknowledgementIntent:
        async with self._admission:
            if self._closed:
                raise WorkerStateUnavailable(_FAILURE)
            return await asyncio.get_running_loop().run_in_executor(
                self._executor,
                self._store.get_or_create_acknowledgement,
                request,
                candidate,
            )
