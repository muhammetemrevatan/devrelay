import hmac
import json
import sqlite3
from datetime import UTC, datetime
from uuid import UUID

from devrelay.application.contracts.assignments import NoWorkReason
from devrelay.application.contracts.claims import WorkerClaimRequest
from devrelay.application.contracts.worker_recovery import (
    AssignmentReference,
    WorkerClaimOutcome,
    WorkerRecoveryPhase,
    WorkerRecoveryState,
)
from devrelay.application.contracts.workers import WorkerJournalScope
from devrelay.application.ports.worker_journal import WorkerStateUnavailable
from devrelay.infrastructure.worker_state.acknowledgements import (
    acknowledgement_assignment_integrity,
    create_acknowledgement_schema,
    read_acknowledgement,
)
from devrelay.infrastructure.worker_state.completions import (
    completion_integrity,
    create_completion_schema,
    read_completion,
)

_FAILURE = "Worker recovery journal is unavailable."


def _create_metadata(
    connection: sqlite3.Connection,
    scope: bytes,
    *,
    prepared: int,
    phase: str,
    completed: int,
) -> None:
    connection.execute("""
      CREATE TABLE metadata (
        slot INTEGER PRIMARY KEY CHECK (slot = 1),
        version INTEGER NOT NULL CHECK (version = 4),
        scope BLOB NOT NULL CHECK (length(scope) = 32),
        prepared INTEGER NOT NULL CHECK (prepared IN (0, 1)),
        phase TEXT NOT NULL CHECK (phase IN ('idle', 'pending', 'assigned', 'acknowledgement_pending', 'acknowledged')),
        completed INTEGER NOT NULL CHECK (completed IN (0, 1)),
        CHECK ((phase = 'idle' AND prepared = 0) OR (phase IN ('pending', 'assigned', 'acknowledgement_pending', 'acknowledged') AND prepared = 1))
      )
    """)
    connection.execute(
        "INSERT INTO metadata VALUES (1, 4, ?, ?, ?, ?)",
        (scope, prepared, phase, completed),
    )


def upgrade_outcome_schema(connection: sqlite3.Connection, scope: bytes) -> None:
    version = connection.execute(
        "SELECT version FROM metadata WHERE slot = 1"
    ).fetchone()
    row = connection.execute(
        "SELECT prepared, phase, completed FROM metadata WHERE slot = 1"
    ).fetchone()
    if row is None:
        raise WorkerStateUnavailable(_FAILURE)
    connection.execute("DROP TABLE metadata")
    _create_metadata(connection, scope, prepared=row[0], phase=row[1], completed=row[2])
    if version == (2,):
        create_acknowledgement_schema(connection)
    create_completion_schema(connection)


def create_outcome_schema(
    connection: sqlite3.Connection, scope: bytes, *, prepared: int
) -> None:
    _create_metadata(
        connection,
        scope,
        prepared=prepared,
        phase="pending" if prepared else "idle",
        completed=0,
    )
    create_acknowledgement_schema(connection)
    create_completion_schema(connection)
    connection.execute("""
      CREATE TABLE assignment (
        slot INTEGER PRIMARY KEY CHECK (slot = 1),
        request_id TEXT NOT NULL CHECK (length(request_id) = 36),
        job_id TEXT NOT NULL CHECK (length(job_id) = 36),
        repository_id TEXT NOT NULL CHECK (length(repository_id) = 36),
        attempt_id TEXT NOT NULL CHECK (length(attempt_id) = 36),
        generation INTEGER NOT NULL CHECK (generation = 1),
        issued_at TEXT NOT NULL CHECK (length(issued_at) = 32),
        expires_at TEXT NOT NULL CHECK (length(expires_at) = 32),
        integrity BLOB NOT NULL CHECK (length(integrity) = 32)
      )
    """)
    connection.execute("""
      CREATE TABLE completed_no_work (
        slot INTEGER PRIMARY KEY CHECK (slot = 1),
        request_id TEXT NOT NULL CHECK (length(request_id) = 36),
        protocol_version INTEGER NOT NULL CHECK (protocol_version = 1),
        verifier BLOB NOT NULL CHECK (length(verifier) = 32),
        reason TEXT NOT NULL CHECK (reason IN ('empty', 'occupied', 'changed', 'stale')),
        recorded_at TEXT NOT NULL CHECK (length(recorded_at) = 32)
      )
    """)


def _uuid(value: object) -> UUID:
    if not isinstance(value, str) or len(value) != 36:
        raise WorkerStateUnavailable(_FAILURE)
    identity = UUID(value)
    if str(identity) != value:
        raise WorkerStateUnavailable(_FAILURE)
    return identity


def timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds")


def _time(value: object) -> datetime:
    if not isinstance(value, str) or len(value) != 32:
        raise WorkerStateUnavailable(_FAILURE)
    instant = datetime.fromisoformat(value)
    if instant.tzinfo is None or timestamp(instant) != value:
        raise WorkerStateUnavailable(_FAILURE)
    return instant


def recovery_state(
    connection: sqlite3.Connection,
    scope: WorkerJournalScope,
    request: WorkerClaimRequest | None,
    *,
    allow_legacy: bool = False,
) -> WorkerRecoveryState:
    header = connection.execute(
        "SELECT version, scope, prepared, phase, completed FROM metadata WHERE slot = 1"
    ).fetchone()
    if (
        header is None
        or header[0] not in ((2, 3, 4) if allow_legacy else (4,))
        or header[1] != scope.key
    ):
        raise WorkerStateUnavailable(_FAILURE)
    _, _, prepared, phase, completed = header
    if (
        prepared not in (0, 1)
        or completed not in (0, 1)
        or bool(prepared) != (request is not None)
    ):
        raise WorkerStateUnavailable(_FAILURE)
    assigned = connection.execute(
        "SELECT request_id, job_id, repository_id, attempt_id, generation, issued_at, expires_at, integrity FROM assignment WHERE slot = 1"
    ).fetchone()
    finished = connection.execute(
        "SELECT request_id, protocol_version, verifier, reason, recorded_at FROM completed_no_work WHERE slot = 1"
    ).fetchone()
    if bool(completed) != (finished is not None) or (
        phase in ("assigned", "acknowledgement_pending", "acknowledged")
    ) != (assigned is not None):
        raise WorkerStateUnavailable(_FAILURE)
    previous = None
    if finished is not None:
        request_id, version, verifier, reason, recorded_at = finished
        if version != 1 or type(verifier) is not bytes or len(verifier) != 32:
            raise WorkerStateUnavailable(_FAILURE)
        previous = WorkerClaimOutcome(
            worker_id=scope.worker_id,
            claim_request_id=_uuid(request_id),
            recorded_at=_time(recorded_at),
            no_work=NoWorkReason(reason),
        )
        if (
            request is not None
            and request.claim_request_id == previous.claim_request_id
        ):
            raise WorkerStateUnavailable(_FAILURE)
    outcome = None
    acknowledgement = None
    completion = None
    if assigned is not None:
        (
            request_id,
            job_id,
            repo_id,
            attempt_id,
            generation,
            issued_at,
            expires_at,
            integrity,
        ) = assigned
        reference = AssignmentReference(
            worker_id=scope.worker_id,
            job_id=_uuid(job_id),
            repository_id=_uuid(repo_id),
            attempt_id=_uuid(attempt_id),
            generation=generation,
            issued_at=_time(issued_at),
            expires_at=_time(expires_at),
        )
        outcome = WorkerClaimOutcome(
            worker_id=scope.worker_id,
            claim_request_id=_uuid(request_id),
            recorded_at=reference.issued_at,
            assignment=reference,
        )
        if request is None:
            raise WorkerStateUnavailable(_FAILURE)
        acknowledgement = (
            read_acknowledgement(connection, scope, request, outcome)
            if header[0] >= 3
            else None
        )
        completion = (
            read_completion(connection, scope, request, outcome, acknowledgement)
            if header[0] == 4
            else None
        )
        expected_integrity = (
            completion_integrity(scope, request, outcome, completion)
            if completion is not None
            else assignment_integrity(scope, request, outcome)
            if acknowledgement is None
            else acknowledgement_assignment_integrity(
                scope, request, outcome, acknowledgement
            )
        )
        if (
            type(integrity) is not bytes
            or len(integrity) != 32
            or not hmac.compare_digest(integrity, expected_integrity)
        ):
            raise WorkerStateUnavailable(_FAILURE)
    elif header[0] >= 3:
        acknowledgement = read_acknowledgement(connection, scope, request, outcome)
        if header[0] == 4:
            completion = read_completion(
                connection, scope, request, outcome, acknowledgement
            )
    return WorkerRecoveryState(
        phase=WorkerRecoveryPhase(phase),
        request=request,
        assignment=outcome,
        last_no_work=previous,
        acknowledgement=acknowledgement,
        completion=completion,
    )


def assignment_integrity(
    scope: WorkerJournalScope, request: WorkerClaimRequest, outcome: WorkerClaimOutcome
) -> bytes:
    reference = outcome.assignment
    if reference is None:
        raise WorkerStateUnavailable(_FAILURE)
    payload = json.dumps(
        {
            "version": 2,
            "scope": scope.key.hex(),
            "worker_id": str(outcome.worker_id),
            "request_id": str(outcome.claim_request_id),
            "protocol": outcome.protocol_version,
            "job_id": str(reference.job_id),
            "repository_id": str(reference.repository_id),
            "attempt_id": str(reference.attempt_id),
            "generation": reference.generation,
            "issued_at": timestamp(reference.issued_at),
            "expires_at": timestamp(reference.expires_at),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    return hmac.digest(
        request.proof.reveal().encode("ascii"),
        b"devrelay.local-assignment.v2\x00" + payload,
        "sha256",
    )


def save_assignment(
    connection: sqlite3.Connection,
    scope: WorkerJournalScope,
    request: WorkerClaimRequest,
    outcome: WorkerClaimOutcome,
) -> None:
    reference = outcome.assignment
    if reference is None:
        raise WorkerStateUnavailable(_FAILURE)
    connection.execute(
        "INSERT INTO assignment VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            str(outcome.claim_request_id),
            str(reference.job_id),
            str(reference.repository_id),
            str(reference.attempt_id),
            reference.generation,
            timestamp(reference.issued_at),
            timestamp(reference.expires_at),
            assignment_integrity(scope, request, outcome),
        ),
    )
    connection.execute("UPDATE metadata SET phase = 'assigned' WHERE slot = 1")


def save_no_work(
    connection: sqlite3.Connection, outcome: WorkerClaimOutcome, verifier: bytes
) -> None:
    if outcome.no_work is None:
        raise WorkerStateUnavailable(_FAILURE)
    connection.execute(
        """
      INSERT INTO completed_no_work VALUES (1, ?, 1, ?, ?, ?)
      ON CONFLICT(slot) DO UPDATE SET request_id=excluded.request_id,
        verifier=excluded.verifier, reason=excluded.reason, recorded_at=excluded.recorded_at
    """,
        (
            str(outcome.claim_request_id),
            verifier,
            outcome.no_work.value,
            timestamp(outcome.recorded_at),
        ),
    )
    connection.execute("DELETE FROM pending_claim WHERE slot = 1")
    connection.execute(
        "UPDATE metadata SET prepared = 0, phase = 'idle', completed = 1 WHERE slot = 1"
    )
