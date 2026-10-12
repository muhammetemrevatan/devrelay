import hmac
import json
import sqlite3
from uuid import UUID

from devrelay.application.contracts.claims import WorkerClaimRequest
from devrelay.application.contracts.worker_recovery import (
    AcknowledgementCompletion,
    WorkerClaimOutcome,
    WorkerHeartbeatIntent,
)
from devrelay.application.contracts.workers import WorkerJournalScope
from devrelay.application.ports.worker_journal import WorkerStateUnavailable
from devrelay.infrastructure.worker_state.completions import completion_integrity

_FAILURE = "Worker recovery journal is unavailable."


def create_heartbeat_schema(connection: sqlite3.Connection) -> None:
    connection.execute("""
        CREATE TABLE heartbeat_input (
            slot INTEGER PRIMARY KEY CHECK(slot IN (1, 2)),
            heartbeat_request_id TEXT NOT NULL CHECK(length(heartbeat_request_id) = 36),
            claim_request_id TEXT NOT NULL CHECK(length(claim_request_id) = 36),
            attempt_id TEXT NOT NULL CHECK(length(attempt_id) = 36),
            generation INTEGER NOT NULL CHECK(generation = 1),
            expected_lease_revision INTEGER NOT NULL CHECK(expected_lease_revision >= 1),
            protocol_version INTEGER NOT NULL CHECK(protocol_version = 1)
        )
    """)


def _payload(intent: WorkerHeartbeatIntent | None) -> dict[str, str | int] | None:
    if intent is None:
        return None
    return {
        "heartbeat_request_id": str(intent.heartbeat_request_id),
        "claim_request_id": str(intent.claim_request_id),
        "attempt_id": str(intent.attempt_id),
        "generation": intent.generation,
        "expected_lease_revision": intent.expected_lease_revision,
        "protocol_version": intent.protocol_version,
    }


def heartbeat_integrity(
    scope: WorkerJournalScope,
    request: WorkerClaimRequest,
    outcome: WorkerClaimOutcome,
    completion: AcknowledgementCompletion,
    pending: WorkerHeartbeatIntent | None,
    last: WorkerHeartbeatIntent | None,
) -> bytes:
    payload = json.dumps(
        {"pending": _payload(pending), "last": _payload(last)},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    return hmac.digest(
        request.proof.reveal().encode("ascii"),
        b"devrelay.local-heartbeat-inputs.v5\x00"
        + completion_integrity(scope, request, outcome, completion)
        + payload,
        "sha256",
    )


def _uuid(value: object) -> UUID:
    if type(value) is not str or len(value) != 36:
        raise WorkerStateUnavailable(_FAILURE)
    identity = UUID(value)
    if str(identity) != value:
        raise WorkerStateUnavailable(_FAILURE)
    return identity


def read_heartbeats(
    connection: sqlite3.Connection,
) -> tuple[WorkerHeartbeatIntent | None, WorkerHeartbeatIntent | None]:
    inputs: dict[int, WorkerHeartbeatIntent] = {}
    for row in connection.execute("SELECT * FROM heartbeat_input ORDER BY slot"):
        if type(row[0]) is not int or row[0] not in (1, 2) or row[0] in inputs:
            raise WorkerStateUnavailable(_FAILURE)
        inputs[row[0]] = WorkerHeartbeatIntent(
            heartbeat_request_id=_uuid(row[1]),
            claim_request_id=_uuid(row[2]),
            attempt_id=_uuid(row[3]),
            generation=row[4],
            expected_lease_revision=row[5],
            protocol_version=row[6],
        )
    return inputs.get(1), inputs.get(2)


def bind_heartbeats(
    connection: sqlite3.Connection,
    scope: WorkerJournalScope,
    request: WorkerClaimRequest,
    outcome: WorkerClaimOutcome,
    completion: AcknowledgementCompletion,
) -> None:
    pending, last = read_heartbeats(connection)
    integrity = heartbeat_integrity(scope, request, outcome, completion, pending, last)
    connection.execute(
        "UPDATE assignment SET integrity = ? WHERE slot = 1", (integrity,)
    )


def save_heartbeat(
    connection: sqlite3.Connection, intent: WorkerHeartbeatIntent, *, slot: int
) -> None:
    connection.execute(
        """INSERT INTO heartbeat_input VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(slot) DO UPDATE SET
             heartbeat_request_id=excluded.heartbeat_request_id,
             claim_request_id=excluded.claim_request_id,
             attempt_id=excluded.attempt_id, generation=excluded.generation,
             expected_lease_revision=excluded.expected_lease_revision,
             protocol_version=excluded.protocol_version""",
        (
            slot,
            str(intent.heartbeat_request_id),
            str(intent.claim_request_id),
            str(intent.attempt_id),
            intent.generation,
            intent.expected_lease_revision,
            intent.protocol_version,
        ),
    )
