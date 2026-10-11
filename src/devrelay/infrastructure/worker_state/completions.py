import hmac
import sqlite3
from datetime import UTC, datetime

from devrelay.application.contracts.acknowledgements import WorkerAcknowledgementIntent
from devrelay.application.contracts.claims import WorkerClaimRequest
from devrelay.application.contracts.worker_recovery import (
    AcknowledgementCompletion,
    WorkerClaimOutcome,
)
from devrelay.application.contracts.workers import WorkerJournalScope
from devrelay.application.ports.worker_journal import WorkerStateUnavailable
from devrelay.infrastructure.worker_state.acknowledgements import (
    acknowledgement_assignment_integrity,
)


def create_completion_schema(connection: sqlite3.Connection) -> None:
    connection.execute("""
        CREATE TABLE acknowledgement_completion (
            slot INTEGER PRIMARY KEY CHECK(slot = 1),
            acknowledged_at TEXT NOT NULL CHECK(length(acknowledged_at) = 32),
            workflow_version INTEGER NOT NULL CHECK(workflow_version = 2),
            integrity BLOB NOT NULL CHECK(length(integrity) = 32)
        )
    """)


def completion_integrity(
    scope: WorkerJournalScope,
    request: WorkerClaimRequest,
    outcome: WorkerClaimOutcome,
    completion: AcknowledgementCompletion,
) -> bytes:
    return hmac.digest(
        request.proof.reveal().encode("ascii"),
        b"devrelay.local-acknowledgement-completion.v4\x00"
        + acknowledgement_assignment_integrity(
            scope, request, outcome, completion.intent
        )
        + completion.acknowledged_at.astimezone(UTC)
        .isoformat(timespec="microseconds")
        .encode("ascii")
        + str(completion.workflow_version).encode("ascii"),
        "sha256",
    )


def read_completion(
    connection: sqlite3.Connection,
    scope: WorkerJournalScope,
    request: WorkerClaimRequest | None,
    outcome: WorkerClaimOutcome | None,
    intent: WorkerAcknowledgementIntent | None,
) -> AcknowledgementCompletion | None:
    row = connection.execute(
        "SELECT acknowledged_at, workflow_version, integrity FROM acknowledgement_completion WHERE slot = 1"
    ).fetchone()
    if row is None:
        return None
    if (
        request is None
        or outcome is None
        or not isinstance(intent, WorkerAcknowledgementIntent)
        or not isinstance(row[0], str)
    ):
        raise WorkerStateUnavailable("Worker recovery journal is unavailable.")
    instant = datetime.fromisoformat(row[0])
    completion = AcknowledgementCompletion(
        intent=intent, acknowledged_at=instant, workflow_version=row[1]
    )
    if (
        instant.tzinfo is None
        or completion.acknowledged_at.isoformat(timespec="microseconds") != row[0]
        or type(row[2]) is not bytes
        or not hmac.compare_digest(
            row[2], completion_integrity(scope, request, outcome, completion)
        )
    ):
        raise WorkerStateUnavailable("Worker recovery journal is unavailable.")
    return completion


def save_completion(
    connection: sqlite3.Connection,
    scope: WorkerJournalScope,
    request: WorkerClaimRequest,
    outcome: WorkerClaimOutcome,
    completion: AcknowledgementCompletion,
) -> None:
    integrity = completion_integrity(scope, request, outcome, completion)
    connection.execute(
        "INSERT INTO acknowledgement_completion VALUES (1, ?, ?, ?)",
        (
            completion.acknowledged_at.isoformat(timespec="microseconds"),
            completion.workflow_version,
            integrity,
        ),
    )
    # Binding the assignment detects completion erasure plus phase regression.
    connection.execute(
        "UPDATE assignment SET integrity = ? WHERE slot = 1", (integrity,)
    )
    connection.execute("UPDATE metadata SET phase = 'acknowledged' WHERE slot = 1")
