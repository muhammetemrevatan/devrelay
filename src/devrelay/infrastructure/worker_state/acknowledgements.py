import hmac
import json
import sqlite3
from uuid import UUID

from devrelay.application.contracts.acknowledgements import WorkerAcknowledgementIntent
from devrelay.application.contracts.claims import WorkerClaimRequest
from devrelay.application.contracts.worker_recovery import WorkerClaimOutcome
from devrelay.application.contracts.workers import WorkerJournalScope
from devrelay.application.ports.worker_journal import WorkerStateUnavailable

_FAILURE = "Worker recovery journal is unavailable."


def create_acknowledgement_schema(connection: sqlite3.Connection) -> None:
    connection.execute("""
      CREATE TABLE acknowledgement_intent (
        slot INTEGER PRIMARY KEY CHECK (slot = 1),
        request_id TEXT NOT NULL CHECK (length(request_id) = 36),
        claim_request_id TEXT NOT NULL CHECK (length(claim_request_id) = 36),
        attempt_id TEXT NOT NULL CHECK (length(attempt_id) = 36),
        generation INTEGER NOT NULL CHECK (generation = 1),
        snapshot_id TEXT NOT NULL CHECK (length(snapshot_id) = 36),
        base_revision TEXT NOT NULL CHECK (length(base_revision) IN (40, 64)),
        definition_digest TEXT NOT NULL CHECK (length(definition_digest) = 64),
        workflow_version INTEGER NOT NULL CHECK (workflow_version = 1),
        digest_version INTEGER NOT NULL CHECK (digest_version = 1),
        protocol_version INTEGER NOT NULL CHECK (protocol_version = 1),
        integrity BLOB NOT NULL CHECK (length(integrity) = 32)
      )
    """)


def _integrity(
    scope: WorkerJournalScope,
    request: WorkerClaimRequest,
    outcome: WorkerClaimOutcome,
    intent: WorkerAcknowledgementIntent,
) -> bytes:
    reference = outcome.assignment
    if reference is None:
        raise WorkerStateUnavailable(_FAILURE)
    payload = json.dumps(
        {
            "version": 3,
            "scope": scope.key.hex(),
            "worker_id": str(outcome.worker_id),
            "claim_request_id": str(intent.claim_request_id),
            "acknowledgement_request_id": str(intent.acknowledgement_request_id),
            "job_id": str(reference.job_id),
            "repository_id": str(reference.repository_id),
            "attempt_id": str(intent.attempt_id),
            "generation": intent.generation,
            "issued_at": reference.issued_at.isoformat(timespec="microseconds"),
            "expires_at": reference.expires_at.isoformat(timespec="microseconds"),
            "snapshot_id": str(intent.snapshot_id),
            "base_revision": intent.base_revision,
            "definition_digest": intent.definition_digest,
            "workflow_version": intent.expected_workflow_version,
            "digest_version": intent.definition_digest_version,
            "protocol_version": intent.protocol_version,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    return hmac.digest(
        request.proof.reveal().encode("ascii"),
        b"devrelay.local-acknowledgement.v3\x00" + payload,
        "sha256",
    )


def read_acknowledgement(
    connection: sqlite3.Connection,
    scope: WorkerJournalScope,
    request: WorkerClaimRequest | None,
    outcome: WorkerClaimOutcome | None,
) -> WorkerAcknowledgementIntent | None:
    row = connection.execute(
        "SELECT request_id, claim_request_id, attempt_id, generation, snapshot_id, base_revision, definition_digest, workflow_version, digest_version, protocol_version, integrity FROM acknowledgement_intent WHERE slot = 1"
    ).fetchone()
    if row is None:
        return None
    if request is None or outcome is None:
        raise WorkerStateUnavailable(_FAILURE)
    for index in (0, 1, 2, 4):
        if not isinstance(row[index], str) or str(UUID(row[index])) != row[index]:
            raise WorkerStateUnavailable(_FAILURE)
    intent = WorkerAcknowledgementIntent(
        acknowledgement_request_id=UUID(row[0]),
        claim_request_id=UUID(row[1]),
        attempt_id=UUID(row[2]),
        generation=row[3],
        snapshot_id=UUID(row[4]),
        base_revision=row[5],
        definition_digest=row[6],
        expected_workflow_version=row[7],
        definition_digest_version=row[8],
        protocol_version=row[9],
    )
    integrity = row[10]
    if (
        type(integrity) is not bytes
        or len(integrity) != 32
        or not hmac.compare_digest(
            integrity, _integrity(scope, request, outcome, intent)
        )
    ):
        raise WorkerStateUnavailable(_FAILURE)
    return intent


def acknowledgement_assignment_integrity(
    scope: WorkerJournalScope,
    request: WorkerClaimRequest,
    outcome: WorkerClaimOutcome,
    intent: WorkerAcknowledgementIntent,
) -> bytes:
    """Bind the retained assignment to intent so phase/row removal fails closed."""
    return hmac.digest(
        request.proof.reveal().encode("ascii"),
        b"devrelay.local-assignment-acknowledgement.v3\x00"
        + _integrity(scope, request, outcome, intent),
        "sha256",
    )


def save_acknowledgement(
    connection: sqlite3.Connection,
    scope: WorkerJournalScope,
    request: WorkerClaimRequest,
    outcome: WorkerClaimOutcome,
    intent: WorkerAcknowledgementIntent,
) -> None:
    connection.execute(
        "INSERT INTO acknowledgement_intent VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            str(intent.acknowledgement_request_id),
            str(intent.claim_request_id),
            str(intent.attempt_id),
            intent.generation,
            str(intent.snapshot_id),
            intent.base_revision,
            intent.definition_digest,
            intent.expected_workflow_version,
            intent.definition_digest_version,
            intent.protocol_version,
            _integrity(scope, request, outcome, intent),
        ),
    )
    connection.execute(
        "UPDATE assignment SET integrity = ? WHERE slot = 1",
        (acknowledgement_assignment_integrity(scope, request, outcome, intent),),
    )
    connection.execute(
        "UPDATE metadata SET phase = 'acknowledgement_pending' WHERE slot = 1"
    )
