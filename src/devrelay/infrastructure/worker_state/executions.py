import hmac
import json
import sqlite3
from uuid import UUID

from devrelay.application.contracts.claims import WorkerClaimRequest
from devrelay.application.contracts.executions import (
    ExecutionIsolation,
    ExecutionPolicy,
    ExecutionRuntime,
    WorkerExecutionIntent,
)
from devrelay.application.contracts.worker_recovery import (
    AcknowledgementCompletion,
    WorkerClaimOutcome,
    WorkerHeartbeatIntent,
)
from devrelay.application.contracts.workers import WorkerJournalScope
from devrelay.application.ports.worker_journal import WorkerStateUnavailable
from devrelay.infrastructure.worker_state.heartbeats import heartbeat_integrity

_FAILURE = "Worker recovery journal is unavailable."


def create_execution_schema(connection: sqlite3.Connection) -> None:
    connection.execute("""
        CREATE TABLE execution_input (
            slot INTEGER PRIMARY KEY CHECK(slot = 1),
            execution_id TEXT NOT NULL CHECK(length(execution_id) = 36),
            runtime TEXT NOT NULL CHECK(runtime = 'codex'),
            isolation TEXT NOT NULL CHECK(isolation IN ('windows_restricted', 'docker_linux')),
            policy_digest TEXT NOT NULL CHECK(length(policy_digest) = 64)
        )
    """)


def upgrade_execution_schema(connection: sqlite3.Connection) -> None:
    """Preserve every original execution pin; only admit the new backend value."""
    connection.execute("ALTER TABLE execution_input RENAME TO execution_input_previous")
    create_execution_schema(connection)
    connection.execute(
        "INSERT INTO execution_input SELECT * FROM execution_input_previous"
    )
    connection.execute("DROP TABLE execution_input_previous")


def read_execution(
    connection: sqlite3.Connection, completion: AcknowledgementCompletion | None
) -> WorkerExecutionIntent | None:
    rows = connection.execute(
        "SELECT slot, execution_id, runtime, isolation, policy_digest FROM execution_input"
    ).fetchall()
    if not rows:
        return None
    if len(rows) != 1 or rows[0][0] != 1 or completion is None:
        raise WorkerStateUnavailable(_FAILURE)
    _, execution_id, runtime, isolation, digest = rows[0]
    if type(execution_id) is not str or len(execution_id) != 36:
        raise WorkerStateUnavailable(_FAILURE)
    identity = UUID(execution_id)
    if str(identity) != execution_id:
        raise WorkerStateUnavailable(_FAILURE)
    return WorkerExecutionIntent(
        execution_id=identity,
        acknowledgement=completion.intent,
        policy=ExecutionPolicy(
            runtime=ExecutionRuntime(runtime),
            isolation=ExecutionIsolation(isolation),
            policy_digest=digest,
        ),
    )


def execution_integrity(
    scope: WorkerJournalScope,
    request: WorkerClaimRequest,
    outcome: WorkerClaimOutcome,
    completion: AcknowledgementCompletion,
    pending: WorkerHeartbeatIntent | None,
    last: WorkerHeartbeatIntent | None,
    execution: WorkerExecutionIntent | None,
) -> bytes:
    payload = json.dumps(
        None
        if execution is None
        else {
            "execution_id": str(execution.execution_id),
            "runtime": execution.policy.runtime.value,
            "isolation": execution.policy.isolation.value,
            "policy_digest": execution.policy.policy_digest,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    return hmac.digest(
        request.proof.reveal().encode("ascii"),
        b"devrelay.local-execution-input.v6\x00"
        + heartbeat_integrity(scope, request, outcome, completion, pending, last)
        + payload,
        "sha256",
    )


def save_execution(
    connection: sqlite3.Connection, intent: WorkerExecutionIntent
) -> None:
    connection.execute(
        "INSERT INTO execution_input VALUES (1, ?, ?, ?, ?)",
        (
            str(intent.execution_id),
            intent.policy.runtime.value,
            intent.policy.isolation.value,
            intent.policy.policy_digest,
        ),
    )
