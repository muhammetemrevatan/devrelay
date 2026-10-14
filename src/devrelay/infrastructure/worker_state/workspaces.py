import hmac
import json
import sqlite3
from uuid import UUID

from devrelay.application.contracts.claims import WorkerClaimRequest
from devrelay.application.contracts.executions import WorkerExecutionIntent
from devrelay.application.contracts.worker_recovery import (
    AcknowledgementCompletion,
    WorkerClaimOutcome,
    WorkerHeartbeatIntent,
)
from devrelay.application.contracts.workers import WorkerJournalScope
from devrelay.application.contracts.workspace_recovery import (
    WorkerWorkspaceCompletion,
    WorkerWorkspaceIntent,
    WorkspaceDirectoryIdentity,
    WorkspaceLocation,
)
from devrelay.application.ports.worker_journal import WorkerStateUnavailable
from devrelay.infrastructure.worker_state.executions import (
    execution_integrity,
    read_execution,
)
from devrelay.infrastructure.worker_state.heartbeats import read_heartbeats

_FAILURE = "Worker recovery journal is unavailable."


def create_workspace_schema(connection: sqlite3.Connection) -> None:
    connection.execute("""
        CREATE TABLE workspace_intent (
            slot INTEGER PRIMARY KEY CHECK(slot = 1),
            workspace_request_id TEXT NOT NULL CHECK(length(workspace_request_id) = 36),
            root_directory TEXT NOT NULL CHECK(length(root_directory) BETWEEN 4 AND 200),
            policy_digest TEXT NOT NULL CHECK(length(policy_digest) = 64),
            volume_serial_number INTEGER NOT NULL CHECK(volume_serial_number BETWEEN 0 AND 4294967295),
            file_index TEXT NOT NULL CHECK(length(file_index) BETWEEN 1 AND 20)
        )
    """)
    connection.execute("""
        CREATE TABLE workspace_completion (
            slot INTEGER PRIMARY KEY CHECK(slot = 1),
            volume_serial_number INTEGER NOT NULL CHECK(volume_serial_number BETWEEN 0 AND 4294967295),
            file_index TEXT NOT NULL CHECK(length(file_index) BETWEEN 1 AND 20)
        )
    """)


def workspace_history_unknown(connection: sqlite3.Connection) -> bool:
    row = connection.execute(
        "SELECT workspace_history_unknown FROM metadata WHERE slot = 1"
    ).fetchone()
    if row not in ((0,), (1,)):
        raise WorkerStateUnavailable(_FAILURE)
    return bool(row == (1,))


def _index(value: object) -> int:
    if (
        type(value) is not str
        or not 1 <= len(value) <= 20
        or not value.isascii()
        or not value.isdecimal()
    ):
        raise WorkerStateUnavailable(_FAILURE)
    index = int(value)
    if str(index) != value or not 0 < index < 2**64:
        raise WorkerStateUnavailable(_FAILURE)
    return index


def _uuid(value: object) -> UUID:
    if type(value) is not str or len(value) != 36:
        raise WorkerStateUnavailable(_FAILURE)
    identity = UUID(value)
    if str(identity) != value:
        raise WorkerStateUnavailable(_FAILURE)
    return identity


def read_workspace(
    connection: sqlite3.Connection, execution: WorkerExecutionIntent | None
) -> tuple[WorkerWorkspaceIntent | None, WorkerWorkspaceCompletion | None]:
    intents = connection.execute(
        "SELECT slot, workspace_request_id, root_directory, policy_digest, volume_serial_number, file_index FROM workspace_intent LIMIT 2"
    ).fetchall()
    completions = connection.execute(
        "SELECT slot, volume_serial_number, file_index FROM workspace_completion LIMIT 2"
    ).fetchall()
    if len(intents) > 1 or len(completions) > 1:
        raise WorkerStateUnavailable(_FAILURE)
    intent = None
    if intents:
        slot, identity, root, digest, volume, index = intents[0]
        if slot != 1 or execution is None:
            raise WorkerStateUnavailable(_FAILURE)
        intent = WorkerWorkspaceIntent(
            workspace_request_id=_uuid(identity),
            execution=execution,
            location=WorkspaceLocation(
                root_directory=root,
                policy_digest=digest,
                identity=WorkspaceDirectoryIdentity(
                    volume_serial_number=volume, file_index=_index(index)
                ),
            ),
        )
    completion = None
    if completions:
        slot, volume, index = completions[0]
        if slot != 1 or intent is None:
            raise WorkerStateUnavailable(_FAILURE)
        completion = WorkerWorkspaceCompletion(
            intent=intent,
            identity=WorkspaceDirectoryIdentity(
                volume_serial_number=volume, file_index=_index(index)
            ),
        )
    return intent, completion


def workspace_integrity(
    scope: WorkerJournalScope,
    request: WorkerClaimRequest,
    outcome: WorkerClaimOutcome,
    completion: AcknowledgementCompletion,
    pending: WorkerHeartbeatIntent | None,
    last: WorkerHeartbeatIntent | None,
    execution: WorkerExecutionIntent | None,
    intent: WorkerWorkspaceIntent | None,
    workspace_completion: WorkerWorkspaceCompletion | None,
    history_unknown: bool,
) -> bytes:
    payload = json.dumps(
        {
            "history_unknown": history_unknown,
            "intent": None
            if intent is None
            else {
                "workspace_request_id": str(intent.workspace_request_id),
                "root_directory": intent.location.root_directory,
                "policy_digest": intent.location.policy_digest,
                "volume_serial_number": intent.location.identity.volume_serial_number,
                "file_index": intent.location.identity.file_index,
            },
            "completion": None
            if workspace_completion is None
            else {
                "volume_serial_number": workspace_completion.identity.volume_serial_number,
                "file_index": workspace_completion.identity.file_index,
            },
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    return hmac.digest(
        request.proof.reveal().encode("ascii"),
        b"devrelay.local-workspace-input.v7\x00"
        + execution_integrity(
            scope, request, outcome, completion, pending, last, execution
        )
        + payload,
        "sha256",
    )


def bind_workspace_state(
    connection: sqlite3.Connection,
    scope: WorkerJournalScope,
    request: WorkerClaimRequest,
    outcome: WorkerClaimOutcome,
    completion: AcknowledgementCompletion,
) -> None:
    execution = read_execution(connection, completion)
    intent, recorded = read_workspace(connection, execution)
    pending, last = read_heartbeats(connection)
    marker = workspace_integrity(
        scope,
        request,
        outcome,
        completion,
        pending,
        last,
        execution,
        intent,
        recorded,
        workspace_history_unknown(connection),
    )
    connection.execute("UPDATE assignment SET integrity = ? WHERE slot = 1", (marker,))


def save_workspace_intent(
    connection: sqlite3.Connection, intent: WorkerWorkspaceIntent
) -> None:
    location = intent.location
    connection.execute(
        "INSERT INTO workspace_intent VALUES (1, ?, ?, ?, ?, ?)",
        (
            str(intent.workspace_request_id),
            location.root_directory,
            location.policy_digest,
            location.identity.volume_serial_number,
            str(location.identity.file_index),
        ),
    )


def save_workspace_completion(
    connection: sqlite3.Connection, completion: WorkerWorkspaceCompletion
) -> None:
    connection.execute(
        "INSERT INTO workspace_completion VALUES (1, ?, ?)",
        (
            completion.identity.volume_serial_number,
            str(completion.identity.file_index),
        ),
    )
