import hashlib
import hmac
import json
import re
import sqlite3
from dataclasses import replace

from devrelay.application.contracts.claims import WorkerClaimRequest
from devrelay.application.contracts.coding_result_receipts import CodingResultAcceptance
from devrelay.application.contracts.coding_result_response_wire import (
    decode_coding_result_acceptance,
    decode_coding_result_status,
    encode_coding_result_acceptance,
    encode_coding_result_status,
)
from devrelay.application.contracts.coding_result_wire import (
    decode_coding_result_intent,
)
from devrelay.application.contracts.worker_recovery import WorkerRecoveryState
from devrelay.application.contracts.worker_result_recovery import (
    WorkerCodingResultState,
)
from devrelay.application.ports.worker_journal import WorkerStateUnavailable

_FAILURE = "Worker recovery journal is unavailable."


def create_result_schema(connection: sqlite3.Connection) -> None:
    connection.execute("""CREATE TABLE coding_result_input (
        slot INTEGER PRIMARY KEY CHECK(slot = 1),
        payload_bytes BLOB NOT NULL CHECK(typeof(payload_bytes) = 'blob' AND length(payload_bytes) BETWEEN 1 AND 16777216),
        payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256) = 64),
        target_branch TEXT NOT NULL CHECK(length(target_branch) BETWEEN 1 AND 256)
    )""")
    connection.execute("""CREATE TABLE coding_result_receipts (
        slot INTEGER PRIMARY KEY CHECK(slot IN (1,2)),
        payload_bytes BLOB NOT NULL CHECK(typeof(payload_bytes) = 'blob' AND length(payload_bytes) BETWEEN 1 AND 65536)
    )""")


def raw_result_state(
    connection: sqlite3.Connection,
) -> tuple[bytes | None, str | None, bytes | None, bytes | None, bool]:
    unknown = connection.execute(
        "SELECT result_history_unknown FROM metadata WHERE slot=1"
    ).fetchone()
    if unknown not in ((0,), (1,)):
        raise WorkerStateUnavailable(_FAILURE)
    inputs = connection.execute(
        "SELECT slot,typeof(payload_bytes),length(payload_bytes),substr(payload_bytes,1,16777217),"
        "typeof(payload_sha256),length(payload_sha256),substr(payload_sha256,1,65),"
        "typeof(target_branch),length(target_branch),substr(target_branch,1,257) FROM coding_result_input LIMIT 2"
    ).fetchall()
    receipts = connection.execute(
        "SELECT slot,typeof(payload_bytes),length(payload_bytes),substr(payload_bytes,1,65537) "
        "FROM coding_result_receipts ORDER BY slot LIMIT 3"
    ).fetchall()
    if len(inputs) > 1 or len(receipts) > 2:
        raise WorkerStateUnavailable(_FAILURE)
    payload, target = None, None
    if inputs:
        (
            slot,
            payload_type,
            payload_size,
            payload,
            digest_type,
            digest_size,
            digest,
            target_type,
            target_size,
            target,
        ) = inputs[0]
        if (
            slot != 1
            or payload_type != "blob"
            or type(payload_size) is not int
            or not 1 <= payload_size <= 16777216
            or digest_type != "text"
            or digest_size != 64
            or target_type != "text"
            or type(target_size) is not int
            or not 1 <= target_size <= 256
            or type(payload) is not bytes
            or not 1 <= len(payload) <= 16777216
            or type(digest) is not str
            or re.fullmatch(r"[0-9a-f]{64}", digest) is None
            or not hmac.compare_digest(hashlib.sha256(payload).hexdigest(), digest)
            or type(target) is not str
            or not 1 <= len(target) <= 256
        ):
            raise WorkerStateUnavailable(_FAILURE)
    values: dict[int, bytes] = {}
    for slot, body_type, body_size, body in receipts:
        if (
            slot not in (1, 2)
            or body_type != "blob"
            or type(body_size) is not int
            or not 1 <= body_size <= 65536
            or slot in values
            or type(body) is not bytes
            or not 1 <= len(body) <= 65536
        ):
            raise WorkerStateUnavailable(_FAILURE)
        values[slot] = body
    if (
        (payload is None and values)
        or (2 in values and 1 not in values)
        or (unknown == (1,) and (payload is not None or values))
    ):
        raise WorkerStateUnavailable(_FAILURE)
    return payload, target, values.get(1), values.get(2), unknown == (1,)


def result_integrity(
    connection: sqlite3.Connection, request: WorkerClaimRequest, previous: bytes
) -> bytes:
    payload, target, acceptance, terminal, unknown = raw_result_state(connection)
    evidence = json.dumps(
        {
            "history_unknown": unknown,
            "target_branch": target,
            "report_sha256": hashlib.sha256(payload).hexdigest()
            if payload is not None
            else None,
            "acceptance_sha256": hashlib.sha256(acceptance).hexdigest()
            if acceptance is not None
            else None,
            "terminal_sha256": hashlib.sha256(terminal).hexdigest()
            if terminal is not None
            else None,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hmac.digest(
        request.proof.reveal().encode("ascii"),
        b"devrelay.local-coding-result.v8\x00" + previous + evidence,
        "sha256",
    )


def bind_result_integrity(
    connection: sqlite3.Connection, request: WorkerClaimRequest, previous: bytes
) -> bytes:
    version = connection.execute("SELECT version FROM metadata WHERE slot=1").fetchone()
    return (
        result_integrity(connection, request, previous)
        if version in ((8,), (9,))
        else previous
    )


def require_absent_result(connection: sqlite3.Connection) -> None:
    if raw_result_state(connection) != (None, None, None, None, False):
        raise WorkerStateUnavailable(_FAILURE)


def read_result_state(
    connection: sqlite3.Connection, original: WorkerRecoveryState
) -> WorkerCodingResultState:
    payload, target, acceptance_bytes, terminal_bytes, unknown = raw_result_state(
        connection
    )
    intent = decode_coding_result_intent(payload) if payload is not None else None
    acceptance = (
        decode_coding_result_acceptance(acceptance_bytes)
        if acceptance_bytes is not None
        else None
    )
    terminal = (
        decode_coding_result_status(terminal_bytes)
        if terminal_bytes is not None
        else None
    )
    if acceptance is not None and (
        acceptance.recovered
        or encode_coding_result_acceptance(acceptance) != acceptance_bytes
    ):
        raise WorkerStateUnavailable(_FAILURE)
    if terminal is not None and encode_coding_result_status(terminal) != terminal_bytes:
        raise WorkerStateUnavailable(_FAILURE)
    return WorkerCodingResultState(
        original=original,
        intent=intent,
        target_branch=target,
        receipt=acceptance.receipt if acceptance else None,
        terminal_status=terminal,
        history_unknown=unknown,
    )


def save_result_receipts(
    connection: sqlite3.Connection, state: WorkerCodingResultState
) -> None:
    state.__post_init__()
    if state.receipt is not None:
        value = encode_coding_result_acceptance(
            CodingResultAcceptance(receipt=replace(state.receipt), recovered=False)
        )
        connection.execute(
            "INSERT INTO coding_result_receipts VALUES(1,?) ON CONFLICT(slot) DO NOTHING",
            (value,),
        )
    if state.terminal_status is not None:
        value = encode_coding_result_status(state.terminal_status)
        connection.execute(
            "INSERT INTO coding_result_receipts VALUES(2,?) ON CONFLICT(slot) DO NOTHING",
            (value,),
        )
