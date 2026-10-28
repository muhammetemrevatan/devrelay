"""Strict synchronous coding-result codec; callers perform this work off-loop."""

import base64
import json
import re
from typing import cast
from uuid import UUID

from devrelay.application.contracts.acknowledgements import WorkerAcknowledgementIntent
from devrelay.application.contracts.base_sources import (
    BaseFileMode,
    BaseSourceFile,
    BaseSourceRequest,
    canonical_base_path,
)
from devrelay.application.contracts.candidates import (
    MAX_CANDIDATE_MESSAGE_BYTES,
    CandidateSnapshot,
    parse_candidate_commit,
)
from devrelay.application.contracts.claims import LeaseProof, WorkerClaimRequest
from devrelay.application.contracts.coding_result_requests import (
    MAX_CODING_RESULT_REQUEST_BYTES,
    WorkerCodingResultIntent,
    WorkerCodingResultRequest,
)
from devrelay.application.contracts.coding_results import (
    MAX_CHECK_DIAGNOSTIC_BYTES,
    MAX_CODING_CHECKS,
    MAX_CODING_CYCLES,
    MAX_CODING_DIAGNOSTIC_BYTES,
    CodingCheckEvidence,
    CodingCycleEvidence,
    SettledProcessEvidence,
    SuccessfulCodingResult,
)
from devrelay.application.contracts.executions import (
    ExecutionIsolation,
    ExecutionPolicy,
    ExecutionRuntime,
    WorkerExecutionIntent,
)
from devrelay.application.contracts.git_objects import (
    MAX_CANDIDATE_FILE_BYTES,
    MAX_CANDIDATE_FILES,
    MAX_CANDIDATE_TOTAL_BYTES,
)
from devrelay.application.contracts.processes import ProcessStopStatus
from devrelay.application.contracts.trusted_processes import TrustedProcessStopReason

_ERROR = "Invalid coding result payload."
_DECODE_ERRORS = (
    ValueError,
    TypeError,
    KeyError,
    AttributeError,
    OverflowError,
    RecursionError,
)
_COMMIT_BYTES = MAX_CANDIDATE_MESSAGE_BYTES + 2048
_INTENT_FIELDS = {
    "protocol_version",
    "result_request_id",
    "expected_workflow_version",
    "execution",
    "result",
}
_ACK_FIELDS = {
    "acknowledgement_request_id",
    "claim_request_id",
    "attempt_id",
    "generation",
    "snapshot_id",
    "base_revision",
    "definition_digest",
    "expected_workflow_version",
    "definition_digest_version",
    "protocol_version",
}
_SETTLEMENT_FIELDS = {
    "execution_id",
    "reason",
    "exit_code",
    "stop_status",
    "stdout_digest",
    "stderr_digest",
    "stdout_bytes",
    "stderr_bytes",
}
_CHECK_FIELDS = {
    "check_id",
    "revision",
    "candidate_digest",
    "settlement",
    "diagnostic_tail",
}
_CYCLE_FIELDS = {
    "cycle",
    "revision",
    "root_tree_sha",
    "candidate_digest",
    "raw_commit",
    "coding",
    "checks",
}
_RESULT_FIELDS = {
    "execution_id",
    "definition_digest",
    "check_policy_digest",
    "cycles",
    "final_candidate",
}
_CANDIDATE_FIELDS = {
    "execution_id",
    "cycle",
    "request",
    "authorized_branch",
    "root_tree_sha",
    "revision",
    "raw_commit",
    "files",
}
_FILE_FIELDS = {"path", "mode", "blob_sha", "size", "content"}


def _object(value: object, fields: set[str]) -> dict[str, object]:
    if type(value) is not dict or set(value) != fields:
        raise ValueError(_ERROR)
    return cast(dict[str, object], value)


def _array(value: object, maximum: int, minimum: int = 0) -> list[object]:
    if type(value) is not list or not minimum <= len(value) <= maximum:
        raise ValueError(_ERROR)
    return cast(list[object], value)


def _text(value: object, maximum: int) -> str:
    if type(value) is not str or len(value.encode("utf-8")) > maximum:
        raise ValueError(_ERROR)
    return value


def _integer(value: object, maximum: int, minimum: int = 0) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(_ERROR)
    return value


def _uuid(value: object) -> UUID:
    text = _text(value, 36)
    if len(text) != 36:
        raise ValueError(_ERROR)
    result = UUID(text)
    if str(result) != text:
        raise ValueError(_ERROR)
    return result


def _digest(value: object, length: int = 64) -> str:
    text = _text(value, length)
    if re.fullmatch(r"[0-9a-f]{" + str(length) + "}", text) is None:
        raise ValueError(_ERROR)
    return text


def _blob_field(value: object, maximum: int, minimum: int = 0) -> tuple[str, int]:
    text = _text(value, 4 * ((maximum + 2) // 3))
    if (
        len(text) % 4
        or not text.isascii()
        or re.fullmatch(r"[A-Za-z0-9+/]*={0,2}", text) is None
    ):
        raise ValueError(_ERROR)
    size = len(text) // 4 * 3 - (len(text) - len(text.rstrip("=")))
    if not minimum <= size <= maximum:
        raise ValueError(_ERROR)
    return text, size


def _blob(value: object, maximum: int, minimum: int = 0) -> bytes:
    text, size = _blob_field(value, maximum, minimum)
    result = base64.b64decode(text, validate=True)
    if len(result) != size or base64.b64encode(result).decode("ascii") != text:
        raise ValueError(_ERROR)
    return result


def _wire_shape(body: bytes) -> None:
    # Opaque encoded source strings do not contribute structural allocations.
    index = depth = containers = commas = 0
    while index < len(body):
        char = body[index]
        if char == 34:
            end = index + 1
            while True:
                end = body.find(b'"', end)
                if end < 0:
                    raise ValueError(_ERROR)
                cursor = end - 1
                while cursor > index and body[cursor] == 92:
                    cursor -= 1
                if (end - 1 - cursor) % 2 == 0:
                    break
                end += 1
            index = end
        elif char in (123, 91):
            depth += 1
            containers += 1
            if depth > 8 or containers > 2048:
                raise ValueError(_ERROR)
        elif char in (125, 93):
            depth -= 1
            if depth < 0:
                raise ValueError(_ERROR)
        elif char == 44:
            commas += 1
            if commas > 16384:
                raise ValueError(_ERROR)
        index += 1
    if depth:
        raise ValueError(_ERROR)


def _pairs(items: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in items:
        if key in result:
            raise ValueError(_ERROR)
        key.encode("utf-8")
        result[key] = value
    return result


def _reject_number(value: str) -> object:
    raise ValueError(_ERROR)


def _parse_integer(value: str) -> int:
    if len(value) > 20:
        raise ValueError(_ERROR)
    return int(value)


def _scalar_strings(value: object) -> None:
    if type(value) is str:
        value.encode("utf-8")
    elif type(value) is dict:
        for child in cast(dict[str, object], value).values():
            _scalar_strings(child)
    elif type(value) is list:
        for child in cast(list[object], value):
            _scalar_strings(child)


def _load(body: bytes) -> object:
    if type(body) is not bytes or not 1 <= len(body) <= MAX_CODING_RESULT_REQUEST_BYTES:
        raise ValueError(_ERROR)
    _wire_shape(body)
    value: object = json.loads(
        body.decode("utf-8"),
        object_pairs_hook=_pairs,
        parse_constant=_reject_number,
        parse_float=_reject_number,
        parse_int=_parse_integer,
    )
    _scalar_strings(value)
    return value


def _dump(payload: dict[str, object]) -> bytes:
    body = json.dumps(
        payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("ascii")
    if not 1 <= len(body) <= MAX_CODING_RESULT_REQUEST_BYTES:
        raise ValueError(_ERROR)
    _wire_shape(body)
    return body


def _ack_payload(value: WorkerAcknowledgementIntent) -> dict[str, object]:
    return {
        "acknowledgement_request_id": str(value.acknowledgement_request_id),
        "claim_request_id": str(value.claim_request_id),
        "attempt_id": str(value.attempt_id),
        "generation": value.generation,
        "snapshot_id": str(value.snapshot_id),
        "base_revision": value.base_revision,
        "definition_digest": value.definition_digest,
        "expected_workflow_version": value.expected_workflow_version,
        "definition_digest_version": value.definition_digest_version,
        "protocol_version": value.protocol_version,
    }


def _settlement_payload(value: SettledProcessEvidence) -> dict[str, object]:
    return {
        "execution_id": str(value.execution_id),
        "reason": value.reason.value,
        "exit_code": value.exit_code,
        "stop_status": value.stop_status.value,
        "stdout_digest": value.stdout_digest,
        "stderr_digest": value.stderr_digest,
        "stdout_bytes": value.stdout_bytes,
        "stderr_bytes": value.stderr_bytes,
    }


def _check_payload(value: CodingCheckEvidence) -> dict[str, object]:
    return {
        "check_id": value.check_id,
        "revision": value.revision,
        "candidate_digest": value.candidate_digest,
        "settlement": _settlement_payload(value.settlement),
        "diagnostic_tail": value.diagnostic_tail,
    }


def _cycle_payload(value: CodingCycleEvidence) -> dict[str, object]:
    return {
        "cycle": value.cycle,
        "revision": value.revision,
        "root_tree_sha": value.root_tree_sha,
        "candidate_digest": value.candidate_digest,
        "raw_commit": base64.b64encode(value.raw_commit).decode("ascii"),
        "coding": _settlement_payload(value.coding),
        "checks": [_check_payload(check) for check in value.checks],
    }


def _candidate_payload(value: CandidateSnapshot) -> dict[str, object]:
    return {
        "execution_id": str(value.execution_id),
        "cycle": value.cycle,
        "request": {
            "repository_id": str(value.request.repository_id),
            "provider_repository_id": value.request.provider_repository_id,
            "repository_slug": value.request.repository_slug,
            "base_revision": value.request.base_revision,
        },
        "authorized_branch": value.authorized_branch,
        "root_tree_sha": value.root_tree_sha,
        "revision": value.revision,
        "raw_commit": base64.b64encode(value.raw_commit).decode("ascii"),
        "files": [
            {
                "path": item.path,
                "mode": item.mode.value,
                "blob_sha": item.blob_sha,
                "size": len(item.content),
                "content": base64.b64encode(item.content).decode("ascii"),
            }
            for item in value.files
        ],
    }


def _intent_payload(value: WorkerCodingResultIntent) -> dict[str, object]:
    if not isinstance(value, WorkerCodingResultIntent):
        raise ValueError(_ERROR)
    value.__post_init__()
    _require_commit_history(value)
    execution, report = value.execution, value.result
    return {
        "protocol_version": value.protocol_version,
        "result_request_id": str(value.result_request_id),
        "expected_workflow_version": value.expected_workflow_version,
        "execution": {
            "execution_id": str(execution.execution_id),
            "acknowledgement": _ack_payload(execution.acknowledgement),
            "policy": {
                "runtime": execution.policy.runtime.value,
                "isolation": execution.policy.isolation.value,
                "policy_digest": execution.policy.policy_digest,
            },
        },
        "result": {
            "execution_id": str(report.execution_id),
            "definition_digest": report.definition_digest,
            "check_policy_digest": report.check_policy_digest,
            "cycles": [_cycle_payload(cycle) for cycle in report.cycles],
            "final_candidate": _candidate_payload(report.final_candidate),
        },
    }


def _parse_ack(value: object) -> WorkerAcknowledgementIntent:
    data = _object(value, _ACK_FIELDS)
    return WorkerAcknowledgementIntent(
        acknowledgement_request_id=_uuid(data["acknowledgement_request_id"]),
        claim_request_id=_uuid(data["claim_request_id"]),
        attempt_id=_uuid(data["attempt_id"]),
        generation=_integer(data["generation"], 1, 1),
        snapshot_id=_uuid(data["snapshot_id"]),
        base_revision=_digest(data["base_revision"], 40),
        definition_digest=_digest(data["definition_digest"]),
        expected_workflow_version=_integer(data["expected_workflow_version"], 1, 1),
        definition_digest_version=_integer(data["definition_digest_version"], 1, 1),
        protocol_version=_integer(data["protocol_version"], 1, 1),
    )


def _parse_execution(value: object) -> WorkerExecutionIntent:
    data = _object(value, {"execution_id", "acknowledgement", "policy"})
    policy = _object(data["policy"], {"runtime", "isolation", "policy_digest"})
    return WorkerExecutionIntent(
        execution_id=_uuid(data["execution_id"]),
        acknowledgement=_parse_ack(data["acknowledgement"]),
        policy=ExecutionPolicy(
            runtime=ExecutionRuntime(_text(policy["runtime"], 32)),
            isolation=ExecutionIsolation(_text(policy["isolation"], 32)),
            policy_digest=_digest(policy["policy_digest"]),
        ),
    )


def _parse_settlement(value: object) -> SettledProcessEvidence:
    data = _object(value, _SETTLEMENT_FIELDS)
    return SettledProcessEvidence(
        execution_id=_uuid(data["execution_id"]),
        reason=TrustedProcessStopReason(_text(data["reason"], 32)),
        exit_code=_integer(data["exit_code"], 0xFFFFFFFF),
        stop_status=ProcessStopStatus(_text(data["stop_status"], 32)),
        stdout_digest=_digest(data["stdout_digest"]),
        stderr_digest=_digest(data["stderr_digest"]),
        stdout_bytes=_integer(data["stdout_bytes"], 1024**2),
        stderr_bytes=_integer(data["stderr_bytes"], 1024**2),
    )


def _parse_check(value: object) -> CodingCheckEvidence:
    data = _object(value, _CHECK_FIELDS)
    return CodingCheckEvidence(
        check_id=_text(data["check_id"], 128),
        revision=_digest(data["revision"], 40),
        candidate_digest=_digest(data["candidate_digest"]),
        settlement=_parse_settlement(data["settlement"]),
        diagnostic_tail=_text(data["diagnostic_tail"], MAX_CHECK_DIAGNOSTIC_BYTES),
    )


def _parse_cycle(value: object) -> CodingCycleEvidence:
    data = _object(value, _CYCLE_FIELDS)
    return CodingCycleEvidence(
        cycle=_integer(data["cycle"], MAX_CODING_CYCLES - 1),
        revision=_digest(data["revision"], 40),
        root_tree_sha=_digest(data["root_tree_sha"], 40),
        candidate_digest=_digest(data["candidate_digest"]),
        raw_commit=_blob(data["raw_commit"], _COMMIT_BYTES, 1),
        coding=_parse_settlement(data["coding"]),
        checks=tuple(
            _parse_check(check)
            for check in _array(data["checks"], MAX_CODING_CHECKS, 1)
        ),
    )


def _preflight_files(value: object) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    total = 0
    for item in _array(value, MAX_CANDIDATE_FILES):
        data = _object(item, _FILE_FIELDS)
        size = _integer(data["size"], MAX_CANDIDATE_FILE_BYTES)
        _, encoded_size = _blob_field(data["content"], MAX_CANDIDATE_FILE_BYTES)
        total += size
        if size != encoded_size or total > MAX_CANDIDATE_TOTAL_BYTES:
            raise ValueError(_ERROR)
        canonical_base_path(_text(data["path"], 240))
        BaseFileMode(_text(data["mode"], 6))
        _digest(data["blob_sha"], 40)
        result.append(data)
    return result


def _parse_candidate(value: object) -> CandidateSnapshot:
    data = _object(value, _CANDIDATE_FIELDS)
    files = _preflight_files(data["files"])
    request = _object(
        data["request"],
        {"repository_id", "provider_repository_id", "repository_slug", "base_revision"},
    )
    return CandidateSnapshot(
        execution_id=_uuid(data["execution_id"]),
        cycle=_integer(data["cycle"], MAX_CODING_CYCLES - 1),
        request=BaseSourceRequest(
            repository_id=_uuid(request["repository_id"]),
            provider_repository_id=_integer(
                request["provider_repository_id"], 2**63 - 1, 1
            ),
            repository_slug=_text(request["repository_slug"], 256),
            base_revision=_digest(request["base_revision"], 40),
        ),
        authorized_branch=_text(data["authorized_branch"], 256),
        root_tree_sha=_digest(data["root_tree_sha"], 40),
        revision=_digest(data["revision"], 40),
        raw_commit=_blob(data["raw_commit"], _COMMIT_BYTES, 1),
        files=tuple(
            BaseSourceFile(
                path=_text(item["path"], 240),
                mode=BaseFileMode(_text(item["mode"], 6)),
                blob_sha=_digest(item["blob_sha"], 40),
                content=_blob(item["content"], MAX_CANDIDATE_FILE_BYTES),
            )
            for item in files
        ),
    )


def _preflight_cycles(value: object) -> list[object]:
    cycles = _array(value, MAX_CODING_CYCLES, 1)
    diagnostics = 0
    for cycle in cycles:
        data = _object(cycle, _CYCLE_FIELDS)
        for check in _array(data["checks"], MAX_CODING_CHECKS, 1):
            evidence = _object(check, _CHECK_FIELDS)
            diagnostics += len(
                _text(evidence["diagnostic_tail"], MAX_CHECK_DIAGNOSTIC_BYTES).encode(
                    "utf-8"
                )
            )
            if diagnostics > MAX_CODING_DIAGNOSTIC_BYTES:
                raise ValueError(_ERROR)
    return cycles


def _parse_result(value: object) -> SuccessfulCodingResult:
    data = _object(value, _RESULT_FIELDS)
    cycles = _preflight_cycles(data["cycles"])
    # All declared file/aggregate sizes are checked before any base64 decoding.
    candidate = _parse_candidate(data["final_candidate"])
    return SuccessfulCodingResult(
        execution_id=_uuid(data["execution_id"]),
        definition_digest=_digest(data["definition_digest"]),
        check_policy_digest=_digest(data["check_policy_digest"]),
        cycles=tuple(_parse_cycle(cycle) for cycle in cycles),
        final_candidate=candidate,
    )


def _parse_intent(
    data: dict[str, object], attempt_id: UUID | None = None
) -> WorkerCodingResultIntent:
    execution = _parse_execution(data["execution"])
    if attempt_id is not None and execution.acknowledgement.attempt_id != attempt_id:
        raise ValueError(_ERROR)
    intent = WorkerCodingResultIntent(
        result_request_id=_uuid(data["result_request_id"]),
        execution=execution,
        result=_parse_result(data["result"]),
        expected_workflow_version=_integer(data["expected_workflow_version"], 2, 2),
        protocol_version=_integer(data["protocol_version"], 1, 1),
    )
    _require_commit_history(intent)
    return intent


def _require_commit_history(intent: WorkerCodingResultIntent) -> None:
    for cycle in intent.result.cycles:
        parse_candidate_commit(
            cycle.raw_commit,
            parent=intent.execution.acknowledgement.base_revision,
            tree=cycle.root_tree_sha,
        )


def encode_coding_result_intent(intent: WorkerCodingResultIntent) -> bytes:
    """Canonical proof-free bytes for protected durable input, not a receipt."""
    try:
        return _dump(_intent_payload(intent))
    except _DECODE_ERRORS:
        raise ValueError(_ERROR) from None


def decode_coding_result_intent(body: bytes) -> WorkerCodingResultIntent:
    """Accept only byte-exact canonical durable input, without adding authority."""
    try:
        intent = _parse_intent(_object(_load(body), _INTENT_FIELDS))
        if encode_coding_result_intent(intent) != body:
            raise ValueError(_ERROR)
        return intent
    except _DECODE_ERRORS:
        raise ValueError(_ERROR) from None


def encode_coding_result_request(request: WorkerCodingResultRequest) -> bytes:
    """Only this transport boundary reveals the original proof into request bytes."""
    try:
        if not isinstance(request, WorkerCodingResultRequest):
            raise ValueError(_ERROR)
        request.__post_init__()
        payload = _intent_payload(request.intent)
        payload["proof"] = request.claim.proof.reveal()
        return _dump(payload)
    except _DECODE_ERRORS:
        raise ValueError(_ERROR) from None


def decode_coding_result_request(
    body: bytes, *, attempt_id: UUID
) -> WorkerCodingResultRequest:
    """Decode bounded transport input; authentication/current authority is external."""
    try:
        if not isinstance(attempt_id, UUID):
            raise ValueError(_ERROR)
        data = _object(_load(body), _INTENT_FIELDS | {"proof"})
        intent = _parse_intent(data, attempt_id)
        return WorkerCodingResultRequest(
            intent=intent,
            claim=WorkerClaimRequest(
                claim_request_id=intent.execution.acknowledgement.claim_request_id,
                proof=LeaseProof(_text(data["proof"], 43)),
                protocol_version=1,
            ),
        )
    except _DECODE_ERRORS:
        raise ValueError(_ERROR) from None
