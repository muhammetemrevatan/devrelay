"""Bounded pure metadata codec, independent of current worker authorization."""

import json
from dataclasses import fields
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID

from devrelay.application.contracts.coding_result_receipts import (
    CodingPublicationState,
    CodingResultAcceptance,
    CodingResultReceipt,
    CodingResultStatus,
    TerminalCodingResult,
)
from devrelay.domain.jobs.values import PullRequestReference

MAX_CODING_RESULT_RESPONSE_BYTES = 64 * 1024
_ERROR = "Invalid coding result response."
_ERRORS = (
    ValueError,
    TypeError,
    AttributeError,
    KeyError,
    OverflowError,
    RecursionError,
)
_RECEIPT_FIELDS = {item.name for item in fields(CodingResultReceipt)}
_TERMINAL_FIELDS = {
    "repository_id",
    "number",
    "branch",
    "target_branch",
    "head_revision",
    "finalized_at",
    "workflow_version",
    "release_ordinal",
}


def _object(value: object, names: set[str]) -> dict[str, object]:
    if type(value) is not dict or set(value) != names:
        raise ValueError(_ERROR)
    return cast(dict[str, object], value)


def _text(value: object, maximum: int = 256) -> str:
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


def _date(value: object) -> datetime:
    text = _text(value, 40)
    parsed = datetime.fromisoformat(text)
    if parsed.utcoffset() != timedelta(0):
        raise ValueError(_ERROR)
    canonical = parsed.astimezone(UTC).isoformat()
    if text not in (canonical, canonical.removesuffix("+00:00") + "Z"):
        raise ValueError(_ERROR)
    return parsed.astimezone(UTC)


def _pairs(items: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in items:
        if key in result:
            raise ValueError(_ERROR)
        key.encode("utf-8")
        if type(value) is str:
            value.encode("utf-8")
        result[key] = value
    return result


def _reject(value: str) -> object:
    raise ValueError(_ERROR)


def _parse_integer(value: str) -> int:
    if len(value) > 20:
        raise ValueError(_ERROR)
    return int(value)


def _shape(body: bytes) -> None:
    index = depth = count = commas = 0
    while index < len(body):
        byte = body[index]
        if byte == 34:
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
        elif byte in (123, 91):
            depth += 1
            count += 1
            if depth > 4 or count > 32:
                raise ValueError(_ERROR)
        elif byte in (125, 93):
            depth -= 1
            if depth < 0:
                raise ValueError(_ERROR)
        elif byte == 44:
            commas += 1
            if commas > 256:
                raise ValueError(_ERROR)
        index += 1
    if depth:
        raise ValueError(_ERROR)


def _load(body: bytes) -> object:
    if (
        type(body) is not bytes
        or not 1 <= len(body) <= MAX_CODING_RESULT_RESPONSE_BYTES
    ):
        raise ValueError(_ERROR)
    _shape(body)
    result: object = json.loads(
        body.decode("utf-8"),
        object_pairs_hook=_pairs,
        parse_float=_reject,
        parse_constant=_reject,
        parse_int=_parse_integer,
    )
    return result


def _dump(payload: dict[str, object]) -> bytes:
    result = json.dumps(
        payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("ascii")
    if len(result) > MAX_CODING_RESULT_RESPONSE_BYTES:
        raise ValueError(_ERROR)
    return result


def _receipt_payload(value: CodingResultReceipt) -> dict[str, object]:
    value.__post_init__()
    return {
        item.name: str(attribute)
        if isinstance(attribute, UUID)
        else attribute.isoformat()
        if isinstance(attribute, datetime)
        else attribute
        for item in fields(value)
        for attribute in (getattr(value, item.name),)
    }


def _receipt(value: object) -> CodingResultReceipt:
    data = _object(value, _RECEIPT_FIELDS)
    return CodingResultReceipt(
        result_id=_uuid(data["result_id"]),
        worker_id=_uuid(data["worker_id"]),
        request_id=_uuid(data["request_id"]),
        claim_request_id=_uuid(data["claim_request_id"]),
        job_id=_uuid(data["job_id"]),
        attempt_id=_uuid(data["attempt_id"]),
        repository_id=_uuid(data["repository_id"]),
        execution_id=_uuid(data["execution_id"]),
        definition_digest=_text(data["definition_digest"], 64),
        request_fingerprint=_text(data["request_fingerprint"], 64),
        candidate_revision=_text(data["candidate_revision"], 40),
        repair_cycles=_integer(data["repair_cycles"], 5),
        publishing_workflow_version=_integer(data["publishing_workflow_version"], 14),
        recorded_at=_date(data["recorded_at"]),
        protocol_version=_integer(data["protocol_version"], 1, 1),
    )


def encode_coding_result_acceptance(value: CodingResultAcceptance) -> bytes:
    try:
        if not isinstance(value, CodingResultAcceptance):
            raise ValueError(_ERROR)
        value.__post_init__()
        return _dump(
            {"receipt": _receipt_payload(value.receipt), "recovered": value.recovered}
        )
    except _ERRORS:
        raise ValueError(_ERROR) from None


def decode_coding_result_acceptance(body: bytes) -> CodingResultAcceptance:
    try:
        data = _object(_load(body), {"receipt", "recovered"})
        if type(data["recovered"]) is not bool:
            raise ValueError(_ERROR)
        return CodingResultAcceptance(
            receipt=_receipt(data["receipt"]), recovered=data["recovered"]
        )
    except _ERRORS:
        raise ValueError(_ERROR) from None


def encode_coding_result_status(value: CodingResultStatus) -> bytes:
    try:
        if not isinstance(value, CodingResultStatus):
            raise ValueError(_ERROR)
        value.__post_init__()
        terminal = value.terminal
        payload = (
            {
                "repository_id": str(terminal.reference.repository_id),
                "number": _integer(terminal.reference.number, 2**63 - 1, 1),
                "branch": terminal.reference.source_branch,
                "target_branch": terminal.reference.target_branch,
                "head_revision": terminal.reference.head_revision,
                "finalized_at": terminal.finalized_at.isoformat(),
                "workflow_version": terminal.workflow_version,
                "release_ordinal": terminal.release_ordinal,
            }
            if terminal is not None
            else None
        )
        return _dump(
            {
                "receipt": _receipt_payload(value.receipt),
                "state": value.state.value,
                "operation_version": value.operation_version,
                "terminal": payload,
            }
        )
    except _ERRORS:
        raise ValueError(_ERROR) from None


def decode_coding_result_status(body: bytes) -> CodingResultStatus:
    try:
        data = _object(
            _load(body), {"receipt", "state", "operation_version", "terminal"}
        )
        receipt = _receipt(data["receipt"])
        terminal = None
        if data["terminal"] is not None:
            raw = _object(data["terminal"], _TERMINAL_FIELDS)
            terminal = TerminalCodingResult(
                receipt=receipt,
                reference=PullRequestReference(
                    repository_id=_uuid(raw["repository_id"]),
                    number=_integer(raw["number"], 2**63 - 1, 1),
                    source_branch=_text(raw["branch"]),
                    target_branch=_text(raw["target_branch"]),
                    head_revision=_text(raw["head_revision"], 40),
                ),
                finalized_at=_date(raw["finalized_at"]),
                workflow_version=_integer(raw["workflow_version"], 15, 5),
                release_ordinal=_integer(raw["release_ordinal"], 2**63 - 1, 1),
            )
        return CodingResultStatus(
            receipt=receipt,
            state=CodingPublicationState(_text(data["state"], 16)),
            operation_version=_integer(data["operation_version"], 2047),
            terminal=terminal,
        )
    except _ERRORS:
        raise ValueError(_ERROR) from None
