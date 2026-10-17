import base64
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import cast
from uuid import UUID

from devrelay.application.contracts.base_sources import (
    BaseFileMode,
    BaseSourceFile,
    BaseSourceLimits,
    BaseSourceRequest,
    BaseSourceSnapshot,
)
from devrelay.application.contracts.git_commits import MAX_GIT_COMMIT_BYTES
from devrelay.application.contracts.worker_recovery import AssignmentReference

MAX_BASE_DELIVERY_BYTES = 12 * 1024**2
BASE_DELIVERY_LIMITS = BaseSourceLimits(max_total_bytes=8 * 1024**2)


@dataclass(frozen=True, slots=True, kw_only=True)
class BaseSourceDelivery:
    """Finite literal source bytes bound to original assignment; no live authority."""

    reference: AssignmentReference
    claim_request_id: UUID
    definition_digest: str
    snapshot: BaseSourceSnapshot

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        if (
            not isinstance(self.reference, AssignmentReference)
            or not isinstance(self.claim_request_id, UUID)
            or type(self.definition_digest) is not str
            or re.fullmatch(r"[0-9a-f]{64}", self.definition_digest) is None
            or not isinstance(self.snapshot, BaseSourceSnapshot)
        ):
            raise ValueError("Base delivery requires original assignment pins.")
        self.reference.__post_init__()
        self.snapshot.validate()
        if (
            self.snapshot.request.repository_id != self.reference.repository_id
            or len(self.snapshot.files) > BASE_DELIVERY_LIMITS.max_files
            or any(
                len(item.content) > BASE_DELIVERY_LIMITS.max_file_bytes
                for item in self.snapshot.files
            )
            or sum(len(item.content) for item in self.snapshot.files)
            > BASE_DELIVERY_LIMITS.max_total_bytes
        ):
            raise ValueError("Base delivery changed scope or exceeded channel limits.")


def encode_base_delivery(value: BaseSourceDelivery) -> bytes:
    if not isinstance(value, BaseSourceDelivery):
        raise ValueError("A typed base delivery is required.")
    value.validate()
    reference = {
        name: item.isoformat()
        if isinstance(item, datetime)
        else str(item)
        if isinstance(item, UUID)
        else item
        for name, item in asdict(value.reference).items()
    }
    request = {
        name: str(item) if isinstance(item, UUID) else item
        for name, item in asdict(value.snapshot.request).items()
    }
    payload = {
        "protocol_version": 1 if value.snapshot.raw_commit is None else 2,
        "reference": reference,
        "claim_request_id": str(value.claim_request_id),
        "definition_digest": value.definition_digest,
        "source": request,
        "root_tree_sha": value.snapshot.root_tree_sha,
        "manifest_digest": value.snapshot.fingerprint(),
        "files": [
            {
                "path": item.path,
                "mode": item.mode.value,
                "blob_sha": item.blob_sha,
                "size": len(item.content),
                "content": base64.b64encode(item.content).decode("ascii"),
            }
            for item in value.snapshot.files
        ],
    }
    if value.snapshot.raw_commit is not None:
        payload["raw_commit"] = base64.b64encode(value.snapshot.raw_commit).decode(
            "ascii"
        )
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(
        "utf-8"
    )
    if len(body) > MAX_BASE_DELIVERY_BYTES:
        raise ValueError("Base delivery exceeds encoded channel limits.")
    return body


def _pairs(items: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for name, value in items:
        if name in result:
            raise ValueError("Base delivery contains duplicate fields.")
        result[name] = value
    return result


def _constant(value: str) -> object:
    raise ValueError("Base delivery contains invalid JSON values.")


def _object(value: object, fields: set[str]) -> dict[str, object]:
    if type(value) is not dict or set(value) != fields:
        raise ValueError("Base delivery fields are invalid.")
    return cast(dict[str, object], value)


def _text(value: object) -> str:
    if type(value) is not str:
        raise ValueError("Base delivery requires literal text.")
    return value


def _uuid(value: object) -> UUID:
    text = _text(value)
    if len(text) != 36:
        raise ValueError("Base delivery requires canonical identities.")
    parsed = UUID(text)
    if str(parsed) != text:
        raise ValueError("Base delivery requires canonical identities.")
    return parsed


def _date(value: object) -> datetime:
    text = _text(value)
    if not 20 <= len(text) <= 40:
        raise ValueError("Base delivery requires canonical timestamps.")
    parsed = datetime.fromisoformat(text)
    offset = parsed.utcoffset()
    if offset is None or offset.total_seconds() != 0 or parsed.isoformat() != text:
        raise ValueError("Base delivery requires canonical UTC timestamps.")
    return parsed


def _integer(value: object, maximum: int) -> int:
    if type(value) is not int or not 0 <= value <= maximum:
        raise ValueError("Base delivery requires bounded integers.")
    return value


def _wire_shape(body: bytes) -> None:
    # Skip string contents with C-backed searches: encoded blobs are opaque data.
    # Bound structural allocation before json.loads builds attacker-supplied lists.
    index = depth = containers = commas = 0
    while index < len(body):
        char = body[index]
        if char == 34:
            end = index + 1
            while True:
                end = body.find(b'"', end)
                if end < 0:
                    raise ValueError("Base delivery contains incomplete text.")
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
            if depth > 4 or containers > BASE_DELIVERY_LIMITS.max_files + 16:
                raise ValueError("Base delivery exceeds JSON structure limits.")
        elif char in (125, 93):
            depth -= 1
            if depth < 0:
                raise ValueError("Base delivery structure is invalid.")
        elif char == 44:
            commas += 1
            if commas > BASE_DELIVERY_LIMITS.max_files * 8 + 64:
                raise ValueError("Base delivery exceeds JSON structure limits.")
        index += 1
    if depth != 0:
        raise ValueError("Base delivery structure is incomplete.")


def decode_base_delivery(body: bytes) -> BaseSourceDelivery:
    if type(body) is not bytes or not 1 <= len(body) <= MAX_BASE_DELIVERY_BYTES:
        raise ValueError("Base delivery exceeds encoded channel limits.")
    _wire_shape(body)
    decoded = json.loads(
        body.decode("utf-8"), object_pairs_hook=_pairs, parse_constant=_constant
    )
    if type(decoded) is not dict:
        raise ValueError("Base delivery requires an object.")
    version = decoded.get("protocol_version")
    if type(version) is not int or version not in (1, 2):
        raise ValueError("Base delivery protocol is unsupported.")
    payload = _object(
        decoded,
        {
            "protocol_version",
            "reference",
            "claim_request_id",
            "definition_digest",
            "source",
            "root_tree_sha",
            "manifest_digest",
            "files",
        }
        | ({"raw_commit"} if version == 2 else set()),
    )
    raw_commit = None
    if version == 2:
        encoded_commit = _text(payload["raw_commit"])
        if not 1 <= len(encoded_commit) <= 4 * ((MAX_GIT_COMMIT_BYTES + 2) // 3):
            raise ValueError("Base delivery commit exceeds byte limits.")
        raw_commit = base64.b64decode(encoded_commit, validate=True)
        if (
            not 1 <= len(raw_commit) <= MAX_GIT_COMMIT_BYTES
            or base64.b64encode(raw_commit).decode("ascii") != encoded_commit
        ):
            raise ValueError("Base delivery commit bytes are not canonical.")
    raw_reference = _object(
        payload["reference"],
        {
            "worker_id",
            "job_id",
            "repository_id",
            "attempt_id",
            "generation",
            "issued_at",
            "expires_at",
        },
    )
    reference = AssignmentReference(
        worker_id=_uuid(raw_reference["worker_id"]),
        job_id=_uuid(raw_reference["job_id"]),
        repository_id=_uuid(raw_reference["repository_id"]),
        attempt_id=_uuid(raw_reference["attempt_id"]),
        generation=_integer(raw_reference["generation"], 1),
        issued_at=_date(raw_reference["issued_at"]),
        expires_at=_date(raw_reference["expires_at"]),
    )
    raw_source = _object(
        payload["source"],
        {"repository_id", "provider_repository_id", "repository_slug", "base_revision"},
    )
    request = BaseSourceRequest(
        repository_id=_uuid(raw_source["repository_id"]),
        provider_repository_id=_integer(
            raw_source["provider_repository_id"], 2**63 - 1
        ),
        repository_slug=_text(raw_source["repository_slug"]),
        base_revision=_text(raw_source["base_revision"]),
    )
    raw_files = payload["files"]
    if type(raw_files) is not list or len(raw_files) > BASE_DELIVERY_LIMITS.max_files:
        raise ValueError("Base delivery exceeds file limits.")
    files: list[BaseSourceFile] = []
    total = 0
    for raw in raw_files:
        item = _object(raw, {"path", "mode", "blob_sha", "size", "content"})
        size = _integer(item["size"], BASE_DELIVERY_LIMITS.max_file_bytes)
        total += size
        encoded = _text(item["content"])
        if total > BASE_DELIVERY_LIMITS.max_total_bytes or len(encoded) != 4 * (
            (size + 2) // 3
        ):
            raise ValueError("Base delivery exceeds decoded byte limits.")
        content = base64.b64decode(encoded, validate=True)
        if len(content) != size or base64.b64encode(content).decode("ascii") != encoded:
            raise ValueError("Base delivery contains noncanonical bytes.")
        files.append(
            BaseSourceFile(
                path=_text(item["path"]),
                mode=BaseFileMode(_text(item["mode"])),
                blob_sha=_text(item["blob_sha"]),
                content=content,
            )
        )
    snapshot = BaseSourceSnapshot(
        request=request,
        root_tree_sha=_text(payload["root_tree_sha"]),
        files=tuple(files),
        raw_commit=raw_commit,
    )
    if snapshot.fingerprint() != _text(payload["manifest_digest"]):
        raise ValueError("Base delivery manifest digest is inconsistent.")
    return BaseSourceDelivery(
        reference=reference,
        claim_request_id=_uuid(payload["claim_request_id"]),
        definition_digest=_text(payload["definition_digest"]),
        snapshot=snapshot,
    )
