import base64
import hashlib
import json
from dataclasses import replace

import pytest

from devrelay.application.contracts.base_source_delivery import (
    BaseSourceDelivery,
    decode_base_delivery,
    encode_base_delivery,
)
from devrelay.application.contracts.git_sources import (
    MAX_GIT_COMMIT_BYTES,
    GitBaseSourceSnapshot,
    validate_git_commit,
)
from tests.git_source_fixtures import git_source_fixture, git_workspace_inputs
from tests.workspace_fixtures import workspace_binding


def delivery() -> BaseSourceDelivery:
    _, assignment, _, fixture = git_workspace_inputs()
    reference = assignment.outcome.assignment
    assert reference is not None
    return BaseSourceDelivery(
        reference=reference,
        claim_request_id=assignment.outcome.claim_request_id,
        definition_digest=assignment.definition.fingerprint(),
        snapshot=fixture.snapshot,
    )


@pytest.mark.parametrize(
    "headers",
    [
        b"",
        b"encoding ISO-8859-1\n",
        b"gpgsig -----BEGIN SIGNATURE-----\n signed value\n -----END SIGNATURE-----\n",
        b"mergetag object " + b"1" * 40 + b"\n type commit\n tag fixture\n",
    ],
)
def test_original_commit_hash_and_additional_headers_are_preserved(
    headers: bytes,
) -> None:
    snapshot = git_source_fixture(workspace_binding(), headers).snapshot
    assert snapshot.raw_commit is not None

    source = GitBaseSourceSnapshot(snapshot=snapshot, raw_commit=snapshot.raw_commit)

    assert source.raw_commit == snapshot.raw_commit
    assert headers in source.raw_commit
    assert source.raw_commit not in repr(source).encode()


@pytest.mark.parametrize(
    "mutation",
    [
        "hash",
        "tree",
        "bytes",
        "type",
        "empty",
        "large",
        "missing_separator",
        "duplicate_tree",
        "missing_author",
        "bad_parent",
        "nul_header",
    ],
)
def test_commit_validation_rejects_wrong_or_malformed_original(mutation: str) -> None:
    snapshot = delivery().snapshot
    raw = snapshot.raw_commit
    assert raw is not None
    sha, tree = snapshot.request.base_revision, snapshot.root_tree_sha
    match mutation:
        case "hash":
            sha = "0" * 40
        case "tree":
            tree = "0" * 40
        case "bytes":
            raw += b"changed"
        case "type":
            raw = bytearray(raw)  # type: ignore[assignment]
        case "empty":
            raw = b""
        case "large":
            raw = b"x" * (MAX_GIT_COMMIT_BYTES + 1)
        case "missing_separator":
            raw = raw.replace(b"\n\n", b"\n")
        case "duplicate_tree":
            raw = b"tree " + tree.encode() + b"\n" + raw
        case "missing_author":
            raw = raw.replace(b"author ", b"ignored ")
        case "bad_parent":
            raw = raw.replace(b"parent " + b"1" * 40, b"parent nope")
        case "nul_header":
            raw = raw.replace(b"author Fixture", b"author Fix\x00ture")
    if mutation in {
        "missing_separator",
        "duplicate_tree",
        "missing_author",
        "bad_parent",
        "nul_header",
    }:
        sha = hashlib.sha1(
            b"commit " + str(len(raw)).encode() + b"\x00" + raw, usedforsecurity=False
        ).hexdigest()

    with pytest.raises(ValueError):
        validate_git_commit(raw, sha, tree)


def test_version_two_wire_preserves_original_commit_and_digest() -> None:
    original = delivery()

    body = encode_base_delivery(original)
    recovered = decode_base_delivery(body)

    assert recovered == original
    assert json.loads(body)["protocol_version"] == 2
    assert recovered.snapshot.raw_commit == original.snapshot.raw_commit
    assert recovered.snapshot.fingerprint() == original.snapshot.fingerprint()


@pytest.mark.parametrize(
    "mutation",
    [
        "omitted",
        "changed",
        "noncanonical",
        "large",
        "downgrade",
        "unknown",
        "version_bool",
        "tree",
    ],
)
def test_version_two_wire_rejects_commit_tampering(mutation: str) -> None:
    payload = json.loads(encode_base_delivery(delivery()))
    match mutation:
        case "omitted":
            del payload["raw_commit"]
        case "changed":
            payload["raw_commit"] = base64.b64encode(b"different").decode()
        case "noncanonical":
            payload["raw_commit"] += "\n"
        case "large":
            payload["raw_commit"] = "A" * (4 * ((MAX_GIT_COMMIT_BYTES + 2) // 3) + 4)
        case "downgrade":
            payload["protocol_version"] = 1
        case "unknown":
            payload["commit_url"] = "https://example.invalid/"
        case "version_bool":
            payload["protocol_version"] = True
        case "tree":
            payload["root_tree_sha"] = "0" * 40
    body = json.dumps(payload).encode()

    with pytest.raises(ValueError):
        decode_base_delivery(body)


def test_literal_version_one_digest_and_wire_remain_compatible() -> None:
    original = delivery()
    literal = replace(original, snapshot=replace(original.snapshot, raw_commit=None))

    body = encode_base_delivery(literal)
    recovered = decode_base_delivery(body)

    assert recovered == literal
    assert json.loads(body)["protocol_version"] == 1
    assert "raw_commit" not in json.loads(body)
    assert literal.snapshot.fingerprint() != original.snapshot.fingerprint()
