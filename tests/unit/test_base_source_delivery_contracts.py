import json
from dataclasses import replace
from typing import Any

import pytest

from devrelay.application.contracts.base_source_delivery import (
    MAX_BASE_DELIVERY_BYTES,
    BaseSourceDelivery,
    decode_base_delivery,
    encode_base_delivery,
)
from tests.base_source_fixtures import source_fixture
from tests.workspace_fixtures import workspace_inputs


def delivery() -> BaseSourceDelivery:
    _, assignment, binding = workspace_inputs()
    reference = assignment.outcome.assignment
    assert reference is not None
    return BaseSourceDelivery(
        reference=reference,
        claim_request_id=assignment.outcome.claim_request_id,
        definition_digest=assignment.definition.fingerprint(),
        snapshot=source_fixture(binding).snapshot,
    )


def test_bounded_source_wire_round_trips_binary_and_original_pins() -> None:
    original = delivery()

    body = encode_base_delivery(original)
    recovered = decode_base_delivery(body)

    assert recovered == original
    assert recovered.snapshot.files[1].content == b"\x00\xffbinary\r\n"
    assert len(body) < MAX_BASE_DELIVERY_BYTES
    assert set(json.loads(body)) == {
        "protocol_version",
        "reference",
        "claim_request_id",
        "definition_digest",
        "source",
        "root_tree_sha",
        "manifest_digest",
        "files",
    }


@pytest.mark.parametrize(
    "mutation",
    [
        "protocol_bool",
        "protocol_2",
        "unknown",
        "reference_unknown",
        "source_unknown",
        "file_unknown",
        "uuid_noncanonical",
        "date_naive",
        "date_noncanonical",
        "generation_bool",
        "provider_bool",
        "size_bool",
        "size_mismatch",
        "content_whitespace",
        "content_noncanonical",
        "content_invalid",
        "blob_changed",
        "mode_link",
        "path_escape",
        "files_duplicate",
        "files_reordered",
        "manifest_changed",
        "repository_changed",
        "digest_invalid",
        "root_invalid",
        "files_type",
    ],
)
def test_source_wire_rejects_malformed_or_changed_manifest(mutation: str) -> None:
    value: Any = json.loads(encode_base_delivery(delivery()))
    match mutation:
        case "protocol_bool":
            value["protocol_version"] = True
        case "protocol_2":
            value["protocol_version"] = 2
        case "unknown":
            value["secret"] = "unrecognized"
        case "reference_unknown":
            value["reference"]["active"] = True
        case "source_unknown":
            value["source"]["url"] = "https://invalid/"
        case "file_unknown":
            value["files"][0]["url"] = "https://invalid/"
        case "uuid_noncanonical":
            value["reference"]["worker_id"] = value["reference"]["worker_id"].replace(
                "-", ""
            )
        case "date_naive":
            value["reference"]["issued_at"] = "2026-01-01T00:00:00"
        case "date_noncanonical":
            value["reference"]["issued_at"] = value["reference"]["issued_at"].replace(
                "+00:00", "Z"
            )
        case "generation_bool":
            value["reference"]["generation"] = True
        case "provider_bool":
            value["source"]["provider_repository_id"] = True
        case "size_bool":
            value["files"][0]["size"] = True
        case "size_mismatch":
            value["files"][0]["size"] += 1
        case "content_whitespace":
            value["files"][0]["content"] += "\n"
        case "content_noncanonical":
            value["files"][0]["content"] = "Yh=="
        case "content_invalid":
            value["files"][0]["content"] = "!" * len(value["files"][0]["content"])
        case "blob_changed":
            value["files"][0]["blob_sha"] = "0" * 40
        case "mode_link":
            value["files"][0]["mode"] = "120000"
        case "path_escape":
            value["files"][0]["path"] = "../outside"
        case "files_duplicate":
            value["files"].append(value["files"][0])
        case "files_reordered":
            value["files"].reverse()
        case "manifest_changed":
            value["manifest_digest"] = "0" * 64
        case "repository_changed":
            value["source"]["repository_id"] = "11111111-2222-3333-4444-555555555555"
        case "digest_invalid":
            value["definition_digest"] = "unsafe"
        case "root_invalid":
            value["root_tree_sha"] = "invalid"
        case "files_type":
            value["files"] = {}

    with pytest.raises(ValueError):
        decode_base_delivery(json.dumps(value).encode())


@pytest.mark.parametrize(
    "body",
    [b"{}", b"", b'{"protocol_version":1,"protocol_version":1}', b"\xff", b'{"x":NaN}'],
)
def test_source_wire_rejects_invalid_json_and_duplicate_fields(body: bytes) -> None:
    with pytest.raises(ValueError):
        decode_base_delivery(body)


def test_source_wire_checks_encoded_limit_before_parsing() -> None:
    body = b" " * (MAX_BASE_DELIVERY_BYTES + 1)

    with pytest.raises(ValueError):
        decode_base_delivery(body)


@pytest.mark.parametrize(
    "body",
    [
        b'{"x":[[[[[0]]]]]}',
        b"[" + b"[]," * 600 + b"[]]",
        b"[" + b"0," * 5000 + b"0]",
    ],
)
def test_source_wire_caps_json_allocation_before_parsing(
    body: bytes,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_parse(*args: object, **kwargs: object) -> object:
        raise AssertionError("Structural limits must precede JSON allocation.")

    monkeypatch.setattr(json, "loads", unexpected_parse)

    with pytest.raises(ValueError):
        decode_base_delivery(body)


def test_structural_markers_in_literal_names_are_opaque_wire_strings() -> None:
    original = delivery()
    item = replace(original.snapshot.files[0], path="[braces]{name}.txt")
    original = replace(original, snapshot=replace(original.snapshot, files=(item,)))

    recovered = decode_base_delivery(encode_base_delivery(original))

    assert recovered == original
