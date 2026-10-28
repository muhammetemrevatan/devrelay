import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

import devrelay.application.contracts.coding_result_response_wire as wire
from devrelay.api.schemas.coding_results import (
    CodingResultAcceptanceResponse,
    CodingResultStatusResponse,
)
from devrelay.application.contracts.coding_result_receipts import (
    CodingPublicationState,
    CodingResultAcceptance,
    CodingResultStatus,
)
from devrelay.application.contracts.coding_result_response_wire import (
    decode_coding_result_acceptance,
    decode_coding_result_status,
    encode_coding_result_acceptance,
    encode_coding_result_status,
)
from tests.unit.test_coding_result_receipts import context, terminal

_ERROR = "Invalid coding result response."


def acceptance() -> CodingResultAcceptance:
    return CodingResultAcceptance(receipt=context().receipt, recovered=False)


def status(finalized: bool = False) -> CodingResultStatus:
    setup = context()
    return CodingResultStatus(
        receipt=setup.receipt,
        state=CodingPublicationState.FINALIZED
        if finalized
        else CodingPublicationState.ACCEPTED,
        operation_version=4 if finalized else 0,
        terminal=terminal(setup) if finalized else None,
    )


def body(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":")).encode("ascii")


def test_acceptance_roundtrip_and_api_utc_z_compatibility() -> None:
    value = acceptance()
    encoded = encode_coding_result_acceptance(value)
    assert decode_coding_result_acceptance(encoded) == value
    api = CodingResultAcceptanceResponse.from_result(value).model_dump_json().encode()
    assert b"Z" in api
    assert decode_coding_result_acceptance(api) == value
    assert (
        encode_coding_result_acceptance(decode_coding_result_acceptance(encoded))
        == encoded
    )


@pytest.mark.parametrize("finalized", [False, True])
def test_status_roundtrip_preserves_terminal_outer_receipt_association(
    finalized: bool,
) -> None:
    value = status(finalized)
    encoded = encode_coding_result_status(value)
    decoded = decode_coding_result_status(encoded)
    assert decoded == value
    assert (
        decode_coding_result_status(
            CodingResultStatusResponse.from_result(value).model_dump_json().encode()
        )
        == value
    )
    data = json.loads(encoded)
    assert set(data) == {"receipt", "state", "operation_version", "terminal"}
    if finalized:
        assert set(data["terminal"]) == {
            "repository_id",
            "number",
            "branch",
            "target_branch",
            "head_revision",
            "finalized_at",
            "workflow_version",
            "release_ordinal",
        }
        assert (
            decoded.terminal is not None and decoded.terminal.receipt is decoded.receipt
        )
    else:
        assert data["terminal"] is None


@pytest.mark.parametrize("which", ["acceptance", "status", "receipt", "terminal"])
@pytest.mark.parametrize("mutation", ["unknown", "missing"])
def test_each_response_object_has_exact_fields(which: str, mutation: str) -> None:
    encoded = (
        encode_coding_result_acceptance(acceptance())
        if which == "acceptance"
        else encode_coding_result_status(status(True))
    )
    data = json.loads(encoded)
    selected = data[which] if which in {"receipt", "terminal"} else data
    if mutation == "unknown":
        selected["secret"] = "Never echo this input"
    else:
        del selected[next(iter(selected))]
    decoder = (
        decode_coding_result_acceptance
        if which == "acceptance"
        else decode_coding_result_status
    )
    with pytest.raises(ValueError) as error:
        decoder(body(data))
    assert str(error.value) == _ERROR


@pytest.mark.parametrize(
    "field,value",
    [
        ("repair_cycles", True),
        ("publishing_workflow_version", 4.0),
        ("protocol_version", 2),
        ("result_id", "000000000000000000000000000003e9"),
        ("result_id", "00000000-0000-0000-0000-0000000003E9"),
        ("candidate_revision", "F" * 40),
        ("request_fingerprint", "b" * 63),
    ],
)
def test_receipt_decoding_rejects_wrong_types_and_noncanonical_ids(
    field: str, value: Any
) -> None:
    data = json.loads(encode_coding_result_acceptance(acceptance()))
    data["receipt"][field] = value
    with pytest.raises(ValueError, match=_ERROR):
        decode_coding_result_acceptance(body(data))


@pytest.mark.parametrize(
    "date",
    [
        "2026-10-18T10:00:00",
        "2026-10-18T11:00:00+01:00",
        "2026-10-18 10:00:00+00:00",
        "2026-10-18T10:00:00.000000Z",
        "2026-10-18T10:00:00z",
        "2026-10-18",
        "secret-not-a-date",
    ],
)
def test_response_timestamps_require_canonical_finite_utc(date: str) -> None:
    data = json.loads(encode_coding_result_acceptance(acceptance()))
    data["receipt"]["recorded_at"] = date
    with pytest.raises(ValueError, match=_ERROR):
        decode_coding_result_acceptance(body(data))


@pytest.mark.parametrize(
    "field,value",
    [
        ("number", True),
        ("number", 0),
        ("number", 2**63),
        ("workflow_version", 4),
        ("release_ordinal", 0),
        ("release_ordinal", True),
        ("head_revision", "f" * 40),
        ("finalized_at", "2020-01-01T00:00:00Z"),
    ],
)
def test_terminal_metadata_cannot_change_accepted_version_head_or_time(
    field: str, value: Any
) -> None:
    data = json.loads(encode_coding_result_status(status(True)))
    data["terminal"][field] = value
    with pytest.raises(ValueError, match=_ERROR):
        decode_coding_result_status(body(data))


@pytest.mark.parametrize(
    "state,version,with_terminal",
    [
        ("accepted", 1, False),
        ("publishing", 0, False),
        ("finalized", 1, True),
        ("finalized", 4, False),
        ("uncertain", 3, True),
        ("unknown", 1, False),
    ],
)
def test_status_history_and_terminal_presence_remain_coherent(
    state: str, version: int, with_terminal: bool
) -> None:
    data = json.loads(encode_coding_result_status(status(True)))
    data.update(state=state, operation_version=version)
    if not with_terminal:
        data["terminal"] = None
    with pytest.raises(ValueError, match=_ERROR):
        decode_coding_result_status(body(data))


@pytest.mark.parametrize(
    "encoded",
    [
        b'{"receipt":{},"receipt":{}}',
        b'{"nested":{"a":1,"a":2}}',
        b'{"x":NaN}',
        b'{"x":Infinity}',
        b'{"x":1.0}',
        b'{"x":"\\ud800"}',
        b'{"x":"\xff"}',
        b"[]",
        b"null",
        b"{}",
    ],
)
def test_json_failures_have_fixed_secret_free_errors(encoded: bytes) -> None:
    with pytest.raises(ValueError) as error:
        decode_coding_result_status(encoded)
    assert str(error.value) == _ERROR and error.value.__suppress_context__


@pytest.mark.parametrize(
    "encoded",
    [
        b"x" * (65536 + 1),
        b"[" * 5 + b"0" + b"]" * 5,
        b"[" + b"{}," * 33 + b"{}]",
        b"[" + b"0," * 257 + b"0]",
    ],
    ids=["bytes", "depth", "containers", "commas"],
)
def test_response_structure_limits_precede_json_allocation(
    encoded: bytes, monkeypatch: pytest.MonkeyPatch
) -> None:
    def unexpected(*args: Any, **kwargs: Any) -> object:
        raise AssertionError("Allocation before response admission.")

    monkeypatch.setattr(json, "loads", unexpected)
    with pytest.raises(ValueError, match=_ERROR):
        decode_coding_result_status(encoded)


def test_encoder_enforces_metadata_byte_limit_and_revalidates_nested_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = acceptance()
    monkeypatch.setattr(wire, "MAX_CODING_RESULT_RESPONSE_BYTES", 100)
    with pytest.raises(ValueError, match=_ERROR):
        encode_coding_result_acceptance(value)
    object.__setattr__(value.receipt, "recorded_at", datetime(2026, 1, 1))
    with pytest.raises(ValueError, match=_ERROR):
        encode_coding_result_acceptance(value)


def test_utc_microsecond_timestamp_roundtrip_is_exact() -> None:
    value = acceptance()
    value = replace(
        value,
        receipt=replace(
            value.receipt,
            recorded_at=datetime(2026, 10, 18, 10, tzinfo=UTC)
            + timedelta(microseconds=123456),
        ),
    )
    assert (
        decode_coding_result_acceptance(encode_coding_result_acceptance(value)) == value
    )
