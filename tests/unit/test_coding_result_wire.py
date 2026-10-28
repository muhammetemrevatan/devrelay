import base64
import json
from dataclasses import replace
from typing import Any
from uuid import UUID

import pytest

from devrelay.application.contracts.base_sources import (
    BaseFileMode,
    BaseSourceFile,
    git_blob_sha,
)
from devrelay.application.contracts.candidates import parse_candidate_commit
from devrelay.application.contracts.claims import ClaimReplayConflict
from devrelay.application.contracts.coding_result_requests import (
    WorkerCodingResultRequest,
)
from devrelay.application.contracts.coding_result_wire import (
    decode_coding_result_intent,
    decode_coding_result_request,
    encode_coding_result_intent,
    encode_coding_result_request,
)
from devrelay.application.contracts.git_objects import GitObject, build_git_tree
from tests.unit.test_coding_result_requests import Context, context
from tests.unit.test_coding_results import context as report_context

_ERROR = "Invalid coding result payload."


def payload(setup: Context) -> dict[str, Any]:
    result: dict[str, Any] = json.loads(encode_coding_result_request(setup.request))
    return result


def body(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":")).encode("ascii")


def decode(value: dict[str, Any], setup: Context) -> WorkerCodingResultRequest:
    return decode_coding_result_request(
        body(value),
        attempt_id=setup.request.intent.execution.acknowledgement.attempt_id,
    )


def object_at(value: dict[str, Any], path: str) -> dict[str, Any]:
    current: Any = value
    for item in path.split(".") if path else ():
        current = current[int(item)] if item.isdecimal() else current[item]
    assert isinstance(current, dict)
    return current


@pytest.mark.parametrize("repairs", [0, 1, 5])
def test_wire_and_canonical_durable_roundtrip_preserve_full_evidence(
    repairs: int,
) -> None:
    setup = context()
    report = report_context(
        repairs, execution_id=setup.request.intent.execution.execution_id
    ).result
    execution = replace(
        setup.request.intent.execution,
        acknowledgement=replace(
            setup.request.intent.execution.acknowledgement,
            definition_digest=report.definition_digest,
        ),
    )
    request = replace(
        setup.request,
        intent=replace(setup.request.intent, execution=execution, result=report),
    )
    original = request.identify(setup.authorization)
    encoded = encode_coding_result_request(request)

    restored = decode_coding_result_request(
        encoded, attempt_id=execution.acknowledgement.attempt_id
    )

    original.require_replay(restored.identify(setup.authorization).fingerprint)
    assert restored.intent == request.intent
    assert restored.claim.proof.reveal() == request.claim.proof.reveal()
    assert restored.intent.result.final_candidate.files == report.final_candidate.files
    assert [cycle.raw_commit for cycle in restored.intent.result.cycles] == [
        cycle.raw_commit for cycle in report.cycles
    ]
    durable = encode_coding_result_intent(restored.intent)
    assert decode_coding_result_intent(durable) == request.intent
    assert encode_coding_result_intent(decode_coding_result_intent(durable)) == durable
    assert request.claim.proof.reveal().encode() not in durable
    assert "proof" not in json.loads(durable)


def test_transport_accepts_whitespace_key_order_and_unicode_while_durable_is_exact() -> (
    None
):
    setup = context()
    data = payload(setup)
    data["result"]["cycles"][0]["checks"][0]["diagnostic_tail"] = "Türkçe 🙂\n"
    text = json.dumps(data, ensure_ascii=False, indent=2)
    restored = decode_coding_result_request(
        text.encode("utf-8"),
        attempt_id=setup.request.intent.execution.acknowledgement.attempt_id,
    )
    assert restored.intent.result.cycles[0].checks[0].diagnostic_tail == "Türkçe 🙂\n"
    canonical = encode_coding_result_intent(restored.intent)
    assert b"\\ud83d\\ude42" in canonical
    noncanonical = json.dumps(
        json.loads(canonical), ensure_ascii=False, indent=2
    ).encode("utf-8")
    with pytest.raises(ValueError, match=_ERROR):
        decode_coding_result_intent(noncanonical)
    with pytest.raises(ValueError, match=_ERROR):
        decode_coding_result_intent(canonical + b"\n")


def test_durable_and_transport_shapes_cannot_be_confused() -> None:
    setup = context()
    with pytest.raises(ValueError, match=_ERROR):
        decode_coding_result_intent(encode_coding_result_request(setup.request))
    with pytest.raises(ValueError, match=_ERROR):
        decode_coding_result_request(
            encode_coding_result_intent(setup.request.intent),
            attempt_id=setup.request.intent.execution.acknowledgement.attempt_id,
        )


@pytest.mark.parametrize(
    "path",
    [
        "",
        "execution",
        "execution.acknowledgement",
        "execution.policy",
        "result",
        "result.cycles.0",
        "result.cycles.0.coding",
        "result.cycles.0.checks.0",
        "result.cycles.0.checks.0.settlement",
        "result.final_candidate",
        "result.final_candidate.request",
        "result.final_candidate.files.0",
    ],
)
@pytest.mark.parametrize("change", ["unknown", "missing"])
def test_every_object_requires_exact_known_fields(path: str, change: str) -> None:
    setup = context()
    data = payload(setup)
    target = object_at(data, path)
    if change == "unknown":
        target["untrusted_extra"] = "do not echo task data"
    else:
        del target[next(iter(target))]
    with pytest.raises(ValueError, match=_ERROR):
        decode(data, setup)


@pytest.mark.parametrize(
    "path,field,value",
    [
        ("", "protocol_version", True),
        ("", "expected_workflow_version", False),
        ("execution.acknowledgement", "generation", True),
        ("result.cycles.0", "cycle", True),
        ("result.cycles.0.coding", "exit_code", True),
        ("result.final_candidate.request", "provider_repository_id", False),
        ("result.final_candidate.files.0", "size", True),
        ("result.cycles.0.coding", "stdout_bytes", -1),
        ("result.cycles.0.coding", "stderr_bytes", 2**20 + 1),
        ("result.cycles.0.coding", "exit_code", 2**32),
        ("result.final_candidate.request", "provider_repository_id", 2**63),
    ],
)
def test_numeric_fields_reject_bool_negative_and_out_of_range(
    path: str, field: str, value: Any
) -> None:
    setup = context()
    data = payload(setup)
    object_at(data, path)[field] = value
    with pytest.raises(ValueError, match=_ERROR):
        decode(data, setup)


@pytest.mark.parametrize(
    "token",
    [b"NaN", b"Infinity", b"-Infinity", b"1.0", b"1e0", b"123456789012345678901"],
)
def test_nonfinite_float_and_oversized_integer_tokens_are_rejected(
    token: bytes,
) -> None:
    setup = context()
    encoded = encode_coding_result_request(setup.request).replace(
        b'"protocol_version":1', b'"protocol_version":' + token, 1
    )
    with pytest.raises(ValueError, match=_ERROR):
        decode_coding_result_request(
            encoded,
            attempt_id=setup.request.intent.execution.acknowledgement.attempt_id,
        )


@pytest.mark.parametrize(
    "value",
    [
        "000000000000000000000000000003e9",
        "00000000-0000-0000-0000-0000000003E9",
        "{00000000-0000-0000-0000-0000000003e9}",
        "urn:uuid:00000000-0000-0000-0000-0000000003e9",
    ],
)
def test_noncanonical_uuid_spellings_are_rejected(value: str) -> None:
    setup = context()
    data = payload(setup)
    data["result_request_id"] = value
    with pytest.raises(ValueError, match=_ERROR):
        decode(data, setup)


@pytest.mark.parametrize(
    "encoded", ["Zg", "Zg=", "Zh==", "Zg==\n", "Zg--", "!!!!", "é==="]
)
def test_file_base64_requires_exact_canonical_encoding(encoded: str) -> None:
    setup = context()
    data = payload(setup)
    file = data["result"]["final_candidate"]["files"][0]
    file["size"], file["content"] = 1, encoded
    with pytest.raises(ValueError, match=_ERROR):
        decode(data, setup)


@pytest.mark.parametrize("target", ["candidate", "cycle"])
def test_raw_commit_bytes_have_an_independent_small_budget(target: str) -> None:
    setup = context()
    data = payload(setup)
    selected = (
        data["result"]["final_candidate"]
        if target == "candidate"
        else data["result"]["cycles"][0]
    )
    selected["raw_commit"] = base64.b64encode(b"x" * (18 * 1024 + 1)).decode()
    with pytest.raises(ValueError, match=_ERROR):
        decode(data, setup)


@pytest.mark.parametrize(
    "mutation",
    ["file_count", "file_bytes", "total_bytes", "declared_size", "encoded_size"],
)
def test_file_budget_preflight_precedes_every_base64_decode(
    mutation: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup = context()
    data = payload(setup)
    candidate = data["result"]["final_candidate"]
    original = candidate["files"][0]
    if mutation == "file_count":
        candidate["files"] = [dict(original)] * 513
    elif mutation == "file_bytes":
        original["size"] = 2 * 1024**2 + 1
    elif mutation == "total_bytes":
        chunk = base64.b64encode(b"x" * (2 * 1024**2)).decode()
        candidate["files"] = [
            dict(original, path=f"file{index}", size=2 * 1024**2, content=chunk)
            for index in range(4)
        ]
        candidate["files"].append(
            dict(original, path="overflow", size=1, content="eA==")
        )
    elif mutation == "declared_size":
        original["size"] += 1
    else:
        original["content"] = "AAAA" * (700000)
    encoded = body(data)
    calls: list[bool] = []

    def unexpected_decode(*args: Any, **kwargs: Any) -> bytes:
        calls.append(True)
        raise AssertionError("Base64 decoding happened before file budget validation.")

    monkeypatch.setattr(base64, "b64decode", unexpected_decode)
    with pytest.raises(ValueError, match=_ERROR):
        decode_coding_result_request(
            encoded,
            attempt_id=setup.request.intent.execution.acknowledgement.attempt_id,
        )
    assert calls == []


@pytest.mark.parametrize(
    "mutation",
    [
        "cycles",
        "checks",
        "diagnostic",
        "control",
        "path",
        "mode",
        "hash",
        "base",
        "policy",
        "settlement",
    ],
)
def test_finite_report_and_candidate_constraints_survive_the_wire(
    mutation: str,
) -> None:
    setup = context()
    data = payload(setup)
    report = data["result"]
    cycle, candidate = report["cycles"][0], report["final_candidate"]
    if mutation == "cycles":
        report["cycles"] *= 7
    elif mutation == "checks":
        cycle["checks"] *= 33
    elif mutation == "diagnostic":
        cycle["checks"][0]["diagnostic_tail"] = "é" * 2049
    elif mutation == "control":
        cycle["checks"][0]["diagnostic_tail"] = "\x1b[31m"
    elif mutation == "path":
        candidate["files"][0]["path"] = "../private"
    elif mutation == "mode":
        candidate["files"][0]["mode"] = "120000"
    elif mutation == "hash":
        candidate["files"][0]["blob_sha"] = "A" * 40
    elif mutation == "base":
        candidate["request"]["base_revision"] = "a" * 40
    elif mutation == "policy":
        data["execution"]["policy"]["runtime"] = "untrusted"
    else:
        cycle["checks"][0]["settlement"]["stop_status"] = "unknown"
    with pytest.raises(ValueError, match=_ERROR):
        decode(data, setup)


def test_path_attempt_pin_is_checked_before_decoding_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup = context()
    encoded = encode_coding_result_request(setup.request)

    def unexpected_decode(*args: Any, **kwargs: Any) -> bytes:
        raise AssertionError("Wrong route must not decode source objects.")

    monkeypatch.setattr(base64, "b64decode", unexpected_decode)
    with pytest.raises(ValueError, match=_ERROR):
        decode_coding_result_request(encoded, attempt_id=UUID(int=999))


@pytest.mark.parametrize(
    "encoded",
    [
        b'{"a":1,"a":2}',
        b'{"a":{"x":1,"x":2}}',
        b'{"a":"\xff"}',
        b'{"a":"\\ud800"}',
        b'{"\\udfff":1}',
        b'{"a":',
        b"[]",
        b"null",
        b'"text"',
        b'{"a": "unterminated}',
        b"\xef\xbb\xbf{}",
    ],
)
def test_json_duplicates_encoding_and_invalid_structure_have_fixed_secret_free_errors(
    encoded: bytes,
) -> None:
    with pytest.raises(ValueError) as error:
        decode_coding_result_intent(encoded)
    assert str(error.value) == _ERROR
    assert error.value.__suppress_context__


@pytest.mark.parametrize(
    "encoded",
    [
        b"[" * 9 + b"0" + b"]" * 9,
        b"[" + b"{}," * 2048 + b"{}]",
        b"[" + b"0," * 16385 + b"0]",
        b"x" * (16 * 1024**2 + 1),
    ],
    ids=["depth", "containers", "commas", "bytes"],
)
def test_structural_budgets_are_checked_before_json_allocation(
    encoded: bytes, monkeypatch: pytest.MonkeyPatch
) -> None:
    def unexpected_json(*args: Any, **kwargs: Any) -> object:
        raise AssertionError("JSON allocation occurred before structure admission.")

    monkeypatch.setattr(json, "loads", unexpected_json)
    with pytest.raises(ValueError, match=_ERROR):
        decode_coding_result_intent(encoded)


def test_json_scanner_handles_escaped_quotes_slashes_and_opaque_brackets() -> None:
    setup = context()
    data = payload(setup)
    text = 'quoted " braces {}[] commas ,,, and backslashes \\\\ finish'
    data["result"]["cycles"][0]["checks"][0]["diagnostic_tail"] = text
    restored = decode(data, setup)
    assert restored.intent.result.cycles[0].checks[0].diagnostic_tail == text


def test_changed_report_keeps_request_uuid_but_conflicts_with_original_replay() -> None:
    setup = context()
    original = setup.request.identify(setup.authorization)
    data = payload(setup)
    data["result"]["cycles"][0]["checks"][0]["diagnostic_tail"] = (
        "changed accepted input"
    )
    changed = decode(data, setup)
    assert changed.intent.result_request_id == setup.request.intent.result_request_id
    with pytest.raises(ClaimReplayConflict):
        original.require_replay(changed.identify(setup.authorization).fingerprint)


def test_discarded_cycle_commit_still_requires_the_original_parent() -> None:
    setup = context()
    report = report_context(
        1, execution_id=setup.request.intent.execution.execution_id
    ).result
    execution = replace(
        setup.request.intent.execution,
        acknowledgement=replace(
            setup.request.intent.execution.acknowledgement,
            definition_digest=report.definition_digest,
        ),
    )
    request = replace(
        setup.request,
        intent=replace(setup.request.intent, execution=execution, result=report),
    )
    data: dict[str, Any] = json.loads(encode_coding_result_request(request))
    cycle = data["result"]["cycles"][0]
    raw = base64.b64decode(cycle["raw_commit"]).replace(
        execution.acknowledgement.base_revision.encode(), b"f" * 40, 1
    )
    cycle["raw_commit"] = base64.b64encode(raw).decode()
    cycle["revision"] = GitObject(kind="commit", data=raw).sha
    for check in cycle["checks"]:
        check["revision"] = cycle["revision"]

    with pytest.raises(ValueError, match=_ERROR):
        decode_coding_result_request(
            body(data), attempt_id=execution.acknowledgement.attempt_id
        )


def test_all_cycle_check_and_diagnostic_boundaries_roundtrip_together() -> None:
    setup = context()
    definition = report_context().definition
    definition = replace(
        definition,
        checks=tuple(
            replace(definition.checks[0], id=f"check_{index:02}") for index in range(32)
        ),
    )
    report = report_context(
        5,
        definition=definition,
        execution_id=setup.request.intent.execution.execution_id,
    ).result
    report = replace(
        report,
        cycles=tuple(
            replace(
                cycle,
                checks=tuple(
                    replace(check, diagnostic_tail="é" * 2048) for check in cycle.checks
                ),
            )
            for cycle in report.cycles
        ),
    )
    report.validate(definition)
    execution = replace(
        setup.request.intent.execution,
        acknowledgement=replace(
            setup.request.intent.execution.acknowledgement,
            definition_digest=report.definition_digest,
        ),
    )
    request = replace(
        setup.request,
        intent=replace(setup.request.intent, execution=execution, result=report),
    )

    restored = decode_coding_result_request(
        encode_coding_result_request(request),
        attempt_id=execution.acknowledgement.attempt_id,
    )

    restored.intent.result.validate(definition)
    assert restored.intent.result.fingerprint() == report.fingerprint()
    assert len(restored.intent.result.cycles) == 6
    assert (
        sum(
            len(check.diagnostic_tail.encode())
            for cycle in restored.intent.result.cycles
            for check in cycle.checks
        )
        == 6 * 32 * 4096
    )


def with_files(
    setup: Context, files: tuple[BaseSourceFile, ...]
) -> WorkerCodingResultRequest:
    old = setup.request.intent.result.final_candidate
    tree = build_git_tree(files).root_tree_sha
    specification = parse_candidate_commit(
        old.raw_commit, parent=old.request.base_revision, tree=old.root_tree_sha
    )
    raw = specification.raw_commit(parent=old.request.base_revision, tree=tree)
    candidate = replace(
        old,
        files=files,
        root_tree_sha=tree,
        raw_commit=raw,
        revision=GitObject(kind="commit", data=raw).sha,
    )
    cycle = setup.request.intent.result.cycles[0]
    cycle = replace(
        cycle,
        revision=candidate.revision,
        root_tree_sha=tree,
        raw_commit=raw,
        candidate_digest=candidate.fingerprint(),
        checks=tuple(
            replace(
                check,
                revision=candidate.revision,
                candidate_digest=candidate.fingerprint(),
            )
            for check in cycle.checks
        ),
    )
    report = replace(
        setup.request.intent.result, cycles=(cycle,), final_candidate=candidate
    )
    return replace(setup.request, intent=replace(setup.request.intent, result=report))


@pytest.mark.parametrize("count,size", [(512, 1), (4, 2 * 1024**2), (1, 0)])
def test_valid_file_boundaries_roundtrip_actual_bytes(count: int, size: int) -> None:
    setup = context()
    content = b"\x00" * size
    files = tuple(
        BaseSourceFile(
            path=f"file{index:03}.bin",
            mode=BaseFileMode.REGULAR,
            blob_sha=git_blob_sha(content),
            content=content,
        )
        for index in range(count)
    )
    request = with_files(setup, files)

    encoded = encode_coding_result_request(request)
    restored = decode_coding_result_request(
        encoded, attempt_id=request.intent.execution.acknowledgement.attempt_id
    )

    assert restored.intent.result.final_candidate.files == files
    assert restored.intent.result.fingerprint() == request.intent.result.fingerprint()
    assert len(encoded) <= 16 * 1024**2


def test_corrupt_mutated_input_never_leaks_proof_or_source_in_encoder_error() -> None:
    setup = context()
    object.__setattr__(
        setup.request.intent.execution.policy,
        "policy_digest",
        setup.request.claim.proof.reveal(),
    )
    for encode, value in (
        (encode_coding_result_request, setup.request),
        (encode_coding_result_intent, setup.request.intent),
    ):
        with pytest.raises(ValueError) as error:
            encode(value)  # type: ignore[arg-type]
        assert str(error.value) == _ERROR
        assert setup.request.claim.proof.reveal() not in repr(error.value)
        assert error.value.__suppress_context__
