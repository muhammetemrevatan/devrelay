import base64
import hashlib
import inspect
from dataclasses import dataclass, fields, replace
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest

import devrelay.application.coding_result_preparation as preparation
from devrelay.application.coding_result_preparation import prepare_coding_result
from devrelay.application.contracts.assignment_delivery import AssignedJobDefinition
from devrelay.application.contracts.base_sources import (
    BaseFileMode,
    BaseSourceFile,
    git_blob_sha,
)
from devrelay.application.contracts.candidates import CandidateSnapshot
from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    LeaseProof,
    WorkerClaimRequest,
)
from devrelay.application.contracts.coding_result_acceptance import PreparedCodingResult
from devrelay.application.contracts.coding_result_requests import (
    WorkerCodingResultIntent,
    WorkerCodingResultRequest,
)
from devrelay.application.contracts.coding_result_wire import (
    decode_coding_result_intent,
    encode_coding_result_intent,
)
from devrelay.application.contracts.coding_results import SuccessfulCodingResult
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.domain.jobs.values import VerificationOutcome
from tests.unit.test_accept_coding_result import context as acceptance_context
from tests.unit.test_coding_result_requests import Context as WireContext
from tests.unit.test_coding_result_wire import with_files

_ERROR = "Invalid coding result preparation."


@dataclass
class Context:
    request: WorkerCodingResultRequest
    authorization: WorkerAuthorization
    definition: AssignedJobDefinition


def context(repairs: int = 0) -> Context:
    original = acceptance_context(repairs)
    execution = original.execution
    return Context(
        WorkerCodingResultRequest(
            intent=WorkerCodingResultIntent(
                result_request_id=UUID(int=7001),
                execution=execution.intent,
                result=original.result,
            ),
            claim=WorkerClaimRequest(
                claim_request_id=execution.intent.acknowledgement.claim_request_id,
                proof=LeaseProof(
                    base64.urlsafe_b64encode(b"x" * 32).decode().rstrip("=")
                ),
            ),
        ),
        WorkerAuthorization(
            worker_id=execution.reference.worker_id,
            allowed_repository_ids=frozenset({original.definition.repository_id}),
            credential_expires_at=execution.reference.expires_at + timedelta(hours=1),
        ),
        original.definition,
    )


def prepare(setup: Context) -> PreparedCodingResult:
    return prepare_coding_result(setup.request, setup.authorization, setup.definition)


@pytest.mark.parametrize("repairs", [0, 1, 5])
def test_preflight_owns_full_canonical_input_and_all_settled_check_results(
    repairs: int,
) -> None:
    setup = context(repairs)
    original = setup.request.identify(setup.authorization)

    result = prepare(setup)

    original.require_replay(result.identity.fingerprint)
    assert result.identity.intent is not setup.request.intent
    assert result.identity.intent.execution is not setup.request.intent.execution
    assert result.identity.intent.result is not setup.request.intent.result
    assert (
        result.definition == setup.definition
        and result.definition is not setup.definition
    )
    assert result.definition.snapshot is not setup.definition.snapshot
    assert result.definition.snapshot.identity is not setup.definition.snapshot.identity
    assert all(
        owned is not original
        for owned, original in zip(
            result.definition.checks, setup.definition.checks, strict=True
        )
    )
    assert result.payload_bytes == encode_coding_result_intent(setup.request.intent)
    assert result.payload_sha256 == hashlib.sha256(result.payload_bytes).hexdigest()
    assert decode_coding_result_intent(result.payload_bytes) == result.identity.intent
    assert len(result.cycle_checks) == repairs + 1
    for index, checks in enumerate(result.cycle_checks):
        cycle = setup.request.intent.result.cycles[index]
        assert tuple(check.check_id for check in checks) == tuple(
            check.id for check in setup.definition.checks
        )
        assert all(check.revision == cycle.revision for check in checks)
        assert all(
            check.exit_code == observed.settlement.exit_code
            for check, observed in zip(checks, cycle.checks, strict=True)
        )
        assert all(
            (check.outcome is VerificationOutcome.PASSED) == (check.exit_code == 0)
            for check in checks
        )


@pytest.mark.parametrize("exit_code", [1, 259, 0xFFFFFFFF])
def test_failed_repair_cycles_preserve_actual_nonzero_native_exit(
    exit_code: int,
) -> None:
    setup = context(1)
    report = setup.request.intent.result
    first = report.cycles[0]
    changed = replace(
        first.checks[0],
        settlement=replace(first.checks[0].settlement, exit_code=exit_code),
    )
    report = replace(
        report,
        cycles=(replace(first, checks=(changed, *first.checks[1:])), report.cycles[1]),
    )
    setup.request = replace(
        setup.request, intent=replace(setup.request.intent, result=report)
    )

    result = prepare(setup)

    assert result.cycle_checks[0][0].exit_code == exit_code
    assert result.cycle_checks[0][0].outcome is VerificationOutcome.FAILED
    assert all(
        check.outcome is VerificationOutcome.PASSED for check in result.cycle_checks[-1]
    )


def test_captured_graph_is_independent_of_later_caller_mutation() -> None:
    setup = context()
    result = prepare(setup)
    original_fingerprint = result.identity.fingerprint
    definition_fingerprint = result.definition.fingerprint()
    body = result.payload_bytes
    object.__setattr__(setup.definition.snapshot, "title", "Changed caller task")
    object.__setattr__(setup.definition.checks[0], "argv", ("malicious",))
    object.__setattr__(
        setup.request.intent.result.final_candidate.files[0],
        "content",
        b"Changed caller bytes",
    )
    object.__setattr__(setup.request.claim, "claim_request_id", UUID(int=999))

    result.identity.intent.result.validate(result.definition)
    assert result.definition.fingerprint() == definition_fingerprint
    assert result.identity.fingerprint == original_fingerprint
    assert result.payload_bytes == body
    assert (
        result.identity.intent.result.final_candidate.files[0].content
        != b"Changed caller bytes"
    )


def test_full_eight_megabyte_candidate_is_owned_before_storage() -> None:
    setup = context()
    content = b"\x00" * (2 * 1024**2)
    files_ = tuple(
        BaseSourceFile(
            path=f"file{index}.bin",
            mode=BaseFileMode.REGULAR,
            blob_sha=git_blob_sha(content),
            content=content,
        )
        for index in range(4)
    )
    setup.request = with_files(WireContext(setup.request, setup.authorization), files_)

    result = prepare(setup)

    assert result.identity.intent.result.final_candidate.files == files_
    assert (
        sum(
            len(item.content)
            for item in result.identity.intent.result.final_candidate.files
        )
        == 8 * 1024**2
    )
    assert result.payload_sha256 == hashlib.sha256(result.payload_bytes).hexdigest()
    assert all(
        owned is not original
        for owned, original in zip(
            result.identity.intent.result.final_candidate.files, files_, strict=True
        )
    )


def test_proof_task_source_and_diagnostics_are_absent_from_prepared_display() -> None:
    setup = context()
    result = prepare(setup)
    assert setup.request.claim.proof.reveal().encode() not in result.payload_bytes
    assert b'"proof"' not in result.payload_bytes
    assert "proof" not in {item.name for item in fields(result.identity.claim)}
    assert not {
        "received_at",
        "accepted_at",
        "current_authority",
        "validated",
        "released",
    } & {item.name for item in fields(result)}
    assert "Sanitized check tail" not in repr(result)
    assert setup.definition.snapshot.title not in repr(result)
    assert setup.request.intent.result.final_candidate.files[
        0
    ].content.decode() not in repr(result)
    assert setup.request.claim.proof.reveal() not in repr(result)


@pytest.mark.parametrize(
    "field",
    [
        "policy_version",
        "repository_slug",
        "authorized_branch",
        "base_revision",
        "max_repair_cycles",
        "snapshot_id",
    ],
)
def test_preflight_requires_the_full_accepted_definition(field: str) -> None:
    setup = context(1)
    values: dict[str, Any] = {
        "policy_version": "changed",
        "repository_slug": "other/repository",
        "authorized_branch": "agent/other",
        "base_revision": "f" * 40,
        "max_repair_cycles": 0,
    }
    if field == "snapshot_id":
        setup.definition = replace(
            setup.definition,
            snapshot=replace(setup.definition.snapshot, id=UUID(int=999)),
        )
    else:
        setup.definition = replace(setup.definition, **{field: values[field]})
    with pytest.raises(ValueError) as error:
        prepare(setup)
    assert str(error.value) == _ERROR


def test_stale_task_content_fingerprint_cannot_enter_owned_definition() -> None:
    setup = context()
    object.__setattr__(
        setup.definition.snapshot, "title", "Tampered without recomputing fingerprint"
    )
    with pytest.raises(ValueError, match=_ERROR):
        prepare(setup)


def test_ack_snapshot_pin_is_independent_of_a_matching_definition_digest() -> None:
    setup = context()
    execution = setup.request.intent.execution
    setup.request = replace(
        setup.request,
        intent=replace(
            setup.request.intent,
            execution=replace(
                execution,
                acknowledgement=replace(
                    execution.acknowledgement, snapshot_id=UUID(int=999)
                ),
            ),
        ),
    )
    with pytest.raises(ValueError, match=_ERROR):
        prepare(setup)


@pytest.mark.parametrize(
    "mutation", ["claim", "report", "definition", "authorization", "payload"]
)
def test_mutation_during_snapshot_capture_cannot_change_accepted_input(
    mutation: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup = context()
    original_encode = encode_coding_result_intent

    def encode(intent: WorkerCodingResultIntent) -> bytes:
        encoded = original_encode(intent)
        if mutation == "claim":
            proof = LeaseProof(base64.urlsafe_b64encode(b"y" * 32).decode().rstrip("="))
            object.__setattr__(setup.request.claim, "proof", proof)
        elif mutation == "report":
            object.__setattr__(
                setup.request.intent.result.cycles[0].checks[0],
                "diagnostic_tail",
                "changed while capturing",
            )
        elif mutation == "definition":
            object.__setattr__(setup.definition, "policy_version", "changed")
        elif mutation == "authorization":
            object.__setattr__(setup.authorization, "worker_id", UUID(int=999))
        else:
            changed = replace(
                intent.result.cycles[0].checks[0],
                diagnostic_tail="changed encoded input",
            )
            report = replace(
                intent.result,
                cycles=(
                    replace(
                        intent.result.cycles[0],
                        checks=(changed, *intent.result.cycles[0].checks[1:]),
                    ),
                ),
            )
            encoded = original_encode(replace(intent, result=report))
        return encoded

    monkeypatch.setattr(preparation, "encode_coding_result_intent", encode)
    with pytest.raises((ValueError, ClaimReplayConflict)) as error:
        prepare(setup)
    assert setup.request.claim.proof.reveal() not in str(error.value)


def test_first_rebound_and_final_request_identities_use_constant_time_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup = context()
    compared: list[tuple[str, str]] = []

    def compare(left: str, right: str) -> bool:
        compared.append((left, right))
        return left == right

    monkeypatch.setattr(
        "devrelay.application.contracts.coding_result_requests.compare_digest", compare
    )
    result = prepare(setup)
    assert (
        compared.count((result.identity.fingerprint, result.identity.fingerprint)) >= 2
    )


def test_dto_construction_does_not_decode_or_rehash_candidate_files(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = prepare(context())

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("Full candidate work repeated in DTO construction.")

    monkeypatch.setattr(CandidateSnapshot, "validate", forbidden)
    monkeypatch.setattr(SuccessfulCodingResult, "fingerprint", forbidden)
    monkeypatch.setattr(preparation, "decode_coding_result_intent", forbidden)
    duplicate = replace(result)
    assert duplicate.payload_bytes is result.payload_bytes
    assert duplicate.identity is result.identity


@pytest.mark.parametrize(
    "field,value",
    [
        ("identity", None),
        ("definition", None),
        ("payload_bytes", b""),
        ("payload_bytes", bytearray(b"data")),
        ("payload_sha256", "A" * 64),
        ("payload_sha256", "f" * 63),
        ("cycle_checks", []),
        ("cycle_checks", ()),
        ("cycle_checks", ((None,),)),
    ],
)
def test_prepared_dto_rejects_untyped_unbounded_or_incomplete_metadata(
    field: str, value: Any
) -> None:
    result = prepare(context())
    with pytest.raises(ValueError, match=_ERROR):
        replace(result, **{field: value})


def test_prepared_dto_rejects_oversized_payload_without_decoding() -> None:
    result = prepare(context())
    with pytest.raises(ValueError, match=_ERROR):
        replace(result, payload_bytes=b"x" * (16 * 1024**2 + 1))


@pytest.mark.parametrize(
    "mutation",
    ["missing_cycle", "foreign_revision", "changed_exit", "definition", "order"],
)
def test_prepared_dto_keeps_compiled_checks_correlated_to_owned_report(
    mutation: str,
) -> None:
    result = prepare(context(1))
    changes: dict[str, Any] = {}
    if mutation == "missing_cycle":
        changes["cycle_checks"] = result.cycle_checks[:1]
    elif mutation == "foreign_revision":
        checks = (
            replace(result.cycle_checks[0][0], revision="f" * 40),
            *result.cycle_checks[0][1:],
        )
        changes["cycle_checks"] = (checks, result.cycle_checks[1])
    elif mutation == "changed_exit":
        checks = (
            replace(result.cycle_checks[0][0], exit_code=2),
            *result.cycle_checks[0][1:],
        )
        changes["cycle_checks"] = (checks, result.cycle_checks[1])
    elif mutation == "definition":
        changes["definition"] = replace(result.definition, policy_version="changed")
    else:
        checks = (
            replace(result.cycle_checks[0][0], check_id="foreign"),
            *result.cycle_checks[0][1:],
        )
        changes["cycle_checks"] = (checks, result.cycle_checks[1])
    with pytest.raises(ValueError, match=_ERROR):
        replace(result, **changes)


def test_preflight_does_not_invent_current_scope_or_expiry_authority() -> None:
    setup = context()
    setup.authorization = replace(
        setup.authorization,
        credential_expires_at=datetime(2020, 1, 1, tzinfo=UTC),
        allowed_repository_ids=frozenset({UUID(int=999)}),
    )
    result = prepare(setup)
    assert result.identity.claim.worker_id == setup.authorization.worker_id
    assert not inspect.iscoroutinefunction(prepare_coding_result)
    assert "off-loop" in (preparation.__doc__ or "")


@pytest.mark.parametrize("field", ["request", "authorization", "definition"])
def test_preflight_translates_invalid_input_to_fixed_nonsecret_error(
    field: str,
) -> None:
    setup = context()
    setattr(setup, field, None)
    with pytest.raises(ValueError) as error:
        prepare(setup)
    assert str(error.value) == _ERROR and error.value.__suppress_context__
