import base64
import hashlib
import json
from dataclasses import dataclass, fields, replace
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest

import devrelay.application.contracts.coding_result_requests as result_requests
from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    LeaseProof,
    WorkerClaimRequest,
)
from devrelay.application.contracts.coding_result_requests import (
    MAX_CODING_RESULT_REQUEST_BYTES,
    CodingResultIdentity,
    WorkerCodingResultIntent,
    WorkerCodingResultRequest,
    coding_result_identity_payload,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from tests.git_source_fixtures import git_workspace_inputs
from tests.unit.test_coding_results import context as result_context


@dataclass
class Context:
    request: WorkerCodingResultRequest
    authorization: WorkerAuthorization


def context() -> Context:
    state, delivery, binding, _ = git_workspace_inputs()
    assert state.request is not None
    reference = binding.execution.reference
    report = result_context(
        definition=delivery.definition,
        execution_id=binding.execution.intent.execution_id,
    ).result
    return Context(
        WorkerCodingResultRequest(
            intent=WorkerCodingResultIntent(
                result_request_id=UUID(int=1001),
                execution=binding.execution.intent,
                result=report,
            ),
            claim=state.request,
        ),
        WorkerAuthorization(
            worker_id=reference.worker_id,
            allowed_repository_ids=frozenset({reference.repository_id}),
            credential_expires_at=reference.expires_at + timedelta(hours=1),
        ),
    )


def golden_context() -> Context:
    setup = context()
    original = setup.request.intent
    report = original.result
    candidate = replace(
        report.final_candidate,
        execution_id=UUID(int=6),
        request=replace(
            report.final_candidate.request,
            repository_id=UUID(int=7),
            provider_repository_id=42,
            repository_slug="fixture/project",
        ),
        authorized_branch="devrelay/fixture",
    )
    cycle = report.cycles[0]
    cycle = replace(
        cycle,
        candidate_digest=candidate.fingerprint(),
        coding=replace(cycle.coding, execution_id=UUID(int=6)),
        checks=tuple(
            replace(
                check,
                candidate_digest=candidate.fingerprint(),
                settlement=replace(check.settlement, execution_id=UUID(int=6)),
            )
            for check in cycle.checks
        ),
    )
    report = replace(
        report,
        execution_id=UUID(int=6),
        definition_digest="b" * 64,
        check_policy_digest="c" * 64,
        cycles=(cycle,),
        final_candidate=candidate,
    )
    acknowledgement = replace(
        original.execution.acknowledgement,
        acknowledgement_request_id=UUID(int=2),
        claim_request_id=UUID(int=3),
        attempt_id=UUID(int=4),
        snapshot_id=UUID(int=5),
        definition_digest=report.definition_digest,
    )
    execution = replace(
        original.execution,
        execution_id=UUID(int=6),
        acknowledgement=acknowledgement,
        policy=replace(original.execution.policy, policy_digest="a" * 64),
    )
    return Context(
        WorkerCodingResultRequest(
            intent=WorkerCodingResultIntent(
                result_request_id=UUID(int=1), execution=execution, result=report
            ),
            claim=WorkerClaimRequest(
                claim_request_id=UUID(int=3),
                proof=LeaseProof(
                    base64.urlsafe_b64encode(bytes(range(32))).decode().rstrip("=")
                ),
            ),
        ),
        WorkerAuthorization(
            worker_id=UUID(int=8),
            allowed_repository_ids=frozenset({UUID(int=7)}),
            credential_expires_at=datetime(2027, 1, 1, tzinfo=UTC),
        ),
    )


def test_identification_pins_authenticated_worker_original_claim_and_full_input() -> (
    None
):
    setup = context()
    identity = setup.request.identify(setup.authorization)

    assert identity.claim.worker_id == setup.authorization.worker_id
    assert identity.intent is setup.request.intent
    assert identity.claim.claim_request_id == setup.request.claim.claim_request_id
    assert identity.claim.verifier.matches(
        setup.request.claim.proof,
        worker_id=setup.authorization.worker_id,
        claim_request_id=setup.request.claim.claim_request_id,
    )
    identity.require_replay(identity.fingerprint)
    reconstructed = replace(setup.request).identify(setup.authorization)
    identity.require_replay(reconstructed.fingerprint)
    assert reconstructed != identity and replace(setup.request) != setup.request


def test_operation_payload_contains_no_raw_proof_task_source_or_process_output() -> (
    None
):
    setup = context()
    identity = setup.request.identify(setup.authorization)
    payload = coding_result_identity_payload(identity.intent, identity.claim)
    encoded = json.dumps(payload, sort_keys=True)

    assert setup.request.claim.proof.reveal() not in encoded
    assert "proof_verifier" not in encoded
    assert "stdout" not in encoded and "diagnostic_tail" not in encoded
    assert "files" not in encoded and "raw_commit" not in encoded
    assert payload["claim_fingerprint"] == identity.claim.fingerprint
    assert payload["operation"] == "coding_result" and payload["version"] == 1
    assert (
        payload["route"]
        == f"/api/v1/worker/attempts/{identity.intent.execution.acknowledgement.attempt_id}/results"
    )
    for value in (setup.request, setup.request.intent, identity):
        assert setup.request.claim.proof.reveal() not in repr(value)
        assert "Sanitized check tail" not in repr(value)
        assert "Apply candidate" not in repr(value)


@pytest.mark.parametrize(
    "field,value",
    [
        ("expected_workflow_version", True),
        ("expected_workflow_version", 1),
        ("expected_workflow_version", 2.0),
        ("expected_workflow_version", "2"),
        ("protocol_version", True),
        ("protocol_version", 0),
        ("protocol_version", 1.0),
        ("protocol_version", 2),
    ],
)
def test_supported_versions_are_strict_integers(field: str, value: Any) -> None:
    intent = context().request.intent
    with pytest.raises(ValueError):
        replace(intent, **{field: value})


@pytest.mark.parametrize(
    "field,value",
    [
        ("result_request_id", "not-a-uuid"),
        ("execution", None),
        ("result", {}),
        ("result", None),
    ],
)
def test_intent_requires_typed_original_execution_and_report(
    field: str, value: Any
) -> None:
    intent = context().request.intent
    with pytest.raises(ValueError):
        replace(intent, **{field: value})


@pytest.mark.parametrize("mutation", ["execution", "definition", "base"])
def test_report_must_correlate_to_original_acknowledged_execution(
    mutation: str,
) -> None:
    intent = context().request.intent
    execution = intent.execution
    if mutation == "execution":
        execution = replace(execution, execution_id=UUID(int=99))
    else:
        changes: dict[str, Any] = (
            {"definition_digest": "f" * 64}
            if mutation == "definition"
            else {"base_revision": "f" * 40}
        )
        execution = replace(
            execution, acknowledgement=replace(execution.acknowledgement, **changes)
        )

    with pytest.raises(ValueError, match="original acknowledged"):
        replace(intent, execution=execution)


@pytest.mark.parametrize("mutation", ["ack_version", "policy_digest", "report_exit"])
def test_nested_corruption_is_revalidated_at_request_boundary(mutation: str) -> None:
    request = context().request
    if mutation == "ack_version":
        object.__setattr__(
            request.intent.execution.acknowledgement, "protocol_version", True
        )
    elif mutation == "policy_digest":
        object.__setattr__(request.intent.execution.policy, "policy_digest", "invalid")
    else:
        object.__setattr__(
            request.intent.result.cycles[0].checks[0].settlement, "exit_code", 1
        )

    with pytest.raises(ValueError):
        replace(request)


def test_request_cannot_substitute_different_claim_id() -> None:
    request = context().request
    claim = replace(request.claim, claim_request_id=UUID(int=99))
    with pytest.raises(ValueError, match="original claim"):
        replace(request, claim=claim)


def test_same_input_can_recover_original_uuid_without_accepting_changed_report() -> (
    None
):
    intent = context().request.intent
    candidate = replace(intent, result_request_id=UUID(int=99))
    intent.require_same_input(candidate)
    check = intent.result.cycles[0].checks[0]
    changed_cycle = replace(
        intent.result.cycles[0],
        checks=(
            replace(check, diagnostic_tail="changed"),
            *intent.result.cycles[0].checks[1:],
        ),
    )
    changed = replace(candidate, result=replace(intent.result, cycles=(changed_cycle,)))

    with pytest.raises(ClaimReplayConflict):
        intent.require_same_input(changed)


@pytest.mark.parametrize(
    "mutation",
    [
        "result_request",
        "ack_request",
        "attempt",
        "snapshot",
        "policy",
        "report",
        "worker",
        "proof",
    ],
)
def test_replay_fingerprint_changes_with_every_accepted_identity_pin(
    mutation: str,
) -> None:
    setup = context()
    request, authorization = setup.request, setup.authorization
    original = request.identify(authorization)
    intent = request.intent
    if mutation == "result_request":
        request = replace(
            request, intent=replace(intent, result_request_id=UUID(int=99))
        )
    elif mutation in {"ack_request", "attempt", "snapshot"}:
        field = {
            "ack_request": "acknowledgement_request_id",
            "attempt": "attempt_id",
            "snapshot": "snapshot_id",
        }[mutation]
        changed_ack: dict[str, Any] = {field: UUID(int=99)}
        execution = replace(
            intent.execution,
            acknowledgement=replace(intent.execution.acknowledgement, **changed_ack),
        )
        request = replace(request, intent=replace(intent, execution=execution))
    elif mutation == "policy":
        request = replace(
            request,
            intent=replace(
                intent,
                execution=replace(
                    intent.execution,
                    policy=replace(intent.execution.policy, policy_digest="e" * 64),
                ),
            ),
        )
    elif mutation == "report":
        request = replace(
            request,
            intent=replace(
                intent, result=replace(intent.result, check_policy_digest="e" * 64)
            ),
        )
    elif mutation == "worker":
        authorization = replace(authorization, worker_id=UUID(int=99))
    else:
        claim = replace(
            request.claim,
            proof=LeaseProof(base64.urlsafe_b64encode(b"z" * 32).decode().rstrip("=")),
        )
        request = replace(request, claim=claim)
    changed = request.identify(authorization)

    with pytest.raises(ClaimReplayConflict):
        original.require_replay(changed.fingerprint)


@pytest.mark.parametrize(
    "fingerprint", [None, True, b"a" * 64, "a" * 63, "F" * 64, "invalid"]
)
def test_replay_rejects_malformed_fingerprint(fingerprint: Any) -> None:
    setup = context()
    with pytest.raises(ClaimReplayConflict):
        setup.request.identify(setup.authorization).require_replay(fingerprint)


def test_replay_uses_constant_time_comparison(monkeypatch: pytest.MonkeyPatch) -> None:
    setup = context()
    identity = setup.request.identify(setup.authorization)
    compared: list[tuple[str, str]] = []

    def compare(left: str, right: str) -> bool:
        compared.append((left, right))
        return left == right

    monkeypatch.setattr(result_requests, "compare_digest", compare)
    identity.require_replay(identity.fingerprint)
    with pytest.raises(ClaimReplayConflict):
        identity.require_replay("f" * 64)
    assert compared == [
        (identity.fingerprint, identity.fingerprint),
        (identity.fingerprint, "f" * 64),
    ]


def test_scope_and_expiry_are_not_persisted_as_current_authority() -> None:
    setup = context()
    original = setup.request.identify(setup.authorization)
    historical_scope = replace(
        setup.authorization,
        credential_expires_at=datetime(2020, 1, 1, tzinfo=UTC),
        allowed_repository_ids=frozenset({UUID(int=999)}),
    )
    original.require_replay(setup.request.identify(historical_scope).fingerprint)
    assert not {
        "received_at",
        "current_authority",
        "released",
        "credential_expires_at",
    } & {item.name for item in fields(original)}
    assert MAX_CODING_RESULT_REQUEST_BYTES == 16 * 1024**2


def test_golden_operation_fingerprint_is_canonical_and_versioned() -> None:
    setup = golden_context()
    identity = setup.request.identify(setup.authorization)
    payload = coding_result_identity_payload(identity.intent, identity.claim)
    expected = hashlib.sha256(
        json.dumps(
            payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        ).encode("ascii")
    ).hexdigest()
    assert identity.fingerprint == expected
    assert (
        identity.fingerprint
        == "99f72ca67adcd180fe591b94551f633962701aace1f766ec54bf79e0ae5def21"
    )
    repeated = golden_context()
    assert (
        repeated.request.identify(repeated.authorization).fingerprint
        == identity.fingerprint
    )


def test_identity_rejects_substituted_original_claim() -> None:
    setup = context()
    claim = replace(setup.request.claim, claim_request_id=UUID(int=99)).identify(
        setup.authorization
    )
    with pytest.raises(ValueError, match="scoped claim"):
        CodingResultIdentity(intent=setup.request.intent, claim=claim)
