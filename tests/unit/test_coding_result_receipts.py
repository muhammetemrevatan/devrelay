from dataclasses import dataclass, fields, replace
from datetime import UTC, datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import pytest

from devrelay.application.contracts.claims import ClaimReplayConflict
from devrelay.application.contracts.coding_result_receipts import (
    CodingPublicationState,
    CodingResultAcceptance,
    CodingResultReceipt,
    CodingResultStatus,
    TerminalCodingResult,
    VerifiedCodingPublication,
)
from devrelay.application.contracts.coding_result_requests import CodingResultIdentity
from devrelay.domain.jobs.values import PullRequestReference
from tests.unit.test_coding_result_requests import context as request_context
from tests.unit.test_coding_results import context as report_context

_NOW = datetime(2026, 10, 18, 10, 0, tzinfo=UTC)
_JOB = UUID(int=901)


@dataclass
class Context:
    receipt: CodingResultReceipt
    identity: CodingResultIdentity
    reference: PullRequestReference


def context(repairs: int = 0) -> Context:
    setup = request_context()
    request = setup.request
    if repairs:
        report = report_context(
            repairs, execution_id=request.intent.execution.execution_id
        ).result
        execution = replace(
            request.intent.execution,
            acknowledgement=replace(
                request.intent.execution.acknowledgement,
                definition_digest=report.definition_digest,
            ),
        )
        request = replace(
            request, intent=replace(request.intent, execution=execution, result=report)
        )
    identity = request.identify(setup.authorization)
    intent = identity.intent
    receipt = CodingResultReceipt(
        result_id=UUID(int=902),
        worker_id=identity.claim.worker_id,
        request_id=intent.result_request_id,
        claim_request_id=identity.claim.claim_request_id,
        job_id=_JOB,
        attempt_id=intent.execution.acknowledgement.attempt_id,
        repository_id=intent.result.final_candidate.request.repository_id,
        execution_id=intent.execution.execution_id,
        definition_digest=intent.result.definition_digest,
        request_fingerprint=identity.fingerprint,
        candidate_revision=intent.result.final_candidate.revision,
        repair_cycles=repairs,
        publishing_workflow_version=4 + 2 * repairs,
        recorded_at=_NOW,
    )
    reference = PullRequestReference(
        repository_id=receipt.repository_id,
        number=42,
        source_branch=intent.result.final_candidate.authorized_branch,
        target_branch="master",
        head_revision=receipt.candidate_revision,
    )
    return Context(receipt, identity, reference)


def terminal(setup: Context) -> TerminalCodingResult:
    return TerminalCodingResult(
        receipt=setup.receipt,
        reference=setup.reference,
        finalized_at=_NOW + timedelta(seconds=3),
        workflow_version=setup.receipt.publishing_workflow_version + 1,
        release_ordinal=1,
    )


@pytest.mark.parametrize("repairs", [0, 1, 5])
def test_receipt_recovers_original_result_and_exact_publishing_version(
    repairs: int,
) -> None:
    setup = context(repairs)

    setup.receipt.require_input(setup.identity, job_id=_JOB)
    replace(setup.receipt).require_input(setup.identity, job_id=_JOB)

    assert setup.receipt.publishing_workflow_version == 4 + 2 * repairs
    assert setup.receipt.repair_cycles == repairs
    assert setup.receipt.recorded_at == _NOW


@pytest.mark.parametrize(
    "field",
    [
        "result_id",
        "worker_id",
        "request_id",
        "claim_request_id",
        "job_id",
        "attempt_id",
        "repository_id",
        "execution_id",
    ],
)
def test_every_receipt_identity_requires_a_uuid(field: str) -> None:
    receipt = context().receipt
    changes: dict[str, Any] = {field: "00000000-0000-0000-0000-000000000001"}
    with pytest.raises(ValueError):
        replace(receipt, **changes)


@pytest.mark.parametrize(
    "field", ["definition_digest", "request_fingerprint", "candidate_revision"]
)
@pytest.mark.parametrize("value", [None, True, "UPPERCASE", "a" * 39, "F" * 64])
def test_receipt_digests_are_full_canonical_lowercase_values(
    field: str, value: Any
) -> None:
    with pytest.raises(ValueError):
        replace(context().receipt, **{field: value})


@pytest.mark.parametrize(
    "field,value",
    [
        ("repair_cycles", True),
        ("repair_cycles", -1),
        ("repair_cycles", 6),
        ("publishing_workflow_version", True),
        ("publishing_workflow_version", 3),
        ("publishing_workflow_version", 6),
        ("protocol_version", True),
        ("protocol_version", 2),
        ("recorded_at", None),
        ("recorded_at", datetime(2026, 10, 18)),
    ],
)
def test_receipt_versions_and_timestamp_are_bounded_and_typed(
    field: str, value: Any
) -> None:
    with pytest.raises(ValueError):
        replace(context().receipt, **{field: value})


def test_receipt_requires_independently_expected_job_identity() -> None:
    setup = context()
    with pytest.raises(ValueError):
        setup.receipt.require_input(setup.identity, job_id=UUID(int=999))
    with pytest.raises(ValueError):
        setup.receipt.require_input(setup.identity, job_id="not-a-uuid")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "field",
    [
        "worker_id",
        "request_id",
        "claim_request_id",
        "job_id",
        "attempt_id",
        "repository_id",
        "execution_id",
        "definition_digest",
        "candidate_revision",
        "repair_cycles",
    ],
)
def test_matching_fingerprint_does_not_hide_changed_receipt_association(
    field: str,
) -> None:
    setup = context()
    changes: dict[str, Any] = {field: UUID(int=999)}
    if field == "definition_digest":
        changes[field] = "f" * 64
    elif field == "candidate_revision":
        changes[field] = "f" * 40
    elif field == "repair_cycles":
        changes[field] = 1
        changes["publishing_workflow_version"] = 6
    changed = replace(setup.receipt, **changes)

    with pytest.raises(ValueError):
        changed.require_input(setup.identity, job_id=_JOB)


def test_receipt_uses_original_scoped_fingerprint_replay() -> None:
    setup = context()
    with pytest.raises(ClaimReplayConflict):
        replace(setup.receipt, request_fingerprint="f" * 64).require_input(
            setup.identity, job_id=_JOB
        )
    with pytest.raises(ValueError):
        setup.receipt.require_input(None, job_id=_JOB)  # type: ignore[arg-type]


@pytest.mark.parametrize("recovered", [False, True])
def test_acceptance_is_original_history_with_explicit_recovery_metadata(
    recovered: bool,
) -> None:
    receipt = context().receipt
    accepted = CodingResultAcceptance(receipt=receipt, recovered=recovered)
    assert accepted.receipt is receipt and accepted.recovered is recovered


@pytest.mark.parametrize(
    "changes", [{"receipt": None}, {"recovered": 1}, {"recovered": "false"}]
)
def test_acceptance_rejects_untyped_metadata(changes: dict[str, Any]) -> None:
    accepted = CodingResultAcceptance(receipt=context().receipt, recovered=False)
    with pytest.raises(ValueError):
        replace(accepted, **changes)


@pytest.mark.parametrize("version", [1, 2046])
def test_verified_publication_keeps_exact_invocation_and_provider_identity(
    version: int,
) -> None:
    setup = context()
    observed = VerifiedCodingPublication(
        result_id=setup.receipt.result_id,
        invocation_id=UUID(int=903),
        expected_operation_version=version,
        reference=setup.reference,
        provider_repository_id=42,
        observed_at=_NOW,
    )
    assert observed.reference is setup.reference
    assert observed.expected_operation_version == version
    assert observed.observed_at == _NOW


@pytest.mark.parametrize(
    "field,value",
    [
        ("result_id", "not-a-uuid"),
        ("invocation_id", None),
        ("expected_operation_version", True),
        ("expected_operation_version", 0),
        ("expected_operation_version", 2047),
        ("expected_operation_version", 1.0),
        ("provider_repository_id", True),
        ("provider_repository_id", 0),
        ("provider_repository_id", 2**63),
        ("reference", None),
        ("observed_at", None),
        ("observed_at", datetime(2026, 10, 18)),
    ],
)
def test_publication_observation_requires_finite_typed_broker_metadata(
    field: str, value: Any
) -> None:
    setup = context()
    observed = VerifiedCodingPublication(
        result_id=setup.receipt.result_id,
        invocation_id=UUID(int=903),
        expected_operation_version=1,
        reference=setup.reference,
        provider_repository_id=42,
        observed_at=_NOW,
    )
    with pytest.raises(ValueError):
        replace(observed, **{field: value})


@pytest.mark.parametrize("repairs", [0, 1, 5])
def test_terminal_result_binds_checked_head_repository_and_next_workflow_version(
    repairs: int,
) -> None:
    setup = context(repairs)
    completed = terminal(setup)
    assert completed.receipt is setup.receipt
    assert completed.reference.head_revision == setup.receipt.candidate_revision
    assert completed.reference.repository_id == setup.receipt.repository_id
    assert completed.workflow_version == 5 + 2 * repairs
    assert completed.release_ordinal == 1
    assert completed.finalized_at > completed.receipt.recorded_at


@pytest.mark.parametrize(
    "field,value",
    [
        ("receipt", None),
        ("reference", None),
        ("workflow_version", True),
        ("workflow_version", 4),
        ("workflow_version", 6),
        ("release_ordinal", True),
        ("release_ordinal", 0),
        ("release_ordinal", 2**63),
        ("finalized_at", None),
        ("finalized_at", datetime(2026, 10, 18)),
        ("finalized_at", _NOW - timedelta(microseconds=1)),
    ],
)
def test_terminal_result_rejects_invalid_association_version_or_time(
    field: str, value: Any
) -> None:
    with pytest.raises(ValueError):
        replace(terminal(context()), **{field: value})


@pytest.mark.parametrize(
    "field,value", [("repository_id", UUID(int=999)), ("head_revision", "f" * 40)]
)
def test_terminal_reference_cannot_change_checked_repository_or_head(
    field: str, value: Any
) -> None:
    setup = context()
    with pytest.raises(ValueError):
        replace(terminal(setup), reference=replace(setup.reference, **{field: value}))


def test_terminal_metadata_does_not_claim_independent_branch_verification() -> None:
    setup = context()
    reference = replace(
        setup.reference, source_branch="other/source", target_branch="other/target"
    )
    # The future authenticated transport/store must compare the original full definition.
    completed = replace(
        terminal(setup), reference=reference, finalized_at=setup.receipt.recorded_at
    )
    assert completed.reference == reference
    assert completed.finalized_at == setup.receipt.recorded_at


@pytest.mark.parametrize(
    "state,version",
    [
        (CodingPublicationState.ACCEPTED, 0),
        (CodingPublicationState.PUBLISHING, 1),
        (CodingPublicationState.UNCERTAIN, 2),
        (CodingPublicationState.FINALIZED, 2),
        (CodingPublicationState.FINALIZED, 2047),
    ],
)
def test_status_distinguishes_initial_pending_uncertain_and_terminal_history(
    state: CodingPublicationState, version: int
) -> None:
    setup = context()
    status = CodingResultStatus(
        receipt=setup.receipt,
        state=state,
        operation_version=version,
        terminal=terminal(setup) if state is CodingPublicationState.FINALIZED else None,
    )
    assert (status.terminal is not None) is (state is CodingPublicationState.FINALIZED)
    assert status.operation_version == version


@pytest.mark.parametrize(
    "state,version",
    [
        (CodingPublicationState.ACCEPTED, 1),
        (CodingPublicationState.PUBLISHING, 0),
        (CodingPublicationState.UNCERTAIN, 0),
        (CodingPublicationState.FINALIZED, 0),
        (CodingPublicationState.PUBLISHING, -1),
        (CodingPublicationState.PUBLISHING, 2048),
        (CodingPublicationState.PUBLISHING, True),
    ],
)
def test_status_versions_cannot_invent_or_erase_operation_history(
    state: CodingPublicationState, version: int
) -> None:
    setup = context()
    with pytest.raises(ValueError):
        CodingResultStatus(
            receipt=setup.receipt,
            state=state,
            operation_version=version,
            terminal=terminal(setup)
            if state is CodingPublicationState.FINALIZED
            else None,
        )


@pytest.mark.parametrize(
    "state",
    [
        CodingPublicationState.ACCEPTED,
        CodingPublicationState.PUBLISHING,
        CodingPublicationState.UNCERTAIN,
    ],
)
def test_nonterminal_status_cannot_carry_terminal_acknowledgement(
    state: CodingPublicationState,
) -> None:
    setup = context()
    with pytest.raises(ValueError):
        CodingResultStatus(
            receipt=setup.receipt,
            state=state,
            operation_version=0 if state is CodingPublicationState.ACCEPTED else 1,
            terminal=terminal(setup),
        )


@pytest.mark.parametrize("value", [None, {}, True])
def test_finalized_status_requires_typed_terminal_metadata(value: Any) -> None:
    setup = context()
    with pytest.raises(ValueError):
        CodingResultStatus(
            receipt=setup.receipt,
            state=CodingPublicationState.FINALIZED,
            operation_version=2,
            terminal=value,
        )


def test_terminal_status_must_retain_the_exact_acceptance_receipt() -> None:
    setup = context()
    changed = replace(setup.receipt, result_id=UUID(int=999))
    with pytest.raises(ValueError):
        CodingResultStatus(
            receipt=changed,
            state=CodingPublicationState.FINALIZED,
            operation_version=2,
            terminal=terminal(setup),
        )


def test_nested_receipts_and_publication_reference_are_revalidated() -> None:
    setup = context()
    object.__setattr__(setup.receipt, "repair_cycles", True)
    with pytest.raises(ValueError):
        CodingResultAcceptance(receipt=setup.receipt, recovered=False)
    setup = context()
    object.__setattr__(setup.reference, "number", 0)
    with pytest.raises(ValueError):
        terminal(setup)


def test_all_reported_datetimes_normalize_to_utc_without_changing_the_instant() -> None:
    setup = context()
    local = _NOW.astimezone(timezone(timedelta(hours=3)))
    receipt = replace(setup.receipt, recorded_at=local)
    completed = replace(terminal(setup), finalized_at=local)
    observed = VerifiedCodingPublication(
        result_id=receipt.result_id,
        invocation_id=UUID(int=903),
        expected_operation_version=1,
        reference=setup.reference,
        provider_repository_id=42,
        observed_at=local,
    )
    assert receipt.recorded_at == completed.finalized_at == observed.observed_at == _NOW
    assert (
        receipt.recorded_at.tzinfo
        is completed.finalized_at.tzinfo
        is observed.observed_at.tzinfo
        is UTC
    )


def test_receipt_status_and_terminal_are_history_not_current_work_capabilities() -> (
    None
):
    setup = context()
    values = (
        setup.receipt,
        CodingResultAcceptance(receipt=setup.receipt, recovered=True),
        terminal(setup),
        CodingResultStatus(
            receipt=setup.receipt,
            state=CodingPublicationState.ACCEPTED,
            operation_version=0,
        ),
    )
    for value in values:
        names = {item.name for item in fields(value)}
        assert (
            not {"proof", "lease_authority_active", "deadline", "handles", "relaunch"}
            & names
        )
        assert "Sanitized check tail" not in repr(value)
        assert "Apply candidate" not in repr(value)
