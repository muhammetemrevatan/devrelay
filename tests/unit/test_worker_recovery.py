from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from devrelay.application.contracts.assignments import ClaimResult, NoWorkReason
from devrelay.application.contracts.claims import LeaseProof, WorkerClaimRequest
from devrelay.application.contracts.worker_recovery import (
    AssignmentReference,
    WorkerClaimOutcome,
    WorkerRecoveryPhase,
    WorkerRecoveryState,
)

NOW = datetime(2026, 10, 11, tzinfo=UTC)


def reference() -> AssignmentReference:
    return AssignmentReference(
        worker_id=uuid4(),
        job_id=uuid4(),
        repository_id=uuid4(),
        attempt_id=uuid4(),
        generation=1,
        issued_at=NOW,
        expires_at=NOW + timedelta(seconds=120),
    )


def test_no_work_result_projection_contains_only_correlated_metadata() -> None:
    worker_id, request_id = uuid4(), uuid4()
    result = ClaimResult(recorded_at=NOW, recovered=True, no_work=NoWorkReason.OCCUPIED)

    outcome = WorkerClaimOutcome.from_result(
        result, worker_id=worker_id, claim_request_id=request_id
    )

    assert outcome.worker_id == worker_id and outcome.claim_request_id == request_id
    assert outcome.no_work is NoWorkReason.OCCUPIED and outcome.assignment is None
    assert outcome.recorded_at == NOW


@pytest.mark.parametrize("generation", [0, 2, True])
def test_assignment_requires_supported_initial_generation(generation: int) -> None:
    accepted = reference()

    with pytest.raises(ValueError):
        replace(accepted, generation=generation)


def test_assignment_requires_expiry_after_issuance() -> None:
    accepted = reference()

    with pytest.raises(ValueError):
        replace(accepted, expires_at=NOW)


@pytest.mark.parametrize("mismatch", ["worker", "time", "reason"])
def test_assignment_outcome_validates_worker_chronology_and_shape(
    mismatch: str,
) -> None:
    accepted = reference()
    worker_id = uuid4() if mismatch == "worker" else accepted.worker_id
    recorded_at = NOW + timedelta(seconds=1) if mismatch == "time" else NOW
    reason = NoWorkReason.EMPTY if mismatch == "reason" else None

    with pytest.raises(ValueError):
        WorkerClaimOutcome(
            worker_id=worker_id,
            claim_request_id=uuid4(),
            recorded_at=recorded_at,
            assignment=accepted,
            no_work=reason,
        )


@pytest.mark.parametrize(
    "phase", [WorkerRecoveryPhase.PENDING, WorkerRecoveryPhase.ASSIGNED]
)
def test_unresolved_recovery_requires_original_proof_request(
    phase: WorkerRecoveryPhase,
) -> None:
    with pytest.raises(ValueError):
        WorkerRecoveryState(phase=phase)


def test_assigned_state_redacts_the_original_secret() -> None:
    accepted = reference()
    pending = WorkerClaimRequest(claim_request_id=uuid4(), proof=LeaseProof.generate())
    outcome = WorkerClaimOutcome(
        worker_id=accepted.worker_id,
        claim_request_id=pending.claim_request_id,
        recorded_at=NOW,
        assignment=accepted,
    )

    state = WorkerRecoveryState(
        phase=WorkerRecoveryPhase.ASSIGNED, request=pending, assignment=outcome
    )

    assert state.request is pending
    assert pending.proof.reveal() not in repr(state)
    assert state.assignment == outcome


def test_assigned_state_rejects_another_request_identity() -> None:
    accepted = reference()
    pending = WorkerClaimRequest(claim_request_id=uuid4(), proof=LeaseProof.generate())
    outcome = WorkerClaimOutcome(
        worker_id=accepted.worker_id,
        claim_request_id=uuid4(),
        recorded_at=NOW,
        assignment=accepted,
    )

    with pytest.raises(ValueError):
        WorkerRecoveryState(
            phase=WorkerRecoveryPhase.ASSIGNED, request=pending, assignment=outcome
        )
