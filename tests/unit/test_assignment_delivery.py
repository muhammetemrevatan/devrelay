import asyncio
from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

import pytest

from devrelay.application.assignment_delivery import ReadAssignedJob
from devrelay.application.contracts.assignment_delivery import (
    AssignmentDelivery,
    AssignmentUnavailable,
)
from devrelay.application.contracts.assignments import ClaimResult, NoWorkReason
from devrelay.application.contracts.claims import (
    ClaimRequestIdentity,
    InvalidLeaseProof,
    LeaseProof,
    WorkerClaimRequest,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from tests.assignment_fixtures import NOW, assigned_result


class Store:
    def __init__(self, result: ClaimResult | None, calls: list[str]) -> None:
        self.result = result
        self.calls = calls
        self.identity: ClaimRequestIdentity | None = None

    async def recover(
        self, identity: ClaimRequestIdentity, authorization: WorkerAuthorization
    ) -> ClaimResult | None:
        self.calls.append("recover")
        self.identity = identity
        return self.result


def authority() -> WorkerAuthorization:
    return WorkerAuthorization(
        worker_id=uuid4(),
        allowed_repository_ids=frozenset({uuid4()}),
        credential_expires_at=NOW + timedelta(hours=1),
    )


def test_assignment_read_recovers_only_original_receipt_and_keeps_complete_pins() -> (
    None
):
    authorization = authority()
    request = WorkerClaimRequest(claim_request_id=uuid4(), proof=LeaseProof.generate())
    result = assigned_result(authorization)
    assert result.attempt is not None and result.stored is not None
    calls: list[str] = []
    store = Store(result, calls)
    reader = ReadAssignedJob(store, lambda proof: calls.append("proof"))

    delivery = asyncio.run(reader.execute(result.attempt.id, request, authorization))

    assert calls == ["proof", "recover"]
    assert (
        store.identity is not None
        and store.identity.worker_id == authorization.worker_id
    )
    assert delivery.definition.snapshot == result.stored.intent.stored.snapshot
    assert delivery.definition.checks == result.stored.intent.policy.checks
    assert delivery.definition.base_revision == result.stored.job.base_revision
    assert request.proof.reveal() not in repr(delivery)
    assert "arbitrary commands" not in repr(delivery)


@pytest.mark.parametrize("missing", ["unknown", "no_work", "attempt"])
def test_assignment_read_cannot_claim_unknown_or_unrelated_work(missing: str) -> None:
    authorization = authority()
    result = assigned_result(authorization) if missing == "attempt" else None
    if missing == "no_work":
        result = ClaimResult(
            recorded_at=NOW, recovered=True, no_work=NoWorkReason.EMPTY
        )
    calls: list[str] = []
    reader = ReadAssignedJob(Store(result, calls), lambda proof: calls.append("proof"))
    request = WorkerClaimRequest(claim_request_id=uuid4(), proof=LeaseProof.generate())

    with pytest.raises(AssignmentUnavailable):
        asyncio.run(reader.execute(uuid4(), request, authorization))

    assert calls == ["proof", "recover"]


def test_known_credentials_never_reach_assignment_storage() -> None:
    calls: list[str] = []
    store = Store(None, calls)

    def guard(proof: LeaseProof) -> None:
        raise InvalidLeaseProof("Independent proof required.")

    reader = ReadAssignedJob(store, guard)
    request = WorkerClaimRequest(claim_request_id=uuid4(), proof=LeaseProof.generate())
    authorization = authority()

    with pytest.raises(InvalidLeaseProof):
        asyncio.run(reader.execute(uuid4(), request, authorization))

    assert calls == []


@pytest.mark.parametrize(
    "change", ["base", "repository", "snapshot", "argv", "timeout", "branch", "budget"]
)
def test_delivery_fingerprint_covers_accepted_execution_inputs(change: str) -> None:
    authorization = authority()
    result = assigned_result(authorization)
    original = AssignmentDelivery.from_result(
        result, worker_id=authorization.worker_id, claim_request_id=uuid4()
    ).definition
    if change == "base":
        changed = replace(original, base_revision="b" * 40)
    elif change == "repository":
        changed = replace(original, provider_repository_id=456)
    elif change == "snapshot":
        changed = replace(
            original,
            snapshot=replace(original.snapshot, description="Changed requirements."),
        )
    elif change in ("argv", "timeout"):
        check = (
            replace(original.checks[0], argv=("python", "-m", "unittest"))
            if change == "argv"
            else replace(original.checks[0], timeout_seconds=200)
        )
        changed = replace(original, checks=(check,))
    elif change == "branch":
        changed = replace(original, authorized_branch="devrelay/jobs/other")
    else:
        changed = replace(original, max_repair_cycles=1)

    original_fingerprint, changed_fingerprint = (
        original.fingerprint(),
        changed.fingerprint(),
    )

    assert original_fingerprint != changed_fingerprint
