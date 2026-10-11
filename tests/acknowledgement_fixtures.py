from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

from devrelay.application.contracts.acknowledgements import WorkerAcknowledgementIntent
from devrelay.application.contracts.assignment_delivery import AssignmentDelivery
from devrelay.application.contracts.claims import LeaseProof, WorkerClaimRequest
from devrelay.application.contracts.workers import WorkerAuthorization
from tests.assignment_fixtures import NOW, assigned_result


def acknowledgement_inputs() -> tuple[WorkerClaimRequest, AssignmentDelivery]:
    authorization = WorkerAuthorization(
        worker_id=uuid4(),
        allowed_repository_ids=frozenset({uuid4()}),
        credential_expires_at=NOW + timedelta(hours=1),
    )
    request = WorkerClaimRequest(claim_request_id=uuid4(), proof=LeaseProof.generate())
    delivery = AssignmentDelivery.from_result(
        assigned_result(authorization),
        worker_id=authorization.worker_id,
        claim_request_id=request.claim_request_id,
    )
    return request, delivery


def acknowledgement_intent(delivery: AssignmentDelivery) -> WorkerAcknowledgementIntent:
    reference = delivery.outcome.assignment
    assert reference is not None
    return WorkerAcknowledgementIntent(
        acknowledgement_request_id=uuid4(),
        claim_request_id=delivery.outcome.claim_request_id,
        attempt_id=reference.attempt_id,
        generation=reference.generation,
        snapshot_id=delivery.definition.snapshot.id,
        base_revision=delivery.definition.base_revision,
        definition_digest=delivery.definition.fingerprint(),
    )


def changed_intent(
    intent: WorkerAcknowledgementIntent, field: str
) -> WorkerAcknowledgementIntent:
    if field == "claim_request_id":
        return replace(intent, claim_request_id=uuid4())
    if field == "attempt_id":
        return replace(intent, attempt_id=uuid4())
    if field == "snapshot_id":
        return replace(intent, snapshot_id=uuid4())
    if field == "base_revision":
        return replace(intent, base_revision="b" * 40)
    if field == "definition_digest":
        return replace(intent, definition_digest="b" * 64)
    raise ValueError("Unknown test mutation.")
