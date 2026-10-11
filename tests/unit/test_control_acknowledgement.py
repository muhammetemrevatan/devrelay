import asyncio
import threading
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from devrelay.application.acknowledgements import AcknowledgeAssignedJob
from devrelay.application.assignment_delivery import ReadAssignedJob
from devrelay.application.contracts.acknowledgements import (
    AcknowledgementIdentity,
    AcknowledgementRejected,
    AcknowledgementResult,
    WorkerAcknowledgementIntent,
    WorkerAcknowledgementRequest,
)
from devrelay.application.contracts.assignment_delivery import (
    AssignedJobDefinition,
    AssignmentDelivery,
)
from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    InvalidLeaseProof,
    LeaseProof,
    WorkerClaimRequest,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.bootstrap.claims import job_acknowledger
from devrelay.config.control import ControlSettings
from devrelay.config.worker_claims import WorkerClaimsSettings
from tests.acknowledgement_fixtures import acknowledgement_intent, changed_intent
from tests.assignment_fixtures import NOW, assigned_result
from tests.integration.test_worker_journal import TOKEN
from tests.unit.test_assignment_delivery import Store, authority


class Writer:
    def __init__(self, delivery: AssignmentDelivery, calls: list[str]) -> None:
        self.delivery = delivery
        self.calls = calls
        self.identity: AcknowledgementIdentity | None = None
        self.digest: str | None = None

    async def acknowledge(
        self,
        identity: AcknowledgementIdentity,
        authorization: WorkerAuthorization,
        *,
        definition_digest: str,
    ) -> AcknowledgementResult:
        self.calls.append("acknowledge")
        self.identity, self.digest = identity, definition_digest
        reference = self.delivery.outcome.assignment
        assert reference is not None
        return AcknowledgementResult(
            worker_id=authorization.worker_id,
            job_id=reference.job_id,
            repository_id=reference.repository_id,
            intent=identity.intent,
            acknowledged_at=reference.issued_at + timedelta(seconds=1),
            expires_at=reference.expires_at,
            recovered=False,
            lease_authority_active=True,
        )


@dataclass(frozen=True)
class Context:
    service: AcknowledgeAssignedJob
    request: WorkerAcknowledgementRequest
    authorization: WorkerAuthorization
    writer: Writer
    calls: list[str]


def context() -> Context:
    authorization = authority()
    pending = WorkerClaimRequest(claim_request_id=uuid4(), proof=LeaseProof.generate())
    result = assigned_result(authorization)
    delivery = AssignmentDelivery.from_result(
        result,
        worker_id=authorization.worker_id,
        claim_request_id=pending.claim_request_id,
    )
    calls: list[str] = []
    writer = Writer(delivery, calls)
    reader = ReadAssignedJob(Store(result, calls), lambda proof: calls.append("proof"))
    return Context(
        AcknowledgeAssignedJob(reader, writer),
        WorkerAcknowledgementRequest(
            intent=acknowledgement_intent(delivery), claim=pending
        ),
        authorization,
        writer,
        calls,
    )


def test_control_acknowledgement_validates_original_proof_and_pins_before_write() -> (
    None
):
    setup = context()

    result = asyncio.run(setup.service.execute(setup.request, setup.authorization))

    assert setup.calls == ["proof", "recover", "acknowledge"]
    assert setup.writer.digest == setup.request.intent.definition_digest
    assert (
        setup.writer.identity is not None
        and setup.writer.identity.claim.worker_id == setup.authorization.worker_id
    )
    assert result.intent == setup.request.intent
    assert setup.request.claim.proof.reveal() not in repr(setup.request)
    assert setup.request.claim.proof.reveal() not in repr(setup.writer.identity)


@pytest.mark.parametrize("field", ["snapshot_id", "base_revision", "definition_digest"])
def test_control_rejects_changed_delivered_pins_before_mutation(field: str) -> None:
    setup = context()
    invalid = replace(setup.request, intent=changed_intent(setup.request.intent, field))

    with pytest.raises(AcknowledgementRejected):
        asyncio.run(setup.service.execute(invalid, setup.authorization))

    assert setup.calls == ["proof", "recover"] and setup.writer.identity is None


def test_independent_proof_guard_precedes_any_acknowledgement_io() -> None:
    setup = context()

    def reject(proof: LeaseProof) -> None:
        raise InvalidLeaseProof("Known credential cannot be used as proof.")

    service = AcknowledgeAssignedJob(
        ReadAssignedJob(Store(None, setup.calls), reject), setup.writer
    )

    with pytest.raises(InvalidLeaseProof):
        asyncio.run(service.execute(setup.request, setup.authorization))

    assert setup.calls == []


def test_definition_hash_does_not_block_the_event_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup = context()
    original = AssignedJobDefinition.fingerprint
    owner = threading.get_ident()
    threads: list[int] = []

    def fingerprint(definition: AssignedJobDefinition) -> str:
        threads.append(threading.get_ident())
        return original(definition)

    monkeypatch.setattr(AssignedJobDefinition, "fingerprint", fingerprint)

    asyncio.run(setup.service.execute(setup.request, setup.authorization))

    assert threads and all(identity != owner for identity in threads)


def test_acknowledgement_request_cannot_substitute_claim_identity() -> None:
    setup = context()
    intent = replace(setup.request.intent, claim_request_id=uuid4())

    with pytest.raises(ValueError):
        WorkerAcknowledgementRequest(intent=intent, claim=setup.request.claim)


@pytest.mark.parametrize(
    "field",
    [
        "snapshot_id",
        "base_revision",
        "definition_digest",
        "attempt_id",
        "claim_request_id",
    ],
)
def test_acknowledgement_fingerprint_binds_complete_operation_input(field: str) -> None:
    setup = context()
    original = setup.request.identify(setup.authorization)
    intent = changed_intent(setup.request.intent, field)
    pending = (
        setup.request.claim
        if field != "claim_request_id"
        else replace(setup.request.claim, claim_request_id=intent.claim_request_id)
    )
    changed = WorkerAcknowledgementRequest(intent=intent, claim=pending).identify(
        setup.authorization
    )

    with pytest.raises(ClaimReplayConflict):
        changed.require_replay(original.fingerprint)


@pytest.mark.parametrize("change", ["worker", "proof", "request"])
def test_acknowledgement_fingerprint_fences_actor_proof_and_operation_identity(
    change: str,
) -> None:
    setup = context()
    original = setup.request.identify(setup.authorization)
    pending, authorization = setup.request, setup.authorization
    if change == "worker":
        authorization = replace(authorization, worker_id=uuid4())
    elif change == "proof":
        pending = replace(
            pending, claim=replace(pending.claim, proof=LeaseProof.generate())
        )
    else:
        pending = replace(
            pending, intent=replace(pending.intent, acknowledgement_request_id=uuid4())
        )
    changed = pending.identify(authorization)

    with pytest.raises(ClaimReplayConflict):
        changed.require_replay(original.fingerprint)


@pytest.mark.parametrize("fingerprint", ["é" * 64, "a" * 63, "A" * 64, 1])
def test_malformed_acknowledgement_fingerprint_is_a_replay_conflict(
    fingerprint: object,
) -> None:
    setup = context()
    identity = setup.request.identify(setup.authorization)

    with pytest.raises(ClaimReplayConflict):
        identity.require_replay(cast(str, fingerprint))


@pytest.mark.parametrize(
    "field,value",
    [
        ("workflow_version", True),
        ("workflow_version", 3),
        ("recovered", 1),
        ("lease_authority_active", 1),
        ("acknowledged_at", NOW + timedelta(days=1)),
    ],
)
def test_acknowledgement_result_rejects_inconsistent_evidence(
    field: str, value: object
) -> None:
    setup = context()
    result = asyncio.run(setup.service.execute(setup.request, setup.authorization))

    with pytest.raises(ValueError):
        replace(result, **cast(Any, {field: value}))


def test_acknowledgement_fingerprint_v1_golden_vector() -> None:
    pending = WorkerClaimRequest(claim_request_id=UUID(int=2), proof=LeaseProof(TOKEN))
    intent = WorkerAcknowledgementIntent(
        acknowledgement_request_id=UUID(int=4),
        claim_request_id=pending.claim_request_id,
        attempt_id=UUID(int=5),
        snapshot_id=UUID(int=6),
        base_revision="a" * 40,
        definition_digest="b" * 64,
        generation=1,
    )
    authorization = WorkerAuthorization(
        worker_id=UUID(int=1),
        allowed_repository_ids=frozenset({UUID(int=3)}),
        credential_expires_at=datetime(2026, 10, 11, tzinfo=UTC),
    )
    request = WorkerAcknowledgementRequest(intent=intent, claim=pending)

    identity = request.identify(authorization)

    assert (
        identity.fingerprint
        == "e19e7485ac2732117cc97687362d34d29a867247a9cf1fbaf9d797780e24531a"
    )


def test_acknowledgement_composition_requires_explicit_claim_gate(
    worker_configuration: ControlSettings,
) -> None:
    engine = cast(AsyncEngine, None)

    with pytest.raises(ValueError, match="explicitly enabled claims"):
        job_acknowledger(worker_configuration, engine)


def test_acknowledgement_composition_opens_no_database_or_provider_connection(
    worker_configuration: ControlSettings,
) -> None:
    settings = worker_configuration.model_copy(
        update={"worker_claims": WorkerClaimsSettings()}
    )
    engine = create_async_engine(
        "postgresql+psycopg://devrelay_test:unused@127.0.0.1:1/devrelay_test"
    )

    service = job_acknowledger(settings, engine)

    assert isinstance(service, AcknowledgeAssignedJob)
    asyncio.run(engine.dispose())
