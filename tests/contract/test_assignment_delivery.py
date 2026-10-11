import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import timedelta
from typing import cast
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi.testclient import TestClient

from devrelay.api.dependencies import WorkerClaimOperations
from devrelay.api.schemas.claims import AssignmentDeliveryResponse
from devrelay.application.contracts.assignment_delivery import (
    MAX_ASSIGNMENT_BYTES,
    AssignmentDelivery,
    InvalidAssignmentDefinition,
)
from devrelay.application.contracts.assignments import ClaimResult
from devrelay.application.contracts.claims import LeaseProof, WorkerClaimRequest
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.control_gateway import InvalidControlResponse
from devrelay.bootstrap import control
from devrelay.bootstrap.control import create_app
from devrelay.bootstrap.security import worker_authenticator
from devrelay.config.control import ControlSettings
from devrelay.infrastructure.control.gateway import HttpControlGateway
from tests.assignment_fixtures import NOW, assigned_result

TOKEN = "test_only_worker_token_0123456789abcdefghijklmnop"


def authority() -> WorkerAuthorization:
    return WorkerAuthorization(
        worker_id=uuid4(),
        allowed_repository_ids=frozenset({uuid4()}),
        credential_expires_at=NOW + timedelta(hours=1),
    )


@pytest.fixture
def delivery() -> AssignmentDelivery:
    authorization = authority()
    return AssignmentDelivery.from_result(
        assigned_result(authorization),
        worker_id=authorization.worker_id,
        claim_request_id=uuid4(),
    )


def test_api_delivery_round_trips_full_immutable_inputs_through_worker_transport(
    delivery: AssignmentDelivery,
) -> None:
    reference = delivery.outcome.assignment
    assert reference is not None
    authorization = WorkerAuthorization(
        worker_id=reference.worker_id,
        allowed_repository_ids=frozenset({reference.repository_id}),
        credential_expires_at=NOW + timedelta(hours=1),
    )
    pending = WorkerClaimRequest(
        claim_request_id=delivery.outcome.claim_request_id, proof=LeaseProof.generate()
    )
    response = AssignmentDeliveryResponse.render(delivery)
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200, headers=dict(response.headers), content=bytes(response.body)
        )

    async def scenario() -> AssignmentDelivery:
        async with httpx.AsyncClient(
            base_url="https://control.example/prefix/",
            transport=httpx.MockTransport(respond),
        ) as client:
            return await HttpControlGateway(client, now=lambda: NOW).read_assignment(
                reference.attempt_id, pending, authorization
            )

    received = asyncio.run(scenario())

    assert received == delivery
    assert (
        received.definition.snapshot.description
        == delivery.definition.snapshot.description
    )
    assert received.definition.checks[0].argv == ("python", "-m", "pytest")
    assert (
        requests[0].url.path
        == f"/prefix/api/v1/worker/attempts/{reference.attempt_id}/assignment"
    )
    assert pending.proof.reveal() not in bytes(response.body).decode()
    assert "test-operator" not in bytes(response.body).decode()


@pytest.mark.parametrize(
    "change",
    [
        "task",
        "digest",
        "base",
        "check",
        "path",
        "unknown",
        "nested_extra",
        "protocol",
        "repository",
        "worker",
        "request",
        "snapshot_version",
    ],
)
def test_worker_rejects_changed_or_uncorrelated_delivery(
    delivery: AssignmentDelivery, change: str
) -> None:
    reference = delivery.outcome.assignment
    assert reference is not None
    body = cast(
        dict[str, object],
        json.loads(bytes(AssignmentDeliveryResponse.render(delivery).body)),
    )
    definition = cast(dict[str, object], body["definition"])
    snapshot = cast(dict[str, object], definition["snapshot"])
    outcome = cast(dict[str, object], body["outcome"])
    if change == "task":
        snapshot["description"] = "Changed requirements."
    elif change == "digest":
        body["definition_fingerprint"] = "0" * 64
    elif change == "base":
        definition["base_revision"] = "b" * 40
    elif change in ("check", "path"):
        checks = cast(list[dict[str, object]], definition["checks"])
        checks[0]["argv" if change == "check" else "working_directory"] = (
            ["malicious"] if change == "check" else "../escape"
        )
    elif change == "unknown":
        body["launch"] = True
    elif change == "nested_extra":
        snapshot["instructions"] = "Execute now."
    elif change == "protocol":
        outcome["protocol_version"] = True
    elif change == "repository":
        definition["repository_id"] = str(uuid4())
    elif change in ("worker", "request"):
        outcome["worker_id" if change == "worker" else "claim_request_id"] = str(
            uuid4()
        )
    else:
        snapshot["fingerprint_version"] = True
    authorization = WorkerAuthorization(
        worker_id=reference.worker_id,
        allowed_repository_ids=frozenset({reference.repository_id}),
        credential_expires_at=NOW + timedelta(hours=1),
    )
    pending = WorkerClaimRequest(
        claim_request_id=delivery.outcome.claim_request_id, proof=LeaseProof.generate()
    )

    async def scenario() -> None:
        async with httpx.AsyncClient(
            base_url="https://control.example/",
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, json=body)
            ),
        ) as client:
            with pytest.raises(InvalidControlResponse):
                await HttpControlGateway(client, now=lambda: NOW).read_assignment(
                    reference.attempt_id, pending, authorization
                )

    asyncio.run(scenario())


def test_delivery_larger_than_claim_metadata_limit_is_supported(
    delivery: AssignmentDelivery,
) -> None:
    definition = replace(
        delivery.definition,
        snapshot=replace(delivery.definition.snapshot, description="x" * 65_536),
    )
    large = replace(delivery, definition=definition)
    response = AssignmentDeliveryResponse.render(large)
    reference = delivery.outcome.assignment
    assert reference is not None
    authorization = WorkerAuthorization(
        worker_id=reference.worker_id,
        allowed_repository_ids=frozenset({reference.repository_id}),
        credential_expires_at=NOW + timedelta(hours=1),
    )
    pending = WorkerClaimRequest(
        claim_request_id=delivery.outcome.claim_request_id, proof=LeaseProof.generate()
    )

    async def scenario() -> AssignmentDelivery:
        async with httpx.AsyncClient(
            base_url="https://control.example/",
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    200, headers=dict(response.headers), content=bytes(response.body)
                )
            ),
        ) as client:
            return await HttpControlGateway(client, now=lambda: NOW).read_assignment(
                reference.attempt_id, pending, authorization
            )

    received = asyncio.run(scenario())

    assert len(response.body) > 16_384
    assert received.definition == definition


def test_assignment_endpoint_authenticates_and_uses_only_original_proof(
    worker_configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    values = worker_configuration.model_dump()
    values["worker_claims"] = {}
    configuration = ControlSettings.model_validate(values)
    registration = configuration.workers[0]
    authorization = WorkerAuthorization(
        worker_id=registration.worker_id,
        allowed_repository_ids=registration.allowed_repository_ids,
        credential_expires_at=registration.expires_at,
    )
    result = assigned_result(authorization)
    assert result.attempt is not None
    attempt_id = result.attempt.id
    calls: list[WorkerClaimRequest] = []
    proof = LeaseProof.generate()
    request_id = uuid4()

    async def claim(
        request: WorkerClaimRequest, authority: WorkerAuthorization
    ) -> ClaimResult:
        raise AssertionError("Payload retrieval must not claim work.")

    async def read(
        attempt: UUID, request: WorkerClaimRequest, authority: WorkerAuthorization
    ) -> AssignmentDelivery:
        assert attempt == attempt_id and authority == authorization
        calls.append(request)
        return AssignmentDelivery.from_result(
            result,
            worker_id=authority.worker_id,
            claim_request_id=request.claim_request_id,
        )

    @asynccontextmanager
    async def resources() -> AsyncIterator[WorkerClaimOperations]:
        yield WorkerClaimOperations(claim=claim, read_assignment=read)

    monkeypatch.setattr(
        control,
        "worker_authenticator",
        lambda registrations: worker_authenticator(registrations, now=lambda: NOW),
    )
    app = create_app(configuration, claim_resources=resources)
    path = f"/api/v1/worker/attempts/{attempt_id}/assignment"
    body = {
        "protocol_version": 1,
        "claim_request_id": str(request_id),
        "proof": proof.reveal(),
    }

    with TestClient(app) as client:
        rejected = client.post(path, json=body)
        invalid_path = client.post(
            f"/api/v1/worker/attempts/{TOKEN}/assignment",
            headers={"Authorization": f"Bearer {TOKEN}"},
            json=body,
        )
        response = client.post(
            path, headers={"Authorization": f"Bearer {TOKEN}"}, json=body
        )

    assert rejected.status_code == 401 and response.status_code == 200
    assert invalid_path.status_code == 422
    assert invalid_path.json()["detail"]["code"] == "invalid_request"
    assert TOKEN not in invalid_path.text and proof.reveal() not in invalid_path.text
    assert len(calls) == 1 and calls[0].proof.reveal() == proof.reveal()
    assert response.headers["cache-control"] == "no-store"
    assert proof.reveal() not in response.text and TOKEN not in response.text
    assert response.json()["definition"]["base_revision"] == "a" * 40


def test_worst_task_json_escaping_and_large_allowed_checks_fit_delivery_budget(
    delivery: AssignmentDelivery,
) -> None:
    snapshot = replace(
        delivery.definition.snapshot,
        description="\x01" * 65_536,
        acceptance_criteria=("\x01" * 4096,) * 32,
    )
    check = delivery.definition.checks[0]
    checks = tuple(
        replace(check, id=f"check_{index}", argv=("python",) + ("x" * 4096,) * 63)
        for index in range(2)
    )
    value = replace(
        delivery,
        definition=replace(delivery.definition, snapshot=snapshot, checks=checks),
    )

    response = AssignmentDeliveryResponse.render(value)

    assert 1_000_000 < len(response.body) < MAX_ASSIGNMENT_BYTES


def test_oversized_stored_check_policy_fails_closed_without_a_partial_payload(
    delivery: AssignmentDelivery,
) -> None:
    check = delivery.definition.checks[0]
    checks = tuple(
        replace(check, id=f"check_{index}", argv=("python",) + ("x" * 4096,) * 63)
        for index in range(3)
    )
    value = replace(delivery, definition=replace(delivery.definition, checks=checks))

    with pytest.raises(InvalidAssignmentDefinition):
        AssignmentDeliveryResponse.render(value)
