import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import cast
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi.testclient import TestClient

from devrelay.api.dependencies import WorkerClaimOperations
from devrelay.api.schemas.acknowledgements import WorkerAcknowledgementResponse
from devrelay.application.contracts.acknowledgements import (
    AcknowledgementRejected,
    AcknowledgementResult,
    WorkerAcknowledgementRequest,
)
from devrelay.application.contracts.assignment_delivery import AssignmentDelivery
from devrelay.application.contracts.assignments import ClaimResult
from devrelay.application.contracts.claims import WorkerClaimRequest
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.control_gateway import InvalidControlResponse
from devrelay.bootstrap import control
from devrelay.bootstrap.control import create_app
from devrelay.bootstrap.security import worker_authenticator
from devrelay.config.control import ControlSettings
from devrelay.infrastructure.control.gateway import HttpControlGateway
from tests.acknowledgement_fixtures import (
    acknowledgement_inputs,
    acknowledgement_intent,
)
from tests.assignment_fixtures import NOW


def exchange() -> tuple[
    WorkerAcknowledgementRequest, WorkerAuthorization, AcknowledgementResult
]:
    claim, delivery = acknowledgement_inputs()
    reference = delivery.outcome.assignment
    assert reference is not None
    request = WorkerAcknowledgementRequest(
        claim=claim, intent=acknowledgement_intent(delivery)
    )
    authority = WorkerAuthorization(
        worker_id=reference.worker_id,
        allowed_repository_ids=frozenset({reference.repository_id}),
        credential_expires_at=NOW + timedelta(hours=1),
    )
    result = AcknowledgementResult(
        worker_id=reference.worker_id,
        job_id=reference.job_id,
        repository_id=reference.repository_id,
        intent=request.intent,
        acknowledged_at=reference.issued_at + timedelta(seconds=1),
        expires_at=reference.expires_at,
        recovered=False,
        lease_authority_active=True,
    )
    return request, authority, result


@pytest.mark.parametrize(
    "change",
    [
        None,
        "worker",
        "scope",
        "intent",
        "protocol_bool",
        "workflow_bool",
        "active_int",
        "extra",
        "nested_extra",
        "expired_cap",
        "duplicate",
        "oversized",
        "compressed",
        "redirect",
    ],
)
def test_bounded_strict_correlated_acknowledgement_transport(
    change: str | None,
) -> None:
    request, authority, result = exchange()
    body = cast(
        dict[str, object],
        json.loads(WorkerAcknowledgementResponse(result=result).model_dump_json()),
    )
    values = cast(dict[str, object], body["result"])
    intent = cast(dict[str, object], values["intent"])
    if change in ("worker", "scope"):
        values["worker_id" if change == "worker" else "repository_id"] = str(uuid4())
    elif change == "intent":
        intent["acknowledgement_request_id"] = str(uuid4())
    elif change == "protocol_bool":
        intent["protocol_version"] = True
    elif change == "workflow_bool":
        values["workflow_version"] = True
    elif change == "active_int":
        values["lease_authority_active"] = 1
    elif change == "extra":
        body["launch"] = True
    elif change == "nested_extra":
        intent["launch"] = True
    elif change == "expired_cap":
        values["expires_at"] = (
            authority.credential_expires_at + timedelta(seconds=1)
        ).isoformat()
    content = json.dumps(body).encode()
    if change == "duplicate":
        content = b'{"result":null,' + content[1:]
    if change == "oversized":
        content += b" " * 16_384
    inputs: list[httpx.Request] = []

    def respond(sent: httpx.Request) -> httpx.Response:
        inputs.append(sent)
        return httpx.Response(
            302 if change == "redirect" else 200,
            stream=httpx.ByteStream(content),
            headers={
                "Content-Type": "application/json",
                **({"Content-Encoding": "gzip"} if change == "compressed" else {}),
            },
        )

    async def run() -> None:
        async with httpx.AsyncClient(
            base_url="https://control.example/prefix/",
            transport=httpx.MockTransport(respond),
        ) as client:
            gateway = HttpControlGateway(client, now=lambda: NOW)
            if change is not None:
                with pytest.raises(InvalidControlResponse):
                    await gateway.acknowledge(request, authority)
            else:
                assert await gateway.acknowledge(request, authority) == result

    asyncio.run(run())
    assert len(inputs) == 1
    assert (
        inputs[0].url.path
        == f"/prefix/api/v1/worker/attempts/{request.intent.attempt_id}/acknowledgements"
    )
    assert json.loads(inputs[0].content)["proof"] == request.claim.proof.reveal()


def test_opt_in_acknowledgement_route_auth_body_errors_and_shutdown(
    worker_configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request, _, result = exchange()
    values = worker_configuration.model_dump()
    values["worker_claims"] = {}
    configuration = ControlSettings.model_validate(values)
    calls: list[WorkerAcknowledgementRequest] = []
    error: Exception | None = None

    async def claim(
        request: WorkerClaimRequest, authority: WorkerAuthorization
    ) -> ClaimResult:
        raise AssertionError("Acknowledgement must not claim work.")

    async def read(
        attempt: UUID, request: WorkerClaimRequest, authority: WorkerAuthorization
    ) -> AssignmentDelivery:
        raise AssertionError("Injected acknowledgement owns its read boundary.")

    async def acknowledge(
        input: WorkerAcknowledgementRequest, authority: WorkerAuthorization
    ) -> AcknowledgementResult:
        assert authority.worker_id == configuration.workers[0].worker_id
        calls.append(input)
        if error is not None:
            raise error
        return result

    @asynccontextmanager
    async def resources() -> AsyncIterator[WorkerClaimOperations]:
        yield WorkerClaimOperations(
            claim=claim, read_assignment=read, acknowledge=acknowledge
        )

    monkeypatch.setattr(
        control,
        "worker_authenticator",
        lambda registrations: worker_authenticator(registrations, now=lambda: NOW),
    )
    app = create_app(configuration, claim_resources=resources)
    path = f"/api/v1/worker/attempts/{request.intent.attempt_id}/acknowledgements"
    body = json.loads(WorkerAcknowledgementResponse(result=result).model_dump_json())[
        "result"
    ]["intent"]
    payload = {"intent": body, "proof": request.claim.proof.reveal()}
    headers = {
        "Authorization": "Bearer "
        + configuration.workers[0].auth_token.get_secret_value()
    }
    with TestClient(create_app()) as client:
        assert client.post(path, json=payload).status_code == 404
    with TestClient(app) as client:
        assert client.post(path, content=b"x" * 2048).status_code == 401
        response = client.post(path, headers=headers, json=payload)
        assert (
            response.status_code == 200
            and response.headers["cache-control"] == "no-store"
        )
        assert request.claim.proof.reveal() not in response.text
        assert len(calls) == 1 and calls[0].intent == request.intent
        for key in (
            "generation",
            "protocol_version",
            "expected_workflow_version",
            "definition_digest_version",
        ):
            invalid = {**payload, "intent": {**body, key: True}}
            denied = client.post(path, headers=headers, json=invalid)
            assert (
                denied.status_code == 422
                and request.claim.proof.reveal() not in denied.text
            )
        assert (
            client.post(
                path,
                headers=headers,
                json={**payload, "intent": {**body, "attempt_id": str(uuid4())}},
            ).status_code
            == 422
        )
        assert (
            client.post(
                path,
                headers={**headers, "Content-Type": "application/json"},
                content=b"x" * 2048,
            ).status_code
            == 413
        )
        error = AcknowledgementRejected(request.claim.proof.reveal())
        denied = client.post(path, headers=headers, json=payload)
        assert (
            denied.status_code == 409
            and request.claim.proof.reveal() not in denied.text
        )
        raw = json.dumps(payload)
        for content in (
            b"\xff\xfe{\x00}\x00",
            b'{"proof":"duplicate",' + raw.encode()[1:],
        ):
            assert (
                client.post(
                    path,
                    headers={**headers, "Content-Type": "application/json"},
                    content=content,
                ).status_code
                == 422
            )
    assert app.state.worker_claim_services is None


def test_acknowledgement_admission_recovers_after_cancellation_and_body_deadline(
    worker_configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request, _, result = exchange()
    values = worker_configuration.model_dump()
    values["worker_claims"] = {
        "max_concurrent_requests": 1,
        "body_timeout_seconds": 0.1,
    }
    configuration = ControlSettings.model_validate(values)
    started, release = asyncio.Event(), asyncio.Event()

    async def claim(
        request: WorkerClaimRequest, authority: WorkerAuthorization
    ) -> ClaimResult:
        raise AssertionError("Unexpected claim.")

    async def read(
        attempt: UUID, request: WorkerClaimRequest, authority: WorkerAuthorization
    ) -> AssignmentDelivery:
        raise AssertionError("Unexpected payload read.")

    async def acknowledge(
        request: WorkerAcknowledgementRequest, authority: WorkerAuthorization
    ) -> AcknowledgementResult:
        started.set()
        await release.wait()
        return result

    @asynccontextmanager
    async def resources() -> AsyncIterator[WorkerClaimOperations]:
        yield WorkerClaimOperations(
            claim=claim, read_assignment=read, acknowledge=acknowledge
        )

    monkeypatch.setattr(
        control,
        "worker_authenticator",
        lambda registrations: worker_authenticator(registrations, now=lambda: NOW),
    )
    app = create_app(configuration, claim_resources=resources)
    path = f"/api/v1/worker/attempts/{request.intent.attempt_id}/acknowledgements"
    payload = {
        "intent": json.loads(
            WorkerAcknowledgementResponse(result=result).model_dump_json()
        )["result"]["intent"],
        "proof": request.claim.proof.reveal(),
    }

    class SlowBody(httpx.AsyncByteStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            await asyncio.sleep(0.2)
            yield json.dumps(payload).encode()

    async def run() -> None:
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                base_url="https://control.example/",
                transport=httpx.ASGITransport(app),
                headers={
                    "Authorization": "Bearer "
                    + configuration.workers[0].auth_token.get_secret_value()
                },
            ) as client:
                pending = asyncio.create_task(client.post(path, json=payload))
                await started.wait()
                busy = await client.post(path, json=payload)
                assert busy.status_code == 503 and busy.headers["retry-after"] == "1"
                pending.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await pending
                release.set()
                accepted = await client.post(path, json=payload)
                assert accepted.status_code == 200
                timed_out = await client.post(
                    path,
                    content=SlowBody(),
                    headers={"Content-Type": "application/json"},
                )
                assert timed_out.status_code == 408
                recovered = await client.post(path, json=payload)
                assert recovered.status_code == 200

    asyncio.run(run())
