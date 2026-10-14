import asyncio
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from devrelay.api.dependencies import BaseSourceOperations, ClaimOperation
from devrelay.api.schemas.base_sources import BaseSourceDeliveryResponse
from devrelay.application.contracts.assignment_delivery import (
    AssignmentDelivery,
    AssignmentUnavailable,
)
from devrelay.application.contracts.assignments import ClaimResult, NoWorkReason
from devrelay.application.contracts.base_source_delivery import (
    BaseSourceDelivery,
    decode_base_delivery,
)
from devrelay.application.contracts.claims import LeaseProof, WorkerClaimRequest
from devrelay.application.contracts.heartbeats import (
    HeartbeatRequest,
    HeartbeatResult,
    LeaseAuthorityObservation,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.base_source import (
    BaseSourceAccessDenied,
    BaseSourceUnavailable,
    InvalidBaseSourceData,
)
from devrelay.bootstrap import control
from devrelay.bootstrap.control import create_app
from devrelay.bootstrap.security import worker_authenticator
from devrelay.config.control import ControlSettings
from tests.assignment_fixtures import NOW, assigned_result
from tests.base_source_fixtures import source_fixture
from tests.workspace_fixtures import workspace_inputs

TOKEN = "test_only_worker_token_0123456789abcdefghijklmnop"
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
ATTEMPT = uuid4()
PATH = f"/api/v1/worker/attempts/{ATTEMPT}/base-source"


class Operations:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.error: BaseException | None = None
        self.verify_error: BaseException | None = None
        self.started = asyncio.Event()
        self.release: asyncio.Event | None = None

    async def read(
        self,
        attempt_id: UUID,
        request: WorkerClaimRequest,
        authorization: WorkerAuthorization,
    ) -> BaseSourceDelivery:
        self.calls.append("read")
        self.started.set()
        if self.release is not None:
            await self.release.wait()
        if self.error is not None:
            raise self.error
        delivery = AssignmentDelivery.from_result(
            assigned_result(authorization),
            worker_id=authorization.worker_id,
            claim_request_id=request.claim_request_id,
        )
        reference = delivery.outcome.assignment
        assert reference is not None
        binding = workspace_inputs()[2]
        fixture = source_fixture(binding)
        source = fixture.snapshot.request
        # Fixture source is immutable but receives the authenticated repository pin.
        from dataclasses import replace

        snapshot = replace(
            fixture.snapshot,
            request=replace(source, repository_id=reference.repository_id),
        )
        return BaseSourceDelivery(
            reference=reference,
            claim_request_id=request.claim_request_id,
            definition_digest=delivery.definition.fingerprint(),
            snapshot=snapshot,
        )

    async def verify(
        self,
        attempt_id: UUID,
        request: WorkerClaimRequest,
        authorization: WorkerAuthorization,
        expected: BaseSourceDelivery,
    ) -> None:
        self.calls.append("verify")
        if self.verify_error is not None:
            raise self.verify_error


def payload() -> dict[str, object]:
    return {
        "claim_request_id": str(uuid4()),
        "proof": LeaseProof.generate().reveal(),
        "protocol_version": 1,
    }


@pytest.fixture
def configuration(worker_configuration: ControlSettings) -> ControlSettings:
    values = worker_configuration.model_dump()
    values["worker_claims"] = {"max_concurrent_requests": 1}
    values["worker_base_sources"] = {
        "body_timeout_seconds": 0.1,
        "request_timeout_seconds": 1,
    }
    return ControlSettings.model_validate(values)


def application(
    configuration: ControlSettings,
    operation: Operations,
    monkeypatch: pytest.MonkeyPatch,
) -> FastAPI:
    monkeypatch.setattr(
        control,
        "worker_authenticator",
        lambda values: worker_authenticator(values, now=lambda: NOW),
    )

    async def claim(
        request: WorkerClaimRequest, authorization: WorkerAuthorization
    ) -> ClaimResult:
        return ClaimResult(recorded_at=NOW, recovered=True, no_work=NoWorkReason.EMPTY)

    @asynccontextmanager
    async def claims() -> AsyncIterator[ClaimOperation]:
        yield claim

    @asynccontextmanager
    async def sources() -> AsyncIterator[BaseSourceOperations]:
        yield BaseSourceOperations(
            read_base=operation.read, verify_base=operation.verify
        )

    return create_app(
        configuration, claim_resources=claims, base_source_resources=sources
    )


def test_route_is_disabled_without_source_gate(
    worker_configuration: ControlSettings,
) -> None:
    app = create_app(worker_configuration)
    with TestClient(app) as client:
        response = client.post(PATH, headers=HEADERS, json=payload())
    assert response.status_code == 404
    assert (
        "/api/v1/worker/attempts/{attempt_id}/base-source" not in app.openapi()["paths"]
    )


def test_success_rechecks_after_encode_and_excludes_credentials(
    configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    operation = Operations()
    app = application(configuration, operation, monkeypatch)
    body = payload()
    with TestClient(app) as client:
        response = client.post(PATH, headers=HEADERS, json=body)
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    delivery = decode_base_delivery(response.content)
    assert delivery.claim_request_id == UUID(str(body["claim_request_id"]))
    assert delivery.reference.worker_id == configuration.workers[0].worker_id
    assert operation.calls == ["read", "verify"]
    assert TOKEN not in response.text + caplog.text
    assert str(body["proof"]) not in response.text + caplog.text
    assert app.state.worker_base_source_services is None


@pytest.mark.parametrize(
    "headers", [{}, {"Authorization": "Bearer wrong"}, {"Cookie": f"token={TOKEN}"}]
)
def test_authentication_precedes_invalid_body(
    configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
    headers: dict[str, str],
) -> None:
    operation = Operations()
    app = application(configuration, operation, monkeypatch)
    with TestClient(app) as client:
        response = client.post(PATH, headers=headers, content=b"x" * 2000)
    assert response.status_code == 401
    assert operation.calls == []


@pytest.mark.parametrize("change", ["extra", "proof", "protocol", "boolean", "missing"])
def test_request_is_original_claim_only(
    configuration: ControlSettings, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    operation = Operations()
    app = application(configuration, operation, monkeypatch)
    body = payload()
    if change == "extra":
        body["repository"] = "arbitrary/repository"
    elif change == "proof":
        body["proof"] = "invalid"
    elif change == "protocol":
        body["protocol_version"] = 2
    elif change == "boolean":
        body["protocol_version"] = True
    else:
        del body["claim_request_id"]
    with TestClient(app) as client:
        response = client.post(PATH, headers=HEADERS, json=body)
    assert response.status_code == 422
    assert operation.calls == []


@pytest.mark.parametrize(
    "error,status,code",
    [
        (BaseSourceUnavailable("private token"), 503, "base_source_unavailable"),
        (BaseSourceAccessDenied("private token"), 502, "base_source_access_denied"),
        (InvalidBaseSourceData("private token"), 502, "invalid_base_source_data"),
        (AssignmentUnavailable("private token"), 404, "assignment_unavailable"),
        (RuntimeError("private token"), 500, "internal_error"),
    ],
)
def test_failures_are_sanitized_and_admission_released(
    configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
    status: int,
    code: str,
) -> None:
    operation = Operations()
    operation.error = error
    app = application(configuration, operation, monkeypatch)
    with TestClient(app) as client:
        response = client.post(PATH, headers=HEADERS, json=payload())
        operation.error = None
        retry = client.post(PATH, headers=HEADERS, json=payload())
    assert response.status_code == status
    assert response.json()["detail"]["code"] == code
    assert "private token" not in response.text
    assert retry.status_code == 200


def test_current_authority_failure_prevents_encoded_disclosure(
    configuration: ControlSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    operation = Operations()
    operation.verify_error = AssignmentUnavailable("expired during serialization")
    app = application(configuration, operation, monkeypatch)
    with TestClient(app) as client:
        response = client.post(PATH, headers=HEADERS, json=payload())
    assert response.status_code == 404
    assert operation.calls == ["read", "verify"]
    assert "Fixture source" not in response.text


def test_serialization_is_off_loop_before_final_authority_read(
    configuration: ControlSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    operation = Operations()
    app = application(configuration, operation, monkeypatch)
    original = BaseSourceDeliveryResponse.encode

    def encode(delivery: BaseSourceDelivery) -> bytes:
        with pytest.raises(RuntimeError, match="no running event loop"):
            asyncio.get_running_loop()
        assert operation.calls == ["read"]
        operation.calls.append("encode")
        return original(delivery)

    monkeypatch.setattr(BaseSourceDeliveryResponse, "encode", encode)

    with TestClient(app) as client:
        response = client.post(PATH, headers=HEADERS, json=payload())

    assert response.status_code == 200
    assert operation.calls == ["read", "encode", "verify"]


@pytest.mark.parametrize(
    "content,content_type,status",
    [
        (b"x" * 1025, "application/json", 413),
        (b"{}", "text/plain", 415),
        (b'{"proof":"x","proof":"y"}', "application/json", 422),
    ],
)
def test_body_limits_and_unique_keys_precede_source_read(
    configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
    content: bytes,
    content_type: str,
    status: int,
) -> None:
    operation = Operations()
    app = application(configuration, operation, monkeypatch)
    with TestClient(app) as client:
        response = client.post(
            PATH, headers={**HEADERS, "Content-Type": content_type}, content=content
        )
    assert response.status_code == status
    assert operation.calls == []


def test_cancelled_source_request_releases_own_admission(
    configuration: ControlSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    operation = Operations()
    app = application(configuration, operation, monkeypatch)

    async def scenario() -> int:
        operation.release = asyncio.Event()
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="https://control.test"
            ) as client:
                pending = asyncio.create_task(
                    client.post(PATH, headers=HEADERS, json=payload())
                )
                await operation.started.wait()
                pending.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await pending
                operation.release = None
                retry = await client.post(PATH, headers=HEADERS, json=payload())
                return retry.status_code

    assert asyncio.run(scenario()) == 200


@pytest.mark.parametrize("late_error", [False, True])
def test_cancelled_encoder_retains_source_admission_until_actual_settlement(
    configuration: ControlSettings, monkeypatch: pytest.MonkeyPatch, late_error: bool
) -> None:
    operation = Operations()
    app = application(configuration, operation, monkeypatch)
    original = BaseSourceDeliveryResponse.encode
    encoder_started = threading.Event()
    encoder_release = threading.Event()

    def encode(delivery: BaseSourceDelivery) -> bytes:
        encoder_started.set()
        assert encoder_release.wait(5)
        if late_error:
            raise InvalidBaseSourceData("fixture late encoder failure")
        return original(delivery)

    monkeypatch.setattr(BaseSourceDeliveryResponse, "encode", encode)

    async def scenario() -> tuple[int, int, int]:
        async with app.router.lifespan_context(app):
            authorization = WorkerAuthorization(
                worker_id=configuration.workers[0].worker_id,
                allowed_repository_ids=configuration.workers[0].allowed_repository_ids,
                credential_expires_at=NOW + timedelta(hours=1),
            )
            delivery = AssignmentDelivery.from_result(
                assigned_result(authorization),
                worker_id=authorization.worker_id,
                claim_request_id=uuid4(),
            )
            reference = delivery.outcome.assignment
            assert reference is not None

            async def heartbeat(
                request: HeartbeatRequest, authorization: WorkerAuthorization
            ) -> HeartbeatResult:
                return HeartbeatResult(
                    heartbeat_request_id=request.heartbeat_request_id,
                    authority=LeaseAuthorityObservation(
                        reference=reference,
                        lease_revision=1,
                        observed_at=NOW,
                        updated_at=NOW,
                        current_expires_at=reference.expires_at,
                    ),
                    renewed=False,
                    stop_required=False,
                )

            app.state.worker_claim_services.heartbeat = heartbeat
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="https://control.test"
            ) as client:
                pending = asyncio.create_task(
                    client.post(PATH, headers=HEADERS, json=payload())
                )
                try:
                    assert await asyncio.to_thread(encoder_started.wait, 5)
                    pending.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await pending
                    busy = await client.post(PATH, headers=HEADERS, json=payload())
                    heartbeat_body = {
                        **payload(),
                        "heartbeat_request_id": str(uuid4()),
                        "generation": 1,
                        "expected_lease_revision": 1,
                    }
                    heartbeat_response = await client.post(
                        f"/api/v1/worker/attempts/{reference.attempt_id}/heartbeats",
                        headers=HEADERS,
                        json=heartbeat_body,
                    )
                    retained = app.state.worker_base_source_services._pending_encode
                    assert retained is not None and not retained.done()
                    encoder_release.set()
                    if late_error:
                        await asyncio.gather(retained, return_exceptions=True)
                    else:
                        await asyncio.shield(retained)
                    monkeypatch.setattr(BaseSourceDeliveryResponse, "encode", original)
                    retry = await client.post(PATH, headers=HEADERS, json=payload())
                    return (
                        busy.status_code,
                        heartbeat_response.status_code,
                        retry.status_code,
                    )
                finally:
                    encoder_release.set()

    assert asyncio.run(scenario()) == (503, 200, 200)


def test_source_admission_does_not_occupy_claim_admission(
    configuration: ControlSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    operation = Operations()
    app = application(configuration, operation, monkeypatch)

    async def scenario() -> tuple[int, int, int]:
        operation.release = asyncio.Event()
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="https://control.test"
            ) as client:
                first = asyncio.create_task(
                    client.post(PATH, headers=HEADERS, json=payload())
                )
                await operation.started.wait()
                busy = await client.post(PATH, headers=HEADERS, json=payload())
                claim = await client.post(
                    "/api/v1/worker/claims", headers=HEADERS, json=payload()
                )
                operation.release.set()
                response = await first
                return busy.status_code, claim.status_code, response.status_code

    assert asyncio.run(scenario()) == (503, 200, 200)


def test_route_deadline_releases_source_admission(
    configuration: ControlSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    operation = Operations()
    operation.release = asyncio.Event()
    app = application(configuration, operation, monkeypatch)
    with TestClient(app) as client:
        response = client.post(PATH, headers=HEADERS, json=payload())
        operation.release = None
        retry = client.post(PATH, headers=HEADERS, json=payload())
    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "base_source_unavailable"
    assert retry.status_code == 200
