import asyncio
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from pydantic import ValidationError

from devrelay.api.dependencies import (
    ClaimOperation,
    CodingResultOperations,
    WorkerCodingResultServices,
)
from devrelay.application.coding_result_preparation import prepare_coding_result
from devrelay.application.contracts.assignments import ClaimResult, NoWorkReason
from devrelay.application.contracts.claims import WorkerClaimRequest
from devrelay.application.contracts.coding_result_receipts import (
    CodingResultAcceptance,
    CodingResultStatus,
    CodingResultUnavailable,
)
from devrelay.application.contracts.coding_result_requests import (
    MAX_CODING_RESULT_REQUEST_BYTES,
    WorkerCodingResultRequest,
)
from devrelay.application.contracts.coding_result_wire import (
    encode_coding_result_request,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.bootstrap import control
from devrelay.bootstrap.control import create_app
from devrelay.bootstrap.security import worker_authenticator
from devrelay.config.control import ControlSettings
from devrelay.config.worker_coding_results import WorkerCodingResultsSettings
from tests.assignment_fixtures import NOW
from tests.unit.test_coding_result_delivery import Store
from tests.unit.test_coding_result_preparation import context

TOKEN = "test_only_worker_token_0123456789abcdefghijklmnop"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}


class Operations:
    def __init__(self) -> None:
        self.setup = context()
        self.store = Store()
        self.calls: list[str] = []
        self.error: Exception | None = None

    @property
    def path(self) -> str:
        attempt = self.setup.request.intent.execution.acknowledgement.attempt_id
        return f"/api/v1/worker/attempts/{attempt}/results"

    async def accept(
        self, request: WorkerCodingResultRequest, authorization: WorkerAuthorization
    ) -> CodingResultAcceptance:
        self.calls.append("accept")
        if self.error is not None:
            raise self.error
        prepared = await asyncio.to_thread(
            prepare_coding_result, request, authorization, self.setup.definition
        )
        if self.store.status is not None:
            return CodingResultAcceptance(
                receipt=self.store.status.receipt, recovered=True
            )
        return await self.store.accept(prepared, authorization)

    async def recover(
        self, request: WorkerCodingResultRequest, authorization: WorkerAuthorization
    ) -> CodingResultStatus | None:
        self.calls.append("recover")
        return self.store.status


def configuration(original: ControlSettings) -> ControlSettings:
    values = original.model_dump()
    values["worker_claims"] = {}
    values["worker_coding_results"] = {
        "body_timeout_seconds": 0.1,
        "request_timeout_seconds": 1,
    }
    return ControlSettings.model_validate(values)


def application(
    original: ControlSettings, operation: Operations, monkeypatch: pytest.MonkeyPatch
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
    async def results() -> AsyncIterator[CodingResultOperations]:
        yield CodingResultOperations(accept=operation.accept, recover=operation.recover)

    return create_app(
        configuration(original), claim_resources=claims, coding_result_resources=results
    )


def test_result_gate_disabled_and_cannot_be_enabled_without_claims(
    worker_configuration: ControlSettings,
) -> None:
    operation = Operations()
    with TestClient(create_app(worker_configuration)) as client:
        assert (
            client.post(operation.path, headers=HEADERS, content=b"{}").status_code
            == 404
        )
    values = worker_configuration.model_dump()
    values["worker_coding_results"] = {}
    with pytest.raises(ValidationError):
        ControlSettings.model_validate(values)


@pytest.mark.parametrize(
    "value",
    [
        {"max_concurrent_requests": 2},
        {"max_concurrent_requests": True},
        {"body_timeout_seconds": float("nan")},
        {"request_timeout_seconds": float("inf")},
        {"body_timeout_seconds": 5, "request_timeout_seconds": 1},
        {"unknown": True},
    ],
)
def test_result_limits_are_finite_and_separately_admitted(
    value: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        WorkerCodingResultsSettings.model_validate(value)


def test_acceptance_replay_and_recovery_return_only_bounded_nonsecret_metadata(
    worker_configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation = Operations()
    body = encode_coding_result_request(operation.setup.request)
    with TestClient(
        application(worker_configuration, operation, monkeypatch)
    ) as client:
        missing = client.post(
            operation.path + "/recovery", headers=HEADERS, content=body
        )
        accepted = client.post(operation.path, headers=HEADERS, content=body)
        replay = client.post(operation.path, headers=HEADERS, content=body)
        recovery = client.post(
            operation.path + "/recovery", headers=HEADERS, content=body
        )
    assert missing.status_code == 404
    assert accepted.status_code == replay.status_code == 202
    assert recovery.status_code == 200
    assert not accepted.json()["recovered"] and replay.json()["recovered"]
    assert (
        accepted.json()["receipt"]
        == replay.json()["receipt"]
        == recovery.json()["receipt"]
    )
    assert (
        recovery.json()["state"] == "accepted" and recovery.json()["terminal"] is None
    )
    for response in (accepted, replay, recovery):
        assert response.headers["Cache-Control"] == "no-store"
        assert "payload_bytes" not in response.text and "files" not in response.text
        assert (
            TOKEN not in response.text
            and operation.setup.request.claim.proof.reveal() not in response.text
        )


def test_authentication_and_route_pin_precede_body_use(
    worker_configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation = Operations()
    with TestClient(
        application(worker_configuration, operation, monkeypatch)
    ) as client:
        assert client.post(operation.path, content=b"not json").status_code == 401
        wrong = operation.path.replace(
            str(operation.setup.request.intent.execution.acknowledgement.attempt_id),
            str(uuid4()),
        )
        assert (
            client.post(
                wrong,
                headers=HEADERS,
                content=encode_coding_result_request(operation.setup.request),
            ).status_code
            == 422
        )
        assert (
            client.post(
                operation.path.replace("/attempts/", "/attempts/not-a-uuid/"),
                headers=HEADERS,
                content=b"{}",
            ).status_code
            == 404
        )
    assert not operation.calls


@pytest.mark.parametrize(
    "body", [b"{}", b"{", b'{"protocol_version":1,"protocol_version":1}', b"[]"]
)
def test_malformed_bodies_are_rejected_before_application(
    worker_configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
    body: bytes,
) -> None:
    operation = Operations()
    with TestClient(
        application(worker_configuration, operation, monkeypatch)
    ) as client:
        response = client.post(operation.path, headers=HEADERS, content=body)
    assert response.status_code == 422 and not operation.calls
    assert TOKEN not in response.text


def test_media_size_and_internal_errors_are_bounded_and_redacted(
    worker_configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation = Operations()
    with TestClient(
        application(worker_configuration, operation, monkeypatch),
        raise_server_exceptions=False,
    ) as client:
        media = client.post(
            operation.path,
            headers={**HEADERS, "Content-Encoding": "gzip"},
            content=b"{}",
        )
        oversized = client.post(
            operation.path,
            headers={
                **HEADERS,
                "Content-Length": str(MAX_CODING_RESULT_REQUEST_BYTES + 1),
            },
            content=b"{}",
        )
        operation.error = CodingResultUnavailable("SECRET provider/source exception")
        failed = client.post(
            operation.path,
            headers=HEADERS,
            content=encode_coding_result_request(operation.setup.request),
        )
    assert media.status_code == 415 and oversized.status_code == 413
    assert failed.status_code == 503
    assert "SECRET" not in failed.text


def test_cancelled_decode_retains_admission_and_shutdown_until_real_future_finishes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import devrelay.application.contracts.coding_result_wire as wire

    original = wire.decode_coding_result_request
    entered, release = threading.Event(), threading.Event()
    operation = Operations()

    def blocked(body: bytes, *, attempt_id: UUID) -> WorkerCodingResultRequest:
        entered.set()
        assert release.wait(5)
        return original(body, attempt_id=attempt_id)

    monkeypatch.setattr(wire, "decode_coding_result_request", blocked)

    async def scenario() -> None:
        services = WorkerCodingResultServices(
            accept_result=operation.accept,
            recover_result=operation.recover,
            max_concurrent_requests=1,
            body_timeout_seconds=0.1,
            request_timeout_seconds=1,
        )
        task = asyncio.create_task(
            services.decode(
                encode_coding_result_request(operation.setup.request),
                attempt_id=operation.setup.request.intent.execution.acknowledgement.attempt_id,
            )
        )
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            with pytest.raises(HTTPException):
                with services.admit_request():
                    pytest.fail("A canceled decoder cannot free admission early.")
            closing = asyncio.create_task(services.aclose())
            await asyncio.sleep(0)
            assert not closing.done()
            release.set()
            await closing
            with pytest.raises(HTTPException):
                with services.admit_request():
                    pytest.fail("Closed service cannot be reused.")
            assert not operation.calls
        finally:
            release.set()

    asyncio.run(scenario())
