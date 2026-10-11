import asyncio
import gzip
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import uuid4

import httpx
import pytest

from devrelay.application.contracts.claims import LeaseProof, WorkerClaimRequest
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.control_gateway import (
    ControlAccessDenied,
    ControlClaimRejected,
    ControlUnavailable,
    InvalidControlResponse,
)
from devrelay.infrastructure.control.gateway import HttpControlGateway

NOW = datetime(2026, 10, 11, tzinfo=UTC)
TOKEN = "test_only_worker_token_0123456789abcdefghijklmnop"


@pytest.fixture
def authorization() -> WorkerAuthorization:
    return WorkerAuthorization(
        worker_id=uuid4(),
        allowed_repository_ids=frozenset({uuid4()}),
        credential_expires_at=NOW + timedelta(hours=1),
    )


@pytest.fixture
def pending() -> WorkerClaimRequest:
    return WorkerClaimRequest(claim_request_id=uuid4(), proof=LeaseProof.generate())


def response_body(
    authorization: WorkerAuthorization,
    pending: WorkerClaimRequest,
    *,
    assigned: bool = False,
) -> dict[str, object]:
    reference = (
        {
            "worker_id": str(authorization.worker_id),
            "job_id": str(uuid4()),
            "repository_id": str(next(iter(authorization.allowed_repository_ids))),
            "attempt_id": str(uuid4()),
            "generation": 1,
            "issued_at": NOW.isoformat(),
            "expires_at": (NOW + timedelta(seconds=120)).isoformat(),
        }
        if assigned
        else None
    )
    return {
        "protocol_version": 1,
        "worker_id": str(authorization.worker_id),
        "claim_request_id": str(pending.claim_request_id),
        "recorded_at": NOW.isoformat(),
        "assignment": reference,
        "no_work": None if assigned else "empty",
        "recovered": False,
        "lease_authority_active": assigned,
    }


@pytest.mark.parametrize("assigned", [True, False])
def test_claim_exchange_is_correlated_scoped_and_sends_only_original_input(
    authorization: WorkerAuthorization,
    pending: WorkerClaimRequest,
    assigned: bool,
) -> None:
    requests: list[httpx.Request] = []
    body = response_body(authorization, pending, assigned=assigned)

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=body)

    async def scenario() -> None:
        async with httpx.AsyncClient(
            base_url="https://control.example/prefix/",
            headers={"Authorization": f"Bearer {TOKEN}"},
            transport=httpx.MockTransport(respond),
        ) as client:
            result = await HttpControlGateway(client, now=lambda: NOW).claim(
                pending, authorization
            )
            assert result.outcome.claim_request_id == pending.claim_request_id
            assert result.outcome.worker_id == authorization.worker_id
            assert (result.outcome.assignment is not None) is assigned
            assert result.lease_authority_active is assigned
            assert pending.proof.reveal() not in repr(result)

    asyncio.run(scenario())

    assert len(requests) == 1
    request = requests[0]
    assert str(request.url) == "https://control.example/prefix/api/v1/worker/claims"
    assert request.method == "POST"
    assert request.headers["authorization"] == f"Bearer {TOKEN}"
    assert request.headers["accept-encoding"] == "identity"
    assert json.loads(request.content) == {
        "claim_request_id": str(pending.claim_request_id),
        "protocol_version": 1,
        "proof": pending.proof.reveal(),
    }


@pytest.mark.parametrize(
    "change",
    [
        "worker",
        "request",
        "protocol",
        "bool_version",
        "boolean",
        "extra",
        "missing",
        "naive_time",
        "reason",
        "active_empty",
        "repository",
        "generation",
        "lease",
        "mixed",
        "nested_extra",
        "nested_worker",
        "issuance",
    ],
)
def test_untrusted_claim_shape_cannot_become_a_journal_outcome(
    authorization: WorkerAuthorization,
    pending: WorkerClaimRequest,
    change: str,
) -> None:
    assigned = change in {
        "repository",
        "generation",
        "lease",
        "mixed",
        "nested_extra",
        "nested_worker",
        "issuance",
    }
    body = response_body(authorization, pending, assigned=assigned)
    if change == "worker":
        body["worker_id"] = str(uuid4())
    elif change == "request":
        body["claim_request_id"] = str(uuid4())
    elif change in ("protocol", "bool_version"):
        body["protocol_version"] = True if change == "bool_version" else 2
    elif change == "boolean":
        body["recovered"] = 1
    elif change == "extra":
        body["proof"] = pending.proof.reveal()
    elif change == "missing":
        del body["lease_authority_active"]
    elif change == "naive_time":
        body["recorded_at"] = "2026-10-11T00:00:00"
    elif change == "reason":
        body["no_work"] = "unknown"
    elif change == "active_empty":
        body["lease_authority_active"] = True
    else:
        reference = cast(dict[str, object], body["assignment"])
        if change == "repository":
            reference["repository_id"] = str(uuid4())
        elif change == "generation":
            reference["generation"] = True
        elif change == "lease":
            reference["expires_at"] = (NOW + timedelta(seconds=301)).isoformat()
        elif change == "mixed":
            body["no_work"] = "empty"
        elif change == "nested_extra":
            reference["launch"] = True
        elif change == "nested_worker":
            reference["worker_id"] = str(uuid4())
        else:
            reference["issued_at"] = (NOW + timedelta(seconds=1)).isoformat()

    async def scenario() -> None:
        async with httpx.AsyncClient(
            base_url="https://control.example",
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, json=body)
            ),
        ) as client:
            with pytest.raises(InvalidControlResponse) as error:
                await HttpControlGateway(client, now=lambda: NOW).claim(
                    pending, authorization
                )
            assert pending.proof.reveal() not in str(error.value)

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "status,error_type",
    [
        (401, ControlAccessDenied),
        (403, ControlAccessDenied),
        (409, ControlClaimRejected),
        (422, ControlClaimRejected),
        (408, ControlUnavailable),
        (429, ControlUnavailable),
        (503, ControlUnavailable),
        (302, InvalidControlResponse),
        (404, InvalidControlResponse),
        (500, InvalidControlResponse),
    ],
)
def test_failure_bodies_and_redirects_are_never_used(
    authorization: WorkerAuthorization,
    pending: WorkerClaimRequest,
    status: int,
    error_type: type[Exception],
) -> None:
    calls: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(
            status,
            headers={"Location": "https://attacker.example"},
            text=TOKEN + pending.proof.reveal(),
        )

    async def scenario() -> None:
        async with httpx.AsyncClient(
            base_url="https://control.example",
            transport=httpx.MockTransport(respond),
            follow_redirects=True,
        ) as client:
            with pytest.raises(error_type) as error:
                await HttpControlGateway(client, now=lambda: NOW).claim(
                    pending, authorization
                )
            assert TOKEN not in str(error.value) and pending.proof.reveal() not in str(
                error.value
            )

    asyncio.run(scenario())

    assert len(calls) == 1 and calls[0].url.host == "control.example"


class Chunks(httpx.AsyncByteStream):
    def __init__(self, *, slow: bool = False) -> None:
        self.slow = slow
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b"{"
        if self.slow:
            await asyncio.Event().wait()
        else:
            yield b"x" * 16_384

    async def aclose(self) -> None:
        self.closed = True


@pytest.mark.parametrize("slow", [True, False])
def test_stream_is_bounded_and_closed_after_timeout_or_size_failure(
    authorization: WorkerAuthorization,
    pending: WorkerClaimRequest,
    slow: bool,
) -> None:
    stream = Chunks(slow=slow)

    async def scenario() -> None:
        async with httpx.AsyncClient(
            base_url="https://control.example",
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    200, headers={"Content-Type": "application/json"}, stream=stream
                )
            ),
        ) as client:
            with pytest.raises(ControlUnavailable if slow else InvalidControlResponse):
                await HttpControlGateway(
                    client, now=lambda: NOW, timeout_seconds=0.01
                ).claim(pending, authorization)

    asyncio.run(scenario())

    assert stream.closed


@pytest.mark.parametrize("case", ["gzip", "html", "declared", "duplicate", "json"])
def test_response_encoding_media_declared_size_and_json_are_checked(
    authorization: WorkerAuthorization,
    pending: WorkerClaimRequest,
    case: str,
) -> None:
    headers = {"Content-Type": "application/json"}
    body = json.dumps(response_body(authorization, pending)).encode()
    if case == "gzip":
        headers["Content-Encoding"] = "gzip"
        body = gzip.compress(body)
    elif case == "html":
        headers["Content-Type"] = "text/html"
    elif case == "declared":
        headers["Content-Length"] = "16385"
    elif case == "duplicate":
        body = body[:-1] + b', "protocol_version": 1}'
    else:
        body = b"invalid-json"

    async def scenario() -> None:
        async with httpx.AsyncClient(
            base_url="https://control.example",
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, headers=headers, content=body)
            ),
        ) as client:
            with pytest.raises(InvalidControlResponse):
                await HttpControlGateway(client, now=lambda: NOW).claim(
                    pending, authorization
                )

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "change", ["none", "expired", "duplicates", "version", "naive", "extra"]
)
def test_identity_validates_authority_before_journal_composition(
    authorization: WorkerAuthorization,
    change: str,
) -> None:
    body: dict[str, object] = {
        "protocol_version": 1,
        "worker_id": str(authorization.worker_id),
        "allowed_repository_ids": [
            str(value) for value in authorization.allowed_repository_ids
        ],
        "credential_expires_at": authorization.credential_expires_at.isoformat(),
    }
    if change == "expired":
        body["credential_expires_at"] = NOW.isoformat()
    elif change == "duplicates":
        body["allowed_repository_ids"] = [
            str(next(iter(authorization.allowed_repository_ids)))
        ] * 2
    elif change == "version":
        body["protocol_version"] = True
    elif change == "naive":
        body["credential_expires_at"] = "2026-10-11T01:00:00"
    elif change == "extra":
        body["token"] = TOKEN

    async def scenario() -> None:
        async with httpx.AsyncClient(
            base_url="https://control.example",
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, json=body)
            ),
        ) as client:
            gateway = HttpControlGateway(client, now=lambda: NOW)
            if change == "none":
                assert await gateway.identify() == authorization
            else:
                with pytest.raises(
                    ControlAccessDenied
                    if change == "expired"
                    else InvalidControlResponse
                ):
                    await gateway.identify()

    asyncio.run(scenario())
