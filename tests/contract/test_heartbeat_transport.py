import asyncio
import gzip
import json
from collections.abc import AsyncIterator, Callable
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import UUID, uuid4

import httpx
import pytest

from devrelay.application.contracts.claims import LeaseProof, WorkerClaimRequest
from devrelay.application.contracts.heartbeats import (
    HeartbeatRequest,
    HeartbeatResult,
    LeaseAuthorityObservation,
)
from devrelay.application.contracts.worker_recovery import AssignmentReference
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.control_gateway import (
    ControlAccessDenied,
    ControlClaimRejected,
    ControlUnavailable,
    InvalidControlResponse,
)
from devrelay.infrastructure.control.gateway import HttpControlGateway

NOW = datetime(2026, 10, 12, tzinfo=UTC)
TOKEN = "test_only_worker_token_0123456789abcdefghijklmnop"


@pytest.fixture
def authorization() -> WorkerAuthorization:
    return WorkerAuthorization(
        worker_id=uuid4(),
        allowed_repository_ids=frozenset({uuid4()}),
        credential_expires_at=NOW + timedelta(hours=1),
    )


@pytest.fixture
def reference(authorization: WorkerAuthorization) -> AssignmentReference:
    return AssignmentReference(
        worker_id=authorization.worker_id,
        job_id=uuid4(),
        repository_id=next(iter(authorization.allowed_repository_ids)),
        attempt_id=uuid4(),
        generation=1,
        issued_at=NOW - timedelta(seconds=10),
        expires_at=NOW + timedelta(seconds=110),
    )


@pytest.fixture
def pending(reference: AssignmentReference) -> HeartbeatRequest:
    return HeartbeatRequest(
        claim=WorkerClaimRequest(claim_request_id=uuid4(), proof=LeaseProof.generate()),
        heartbeat_request_id=uuid4(),
        attempt_id=reference.attempt_id,
        generation=1,
        expected_lease_revision=1,
    )


def wire_value(value: object) -> str:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError("Unsupported fixture wire value.")


def response_body(
    pending: HeartbeatRequest, reference: AssignmentReference
) -> dict[str, Any]:
    result = HeartbeatResult(
        heartbeat_request_id=pending.heartbeat_request_id,
        authority=LeaseAuthorityObservation(
            reference=reference,
            lease_revision=2,
            observed_at=NOW,
            updated_at=NOW,
            current_expires_at=NOW + timedelta(seconds=120),
        ),
        renewed=True,
        stop_required=False,
    )
    return cast(
        dict[str, Any],
        json.loads(
            json.dumps(
                {"protocol_version": 1, "result": asdict(result)}, default=wire_value
            )
        ),
    )


async def exchange(
    pending: HeartbeatRequest,
    authorization: WorkerAuthorization,
    reference: AssignmentReference,
    respond: Callable[[httpx.Request], httpx.Response],
    *,
    now: Callable[[], datetime] = lambda: NOW,
    timeout_seconds: float = 60,
) -> HeartbeatResult:
    async with httpx.AsyncClient(
        base_url="https://control.example/prefix/",
        headers={"Authorization": f"Bearer {TOKEN}"},
        transport=httpx.MockTransport(respond),
        follow_redirects=True,
        trust_env=False,
    ) as client:
        gateway = HttpControlGateway(client, now=now, timeout_seconds=timeout_seconds)

        return await gateway.heartbeat(pending, authorization, reference=reference)


@pytest.mark.parametrize("state", ["renewed", "replayed", "expired", "revoked"])
def test_heartbeat_preserves_original_pins_and_opaque_wire_input(
    authorization: WorkerAuthorization,
    reference: AssignmentReference,
    pending: HeartbeatRequest,
    state: str,
) -> None:
    body = response_body(pending, reference)
    result = body["result"]
    authority = result["authority"]
    calls: list[httpx.Request] = []
    if state != "renewed":
        result["renewed"] = False
    if state == "expired":
        authority["observed_at"] = (NOW + timedelta(seconds=120)).isoformat()
        result["stop_required"] = True
    if state == "revoked":
        authority["revoked_at"] = authority["updated_at"]
        result["stop_required"] = True

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json=body)

    outcome = asyncio.run(exchange(pending, authorization, reference, respond))

    assert outcome.authority.reference == reference
    assert outcome.renewed is (state == "renewed")
    assert outcome.stop_required is (state in ("expired", "revoked"))
    assert pending.claim.proof.reveal() not in repr(outcome)
    assert TOKEN not in repr(outcome)
    assert len(calls) == 1
    sent = calls[0]
    assert str(sent.url) == (
        f"https://control.example/prefix/api/v1/worker/attempts/{reference.attempt_id}"
        "/heartbeats"
    )
    assert sent.method == "POST"
    assert sent.headers["authorization"] == f"Bearer {TOKEN}"
    assert sent.headers["accept-encoding"] == "identity"
    assert json.loads(sent.content) == {
        "protocol_version": 1,
        "claim_request_id": str(pending.claim.claim_request_id),
        "proof": pending.claim.proof.reveal(),
        "heartbeat_request_id": str(pending.heartbeat_request_id),
        "generation": 1,
        "expected_lease_revision": 1,
    }


@pytest.mark.parametrize(
    "change",
    [
        "protocol",
        "bool_protocol",
        "missing_protocol",
        "extra_outer",
        "missing_result",
        "request",
        "extra_result",
        "missing_result_field",
        "boolean",
        "revision",
        "bool_revision",
        "large_revision",
        "string_revision",
        "stale_revision",
        "renewal_skip",
        "credential_cap",
        "extra_authority",
        "missing_authority_field",
        "naive",
        "future_update",
        "ttl",
        "mixed_stop",
        "expired_active",
        "revoked_active",
        "worker",
        "repository",
        "attempt",
        "job",
        "issuance",
        "original_expiry",
        "generation",
        "nested_extra",
    ],
)
def test_malformed_or_uncorrelated_observations_fail_without_secret_values(
    authorization: WorkerAuthorization,
    reference: AssignmentReference,
    pending: HeartbeatRequest,
    change: str,
) -> None:
    body = response_body(pending, reference)
    result = body["result"]
    authority = result["authority"]
    anchors = authority["reference"]
    if change in ("protocol", "bool_protocol"):
        body["protocol_version"] = True if change == "bool_protocol" else 2
    elif change == "missing_protocol":
        del body["protocol_version"]
    elif change == "extra_outer":
        body["proof"] = pending.claim.proof.reveal()
    elif change == "missing_result":
        del body["result"]
    elif change == "request":
        result["heartbeat_request_id"] = str(uuid4())
    elif change == "extra_result":
        result["token"] = TOKEN
    elif change == "missing_result_field":
        del result["renewed"]
    elif change == "boolean":
        result["renewed"] = 1
    elif change in (
        "revision",
        "bool_revision",
        "renewal_skip",
        "large_revision",
        "string_revision",
        "stale_revision",
    ):
        authority["lease_revision"] = {
            "revision": 0,
            "bool_revision": True,
            "renewal_skip": 3,
            "large_revision": 1 << 63,
            "string_revision": "2",
            "stale_revision": 2,
        }[change]
        if change == "stale_revision":
            pending = replace(pending, expected_lease_revision=3)
            result["renewed"] = False
    elif change == "credential_cap":
        authorization = replace(
            authorization, credential_expires_at=NOW + timedelta(seconds=115)
        )
    elif change == "extra_authority":
        authority["launch"] = True
    elif change == "missing_authority_field":
        del authority["observed_at"]
    elif change == "naive":
        authority["observed_at"] = NOW.replace(tzinfo=None).isoformat()
    elif change == "future_update":
        authority["updated_at"] = (NOW + timedelta(seconds=1)).isoformat()
    elif change == "ttl":
        authority["current_expires_at"] = (NOW + timedelta(seconds=301)).isoformat()
    elif change == "mixed_stop":
        result["stop_required"] = True
    elif change == "expired_active":
        authority["observed_at"] = (NOW + timedelta(seconds=120)).isoformat()
    elif change == "revoked_active":
        authority["revoked_at"] = authority["updated_at"]
    elif change in ("worker", "repository", "attempt", "job"):
        anchors[f"{change}_id"] = str(uuid4())
    elif change == "issuance":
        anchors["issued_at"] = (reference.issued_at - timedelta(seconds=1)).isoformat()
    elif change == "original_expiry":
        anchors["expires_at"] = (
            reference.expires_at - timedelta(seconds=1)
        ).isoformat()
    elif change == "generation":
        anchors["generation"] = True
    else:
        anchors["proof"] = pending.claim.proof.reveal()

    with pytest.raises(InvalidControlResponse) as error:
        asyncio.run(
            exchange(
                pending,
                authorization,
                reference,
                lambda request: httpx.Response(200, json=body),
            )
        )

    assert TOKEN not in str(error.value)
    assert pending.claim.proof.reveal() not in str(error.value)
    assert error.value.__cause__ is None
    assert error.value.__suppress_context__


@pytest.mark.parametrize(
    "case",
    [
        "worker",
        "repository",
        "attempt",
        "ttl",
        "request_type",
        "scope_type",
        "reference_type",
        "expired",
    ],
)
def test_wrong_local_input_is_rejected_before_proof_disclosure(
    authorization: WorkerAuthorization,
    reference: AssignmentReference,
    pending: HeartbeatRequest,
    case: str,
) -> None:
    calls: list[httpx.Request] = []
    request_value: Any = pending
    authorization_value: Any = authorization
    reference_value: Any = reference
    if case == "worker":
        reference_value = replace(reference, worker_id=uuid4())
    elif case == "repository":
        reference_value = replace(reference, repository_id=uuid4())
    elif case == "attempt":
        reference_value = replace(reference, attempt_id=uuid4())
    elif case == "ttl":
        reference_value = replace(
            reference, expires_at=reference.issued_at + timedelta(seconds=301)
        )
    elif case == "request_type":
        request_value = pending.claim
    elif case == "scope_type":
        authorization_value = None
    elif case == "reference_type":
        reference_value = None
    else:
        authorization_value = replace(authorization, credential_expires_at=NOW)

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(500)

    with pytest.raises(
        ControlAccessDenied if case == "expired" else ControlClaimRejected
    ):
        asyncio.run(
            exchange(request_value, authorization_value, reference_value, respond)
        )

    assert calls == []


def test_credential_expiry_during_exchange_rejects_even_a_valid_response(
    authorization: WorkerAuthorization,
    reference: AssignmentReference,
    pending: HeartbeatRequest,
) -> None:
    body = response_body(pending, reference)
    observations = iter([NOW, authorization.credential_expires_at])

    with pytest.raises(ControlAccessDenied):
        asyncio.run(
            exchange(
                pending,
                authorization,
                reference,
                lambda request: httpx.Response(200, json=body),
                now=lambda: next(observations),
            )
        )


def test_unrenewed_server_metadata_can_outlive_a_rotated_credential(
    authorization: WorkerAuthorization,
    reference: AssignmentReference,
    pending: HeartbeatRequest,
) -> None:
    body = response_body(pending, reference)
    body["result"]["renewed"] = False
    authorization = replace(
        authorization, credential_expires_at=NOW + timedelta(seconds=30)
    )

    result = asyncio.run(
        exchange(
            pending,
            authorization,
            reference,
            lambda request: httpx.Response(200, json=body),
        )
    )

    assert result.authority.current_expires_at > authorization.credential_expires_at
    assert not result.renewed


@pytest.mark.parametrize(
    "status,error_type",
    [
        (401, ControlAccessDenied),
        (403, ControlAccessDenied),
        (409, ControlClaimRejected),
        (422, ControlClaimRejected),
        (408, ControlUnavailable),
        (429, ControlUnavailable),
        (502, ControlUnavailable),
        (503, ControlUnavailable),
        (504, ControlUnavailable),
        (302, InvalidControlResponse),
        (404, InvalidControlResponse),
        (500, InvalidControlResponse),
    ],
)
def test_error_bodies_and_redirect_targets_are_not_consumed(
    authorization: WorkerAuthorization,
    reference: AssignmentReference,
    pending: HeartbeatRequest,
    status: int,
    error_type: type[Exception],
) -> None:
    calls: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(
            status,
            text=TOKEN + pending.claim.proof.reveal(),
            headers={"Location": "https://attacker.example/"},
        )

    with pytest.raises(error_type) as error:
        asyncio.run(exchange(pending, authorization, reference, respond))

    assert len(calls) == 1
    assert calls[0].url.host == "control.example"
    assert TOKEN not in str(error.value)
    assert pending.claim.proof.reveal() not in str(error.value)


@pytest.mark.parametrize(
    "case", ["gzip", "html", "declared", "duplicate", "nested_duplicate", "json"]
)
def test_invalid_encoding_media_size_and_json_are_rejected(
    authorization: WorkerAuthorization,
    reference: AssignmentReference,
    pending: HeartbeatRequest,
    case: str,
) -> None:
    headers = {"Content-Type": "application/json"}
    body = json.dumps(response_body(pending, reference)).encode()
    if case == "gzip":
        headers["Content-Encoding"] = "gzip"
        body = gzip.compress(body)
    elif case == "html":
        headers["Content-Type"] = "text/html"
    elif case == "declared":
        headers["Content-Length"] = "16385"
    elif case == "duplicate":
        body = body[:-1] + b', "protocol_version": 1}'
    elif case == "nested_duplicate":
        body = body.replace(b'"renewed": true', b'"renewed": true, "renewed": false')
    else:
        body = b"invalid-json"

    with pytest.raises(InvalidControlResponse):
        asyncio.run(
            exchange(
                pending,
                authorization,
                reference,
                lambda request: httpx.Response(200, headers=headers, content=body),
            )
        )


class BoundedStream(httpx.AsyncByteStream):
    def __init__(self, *, slow: bool) -> None:
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
def test_stream_is_closed_after_total_deadline_or_size_failure(
    authorization: WorkerAuthorization,
    reference: AssignmentReference,
    pending: HeartbeatRequest,
    slow: bool,
) -> None:
    stream = BoundedStream(slow=slow)

    with pytest.raises(ControlUnavailable if slow else InvalidControlResponse):
        asyncio.run(
            exchange(
                pending,
                authorization,
                reference,
                lambda request: httpx.Response(
                    200, headers={"Content-Type": "application/json"}, stream=stream
                ),
                timeout_seconds=0.01,
            )
        )

    assert stream.closed


def test_network_failures_suppress_raw_exception_details(
    authorization: WorkerAuthorization,
    reference: AssignmentReference,
    pending: HeartbeatRequest,
) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError(TOKEN + pending.claim.proof.reveal(), request=request)

    with pytest.raises(ControlUnavailable) as error:
        asyncio.run(exchange(pending, authorization, reference, respond))

    assert TOKEN not in str(error.value)
    assert pending.claim.proof.reveal() not in str(error.value)
    assert error.value.__cause__ is None
    assert error.value.__suppress_context__


def test_ambiguous_failure_retries_only_explicit_original_input(
    authorization: WorkerAuthorization,
    reference: AssignmentReference,
    pending: HeartbeatRequest,
) -> None:
    body = response_body(pending, reference)
    calls: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if len(calls) == 1:
            raise httpx.ReadTimeout("Test-only ambiguous timeout.", request=request)
        body["result"]["renewed"] = False
        return httpx.Response(200, json=body)

    async def scenario() -> HeartbeatResult:
        async with httpx.AsyncClient(
            base_url="https://control.example/",
            transport=httpx.MockTransport(respond),
        ) as client:
            gateway = HttpControlGateway(client, now=lambda: NOW)
            with pytest.raises(ControlUnavailable):
                await gateway.heartbeat(pending, authorization, reference=reference)

            return await gateway.heartbeat(pending, authorization, reference=reference)

    result = asyncio.run(scenario())

    assert len(calls) == 2
    assert calls[0].content == calls[1].content
    assert calls[0].url == calls[1].url
    assert not result.renewed
    assert result.heartbeat_request_id == pending.heartbeat_request_id


def test_cancellation_is_not_converted_into_recovery_success(
    authorization: WorkerAuthorization,
    reference: AssignmentReference,
    pending: HeartbeatRequest,
) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(exchange(pending, authorization, reference, respond))
