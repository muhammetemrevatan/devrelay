import asyncio
import json
import threading
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

import httpx
import pytest

from devrelay.application.contracts.assignment_delivery import AssignmentDelivery
from devrelay.application.contracts.base_source_delivery import (
    MAX_BASE_DELIVERY_BYTES,
    BaseSourceDelivery,
    decode_base_delivery,
    encode_base_delivery,
)
from devrelay.application.contracts.base_sources import (
    BaseFileMode,
    BaseSourceFile,
    BaseSourceRequest,
    BaseSourceSnapshot,
    git_blob_sha,
)
from devrelay.application.contracts.claims import LeaseProof, WorkerClaimRequest
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.control_gateway import (
    ControlAccessDenied,
    ControlClaimRejected,
    ControlUnavailable,
    InvalidControlResponse,
)
from devrelay.infrastructure.control import base_sources
from devrelay.infrastructure.control.base_sources import HttpControlBaseSource
from tests.acknowledgement_fixtures import acknowledgement_inputs
from tests.assignment_fixtures import NOW

TOKEN = "test_only_worker_token_0123456789abcdefghijklmnop"
type Inputs = tuple[
    WorkerClaimRequest,
    WorkerAuthorization,
    AssignmentDelivery,
    BaseSourceRequest,
    BaseSourceDelivery,
]


@pytest.fixture
def inputs() -> Inputs:
    claim, assignment = acknowledgement_inputs()
    authorization = WorkerAuthorization(
        worker_id=assignment.outcome.worker_id,
        allowed_repository_ids=frozenset({assignment.definition.repository_id}),
        credential_expires_at=NOW + timedelta(hours=1),
    )
    definition = assignment.definition
    source = BaseSourceRequest(
        repository_id=definition.repository_id,
        provider_repository_id=definition.provider_repository_id,
        repository_slug=definition.repository_slug,
        base_revision=definition.base_revision,
    )
    content = b"Literal fixture\x00\xff\r\n"
    snapshot = BaseSourceSnapshot(
        request=source,
        root_tree_sha="c" * 40,
        files=(
            BaseSourceFile(
                path="README.md",
                blob_sha=git_blob_sha(content),
                mode=BaseFileMode.REGULAR,
                content=content,
            ),
        ),
    )
    reference = assignment.outcome.assignment
    assert reference is not None
    return (
        claim,
        authorization,
        assignment,
        source,
        BaseSourceDelivery(
            reference=reference,
            claim_request_id=claim.claim_request_id,
            definition_digest=definition.fingerprint(),
            snapshot=snapshot,
        ),
    )


def adapter(
    client: httpx.AsyncClient, inputs: Inputs, *, timeout: float = 90
) -> HttpControlBaseSource:
    claim, authorization, assignment, _, _ = inputs
    return HttpControlBaseSource(
        client,
        claim,
        authorization,
        assignment,
        now=lambda: NOW,
        timeout_seconds=timeout,
    )


class LiteralStream(httpx.AsyncByteStream):
    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b"{}"


def test_source_channel_posts_original_proof_only_to_known_control_attempt(
    inputs: Inputs,
) -> None:
    # Arrange: a broker envelope contains literal bytes and original accepted pins.
    claim, _, _, request, delivery = inputs
    calls: list[httpx.Request] = []

    def respond(sent: httpx.Request) -> httpx.Response:
        calls.append(sent)
        return httpx.Response(
            200,
            headers={"Content-Type": "application/json; charset=utf-8"},
            content=encode_base_delivery(delivery),
        )

    async def scenario() -> BaseSourceSnapshot:
        async with httpx.AsyncClient(
            base_url="https://control.example/prefix/",
            headers={"Authorization": f"Bearer {TOKEN}"},
            transport=httpx.MockTransport(respond),
            trust_env=False,
        ) as client:
            return await adapter(client, inputs).read(request)

    # Act: perform one explicit source read without identity/provider side effects.
    snapshot = asyncio.run(scenario())

    # Assert: exact bytes/pins survive and the worker sends no source URL or policy.
    assert snapshot == delivery.snapshot
    assert len(calls) == 1
    sent = calls[0]
    assert sent.method == "POST"
    assert str(sent.url) == (
        "https://control.example/prefix/api/v1/worker/attempts/"
        f"{delivery.reference.attempt_id}/base-source"
    )
    assert sent.headers["authorization"] == f"Bearer {TOKEN}"
    assert sent.headers["accept-encoding"] == "identity"
    assert sent.headers["content-type"] == "application/json"
    assert len(sent.content) <= 1024
    assert json.loads(sent.content) == {
        "protocol_version": claim.protocol_version,
        "claim_request_id": str(claim.claim_request_id),
        "proof": claim.proof.reveal(),
    }
    assert claim.proof.reveal() not in repr(snapshot)


@pytest.mark.parametrize(
    "url",
    [
        "http://control.example",
        "https://user:pass@control.example",
        "https://control.example?source=anything",
        "https://control.example/#fragment",
        "http://localhost.attacker.example",
        "http://192.168.1.2",
        "http://[::2]",
    ],
)
def test_unsafe_control_urls_are_rejected_before_transport(
    inputs: Inputs, url: str
) -> None:
    # Arrange: the worker receives an unsupported control destination.
    client = httpx.AsyncClient(base_url=url, trust_env=False)

    # Act / Assert: configuration cannot become a request.
    with pytest.raises(ValueError, match="control URL"):
        adapter(client, inputs)
    asyncio.run(client.aclose())


@pytest.mark.parametrize(
    "url", ["http://localhost:8080", "http://127.0.0.1:8080", "http://[::1]:8080"]
)
def test_explicit_loopback_tunnel_can_use_matching_fixed_control_destination(
    inputs: Inputs, url: str
) -> None:
    # Arrange: existing worker policy permits plain HTTP only on a loopback tunnel.
    sent: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        return httpx.Response(
            200,
            headers={"Content-Type": "application/json"},
            content=encode_base_delivery(inputs[4]),
        )

    async def scenario() -> None:
        async with httpx.AsyncClient(
            base_url=url, transport=httpx.MockTransport(respond), trust_env=False
        ) as client:
            # Act: read only from the known control route without changing origin.
            assert await adapter(client, inputs).read(inputs[3]) == inputs[4].snapshot

    asyncio.run(scenario())
    # Assert: local HTTP permits no destination or protocol widening.
    assert len(sent) == 1
    assert str(sent[0].url).startswith(url + "/api/v1/worker/attempts/")


@pytest.mark.parametrize("timeout", [True, float("nan"), float("inf"), 10**400, 0, 121])
def test_transport_deadline_is_finite_and_bounded(
    inputs: Inputs, timeout: float
) -> None:
    # Arrange: invalid timing must not produce an unbounded exchange.
    client = httpx.AsyncClient(base_url="https://control.example", trust_env=False)

    # Act / Assert: reject even huge integers without overflow.
    with pytest.raises(ValueError, match="bounded trusted input"):
        adapter(client, inputs, timeout=timeout)
    asyncio.run(client.aclose())


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
def test_failures_and_redirects_never_disclose_or_follow_response_data(
    inputs: Inputs, status: int, error_type: type[Exception]
) -> None:
    # Arrange: a failure body and Location contain untrusted secret-shaped values.
    calls: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(
            status,
            headers={"Location": "https://attacker.example/source"},
            content=(TOKEN + inputs[0].proof.reveal()).encode(),
        )

    async def scenario() -> None:
        async with httpx.AsyncClient(
            base_url="https://control.example",
            follow_redirects=True,
            transport=httpx.MockTransport(respond),
            trust_env=False,
        ) as client:
            # Act / Assert: transport classifies status without trusting body/redirect.
            with pytest.raises(error_type) as caught:
                await adapter(client, inputs).read(inputs[3])
            assert TOKEN not in str(caught.value)
            assert inputs[0].proof.reveal() not in str(caught.value)

    asyncio.run(scenario())
    assert len(calls) == 1
    assert calls[0].url.host == "control.example"


@pytest.mark.parametrize(
    "change",
    [
        "attempt",
        "worker",
        "job",
        "repository",
        "issuance",
        "expiry",
        "claim",
        "definition",
        "numeric_repository",
        "slug",
        "base",
        "manifest",
        "blob",
        "content",
        "duplicate",
        "extra",
        "version",
    ],
)
def test_response_cannot_replace_original_assignment_or_literal_source(
    inputs: Inputs, change: str
) -> None:
    # Arrange: mutate a valid broker envelope while preserving all other fields.
    payload = json.loads(encode_base_delivery(inputs[4]))
    reference = payload["reference"]
    if change in {"attempt", "worker", "job", "repository"}:
        reference[f"{change}_id"] = str(uuid4())
    elif change in {"issuance", "expiry"}:
        key = "issued_at" if change == "issuance" else "expires_at"
        reference[key] = (
            NOW + timedelta(seconds=1 if change == "issuance" else 121)
        ).isoformat()
    elif change == "claim":
        payload["claim_request_id"] = str(uuid4())
    elif change == "definition":
        payload["definition_digest"] = "d" * 64
    elif change == "numeric_repository":
        payload["source"]["provider_repository_id"] += 1
    elif change == "slug":
        payload["source"]["repository_slug"] = "other/repository"
    elif change == "base":
        payload["source"]["base_revision"] = "d" * 40
    elif change == "manifest":
        payload["manifest_digest"] = "d" * 64
    elif change == "blob":
        payload["files"][0]["blob_sha"] = "d" * 40
    elif change == "content":
        payload["files"][0]["content"] = "!" * len(payload["files"][0]["content"])
    elif change == "extra":
        payload["proof"] = inputs[0].proof.reveal()
    elif change == "version":
        payload["protocol_version"] = True
    body = json.dumps(payload).encode()
    if change == "duplicate":
        body = body[:-1] + b',"protocol_version":1}'

    async def scenario() -> None:
        async with httpx.AsyncClient(
            base_url="https://control.example",
            transport=httpx.MockTransport(
                lambda sent: httpx.Response(
                    200, headers={"Content-Type": "application/json"}, content=body
                )
            ),
            trust_env=False,
        ) as client:
            # Act / Assert: no partial/unrelated response becomes source evidence.
            with pytest.raises(InvalidControlResponse) as caught:
                await adapter(client, inputs).read(inputs[3])
            assert inputs[0].proof.reveal() not in str(caught.value)

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "headers",
    [
        {"Content-Type": "text/html"},
        {"Content-Type": "application/json", "Content-Encoding": "gzip"},
        {"Content-Type": "application/json", "Content-Encoding": "br"},
        {"Content-Type": "application/json", "Content-Length": "-1"},
        {"Content-Type": "application/json", "Content-Length": "１２"},
        {
            "Content-Type": "application/json",
            "Content-Length": str(MAX_BASE_DELIVERY_BYTES + 1),
        },
        {"Content-Type": "application/json", "Content-Length": "0"},
    ],
)
def test_untrusted_media_compression_and_lengths_are_rejected(
    inputs: Inputs, headers: dict[str, str]
) -> None:
    # Arrange: the response body must not override transport metadata constraints.
    async def scenario() -> None:
        async with httpx.AsyncClient(
            base_url="https://control.example",
            transport=httpx.MockTransport(
                lambda sent: httpx.Response(
                    200,
                    headers=httpx.Headers(headers, encoding="utf-8"),
                    stream=LiteralStream(),
                )
            ),
            trust_env=False,
        ) as client:
            # Act / Assert: unsupported response metadata fails closed.
            with pytest.raises(InvalidControlResponse):
                await adapter(client, inputs).read(inputs[3])

    asyncio.run(scenario())


class SourceStream(httpx.AsyncByteStream):
    def __init__(self, *, oversized: bool = False) -> None:
        self.oversized = oversized
        self.entered = asyncio.Event()
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        self.entered.set()
        yield b"{"
        if self.oversized:
            yield b"x" * MAX_BASE_DELIVERY_BYTES
        else:
            await asyncio.Event().wait()

    async def aclose(self) -> None:
        self.closed = True


@pytest.mark.parametrize("oversized", [True, False])
def test_raw_stream_bounds_close_response_and_preserve_timeout_classification(
    inputs: Inputs, oversized: bool
) -> None:
    # Arrange: a raw stream exceeds the size or total read deadline.
    stream = SourceStream(oversized=oversized)

    async def scenario() -> None:
        async with httpx.AsyncClient(
            base_url="https://control.example",
            transport=httpx.MockTransport(
                lambda sent: httpx.Response(
                    200, headers={"Content-Type": "application/json"}, stream=stream
                )
            ),
            trust_env=False,
        ) as client:
            # Act / Assert: no incomplete bytes escape as a source snapshot.
            with pytest.raises(
                InvalidControlResponse if oversized else ControlUnavailable
            ):
                await adapter(client, inputs, timeout=0.05).read(inputs[3])

    asyncio.run(scenario())
    assert stream.closed


def test_cancelled_read_closes_stream_and_rejects_concurrent_exchange(
    inputs: Inputs,
) -> None:
    # Arrange: hold the only explicit source exchange inside a response stream.
    stream = SourceStream()
    calls: list[httpx.Request] = []

    def respond(sent: httpx.Request) -> httpx.Response:
        calls.append(sent)
        return httpx.Response(
            200, headers={"Content-Type": "application/json"}, stream=stream
        )

    async def scenario() -> None:
        async with httpx.AsyncClient(
            base_url="https://control.example",
            transport=httpx.MockTransport(respond),
            trust_env=False,
        ) as client:
            source = adapter(client, inputs)
            pending = asyncio.create_task(source.read(inputs[3]))
            await stream.entered.wait()
            # Act / Assert: overlap cannot send a second proof-bound request.
            with pytest.raises(ControlClaimRejected, match="unsettled"):
                await source.read(inputs[3])
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending

    asyncio.run(scenario())
    assert len(calls) == 1 and stream.closed


@pytest.mark.parametrize("boundary", ["before", "response", "decode"])
def test_expired_credentials_are_rechecked_after_every_awaited_stage(
    inputs: Inputs, boundary: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange: change trusted current time at a chosen asynchronous boundary.
    current = [NOW + timedelta(hours=1) if boundary == "before" else NOW]
    calls: list[httpx.Request] = []
    decoder = decode_base_delivery

    def decode(body: bytes) -> BaseSourceDelivery:
        result = decoder(body)
        if boundary == "decode":
            current[0] = NOW + timedelta(hours=1)
        return result

    monkeypatch.setattr(base_sources, "decode_base_delivery", decode)

    def respond(sent: httpx.Request) -> httpx.Response:
        calls.append(sent)
        if boundary == "response":
            current[0] = NOW + timedelta(hours=1)
        return httpx.Response(
            200,
            headers={"Content-Type": "application/json"},
            content=encode_base_delivery(inputs[4]),
        )

    async def scenario() -> None:
        async with httpx.AsyncClient(
            base_url="https://control.example",
            transport=httpx.MockTransport(respond),
            trust_env=False,
        ) as client:
            source = HttpControlBaseSource(
                client, inputs[0], inputs[1], inputs[2], now=lambda: current[0]
            )
            # Act / Assert: valid bytes cannot revive expired worker credentials.
            with pytest.raises(ControlAccessDenied):
                await source.read(inputs[3])

    asyncio.run(scenario())
    assert len(calls) == (0 if boundary == "before" else 1)


@pytest.mark.parametrize(
    "change",
    [
        "source",
        "proof",
        "claim",
        "scope",
        "url",
        "query",
        "definition",
        "outcome",
        "reference",
        "bool_generation",
        "bool_outcome_protocol",
    ],
)
def test_captured_original_input_is_fenced_before_provider_free_read(
    inputs: Inputs, change: str
) -> None:
    # Arrange: capture trusted pins, then change caller-owned state before reading.
    calls: list[httpx.Request] = []

    def respond(sent: httpx.Request) -> httpx.Response:
        calls.append(sent)
        return httpx.Response(500)

    async def scenario() -> None:
        async with httpx.AsyncClient(
            base_url="https://control.example",
            transport=httpx.MockTransport(respond),
            trust_env=False,
        ) as client:
            source = adapter(client, inputs)
            request = inputs[3]
            if change == "source":
                request = replace(request, base_revision="e" * 40)
            elif change == "proof":
                object.__setattr__(inputs[0], "proof", LeaseProof.generate())
            elif change == "claim":
                object.__setattr__(inputs[0], "claim_request_id", uuid4())
            elif change == "scope":
                object.__setattr__(
                    inputs[1], "allowed_repository_ids", frozenset({uuid4()})
                )
            elif change == "url":
                client.base_url = "https://attacker.example"
            elif change == "query":
                client.params = {"source": "anything"}
            elif change == "definition":
                object.__setattr__(inputs[2].definition, "target_branch", "other")
            elif change == "reference":
                reference = inputs[2].outcome.assignment
                assert reference is not None
                object.__setattr__(reference, "attempt_id", uuid4())
            elif change == "bool_generation":
                reference = inputs[2].outcome.assignment
                assert reference is not None
                object.__setattr__(reference, "generation", True)
            elif change == "bool_outcome_protocol":
                object.__setattr__(inputs[2].outcome, "protocol_version", True)
            else:
                object.__setattr__(
                    inputs[2].outcome, "recorded_at", NOW + timedelta(seconds=1)
                )
            # Act / Assert: no changed input is sent to control.
            with pytest.raises(ControlClaimRejected):
                await source.read(request)

    asyncio.run(scenario())
    assert calls == []


def test_decode_and_definition_work_stay_off_the_event_loop(
    inputs: Inputs, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange: record the thread where the consumed codec validates bytes/hashes.
    threads: list[int] = []
    decoder = decode_base_delivery

    def decode(body: bytes) -> BaseSourceDelivery:
        threads.append(threading.get_ident())
        return decoder(body)

    monkeypatch.setattr(base_sources, "decode_base_delivery", decode)
    loop_thread: list[int] = []

    async def scenario() -> None:
        loop_thread.append(threading.get_ident())
        async with httpx.AsyncClient(
            base_url="https://control.example",
            transport=httpx.MockTransport(
                lambda sent: httpx.Response(
                    200,
                    headers={"Content-Type": "application/json"},
                    content=encode_base_delivery(inputs[4]),
                )
            ),
            trust_env=False,
        ) as client:
            source = await asyncio.to_thread(adapter, client, inputs)
            # Act: decode a real bounded fixture envelope through the adapter.
            await source.read(inputs[3])

    asyncio.run(scenario())
    # Assert: the CPU validation boundary never runs on the event-loop thread.
    assert len(threads) == 1 and threads[0] != loop_thread[0]


def test_network_failure_has_a_fixed_credential_free_error(inputs: Inputs) -> None:
    # Arrange: a transport exception carries secret-shaped diagnostic data.
    def respond(sent: httpx.Request) -> httpx.Response:
        raise httpx.ReadError(TOKEN + inputs[0].proof.reveal(), request=sent)

    async def scenario() -> None:
        async with httpx.AsyncClient(
            base_url="https://control.example",
            transport=httpx.MockTransport(respond),
            trust_env=False,
        ) as client:
            # Act / Assert: sensitive transport diagnostics are discarded.
            with pytest.raises(ControlUnavailable) as caught:
                await adapter(client, inputs).read(inputs[3])
            assert TOKEN not in str(caught.value)
            assert inputs[0].proof.reveal() not in str(caught.value)

    asyncio.run(scenario())
