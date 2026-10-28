import asyncio
import base64
import json
import threading
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import timedelta
from typing import Any
from uuid import uuid4

import httpx
import pytest

import devrelay.infrastructure.control.coding_results as transport
from devrelay.application.contracts.candidates import CandidateSnapshot
from devrelay.application.contracts.claims import LeaseProof
from devrelay.application.contracts.coding_result_receipts import (
    CodingPublicationState,
    CodingResultAcceptance,
    CodingResultReceipt,
    CodingResultStatus,
    TerminalCodingResult,
)
from devrelay.application.contracts.coding_result_response_wire import (
    encode_coding_result_acceptance,
    encode_coding_result_status,
)
from devrelay.application.ports.control_gateway import (
    ControlAccessDenied,
    ControlClaimRejected,
    ControlUnavailable,
    InvalidControlResponse,
)
from devrelay.domain.jobs.values import PullRequestReference
from devrelay.infrastructure.control.coding_results import (
    HttpControlCodingResultGateway,
    open_http_control_coding_results,
)
from tests.unit.test_coding_result_preparation import Context, context

_TOKEN = "test_only_worker_token_0123456789abcdefghijklmnop"


def acceptance(setup: Context) -> CodingResultAcceptance:
    identity = setup.request.identify(setup.authorization)
    intent = identity.intent
    return CodingResultAcceptance(
        receipt=CodingResultReceipt(
            result_id=uuid4(),
            worker_id=setup.authorization.worker_id,
            request_id=intent.result_request_id,
            claim_request_id=identity.claim.claim_request_id,
            job_id=setup.definition.job_id,
            attempt_id=intent.execution.acknowledgement.attempt_id,
            repository_id=setup.definition.repository_id,
            execution_id=intent.execution.execution_id,
            definition_digest=setup.definition.fingerprint(),
            request_fingerprint=identity.fingerprint,
            candidate_revision=intent.result.final_candidate.revision,
            repair_cycles=len(intent.result.cycles) - 1,
            publishing_workflow_version=4 + 2 * (len(intent.result.cycles) - 1),
            recorded_at=setup.authorization.credential_expires_at
            - timedelta(minutes=1),
        ),
        recovered=False,
    )


def status(setup: Context, *, finalized: bool = False) -> CodingResultStatus:
    receipt = acceptance(setup).receipt
    terminal = (
        TerminalCodingResult(
            receipt=receipt,
            reference=PullRequestReference(
                repository_id=setup.definition.repository_id,
                number=42,
                source_branch=setup.definition.authorized_branch,
                target_branch=setup.definition.target_branch,
                head_revision=receipt.candidate_revision,
            ),
            finalized_at=receipt.recorded_at + timedelta(seconds=1),
            workflow_version=receipt.publishing_workflow_version + 1,
            release_ordinal=1,
        )
        if finalized
        else None
    )
    return CodingResultStatus(
        receipt=receipt,
        state=CodingPublicationState.FINALIZED
        if finalized
        else CodingPublicationState.ACCEPTED,
        operation_version=4 if finalized else 0,
        terminal=terminal,
    )


def client(handler: Any, **changes: Any) -> httpx.AsyncClient:
    options: dict[str, Any] = {
        "base_url": "https://control.example/",
        "headers": {"Authorization": "Bearer " + _TOKEN},
        "trust_env": False,
        "transport": httpx.MockTransport(handler),
    }
    options.update(changes)
    return httpx.AsyncClient(**options)


def gateway(
    connection: httpx.AsyncClient, setup: Context, **changes: Any
) -> HttpControlCodingResultGateway:
    options: dict[str, Any] = {
        "now": lambda: setup.authorization.credential_expires_at - timedelta(seconds=30)
    }
    options.update(changes)
    return HttpControlCodingResultGateway(connection, **options)


async def submit(
    value: HttpControlCodingResultGateway, setup: Context
) -> CodingResultAcceptance:
    return await value.submit(
        setup.request,
        setup.authorization,
        job_id=setup.definition.job_id,
        target_branch=setup.definition.target_branch,
    )


async def recover(
    value: HttpControlCodingResultGateway, setup: Context
) -> CodingResultStatus | None:
    return await value.recover(
        setup.request,
        setup.authorization,
        job_id=setup.definition.job_id,
        target_branch=setup.definition.target_branch,
    )


def test_submit_sends_one_fixed_worker_request_and_correlates_acceptance() -> None:
    setup = context()
    accepted = acceptance(setup)
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "POST"
        assert request.url.host == "control.example"
        assert (
            request.url.path
            == f"/api/v1/worker/attempts/{setup.request.intent.execution.acknowledgement.attempt_id}/results"
        )
        assert request.headers["authorization"] == "Bearer " + _TOKEN
        assert request.headers["accept-encoding"] == "identity"
        assert (
            json.loads(request.content)["proof"] == setup.request.claim.proof.reveal()
        )
        return httpx.Response(
            202,
            content=encode_coding_result_acceptance(accepted),
            headers={"content-type": "application/json"},
        )

    async def scenario() -> None:
        connection = client(handle)
        value = gateway(connection, setup)
        try:
            assert await submit(value, setup) == accepted
        finally:
            await value.aclose()
        assert connection.is_closed

    asyncio.run(scenario())
    assert len(requests) == 1


@pytest.mark.parametrize("finalized", [False, True])
def test_recovery_preserves_pending_or_terminal_history(finalized: bool) -> None:
    setup = context()
    result = status(setup, finalized=finalized)

    def handle(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/results/recovery")
        return httpx.Response(
            200,
            content=encode_coding_result_status(result),
            headers={"content-type": "application/json"},
        )

    async def scenario() -> None:
        value = gateway(client(handle), setup)
        try:
            assert await recover(value, setup) == result
        finally:
            await value.aclose()

    asyncio.run(scenario())


@pytest.mark.parametrize("missing", [True, False])
def test_only_named_recovery_404_means_no_result(missing: bool) -> None:
    setup = context()
    error = {
        "detail": {
            "code": "coding_result_not_found" if missing else "route_not_found",
            "message": "private upstream body",
        }
    }

    async def scenario() -> None:
        value = gateway(client(lambda request: httpx.Response(404, json=error)), setup)
        try:
            if missing:
                assert await recover(value, setup) is None
            else:
                with pytest.raises(InvalidControlResponse):
                    await recover(value, setup)
            with pytest.raises(InvalidControlResponse):
                await submit(value, setup)
        finally:
            await value.aclose()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "status_code,error",
    [
        (401, ControlAccessDenied),
        (403, ControlAccessDenied),
        (408, ControlUnavailable),
        (429, ControlUnavailable),
        (500, ControlUnavailable),
        (503, ControlUnavailable),
        (409, ControlClaimRejected),
        (413, ControlClaimRejected),
        (422, ControlClaimRejected),
        (200, InvalidControlResponse),
        (302, InvalidControlResponse),
        (404, InvalidControlResponse),
    ],
)
def test_http_errors_do_not_retry_follow_or_echo_response(
    status_code: int, error: type[Exception]
) -> None:
    setup = context()
    calls = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            status_code,
            content=b"Never echo upstream secret",
            headers={"location": "https://provider.invalid/private"},
        )

    async def scenario() -> None:
        value = gateway(client(handle), setup)
        try:
            with pytest.raises(error) as failure:
                await submit(value, setup)
            assert "upstream secret" not in str(failure.value)
            assert setup.request.claim.proof.reveal() not in str(failure.value)
        finally:
            await value.aclose()

    asyncio.run(scenario())
    assert calls == 1


@pytest.mark.parametrize("field", ["job", "head", "fingerprint", "target", "source"])
def test_response_must_match_full_original_receipt_and_terminal_branches(
    field: str,
) -> None:
    setup = context()
    result = status(setup, finalized=True)
    terminal = result.terminal
    assert terminal is not None
    if field == "job":
        receipt = replace(result.receipt, job_id=uuid4())
        result = replace(
            result, receipt=receipt, terminal=replace(terminal, receipt=receipt)
        )
    elif field == "head":
        receipt = replace(result.receipt, candidate_revision="f" * 40)
        result = replace(
            result,
            receipt=receipt,
            terminal=replace(
                terminal,
                receipt=receipt,
                reference=replace(terminal.reference, head_revision="f" * 40),
            ),
        )
    elif field == "fingerprint":
        receipt = replace(result.receipt, request_fingerprint="f" * 64)
        result = replace(
            result, receipt=receipt, terminal=replace(terminal, receipt=receipt)
        )
    else:
        changes: dict[str, Any] = {
            "target_branch" if field == "target" else "source_branch": "other/branch"
        }
        result = replace(
            result,
            terminal=replace(
                terminal, reference=replace(terminal.reference, **changes)
            ),
        )

    async def scenario() -> None:
        value = gateway(
            client(
                lambda request: httpx.Response(
                    200,
                    content=encode_coding_result_status(result),
                    headers={"content-type": "application/json"},
                )
            ),
            setup,
        )
        try:
            with pytest.raises(InvalidControlResponse):
                await recover(value, setup)
        finally:
            await value.aclose()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "headers",
    [
        [("content-type", "text/plain")],
        [("content-type", "application/json"), ("content-type", "application/json")],
        [("content-type", "application/json"), ("content-encoding", "br")],
        [("content-type", "application/json"), ("content-length", "65537")],
        [("content-type", "application/json"), ("content-length", "2")],
        [("content-type", "application/json"), ("content-length", "abc")],
    ],
)
def test_response_media_and_declared_size_are_bounded(
    headers: list[tuple[str, str]],
) -> None:
    setup = context()
    encoded = encode_coding_result_acceptance(acceptance(setup))

    async def scenario() -> None:
        value = gateway(
            client(
                lambda request: httpx.Response(202, content=encoded, headers=headers)
            ),
            setup,
        )
        try:
            with pytest.raises((InvalidControlResponse, ControlUnavailable)):
                await submit(value, setup)
        finally:
            await value.aclose()

    asyncio.run(scenario())


class Chunks(httpx.AsyncByteStream):
    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b"x" * 32768
        yield b"x" * 32769


def test_streamed_response_limit_does_not_depend_on_declared_length() -> None:
    setup = context()

    async def scenario() -> None:
        value = gateway(
            client(
                lambda request: httpx.Response(
                    202, stream=Chunks(), headers={"content-type": "application/json"}
                )
            ),
            setup,
        )
        try:
            with pytest.raises(InvalidControlResponse):
                await submit(value, setup)
        finally:
            await value.aclose()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "url",
    [
        "http://remote.invalid/",
        "https://user:password@control.example/",
        "https://control.example/?token=x",
        "https://control.example/#fragment",
    ],
)
def test_constructor_rejects_nonlocal_http_or_credential_bearing_urls(url: str) -> None:
    async def scenario() -> None:
        async with client(
            lambda request: httpx.Response(500), base_url=url
        ) as connection:
            with pytest.raises(ValueError):
                gateway(connection, context())

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "url", ["http://127.0.0.1:8000/", "http://[::1]:8000/", "http://localhost:8000/"]
)
def test_explicit_loopback_http_is_supported(url: str) -> None:
    async def scenario() -> None:
        value = gateway(
            client(lambda request: httpx.Response(500), base_url=url), context()
        )
        await value.aclose()

    asyncio.run(scenario())


def test_input_snapshot_precedes_first_await_and_caller_mutation_cannot_repin_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup = context()
    accepted = acceptance(setup)
    original = transport._encode
    entered, release = threading.Event(), threading.Event()
    sent: list[dict[str, Any]] = []
    old_proof = setup.request.claim.proof.reveal()
    old_bytes = setup.request.intent.result.final_candidate.files[0].content

    def encode(value: Any) -> Any:
        entered.set()
        assert release.wait(5)
        return original(value)

    def handle(request: httpx.Request) -> httpx.Response:
        sent.append(json.loads(request.content))
        return httpx.Response(
            202,
            content=encode_coding_result_acceptance(accepted),
            headers={"content-type": "application/json"},
        )

    monkeypatch.setattr(transport, "_encode", encode)

    async def scenario() -> None:
        value = gateway(client(handle), setup)
        pending = asyncio.create_task(submit(value, setup))
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            object.__setattr__(
                setup.request.intent.result.final_candidate.files[0],
                "content",
                b"Changed after capture",
            )
            object.__setattr__(
                setup.request.claim,
                "proof",
                LeaseProof(base64.urlsafe_b64encode(b"y" * 32).decode().rstrip("=")),
            )
            object.__setattr__(setup.authorization, "worker_id", uuid4())
            release.set()
            assert await pending == accepted
        finally:
            release.set()
            await value.aclose()

    asyncio.run(scenario())
    assert sent[0]["proof"] == old_proof
    assert (
        base64.b64decode(sent[0]["result"]["final_candidate"]["files"][0]["content"])
        == old_bytes
    )


def test_candidate_validation_runs_off_event_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup = context()
    encoded = encode_coding_result_acceptance(acceptance(setup))
    original = CandidateSnapshot.validate
    observed: list[int] = []

    def validate(candidate: CandidateSnapshot) -> None:
        observed.append(threading.get_ident())
        original(candidate)

    monkeypatch.setattr(CandidateSnapshot, "validate", validate)

    async def scenario() -> None:
        loop_thread = threading.get_ident()
        value = gateway(
            client(
                lambda request: httpx.Response(
                    202, content=encoded, headers={"content-type": "application/json"}
                )
            ),
            setup,
        )
        try:
            await submit(value, setup)
        finally:
            await value.aclose()
        assert observed and all(identity != loop_thread for identity in observed)

    asyncio.run(scenario())


@pytest.mark.parametrize("stage", ["_encode", "_decode"])
def test_cancelled_cpu_work_retains_admission_until_actual_future_and_close_settle(
    stage: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup = context()
    encoded = encode_coding_result_acceptance(acceptance(setup))
    original = getattr(transport, stage)
    entered, release = threading.Event(), threading.Event()
    calls = 0

    def held(*args: Any) -> Any:
        entered.set()
        assert release.wait(5)
        return original(*args)

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            202, content=encoded, headers={"content-type": "application/json"}
        )

    monkeypatch.setattr(transport, stage, held)

    async def scenario() -> None:
        value = gateway(client(handle), setup)
        pending = asyncio.create_task(submit(value, setup))
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
            with pytest.raises(ControlClaimRejected):
                await recover(value, setup)
            closing = asyncio.create_task(value.aclose())
            await asyncio.sleep(0)
            assert not closing.done()
            release.set()
            await closing
            with pytest.raises(ControlClaimRejected):
                await submit(value, setup)
        finally:
            release.set()
            await value.aclose()

    asyncio.run(scenario())
    assert calls == (0 if stage == "_encode" else 1)


def test_timeout_does_not_cancel_actual_encoder_or_allow_a_second_exchange(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup = context()
    original = transport._encode
    entered, release = threading.Event(), threading.Event()

    def held(value: Any) -> Any:
        entered.set()
        assert release.wait(5)
        return original(value)

    monkeypatch.setattr(transport, "_encode", held)

    async def scenario() -> None:
        value = gateway(
            client(
                lambda request: (_ for _ in ()).throw(
                    AssertionError("No request before encoder settles.")
                )
            ),
            setup,
            timeout_seconds=0.05,
        )
        try:
            with pytest.raises(ControlUnavailable):
                await submit(value, setup)
            assert entered.is_set()
            with pytest.raises(ControlClaimRejected):
                await submit(value, setup)
        finally:
            release.set()
            await value.aclose()

    asyncio.run(scenario())


def test_factory_configures_separate_worker_only_connection_without_proxy_or_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup = context()
    response = encode_coding_result_acceptance(acceptance(setup))
    original_client = httpx.AsyncClient
    observed: dict[str, Any] = {}

    def make_transport(**options: Any) -> httpx.MockTransport:
        observed["transport"] = options
        return httpx.MockTransport(
            lambda request: httpx.Response(
                202, content=response, headers={"content-type": "application/json"}
            )
        )

    def make_client(**options: Any) -> httpx.AsyncClient:
        observed["client"] = options
        return original_client(**options)

    monkeypatch.setattr(httpx, "AsyncHTTPTransport", make_transport)
    monkeypatch.setattr(httpx, "AsyncClient", make_client)

    async def scenario() -> None:
        async with open_http_control_coding_results(
            "https://control.example/",
            _TOKEN,
            now=lambda: (
                setup.authorization.credential_expires_at - timedelta(seconds=30)
            ),
        ) as value:
            await submit(value, setup)

    asyncio.run(scenario())
    assert observed["client"]["trust_env"] is False
    assert observed["client"]["follow_redirects"] is False
    assert observed["client"]["limits"].max_connections == 1
    assert observed["transport"]["retries"] == 0
    assert observed["transport"]["limits"].max_connections == 1


def test_concurrent_exchange_is_rejected_while_http_response_is_pending() -> None:
    setup = context()
    response = encode_coding_result_acceptance(acceptance(setup))

    async def scenario() -> None:
        entered, release = asyncio.Event(), asyncio.Event()

        async def handle(request: httpx.Request) -> httpx.Response:
            entered.set()
            await release.wait()
            return httpx.Response(
                202, content=response, headers={"content-type": "application/json"}
            )

        value = gateway(client(handle), setup)
        pending = asyncio.create_task(submit(value, setup))
        try:
            await entered.wait()
            with pytest.raises(ControlClaimRejected):
                await recover(value, setup)
            release.set()
            await pending
        finally:
            release.set()
            await value.aclose()

    asyncio.run(scenario())


def test_credential_expiry_during_http_response_cannot_return_acceptance() -> None:
    setup = context()
    response = encode_coding_result_acceptance(acceptance(setup))
    current = [setup.authorization.credential_expires_at - timedelta(seconds=1)]

    def handle(request: httpx.Request) -> httpx.Response:
        current[0] = setup.authorization.credential_expires_at
        return httpx.Response(
            202, content=response, headers={"content-type": "application/json"}
        )

    async def scenario() -> None:
        value = gateway(client(handle), setup, now=lambda: current[0])
        try:
            with pytest.raises(ControlAccessDenied):
                await submit(value, setup)
        finally:
            await value.aclose()

    asyncio.run(scenario())


def test_fixed_control_destination_cannot_change_during_cpu_capture(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup = context()
    original = transport._encode
    entered, release = threading.Event(), threading.Event()
    sent: list[str] = []

    def encode(value: Any) -> Any:
        entered.set()
        assert release.wait(5)
        return original(value)

    def handle(request: httpx.Request) -> httpx.Response:
        sent.append(str(request.url))
        return httpx.Response(500)

    monkeypatch.setattr(transport, "_encode", encode)

    async def scenario() -> None:
        connection = client(handle)
        value = gateway(connection, setup)
        pending = asyncio.create_task(submit(value, setup))
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            connection.base_url = "https://other.invalid/"
            release.set()
            with pytest.raises(ControlClaimRejected):
                await pending
        finally:
            release.set()
            await value.aclose()

    asyncio.run(scenario())
    assert sent == []


@pytest.mark.parametrize("stopped", [False, True])
def test_submission_guard_runs_after_encoding_and_immediately_before_http(
    monkeypatch: pytest.MonkeyPatch, stopped: bool
) -> None:
    setup = context()
    accepted = acceptance(setup)
    original = transport._encode
    entered, release = threading.Event(), threading.Event()
    stages: list[str] = []

    class StoppedResult(RuntimeError):
        pass

    def encode(value: Any) -> Any:
        entered.set()
        assert release.wait(5)
        result = original(value)
        stages.append("encoded")
        return result

    def guard() -> None:
        assert stages == ["encoded"]
        stages.append("guard")
        if stopped:
            raise StoppedResult

    def handle(request: httpx.Request) -> httpx.Response:
        assert stages == ["encoded", "guard"]
        stages.append("http")
        return httpx.Response(
            202,
            content=encode_coding_result_acceptance(accepted),
            headers={"content-type": "application/json"},
        )

    monkeypatch.setattr(transport, "_encode", encode)

    async def scenario() -> None:
        value = gateway(client(handle), setup)
        pending = asyncio.create_task(
            value.submit(
                setup.request,
                setup.authorization,
                job_id=setup.definition.job_id,
                target_branch=setup.definition.target_branch,
                before_submit=guard,
            )
        )
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            assert stages == []
            release.set()
            if stopped:
                with pytest.raises(StoppedResult):
                    await pending
            else:
                assert await pending == accepted
        finally:
            release.set()
            await value.aclose()

    asyncio.run(scenario())
    assert stages == (["encoded", "guard"] if stopped else ["encoded", "guard", "http"])


def test_network_failure_is_redacted_without_a_blind_retry() -> None:
    setup = context()
    calls = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.ConnectError(
            "private upstream exception " + setup.request.claim.proof.reveal(),
            request=request,
        )

    async def scenario() -> None:
        value = gateway(client(handle), setup)
        try:
            with pytest.raises(ControlUnavailable) as error:
                await submit(value, setup)
            assert setup.request.claim.proof.reveal() not in str(error.value)
            assert error.value.__suppress_context__
        finally:
            await value.aclose()

    asyncio.run(scenario())
    assert calls == 1


def test_factory_injected_transport_does_not_make_a_startup_request() -> None:
    calls = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500)

    async def scenario() -> None:
        async with open_http_control_coding_results(
            "https://control.example/", _TOKEN, transport=httpx.MockTransport(handle)
        ):
            assert calls == 0

    asyncio.run(scenario())
    assert calls == 0
