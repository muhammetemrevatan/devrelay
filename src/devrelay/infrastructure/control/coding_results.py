"""Worker-only result transport with owned bounded CPU work and no retries."""

import asyncio
import copy
import json
import re
from collections.abc import AsyncIterator, Callable
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import asynccontextmanager
from dataclasses import dataclass, fields, replace
from datetime import UTC, datetime, timedelta
from enum import Enum
from ipaddress import ip_address
from math import isfinite
from typing import TypeVar, cast
from uuid import UUID

import httpx

from devrelay.application.contracts.acknowledgements import WorkerAcknowledgementIntent
from devrelay.application.contracts.base_sources import (
    BaseSourceFile,
    BaseSourceRequest,
)
from devrelay.application.contracts.candidates import (
    CandidateSnapshot,
    validate_candidate_branch,
)
from devrelay.application.contracts.claims import LeaseProof, WorkerClaimRequest
from devrelay.application.contracts.coding_result_receipts import (
    CodingResultAcceptance,
    CodingResultStatus,
)
from devrelay.application.contracts.coding_result_requests import (
    CodingResultIdentity,
    WorkerCodingResultIntent,
    WorkerCodingResultRequest,
)
from devrelay.application.contracts.coding_result_response_wire import (
    MAX_CODING_RESULT_RESPONSE_BYTES,
    decode_coding_result_acceptance,
    decode_coding_result_status,
)
from devrelay.application.contracts.coding_result_wire import (
    encode_coding_result_request,
)
from devrelay.application.contracts.coding_results import (
    CodingCheckEvidence,
    CodingCycleEvidence,
    SettledProcessEvidence,
    SuccessfulCodingResult,
)
from devrelay.application.contracts.executions import (
    ExecutionPolicy,
    WorkerExecutionIntent,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.control_gateway import (
    ControlAccessDenied,
    ControlClaimRejected,
    ControlUnavailable,
    InvalidControlResponse,
)

_Value = TypeVar("_Value")
_OWNED_TYPES = frozenset(
    {
        WorkerCodingResultIntent,
        WorkerExecutionIntent,
        WorkerAcknowledgementIntent,
        ExecutionPolicy,
        SuccessfulCodingResult,
        CodingCycleEvidence,
        CodingCheckEvidence,
        SettledProcessEvidence,
        CandidateSnapshot,
        BaseSourceRequest,
        BaseSourceFile,
    }
)


@dataclass(frozen=True, slots=True, repr=False)
class _Input:
    intent: WorkerCodingResultIntent
    claim: WorkerClaimRequest
    authorization: WorkerAuthorization
    job_id: UUID
    target_branch: str


@dataclass(frozen=True, slots=True, repr=False)
class _Encoded:
    identity: CodingResultIdentity
    authorization: WorkerAuthorization
    job_id: UUID
    target_branch: str
    body: bytes


def _bounded_copy_shape(value: object) -> None:
    """Bound graph copying without hashing source bytes on the event-loop thread."""
    nodes = 0

    def visit(item: object, depth: int) -> None:
        nonlocal nodes
        nodes += 1
        if depth > 10 or nodes > 12_000:
            raise ValueError("Unbounded result input.")
        if type(item) in (UUID, int) or isinstance(item, Enum):
            return
        if type(item) is bytes:
            if len(item) > 2 * 1024**2:
                raise ValueError("Unbounded result bytes.")
            return
        if type(item) is str:
            if len(item) > 65536:
                raise ValueError("Unbounded result text.")
            return
        if type(item) is tuple:
            if len(item) > 512:
                raise ValueError("Unbounded result sequence.")
            for child in item:
                visit(child, depth + 1)
            return
        if type(item) not in _OWNED_TYPES:
            raise ValueError("Unexpected result input.")
        for field in fields(item):  # type: ignore[arg-type]
            visit(getattr(item, field.name), depth + 1)

    visit(value, 0)


def _snapshot(
    request: WorkerCodingResultRequest,
    authorization: WorkerAuthorization,
    job_id: UUID,
    target_branch: str,
) -> _Input:
    try:
        if (
            not isinstance(request, WorkerCodingResultRequest)
            or not isinstance(authorization, WorkerAuthorization)
            or not isinstance(job_id, UUID)
        ):
            raise ValueError("Typed original result input is required.")
        validate_candidate_branch(target_branch)
        authorization.__post_init__()
        request.claim.__post_init__()
        _bounded_copy_shape(request.intent)
        # Frozen slot dataclasses copy their graph without invoking full-byte
        # validators. Immutable bytes/strings need no source-size memory copy.
        intent = copy.deepcopy(request.intent)
        claim = WorkerClaimRequest(
            claim_request_id=request.claim.claim_request_id,
            proof=LeaseProof(request.claim.proof.reveal()),
            protocol_version=request.claim.protocol_version,
        )
        return _Input(intent, claim, replace(authorization), job_id, target_branch)
    except ValueError, TypeError, AttributeError, RecursionError:
        raise ControlClaimRejected("Coding result input is invalid.") from None


def _encode(value: _Input) -> _Encoded:
    try:
        request = WorkerCodingResultRequest(intent=value.intent, claim=value.claim)
        request.__post_init__()
        candidate = request.intent.result.final_candidate
        if (
            candidate.request.repository_id
            not in value.authorization.allowed_repository_ids
            or candidate.authorized_branch == value.target_branch
        ):
            raise ValueError("Original result scope is unavailable.")
        return _Encoded(
            request.identify(value.authorization),
            value.authorization,
            value.job_id,
            value.target_branch,
            encode_coding_result_request(request),
        )
    except ValueError, TypeError, AttributeError, RecursionError:
        raise ControlClaimRejected("Coding result input failed validation.") from None


def _decode(
    body: bytes, value: _Encoded, recovery: bool
) -> CodingResultAcceptance | CodingResultStatus:
    try:
        result = (
            decode_coding_result_status(body)
            if recovery
            else decode_coding_result_acceptance(body)
        )
        result.receipt.require_input(value.identity, job_id=value.job_id)
        if isinstance(result, CodingResultStatus) and result.terminal is not None:
            reference = result.terminal.reference
            candidate = value.identity.intent.result.final_candidate
            if (
                reference.repository_id,
                reference.source_branch,
                reference.target_branch,
                reference.head_revision,
            ) != (
                candidate.request.repository_id,
                candidate.authorized_branch,
                value.target_branch,
                candidate.revision,
            ):
                raise ValueError("Terminal result changed original publication pins.")
        return result
    except ValueError, TypeError, AttributeError, RecursionError:
        raise InvalidControlResponse(
            "Control coding result failed validation."
        ) from None


def _consume[Consumed](future: asyncio.Future[Consumed]) -> None:
    if not future.cancelled():
        future.exception()


class HttpControlCodingResultGateway:
    """Use a dedicated factory-owned one-connection worker client in production."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        timeout_seconds: float = 60,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if (
            type(timeout_seconds) not in (int, float)
            or not 0.05 <= timeout_seconds <= 120
            or not isfinite(timeout_seconds)
        ):
            raise ValueError("Coding result transport requires a finite timeout.")
        url = client.base_url
        loopback = url.host == "localhost"
        if not loopback:
            try:
                loopback = ip_address(url.host).is_loopback
            except ValueError:
                pass
        header = client.headers.get("authorization", "")
        if (
            (url.scheme != "https" and not (url.scheme == "http" and loopback))
            or not url.host
            or url.username
            or url.password
            or url.query
            or url.fragment
            or client.params
            or client.cookies
            or client.auth is not None
            or client.trust_env
            or len(client.headers.get_list("authorization")) != 1
            or re.fullmatch(r"Bearer [A-Za-z0-9_-]{43,128}", header) is None
        ):
            raise ValueError(
                "Coding results require a dedicated fixed worker control client."
            )
        self._client, self._url, self._header = client, url, header
        self._timeout, self._now = timeout_seconds, now
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="devrelay-result-wire"
        )
        self._pending: Future[object] | None = None
        self._active = self._closed = False
        self._idle = asyncio.Event()
        self._idle.set()

    def _check(self, authorization: WorkerAuthorization) -> None:
        if (
            self._client.base_url != self._url
            or self._client.params
            or self._client.cookies
            or self._client.auth is not None
            or self._client.trust_env
            or self._client.headers.get_list("authorization") != [self._header]
        ):
            raise ControlClaimRejected(
                "Coding result transport changed original scope."
            )
        current = self._now()
        if (
            not isinstance(current, datetime)
            or current.utcoffset() != timedelta(0)
            or current >= authorization.credential_expires_at
        ):
            raise ControlAccessDenied("Control worker authority has expired.")

    async def _offloop(self, operation: Callable[[], _Value]) -> _Value:
        if self._pending is not None and not self._pending.done():
            raise ControlClaimRejected("A coding result operation is still unsettled.")
        future = self._executor.submit(operation)
        self._pending = cast(Future[object], future)
        wrapped = asyncio.wrap_future(future)
        wrapped.add_done_callback(_consume)
        return await asyncio.shield(wrapped)

    async def submit(
        self,
        request: WorkerCodingResultRequest,
        authorization: WorkerAuthorization,
        *,
        job_id: UUID,
        target_branch: str,
        before_submit: Callable[[], None] | None = None,
    ) -> CodingResultAcceptance:
        result = await self._run(
            request,
            authorization,
            job_id,
            target_branch,
            recovery=False,
            before_submit=before_submit,
        )
        if not isinstance(result, CodingResultAcceptance):
            raise InvalidControlResponse(
                "Control coding result acceptance is unavailable."
            )
        return result

    async def recover(
        self,
        request: WorkerCodingResultRequest,
        authorization: WorkerAuthorization,
        *,
        job_id: UUID,
        target_branch: str,
    ) -> CodingResultStatus | None:
        result = await self._run(
            request, authorization, job_id, target_branch, recovery=True
        )
        if result is not None and not isinstance(result, CodingResultStatus):
            raise InvalidControlResponse(
                "Control coding result recovery is unavailable."
            )
        return result

    async def _run(
        self,
        request: WorkerCodingResultRequest,
        authorization: WorkerAuthorization,
        job_id: UUID,
        target_branch: str,
        *,
        recovery: bool,
        before_submit: Callable[[], None] | None = None,
    ) -> CodingResultAcceptance | CodingResultStatus | None:
        if (
            self._closed
            or self._active
            or (self._pending is not None and not self._pending.done())
        ):
            raise ControlClaimRejected(
                "A coding result exchange is closed or unsettled."
            )
        snapshot = _snapshot(request, authorization, job_id, target_branch)
        self._active = True
        self._idle.clear()
        try:
            async with asyncio.timeout(self._timeout):
                self._check(snapshot.authorization)
                encoded = await self._offloop(lambda: _encode(snapshot))
                self._check(encoded.authorization)
                response = await self._exchange(encoded, recovery, before_submit)
                self._check(encoded.authorization)
                if response is None:
                    return None
                result = await self._offloop(
                    lambda: _decode(response, encoded, recovery)
                )
                self._check(encoded.authorization)
                return result
        except TimeoutError, httpx.RequestError:
            raise ControlUnavailable(
                "Control coding result exchange failed or timed out."
            ) from None
        finally:
            self._active = False
            self._idle.set()
            if self._pending is not None and self._pending.done():
                self._pending = None

    async def _exchange(
        self,
        value: _Encoded,
        recovery: bool,
        before_submit: Callable[[], None] | None,
    ) -> bytes | None:
        attempt_id = value.identity.intent.execution.acknowledgement.attempt_id
        target = self._url.join(
            f"api/v1/worker/attempts/{attempt_id}/results"
            + ("/recovery" if recovery else "")
        )
        self._check(value.authorization)
        if before_submit is not None:
            before_submit()
        async with self._client.stream(
            "POST",
            target,
            content=value.body,
            follow_redirects=False,
            headers={
                "Accept": "application/json",
                "Accept-Encoding": "identity",
                "Content-Type": "application/json",
            },
            timeout=httpx.Timeout(
                self._timeout,
                connect=min(self._timeout, 5),
                write=min(self._timeout, 30),
                pool=min(self._timeout, 1),
            ),
        ) as response:
            self._check(value.authorization)
            if response.request.url != target:
                raise InvalidControlResponse(
                    "Control coding result changed its destination."
                )
            if response.status_code in (401, 403):
                raise ControlAccessDenied("Control rejected worker result access.")
            if response.status_code in (408, 429, 500, 502, 503, 504):
                raise ControlUnavailable("Control coding result is unavailable.")
            if response.status_code in (409, 413, 422):
                raise ControlClaimRejected("Control rejected coding result input.")
            missing = recovery and response.status_code == 404
            if response.status_code != (200 if recovery else 202) and not missing:
                raise InvalidControlResponse(
                    "Control coding result status is unsupported."
                )
            body = await self._read(response, value.authorization)
            if missing:

                def validate_missing() -> None:
                    try:

                        def pairs(items: list[tuple[str, object]]) -> dict[str, object]:
                            result: dict[str, object] = {}
                            for key, item in items:
                                if key in result:
                                    raise ValueError
                                result[key] = item
                            return result

                        data = json.loads(body.decode("utf-8"), object_pairs_hook=pairs)
                        if (
                            type(data) is not dict
                            or set(data) != {"detail"}
                            or type(data["detail"]) is not dict
                            or set(data["detail"]) != {"code", "message"}
                            or data["detail"]["code"] != "coding_result_not_found"
                            or type(data["detail"]["message"]) is not str
                            or len(data["detail"]["message"].encode("utf-8")) > 4096
                        ):
                            raise ValueError
                    except ValueError, TypeError, RecursionError:
                        raise InvalidControlResponse(
                            "Control result-not-found response failed validation."
                        ) from None

                await self._offloop(validate_missing)
                return None
            return body

    async def _read(
        self, response: httpx.Response, authorization: WorkerAuthorization
    ) -> bytes:
        if (
            len(response.headers.get_list("content-type")) != 1
            or response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
            != "application/json"
            or len(response.headers.get_list("content-encoding")) > 1
            or response.headers.get("content-encoding", "identity").strip().lower()
            != "identity"
        ):
            raise InvalidControlResponse(
                "Control coding result response media is unsupported."
            )
        lengths = response.headers.get_list("content-length")
        if len(lengths) > 1 or (
            lengths
            and (
                not lengths[0].isascii()
                or not lengths[0].isdecimal()
                or len(lengths[0]) > 10
                or int(lengths[0]) > MAX_CODING_RESULT_RESPONSE_BYTES
            )
        ):
            raise InvalidControlResponse(
                "Control coding result exceeds response bounds."
            )
        body = bytearray()
        if response.is_stream_consumed:
            if len(response.content) > MAX_CODING_RESULT_RESPONSE_BYTES:
                raise InvalidControlResponse(
                    "Control coding result exceeds response bounds."
                )
            body.extend(response.content)
        else:
            async for chunk in response.aiter_raw():
                self._check(authorization)
                if len(body) + len(chunk) > MAX_CODING_RESULT_RESPONSE_BYTES:
                    raise InvalidControlResponse(
                        "Control coding result exceeds response bounds."
                    )
                body.extend(chunk)
        if lengths and len(body) != int(lengths[0]):
            raise InvalidControlResponse(
                "Control coding result response is incomplete."
            )
        return bytes(body)

    async def aclose(self) -> None:
        self._closed = True
        await self._idle.wait()
        if self._pending is not None:
            wrapped = asyncio.wrap_future(self._pending)
            wrapped.add_done_callback(_consume)
            try:
                await asyncio.shield(wrapped)
            except Exception:
                pass
            self._pending = None
        await asyncio.to_thread(
            self._executor.shutdown, wait=True, cancel_futures=False
        )
        await self._client.aclose()


@asynccontextmanager
async def open_http_control_coding_results(
    control_url: str,
    worker_token: str,
    *,
    timeout_seconds: float = 60,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    transport: httpx.AsyncBaseTransport | None = None,
) -> AsyncIterator[HttpControlCodingResultGateway]:
    if (
        type(worker_token) is not str
        or re.fullmatch(r"[A-Za-z0-9_-]{43,128}", worker_token) is None
    ):
        raise ValueError(
            "Coding result transport requires a separate worker credential."
        )
    async with httpx.AsyncClient(
        base_url=control_url,
        headers={"Authorization": "Bearer " + worker_token},
        trust_env=False,
        follow_redirects=False,
        limits=httpx.Limits(max_connections=1, max_keepalive_connections=1),
        transport=transport
        if transport is not None
        else httpx.AsyncHTTPTransport(
            retries=0,
            trust_env=False,
            limits=httpx.Limits(max_connections=1, max_keepalive_connections=1),
        ),
    ) as client:
        gateway = HttpControlCodingResultGateway(
            client, timeout_seconds=timeout_seconds, now=now
        )
        try:
            yield gateway
        finally:
            await gateway.aclose()
