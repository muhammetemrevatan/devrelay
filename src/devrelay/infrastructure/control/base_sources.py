import asyncio
import json
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from ipaddress import ip_address
from math import isfinite
from secrets import compare_digest

import httpx

from devrelay.application.contracts.assignment_delivery import (
    AssignmentDelivery,
    InvalidAssignmentDefinition,
)
from devrelay.application.contracts.base_source_delivery import (
    MAX_BASE_DELIVERY_BYTES,
    decode_base_delivery,
)
from devrelay.application.contracts.base_sources import (
    BaseSourceRequest,
    BaseSourceSnapshot,
)
from devrelay.application.contracts.claims import LeaseProof, WorkerClaimRequest
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.control_gateway import (
    ControlAccessDenied,
    ControlClaimRejected,
    ControlUnavailable,
    InvalidControlResponse,
)


class HttpControlBaseSource:
    """Worker-only source channel; bootstrap owns TLS and credential configuration.

    Construct off-loop: capturing the accepted definition fingerprint is bounded
    CPU work. This adapter receives no provider credential, URL or Git executable.
    """

    def __init__(
        self,
        client: httpx.AsyncClient,
        claim: WorkerClaimRequest,
        authorization: WorkerAuthorization,
        delivery: AssignmentDelivery,
        *,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        timeout_seconds: float = 90,
    ) -> None:
        if (
            type(timeout_seconds) not in (int, float)
            or not 0.05 <= timeout_seconds <= 120
            or not isfinite(timeout_seconds)
            or not isinstance(claim, WorkerClaimRequest)
            or not isinstance(authorization, WorkerAuthorization)
            or not isinstance(delivery, AssignmentDelivery)
        ):
            raise ValueError("Base source transport requires bounded trusted input.")
        url = client.base_url
        loopback = url.host == "localhost"
        if not loopback:
            try:
                loopback = ip_address(url.host).is_loopback
            except ValueError:
                pass
        if (
            (url.scheme != "https" and not (url.scheme == "http" and loopback))
            or not url.host
            or url.username
            or url.password
            or url.query
            or url.fragment
            or client.params
        ):
            raise ValueError(
                "Base source requires HTTPS or a loopback HTTP control URL."
            )
        reference = delivery.outcome.assignment
        claim.__post_init__()
        authorization.__post_init__()
        delivery.__post_init__()
        if (
            reference is None
            or reference.worker_id != authorization.worker_id
            or reference.repository_id not in authorization.allowed_repository_ids
            or reference.expires_at - reference.issued_at > timedelta(seconds=300)
            or delivery.outcome.claim_request_id != claim.claim_request_id
        ):
            raise ValueError("Base source requires the original scoped assignment.")
        self._client = client
        self._url = url
        self._claim = claim
        self._claim_id = claim.claim_request_id
        self._protocol = claim.protocol_version
        self._proof = claim.proof.reveal()
        self._authorization = authorization
        self._scope = replace(authorization)
        self._delivery = delivery
        self._outcome = replace(delivery.outcome)
        self._reference = replace(reference)
        self._definition_digest = delivery.definition.fingerprint()
        self._source = BaseSourceRequest(
            repository_id=delivery.definition.repository_id,
            provider_repository_id=delivery.definition.provider_repository_id,
            repository_slug=delivery.definition.repository_slug,
            base_revision=delivery.definition.base_revision,
        )
        self._now = now
        self._timeout = timeout_seconds
        self._reading = False

    def _check(self, request: BaseSourceRequest) -> None:
        try:
            if not isinstance(request, BaseSourceRequest):
                raise ValueError("Missing source input.")
            request.__post_init__()
            self._claim.__post_init__()
            self._authorization.__post_init__()
            self._delivery.outcome.__post_init__()
            reference = self._delivery.outcome.assignment
            if reference is None:
                raise ValueError("Missing original assignment.")
            reference.__post_init__()
        except ValueError, TypeError:
            raise ControlClaimRejected(
                "Base source input changed original scope."
            ) from None
        if (
            request != self._source
            or self._client.base_url != self._url
            or self._client.params
            or self._authorization != self._scope
            or self._delivery.outcome != self._outcome
            or self._delivery.outcome.assignment != self._reference
            or self._claim.claim_request_id != self._claim_id
            or self._claim.protocol_version != self._protocol
            or type(self._claim.protocol_version) is not int
            or not isinstance(self._claim.proof, LeaseProof)
            or not compare_digest(self._claim.proof.reveal(), self._proof)
        ):
            raise ControlClaimRejected("Base source input changed original scope.")
        current = self._now()
        if (
            not isinstance(current, datetime)
            or current.utcoffset() != timedelta(0)
            or current >= self._scope.credential_expires_at
        ):
            raise ControlAccessDenied("Control worker authority has expired.")

    def _check_definition(self) -> None:
        try:
            self._delivery.__post_init__()
            self._delivery.definition.__post_init__()
            if not compare_digest(
                self._delivery.definition.fingerprint(), self._definition_digest
            ):
                raise ValueError("Changed definition.")
        except ValueError, TypeError, InvalidAssignmentDefinition:
            raise ControlClaimRejected(
                "Base source definition changed original pins."
            ) from None

    async def read(self, request: BaseSourceRequest) -> BaseSourceSnapshot:
        self._check(request)
        if self._reading:
            raise ControlClaimRejected("A base source exchange is already unsettled.")
        self._reading = True
        try:
            async with asyncio.timeout(self._timeout):
                await asyncio.to_thread(self._check_definition)
                self._check(request)
                body = await self._exchange(request)
                self._check(request)

                def parse() -> BaseSourceSnapshot:
                    try:
                        value = decode_base_delivery(body)
                        if (
                            value.reference != self._reference
                            or value.claim_request_id != self._claim_id
                            or not compare_digest(
                                value.definition_digest, self._definition_digest
                            )
                            or value.snapshot.request != self._source
                        ):
                            raise ValueError("Uncorrelated base delivery.")
                        return value.snapshot
                    except ValueError, TypeError, RecursionError:
                        raise InvalidControlResponse(
                            "Control base source failed validation."
                        ) from None

                snapshot = await asyncio.to_thread(parse)
                self._check(request)
                await asyncio.to_thread(self._check_definition)
                self._check(request)
                return snapshot
        except TimeoutError, httpx.RequestError:
            raise ControlUnavailable(
                "Control base source failed or timed out."
            ) from None
        finally:
            self._reading = False

    async def _exchange(self, request: BaseSourceRequest) -> bytes:
        path = f"api/v1/worker/attempts/{self._reference.attempt_id}/base-source"
        target = self._url.join(path)
        payload = json.dumps(
            {
                "protocol_version": self._protocol,
                "claim_request_id": str(self._claim_id),
                "proof": self._proof,
            },
            separators=(",", ":"),
        ).encode("ascii")
        if len(payload) > 1024:
            raise ControlClaimRejected("Base source input exceeds request bounds.")
        async with self._client.stream(
            "POST",
            target,
            content=payload,
            follow_redirects=False,
            headers={
                "Accept": "application/json",
                "Accept-Encoding": "identity",
                "Content-Type": "application/json",
            },
            timeout=httpx.Timeout(self._timeout, connect=5, write=5, pool=1),
        ) as response:
            self._check(request)
            if response.request.url != target:
                raise InvalidControlResponse(
                    "Control base source changed its destination."
                )
            if response.status_code in (401, 403):
                raise ControlAccessDenied("Control rejected worker source access.")
            if response.status_code in (408, 429, 502, 503, 504):
                raise ControlUnavailable(
                    "Control source is unavailable or admission is restricted."
                )
            if response.status_code in (409, 422):
                raise ControlClaimRejected("Control rejected base source input.")
            if (
                response.status_code != 200
                or response.headers.get("content-type", "")
                .split(";", 1)[0]
                .strip()
                .lower()
                != "application/json"
                or response.headers.get("content-encoding", "identity").lower()
                != "identity"
            ):
                raise InvalidControlResponse(
                    "Control source type or status is unsupported."
                )
            declared = response.headers.get("content-length")
            if declared is not None and (
                not declared.isascii()
                or not declared.isdecimal()
                or len(declared) > 10
                or int(declared) > MAX_BASE_DELIVERY_BYTES
            ):
                raise InvalidControlResponse(
                    "Control source exceeds encoded byte bounds."
                )
            body = bytearray()
            if response.is_stream_consumed:
                # Mock/in-process transports may supply an already buffered body.
                if len(response.content) > MAX_BASE_DELIVERY_BYTES:
                    raise InvalidControlResponse(
                        "Control source exceeds encoded byte bounds."
                    )
                body.extend(response.content)
            else:
                async for chunk in response.aiter_raw():
                    self._check(request)
                    if len(body) + len(chunk) > MAX_BASE_DELIVERY_BYTES:
                        raise InvalidControlResponse(
                            "Control source exceeds encoded byte bounds."
                        )
                    body.extend(chunk)
            self._check(request)
            if declared is not None and len(body) != int(declared):
                raise InvalidControlResponse("Control source length is inconsistent.")
            return bytes(body)
