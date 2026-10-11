import asyncio
import json
from collections.abc import Callable
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import Literal
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from devrelay.application.contracts.acknowledgements import (
    AcknowledgementResult,
    WorkerAcknowledgementRequest,
)
from devrelay.application.contracts.assignment_delivery import (
    MAX_ASSIGNMENT_BYTES,
    AssignmentDelivery,
    InvalidAssignmentDefinition,
)
from devrelay.application.contracts.assignments import NoWorkReason
from devrelay.application.contracts.claims import WorkerClaimRequest
from devrelay.application.contracts.worker_recovery import (
    AssignmentReference,
    ClaimExchange,
    WorkerClaimOutcome,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.control_gateway import (
    ControlAccessDenied,
    ControlClaimRejected,
    ControlUnavailable,
    InvalidControlResponse,
)
from devrelay.infrastructure.control.acknowledgements import AcknowledgementPayload
from devrelay.infrastructure.control.assignments import DeliveryPayload


class IdentityPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)

    protocol_version: int = Field(ge=1, le=1)
    worker_id: UUID
    allowed_repository_ids: tuple[UUID, ...] = Field(min_length=1, max_length=32)
    credential_expires_at: datetime


class ClaimPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)

    protocol_version: int = Field(ge=1, le=1)
    worker_id: UUID
    claim_request_id: UUID
    recorded_at: datetime
    assignment: AssignmentReference | None
    no_work: NoWorkReason | None
    recovered: bool
    lease_authority_active: bool


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    values: dict[str, object] = {}
    for key, value in pairs:
        if key in values:
            raise ValueError("Duplicate response key.")
        values[key] = value
    return values


class HttpControlGateway:
    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        timeout_seconds: float = 60,
        max_response_bytes: int = 16_384,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if not 0 < timeout_seconds <= 60 or not 1 <= max_response_bytes <= 16_384:
            raise ValueError("Control transport requires bounded response limits.")
        self._client = client
        self._timeout = timeout_seconds
        self._max_bytes = max_response_bytes
        self._now = now

    async def identify(self) -> WorkerAuthorization:
        body = await self._exchange("GET", "api/v1/worker/identity")
        try:
            payload = IdentityPayload.model_validate_json(body)
            if len(set(payload.allowed_repository_ids)) != len(
                payload.allowed_repository_ids
            ):
                raise ValueError("Duplicate repository scope.")
            authorization = WorkerAuthorization(
                worker_id=payload.worker_id,
                allowed_repository_ids=frozenset(payload.allowed_repository_ids),
                credential_expires_at=payload.credential_expires_at,
            )
        except ValidationError, ValueError:
            raise InvalidControlResponse(
                "Control identity failed validation."
            ) from None
        if self._now() >= authorization.credential_expires_at:
            raise ControlAccessDenied("Control worker authority has expired.")
        return authorization

    async def claim(
        self, request: WorkerClaimRequest, authorization: WorkerAuthorization
    ) -> ClaimExchange:
        if self._now() >= authorization.credential_expires_at:
            raise ControlAccessDenied("Control worker authority has expired.")
        body = await self._exchange(
            "POST",
            "api/v1/worker/claims",
            payload={
                "protocol_version": request.protocol_version,
                "claim_request_id": str(request.claim_request_id),
                "proof": request.proof.reveal(),
            },
        )
        try:
            payload = ClaimPayload.model_validate_json(body)
            outcome = WorkerClaimOutcome(
                worker_id=payload.worker_id,
                claim_request_id=payload.claim_request_id,
                recorded_at=payload.recorded_at,
                assignment=payload.assignment,
                no_work=payload.no_work,
            )
            if (
                outcome.worker_id != authorization.worker_id
                or outcome.claim_request_id != request.claim_request_id
            ):
                raise ValueError("Uncorrelated claim response.")
            reference = outcome.assignment
            if reference is not None and (
                reference.repository_id not in authorization.allowed_repository_ids
                or reference.expires_at - reference.issued_at > timedelta(seconds=300)
                or (
                    not payload.recovered
                    and reference.expires_at > authorization.credential_expires_at
                )
            ):
                raise ValueError(
                    "Assignment exceeds worker scope or initial lease bounds."
                )
            return ClaimExchange(
                outcome=outcome,
                recovered=payload.recovered,
                lease_authority_active=payload.lease_authority_active,
            )
        except ValidationError, ValueError:
            raise InvalidControlResponse("Control claim failed validation.") from None

    async def _exchange(
        self,
        method: Literal["GET", "POST"],
        path: str,
        *,
        payload: dict[str, object] | None = None,
        max_response_bytes: int | None = None,
    ) -> bytes:
        max_bytes = (
            self._max_bytes if max_response_bytes is None else max_response_bytes
        )
        try:
            async with asyncio.timeout(self._timeout):
                async with self._client.stream(
                    method,
                    path,
                    json=payload,
                    follow_redirects=False,
                    headers={
                        "Accept": "application/json",
                        "Accept-Encoding": "identity",
                    },
                    timeout=httpx.Timeout(self._timeout, connect=5, write=5, pool=1),
                ) as response:
                    if response.status_code in (401, 403):
                        raise ControlAccessDenied("Control rejected worker access.")
                    if response.status_code in (408, 429, 502, 503, 504):
                        raise ControlUnavailable(
                            "Control is unavailable or admission is restricted."
                        )
                    if response.status_code in (409, 422):
                        raise ControlClaimRejected(
                            "Control rejected claim recovery input."
                        )
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
                            "Control response type or status is unsupported."
                        )
                    declared = response.headers.get("content-length")
                    if declared is not None and (
                        not declared.isascii()
                        or not declared.isdecimal()
                        or len(declared) > 10
                        or int(declared) > max_bytes
                    ):
                        raise InvalidControlResponse(
                            "Control response exceeds size bounds."
                        )
                    body = bytearray()
                    async for chunk in response.aiter_bytes():
                        if len(body) + len(chunk) > max_bytes:
                            raise InvalidControlResponse(
                                "Control response exceeds size bounds."
                            )
                        body.extend(chunk)
                    try:
                        if max_response_bytes is not None:
                            await asyncio.to_thread(
                                json.loads, body, object_pairs_hook=_unique_object
                            )
                        else:
                            json.loads(body, object_pairs_hook=_unique_object)
                    except ValueError, RecursionError:
                        raise InvalidControlResponse(
                            "Control response is invalid JSON."
                        ) from None
                    return bytes(body)
        except TimeoutError, httpx.RequestError:
            raise ControlUnavailable("Control request failed or timed out.") from None

    async def read_assignment(
        self,
        attempt_id: UUID,
        request: WorkerClaimRequest,
        authorization: WorkerAuthorization,
    ) -> AssignmentDelivery:
        try:
            async with asyncio.timeout(self._timeout):
                return await self._read_assignment(attempt_id, request, authorization)
        except TimeoutError:
            raise ControlUnavailable("Control assignment read timed out.") from None

    async def _read_assignment(
        self,
        attempt_id: UUID,
        request: WorkerClaimRequest,
        authorization: WorkerAuthorization,
    ) -> AssignmentDelivery:
        if self._now() >= authorization.credential_expires_at:
            raise ControlAccessDenied("Control worker authority has expired.")
        body = await self._exchange(
            "POST",
            f"api/v1/worker/attempts/{attempt_id}/assignment",
            payload={
                "protocol_version": request.protocol_version,
                "claim_request_id": str(request.claim_request_id),
                "proof": request.proof.reveal(),
            },
            max_response_bytes=MAX_ASSIGNMENT_BYTES,
        )

        def parse() -> AssignmentDelivery:
            try:
                delivery = DeliveryPayload.model_validate_json(body).to_delivery()
                reference = delivery.outcome.assignment
                if (
                    reference is None
                    or reference.attempt_id != attempt_id
                    or delivery.outcome.worker_id != authorization.worker_id
                    or delivery.outcome.claim_request_id != request.claim_request_id
                    or reference.repository_id
                    not in authorization.allowed_repository_ids
                    or reference.expires_at - reference.issued_at
                    > timedelta(seconds=300)
                ):
                    raise ValueError("Uncorrelated assignment delivery.")
                return delivery
            except (
                ValidationError,
                ValueError,
                TypeError,
                InvalidAssignmentDefinition,
                RecursionError,
            ):
                raise InvalidControlResponse(
                    "Control assignment failed validation."
                ) from None

        return await asyncio.to_thread(parse)

    async def acknowledge(
        self, request: WorkerAcknowledgementRequest, authorization: WorkerAuthorization
    ) -> AcknowledgementResult:
        if self._now() >= authorization.credential_expires_at:
            raise ControlAccessDenied("Control worker authority has expired.")
        intent = {
            key: str(value) if isinstance(value, UUID) else value
            for key, value in asdict(request.intent).items()
        }
        body = await self._exchange(
            "POST",
            f"api/v1/worker/attempts/{request.intent.attempt_id}/acknowledgements",
            payload={"intent": intent, "proof": request.claim.proof.reveal()},
        )
        try:
            result = AcknowledgementPayload.model_validate_json(body).result
            if (
                result.worker_id != authorization.worker_id
                or result.repository_id not in authorization.allowed_repository_ids
                or result.intent != request.intent
                or (
                    not result.recovered
                    and result.expires_at > authorization.credential_expires_at
                )
            ):
                raise ValueError("Uncorrelated acknowledgement response.")
            return result
        except ValidationError, ValueError:
            raise InvalidControlResponse(
                "Control acknowledgement failed validation."
            ) from None
