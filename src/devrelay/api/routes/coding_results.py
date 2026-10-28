import asyncio
from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Request, Response
from starlette.requests import ClientDisconnect

from devrelay.api.dependencies import (
    WorkerCodingResultServices,
    require_worker_access,
    require_worker_coding_result_services,
)
from devrelay.api.errors import (
    ApiErrorCode,
    RequestBodyInterrupted,
    RequestBodyTimeout,
    RequestBodyTooLarge,
    UnsupportedRequestMedia,
    api_error,
    error_responses,
)
from devrelay.api.schemas.coding_results import (
    CodingResultAcceptanceResponse,
    CodingResultStatusResponse,
)
from devrelay.application.contracts.coding_result_receipts import (
    CodingResultUnavailable,
)
from devrelay.application.contracts.coding_result_requests import (
    MAX_CODING_RESULT_REQUEST_BYTES,
)
from devrelay.application.contracts.workers import WorkerAuthorization

router = APIRouter(prefix="/api/v1/worker", tags=["worker"])
_BODY_SCHEMA = {
    "requestBody": {
        "required": True,
        "content": {
            "application/json": {
                "schema": {
                    "type": "object",
                    "description": "Original bounded v1 coding result request.",
                }
            }
        },
    }
}


async def read_coding_result_body(request: Request, timeout: float) -> bytes:
    if (
        len(request.headers.getlist("content-type")) != 1
        or request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
        != "application/json"
        or len(request.headers.getlist("content-encoding")) > 1
        or request.headers.get("content-encoding", "identity").strip().lower()
        != "identity"
    ):
        raise UnsupportedRequestMedia
    lengths = request.headers.getlist("content-length")
    if len(lengths) > 1:
        raise RequestBodyInterrupted
    if lengths:
        length = lengths[0]
        if not length.isascii() or not length.isdecimal() or len(length) > 20:
            raise RequestBodyInterrupted
        if int(length) > MAX_CODING_RESULT_REQUEST_BYTES:
            raise RequestBodyTooLarge
    body = bytearray()
    try:
        async with asyncio.timeout(timeout):
            async for chunk in request.stream():
                if len(body) + len(chunk) > MAX_CODING_RESULT_REQUEST_BYTES:
                    raise RequestBodyTooLarge
                body.extend(chunk)
    except TimeoutError:
        raise RequestBodyTimeout from None
    except ClientDisconnect:
        raise RequestBodyInterrupted from None
    if lengths and len(body) != int(lengths[0]):
        raise RequestBodyInterrupted
    return bytes(body)


@router.post(
    "/attempts/{attempt_id}/results",
    status_code=202,
    response_model=CodingResultAcceptanceResponse,
    responses=error_responses(
        HTTPStatus.UNAUTHORIZED, HTTPStatus.INTERNAL_SERVER_ERROR
    ),
    openapi_extra=_BODY_SCHEMA,
)
async def accept_coding_result(
    attempt_id: UUID,
    request: Request,
    response: Response,
    authorization: Annotated[WorkerAuthorization, Depends(require_worker_access)],
    services: Annotated[
        WorkerCodingResultServices, Depends(require_worker_coding_result_services)
    ],
) -> CodingResultAcceptanceResponse:
    with services.admit_request():
        try:
            async with asyncio.timeout(services.request_timeout_seconds):
                body = await read_coding_result_body(
                    request, services.body_timeout_seconds
                )
                value = await services.decode(body, attempt_id=attempt_id)
                result = await services.accept_result(value, authorization)
                rendered = CodingResultAcceptanceResponse.from_result(result)
        except TimeoutError:
            raise CodingResultUnavailable("Coding result deadline elapsed.") from None
    response.headers["Cache-Control"] = "no-store"
    return rendered


@router.post(
    "/attempts/{attempt_id}/results/recovery",
    response_model=CodingResultStatusResponse,
    responses=error_responses(
        HTTPStatus.UNAUTHORIZED, HTTPStatus.INTERNAL_SERVER_ERROR
    ),
    openapi_extra=_BODY_SCHEMA,
)
async def recover_coding_result(
    attempt_id: UUID,
    request: Request,
    response: Response,
    authorization: Annotated[WorkerAuthorization, Depends(require_worker_access)],
    services: Annotated[
        WorkerCodingResultServices, Depends(require_worker_coding_result_services)
    ],
) -> CodingResultStatusResponse:
    with services.admit_request():
        try:
            async with asyncio.timeout(services.request_timeout_seconds):
                body = await read_coding_result_body(
                    request, services.body_timeout_seconds
                )
                value = await services.decode(body, attempt_id=attempt_id)
                result = await services.recover_result(value, authorization)
                if result is None:
                    raise api_error(
                        404,
                        ApiErrorCode.CODING_RESULT_NOT_FOUND,
                        "Coding result receipt is unavailable.",
                        headers={"Cache-Control": "no-store"},
                    )
                rendered = CodingResultStatusResponse.from_result(result)
        except TimeoutError:
            raise CodingResultUnavailable("Coding result deadline elapsed.") from None
    response.headers["Cache-Control"] = "no-store"
    return rendered
