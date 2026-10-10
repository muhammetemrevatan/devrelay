import asyncio
from http import HTTPStatus
from typing import Annotated

from fastapi import APIRouter, Depends, Request, Response
from pydantic import ValidationError
from starlette.requests import ClientDisconnect

from devrelay.api.dependencies import TaskImportServices, require_task_import_access
from devrelay.api.errors import (
    ImportBodyInterrupted,
    ImportBodyTimeout,
    ImportBodyTooLarge,
    InvalidImportRequest,
    UnsupportedImportMedia,
    error_responses,
)
from devrelay.api.schemas.tasks import TaskImportRequest, TaskImportResponse

router = APIRouter(prefix="/api/v1/tasks", tags=["tasks"])
MAX_BODY_BYTES = 1024


async def read_import_request(request: Request, timeout: float) -> TaskImportRequest:
    if (
        request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
        != "application/json"
        or request.headers.get("content-encoding", "identity").lower() != "identity"
    ):
        raise UnsupportedImportMedia
    body = bytearray()
    try:
        async with asyncio.timeout(timeout):
            async for chunk in request.stream():
                if len(body) + len(chunk) > MAX_BODY_BYTES:
                    raise ImportBodyTooLarge
                body.extend(chunk)
    except TimeoutError:
        raise ImportBodyTimeout from None
    except ClientDisconnect:
        raise ImportBodyInterrupted from None
    try:
        return TaskImportRequest.model_validate_json(body)
    except ValidationError:
        raise InvalidImportRequest from None


@router.post(
    "/import",
    response_model=TaskImportResponse,
    responses={
        **error_responses(HTTPStatus.UNAUTHORIZED, HTTPStatus.INTERNAL_SERVER_ERROR),
        201: {"model": TaskImportResponse, "description": "New content captured."},
    },
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {
                "application/json": {"schema": TaskImportRequest.model_json_schema()}
            },
        }
    },
)
async def import_task(
    request: Request,
    response: Response,
    services: Annotated[TaskImportServices, Depends(require_task_import_access)],
) -> TaskImportResponse:
    with services.admit_request():
        payload = await read_import_request(request, services.body_timeout_seconds)
        result = await services.import_task(payload.task_key)
    response.status_code = 201 if result.saved.created else 200
    response.headers["Cache-Control"] = "no-store"
    return TaskImportResponse.from_result(result)
