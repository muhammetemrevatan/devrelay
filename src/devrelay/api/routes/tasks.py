from http import HTTPStatus
from typing import Annotated

from fastapi import APIRouter, Depends, Request, Response

from devrelay.api.dependencies import TaskImportServices, require_task_import_access
from devrelay.api.errors import (
    InvalidImportRequest,
    error_responses,
)
from devrelay.api.request_body import json_body_schema, read_json_request
from devrelay.api.schemas.tasks import TaskImportRequest, TaskImportResponse

router = APIRouter(prefix="/api/v1/tasks", tags=["tasks"])


async def read_import_request(request: Request, timeout: float) -> TaskImportRequest:
    return await read_json_request(
        request, timeout, TaskImportRequest, InvalidImportRequest
    )


@router.post(
    "/import",
    response_model=TaskImportResponse,
    responses={
        **error_responses(HTTPStatus.UNAUTHORIZED, HTTPStatus.INTERNAL_SERVER_ERROR),
        201: {"model": TaskImportResponse, "description": "New content captured."},
    },
    openapi_extra=json_body_schema(TaskImportRequest),
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
