import asyncio
from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Request, Response

from devrelay.api.dependencies import (
    WorkerClaimServices,
    require_worker_access,
    require_worker_claim_services,
)
from devrelay.api.errors import InvalidWorkerClaimRequest, error_responses
from devrelay.api.request_body import json_body_schema, read_json_request
from devrelay.api.schemas.claims import (
    AssignmentDeliveryResponse,
    WorkerClaimPayload,
    WorkerClaimResponse,
)
from devrelay.application.contracts.assignment_delivery import AssignmentUnavailable
from devrelay.application.contracts.workers import WorkerAuthorization

router = APIRouter(prefix="/api/v1/worker", tags=["worker"])


@router.post(
    "/claims",
    response_model=WorkerClaimResponse,
    responses=error_responses(
        HTTPStatus.UNAUTHORIZED, HTTPStatus.INTERNAL_SERVER_ERROR
    ),
    openapi_extra=json_body_schema(WorkerClaimPayload),
)
async def claim_job(
    request: Request,
    response: Response,
    authorization: Annotated[WorkerAuthorization, Depends(require_worker_access)],
    services: Annotated[WorkerClaimServices, Depends(require_worker_claim_services)],
) -> WorkerClaimResponse:
    with services.admit_request():
        payload = await read_json_request(
            request,
            services.body_timeout_seconds,
            WorkerClaimPayload,
            InvalidWorkerClaimRequest,
        )
        result = await services.claim_job(payload.to_request(), authorization)
        value = WorkerClaimResponse.from_result(
            result,
            worker_id=authorization.worker_id,
            claim_request_id=payload.claim_request_id,
        )
    response.headers["Cache-Control"] = "no-store"
    return value


@router.post(
    "/attempts/{attempt_id}/assignment",
    response_model=AssignmentDeliveryResponse,
    responses=error_responses(
        HTTPStatus.UNAUTHORIZED, HTTPStatus.INTERNAL_SERVER_ERROR
    ),
    openapi_extra=json_body_schema(WorkerClaimPayload),
)
async def read_assignment(
    attempt_id: UUID,
    request: Request,
    authorization: Annotated[WorkerAuthorization, Depends(require_worker_access)],
    services: Annotated[WorkerClaimServices, Depends(require_worker_claim_services)],
) -> Response:
    with services.admit_request():
        payload = await read_json_request(
            request,
            services.body_timeout_seconds,
            WorkerClaimPayload,
            InvalidWorkerClaimRequest,
        )
        claim_request = payload.to_request()
        if services.read_assignment is None:
            raise AssignmentUnavailable("Assignment reader is unavailable.")
        delivery = await services.read_assignment(
            attempt_id, claim_request, authorization
        )
        return await asyncio.to_thread(AssignmentDeliveryResponse.render, delivery)
