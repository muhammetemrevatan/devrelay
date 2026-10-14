import asyncio
from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Request, Response

from devrelay.api.dependencies import (
    WorkerBaseSourceServices,
    require_worker_access,
    require_worker_base_source_services,
)
from devrelay.api.errors import InvalidWorkerClaimRequest, error_responses
from devrelay.api.request_body import json_body_schema, read_json_request
from devrelay.api.schemas.base_sources import BaseSourceDeliveryResponse
from devrelay.api.schemas.claims import WorkerClaimPayload
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.base_source import BaseSourceUnavailable

router = APIRouter(prefix="/api/v1/worker", tags=["worker"])


@router.post(
    "/attempts/{attempt_id}/base-source",
    responses=error_responses(
        HTTPStatus.UNAUTHORIZED, HTTPStatus.INTERNAL_SERVER_ERROR
    ),
    openapi_extra=json_body_schema(WorkerClaimPayload),
)
async def read_base_source(
    attempt_id: UUID,
    request: Request,
    authorization: Annotated[WorkerAuthorization, Depends(require_worker_access)],
    services: Annotated[
        WorkerBaseSourceServices, Depends(require_worker_base_source_services)
    ],
) -> Response:
    with services.admit_request():
        try:
            async with asyncio.timeout(services.request_timeout_seconds):
                payload = await read_json_request(
                    request,
                    services.body_timeout_seconds,
                    WorkerClaimPayload,
                    InvalidWorkerClaimRequest,
                )
                claim_request = payload.to_request()
                delivery = await services.read_base(
                    attempt_id, claim_request, authorization
                )
                body = await services.encode(
                    delivery, BaseSourceDeliveryResponse.encode
                )
                await services.verify_base(
                    attempt_id, claim_request, authorization, delivery
                )
                return Response(
                    body,
                    media_type="application/json",
                    headers={"Cache-Control": "no-store"},
                )
        except TimeoutError:
            raise BaseSourceUnavailable(
                "Base source delivery deadline elapsed."
            ) from None
