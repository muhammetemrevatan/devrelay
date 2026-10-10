from http import HTTPStatus
from typing import Annotated

from fastapi import APIRouter, Depends, Response

from devrelay.api.dependencies import require_worker_access
from devrelay.api.errors import error_responses
from devrelay.api.schemas.workers import WorkerIdentityResponse
from devrelay.application.contracts.workers import WorkerAuthorization

router = APIRouter(prefix="/api/v1/worker", tags=["worker"])


@router.get(
    "/identity",
    response_model=WorkerIdentityResponse,
    responses=error_responses(
        HTTPStatus.UNAUTHORIZED,
        HTTPStatus.SERVICE_UNAVAILABLE,
        HTTPStatus.INTERNAL_SERVER_ERROR,
    ),
)
async def worker_identity(
    response: Response,
    authorization: Annotated[WorkerAuthorization, Depends(require_worker_access)],
) -> WorkerIdentityResponse:
    response.headers["Cache-Control"] = "no-store"
    return WorkerIdentityResponse.from_authorization(authorization)
