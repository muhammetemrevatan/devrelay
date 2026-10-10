from http import HTTPStatus
from typing import Annotated

from fastapi import APIRouter, Depends, Request, Response

from devrelay.api.dependencies import (
    JobSubmissionServices,
    require_job_submission_access,
)
from devrelay.api.errors import InvalidSubmissionRequest, error_responses
from devrelay.api.request_body import json_body_schema, read_json_request
from devrelay.api.schemas.jobs import JobSubmissionRequest, JobSubmissionResponse

router = APIRouter(prefix="/api/v1/jobs", tags=["jobs"])


@router.post(
    "",
    response_model=JobSubmissionResponse,
    responses={
        **error_responses(HTTPStatus.UNAUTHORIZED, HTTPStatus.INTERNAL_SERVER_ERROR),
        201: {
            "model": JobSubmissionResponse,
            "description": "New queued Job recorded.",
        },
    },
    openapi_extra=json_body_schema(JobSubmissionRequest),
)
async def submit_job(
    request: Request,
    response: Response,
    services: Annotated[JobSubmissionServices, Depends(require_job_submission_access)],
) -> JobSubmissionResponse:
    with services.admit_request():
        payload = await read_json_request(
            request,
            services.body_timeout_seconds,
            JobSubmissionRequest,
            InvalidSubmissionRequest,
        )
        result = await services.submit_job(
            snapshot_id=payload.snapshot_id,
            repository_id=payload.repository_id,
            authorization=services.authorization,
        )
    response.status_code = 201 if result.created else 200
    response.headers["Cache-Control"] = "no-store"
    return JobSubmissionResponse.from_result(result)
