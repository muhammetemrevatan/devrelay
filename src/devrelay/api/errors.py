from collections.abc import Awaitable, Callable
from enum import StrEnum
from http import HTTPStatus

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, ConfigDict
from starlette.responses import JSONResponse

from devrelay.application.contracts.acknowledgements import AcknowledgementRejected
from devrelay.application.contracts.assignment_delivery import (
    AssignmentUnavailable,
    InvalidAssignmentDefinition,
)
from devrelay.application.contracts.assignments import (
    ClaimAdmissionRejected,
    ClaimPersistenceError,
    ClaimScopeRejected,
)
from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    InvalidClaimRequest,
    InvalidLeaseProof,
)
from devrelay.application.contracts.eligibility import (
    ClaimEligibilityRejected,
    WorkerAuthorityExpired,
)
from devrelay.application.contracts.heartbeats import (
    HeartbeatAdmissionRejected,
    HeartbeatRejected,
)
from devrelay.application.jobs import (
    JobSubmissionConflict,
    JobSubmissionRejected,
    TaskNotReady,
    TaskRequirementsChanged,
)
from devrelay.application.ports.job_persistence import JobPersistenceError
from devrelay.application.ports.repository_source import (
    InvalidRepositorySourceData,
    RepositorySourceAccessDenied,
    RepositorySourceError,
    RepositorySourceUnavailable,
)
from devrelay.application.ports.task_persistence import (
    TaskPersistenceError,
    TaskSnapshotNotFound,
)
from devrelay.application.ports.task_source import (
    InvalidTaskSourceData,
    TaskNotFound,
    TaskSourceAccessDenied,
    TaskSourceError,
    TaskSourceScopeDenied,
    TaskSourceUnavailable,
)
from devrelay.application.tasks import TaskImportRejected


class ApiErrorCode(StrEnum):
    UNAUTHORIZED = "unauthorized"
    WORKER_UNAVAILABLE = "worker_unavailable"
    UNAVAILABLE = "import_unavailable"
    BUSY = "import_busy"
    INVALID_REQUEST = "invalid_request"
    BODY_TOO_LARGE = "body_too_large"
    BODY_TIMEOUT = "body_timeout"
    UNSUPPORTED_MEDIA = "unsupported_media_type"
    TASK_NOT_FOUND = "task_not_found"
    IMPORT_REJECTED = "import_rejected"
    SOURCE_ACCESS_DENIED = "source_access_denied"
    SOURCE_UNAVAILABLE = "source_unavailable"
    INVALID_SOURCE = "invalid_source_data"
    STORAGE_UNAVAILABLE = "storage_unavailable"
    INTERNAL_ERROR = "internal_error"
    SUBMISSION_UNAVAILABLE = "submission_unavailable"
    SUBMISSION_BUSY = "submission_busy"
    SUBMISSION_REJECTED = "submission_rejected"
    SUBMISSION_CONFLICT = "submission_conflict"
    TASK_NOT_READY = "task_not_ready"
    TASK_REQUIREMENTS_CHANGED = "task_requirements_changed"
    REPOSITORY_ACCESS_DENIED = "repository_access_denied"
    REPOSITORY_UNAVAILABLE = "repository_unavailable"
    INVALID_REPOSITORY_DATA = "invalid_repository_data"
    SNAPSHOT_NOT_FOUND = "snapshot_not_found"
    CLAIM_UNAVAILABLE = "claim_unavailable"
    CLAIM_BUSY = "claim_busy"
    CLAIM_ADMISSION_REJECTED = "claim_admission_rejected"
    CLAIM_SCOPE_REJECTED = "claim_scope_rejected"
    CLAIM_CONFLICT = "claim_conflict"
    ASSIGNMENT_UNAVAILABLE = "assignment_unavailable"
    ACKNOWLEDGEMENT_REJECTED = "acknowledgement_rejected"
    HEARTBEAT_REJECTED = "heartbeat_rejected"
    HEARTBEAT_ADMISSION_REJECTED = "heartbeat_admission_rejected"


class ApiErrorDetail(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: ApiErrorCode
    message: str


class ApiErrorResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    detail: ApiErrorDetail


class UnsupportedRequestMedia(Exception):
    """The request body is not uncompressed JSON."""


class RequestBodyTooLarge(Exception):
    """The request body exceeds the configured size bound."""


class RequestBodyTimeout(Exception):
    """The request body did not arrive before its read deadline."""


class RequestBodyInterrupted(Exception):
    """The client disconnected while the request body was being read."""


class InvalidImportRequest(Exception):
    """The bounded import body does not satisfy its request schema."""


class InvalidSubmissionRequest(Exception):
    """The bounded submission body does not satisfy its request schema."""


class InvalidWorkerClaimRequest(Exception):
    """The bounded claim body does not satisfy its request schema."""


_ERROR_MAP: dict[type[Exception], tuple[int, ApiErrorCode, str]] = {
    HeartbeatRejected: (
        409,
        ApiErrorCode.HEARTBEAT_REJECTED,
        "Assignment heartbeat was rejected.",
    ),
    HeartbeatAdmissionRejected: (
        429,
        ApiErrorCode.HEARTBEAT_ADMISSION_REJECTED,
        "New heartbeat admission is restricted.",
    ),
    AcknowledgementRejected: (
        409,
        ApiErrorCode.ACKNOWLEDGEMENT_REJECTED,
        "Assignment acknowledgement was rejected.",
    ),
    RequestValidationError: (
        422,
        ApiErrorCode.INVALID_REQUEST,
        "Request parameters are invalid.",
    ),
    WorkerAuthorityExpired: (
        401,
        ApiErrorCode.UNAUTHORIZED,
        "Valid worker authentication is required.",
    ),
    ClaimEligibilityRejected: (
        409,
        ApiErrorCode.CLAIM_CONFLICT,
        "Queued work is not eligible for assignment.",
    ),
    AssignmentUnavailable: (
        404,
        ApiErrorCode.ASSIGNMENT_UNAVAILABLE,
        "Assigned receipt is unavailable.",
    ),
    InvalidAssignmentDefinition: (
        503,
        ApiErrorCode.ASSIGNMENT_UNAVAILABLE,
        "Assignment delivery is unavailable.",
    ),
    InvalidWorkerClaimRequest: (
        422,
        ApiErrorCode.INVALID_REQUEST,
        "Provide only a valid claim request.",
    ),
    InvalidClaimRequest: (
        422,
        ApiErrorCode.INVALID_REQUEST,
        "Claim protocol or identity is invalid.",
    ),
    InvalidLeaseProof: (
        422,
        ApiErrorCode.INVALID_REQUEST,
        "An independent canonical lease proof is required.",
    ),
    ClaimReplayConflict: (
        409,
        ApiErrorCode.CLAIM_CONFLICT,
        "Claim input conflicts with recorded recovery.",
    ),
    ClaimAdmissionRejected: (
        429,
        ApiErrorCode.CLAIM_ADMISSION_REJECTED,
        "New claim admission is restricted.",
    ),
    ClaimScopeRejected: (
        403,
        ApiErrorCode.CLAIM_SCOPE_REJECTED,
        "Claim is outside current worker authority.",
    ),
    ClaimPersistenceError: (
        503,
        ApiErrorCode.STORAGE_UNAVAILABLE,
        "Claim storage is unavailable.",
    ),
    UnsupportedRequestMedia: (
        415,
        ApiErrorCode.UNSUPPORTED_MEDIA,
        "Uncompressed application/json is required.",
    ),
    RequestBodyTooLarge: (
        413,
        ApiErrorCode.BODY_TOO_LARGE,
        "Request exceeds the size limit.",
    ),
    RequestBodyTimeout: (
        408,
        ApiErrorCode.BODY_TIMEOUT,
        "Request body timed out.",
    ),
    RequestBodyInterrupted: (
        400,
        ApiErrorCode.INVALID_REQUEST,
        "Request was interrupted.",
    ),
    InvalidImportRequest: (
        422,
        ApiErrorCode.INVALID_REQUEST,
        "Provide only a valid task_key.",
    ),
    TaskImportRejected: (
        403,
        ApiErrorCode.IMPORT_REJECTED,
        "Task is outside the import scope.",
    ),
    TaskSourceScopeDenied: (
        403,
        ApiErrorCode.IMPORT_REJECTED,
        "Task is outside the import scope.",
    ),
    TaskNotFound: (
        404,
        ApiErrorCode.TASK_NOT_FOUND,
        "Task was not found in the configured scope.",
    ),
    TaskSourceAccessDenied: (
        502,
        ApiErrorCode.SOURCE_ACCESS_DENIED,
        "Task source access failed.",
    ),
    TaskSourceUnavailable: (
        503,
        ApiErrorCode.SOURCE_UNAVAILABLE,
        "Task source is unavailable.",
    ),
    InvalidTaskSourceData: (
        502,
        ApiErrorCode.INVALID_SOURCE,
        "Task source data could not be imported.",
    ),
    TaskSourceError: (502, ApiErrorCode.INVALID_SOURCE, "Task source read failed."),
    TaskPersistenceError: (
        503,
        ApiErrorCode.STORAGE_UNAVAILABLE,
        "Task storage is unavailable.",
    ),
    InvalidSubmissionRequest: (
        422,
        ApiErrorCode.INVALID_REQUEST,
        "Provide only valid snapshot_id and repository_id values.",
    ),
    JobSubmissionRejected: (
        403,
        ApiErrorCode.SUBMISSION_REJECTED,
        "Job submission is outside the allowed scope.",
    ),
    JobSubmissionConflict: (
        409,
        ApiErrorCode.SUBMISSION_CONFLICT,
        "Job submission conflicts with an existing delivery.",
    ),
    TaskNotReady: (
        409,
        ApiErrorCode.TASK_NOT_READY,
        "Task is not ready for submission.",
    ),
    TaskRequirementsChanged: (
        409,
        ApiErrorCode.TASK_REQUIREMENTS_CHANGED,
        "Task requirements changed; import them again.",
    ),
    RepositorySourceAccessDenied: (
        502,
        ApiErrorCode.REPOSITORY_ACCESS_DENIED,
        "Repository access failed.",
    ),
    RepositorySourceUnavailable: (
        503,
        ApiErrorCode.REPOSITORY_UNAVAILABLE,
        "Repository observation is unavailable.",
    ),
    InvalidRepositorySourceData: (
        502,
        ApiErrorCode.INVALID_REPOSITORY_DATA,
        "Repository observation could not be verified.",
    ),
    RepositorySourceError: (
        502,
        ApiErrorCode.INVALID_REPOSITORY_DATA,
        "Repository observation failed.",
    ),
    JobPersistenceError: (
        503,
        ApiErrorCode.STORAGE_UNAVAILABLE,
        "Job storage is unavailable.",
    ),
    TaskSnapshotNotFound: (
        404,
        ApiErrorCode.SNAPSHOT_NOT_FOUND,
        "Task snapshot was not found.",
    ),
}


def _error_detail(code: ApiErrorCode, message: str) -> dict[str, str]:
    return {"code": code, "message": message}


def api_error(
    status: int,
    code: ApiErrorCode,
    message: str,
    *,
    headers: dict[str, str] | None = None,
) -> HTTPException:
    return HTTPException(
        status_code=status, detail=_error_detail(code, message), headers=headers
    )


def api_error_response(
    status: int,
    code: ApiErrorCode,
    message: str,
) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={"detail": _error_detail(code, message)},
        headers={
            "Cache-Control": "no-store",
            **({"WWW-Authenticate": "Bearer"} if status == 401 else {}),
        },
    )


def _make_error_handler(
    status: int, code: ApiErrorCode, message: str
) -> Callable[[Request, Exception], Awaitable[JSONResponse]]:
    async def handler(request: Request, error: Exception) -> JSONResponse:
        return api_error_response(status, code, message)

    return handler


def register_error_handlers(app: FastAPI) -> None:
    """Translate classified API/application errors using exception MRO.

    UnexpectedErrorMiddleware sanitizes unhandled HTTP failures. Registering
    Exception here would let ServerErrorMiddleware re-raise arbitrary error values
    for the server to log, even after sending a sanitized response.
    """
    for error_type, (status, code, message) in _ERROR_MAP.items():
        app.add_exception_handler(
            error_type, _make_error_handler(status, code, message)
        )


def error_responses(
    *additional_statuses: HTTPStatus,
) -> dict[int | str, dict[str, object]]:
    """Document the central registry plus endpoint-specific transport failures."""
    statuses = {status for status, _, _ in _ERROR_MAP.values()}
    statuses.update(additional_statuses)
    return {
        status: {"model": ApiErrorResponse, "description": HTTPStatus(status).phrase}
        for status in sorted(statuses)
    }
