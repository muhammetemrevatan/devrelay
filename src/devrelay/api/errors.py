from collections.abc import Awaitable, Callable
from enum import StrEnum
from http import HTTPStatus

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, ConfigDict
from starlette.responses import JSONResponse

from devrelay.application.ports.task_persistence import TaskPersistenceError
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


class ApiErrorDetail(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: ApiErrorCode
    message: str


class ApiErrorResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    detail: ApiErrorDetail


class UnsupportedImportMedia(Exception):
    """The import body is not uncompressed JSON."""


class ImportBodyTooLarge(Exception):
    """The import body exceeds the configured size bound."""


class ImportBodyTimeout(Exception):
    """The import body did not arrive before its read deadline."""


class ImportBodyInterrupted(Exception):
    """The client disconnected while the import body was being read."""


class InvalidImportRequest(Exception):
    """The bounded import body does not satisfy its request schema."""


_ERROR_MAP: dict[type[Exception], tuple[int, ApiErrorCode, str]] = {
    UnsupportedImportMedia: (
        415,
        ApiErrorCode.UNSUPPORTED_MEDIA,
        "Uncompressed application/json is required.",
    ),
    ImportBodyTooLarge: (
        413,
        ApiErrorCode.BODY_TOO_LARGE,
        "Import request exceeds the size limit.",
    ),
    ImportBodyTimeout: (
        408,
        ApiErrorCode.BODY_TIMEOUT,
        "Import request body timed out.",
    ),
    ImportBodyInterrupted: (
        400,
        ApiErrorCode.INVALID_REQUEST,
        "Import request was interrupted.",
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
        status_code=status, content={"detail": _error_detail(code, message)}
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
