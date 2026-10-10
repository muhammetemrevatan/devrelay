from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Annotated

from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from devrelay.api.errors import ApiErrorCode, api_error
from devrelay.application.tasks import TaskImportResult

type ImportOperation = Callable[[str], Awaitable[TaskImportResult]]


@dataclass(slots=True, kw_only=True)
class TaskImportServices:
    import_task: ImportOperation
    verify_token: Callable[[str], bool] = field(repr=False)
    max_concurrent_requests: int
    body_timeout_seconds: float
    _active: int = field(default=0, init=False, repr=False)

    @contextmanager
    def admit_request(self) -> Iterator[None]:
        # Check/increment has no await: admission is atomic on this app's event loop.
        if self._active >= self.max_concurrent_requests:
            raise api_error(
                503,
                ApiErrorCode.BUSY,
                "Task import is busy.",
                headers={"Retry-After": "1"},
            )
        self._active += 1
        try:
            yield
        finally:
            self._active -= 1


bearer = HTTPBearer(
    auto_error=False,
    scheme_name="TaskImportBearer",
    description="Operator token scoped to task import.",
)


async def require_task_import_access(
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
) -> TaskImportServices:
    services = getattr(request.app.state, "task_import_services", None)
    if not isinstance(services, TaskImportServices):
        raise api_error(503, ApiErrorCode.UNAVAILABLE, "Task import is unavailable.")
    if (
        len(request.headers.getlist("authorization")) != 1
        or credentials is None
        or len(credentials.credentials) > 128
        or not services.verify_token(credentials.credentials)
    ):
        raise api_error(
            401,
            ApiErrorCode.UNAUTHORIZED,
            "Valid import authentication is required.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return services
