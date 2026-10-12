from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Annotated, Protocol
from uuid import UUID

from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from devrelay.api.errors import ApiErrorCode, api_error
from devrelay.application.contracts.acknowledgements import (
    AcknowledgementResult,
    WorkerAcknowledgementRequest,
)
from devrelay.application.contracts.assignment_delivery import AssignmentDelivery
from devrelay.application.contracts.assignments import ClaimResult
from devrelay.application.contracts.claims import WorkerClaimRequest
from devrelay.application.contracts.heartbeats import HeartbeatRequest, HeartbeatResult
from devrelay.application.contracts.submissions import (
    SavedJobSubmission,
    SubmissionAuthorization,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.tasks import TaskImportResult

type ImportOperation = Callable[[str], Awaitable[TaskImportResult]]


class SubmissionOperation(Protocol):
    async def __call__(
        self,
        *,
        snapshot_id: UUID,
        repository_id: UUID,
        authorization: SubmissionAuthorization,
    ) -> SavedJobSubmission: ...


type ClaimOperation = Callable[
    [WorkerClaimRequest, WorkerAuthorization], Awaitable[ClaimResult]
]
type AssignmentOperation = Callable[
    [UUID, WorkerClaimRequest, WorkerAuthorization], Awaitable[AssignmentDelivery]
]
type AcknowledgementOperation = Callable[
    [WorkerAcknowledgementRequest, WorkerAuthorization],
    Awaitable[AcknowledgementResult],
]
type HeartbeatOperation = Callable[
    [HeartbeatRequest, WorkerAuthorization], Awaitable[HeartbeatResult]
]


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerClaimOperations:
    claim: ClaimOperation
    read_assignment: AssignmentOperation
    acknowledge: AcknowledgementOperation | None = None
    heartbeat: HeartbeatOperation | None = None


@dataclass(slots=True, kw_only=True)
class RequestAdmission:
    max_concurrent_requests: int
    body_timeout_seconds: float
    busy_code: ApiErrorCode = ApiErrorCode.BUSY
    busy_message: str = "Task import is busy."
    _active: int = field(default=0, init=False, repr=False)

    @contextmanager
    def admit_request(self) -> Iterator[None]:
        # Check/increment has no await: admission is atomic on this app's event loop.
        if self._active >= self.max_concurrent_requests:
            raise api_error(
                503,
                self.busy_code,
                self.busy_message,
                headers={"Retry-After": "1"},
            )
        self._active += 1
        try:
            yield
        finally:
            self._active -= 1


@dataclass(slots=True, kw_only=True)
class TaskImportServices(RequestAdmission):
    import_task: ImportOperation
    verify_token: Callable[[str], bool] = field(repr=False)


@dataclass(slots=True, kw_only=True)
class JobSubmissionServices(RequestAdmission):
    submit_job: SubmissionOperation
    verify_token: Callable[[str], bool] = field(repr=False)
    authorization: SubmissionAuthorization
    busy_code: ApiErrorCode = ApiErrorCode.SUBMISSION_BUSY
    busy_message: str = "Job submission is busy."


@dataclass(slots=True, kw_only=True)
class WorkerAuthenticationServices:
    authenticate: Callable[[str], WorkerAuthorization | None] = field(repr=False)


@dataclass(slots=True, kw_only=True)
class WorkerClaimServices(RequestAdmission):
    claim_job: ClaimOperation
    read_assignment: AssignmentOperation | None = None
    acknowledge: AcknowledgementOperation | None = None
    heartbeat: HeartbeatOperation | None = None
    busy_code: ApiErrorCode = ApiErrorCode.CLAIM_BUSY
    busy_message: str = "Worker claims are busy."


async def require_worker_claim_services(
    request: Request,
) -> WorkerClaimServices:
    services = getattr(request.app.state, "worker_claim_services", None)
    if not isinstance(services, WorkerClaimServices):
        raise api_error(
            503, ApiErrorCode.CLAIM_UNAVAILABLE, "Worker claims unavailable."
        )
    return services


bearer = HTTPBearer(
    auto_error=False,
    scheme_name="TaskImportBearer",
    description="Operator token scoped to task import.",
)

submission_bearer = HTTPBearer(
    auto_error=False,
    scheme_name="JobSubmissionBearer",
    description="Operator token scoped to explicit repository Job submission.",
)

worker_bearer = HTTPBearer(
    auto_error=False,
    scheme_name="WorkerBearer",
    description="Separate expiring token for an explicitly enrolled worker.",
)


def bearer_token(
    request: Request, credentials: HTTPAuthorizationCredentials | None
) -> str | None:
    if (
        len(request.headers.getlist("authorization")) != 1
        or credentials is None
        or len(credentials.credentials) > 128
    ):
        return None
    return credentials.credentials


def valid_bearer(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None,
    verify_token: Callable[[str], bool],
) -> bool:
    presented = bearer_token(request, credentials)
    return presented is not None and verify_token(presented)


async def require_task_import_access(
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
) -> TaskImportServices:
    services = getattr(request.app.state, "task_import_services", None)
    if not isinstance(services, TaskImportServices):
        raise api_error(503, ApiErrorCode.UNAVAILABLE, "Task import is unavailable.")
    if not valid_bearer(request, credentials, services.verify_token):
        raise api_error(
            401,
            ApiErrorCode.UNAUTHORIZED,
            "Valid import authentication is required.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return services


async def require_job_submission_access(
    request: Request,
    credentials: Annotated[
        HTTPAuthorizationCredentials | None, Depends(submission_bearer)
    ],
) -> JobSubmissionServices:
    services = getattr(request.app.state, "job_submission_services", None)
    if not isinstance(services, JobSubmissionServices):
        raise api_error(
            503, ApiErrorCode.SUBMISSION_UNAVAILABLE, "Job submission is unavailable."
        )
    if not valid_bearer(request, credentials, services.verify_token):
        raise api_error(
            401,
            ApiErrorCode.UNAUTHORIZED,
            "Valid submission authentication is required.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return services


async def require_worker_access(
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(worker_bearer)],
) -> WorkerAuthorization:
    services = getattr(request.app.state, "worker_authentication_services", None)
    if not isinstance(services, WorkerAuthenticationServices):
        raise api_error(
            503,
            ApiErrorCode.WORKER_UNAVAILABLE,
            "Worker authentication is unavailable.",
        )
    presented = bearer_token(request, credentials)
    authorization = services.authenticate(presented) if presented is not None else None
    if authorization is None:
        raise api_error(
            401,
            ApiErrorCode.UNAUTHORIZED,
            "Valid worker authentication is required.",
            headers={"WWW-Authenticate": "Bearer", "Cache-Control": "no-store"},
        )
    return authorization
