import argparse
import asyncio
import sys
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from importlib.metadata import version
from pathlib import Path
from uuid import UUID

import uvicorn
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncEngine

from devrelay.api.dependencies import (
    BaseSourceOperations,
    ClaimOperation,
    CodingResultOperations,
    ImportOperation,
    JobSubmissionServices,
    SubmissionOperation,
    TaskImportServices,
    WorkerAuthenticationServices,
    WorkerBaseSourceServices,
    WorkerClaimOperations,
    WorkerClaimServices,
    WorkerCodingResultServices,
)
from devrelay.api.errors import register_error_handlers
from devrelay.api.middleware import UnexpectedErrorMiddleware
from devrelay.api.routes.base_sources import router as base_sources_router
from devrelay.api.routes.claims import router as claims_router
from devrelay.api.routes.health import router
from devrelay.api.routes.jobs import router as jobs_router
from devrelay.api.routes.tasks import router as tasks_router
from devrelay.api.routes.workers import router as workers_router
from devrelay.application.contracts.submissions import SubmissionAuthorization
from devrelay.application.publish_coding_result import PublishCodingResult
from devrelay.bootstrap.claims import (
    job_acknowledger,
    job_heartbeater,
    open_job_base_reader,
    open_job_claimer,
)
from devrelay.bootstrap.jobs import open_github_job_submitter
from devrelay.bootstrap.persistence import open_database_engine
from devrelay.bootstrap.security import token_verifier, worker_authenticator
from devrelay.bootstrap.tasks import open_task_importer
from devrelay.config.control import ControlSettings, RuntimeEnvironment

type ImportResourceFactory = Callable[[], AbstractAsyncContextManager[ImportOperation]]
type SubmissionResourceFactory = Callable[
    [], AbstractAsyncContextManager[SubmissionOperation]
]
type ClaimResourceFactory = Callable[
    [], AbstractAsyncContextManager[ClaimOperation | WorkerClaimOperations]
]
type BaseSourceResourceFactory = Callable[
    [], AbstractAsyncContextManager[BaseSourceOperations]
]
type CodingResultResourceFactory = Callable[
    [], AbstractAsyncContextManager[CodingResultOperations]
]
type PublicationResourceFactory = Callable[
    [], AbstractAsyncContextManager[PublishCodingResult]
]


@asynccontextmanager
async def open_control_coding_publisher(
    settings: ControlSettings, engine: AsyncEngine
) -> AsyncIterator[PublishCodingResult]:
    from devrelay.bootstrap.coding_publication import open_coding_publisher

    async with open_coding_publisher(settings, engine) as publisher:
        yield publisher


@asynccontextmanager
async def open_control_coding_result_operation(
    settings: ControlSettings,
    engine: AsyncEngine,
) -> AsyncIterator[CodingResultOperations]:
    from devrelay.bootstrap.coding_results import open_worker_coding_results

    async with open_worker_coding_results(settings, engine) as results:
        yield CodingResultOperations(
            accept=results.execute,
            recover=results.recover,
            require_ready=results.require_ready,
        )


@asynccontextmanager
async def open_control_base_source_operation(
    settings: ControlSettings, engine: AsyncEngine
) -> AsyncIterator[BaseSourceOperations]:
    async with open_job_base_reader(settings, engine) as reader:
        yield BaseSourceOperations(
            read_base=reader.execute, verify_base=reader.require_current
        )


@asynccontextmanager
async def open_control_claim_operation(
    settings: ControlSettings,
    engine: AsyncEngine,
) -> AsyncIterator[WorkerClaimOperations]:
    async with open_job_claimer(settings, engine) as claimer:
        yield WorkerClaimOperations(
            claim=claimer.execute,
            read_assignment=claimer.assignment_reader.execute,
            acknowledge=job_acknowledger(settings, engine).execute,
            heartbeat=job_heartbeater(settings, engine).execute,
        )


@asynccontextmanager
async def open_control_import_operation(
    settings: ControlSettings,
    engine: AsyncEngine,
) -> AsyncIterator[ImportOperation]:
    if settings.database is None or settings.jira is None:
        raise RuntimeError("Task import integrations are not configured.")
    async with open_task_importer(settings.jira, engine) as importer:
        yield importer.execute


@asynccontextmanager
async def open_control_submission_operation(
    settings: ControlSettings,
    engine: AsyncEngine,
) -> AsyncIterator[SubmissionOperation]:
    if settings.database is None or settings.jira is None or settings.github is None:
        raise RuntimeError("Job submission integrations are not configured.")
    async with open_github_job_submitter(
        settings.jira, settings.github, settings.repositories, engine
    ) as submitter:
        yield submitter.execute


def create_app(
    settings: ControlSettings | None = None,
    *,
    import_resources: ImportResourceFactory | None = None,
    submission_resources: SubmissionResourceFactory | None = None,
    claim_resources: ClaimResourceFactory | None = None,
    base_source_resources: BaseSourceResourceFactory | None = None,
    coding_result_resources: CodingResultResourceFactory | None = None,
    publication_resources: PublicationResourceFactory | None = None,
) -> FastAPI:
    configuration = settings if settings is not None else ControlSettings()
    if publication_resources is not None and configuration.coding_publication is None:
        raise ValueError(
            "Publication resources require explicitly enabled publication."
        )
    show_docs = configuration.environment != RuntimeEnvironment.PRODUCTION
    if import_resources is not None and configuration.task_import is None:
        raise ValueError("Import resources require explicit import authentication.")
    if submission_resources is not None and configuration.job_submission is None:
        raise ValueError(
            "Submission resources require explicit submission authentication."
        )
    if claim_resources is not None and configuration.worker_claims is None:
        raise ValueError("Claim resources require explicitly enabled worker claims.")
    if base_source_resources is not None and configuration.worker_base_sources is None:
        raise ValueError(
            "Base source resources require explicitly enabled source delivery."
        )
    if (
        coding_result_resources is not None
        and configuration.worker_coding_results is None
    ):
        raise ValueError(
            "Coding result resources require explicitly enabled result acceptance."
        )

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        application.state.task_import_services = None
        application.state.job_submission_services = None
        application.state.worker_authentication_services = None
        application.state.worker_claim_services = None
        application.state.worker_base_source_services = None
        application.state.worker_coding_result_services = None
        application.state.coding_result_publisher = None
        async with AsyncExitStack() as stack:
            try:
                if configuration.workers:
                    application.state.worker_authentication_services = (
                        WorkerAuthenticationServices(
                            authenticate=worker_authenticator(configuration.workers)
                        )
                    )
                engine = None
                if (
                    (configuration.task_import is not None and import_resources is None)
                    or (
                        configuration.job_submission is not None
                        and submission_resources is None
                    )
                    or (
                        configuration.worker_claims is not None
                        and claim_resources is None
                    )
                    or (
                        configuration.worker_base_sources is not None
                        and base_source_resources is None
                    )
                    or (
                        configuration.worker_coding_results is not None
                        and coding_result_resources is None
                    )
                    or (
                        configuration.coding_publication is not None
                        and publication_resources is None
                    )
                ):
                    if configuration.database is None:
                        raise RuntimeError("Control database settings are missing.")
                    engine = await stack.enter_async_context(
                        open_database_engine(configuration.database)
                    )
                import_settings = configuration.task_import
                if import_settings is not None:
                    if import_resources is not None:
                        import_context = import_resources()
                    else:
                        assert engine is not None
                        import_context = open_control_import_operation(
                            configuration, engine
                        )
                    import_operation = await stack.enter_async_context(import_context)
                    application.state.task_import_services = TaskImportServices(
                        import_task=import_operation,
                        verify_token=token_verifier(import_settings.auth_token),
                        max_concurrent_requests=import_settings.max_concurrent_requests,
                        body_timeout_seconds=import_settings.body_timeout_seconds,
                    )
                submission_settings = configuration.job_submission
                if submission_settings is not None:
                    if submission_resources is not None:
                        submission_context = submission_resources()
                    else:
                        assert engine is not None
                        submission_context = open_control_submission_operation(
                            configuration, engine
                        )
                    submission_operation = await stack.enter_async_context(
                        submission_context
                    )
                    application.state.job_submission_services = JobSubmissionServices(
                        submit_job=submission_operation,
                        verify_token=token_verifier(submission_settings.auth_token),
                        authorization=SubmissionAuthorization(
                            actor_id=submission_settings.actor_id,
                            allowed_repository_ids=submission_settings.allowed_repository_ids,
                        ),
                        max_concurrent_requests=submission_settings.max_concurrent_requests,
                        body_timeout_seconds=submission_settings.body_timeout_seconds,
                    )
                claim_settings = configuration.worker_claims
                if claim_settings is not None:
                    if claim_resources is not None:
                        claim_context = claim_resources()
                    else:
                        assert engine is not None
                        claim_context = open_control_claim_operation(
                            configuration, engine
                        )
                    claim_operation = await stack.enter_async_context(claim_context)
                    application.state.worker_claim_services = WorkerClaimServices(
                        claim_job=claim_operation.claim
                        if isinstance(claim_operation, WorkerClaimOperations)
                        else claim_operation,
                        read_assignment=claim_operation.read_assignment
                        if isinstance(claim_operation, WorkerClaimOperations)
                        else None,
                        acknowledge=claim_operation.acknowledge
                        if isinstance(claim_operation, WorkerClaimOperations)
                        else None,
                        heartbeat=claim_operation.heartbeat
                        if isinstance(claim_operation, WorkerClaimOperations)
                        else None,
                        max_concurrent_requests=claim_settings.max_concurrent_requests,
                        body_timeout_seconds=claim_settings.body_timeout_seconds,
                    )
                base_source_settings = configuration.worker_base_sources
                if base_source_settings is not None:
                    if base_source_resources is not None:
                        base_source_context = base_source_resources()
                    else:
                        assert engine is not None
                        base_source_context = open_control_base_source_operation(
                            configuration, engine
                        )
                    base_source_operation = await stack.enter_async_context(
                        base_source_context
                    )
                    application.state.worker_base_source_services = WorkerBaseSourceServices(
                        read_base=base_source_operation.read_base,
                        verify_base=base_source_operation.verify_base,
                        max_concurrent_requests=base_source_settings.max_concurrent_requests,
                        body_timeout_seconds=base_source_settings.body_timeout_seconds,
                        request_timeout_seconds=base_source_settings.request_timeout_seconds,
                    )
                coding_result_settings = configuration.worker_coding_results
                if coding_result_settings is not None:
                    if coding_result_resources is not None:
                        coding_result_context = coding_result_resources()
                    else:
                        assert engine is not None
                        coding_result_context = open_control_coding_result_operation(
                            configuration, engine
                        )
                    coding_operations = await stack.enter_async_context(
                        coding_result_context
                    )
                    if not isinstance(coding_operations, CodingResultOperations):
                        raise ValueError(
                            "Coding result resources require complete explicit operations."
                        )
                    coding_services = WorkerCodingResultServices(
                        accept_result=coding_operations.accept,
                        recover_result=coding_operations.recover,
                        require_ready=coding_operations.require_ready,
                        max_concurrent_requests=coding_result_settings.max_concurrent_requests,
                        body_timeout_seconds=coding_result_settings.body_timeout_seconds,
                        request_timeout_seconds=coding_result_settings.request_timeout_seconds,
                    )
                    application.state.worker_coding_result_services = coding_services
                    stack.push_async_callback(coding_services.aclose)
                if configuration.coding_publication is not None:
                    if publication_resources is not None:
                        publication_context = publication_resources()
                    else:
                        assert engine is not None
                        publication_context = open_control_coding_publisher(
                            configuration, engine
                        )
                    application.state.coding_result_publisher = (
                        await stack.enter_async_context(publication_context)
                    )
                yield
            finally:
                application.state.task_import_services = None
                application.state.job_submission_services = None
                application.state.worker_authentication_services = None
                application.state.worker_claim_services = None
                application.state.worker_base_source_services = None
                application.state.worker_coding_result_services = None
                application.state.coding_result_publisher = None

    app = FastAPI(
        title="DevRelay",
        version=version("devrelay"),
        docs_url="/docs" if show_docs else None,
        redoc_url=None,
        openapi_url="/openapi.json" if show_docs else None,
        lifespan=lifespan,
    )
    register_error_handlers(app)
    # Register last among user middleware: classified handlers run inside it.
    app.add_middleware(UnexpectedErrorMiddleware)
    app.include_router(router)
    if configuration.task_import is not None:
        app.include_router(tasks_router)
    if configuration.job_submission is not None:
        app.include_router(jobs_router)
    if configuration.workers:
        app.include_router(workers_router)
    if configuration.worker_claims is not None:
        app.include_router(claims_router)
    if configuration.worker_base_sources is not None:
        app.include_router(base_sources_router)
    if configuration.worker_coding_results is not None:
        from devrelay.api.routes.coding_results import router as coding_results_router

        app.include_router(coding_results_router)
    return app


def control_event_loop() -> asyncio.AbstractEventLoop:
    # psycopg async connections require the selector loop on Windows.
    return (
        asyncio.SelectorEventLoop()
        if sys.platform == "win32"
        else asyncio.new_event_loop()
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Start the DevRelay control service.")
    parser.add_argument(
        "--env-file", type=Path, help="Explicit local environment file."
    )
    result_command = parser.add_mutually_exclusive_group()
    result_command.add_argument(
        "--publish-result",
        type=UUID,
        help="Explicitly publish one accepted result UUID instead of starting HTTP.",
    )
    result_command.add_argument(
        "--reconcile-result",
        type=UUID,
        help="Observe one accepted result UUID without new publication writes.",
    )
    parser.add_argument(
        "--worker-id", type=UUID, help="Current enrolled owner of the result command."
    )
    arguments = parser.parse_args()
    result_id: UUID | None = arguments.publish_result or arguments.reconcile_result
    if (result_id is None) != (arguments.worker_id is None):
        parser.error("A result command and --worker-id must be supplied together.")
    env_file: Path | None = arguments.env_file
    if result_id is not None:
        from devrelay.bootstrap.accepted_coding_results import (
            publish_accepted_result,
            reconcile_accepted_result,
        )

        try:
            settings = ControlSettings(_env_file=env_file)  # pyright: ignore[reportCallIssue]
            with asyncio.Runner(loop_factory=control_event_loop) as runner:
                if arguments.publish_result is not None:
                    terminal = runner.run(
                        publish_accepted_result(
                            settings, result_id, arguments.worker_id
                        )
                    )
                    message = (
                        f"Published accepted result {terminal.receipt.result_id}: "
                        f"pull request #{terminal.reference.number}, "
                        f"head {terminal.reference.head_revision}; awaiting human review."
                    )
                else:
                    status = runner.run(
                        reconcile_accepted_result(
                            settings, result_id, arguments.worker_id
                        )
                    )
                    if status is None:
                        message = "No accepted result was available for reconciliation."
                    elif status.terminal is not None:
                        message = (
                            f"Reconciled accepted result {status.receipt.result_id}: "
                            f"{status.state.value}, pull request "
                            f"#{status.terminal.reference.number}, head "
                            f"{status.terminal.reference.head_revision}; awaiting human review."
                        )
                    else:
                        message = (
                            f"Reconciled accepted result {status.receipt.result_id}: "
                            f"{status.state.value}; no new publication write authorized."
                        )
        except Exception:
            # Command failures can contain SQL/provider/configuration secrets.
            parser.exit(
                1,
                "Coding result operation did not complete; inspect retained history before retrying.\n",
            )
        print(message)
        return
    # Pylance synthesizes model-field parameters; BaseSettings accepts _env_file.
    settings = ControlSettings(_env_file=env_file)  # pyright: ignore[reportCallIssue]
    uvicorn.run(
        create_app(settings),
        host=str(settings.host),
        port=settings.port,
        log_level=settings.log_level.value,
        loop="devrelay.bootstrap.control:control_event_loop",
        access_log=False,
    )
