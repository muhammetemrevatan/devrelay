import argparse
import asyncio
import sys
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from importlib.metadata import version
from pathlib import Path

import uvicorn
from fastapi import FastAPI

from devrelay.api.dependencies import ImportOperation, TaskImportServices
from devrelay.api.errors import register_error_handlers
from devrelay.api.middleware import UnexpectedErrorMiddleware
from devrelay.api.routes.health import router
from devrelay.api.routes.tasks import router as tasks_router
from devrelay.bootstrap.persistence import open_database_engine
from devrelay.bootstrap.security import token_verifier
from devrelay.bootstrap.tasks import open_task_importer
from devrelay.config.control import ControlSettings, RuntimeEnvironment

type ImportResourceFactory = Callable[[], AbstractAsyncContextManager[ImportOperation]]


@asynccontextmanager
async def open_control_import_operation(
    settings: ControlSettings,
) -> AsyncIterator[ImportOperation]:
    if settings.database is None or settings.jira is None:
        raise RuntimeError("Task import integrations are not configured.")
    async with open_database_engine(settings.database) as engine:
        async with open_task_importer(settings.jira, engine) as importer:
            yield importer.execute


def create_app(
    settings: ControlSettings | None = None,
    *,
    import_resources: ImportResourceFactory | None = None,
) -> FastAPI:
    configuration = settings if settings is not None else ControlSettings()
    show_docs = configuration.environment != RuntimeEnvironment.PRODUCTION
    if import_resources is not None and configuration.task_import is None:
        raise ValueError("Import resources require explicit import authentication.")

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        application.state.task_import_services = None
        import_settings = configuration.task_import
        if import_settings is None:
            yield
            return
        factory = import_resources or (
            lambda: open_control_import_operation(configuration)
        )
        async with factory() as operation:
            application.state.task_import_services = TaskImportServices(
                import_task=operation,
                verify_token=token_verifier(import_settings.auth_token),
                max_concurrent_requests=import_settings.max_concurrent_requests,
                body_timeout_seconds=import_settings.body_timeout_seconds,
            )
            try:
                yield
            finally:
                application.state.task_import_services = None

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
    arguments = parser.parse_args()
    env_file: Path | None = arguments.env_file
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
