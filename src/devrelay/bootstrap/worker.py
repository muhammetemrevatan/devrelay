import asyncio
import os
from collections.abc import AsyncIterator, Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path
from uuid import UUID

import httpx

from devrelay.application.contracts.assignment_delivery import AssignmentDelivery
from devrelay.application.contracts.claims import WorkerClaimRequest
from devrelay.application.contracts.workers import (
    WorkerAuthorization,
    WorkerJournalScope,
)
from devrelay.application.ports.base_source import BaseSourcePort
from devrelay.application.ports.worker_journal import WorkerStateUnavailable
from devrelay.application.worker_claims import PrepareWorkerClaim, RequestWorkerClaim
from devrelay.config.worker import WorkerSettings
from devrelay.infrastructure.control.base_sources import HttpControlBaseSource
from devrelay.infrastructure.control.coding_results import (
    open_http_control_coding_results,
)
from devrelay.infrastructure.control.gateway import HttpControlGateway
from devrelay.infrastructure.worker_state.clock import WindowsLeaseClock
from devrelay.infrastructure.worker_state.journal import (
    AsyncClaimJournal,
    SqliteClaimStore,
)
from devrelay.infrastructure.worker_state.windows import WindowsWorkerSecurity


@asynccontextmanager
async def open_worker_claim_client(
    settings: WorkerSettings,
    *,
    state_directory: Path,
    transport: httpx.AsyncBaseTransport | None = None,
    source_transport: httpx.AsyncBaseTransport | None = None,
    result_transport: httpx.AsyncBaseTransport | None = None,
    monotonic_clock: Callable[[], float] | None = None,
) -> AsyncIterator[RequestWorkerClaim]:
    """Verify control identity before opening its journal; perform no claim on startup."""
    async with httpx.AsyncClient(
        base_url=str(settings.control_url),
        headers={"Authorization": f"Bearer {settings.auth_token.get_secret_value()}"},
        follow_redirects=False,
        trust_env=False,
        limits=httpx.Limits(max_connections=1, max_keepalive_connections=1),
        transport=transport,
    ) as client:
        gateway = HttpControlGateway(client)
        authorization = await gateway.identify()
        clock = monotonic_clock
        if clock is None:
            clock = await asyncio.to_thread(WindowsLeaseClock)
        async with open_worker_journal(
            settings,
            worker_id=authorization.worker_id,
            state_directory=state_directory,
        ) as preparation:
            # A source response never occupies the claim/heartbeat connection.
            async with httpx.AsyncClient(
                base_url=str(settings.control_url),
                headers={
                    "Authorization": f"Bearer {settings.auth_token.get_secret_value()}"
                },
                follow_redirects=False,
                trust_env=False,
                limits=httpx.Limits(max_connections=1, max_keepalive_connections=1),
                timeout=httpx.Timeout(90, connect=5, pool=1),
                transport=source_transport,
            ) as source_client:

                async def bind_source(
                    request: WorkerClaimRequest,
                    current: WorkerAuthorization,
                    delivery: AssignmentDelivery,
                ) -> BaseSourcePort:
                    return await asyncio.to_thread(
                        HttpControlBaseSource,
                        source_client,
                        request,
                        current,
                        delivery,
                    )

                async with AsyncExitStack() as result_resources:
                    result_gateway = None
                    if settings.coding_results_enabled:
                        result_gateway = await result_resources.enter_async_context(
                            open_http_control_coding_results(
                                str(settings.control_url),
                                settings.auth_token.get_secret_value(),
                                transport=result_transport,
                            )
                        )
                    worker = RequestWorkerClaim(
                        preparation,
                        gateway,
                        authorization,
                        heartbeat_gateway=gateway,
                        monotonic_clock=clock,
                        base_source_binding=bind_source,
                        coding_result_gateway=result_gateway,
                    )
                    try:
                        yield worker
                    finally:
                        await worker.close_coding_results()


def validate_worker_configuration(env_file: Path | None = None) -> None:
    # Pylance omits _env_file and treats environment-sourced fields as required args.
    WorkerSettings(_env_file=env_file)  # pyright: ignore[reportCallIssue]


async def check_worker_runtime(
    env_file: Path | None = None, image_id: str | None = None
) -> None:
    """Model-free diagnostics need no control token or real authentication."""
    from devrelay.bootstrap.worker_coding import check_runtime
    from devrelay.config.docker_runtime import (
        DockerRuntimeSettings,
        WorkerRuntimeSettings,
    )

    configured = WorkerRuntimeSettings(_env_file=env_file).docker_runtime
    if image_id is not None:
        configured = DockerRuntimeSettings(image_id=image_id)
    if configured is None:
        raise WorkerStateUnavailable("Configure a pinned Docker image for diagnostics.")
    await check_runtime(configured)


async def run_worker_once(env_file: Path | None, state_directory: Path) -> str:
    from devrelay.bootstrap.worker_coding import (
        check_engine,
        drain_cleanup,
        run_claimed_task,
        validate_run_configuration,
    )
    from devrelay.infrastructure.containers import DockerEngine
    from devrelay.infrastructure.docker_coding import probe_runtime

    settings = WorkerSettings(_env_file=env_file)  # pyright: ignore[reportCallIssue]
    # Reject disabled real execution before connecting to control or claiming.
    profile = validate_run_configuration(settings, state_directory)
    assert settings.docker_runtime is not None
    clock = await asyncio.to_thread(WindowsLeaseClock)
    engine = DockerEngine(settings.docker_runtime.endpoint)
    try:
        await check_engine(engine, profile)
        await probe_runtime(engine, profile, clock)
        async with open_worker_claim_client(
            settings, state_directory=state_directory, monotonic_clock=clock
        ) as worker:
            return await run_claimed_task(
                worker, settings, state_directory, engine, profile, clock
            )
    finally:
        await drain_cleanup(engine.aclose())


@asynccontextmanager
async def open_worker_journal(
    settings: WorkerSettings, *, worker_id: UUID, state_directory: Path
) -> AsyncIterator[PrepareWorkerClaim]:
    """Open local recovery only; caller must verify worker identity with control."""
    if not state_directory.is_absolute() or ".." in state_directory.parts:
        raise WorkerStateUnavailable("Worker state requires an explicit private path.")
    directory = Path(os.path.abspath(state_directory))
    scope = WorkerJournalScope(
        worker_id=worker_id, control_url=str(settings.control_url)
    )
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="devrelay-state")
    loop = asyncio.get_running_loop()
    resources: tuple[WindowsWorkerSecurity, SqliteClaimStore] | None = None

    def initialize() -> tuple[WindowsWorkerSecurity, SqliteClaimStore]:
        nonlocal resources
        workspace = settings.workspace_root.resolve()
        resolved = directory.resolve()
        if resolved.is_relative_to(workspace) or workspace.is_relative_to(resolved):
            raise WorkerStateUnavailable(
                "Worker state must be separate from workspaces."
            )
        if any((parent / ".git").exists() for parent in (resolved, *resolved.parents)):
            raise WorkerStateUnavailable(
                "Worker state must be outside repository checkouts."
            )
        security = WindowsWorkerSecurity()
        try:
            security.acquire(scope.key)
            created = security.ensure_private_directory(directory)
            if not created and not (directory / "journal.sqlite3").exists():
                raise WorkerStateUnavailable(
                    "Existing worker state has lost its journal."
                )
            store = SqliteClaimStore(directory, scope, security, security)
            resources = security, store
            return resources
        except WorkerStateUnavailable:
            security.close()
            raise
        except Exception:
            security.close()
            raise WorkerStateUnavailable(
                "Protected worker journal could not open."
            ) from None
        except BaseException:
            security.close()
            raise

    future = loop.run_in_executor(executor, initialize)
    future.add_done_callback(
        lambda done: None if done.cancelled() else done.exception()
    )
    journal: AsyncClaimJournal | None = None
    try:
        resources = await asyncio.shield(future)
        journal = AsyncClaimJournal(resources[1], executor)
        yield PrepareWorkerClaim(
            journal,
            heartbeat_journal=journal,
            execution_journal=journal,
            workspace_journal=journal,
            result_journal=journal,
        )
    finally:
        if journal is not None:
            journal.stop_admission()

        def close() -> None:
            if resources is not None:
                security, store = resources
                try:
                    store.close()
                finally:
                    security.close()

        # FIFO cleanup must follow initialization even if setup is cancelled twice.
        cleanup = loop.run_in_executor(executor, close)
        cleanup.add_done_callback(
            lambda done: None if done.cancelled() else done.exception()
        )
        try:
            await asyncio.shield(cleanup)
        finally:
            await asyncio.to_thread(executor.shutdown, wait=True, cancel_futures=False)
