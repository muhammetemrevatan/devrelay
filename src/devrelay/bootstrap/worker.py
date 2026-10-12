import asyncio
import os
from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import UUID

import httpx

from devrelay.application.contracts.workers import WorkerJournalScope
from devrelay.application.ports.worker_journal import WorkerStateUnavailable
from devrelay.application.worker_claims import PrepareWorkerClaim, RequestWorkerClaim
from devrelay.config.worker import WorkerSettings
from devrelay.infrastructure.control.gateway import HttpControlGateway
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
        async with open_worker_journal(
            settings,
            worker_id=authorization.worker_id,
            state_directory=state_directory,
        ) as preparation:
            yield RequestWorkerClaim(
                preparation, gateway, authorization, heartbeat_gateway=gateway
            )


def validate_worker_configuration(env_file: Path | None = None) -> None:
    # Pylance omits _env_file and treats environment-sourced fields as required args.
    WorkerSettings(_env_file=env_file)  # pyright: ignore[reportCallIssue]


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
        yield PrepareWorkerClaim(journal, heartbeat_journal=journal)
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
