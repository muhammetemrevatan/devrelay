import asyncio
import json
import sys
from collections.abc import AsyncIterator, Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from devrelay.api.dependencies import WorkerClaimOperations
from devrelay.application.acknowledgements import AcknowledgeAssignedJob
from devrelay.application.claims import ClaimJob
from devrelay.application.contracts.heartbeats import HeartbeatLimits
from devrelay.application.contracts.worker_recovery import (
    WorkerRecoveryPhase,
    WorkerRecoveryState,
)
from devrelay.application.contracts.workers import WorkerJournalScope
from devrelay.application.heartbeats import HeartbeatAssignedJob
from devrelay.application.ports.control_gateway import ControlUnavailable
from devrelay.application.worker_claims import PrepareWorkerClaim, RequestWorkerClaim
from devrelay.bootstrap.control import create_app
from devrelay.bootstrap.persistence import (
    acknowledgement_store,
    claim_store,
    heartbeat_store,
)
from devrelay.bootstrap.worker import open_worker_claim_client
from devrelay.config.control import ControlSettings
from devrelay.config.worker import WorkerSettings
from devrelay.config.worker_claims import WorkerClaimsSettings
from devrelay.infrastructure.control.gateway import HttpControlGateway
from devrelay.infrastructure.worker_state.journal import (
    AsyncClaimJournal,
    SqliteClaimStore,
)
from tests.integration.job_fixtures import save_job
from tests.integration.test_worker_claim_http_recovery import Preparer
from tests.integration.test_worker_journal import TestProtector, TestStorage


class HeartbeatResponses(httpx.AsyncBaseTransport):
    def __init__(self, inner: httpx.AsyncBaseTransport) -> None:
        self.inner = inner
        self.inputs: list[dict[str, object]] = []
        self.lose_next = False

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        response = await self.inner.handle_async_request(request)
        if request.url.path.endswith("/heartbeats"):
            self.inputs.append(json.loads(request.content))
            if self.lose_next:
                self.lose_next = False
                assert response.status_code == 200
                await response.aclose()
                raise httpx.ReadError(
                    "Simulated heartbeat response loss after commit.", request=request
                )
        return response

    async def aclose(self) -> None:
        await self.inner.aclose()


@dataclass(frozen=True)
class WorkerHarness:
    open_worker: Callable[
        [HeartbeatResponses], AbstractAsyncContextManager[RequestWorkerClaim]
    ]
    transport: Callable[[], HeartbeatResponses]
    initial: WorkerRecoveryState
    private: Path


@asynccontextmanager
async def prepared_worker(
    engine: AsyncEngine,
    worker_configuration: ControlSettings,
    tmp_path: Path,
    *,
    native: bool,
    clock: Callable[[], float] | None = None,
) -> AsyncIterator[WorkerHarness]:
    stored = await save_job(engine)
    values = worker_configuration.model_dump()
    repository_id = stored.job.repository_id
    values["repositories"][0]["repository_id"] = repository_id
    values["github"]["repository_ids"][0]["repository_id"] = repository_id
    values["workers"][0]["allowed_repository_ids"] = frozenset({repository_id})
    values["workers"][0]["expires_at"] = datetime.now(UTC) + timedelta(hours=1)
    values["worker_claims"] = WorkerClaimsSettings()
    configuration = ControlSettings.model_validate(values)
    registration = configuration.workers[0]
    preparer = Preparer(stored, True)
    claimer = ClaimJob(
        claim_store(engine, (stored.intent.policy,)), preparer, lambda proof: None
    )
    acknowledger = AcknowledgeAssignedJob(
        claimer.assignment_reader,
        acknowledgement_store(engine, (stored.intent.policy,)),
    )
    heartbeater = HeartbeatAssignedJob(
        heartbeat_store(
            engine,
            (stored.intent.policy,),
            HeartbeatLimits(minimum_interval_seconds=1),
        ),
        lambda proof: None,
    )

    @asynccontextmanager
    async def resources() -> AsyncIterator[WorkerClaimOperations]:
        yield WorkerClaimOperations(
            claim=claimer.execute,
            read_assignment=claimer.assignment_reader.execute,
            acknowledge=acknowledger.execute,
            heartbeat=heartbeater.execute,
        )

    app = create_app(configuration, claim_resources=resources)
    settings = WorkerSettings(
        control_url="https://control.example/",
        auth_token=registration.auth_token,
        workspace_root=tmp_path / "workspaces",
    )
    scope = WorkerJournalScope(
        worker_id=registration.worker_id, control_url=str(settings.control_url)
    )
    private = tmp_path / "private"
    protector = TestProtector()

    @asynccontextmanager
    async def portable(
        transport: HeartbeatResponses,
    ) -> AsyncIterator[RequestWorkerClaim]:
        async with httpx.AsyncClient(
            base_url=str(settings.control_url),
            headers={
                "Authorization": f"Bearer {settings.auth_token.get_secret_value()}"
            },
            transport=transport,
            follow_redirects=False,
            trust_env=False,
        ) as client:
            gateway = HttpControlGateway(client)
            authorization = await gateway.identify()
            with ThreadPoolExecutor(max_workers=1) as executor:
                loop = asyncio.get_running_loop()

                def open_store() -> SqliteClaimStore:
                    private.mkdir(exist_ok=True)
                    return SqliteClaimStore(private, scope, protector, TestStorage())

                local = await loop.run_in_executor(executor, open_store)
                journal = AsyncClaimJournal(local, executor)
                try:
                    yield RequestWorkerClaim(
                        PrepareWorkerClaim(journal, heartbeat_journal=journal),
                        gateway,
                        authorization,
                        heartbeat_gateway=gateway,
                        monotonic_clock=clock,
                    )
                finally:
                    journal.stop_admission()
                    await loop.run_in_executor(executor, local.close)

    def open_worker(
        transport: HeartbeatResponses,
    ) -> AbstractAsyncContextManager[RequestWorkerClaim]:
        return (
            open_worker_claim_client(
                settings, state_directory=private, transport=transport
            )
            if native
            else portable(transport)
        )

    def transport() -> HeartbeatResponses:
        return HeartbeatResponses(httpx.ASGITransport(app))

    async with app.router.lifespan_context(app):
        async with open_worker(transport()) as worker:
            await worker.execute()
            await worker.acknowledge()
            initial = await worker.recover()
        yield WorkerHarness(
            open_worker=open_worker,
            transport=transport,
            initial=initial,
            private=private,
        )


async def heartbeat_storage(engine: AsyncEngine) -> tuple[int, int, int]:
    async with engine.connect() as connection:
        row = (
            await connection.execute(
                text(
                    "SELECT lease_revision, receipt_count, "
                    "(SELECT count(*) FROM worker_heartbeat_receipts) "
                    "FROM attempt_lease_authorities"
                )
            )
        ).one()
    return int(row[0]), int(row[1]), int(row[2])


def require_original_anchors(
    state: WorkerRecoveryState, original: WorkerRecoveryState
) -> None:
    assert state.phase is WorkerRecoveryPhase.ACKNOWLEDGED
    assert state.assignment == original.assignment
    assert state.acknowledgement == original.acknowledgement
    assert state.completion == original.completion
    assert state.request is not None and original.request is not None
    assert state.request.claim_request_id == original.request.claim_request_id
    assert state.request.proof.reveal() == original.request.proof.reveal()


@pytest.mark.postgres
@pytest.mark.parametrize("native", [True, False])
def test_committed_lost_heartbeat_reopens_with_exact_pending_input(
    database: Callable[[], AbstractAsyncContextManager[AsyncEngine]],
    worker_configuration: ControlSettings,
    tmp_path: Path,
    native: bool,
) -> None:
    if native and sys.platform != "win32":
        pytest.skip("Native journal requires Windows DPAPI/private ACLs.")

    async def scenario() -> None:
        async with database() as engine:
            async with prepared_worker(
                engine, worker_configuration, tmp_path, native=native
            ) as harness:
                lost = harness.transport()
                lost.lose_next = True
                async with harness.open_worker(lost) as worker:
                    with pytest.raises(ControlUnavailable):
                        await worker.heartbeat()
                    pending = await worker.recover()
                after_loss = await heartbeat_storage(engine)
                replay = harness.transport()

                async with harness.open_worker(replay) as worker:
                    recovered = await worker.heartbeat()
                    final = await worker.recover()
                after_replay = await heartbeat_storage(engine)

                assert pending.pending_heartbeat is not None
                assert pending.last_heartbeat is None
                assert after_loss == (2, 1, 1)
                assert after_replay == after_loss
                assert lost.inputs == replay.inputs
                assert not recovered.result.renewed
                assert recovered.result.authority.lease_revision == 2
                assert recovered.intent == pending.pending_heartbeat
                assert final.last_heartbeat == pending.pending_heartbeat
                assert final.pending_heartbeat is None
                assert recovered.request_started_at <= recovered.received_at
                require_original_anchors(pending, harness.initial)
                require_original_anchors(final, harness.initial)
                assert harness.initial.request is not None
                assert (
                    harness.initial.request.proof.reveal().encode()
                    not in (harness.private / "journal.sqlite3").read_bytes()
                )

    asyncio.run(
        scenario(),
        loop_factory=asyncio.SelectorEventLoop if sys.platform == "win32" else None,
    )


@pytest.mark.postgres
@pytest.mark.parametrize("native", [True, False])
def test_completed_heartbeat_reopens_by_replay_before_new_revision_renewal(
    database: Callable[[], AbstractAsyncContextManager[AsyncEngine]],
    worker_configuration: ControlSettings,
    tmp_path: Path,
    native: bool,
) -> None:
    if native and sys.platform != "win32":
        pytest.skip("Native journal requires Windows DPAPI/private ACLs.")

    async def scenario() -> None:
        async with database() as engine:
            async with prepared_worker(
                engine, worker_configuration, tmp_path, native=native
            ) as harness:
                first_transport = harness.transport()
                async with harness.open_worker(first_transport) as worker:
                    original = await worker.heartbeat()
                    completed = await worker.recover()
                replay_transport = harness.transport()
                async with harness.open_worker(replay_transport) as worker:
                    replayed = await worker.heartbeat()
                    replay_state = await worker.recover()
                    after_replay = await heartbeat_storage(engine)
                    await asyncio.sleep(1.05)

                    renewed = await worker.heartbeat()
                    renewed_state = await worker.recover()
                after_renewal = await heartbeat_storage(engine)

                assert original.result.renewed
                assert original.result.authority.lease_revision == 2
                assert completed.pending_heartbeat is None
                assert completed.last_heartbeat == original.intent
                assert not replayed.result.renewed
                assert replayed.intent == original.intent
                assert replayed.result.authority.lease_revision == 2
                assert (
                    replayed.result.authority.observed_at
                    > original.result.authority.observed_at
                )
                assert replay_transport.inputs[0] == first_transport.inputs[0]
                assert after_replay == (2, 1, 1)
                assert (
                    renewed.intent.heartbeat_request_id
                    != original.intent.heartbeat_request_id
                )
                assert renewed.intent.expected_lease_revision == 2
                assert renewed.result.renewed
                assert renewed.result.authority.lease_revision == 3
                assert (
                    renewed.result.authority.current_expires_at
                    > original.result.authority.current_expires_at
                )
                assert after_renewal == (3, 2, 2)
                assert renewed_state.pending_heartbeat is None
                assert renewed_state.last_heartbeat == renewed.intent
                require_original_anchors(completed, harness.initial)
                require_original_anchors(replay_state, harness.initial)
                require_original_anchors(renewed_state, harness.initial)
                assert harness.initial.assignment is not None
                assert (
                    renewed.result.authority.reference
                    == harness.initial.assignment.assignment
                )

    asyncio.run(
        scenario(),
        loop_factory=asyncio.SelectorEventLoop if sys.platform == "win32" else None,
    )
