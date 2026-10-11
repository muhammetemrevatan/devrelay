import asyncio
import json
import sys
from collections.abc import AsyncIterator, Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from devrelay.api.dependencies import WorkerClaimOperations
from devrelay.application.acknowledgements import AcknowledgeAssignedJob
from devrelay.application.claims import ClaimJob
from devrelay.application.contracts.eligibility import PreparedJobClaim
from devrelay.application.contracts.submissions import StoredJobSubmission
from devrelay.application.contracts.worker_recovery import WorkerRecoveryPhase
from devrelay.application.contracts.workers import (
    WorkerAuthorization,
    WorkerJournalScope,
)
from devrelay.application.ports.control_gateway import ControlUnavailable
from devrelay.application.worker_claims import PrepareWorkerClaim, RequestWorkerClaim
from devrelay.bootstrap.control import create_app
from devrelay.bootstrap.persistence import acknowledgement_store, claim_store
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
from tests.integration.test_worker_journal import TestProtector, TestStorage


class Preparer:
    def __init__(self, stored: StoredJobSubmission, assigned: bool) -> None:
        self.stored = stored
        self.assigned = assigned
        self.calls = 0

    async def execute(
        self, authorization: WorkerAuthorization
    ) -> PreparedJobClaim | None:
        self.calls += 1
        return (
            PreparedJobClaim(
                worker_id=authorization.worker_id,
                stored=self.stored,
                observed_at=datetime.now(UTC),
            )
            if self.assigned
            else None
        )


class LostResponse(httpx.AsyncBaseTransport):
    def __init__(self, inner: httpx.AsyncBaseTransport) -> None:
        self.inner = inner
        self.inputs: list[dict[str, object]] = []
        self.lose_next = True

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        response = await self.inner.handle_async_request(request)
        if request.method == "POST":
            self.inputs.append(json.loads(request.content))
            if self.lose_next:
                self.lose_next = False
                assert response.status_code == 200
                await response.aclose()
                raise httpx.ReadError(
                    "Simulated response loss after commit.", request=request
                )
        return response

    async def aclose(self) -> None:
        await self.inner.aclose()


@pytest.mark.postgres
@pytest.mark.parametrize("assigned", [True, False])
@pytest.mark.parametrize("native", [True, False])
def test_lost_http_response_recovers_original_claim_after_worker_reopen(
    database: Callable[[], AbstractAsyncContextManager[AsyncEngine]],
    worker_configuration: ControlSettings,
    tmp_path: Path,
    assigned: bool,
    native: bool,
) -> None:
    if native and sys.platform != "win32":
        pytest.skip("Native journal requires Windows DPAPI/private ACLs.")

    async def scenario() -> None:
        async with database() as engine:
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
            preparer = Preparer(stored, assigned)
            claimer = ClaimJob(
                claim_store(engine, (stored.intent.policy,)),
                preparer,
                lambda proof: None,
            )

            @asynccontextmanager
            async def resources() -> AsyncIterator[WorkerClaimOperations]:
                yield WorkerClaimOperations(
                    claim=claimer.execute,
                    read_assignment=claimer.assignment_reader.execute,
                    acknowledge=AcknowledgeAssignedJob(
                        claimer.assignment_reader,
                        acknowledgement_store(engine, (stored.intent.policy,)),
                    ).execute,
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
                transport: httpx.AsyncBaseTransport,
            ) -> AsyncIterator[RequestWorkerClaim]:
                async with httpx.AsyncClient(
                    base_url=str(settings.control_url),
                    headers={
                        "Authorization": f"Bearer {settings.auth_token.get_secret_value()}"
                    },
                    transport=transport,
                ) as client:
                    gateway = HttpControlGateway(client)
                    authorization = await gateway.identify()
                    with ThreadPoolExecutor(max_workers=1) as executor:
                        loop = asyncio.get_running_loop()

                        def open_store() -> SqliteClaimStore:
                            private.mkdir(exist_ok=True)
                            return SqliteClaimStore(
                                private, scope, protector, TestStorage()
                            )

                        local = await loop.run_in_executor(executor, open_store)
                        journal = AsyncClaimJournal(local, executor)
                        try:
                            yield RequestWorkerClaim(
                                PrepareWorkerClaim(journal), gateway, authorization
                            )
                        finally:
                            journal.stop_admission()
                            await loop.run_in_executor(executor, local.close)

            def open_worker(
                transport: httpx.AsyncBaseTransport,
            ) -> AbstractAsyncContextManager[RequestWorkerClaim]:
                return (
                    open_worker_claim_client(
                        settings, state_directory=private, transport=transport
                    )
                    if native
                    else portable(transport)
                )

            async with app.router.lifespan_context(app):
                lost = LostResponse(httpx.ASGITransport(app))
                async with open_worker(lost) as worker:
                    assert preparer.calls == 0
                    with pytest.raises(ControlUnavailable):
                        await worker.execute()
                    pending = await worker.recover()
                    assert pending.phase is WorkerRecoveryPhase.PENDING
                    assert pending.request is not None
                    original_id = pending.request.claim_request_id
                    original_proof = pending.request.proof.reveal()
                    assert preparer.calls == 1
                recovered_transport = LostResponse(httpx.ASGITransport(app))
                recovered_transport.lose_next = False
                async with open_worker(recovered_transport) as worker:
                    result = await worker.execute()
                    state = await worker.recover()
                    assert result.recovered and preparer.calls == 1
                    assert result.outcome.claim_request_id == original_id
                    assert state.phase is (
                        WorkerRecoveryPhase.ASSIGNED
                        if assigned
                        else WorkerRecoveryPhase.IDLE
                    )
                    if assigned:
                        assert (
                            state.assignment is not None
                            and state.assignment.assignment is not None
                        )
                        assert state.assignment.assignment.job_id == stored.job.id
                        assert (
                            state.request is not None
                            and state.request.proof.reveal() == original_proof
                        )
                        repeated = await worker.execute()
                        assert repeated.outcome == result.outcome
                        delivered = await worker.fetch_assignment()
                        assert (
                            delivered.definition.snapshot
                            == stored.intent.stored.snapshot
                        )
                        assert (
                            delivered.definition.checks == stored.intent.policy.checks
                        )
                        assert (
                            delivered.definition.base_revision
                            == stored.job.base_revision
                        )
                        assert delivered.outcome == result.outcome
                        assert (await worker.recover()).assignment == state.assignment
                        assert preparer.calls == 1
                        intent = await worker.prepare_acknowledgement()
                        recovered_transport.lose_next = True
                        with pytest.raises(ControlUnavailable):
                            await worker.acknowledge()
                        assert (
                            await worker.recover()
                        ).phase is WorkerRecoveryPhase.ACKNOWLEDGEMENT_PENDING
                        assert (await worker.recover()).acknowledgement == intent
                    else:
                        assert (
                            state.last_no_work == result.outcome
                            and state.request is None
                        )
                    assert (
                        original_proof.encode()
                        not in (private / "journal.sqlite3").read_bytes()
                    )
                assert recovered_transport.inputs[0] == lost.inputs[0]
                if assigned:
                    final_transport = LostResponse(httpx.ASGITransport(app))
                    final_transport.lose_next = False
                    async with open_worker(final_transport) as worker:
                        acknowledged = await worker.acknowledge()
                        assert (
                            acknowledged.recovered
                            and acknowledged.lease_authority_active
                        )
                        state = await worker.recover()
                        assert state.phase is WorkerRecoveryPhase.ACKNOWLEDGED
                        assert state.completion is not None
                        assert (
                            state.completion.acknowledged_at
                            == acknowledged.acknowledged_at
                        )
                        assert state.assignment == result.outcome
                        assert (
                            state.request is not None
                            and state.request.proof.reveal() == original_proof
                        )
                        assert (
                            final_transport.inputs[0] == recovered_transport.inputs[-1]
                        )
                        repeated_ack = await worker.acknowledge()
                        assert (
                            repeated_ack.acknowledged_at == acknowledged.acknowledged_at
                        )
                        assert (await worker.execute()).outcome == result.outcome
                        assert (
                            await worker.recover()
                        ).phase is WorkerRecoveryPhase.ACKNOWLEDGED
                        assert (
                            await worker.fetch_assignment()
                        ).outcome == result.outcome
                        assert preparer.calls == 1
                assert settings.auth_token.get_secret_value() not in repr(result)

    asyncio.run(
        scenario(),
        loop_factory=asyncio.SelectorEventLoop if sys.platform == "win32" else None,
    )
