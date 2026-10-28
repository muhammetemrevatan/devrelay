"""Real private SQLite, result HTTP codecs and disposable PostgreSQL together.

The input report and controlled GitHub HTTP replies are fixtures, not evidence
of actual Codex execution or live GitHub writes.
"""

import asyncio
from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncEngine

from devrelay.api.dependencies import ClaimOperation
from devrelay.application.assignment_delivery import ReadAssignedJob
from devrelay.application.contracts.assignments import ClaimResult
from devrelay.application.contracts.claims import WorkerClaimRequest
from devrelay.application.contracts.coding_publications import PublicationPolicy
from devrelay.application.contracts.worker_recovery import AcknowledgementCompletion
from devrelay.application.contracts.workers import (
    WorkerAuthorization,
    WorkerJournalScope,
)
from devrelay.application.plan_coding_publication import build_publication_plan
from devrelay.application.ports.control_gateway import ControlUnavailable
from devrelay.application.worker_coding_results import WorkerCodingResultExchange
from devrelay.bootstrap import control
from devrelay.bootstrap.coding_publication import open_coding_publisher
from devrelay.bootstrap.control import create_app
from devrelay.config.control import ControlSettings
from devrelay.infrastructure.control.coding_results import (
    open_http_control_coding_results,
)
from devrelay.infrastructure.worker_state.journal import (
    AsyncClaimJournal,
    SqliteClaimStore,
)
from tests.integration.test_atomic_claim_persistence import run
from tests.integration.test_coding_publication_flow import (
    settings as publication_settings,
)
from tests.integration.test_coding_result_http import TOKEN, configuration
from tests.integration.test_coding_result_store import (
    Context,
    DatabaseFactory,
    context,
    projection,
)
from tests.integration.test_worker_journal import TestProtector, TestStorage
from tests.integration.test_worker_workspace_journal import inputs as workspace_inputs
from tests.unit.test_coding_publication_configuration import TOKEN as PUBLICATION_TOKEN
from tests.unit.test_github_coding_publication import GitHub
from tests.unit.test_worker_coding_results import Guard

pytestmark = pytest.mark.postgres


class LoseAcceptedResponse(httpx.AsyncBaseTransport):
    def __init__(self, app: FastAPI, *, lose: bool) -> None:
        self.inner = httpx.ASGITransport(app=app)
        self.lose = lose
        self.accepts = self.recovers = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        recovery = request.url.path.endswith("/recovery")
        self.recovers += int(recovery)
        self.accepts += int(not recovery)
        response = await self.inner.handle_async_request(request)
        if self.lose and not recovery:
            self.lose = False
            assert response.status_code == 202
            await response.aread()
            await response.aclose()
            raise httpx.ReadError(
                "Fixture response lost after acceptance.", request=request
            )
        return response

    async def aclose(self) -> None:
        await self.inner.aclose()


@asynccontextmanager
async def journal(
    path: Path, setup: Context, protector: TestProtector, *, initialize: bool
) -> AsyncIterator[AsyncClaimJournal]:
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="result-e2e")
    loop = asyncio.get_running_loop()
    delivery = await ReadAssignedJob(setup.assigned.claims, lambda _: None).execute(
        setup.request.intent.execution.acknowledgement.attempt_id,
        setup.request.claim,
        setup.authorization,
    )
    acknowledged = await setup.assigned.service.execute(
        setup.assigned.request, setup.authorization
    )
    scope = WorkerJournalScope(
        worker_id=setup.authorization.worker_id, control_url="http://127.0.0.1/"
    )

    def open_store() -> SqliteClaimStore:
        store = SqliteClaimStore(path, scope, protector, TestStorage())
        if initialize:
            request, execution = setup.request.claim, setup.request.intent.execution
            store.get_or_create_pending(request)
            store.record_outcome(request, delivery.outcome)
            store.get_or_create_acknowledgement(request, execution.acknowledgement)
            store.record_acknowledgement(
                request,
                AcknowledgementCompletion.from_result(
                    acknowledged, delivery.outcome, execution.acknowledgement
                ),
            )
            store.get_or_create_execution(request, execution)
            _, workspace, completion = workspace_inputs(store)
            store.get_or_create_workspace(request, workspace)
            store.record_workspace_completion(request, completion)
        return store

    try:
        store = await loop.run_in_executor(executor, open_store)
        adapter = AsyncClaimJournal(store, executor)
        try:
            yield adapter
        finally:
            adapter.stop_admission()
            await loop.run_in_executor(executor, store.close)
    finally:
        await asyncio.to_thread(executor.shutdown, wait=True)


@pytest.mark.parametrize("lose_response", [False, True])
def test_worker_reopens_original_report_and_recovers_accepted_and_terminal_results(
    database: DatabaseFactory,
    worker_configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    lose_response: bool,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine, repairs=2, check_count=2)
            delivery = await ReadAssignedJob(
                setup.assigned.claims, lambda _: None
            ).execute(
                setup.request.intent.execution.acknowledgement.attempt_id,
                setup.request.claim,
                setup.authorization,
            )
            reference = delivery.outcome.assignment
            assert reference is not None
            guard = Guard(reference, [])
            protector = TestProtector()

            async def claim(
                request: WorkerClaimRequest, authorization: WorkerAuthorization
            ) -> ClaimResult:
                raise AssertionError("Result recovery cannot create another claim.")

            @asynccontextmanager
            async def claims() -> AsyncIterator[ClaimOperation]:
                yield claim

            @asynccontextmanager
            async def existing_database(settings: object) -> AsyncIterator[AsyncEngine]:
                yield engine

            monkeypatch.setattr(control, "open_database_engine", existing_database)
            app = create_app(
                configuration(worker_configuration, setup), claim_resources=claims
            )
            transport = LoseAcceptedResponse(app, lose=lose_response)
            async with (
                app.router.lifespan_context(app),
                open_http_control_coding_results(
                    "http://127.0.0.1/", TOKEN, transport=transport
                ) as gateway,
            ):
                async with journal(
                    tmp_path, setup, protector, initialize=True
                ) as local:
                    exchange = WorkerCodingResultExchange(
                        local, gateway, setup.authorization
                    )
                    if lose_response:
                        with pytest.raises(ControlUnavailable):
                            await exchange.submit(
                                delivery, setup.request.intent, guard=guard
                            )
                    else:
                        accepted = await exchange.submit(
                            delivery, setup.request.intent, guard=guard
                        )
                        assert not accepted.recovered
                    await exchange.aclose()
                    pending = await local.load_result_state()
                    assert pending.intent == setup.request.intent
                    assert (pending.receipt is None) is lose_response

                # Reopen actual SQLite and recover the original request UUID. A
                # restarted journal grants no execution or native handle rights.
                async with journal(
                    tmp_path, setup, protector, initialize=False
                ) as local:
                    recovered = WorkerCodingResultExchange(
                        local, gateway, setup.authorization
                    )
                    status = await recovered.recover()
                    assert (
                        status is not None
                        and status.receipt.request_id
                        == setup.request.intent.result_request_id
                    )
                    plan = await asyncio.to_thread(
                        build_publication_plan,
                        setup.prepared,
                        status.receipt,
                        PublicationPolicy(),
                    )
                    github = GitHub(plan, token=PUBLICATION_TOKEN)
                    async with open_coding_publisher(
                        publication_settings(worker_configuration, setup),
                        engine,
                        transport=httpx.MockTransport(github),
                    ) as publisher:
                        terminal = await publisher.execute(
                            setup.prepared, setup.authorization
                        )
                    guard.active = False
                    final = await recovered.recover()
                    assert final is not None and final.terminal == terminal
                    persisted = await local.load_result_state()
                    assert persisted.terminal_status == final
                    assert (
                        persisted.original.execution == setup.request.intent.execution
                    )
                    assert persisted.original.request is not None
                    assert (
                        persisted.original.request.claim_request_id
                        == setup.request.claim.claim_request_id
                    )
                    assert persisted.original.workspace_completion is not None
                    await recovered.aclose()
                    # Current worker authority is not restarted from receipt data.
                    assert not guard.active
                    assert transport.accepts == 1 and transport.recovers == 2
                    assert sum(
                        request.method == "POST" for request in github.requests
                    ) == len(plan.writes)
                    state = await projection(engine)
                    assert (
                        state["results"]
                        == state["operations"]
                        == state["release_count"]
                        == 1
                    )

    run(scenario())
