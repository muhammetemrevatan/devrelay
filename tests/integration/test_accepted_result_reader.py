"""Accepted intent loading uses disposable PostgreSQL, never real provider I/O."""

import asyncio
import threading
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import replace
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from devrelay.application.accepted_coding_results import DispatchAcceptedCodingResult
from devrelay.application.contracts.assignments import ClaimScopeRejected
from devrelay.application.contracts.coding_publications import (
    PublicationHistory,
    PublicationPolicy,
    PublicationWriteKind,
)
from devrelay.application.contracts.coding_result_acceptance import PreparedCodingResult
from devrelay.application.contracts.coding_result_receipts import (
    CodingPublicationState,
    CodingResultUnavailable,
    TerminalCodingResult,
)
from devrelay.application.contracts.eligibility import WorkerAuthorityExpired
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.plan_coding_publication import build_publication_plan
from devrelay.bootstrap.coding_publication import open_coding_publisher
from devrelay.config.control import ControlSettings
from devrelay.infrastructure.persistence import accepted_result_reader as implementation
from devrelay.infrastructure.persistence.accepted_result_reader import (
    SqlAlchemyAcceptedCodingResultReader,
)
from devrelay.infrastructure.persistence.result_store import SqlAlchemyResultStore
from tests.integration.test_atomic_claim_persistence import run
from tests.integration.test_coding_publication_flow import settings
from tests.integration.test_coding_result_store import context, projection, publication
from tests.unit.test_coding_publication_configuration import TOKEN
from tests.unit.test_github_coding_publication import GitHub

pytestmark = pytest.mark.postgres
type DatabaseFactory = Callable[[], AbstractAsyncContextManager[AsyncEngine]]


@pytest.mark.parametrize("repairs,checks", [(0, 1), (2, 32)])
def test_read_original_bounded_definition_and_proof_free_report_off_loop(
    database: DatabaseFactory,
    monkeypatch: pytest.MonkeyPatch,
    repairs: int,
    checks: int,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine, repairs=repairs, check_count=checks)
            accepted = await setup.store.accept(setup.prepared, setup.authorization)
            reader = SqlAlchemyAcceptedCodingResultReader(
                async_sessionmaker(engine, expire_on_commit=False, autoflush=False),
                (setup.assigned.stored.intent.policy,),
                setup.store,
            )
            original = implementation._prepare
            main_thread = threading.get_ident()
            threads: list[int] = []
            statements: list[str] = []

            def capture(
                source: implementation._AcceptedInput,
            ) -> PreparedCodingResult:
                threads.append(threading.get_ident())
                return original(source)

            def sql(
                connection: object,
                cursor: object,
                statement: str,
                parameters: object,
                execution_context: object,
                executemany: object,
            ) -> None:
                statements.append(statement)

            monkeypatch.setattr(implementation, "_prepare", capture)
            event.listen(engine.sync_engine, "before_cursor_execute", sql)
            try:
                loaded = await reader.read(
                    accepted.receipt.result_id, setup.authorization
                )
            finally:
                event.remove(engine.sync_engine, "before_cursor_execute", sql)
            assert loaded is not None
            assert loaded.payload_bytes == setup.prepared.payload_bytes
            assert (
                loaded.definition.fingerprint()
                == setup.prepared.definition.fingerprint()
            )
            loaded.identity.claim.require_replay(setup.prepared.identity.claim)
            assert len(loaded.cycle_checks) == repairs + 1
            assert threads and all(identity != main_thread for identity in threads)
            assert len(statements) <= 32
            assert not any(
                sql.lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE"))
                for sql in statements
            )
            assert (
                sum("SELECT attempt_results.payload_bytes" in sql for sql in statements)
                == 1
            )

    run(scenario())


def test_current_scope_and_unknown_worker_hide_result_before_payload(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine)
            accepted = await setup.store.accept(setup.prepared, setup.authorization)
            sessions = async_sessionmaker(
                engine, expire_on_commit=False, autoflush=False
            )
            reader = SqlAlchemyAcceptedCodingResultReader(
                sessions, (setup.assigned.stored.intent.policy,), setup.store
            )
            assert await reader.read(uuid4(), setup.authorization) is None
            assert (
                await reader.read(
                    accepted.receipt.result_id,
                    replace(setup.authorization, worker_id=uuid4()),
                )
                is None
            )
            with pytest.raises(ClaimScopeRejected):
                await reader.read(
                    accepted.receipt.result_id,
                    replace(
                        setup.authorization, allowed_repository_ids=frozenset({uuid4()})
                    ),
                )
            with pytest.raises(WorkerAuthorityExpired):
                await reader.read(
                    accepted.receipt.result_id,
                    replace(
                        setup.authorization,
                        credential_expires_at=setup.prepared.definition.created_at,
                    ),
                )

    run(scenario())


def test_historical_finalized_result_is_readable_without_reopening_operation(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine)
            history = await publication(engine, setup)
            terminal = await setup.store.finalize_publication(
                setup.prepared, setup.authorization, publication=history.verified
            )
            reader = SqlAlchemyAcceptedCodingResultReader(
                async_sessionmaker(engine, expire_on_commit=False, autoflush=False),
                (setup.assigned.stored.intent.policy,),
                setup.store,
            )
            loaded = await reader.read(
                history.accepted.receipt.result_id, setup.authorization
            )
            assert (
                loaded is not None
                and terminal.receipt.result_id == history.accepted.receipt.result_id
            )
            current = await setup.store.read_publication(loaded, setup.authorization)
            assert current is not None and current.status.terminal == terminal

    run(scenario())


def test_wrong_current_numeric_repository_binding_fails_closed(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine)
            accepted = await setup.store.accept(setup.prepared, setup.authorization)
            policy = replace(
                setup.assigned.stored.intent.policy, provider_repository_id=987654
            )
            reader = SqlAlchemyAcceptedCodingResultReader(
                async_sessionmaker(engine, expire_on_commit=False, autoflush=False),
                (policy,),
                setup.store,
            )
            with pytest.raises(CodingResultUnavailable):
                await reader.read(accepted.receipt.result_id, setup.authorization)

    run(scenario())


def test_final_authentication_occurs_after_off_loop_preflight(
    database: DatabaseFactory,
) -> None:
    class ExpiredFinalRead(SqlAlchemyResultStore):
        async def read_publication(
            self, prepared: PreparedCodingResult, authorization: WorkerAuthorization
        ) -> PublicationHistory | None:
            return await super().read_publication(
                prepared,
                replace(
                    authorization, credential_expires_at=prepared.definition.created_at
                ),
            )

    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine)
            accepted = await setup.store.accept(setup.prepared, setup.authorization)
            sessions = async_sessionmaker(
                engine, expire_on_commit=False, autoflush=False
            )
            policies = (setup.assigned.stored.intent.policy,)
            reader = SqlAlchemyAcceptedCodingResultReader(
                sessions, policies, ExpiredFinalRead(sessions, policies)
            )
            with pytest.raises(WorkerAuthorityExpired):
                await reader.read(accepted.receipt.result_id, setup.authorization)

    run(scenario())


def test_oversize_metadata_is_rejected_before_any_payload_fetch(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine)
            accepted = await setup.store.accept(setup.prepared, setup.authorization)
            reader = SqlAlchemyAcceptedCodingResultReader(
                async_sessionmaker(engine, expire_on_commit=False, autoflush=False),
                (setup.assigned.stored.intent.policy,),
                setup.store,
            )
            statements: list[str] = []

            def oversized(
                connection: object,
                cursor: object,
                statement: str,
                parameters: object,
                execution_context: object,
                executemany: object,
            ) -> tuple[str, object]:
                statements.append(statement)
                return statement.replace(
                    "octet_length(attempt_results.payload_bytes) AS octet_length_1",
                    "16777217 AS octet_length_1",
                ), parameters

            event.listen(
                engine.sync_engine, "before_cursor_execute", oversized, retval=True
            )
            try:
                with pytest.raises(CodingResultUnavailable):
                    await reader.read(accepted.receipt.result_id, setup.authorization)
            finally:
                event.remove(engine.sync_engine, "before_cursor_execute", oversized)
            assert not any(
                "SELECT attempt_results.payload_bytes" in sql for sql in statements
            )

    run(scenario())


def test_durable_reader_dispatch_controlled_github_terminal_and_replay(
    database: DatabaseFactory, worker_configuration: ControlSettings
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine, repairs=2, check_count=2)
            accepted = await setup.store.accept(setup.prepared, setup.authorization)
            plan = await asyncio.to_thread(
                build_publication_plan,
                setup.prepared,
                accepted.receipt,
                PublicationPolicy(),
            )
            github = GitHub(plan, token=TOKEN)
            reader = SqlAlchemyAcceptedCodingResultReader(
                async_sessionmaker(engine, expire_on_commit=False, autoflush=False),
                (setup.assigned.stored.intent.policy,),
                setup.store,
            )
            async with open_coding_publisher(
                settings(worker_configuration, setup),
                engine,
                transport=httpx.MockTransport(github),
            ) as publisher:
                service = DispatchAcceptedCodingResult(reader, publisher)
                assert github.requests == []
                terminal = await service.execute(
                    accepted.receipt.result_id, setup.authorization
                )
                posts = sum(request.method == "POST" for request in github.requests)
                replay = await service.execute(
                    accepted.receipt.result_id, setup.authorization
                )
            state = await projection(engine)
            assert terminal == replay and terminal.reference.number == 42
            assert terminal.reference.head_revision == plan.head_revision
            assert posts == len(plan.writes)
            assert sum(request.method == "POST" for request in github.requests) == posts
            assert (
                state["status"],
                state["attempt_status"],
                state["release_count"],
            ) == ("awaiting_review", "succeeded", 1)
            assert state["reserved_attempt_id"] is None
            assert state["results"] == state["operations"] == 1

    run(scenario())


@pytest.mark.parametrize(
    "kind", [PublicationWriteKind.BRANCH, PublicationWriteKind.PULL_REQUEST]
)
def test_explicit_reconcile_observes_lost_effect_without_new_mutation(
    database: DatabaseFactory,
    worker_configuration: ControlSettings,
    kind: PublicationWriteKind,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine)
            accepted = await setup.store.accept(setup.prepared, setup.authorization)
            plan = await asyncio.to_thread(
                build_publication_plan,
                setup.prepared,
                accepted.receipt,
                PublicationPolicy(),
            )
            github = GitHub(plan, token=TOKEN)
            target = next(write for write in plan.writes if write.kind is kind)
            lost = False

            def lose(
                request: httpx.Request, response: httpx.Response
            ) -> httpx.Response:
                nonlocal lost
                if (
                    not lost
                    and request.method == "POST"
                    and request.content == target.body
                ):
                    lost = True
                    raise httpx.ReadError("Controlled lost response.", request=request)
                return response

            github.hook = lose
            reader = SqlAlchemyAcceptedCodingResultReader(
                async_sessionmaker(engine, expire_on_commit=False, autoflush=False),
                (setup.assigned.stored.intent.policy,),
                setup.store,
            )
            async with open_coding_publisher(
                settings(worker_configuration, setup),
                engine,
                transport=httpx.MockTransport(github),
            ) as publisher:
                service = DispatchAcceptedCodingResult(reader, publisher)
                with pytest.raises(CodingResultUnavailable):
                    await service.execute(
                        accepted.receipt.result_id, setup.authorization
                    )
                posts = sum(request.method == "POST" for request in github.requests)
                reconciled = await service.reconcile(
                    accepted.receipt.result_id, setup.authorization
                )
                repeated = await service.reconcile(
                    accepted.receipt.result_id, setup.authorization
                )
                with pytest.raises(CodingResultUnavailable):
                    await service.execute(
                        accepted.receipt.result_id, setup.authorization
                    )
            history = await setup.store.read_publication(
                setup.prepared, setup.authorization
            )
            state = await projection(engine)
            assert lost and reconciled is not None and repeated == reconciled
            assert (
                reconciled.state is CodingPublicationState.UNCERTAIN
                and reconciled.terminal is None
            )
            assert history is not None and history.last_observation_digest is not None
            assert (
                history.last_step is not None
                and history.last_step.step_id == target.write_id
            )
            assert sum(request.method == "POST" for request in github.requests) == posts
            assert (
                sum(
                    request.method == "POST" and request.content == target.body
                    for request in github.requests
                )
                == 1
            )
            assert state["release_count"] == 0
            assert state["reserved_attempt_id"] == accepted.receipt.attempt_id

    run(scenario())


def test_cancelled_reader_preflight_cannot_publish_and_later_explicit_dispatch_succeeds(
    database: DatabaseFactory,
    worker_configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine)
            accepted = await setup.store.accept(setup.prepared, setup.authorization)
            plan = await asyncio.to_thread(
                build_publication_plan,
                setup.prepared,
                accepted.receipt,
                PublicationPolicy(),
            )
            github = GitHub(plan, token=TOKEN)
            reader = SqlAlchemyAcceptedCodingResultReader(
                async_sessionmaker(engine, expire_on_commit=False, autoflush=False),
                (setup.assigned.stored.intent.policy,),
                setup.store,
            )
            entered, release, finished = (
                threading.Event(),
                threading.Event(),
                threading.Event(),
            )
            prepare = implementation._prepare

            def held(source: implementation._AcceptedInput) -> PreparedCodingResult:
                entered.set()
                try:
                    if not release.wait(timeout=10):
                        raise AssertionError("Controlled preflight was not released.")
                    return prepare(source)
                finally:
                    finished.set()

            async with open_coding_publisher(
                settings(worker_configuration, setup),
                engine,
                transport=httpx.MockTransport(github),
            ) as publisher:
                publish_calls: list[PreparedCodingResult] = []
                execute = publisher.execute

                async def publish(
                    prepared: PreparedCodingResult,
                    authorization: WorkerAuthorization,
                ) -> TerminalCodingResult:
                    publish_calls.append(prepared)
                    return await execute(prepared, authorization)

                monkeypatch.setattr(publisher, "execute", publish)
                monkeypatch.setattr(implementation, "_prepare", held)
                service = DispatchAcceptedCodingResult(reader, publisher)
                task = asyncio.create_task(
                    service.execute(accepted.receipt.result_id, setup.authorization)
                )

                try:
                    started = await asyncio.to_thread(entered.wait, 5)
                    assert started
                    task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await asyncio.wait_for(task, timeout=5)
                finally:
                    release.set()
                    if not task.done():
                        task.cancel()
                        await asyncio.wait_for(
                            asyncio.gather(task, return_exceptions=True), timeout=5
                        )
                    if entered.is_set():
                        settled = await asyncio.to_thread(finished.wait, 5)
                        assert settled
                    monkeypatch.setattr(implementation, "_prepare", prepare)

                history = await setup.store.read_publication(
                    setup.prepared, setup.authorization
                )
                state = await projection(engine)
                assert publish_calls == [] and github.requests == []
                assert history is not None and history.invocation_id is None
                assert history.status.state is CodingPublicationState.ACCEPTED
                assert history.status.operation_version == 0
                assert state["operation_events"] == 1 and state["release_count"] == 0

                terminal = await service.execute(
                    accepted.receipt.result_id, setup.authorization
                )

                assert len(publish_calls) == 1
                assert terminal.reference.head_revision == plan.head_revision
                assert sum(
                    request.method == "POST" for request in github.requests
                ) == len(plan.writes)

    run(scenario())
