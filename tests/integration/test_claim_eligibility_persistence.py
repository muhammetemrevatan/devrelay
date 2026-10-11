import asyncio
import json
import sys
from collections.abc import Callable, Coroutine
from contextlib import AbstractAsyncContextManager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import event, select, text, update
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.pool import AsyncAdaptedQueuePool

from devrelay.application.claim_eligibility import PrepareJobClaim
from devrelay.application.contracts.submissions import (
    RepositoryHead,
    RepositorySubmissionObservation,
    SubmissionAuthorization,
)
from devrelay.application.contracts.tasks import TaskRead
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.job_persistence import JobPersistenceError
from devrelay.bootstrap.claims import open_claim_preparer
from devrelay.bootstrap.jobs import open_github_job_submitter
from devrelay.bootstrap.persistence import (
    job_submission_reader,
    queued_job_reader,
)
from devrelay.bootstrap.tasks import open_task_importer
from devrelay.config.control import ControlSettings
from devrelay.domain.tasks import TaskIdentity
from devrelay.infrastructure.persistence.job_models import JobRecord
from tests.integration.job_fixtures import save_job

pytestmark = pytest.mark.postgres
DatabaseFactory = Callable[[], AbstractAsyncContextManager[AsyncEngine]]


def run(scenario: Coroutine[object, object, None]) -> None:
    asyncio.run(
        scenario,
        loop_factory=asyncio.SelectorEventLoop if sys.platform == "win32" else None,
    )


def assert_pool_released(engine: AsyncEngine) -> None:
    assert isinstance(engine.sync_engine.pool, AsyncAdaptedQueuePool)
    assert engine.sync_engine.pool.checkedout() == 0


@pytest.mark.parametrize("check_count", [1, 32])
def test_queue_selection_has_constant_budget_and_releases_session(
    database: DatabaseFactory, check_count: int
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            stored = await save_job(engine, check_count=check_count)
            await save_job(engine, repository=stored.intent.policy)
            reader = queued_job_reader(engine)
            statements: list[str] = []

            def count_query(
                connection: Connection,
                cursor: object,
                statement: str,
                parameters: object,
                context: object,
                executemany: bool,
            ) -> None:
                statements.append(statement)

            event.listen(engine.sync_engine, "before_cursor_execute", count_query)
            try:
                result = await reader.find_candidate(
                    repository_ids=frozenset({stored.job.repository_id})
                )

                assert result == stored
                assert len(statements) == 3
                assert "LIMIT" in statements[0]
                assert "ORDER BY" in statements[0]
                assert "FOR UPDATE" not in " ".join(statements)
                assert_pool_released(engine)
            finally:
                event.remove(engine.sync_engine, "before_cursor_execute", count_query)
            async with engine.connect() as connection:
                assert (
                    await connection.scalar(
                        select(JobRecord.status).where(JobRecord.id == stored.job.id)
                    )
                ) == "queued"

    run(scenario())


def test_queue_order_is_stable_when_creation_times_tie(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            now = datetime.now(UTC)
            first = await save_job(engine, created_at=now, job_id=UUID(int=2))
            second = await save_job(
                engine,
                repository=first.intent.policy,
                created_at=now,
                job_id=UUID(int=1),
            )
            reader = queued_job_reader(engine)

            result = await reader.find_candidate(
                repository_ids=frozenset({first.job.repository_id})
            )

            assert result == second
            assert_pool_released(engine)

    run(scenario())


def test_queue_excludes_other_repository_and_empty_queue_costs_one_query(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            await save_job(engine)
            statements: list[str] = []

            def count_query(
                connection: Connection,
                cursor: object,
                statement: str,
                parameters: object,
                context: object,
                executemany: bool,
            ) -> None:
                statements.append(statement)

            reader = queued_job_reader(engine)
            event.listen(engine.sync_engine, "before_cursor_execute", count_query)
            try:
                result = await reader.find_candidate(
                    repository_ids=frozenset({uuid4()})
                )

                assert result is None
                assert len(statements) == 1
                assert_pool_released(engine)
            finally:
                event.remove(engine.sync_engine, "before_cursor_execute", count_query)

    run(scenario())


def test_queue_never_selects_legacy_unbound_definitions(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            stored = await save_job(engine)
            legacy = replace(
                stored.intent,
                policy=replace(stored.intent.policy, provider_repository_id=None),
            )
            async with engine.begin() as connection:
                await connection.execute(
                    text("ALTER TABLE jobs DISABLE TRIGGER jobs_no_mutation")
                )
                await connection.execute(
                    update(JobRecord)
                    .where(JobRecord.id == stored.job.id)
                    .values(
                        definition_version=1,
                        provider_repository_id=None,
                        submission_fingerprint=legacy.submission_fingerprint,
                    )
                )
                await connection.execute(
                    text("ALTER TABLE jobs ENABLE TRIGGER jobs_no_mutation")
                )
            legacy_record = await job_submission_reader(engine).find_by_key(
                legacy.submission_key
            )
            assert legacy_record is not None
            assert legacy_record.intent.policy.provider_repository_id is None
            reader = queued_job_reader(engine)

            result = await reader.find_candidate(
                repository_ids=frozenset({stored.job.repository_id})
            )

            assert result is None
            assert_pool_released(engine)

    run(scenario())


@pytest.mark.parametrize(
    "scope",
    [
        frozenset(),
        frozenset(uuid4() for _ in range(33)),
        frozenset({"not-a-uuid"}),
        (uuid4(),),
    ],
)
def test_queue_rejects_unbounded_or_untyped_scope_before_db(
    database: DatabaseFactory, scope: object
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            reader = queued_job_reader(engine)

            with pytest.raises(JobPersistenceError):
                await reader.find_candidate(repository_ids=cast(frozenset[UUID], scope))

            assert_pool_released(engine)

    run(scenario())


def test_preparation_provider_reads_run_with_no_checked_out_connection(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            stored = await save_job(engine)

            class Source:
                async def read_by_identity(self, identity: TaskIdentity) -> TaskRead:
                    assert_pool_released(engine)
                    assert identity == stored.intent.stored.snapshot.identity
                    return TaskRead(
                        snapshot=stored.intent.stored.snapshot,
                        scope_key="DRY",
                        labels=frozenset({"agent-ready"}),
                    )

            class Git:
                async def observe(
                    self,
                    *,
                    repository_id: UUID,
                    target_branch: str,
                    source_task_id: UUID,
                ) -> RepositorySubmissionObservation:
                    assert_pool_released(engine)
                    return RepositorySubmissionObservation(
                        head=RepositoryHead(
                            repository_id=repository_id,
                            provider_repository_id=123,
                            target_branch=target_branch,
                            revision=stored.job.base_revision,
                        )
                    )

            authorization = WorkerAuthorization(
                worker_id=uuid4(),
                allowed_repository_ids=frozenset({stored.job.repository_id}),
                credential_expires_at=datetime.now(UTC) + timedelta(minutes=2),
            )
            service = PrepareJobClaim(
                queued_job_reader(engine), Source(), Git(), (stored.intent.policy,)
            )

            result = await service.execute(authorization)

            assert result is not None
            assert result.stored == stored
            assert_pool_released(engine)

    run(scenario())


def test_queue_materialization_failure_releases_connection_and_hides_sql(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            stored = await save_job(engine)
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "ALTER TABLE job_workflow_events DISABLE TRIGGER job_workflow_events_no_mutation"
                    )
                )
                await connection.execute(
                    text(
                        "UPDATE job_workflow_events SET occurred_at=occurred_at + interval '1 second'"
                    )
                )
                await connection.execute(
                    text(
                        "ALTER TABLE job_workflow_events ENABLE TRIGGER job_workflow_events_no_mutation"
                    )
                )
            reader = queued_job_reader(engine)

            with pytest.raises(JobPersistenceError) as caught:
                await reader.find_candidate(
                    repository_ids=frozenset({stored.job.repository_id})
                )

            assert "history" in str(caught.value)
            assert "SELECT" not in str(caught.value)
            assert_pool_released(engine)

    run(scenario())


def test_claim_preparer_composes_real_adapters_with_bounded_fixture_transport(
    database: DatabaseFactory, submission_configuration: ControlSettings
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            configuration = submission_configuration
            assert configuration.jira is not None and configuration.github is not None
            jira, github = configuration.jira, configuration.github
            repository = configuration.repositories[0]
            payload = json.loads(
                (
                    Path(__file__).resolve().parents[1]
                    / "contract/fixtures/jira/issue.json"
                ).read_text()
            )
            calls: list[str] = []

            def respond(request: httpx.Request) -> httpx.Response:
                assert_pool_released(engine)
                assert request.method == "GET"
                if request.url.host == "api.atlassian.com":
                    calls.append("jira")
                    return httpx.Response(200, json=payload)
                calls.append("github")
                if request.url.path.endswith("/pulls"):
                    return httpx.Response(200, json=[])
                if "/git/ref/" in request.url.path:
                    return httpx.Response(
                        200,
                        json={
                            "ref": "refs/heads/master",
                            "object": {"type": "commit", "sha": "a" * 40},
                        },
                    )
                return httpx.Response(
                    200,
                    json={
                        "id": 123,
                        "full_name": "example/devrelay",
                        "archived": False,
                        "disabled": False,
                    },
                )

            transport = httpx.MockTransport(respond)
            async with open_task_importer(
                jira, engine, transport=transport
            ) as importer:
                imported = await importer.execute("DRY-7")
            async with open_github_job_submitter(
                jira,
                github,
                configuration.repositories,
                engine,
                jira_transport=transport,
                github_transport=transport,
            ) as submitter:
                saved = await submitter.execute(
                    snapshot_id=imported.saved.snapshot.id,
                    repository_id=repository.repository_id,
                    authorization=SubmissionAuthorization(
                        actor_id="test-operator",
                        allowed_repository_ids=frozenset({repository.repository_id}),
                    ),
                )
            calls.clear()
            authorization = WorkerAuthorization(
                worker_id=uuid4(),
                allowed_repository_ids=frozenset({repository.repository_id}),
                credential_expires_at=datetime.now(UTC) + timedelta(minutes=2),
            )
            async with open_claim_preparer(
                jira,
                github,
                configuration.repositories,
                engine,
                jira_transport=transport,
                github_transport=transport,
            ) as service:
                result = await service.execute(authorization)

            assert result is not None
            assert result.stored == saved.stored
            assert calls == ["jira", "github", "github", "github", "github"]
            assert_pool_released(engine)

    run(scenario())
