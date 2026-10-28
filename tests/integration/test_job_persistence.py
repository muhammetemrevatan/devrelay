import asyncio
import sys
from collections.abc import Callable, Coroutine
from contextlib import AbstractAsyncContextManager
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from alembic import command
from alembic.config import Config
from pydantic import SecretStr
from sqlalchemy import delete, event, insert, select, text, update
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine
from sqlalchemy.pool import AsyncAdaptedQueuePool

from devrelay.application.contracts.submissions import (
    CheckPolicy,
    PreparedJobSubmission,
    RepositoryHead,
    RepositoryPolicy,
    RepositorySubmissionObservation,
    SavedJobSubmission,
    SubmissionAuthorization,
)
from devrelay.application.contracts.tasks import StoredTaskSnapshot, TaskRead
from devrelay.application.jobs import (
    JobSubmissionConflict,
    PrepareJobSubmission,
    SubmitJob,
)
from devrelay.application.ports.job_persistence import JobPersistenceError
from devrelay.application.tasks import TaskImportPolicy
from devrelay.bootstrap.github import open_github_repository_source
from devrelay.bootstrap.jobs import open_github_job_submitter
from devrelay.bootstrap.persistence import (
    job_branch_reader,
    job_submission_reader,
    job_submission_unit_of_work_factory,
    task_snapshot_reader,
    task_unit_of_work_factory,
)
from devrelay.config.github import GitHubRepositoryId, GitHubSettings
from devrelay.config.jira import JiraSettings
from devrelay.config.repositories import CheckSettings, RepositorySettings
from devrelay.domain.tasks import TaskIdentity, TaskSnapshot
from devrelay.infrastructure.persistence.job_models import (
    JobCheckRecord,
    JobRecord,
    RepositoryBindingRecord,
    RepositoryRecord,
)
from devrelay.infrastructure.persistence.models import (
    SourceTaskRecord,
    TaskSnapshotRecord,
)
from devrelay.infrastructure.persistence.queued_job_reader import queued_candidate_query
from devrelay.infrastructure.persistence.workflow_models import (
    JobWorkflowEventRecord,
    JobWorkflowRecord,
)

pytestmark = pytest.mark.postgres
DatabaseFactory = Callable[[], AbstractAsyncContextManager[AsyncEngine]]


def run(scenario: Coroutine[object, object, None]) -> None:
    asyncio.run(
        scenario,
        loop_factory=asyncio.SelectorEventLoop if sys.platform == "win32" else None,
    )


async def save_task(
    engine: AsyncEngine,
    external: str = "10005",
    title: str = "Import a task",
    *,
    site_id: str = "test-site",
) -> StoredTaskSnapshot:
    snapshot = TaskSnapshot(
        id=uuid4(),
        identity=TaskIdentity(provider="jira", site_id=site_id, external_id=external),
        display_key="DRY-5",
        title=title,
        description="Preserve requirements.",
        acceptance_criteria=("Save once.",),
        captured_at=datetime.now(UTC),
    )
    async with task_unit_of_work_factory(engine)() as unit:
        saved = await unit.tasks.save_snapshot(snapshot)
        await unit.commit()
    return StoredTaskSnapshot(
        source_task_id=saved.source_task_id, snapshot=saved.snapshot
    )


def policy(check_count: int = 1) -> RepositoryPolicy:
    return RepositoryPolicy(
        provider_repository_id=123,
        repository_id=uuid4(),
        slug="example/devrelay",
        enabled=True,
        target_branch="main",
        coding_branch_prefix="devrelay/jobs",
        version="v1",
        task_scope=TaskImportPolicy(
            provider="jira",
            site_id="test-site",
            allowed_scope_keys=frozenset({"DRY"}),
        ),
        checks=tuple(
            CheckPolicy(
                id=f"check_{index}",
                argv=("uv", "run", "--locked", "pytest"),
                working_directory=".",
                timeout_seconds=300,
            )
            for index in range(check_count)
        ),
        max_repair_cycles=2,
    )


def prepared(
    stored: StoredTaskSnapshot, repository: RepositoryPolicy
) -> PreparedJobSubmission:
    return PreparedJobSubmission(
        stored=stored, policy=repository, actor_id="test-operator"
    )


async def record(
    engine: AsyncEngine, intent: PreparedJobSubmission, *, revision: str = "a" * 40
) -> SavedJobSubmission:
    assert intent.policy.provider_repository_id is not None
    candidate = intent.build_candidate(
        job_id=uuid4(),
        head=RepositoryHead(
            provider_repository_id=intent.policy.provider_repository_id,
            repository_id=intent.policy.repository_id,
            target_branch="main",
            revision=revision,
        ),
        now=datetime.now(UTC),
    )
    async with job_submission_unit_of_work_factory(engine)() as unit:
        saved = await unit.jobs.admit(intent, candidate)
        await unit.commit()
    return saved


async def counts(engine: AsyncEngine) -> tuple[int, int, int]:
    async with engine.connect() as connection:
        row = (
            await connection.execute(
                text(
                    "SELECT (SELECT count(*) FROM repositories), (SELECT count(*) FROM jobs), (SELECT count(*) FROM job_checks)"
                )
            )
        ).one()
        return int(row[0]), int(row[1]), int(row[2])


async def seed_workflows(
    connection: AsyncConnection, definitions: list[dict[str, Any]]
) -> None:
    await connection.execute(
        insert(JobWorkflowRecord),
        [
            {
                "job_id": item["id"],
                "source_task_id": item["source_task_id"],
                "repository_id": item["repository_id"],
                "status": "queued",
                "workflow_version": 0,
                "blocks_new_submission": True,
                "created_at": item["created_at"],
                "updated_at": item["created_at"],
            }
            for item in definitions
        ],
    )
    await connection.execute(
        insert(JobWorkflowEventRecord),
        [
            {
                "job_id": item["id"],
                "workflow_version": 0,
                "status": "queued",
                "event_type": "submitted",
                "occurred_at": item["created_at"],
            }
            for item in definitions
        ],
    )


class FreshSource:
    def __init__(
        self,
        stored: StoredTaskSnapshot,
        engine: AsyncEngine,
        *,
        gate: asyncio.Barrier | None = None,
    ) -> None:
        self.stored, self.engine, self.gate = stored, engine, gate
        self.reads = 0
        self.ready = True

    async def read_by_identity(self, identity: TaskIdentity) -> TaskRead:
        self.reads += 1
        if self.gate is None:
            assert isinstance(self.engine.sync_engine.pool, AsyncAdaptedQueuePool)
            assert self.engine.sync_engine.pool.checkedout() == 0
        else:
            await self.gate.wait()
        return TaskRead(
            snapshot=self.stored.snapshot,
            scope_key="DRY",
            labels=frozenset({"agent-ready"}) if self.ready else frozenset(),
        )


class GitSource:
    def __init__(self, engine: AsyncEngine, *, concurrent: bool = False) -> None:
        self.engine, self.concurrent = engine, concurrent
        self.reads = 0

    async def observe(
        self, *, repository_id: UUID, target_branch: str, source_task_id: UUID
    ) -> RepositorySubmissionObservation:
        self.reads += 1
        if not self.concurrent:
            assert isinstance(self.engine.sync_engine.pool, AsyncAdaptedQueuePool)
            assert self.engine.sync_engine.pool.checkedout() == 0
        return RepositorySubmissionObservation(
            head=RepositoryHead(
                provider_repository_id=123,
                repository_id=repository_id,
                target_branch=target_branch,
                revision="a" * 40,
            )
        )


def submitter(
    engine: AsyncEngine,
    repository: RepositoryPolicy,
    source: FreshSource,
    git: GitSource,
) -> SubmitJob:
    return SubmitJob(
        PrepareJobSubmission(task_snapshot_reader(engine), source, (repository,)),
        job_submission_reader(engine),
        job_submission_unit_of_work_factory(engine),
        git,
    )


async def submit(
    service: SubmitJob, stored: StoredTaskSnapshot, repository: RepositoryPolicy
) -> SavedJobSubmission:
    return await service.execute(
        snapshot_id=stored.snapshot.id,
        repository_id=repository.repository_id,
        authorization=SubmissionAuthorization(
            actor_id="test-operator",
            allowed_repository_ids=frozenset({repository.repository_id}),
        ),
    )


@pytest.mark.parametrize("check_count", [1, 32])
def test_complete_policy_round_trip_and_constant_query_budget(
    database: DatabaseFactory, check_count: int
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            stored = await save_task(engine)
            intent = prepared(stored, policy(check_count))
            statements: list[str] = []

            # Driver event parameters are localized external data.
            def observe(
                connection: Any,
                cursor: Any,
                statement: str,
                parameters: Any,
                context: Any,
                executemany: bool,
            ) -> None:
                if any(
                    table in statement
                    for table in (
                        "source_tasks",
                        "task_snapshots",
                        "repositories",
                        "repository_bindings",
                        "jobs",
                        "job_checks",
                        "job_workflows",
                        "job_workflow_events",
                    )
                ):
                    statements.append(statement)

            event.listen(engine.sync_engine, "before_cursor_execute", observe)
            try:
                first = await record(engine, intent)
                assert first.created
                assert len(statements) == 10
                statements.clear()
                read = await job_submission_reader(engine).find_by_key(
                    intent.submission_key
                )
                assert read == first.stored
                assert len(statements) == 2
                assert (
                    read is not None
                    and read.intent.policy.checks == intent.policy.checks
                )
                assert read.intent.actor_id == "test-operator"
                assert read.workflow == first.stored.workflow
                assert (
                    read.workflow.version == 0 and read.workflow.blocks_new_submission
                )
                assert len(statements) == 2
                statements.clear()
                second = await record(
                    engine,
                    replace(intent, actor_id="another-operator"),
                    revision="b" * 40,
                )
                assert not second.created and second.stored == first.stored
                assert len(statements) == 3
                statements.clear()
                assert await job_submission_reader(engine).find_by_key("f" * 64) is None
                assert len(statements) == 1
            finally:
                event.remove(engine.sync_engine, "before_cursor_execute", observe)
            assert await counts(engine) == (1, 1, check_count)

    run(scenario())


@pytest.mark.parametrize("missing", ["job_workflows", "job_workflow_events"])
def test_incomplete_workflow_submission_cannot_commit(
    database: DatabaseFactory, missing: str
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            intent = prepared(await save_task(engine), policy())

            def skip(
                connection: Any,
                cursor: Any,
                statement: str,
                parameters: Any,
                context: Any,
                executemany: bool,
            ) -> tuple[str, Any]:
                if statement.startswith("INSERT INTO " + missing + " "):
                    return "SELECT 1", ()
                return statement, parameters

            event.listen(engine.sync_engine, "before_cursor_execute", skip, retval=True)
            try:
                with pytest.raises(JobPersistenceError, match="could not commit"):
                    await record(engine, intent)
            finally:
                event.remove(engine.sync_engine, "before_cursor_execute", skip)

            assert await counts(engine) == (0, 0, 0)
            async with engine.connect() as connection:
                row = (
                    await connection.execute(
                        text(
                            "SELECT (SELECT count(*) FROM job_workflows), (SELECT count(*) FROM job_workflow_events)"
                        )
                    )
                ).one()
                assert tuple(row) == (0, 0)

    run(scenario())


@pytest.mark.parametrize("table", ["job_workflows", "job_workflow_events"])
@pytest.mark.parametrize("operation", ["update", "delete"])
def test_workflow_and_creation_history_reject_unimplemented_mutations(
    database: DatabaseFactory, table: str, operation: str
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            intent = prepared(await save_task(engine), policy())
            saved = await record(engine, intent)
            model = (
                JobWorkflowRecord
                if table == "job_workflows"
                else JobWorkflowEventRecord
            )
            statement = (
                update(model).values(status="queued")
                if operation == "update"
                else delete(model)
            )

            with pytest.raises(IntegrityError):
                async with engine.begin() as connection:
                    await connection.execute(statement)

            restored = await job_submission_reader(engine).find_by_key(
                intent.submission_key
            )
            assert restored == saved.stored

    run(scenario())


@pytest.mark.parametrize(
    "damage", ["missing_workflow", "missing_event", "workflow_time", "event_time"]
)
def test_reader_fails_closed_on_privileged_workflow_corruption(
    database: DatabaseFactory, damage: str
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            intent = prepared(await save_task(engine), policy())
            await record(engine, intent)
            async with engine.begin() as connection:
                if damage == "missing_workflow":
                    await connection.execute(
                        text("ALTER TABLE jobs DROP CONSTRAINT fk_jobs_workflow")
                    )
                    await connection.execute(
                        text(
                            "ALTER TABLE job_workflow_events DROP CONSTRAINT fk_job_workflow_events_workflow"
                        )
                    )
                    await connection.execute(
                        text(
                            "DROP TRIGGER job_workflows_claim_transition ON job_workflows"
                        )
                    )
                    await connection.execute(delete(JobWorkflowRecord))
                elif damage == "missing_event":
                    await connection.execute(
                        text(
                            "ALTER TABLE job_workflows DROP CONSTRAINT fk_job_workflows_current_event"
                        )
                    )
                    await connection.execute(
                        text(
                            "DROP TRIGGER job_workflow_events_no_mutation ON job_workflow_events"
                        )
                    )
                    await connection.execute(delete(JobWorkflowEventRecord))
                elif damage == "workflow_time":
                    await connection.execute(
                        text(
                            "DROP TRIGGER job_workflows_claim_transition ON job_workflows"
                        )
                    )
                    await connection.execute(
                        text(
                            "DROP TRIGGER job_workflows_updated_history ON job_workflows"
                        )
                    )
                    await connection.execute(
                        text(
                            "UPDATE job_workflows SET created_at = created_at + interval '1 second', updated_at = updated_at + interval '1 second'"
                        )
                    )
                else:
                    await connection.execute(
                        text(
                            "DROP TRIGGER job_workflow_events_no_mutation ON job_workflow_events"
                        )
                    )
                    await connection.execute(
                        text(
                            "UPDATE job_workflow_events SET occurred_at = occurred_at + interval '1 second'"
                        )
                    )

            with pytest.raises(JobPersistenceError, match="workflow/history"):
                await job_submission_reader(engine).find_by_key(intent.submission_key)

            assert isinstance(engine.sync_engine.pool, AsyncAdaptedQueuePool)
            assert engine.sync_engine.pool.checkedout() == 0

    run(scenario())


def test_workflow_migration_preserves_bound_definitions_on_reupgrade(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            intent = prepared(await save_task(engine), policy())
            saved = await record(engine, intent)
            before = await header(engine)
            async with engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: migrate_to(
                        sync, "0003_repository_bindings", downgrade=True
                    )
                )
            async with engine.begin() as connection:
                await connection.run_sync(lambda sync: migrate_to(sync, "head"))

            restored = await job_submission_reader(engine).find_by_key(
                intent.submission_key
            )

            assert restored == saved.stored and await header(engine) == before
            async with engine.connect() as connection:
                row = (
                    await connection.execute(
                        text(
                            "SELECT (SELECT count(*) FROM job_workflows), (SELECT count(*) FROM job_workflow_events)"
                        )
                    )
                ).one()
                assert tuple(row) == (1, 1)
                indexes = set(
                    (
                        await connection.execute(
                            text(
                                "SELECT indexname FROM pg_indexes WHERE schemaname = current_schema()"
                            )
                        )
                    ).scalars()
                )
                assert "uq_job_workflows_unresolved_task_repository" in indexes
                assert "uq_jobs_unresolved_task_repository" not in indexes

    run(scenario())


def test_workflow_downgrade_refuses_inconsistent_history(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            await record(engine, prepared(await save_task(engine), policy()))
            async with engine.begin() as connection:
                await connection.execute(
                    text("DROP TRIGGER job_workflows_claim_transition ON job_workflows")
                )
                await connection.execute(
                    text("DROP TRIGGER job_workflows_updated_history ON job_workflows")
                )
                await connection.execute(
                    text(
                        "UPDATE job_workflows SET created_at = created_at + interval '1 second', updated_at = updated_at + interval '1 second'"
                    )
                )

            with pytest.raises(IntegrityError, match="Cannot discard"):
                async with engine.begin() as connection:
                    await connection.run_sync(
                        lambda sync: migrate_to(
                            sync, "0003_repository_bindings", downgrade=True
                        )
                    )

            async with engine.connect() as connection:
                assert (
                    await connection.execute(
                        text("SELECT version_num FROM alembic_version")
                    )
                ).scalar_one() == "0009_coding_results"
                assert (
                    await connection.execute(
                        text("SELECT count(*) FROM job_workflow_events")
                    )
                ).scalar_one() == 1

    run(scenario())


@pytest.mark.parametrize("mismatch", ["source", "repository"])
def test_workflow_identity_must_match_immutable_definition(
    database: DatabaseFactory, mismatch: str
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            repository = policy()
            await record(engine, prepared(await save_task(engine), repository))
            values = await header(engine)
            task = await save_task(engine, external="10006")
            other = await save_task(engine, external="10007")
            intent = prepared(task, repository)
            now = datetime.now(UTC)
            values.update(
                id=uuid4(),
                source_task_id=task.source_task_id,
                snapshot_id=task.snapshot.id,
                submission_key=intent.submission_key,
                submission_fingerprint=intent.submission_fingerprint,
                created_at=now,
                updated_at=now,
            )
            values["authorized_branch"] = f"devrelay/jobs/{values['id'].hex}"
            forged = dict(values)
            forged["source_task_id" if mismatch == "source" else "repository_id"] = (
                other.source_task_id if mismatch == "source" else uuid4()
            )

            with pytest.raises(IntegrityError, match="fk_job_workflows_definition"):
                async with engine.begin() as connection:
                    await connection.execute(insert(JobRecord).values(**values))
                    await connection.execute(
                        insert(JobCheckRecord).values(
                            job_id=values["id"],
                            check_id="check_0",
                            check_count=1,
                            position=0,
                            argv=["uv", "run", "pytest"],
                            working_directory=".",
                            timeout_seconds=300,
                        )
                    )
                    await seed_workflows(connection, [forged])

            assert await counts(engine) == (1, 1, 1)

    run(scenario())


@pytest.mark.parametrize("collision", ["same_uuid", "same_provider_id"])
def test_concurrent_repository_binding_conflicts_roll_back_all_new_rows(
    database: DatabaseFactory, collision: str
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            repository = policy()
            other = (
                replace(repository, provider_repository_id=456)
                if collision == "same_uuid"
                else replace(repository, repository_id=uuid4(), slug="example/other")
            )
            intents = (
                prepared(await save_task(engine), repository),
                prepared(await save_task(engine, external="10006"), other),
            )

            async with asyncio.timeout(15):
                results = await asyncio.gather(
                    *(record(engine, intent) for intent in intents),
                    return_exceptions=True,
                )

            assert sum(isinstance(item, SavedJobSubmission) for item in results) == 1
            assert sum(isinstance(item, JobSubmissionConflict) for item in results) == 1
            assert await counts(engine) == (1, 1, 1)
            async with engine.connect() as connection:
                bindings = (
                    (
                        await connection.execute(
                            select(RepositoryBindingRecord.__table__)
                        )
                    )
                    .mappings()
                    .all()
                )
                job = (
                    (await connection.execute(select(JobRecord.__table__)))
                    .mappings()
                    .one()
                )
            assert len(bindings) == 1
            assert bindings[0]["repository_id"] == job["repository_id"]
            assert (
                bindings[0]["provider_repository_id"] == job["provider_repository_id"]
            )

    run(scenario())


def migrate_to(
    connection: Connection, revision: str, *, downgrade: bool = False
) -> None:
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    config.attributes["connection"] = connection
    if downgrade:
        command.downgrade(config, revision)
    else:
        command.upgrade(config, revision)


def test_binding_migration_preserves_legacy_definitions_without_backfill(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        repository = policy()
        async with database() as template_engine:
            saved = await record(
                template_engine, prepared(await save_task(template_engine), repository)
            )
            values = await header(template_engine)
        async with database() as engine:
            task = await save_task(engine)
            legacy = prepared(task, replace(repository, provider_repository_id=None))
            values.pop("provider_repository_id")
            values.update(
                source_task_id=task.source_task_id,
                snapshot_id=task.snapshot.id,
                definition_version=1,
                submission_key=legacy.submission_key,
                submission_fingerprint=legacy.submission_fingerprint,
            )
            async with engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: migrate_to(
                        sync, "0002_job_submissions", downgrade=True
                    )
                )
                await connection.execute(
                    insert(RepositoryRecord).values(
                        id=repository.repository_id,
                        provider="github",
                        slug=repository.slug,
                    )
                )
                await connection.execute(insert(JobRecord).values(**values))
                await connection.execute(
                    insert(JobCheckRecord).values(
                        job_id=values["id"],
                        check_id="check_0",
                        check_count=1,
                        position=0,
                        argv=list(repository.checks[0].argv),
                        working_directory=".",
                        timeout_seconds=300,
                    )
                )
            async with engine.begin() as connection:
                await connection.run_sync(lambda sync: migrate_to(sync, "head"))

            receipt = await job_submission_reader(engine).find_by_key(
                legacy.submission_key
            )

            assert receipt == type(saved.stored)(
                intent=legacy,
                job=replace(
                    saved.stored.job,
                    source_task_id=task.source_task_id,
                    snapshot_id=task.snapshot.id,
                ),
                workflow=saved.stored.workflow,
            )
            assert (
                receipt is not None
                and receipt.intent.policy.provider_repository_id is None
            )
            assert (
                receipt.intent.submission_fingerprint == legacy.submission_fingerprint
            )
            async with engine.connect() as connection:
                assert (
                    await connection.execute(select(RepositoryBindingRecord.__table__))
                ).all() == []
                assert (
                    await connection.execute(select(JobRecord.definition_version))
                ).scalar_one() == 1
            source, git = FreshSource(task, engine), GitSource(engine)
            with pytest.raises(JobSubmissionConflict):
                await submit(
                    submitter(engine, repository, source, git), task, repository
                )
            assert source.reads == git.reads == 0
            async with engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: migrate_to(
                        sync, "0002_job_submissions", downgrade=True
                    )
                )
                await connection.run_sync(lambda sync: migrate_to(sync, "head"))
            new = await record(
                engine, prepared(await save_task(engine, external="10006"), repository)
            )
            assert new.created
            unchanged = await job_submission_reader(engine).find_by_key(
                legacy.submission_key
            )
            assert unchanged == receipt
            associations = await job_branch_reader(engine).find_by_ids(
                repository_id=repository.repository_id,
                job_ids=(values["id"], new.stored.job.id),
            )
            assert {row.job_id: row.provider_repository_id for row in associations} == {
                values["id"]: None,
                new.stored.job.id: 123,
            }

    run(scenario())


def test_downgrade_cannot_discard_accepted_provider_bindings(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            intent = prepared(await save_task(engine), policy())
            saved = await record(engine, intent)

            with pytest.raises(IntegrityError):
                async with engine.begin() as connection:
                    await connection.run_sync(
                        lambda sync: migrate_to(
                            sync, "0002_job_submissions", downgrade=True
                        )
                    )

            assert (
                await job_submission_reader(engine).find_by_key(intent.submission_key)
                == saved.stored
            )
            async with engine.connect() as connection:
                assert (
                    await connection.execute(
                        text("SELECT version_num FROM alembic_version")
                    )
                ).scalar_one() == "0009_coding_results"

    run(scenario())


def test_submit_job_replay_skips_provider_reads_after_readiness_removed(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            stored, repository = await save_task(engine), policy()
            source, git = FreshSource(stored, engine), GitSource(engine)
            service = submitter(engine, repository, source, git)
            first = await submit(service, stored, repository)
            source.ready = False
            second = await submit(service, stored, repository)
            assert first.created and not second.created
            assert second.stored == first.stored
            assert (source.reads, git.reads) == (1, 1)
            assert await counts(engine) == (1, 1, 1)

    run(scenario())


def test_four_concurrent_submissions_record_one_job(database: DatabaseFactory) -> None:
    async def scenario() -> None:
        async with database() as engine:
            stored, repository = await save_task(engine), policy(3)
            source, git = (
                FreshSource(stored, engine, gate=asyncio.Barrier(4)),
                GitSource(engine, concurrent=True),
            )
            service = submitter(engine, repository, source, git)
            async with asyncio.timeout(15):
                results = await asyncio.gather(
                    *(submit(service, stored, repository) for _ in range(4))
                )
            assert sum(result.created for result in results) == 1
            assert len({result.stored.job.id for result in results}) == 1
            assert len({result.stored.job.authorized_branch for result in results}) == 1
            assert await counts(engine) == (1, 1, 3)

    run(scenario())


def test_unresolved_guard_is_scoped_to_task_and_repository(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            task, first_policy = await save_task(engine), policy()
            second_policy = replace(
                first_policy,
                repository_id=uuid4(),
                slug="example/another",
                provider_repository_id=456,
            )
            first = await record(engine, prepared(task, first_policy))
            second = await record(engine, prepared(task, second_policy))
            assert first.created and second.created
            assert first.stored.job.id != second.stored.job.id
            assert await counts(engine) == (2, 2, 2)

    run(scenario())


@pytest.mark.parametrize("conflict", ["requirements", "policy", "provider_id"])
def test_competing_intents_cannot_overwrite_or_create_two_deliveries(
    database: DatabaseFactory, conflict: str
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            first_task, repository = await save_task(engine), policy()
            second_task = (
                await save_task(engine, title="Changed requirements")
                if conflict == "requirements"
                else first_task
            )
            second_policy = (
                replace(repository, version="v2")
                if conflict == "policy"
                else replace(repository, provider_repository_id=456)
                if conflict == "provider_id"
                else repository
            )
            intents = (
                prepared(first_task, repository),
                prepared(second_task, second_policy),
            )
            async with asyncio.timeout(15):
                results = await asyncio.gather(
                    *(record(engine, intent) for intent in intents),
                    return_exceptions=True,
                )
            assert (
                sum(isinstance(result, SavedJobSubmission) for result in results) == 1
            )
            assert (
                sum(isinstance(result, JobSubmissionConflict) for result in results)
                == 1
            )
            assert await counts(engine) == (1, 1, 1)

    run(scenario())


def test_no_commit_rolls_back_repository_job_and_checks(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            intent = prepared(await save_task(engine), policy())
            candidate = intent.build_candidate(
                job_id=uuid4(),
                head=RepositoryHead(
                    provider_repository_id=123,
                    repository_id=intent.policy.repository_id,
                    target_branch="main",
                    revision="a" * 40,
                ),
                now=datetime.now(UTC),
            )
            async with job_submission_unit_of_work_factory(engine)() as unit:
                assert (await unit.jobs.admit(intent, candidate)).created
            assert await counts(engine) == (0, 0, 0)
            async with engine.connect() as connection:
                assert (
                    await connection.execute(select(RepositoryBindingRecord.__table__))
                ).all() == []

    run(scenario())


@pytest.mark.parametrize(
    "table", ["job_checks", "job_workflows", "job_workflow_events"]
)
def test_failed_submission_write_rolls_back_without_sql_or_exception_leak(
    database: DatabaseFactory,
    table: str,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            intent = prepared(await save_task(engine), policy())

            def fail(
                connection: Any,
                cursor: Any,
                statement: str,
                parameters: Any,
                context: Any,
                executemany: bool,
            ) -> None:
                if statement.startswith("INSERT INTO " + table + " "):
                    raise SQLAlchemyError("secret-connection-and-sql-details")

            event.listen(engine.sync_engine, "before_cursor_execute", fail)
            try:
                with pytest.raises(JobPersistenceError) as error:
                    await record(engine, intent)
                assert "secret-connection" not in str(error.value)
            finally:
                event.remove(engine.sync_engine, "before_cursor_execute", fail)
            assert await counts(engine) == (0, 0, 0)
            async with engine.connect() as connection:
                assert (
                    await connection.execute(text("SELECT count(*) FROM job_workflows"))
                ).scalar_one() == 0
                assert (
                    await connection.execute(
                        text("SELECT count(*) FROM job_workflow_events")
                    )
                ).scalar_one() == 0

    run(scenario())


@pytest.mark.parametrize("rebind", ["id", "slug"])
def test_repository_identity_cannot_be_rebound_to_bypass_task_guard(
    database: DatabaseFactory, rebind: str
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            repository = policy()
            await record(engine, prepared(await save_task(engine), repository))
            changed = (
                replace(repository, slug="example/other")
                if rebind == "slug"
                else replace(repository, repository_id=uuid4())
            )
            with pytest.raises(JobSubmissionConflict):
                await record(
                    engine, prepared(await save_task(engine, external="10006"), changed)
                )
            assert await counts(engine) == (1, 1, 1)

    run(scenario())


async def header(engine: AsyncEngine) -> dict[str, Any]:
    # SQLAlchemy row-mapping values span database types; keep Any localized here.
    async with engine.connect() as connection:
        return dict(
            (await connection.execute(select(JobRecord.__table__))).mappings().one()
        )


@pytest.mark.parametrize(
    "violation",
    [
        "source_fk",
        "unresolved",
        "incomplete",
        "count",
        "position",
        "argv",
        "legacy",
        "unbound",
        "provider_fk",
        "negative_provider",
    ],
)
def test_database_rejects_inconsistent_submission_definitions(
    database: DatabaseFactory, violation: str
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            repository = policy()
            first_task = await save_task(engine)
            await record(engine, prepared(first_task, repository))
            values = await header(engine)
            task = (
                await save_task(engine, title="Changed requirements")
                if violation == "unresolved"
                else await save_task(engine, external="10006")
            )
            values.update(
                id=uuid4(),
                source_task_id=task.source_task_id,
                snapshot_id=task.snapshot.id,
                submission_key="b" * 64,
                submission_fingerprint="c" * 64,
            )
            if violation == "source_fk":
                third = await save_task(engine, external="10008")
                values["source_task_id"] = third.source_task_id
            elif violation == "legacy":
                values.update(definition_version=1, provider_repository_id=None)
            elif violation == "unbound":
                values["provider_repository_id"] = None
            elif violation == "provider_fk":
                values["provider_repository_id"] = 456
            elif violation == "negative_provider":
                values["provider_repository_id"] = -1
            with pytest.raises(IntegrityError):
                async with engine.begin() as connection:
                    await connection.execute(insert(JobRecord).values(**values))
                    if violation not in {"source_fk", "unresolved", "incomplete"}:
                        await connection.execute(
                            insert(JobCheckRecord).values(
                                job_id=values["id"],
                                check_id="check_0",
                                check_count=2 if violation == "count" else 1,
                                position=1 if violation == "position" else 0,
                                argv=[]
                                if violation == "argv"
                                else ["uv", "run", "pytest"],
                                working_directory=".",
                                timeout_seconds=300,
                            )
                        )
            assert await counts(engine) == (1, 1, 1)

    run(scenario())


@pytest.mark.parametrize(
    "table", ["jobs", "job_checks", "repositories", "repository_bindings"]
)
def test_definition_updates_and_deletes_are_rejected(
    database: DatabaseFactory, table: str
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            await record(engine, prepared(await save_task(engine), policy()))
            model = {
                "jobs": JobRecord,
                "job_checks": JobCheckRecord,
                "repositories": RepositoryRecord,
                "repository_bindings": RepositoryBindingRecord,
            }[table]
            field = {
                "jobs": "actor_id",
                "job_checks": "working_directory",
                "repositories": "slug",
                "repository_bindings": "provider_repository_id",
            }[table]
            with pytest.raises(IntegrityError):
                async with engine.begin() as connection:
                    await connection.execute(
                        update(model).values(
                            **{
                                field: 456
                                if table == "repository_bindings"
                                else "changed"
                            }
                        )
                    )
            with pytest.raises(IntegrityError):
                async with engine.begin() as connection:
                    await connection.execute(delete(model))

    run(scenario())


def test_second_migration_preserves_preexisting_task_content(
    database: DatabaseFactory,
) -> None:
    def migrate(connection: Connection, downgrade: bool) -> None:
        config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
        config.attributes["connection"] = connection
        command.downgrade(
            config, "0001_task_snapshots"
        ) if downgrade else command.upgrade(config, "head")

    async def scenario() -> None:
        async with database() as engine:
            task = await save_task(engine)
            async with engine.begin() as connection:
                await connection.run_sync(lambda sync: migrate(sync, True))
            assert (
                await task_snapshot_reader(engine).get_by_id(task.snapshot.id) == task
            )
            async with engine.begin() as connection:
                await connection.run_sync(lambda sync: migrate(sync, False))
            assert (await record(engine, prepared(task, policy()))).created

    run(scenario())


@pytest.mark.parametrize("forgery", ["content", "binding", "identity"])
def test_repository_rechecks_canonical_snapshot_before_writing(
    database: DatabaseFactory,
    forgery: str,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            task = await save_task(engine)
            if forgery == "content":
                task = replace(
                    task, snapshot=replace(task.snapshot, title="Forged requirements")
                )
            elif forgery == "binding":
                other = await save_task(engine, external="10006")
                task = replace(task, source_task_id=other.source_task_id)
            else:
                task = replace(
                    task,
                    snapshot=replace(
                        task.snapshot,
                        identity=replace(
                            task.snapshot.identity,
                            external_id="10006",
                        ),
                    ),
                )
            with pytest.raises(JobSubmissionConflict):
                await record(engine, prepared(task, policy()))
            assert await counts(engine) == (0, 0, 0)

    run(scenario())


def test_database_connections_use_required_read_committed_isolation(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            async with engine.connect() as connection:
                assert (
                    await connection.execute(text("SHOW transaction_isolation"))
                ).scalar_one() == "read committed"

    run(scenario())


@pytest.mark.parametrize("corruption", ["intent", "snapshot"])
def test_reader_rejects_corrupt_fingerprints_and_releases_connection(
    database: DatabaseFactory,
    corruption: str,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            repository = policy()
            await record(engine, prepared(await save_task(engine), repository))
            values = await header(engine)
            task = await save_task(engine, external="10006")
            intent = prepared(task, repository)
            values.update(
                id=uuid4(),
                source_task_id=task.source_task_id,
                snapshot_id=task.snapshot.id,
                submission_key=intent.submission_key,
                submission_fingerprint=intent.submission_fingerprint,
            )
            async with engine.begin() as connection:
                if corruption == "intent":
                    values["submission_fingerprint"] = "f" * 64
                else:
                    corrupt_id = uuid4()
                    await connection.execute(
                        insert(TaskSnapshotRecord).values(
                            id=corrupt_id,
                            source_task_id=task.source_task_id,
                            content_fingerprint="f" * 64,
                            fingerprint_version=1,
                            title="Corrupt content",
                            description="Requirements",
                            acceptance_criteria=["Check."],
                            display_key="DRY-5",
                            captured_at=datetime.now(UTC),
                        )
                    )
                    values["snapshot_id"] = corrupt_id
                await connection.execute(insert(JobRecord).values(**values))
                await connection.execute(
                    insert(JobCheckRecord).values(
                        job_id=values["id"],
                        check_id="check_0",
                        check_count=1,
                        position=0,
                        argv=list(repository.checks[0].argv),
                        working_directory=".",
                        timeout_seconds=300,
                    )
                )
                await seed_workflows(connection, [values])
            with pytest.raises(JobPersistenceError):
                await job_submission_reader(engine).find_by_key(intent.submission_key)
            assert isinstance(engine.sync_engine.pool, AsyncAdaptedQueuePool)
            assert engine.sync_engine.pool.checkedout() == 0

    run(scenario())


def test_submission_lookup_plans_on_500_jobs(database: DatabaseFactory) -> None:
    async def scenario() -> None:
        async with database() as engine:
            repository = policy()
            await record(engine, prepared(await save_task(engine), repository))
            template = await header(engine)
            sources, snapshots, jobs, checks = [], [], [], []
            now = datetime.now(UTC)
            for index in range(500):
                source_id, snapshot_id, job_id = uuid4(), uuid4(), uuid4()
                sources.append(
                    {
                        "id": source_id,
                        "source_provider": "jira",
                        "source_site_id": "test-site",
                        "external_task_id": str(index),
                        "display_key": f"DRY-{index}",
                        "last_seen_at": now,
                    }
                )
                snapshots.append(
                    {
                        "id": snapshot_id,
                        "source_task_id": source_id,
                        "content_fingerprint": "a" * 64,
                        "fingerprint_version": 1,
                        "title": "Fixture",
                        "description": "Requirements",
                        "acceptance_criteria": ["Check."],
                        "display_key": f"DRY-{index}",
                        "captured_at": now,
                    }
                )
                jobs.append(
                    {
                        **template,
                        "id": job_id,
                        "snapshot_id": snapshot_id,
                        "source_task_id": source_id,
                        "submission_key": f"{index:064x}",
                        "authorized_branch": f"devrelay/jobs/{job_id.hex}",
                    }
                )
                checks.append(
                    {
                        "job_id": job_id,
                        "check_id": "check_0",
                        "check_count": 1,
                        "position": 0,
                        "argv": ["uv", "run", "pytest"],
                        "working_directory": ".",
                        "timeout_seconds": 300,
                    }
                )
            async with engine.begin() as connection:
                await connection.execute(insert(SourceTaskRecord), sources)
                await connection.execute(insert(TaskSnapshotRecord), snapshots)
                await connection.execute(insert(JobRecord), jobs)
                await connection.execute(insert(JobCheckRecord), checks)
                await seed_workflows(connection, jobs)
                await connection.execute(text("ANALYZE jobs"))
                await connection.execute(text("ANALYZE job_checks"))
                await connection.execute(text("ANALYZE job_workflows"))
                queries = (
                    (
                        "SELECT j.id, b.provider_repository_id FROM jobs j LEFT JOIN repository_bindings b ON j.repository_id=b.repository_id WHERE j.submission_key=:key",
                        {"key": jobs[250]["submission_key"]},
                    ),
                    (
                        "SELECT job_id FROM job_workflows WHERE source_task_id=:source AND repository_id=:repo AND blocks_new_submission IS TRUE LIMIT 1",
                        {
                            "source": sources[250]["id"],
                            "repo": repository.repository_id,
                        },
                    ),
                    (
                        "SELECT * FROM job_checks WHERE job_id=:job ORDER BY position LIMIT 33",
                        {"job": jobs[250]["id"]},
                    ),
                    (
                        "SELECT id, repository_id, source_task_id, authorized_branch, coding_branch_prefix FROM jobs WHERE repository_id=:repo AND id=ANY(:ids) ORDER BY id LIMIT 100",
                        {"repo": repository.repository_id, "ids": [jobs[250]["id"]]},
                    ),
                )
                for query, parameters in queries:
                    plan = "\n".join(
                        (
                            await connection.execute(
                                text("EXPLAIN " + query), parameters
                            )
                        ).scalars()
                    )
                    print(plan)
                    assert "Index Scan" in plan or "Bitmap" in plan

                candidate_sql = str(
                    queued_candidate_query(
                        frozenset({repository.repository_id})
                    ).compile(
                        dialect=connection.dialect,
                        compile_kwargs={"literal_binds": True},
                    )
                )
                await connection.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
                await connection.execute(
                    text("DROP INDEX IF EXISTS ix_job_workflows_queued_candidate")
                )
                before = "\n".join(
                    (
                        await connection.execute(text("EXPLAIN " + candidate_sql))
                    ).scalars()
                )
                await connection.execute(
                    text(
                        "CREATE INDEX ix_job_workflows_queued_candidate ON job_workflows (repository_id, created_at, job_id) WHERE status='queued' AND workflow_version=0 AND blocks_new_submission IS TRUE"
                    )
                )
                after = "\n".join(
                    (
                        await connection.execute(text("EXPLAIN " + candidate_sql))
                    ).scalars()
                )
                print("Queue before index:\n" + before)
                print("Queue after index:\n" + after)
                assert "ix_job_workflows_queued_candidate" in after

            statements: list[str] = []

            def count_query(
                connection: Any,
                cursor: Any,
                statement: str,
                parameters: Any,
                context: Any,
                executemany: bool,
            ) -> None:
                statements.append(statement)

            event.listen(engine.sync_engine, "before_cursor_execute", count_query)
            try:
                reader = job_branch_reader(engine)
                assert (
                    await reader.find_by_ids(
                        repository_id=repository.repository_id, job_ids=()
                    )
                    == ()
                )
                assert statements == []
                for size in (1, 100):
                    statements.clear()
                    rows = await reader.find_by_ids(
                        repository_id=repository.repository_id,
                        job_ids=tuple(item["id"] for item in jobs[:size]),
                    )
                    assert len(rows) == size
                    assert len(statements) == 1
                    assert all(
                        row.authorized_branch == f"devrelay/jobs/{row.job_id.hex}"
                        for row in rows
                    )
                    assert isinstance(engine.sync_engine.pool, AsyncAdaptedQueuePool)
                    assert engine.sync_engine.pool.checkedout() == 0
                statements.clear()
                assert (
                    await reader.find_by_ids(
                        repository_id=uuid4(), job_ids=(jobs[0]["id"],)
                    )
                    == ()
                )
                assert len(statements) == 1
                for ids in ((jobs[0]["id"],) * 2, tuple(uuid4() for _ in range(101))):
                    with pytest.raises(JobPersistenceError):
                        await reader.find_by_ids(
                            repository_id=repository.repository_id, job_ids=ids
                        )
            finally:
                event.remove(engine.sync_engine, "before_cursor_execute", count_query)

    run(scenario())


def test_composed_github_submission_preserves_exact_replay_and_reconciles_stored_pr(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            cloud_id = uuid4()
            task = await save_task(engine, site_id=str(cloud_id))
            repository_id = uuid4()
            jira = JiraSettings(
                site_url="https://example.atlassian.net",
                cloud_id=cloud_id,
                email="reader@example.test",
                api_token=SecretStr("test-only-jira-token"),
                allowed_project_keys=frozenset({"DRY"}),
                acceptance_criteria_field="customfield_12345",
            )
            github = GitHubSettings(
                api_token=SecretStr("test-only-github-token"),
                repository_ids=(
                    GitHubRepositoryId(repository_id=repository_id, github_id=123),
                ),
            )
            repo = RepositorySettings(
                repository_id=repository_id,
                slug="example/devrelay",
                enabled=True,
                target_branch="main",
                coding_branch_prefix="devrelay/jobs",
                policy_version="v1",
                allowed_project_keys=frozenset({"DRY"}),
                checks=(
                    CheckSettings(
                        id="pytest", argv=("uv", "run", "pytest"), timeout_seconds=300
                    ),
                ),
                max_repair_cycles=2,
            )
            calls: list[str] = []
            current_title = "Import a task"

            def no_open_transaction() -> None:
                assert isinstance(engine.sync_engine.pool, AsyncAdaptedQueuePool)
                assert engine.sync_engine.pool.checkedout() == 0

            def jira_response(request: httpx.Request) -> httpx.Response:
                no_open_transaction()
                calls.append("jira")
                return httpx.Response(
                    200,
                    json={
                        "id": "10005",
                        "key": "DRY-5",
                        "fields": {
                            "summary": current_title,
                            "description": {
                                "type": "doc",
                                "version": 1,
                                "content": [
                                    {
                                        "type": "paragraph",
                                        "content": [
                                            {
                                                "type": "text",
                                                "text": "Preserve requirements.",
                                            }
                                        ],
                                    }
                                ],
                            },
                            "updated": None,
                            "project": {"key": "DRY"},
                            "labels": ["agent-ready"],
                            "customfield_12345": "Save once.",
                        },
                    },
                )

            def github_response(request: httpx.Request) -> httpx.Response:
                no_open_transaction()
                calls.append("github")
                if request.url.path.endswith("/pulls"):
                    return httpx.Response(200, json=[])
                if "/git/ref/" in request.url.path:
                    return httpx.Response(
                        200,
                        json={
                            "ref": "refs/heads/main",
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

            authorization = SubmissionAuthorization(
                actor_id="test-operator",
                allowed_repository_ids=frozenset({repository_id}),
            )
            async with open_github_job_submitter(
                jira,
                github,
                (repo,),
                engine,
                jira_transport=httpx.MockTransport(jira_response),
                github_transport=httpx.MockTransport(github_response),
            ) as submitter:
                saved = await submitter.execute(
                    snapshot_id=task.snapshot.id,
                    repository_id=repository_id,
                    authorization=authorization,
                )
                assert saved.created
                assert calls == ["jira", "github", "github", "github", "github"]
                calls.clear()
                replay = await submitter.execute(
                    snapshot_id=task.snapshot.id,
                    repository_id=repository_id,
                    authorization=authorization,
                )
                assert not replay.created and replay.stored == saved.stored
                assert calls == []

            # The real association reader must identify this task from its stored
            # branch, independent of remote PR title/body or current Job status.
            def open_pr(request: httpx.Request) -> httpx.Response:
                if request.url.path.endswith("/pulls"):
                    no_open_transaction()
                    return httpx.Response(
                        200,
                        json=[
                            {
                                "number": 7,
                                "state": "open",
                                "head": {
                                    "ref": saved.stored.job.authorized_branch,
                                    "sha": "b" * 40,
                                    "repo": {
                                        "id": 123,
                                        "full_name": "example/devrelay",
                                    },
                                },
                                "base": {
                                    "ref": "main",
                                    "sha": "a" * 40,
                                    "repo": {
                                        "id": 123,
                                        "full_name": "example/devrelay",
                                    },
                                },
                            }
                        ],
                    )
                return github_response(request)

            async with open_github_repository_source(
                github,
                (repo,),
                job_branch_reader(engine),
                transport=httpx.MockTransport(open_pr),
            ) as observer:
                observed = await observer.observe(
                    repository_id=repository_id,
                    target_branch="main",
                    source_task_id=task.source_task_id,
                )
                assert observed.unresolved_pull_request is not None
                assert observed.unresolved_pull_request.number == 7
                assert (
                    await observer.observe(
                        repository_id=repository_id,
                        target_branch="main",
                        source_task_id=uuid4(),
                    )
                ).unresolved_pull_request is None

            current_title = "Changed task"
            changed = await save_task(
                engine, title=current_title, site_id=str(cloud_id)
            )
            async with open_github_job_submitter(
                jira,
                github,
                (repo,),
                engine,
                jira_transport=httpx.MockTransport(jira_response),
                github_transport=httpx.MockTransport(open_pr),
            ) as submitter:
                with pytest.raises(
                    JobSubmissionConflict, match="unresolved pull request"
                ):
                    await submitter.execute(
                        snapshot_id=changed.snapshot.id,
                        repository_id=repository_id,
                        authorization=authorization,
                    )
            assert (await counts(engine))[1] == 1

    run(scenario())
