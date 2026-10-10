import asyncio
import os
import subprocess
import sys
from collections.abc import AsyncIterator, Callable, Coroutine
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from alembic import command
from alembic.config import Config
from pydantic import SecretStr
from sqlalchemy import delete, event, func, insert, select, text, update
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.pool import AsyncAdaptedQueuePool

from devrelay.api.dependencies import ImportOperation
from devrelay.application.contracts.submissions import SubmissionAuthorization
from devrelay.application.contracts.tasks import SavedTaskSnapshot
from devrelay.application.jobs import TaskNotReady
from devrelay.application.ports.task_persistence import (
    TaskPersistenceError,
    TaskSnapshotNotFound,
)
from devrelay.application.tasks import RequirementsStatus
from devrelay.bootstrap.control import create_app
from devrelay.bootstrap.jobs import open_job_preparer
from devrelay.bootstrap.persistence import (
    task_snapshot_reader,
    task_unit_of_work_factory,
)
from devrelay.bootstrap.tasks import open_task_importer
from devrelay.config.control import ControlSettings
from devrelay.config.database import DatabaseSettings
from devrelay.config.jira import JiraSettings
from devrelay.config.repositories import CheckSettings, RepositorySettings
from devrelay.config.task_import import TaskImportSettings
from devrelay.domain.tasks import TaskIdentity, TaskSnapshot
from devrelay.infrastructure.persistence.models import (
    SourceTaskRecord,
    TaskSnapshotRecord,
)

pytestmark = pytest.mark.postgres
DatabaseFactory = Callable[[], AbstractAsyncContextManager[AsyncEngine]]


def run(scenario: Coroutine[object, object, None]) -> None:
    asyncio.run(
        scenario,
        loop_factory=asyncio.SelectorEventLoop if sys.platform == "win32" else None,
    )


def migration(connection: Connection, action: str = "upgrade") -> None:
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    config.attributes["connection"] = connection
    if action == "downgrade":
        command.downgrade(config, "base")
    elif action == "check":
        command.check(config)
    else:
        command.upgrade(config, "head")


def snapshot(external_id: str = "10005") -> TaskSnapshot:
    return TaskSnapshot(
        id=uuid4(),
        identity=TaskIdentity(
            provider="jira", site_id="test-site", external_id=external_id
        ),
        display_key="DRY-5",
        title="Import a task",
        description="Preserve requirements.",
        acceptance_criteria=("Save once.",),
        captured_at=datetime.now(UTC),
    )


async def save(engine: AsyncEngine, value: TaskSnapshot) -> SavedTaskSnapshot:
    async with task_unit_of_work_factory(engine)() as unit:
        saved = await unit.tasks.save_snapshot(value)
        await unit.commit()
    return saved


async def counts(engine: AsyncEngine) -> tuple[int, int]:
    async with engine.connect() as connection:
        return (
            (
                await connection.execute(
                    select(func.count()).select_from(SourceTaskRecord)
                )
            ).scalar_one(),
            (
                await connection.execute(
                    select(func.count()).select_from(TaskSnapshotRecord)
                )
            ).scalar_one(),
        )


def test_migrations_round_trip_and_match_metadata(database: DatabaseFactory) -> None:
    async def scenario() -> None:
        async with database() as engine:
            async with engine.begin() as connection:
                await connection.run_sync(lambda sync: migration(sync, "check"))
                await connection.run_sync(lambda sync: migration(sync, "downgrade"))
                assert (
                    await connection.execute(
                        text("SELECT to_regclass('task_snapshots')")
                    )
                ).scalar_one() is None
                await connection.run_sync(migration)
            assert (await save(engine, snapshot())).created

    run(scenario())


def test_jira_read_import_and_postgresql_storage(database: DatabaseFactory) -> None:
    async def scenario() -> None:
        async with database() as engine:
            payload = (
                Path(__file__).resolve().parents[1]
                / "contract/fixtures/jira/issue.json"
            ).read_bytes()
            requests: list[httpx.Request] = []

            def handle(request: httpx.Request) -> httpx.Response:
                # No DB connection may be retained while awaiting Jira.
                pool = engine.sync_engine.pool
                assert isinstance(pool, AsyncAdaptedQueuePool)
                assert pool.checkedout() == 0
                requests.append(request)
                return httpx.Response(
                    200, content=payload, headers={"content-type": "application/json"}
                )

            settings = JiraSettings(
                site_url="https://example.atlassian.net",
                cloud_id=UUID("11111111-2222-3333-4444-555555555555"),
                email="reader@example.test",
                api_token=SecretStr("test-only-jira-token"),
                allowed_project_keys=frozenset({"DRY"}),
            )
            async with open_task_importer(
                settings, engine, transport=httpx.MockTransport(handle)
            ) as importer:
                first = await importer.execute("DRY-7")
                second = await importer.execute("DRY-7")
            assert first.saved.created and not second.saved.created
            assert first.requirements_status is RequirementsStatus.COMPLETE
            assert second.saved.snapshot == first.saved.snapshot
            assert len(requests) == 2
            assert await counts(engine) == (1, 1)

    run(scenario())


def test_explicit_migration_command_on_windows_or_linux(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            async with engine.begin() as connection:
                schema = (
                    await connection.execute(text("SELECT current_schema()"))
                ).scalar_one()
                await connection.run_sync(lambda sync: migration(sync, "downgrade"))
            environment = dict(os.environ)
            environment.update(
                {
                    "DEVRELAY_CONTROL_DATABASE__HOST": "127.0.0.1",
                    "DEVRELAY_CONTROL_DATABASE__PORT": os.environ[
                        "DEVRELAY_TEST_POSTGRES_PORT"
                    ],
                    "DEVRELAY_CONTROL_DATABASE__DATABASE": "devrelay_test",
                    "DEVRELAY_CONTROL_DATABASE__USERNAME": "devrelay_test",
                    "DEVRELAY_CONTROL_DATABASE__PASSWORD": "devrelay-test-only-password",
                    "DEVRELAY_CONTROL_DATABASE__SCHEMA_NAME": schema,
                    "DEVRELAY_CONTROL_DATABASE__SSLMODE": "disable",
                }
            )
            process = await asyncio.to_thread(
                subprocess.run,
                [sys.executable, "-m", "alembic", "upgrade", "head"],
                cwd=Path(__file__).resolve().parents[2],
                env=environment,
                capture_output=True,
                timeout=30,
                check=False,
            )
            assert process.returncode == 0, process.stderr.decode()
            assert (await save(engine, snapshot())).created

    run(scenario())


def test_authenticated_http_import_through_real_storage(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            payload = (
                Path(__file__).resolve().parents[1]
                / "contract/fixtures/jira/issue.json"
            ).read_bytes()
            requests: list[httpx.Request] = []

            def handle(request: httpx.Request) -> httpx.Response:
                requests.append(request)
                return httpx.Response(
                    200, content=payload, headers={"content-type": "application/json"}
                )

            jira = JiraSettings(
                site_url="https://example.atlassian.net",
                cloud_id=UUID("11111111-2222-3333-4444-555555555555"),
                email="reader@example.test",
                api_token=SecretStr("test-only-jira-token"),
                allowed_project_keys=frozenset({"DRY"}),
            )
            token = "test_only_import_token_0123456789abcdefghijklmnop"

            @asynccontextmanager
            async def resource() -> AsyncIterator[ImportOperation]:
                async with open_task_importer(
                    jira, engine, transport=httpx.MockTransport(handle)
                ) as importer:
                    yield importer.execute

            application = create_app(
                ControlSettings(
                    jira=jira,
                    database=DatabaseSettings(
                        host="127.0.0.1",
                        database="devrelay_test",
                        username="devrelay_test",
                        password=SecretStr("test-only-database-password"),
                    ),
                    task_import=TaskImportSettings(auth_token=SecretStr(token)),
                ),
                import_resources=resource,
            )
            async with application.router.lifespan_context(application):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=application),
                    base_url="http://testserver",
                ) as client:
                    path = "/api/v1/tasks/import"
                    unauthorized = await client.post(path, json={"task_key": "DRY-7"})
                    assert unauthorized.status_code == 401
                    assert requests == []
                    assert await counts(engine) == (0, 0)
                    headers = {"Authorization": f"Bearer {token}"}
                    first = await client.post(
                        path, headers=headers, json={"task_key": "DRY-7"}
                    )
                    second = await client.post(
                        path, headers=headers, json={"task_key": "DRY-7"}
                    )
                    rejected = await client.post(
                        path, headers=headers, json={"task_key": "OTHER-7"}
                    )
                    assert first.status_code == 201
                    assert second.status_code == 200
                    assert first.json()["snapshot_id"] == second.json()["snapshot_id"]
                    assert first.json()["requirements_status"] == "complete"
                    assert rejected.status_code == 403
                    assert len(requests) == 2
            assert await counts(engine) == (1, 1)
            assert application.state.task_import_services is None

    run(scenario())


def test_concurrent_changed_requirements_preserve_both_versions(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            first = snapshot()
            changed = replace(first, id=uuid4(), title="Changed requirements")
            results = await asyncio.gather(save(engine, first), save(engine, changed))
            assert all(result.created for result in results)
            assert results[0].source_task_id == results[1].source_task_id
            assert await counts(engine) == (1, 2)

    run(scenario())


def test_duplicate_returns_original_immutable_snapshot_and_tracks_latest_key(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            original = snapshot()
            first = await save(engine, original)
            latest = replace(
                original,
                id=uuid4(),
                display_key="NEW-5",
                captured_at=original.captured_at + timedelta(days=1),
            )
            duplicate = await save(engine, latest)
            await save(
                engine,
                replace(
                    original,
                    id=uuid4(),
                    captured_at=original.captured_at - timedelta(days=1),
                ),
            )
            assert first.created and not duplicate.created
            assert duplicate.snapshot == original
            assert duplicate.source_task_id == first.source_task_id
            assert await counts(engine) == (1, 1)
            async with engine.connect() as connection:
                assert (
                    await connection.execute(select(SourceTaskRecord.display_key))
                ).scalar_one() == "NEW-5"
            changed = await save(
                engine, replace(original, id=uuid4(), title="Changed requirements")
            )
            other = await save(
                engine,
                replace(
                    original,
                    id=uuid4(),
                    identity=TaskIdentity(
                        provider="jira", site_id="test-site", external_id="10006"
                    ),
                ),
            )
            assert changed.source_task_id == first.source_task_id
            assert other.source_task_id != first.source_task_id
            assert await counts(engine) == (2, 3)

    run(scenario())


def test_concurrent_imports_create_one_snapshot(database: DatabaseFactory) -> None:
    async def scenario() -> None:
        async with database() as engine:
            value = snapshot()
            saved = await asyncio.gather(
                *(save(engine, replace(value, id=uuid4())) for _ in range(8))
            )
            assert sum(item.created for item in saved) == 1
            assert len({item.source_task_id for item in saved}) == 1
            assert len({item.snapshot.id for item in saved}) == 1
            assert await counts(engine) == (1, 1)

    run(scenario())


@pytest.mark.parametrize("fail", [False, True])
def test_no_commit_or_exception_rolls_back_both_records(
    database: DatabaseFactory, fail: bool
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            try:
                async with task_unit_of_work_factory(engine)() as unit:
                    await unit.tasks.save_snapshot(snapshot())
                    if fail:
                        raise RuntimeError("Simulated application failure.")
            except RuntimeError:
                assert fail
            assert await counts(engine) == (0, 0)

    run(scenario())


def test_database_rejects_snapshot_updates_and_source_deletion(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            await save(engine, snapshot())
            for statement in (
                update(TaskSnapshotRecord).values(title="Changed"),
                delete(SourceTaskRecord),
            ):
                with pytest.raises(IntegrityError):
                    async with engine.begin() as connection:
                        await connection.execute(statement)
            assert await counts(engine) == (1, 1)

    run(scenario())


@pytest.mark.parametrize(
    "criteria", [[None], [""], ["  "], ["x" * 4097], ["Valid"] * 33, [["Nested"]]]
)
def test_database_checks_criteria_independently_of_domain(
    database: DatabaseFactory, criteria: list[Any]
) -> None:
    # Any is restricted to intentionally malformed external test data.
    async def scenario() -> None:
        async with database() as engine:
            value = snapshot()
            saved = await save(engine, value)
            with pytest.raises(IntegrityError):
                async with engine.begin() as connection:
                    await connection.execute(
                        text("""
                        INSERT INTO task_snapshots
                          (id, source_task_id, content_fingerprint, fingerprint_version, title,
                           description, acceptance_criteria, display_key, captured_at)
                        VALUES (:id, :source, :fingerprint, 1, 'Valid', '', CAST(:criteria AS text[]), 'DRY-5', now())
                    """),
                        {
                            "id": uuid4(),
                            "source": saved.source_task_id,
                            "fingerprint": "a" * 64,
                            "criteria": criteria,
                        },
                    )
            assert await counts(engine) == (1, 1)

    run(scenario())


@pytest.mark.parametrize("criteria_count", [1, 32])
def test_import_query_budget_and_materialized_result(
    database: DatabaseFactory, criteria_count: int
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            statements: list[str] = []

            # SQLAlchemy's DBAPI event parameters are driver-dependent external values.
            def observe(
                connection: Any,
                cursor: Any,
                statement: str,
                parameters: Any,
                context: Any,
                executemany: bool,
            ) -> None:
                if "source_tasks" in statement or "task_snapshots" in statement:
                    statements.append(statement)

            event.listen(engine.sync_engine, "before_cursor_execute", observe)
            value = replace(
                snapshot(),
                acceptance_criteria=tuple(
                    f"Criterion {number}" for number in range(criteria_count)
                ),
            )
            first = await save(engine, value)
            assert len(statements) == 2
            statements.clear()
            second = await save(engine, replace(value, id=uuid4()))
            assert len(statements) == 3
            assert second.snapshot == first.snapshot
            assert len(second.snapshot.acceptance_criteria) == criteria_count
            assert len(statements) == 3
            event.remove(engine.sync_engine, "before_cursor_execute", observe)

    run(scenario())


def test_point_lookup_plans_on_500_tasks(database: DatabaseFactory) -> None:
    async def scenario() -> None:
        async with database() as engine:
            identifiers = [uuid4() for _ in range(500)]
            snapshot_identifiers = [uuid4() for _ in range(500)]
            now = datetime.now(UTC)
            async with engine.begin() as connection:
                await connection.execute(
                    insert(SourceTaskRecord),
                    [
                        {
                            "id": identifier,
                            "source_provider": "jira",
                            "source_site_id": "test-site",
                            "external_task_id": str(index),
                            "display_key": f"DRY-{index}",
                            "last_seen_at": now,
                        }
                        for index, identifier in enumerate(identifiers)
                    ],
                )
                await connection.execute(
                    insert(TaskSnapshotRecord),
                    [
                        {
                            "id": snapshot_identifiers[index],
                            "source_task_id": identifier,
                            "content_fingerprint": "a" * 64,
                            "fingerprint_version": 1,
                            "title": "Valid",
                            "description": "x" * 1024,
                            "acceptance_criteria": ["Valid"],
                            "display_key": f"DRY-{index}",
                            "captured_at": now,
                        }
                        for index, identifier in enumerate(identifiers)
                    ],
                )
                await connection.execute(text("ANALYZE source_tasks"))
                await connection.execute(text("ANALYZE task_snapshots"))
                queries: list[tuple[str, dict[str, object]]] = [
                    (
                        "SELECT id FROM source_tasks WHERE source_provider='jira' AND source_site_id='test-site' AND external_task_id=:external",
                        {"external": "250"},
                    ),
                    (
                        "SELECT * FROM task_snapshots WHERE source_task_id=:source AND fingerprint_version=1 AND content_fingerprint=:fingerprint",
                        {"source": identifiers[250], "fingerprint": "a" * 64},
                    ),
                    (
                        "SELECT snapshots.*, sources.* FROM task_snapshots snapshots JOIN source_tasks sources ON sources.id=snapshots.source_task_id WHERE snapshots.id=:snapshot",
                        {"snapshot": snapshot_identifiers[250]},
                    ),
                ]
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

    run(scenario())


@pytest.mark.parametrize("criteria_count", [1, 32])
def test_snapshot_reader_materializes_one_join_and_releases_session(
    database: DatabaseFactory,
    criteria_count: int,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            saved = await save(
                engine,
                replace(
                    snapshot(),
                    acceptance_criteria=tuple(
                        f"Criterion {index}" for index in range(criteria_count)
                    ),
                ),
            )
            statements: list[str] = []

            # Driver event arguments are intentionally localized external values.
            def observe(
                connection: Any,
                cursor: Any,
                statement: str,
                parameters: Any,
                context: Any,
                executemany: bool,
            ) -> None:
                if "task_snapshots" in statement:
                    statements.append(statement)

            event.listen(engine.sync_engine, "before_cursor_execute", observe)
            try:
                stored = await task_snapshot_reader(engine).get_by_id(saved.snapshot.id)
                assert stored.snapshot == saved.snapshot
                assert stored.source_task_id == saved.source_task_id
                assert len(statements) == 1
                assert "JOIN source_tasks" in statements[0]
                assert isinstance(engine.sync_engine.pool, AsyncAdaptedQueuePool)
                assert engine.sync_engine.pool.checkedout() == 0
                assert len(stored.snapshot.acceptance_criteria) == criteria_count
                assert (
                    stored.snapshot.content_fingerprint
                    == saved.snapshot.content_fingerprint
                )
                assert len(statements) == 1
                with pytest.raises(TaskSnapshotNotFound):
                    await task_snapshot_reader(engine).get_by_id(uuid4())
                assert len(statements) == 2
                assert engine.sync_engine.pool.checkedout() == 0
            finally:
                event.remove(engine.sync_engine, "before_cursor_execute", observe)

    run(scenario())


def test_snapshot_reader_rejects_stored_content_fingerprint_mismatch(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            saved = await save(engine, snapshot())
            corrupt_id = uuid4()
            # Direct inserts emulate corrupt/imported storage; immutable rows are
            # never updated or constraints disabled by the application reader.
            async with engine.begin() as connection:
                await connection.execute(
                    insert(TaskSnapshotRecord).values(
                        id=corrupt_id,
                        source_task_id=saved.source_task_id,
                        content_fingerprint="a" * 64,
                        fingerprint_version=1,
                        title="Corrupt record",
                        description="Requirements",
                        acceptance_criteria=["Verify integrity."],
                        display_key="DRY-5",
                        captured_at=datetime.now(UTC),
                    )
                )
            with pytest.raises(TaskPersistenceError, match="integrity"):
                await task_snapshot_reader(engine).get_by_id(corrupt_id)
            assert isinstance(engine.sync_engine.pool, AsyncAdaptedQueuePool)
            assert engine.sync_engine.pool.checkedout() == 0

    run(scenario())


def test_real_storage_job_preparation_reads_fresh_identity_without_db_checkout(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            payload = (
                Path(__file__).resolve().parents[1]
                / "contract/fixtures/jira/issue.json"
            ).read_bytes()
            settings = JiraSettings(
                site_url="https://example.atlassian.net",
                cloud_id=UUID("11111111-2222-3333-4444-555555555555"),
                email="reader@example.test",
                api_token=SecretStr("test-only-jira-token"),
                allowed_project_keys=frozenset({"DRY"}),
            )
            requests: list[str] = []
            renamed = False
            ready = True

            def handle(request: httpx.Request) -> httpx.Response:
                pool = engine.sync_engine.pool
                assert isinstance(pool, AsyncAdaptedQueuePool)
                assert pool.checkedout() == 0
                requests.append(request.url.path)
                body = payload
                if renamed:
                    body = body.replace(b'"key": "DRY-7"', b'"key": "DRY-99"')
                if not ready:
                    body = body.replace(b'"agent-ready", "backend"', b'"backend"')
                return httpx.Response(
                    200, content=body, headers={"Content-Type": "application/json"}
                )

            transport = httpx.MockTransport(handle)
            async with open_task_importer(
                settings, engine, transport=transport
            ) as importer:
                imported = (await importer.execute("DRY-7")).saved
            repo = RepositorySettings(
                repository_id=uuid4(),
                slug="example/devrelay",
                enabled=True,
                target_branch="main",
                coding_branch_prefix="devrelay/jobs",
                policy_version="v1",
                allowed_project_keys=frozenset({"DRY"}),
                checks=(
                    CheckSettings(
                        id="pytest",
                        argv=("uv", "run", "--locked", "pytest"),
                        timeout_seconds=300,
                    ),
                ),
                max_repair_cycles=2,
            )
            authorization = SubmissionAuthorization(
                actor_id="test-operator",
                allowed_repository_ids=frozenset({repo.repository_id}),
            )
            renamed = True
            async with open_job_preparer(
                settings,
                (repo,),
                engine,
                repository_ids={repo.repository_id: 123},
                transport=transport,
            ) as preparer:
                prepared = await preparer.execute(
                    snapshot_id=imported.snapshot.id,
                    repository_id=repo.repository_id,
                    authorization=authorization,
                )
                assert prepared.stored.snapshot == imported.snapshot
                assert prepared.stored.source_task_id == imported.source_task_id
                assert requests[-1].endswith("/issue/10007")
                ready = False
                with pytest.raises(TaskNotReady):
                    await preparer.execute(
                        snapshot_id=imported.snapshot.id,
                        repository_id=repo.repository_id,
                        authorization=authorization,
                    )
            assert await counts(engine) == (1, 1)
            assert len(requests) == 3

    run(scenario())
