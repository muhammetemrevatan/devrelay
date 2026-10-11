import asyncio
import json
import sys
from collections.abc import Callable, Coroutine
from contextlib import AbstractAsyncContextManager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import httpx
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import event, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.pool import AsyncAdaptedQueuePool

from devrelay.application.contracts.assignments import (
    ClaimAdmissionRejected,
    ClaimLimits,
    ClaimPersistenceError,
    ClaimScopeRejected,
    NoWorkReason,
)
from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    ClaimRequestIdentity,
    LeaseProof,
    WorkerClaimRequest,
)
from devrelay.application.contracts.eligibility import (
    ClaimEligibilityRejected,
    PreparedJobClaim,
    WorkerAuthorityExpired,
)
from devrelay.application.contracts.submissions import (
    StoredJobSubmission,
    SubmissionAuthorization,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.bootstrap.claims import open_job_claimer
from devrelay.bootstrap.jobs import open_github_job_submitter
from devrelay.bootstrap.persistence import (
    claim_store,
    job_submission_reader,
    queued_job_reader,
)
from devrelay.bootstrap.security import worker_authenticator
from devrelay.bootstrap.tasks import open_task_importer
from devrelay.config.control import ControlSettings
from devrelay.domain.jobs.values import JobStatus
from devrelay.infrastructure.persistence.claim_models import (
    ClaimReceiptRecord,
    WorkerCoordinationRecord,
)
from tests.integration.job_fixtures import save_job

pytestmark = pytest.mark.postgres
DatabaseFactory = Callable[[], AbstractAsyncContextManager[AsyncEngine]]


def run(scenario: Coroutine[object, object, None]) -> None:
    asyncio.run(
        scenario,
        loop_factory=asyncio.SelectorEventLoop if sys.platform == "win32" else None,
    )


def authority(
    stored: StoredJobSubmission,
    *,
    worker_id: UUID | None = None,
    expiry: datetime | None = None,
) -> WorkerAuthorization:
    return WorkerAuthorization(
        worker_id=worker_id or uuid4(),
        allowed_repository_ids=frozenset({stored.job.repository_id}),
        credential_expires_at=expiry or datetime.now(UTC) + timedelta(hours=1),
    )


def request() -> WorkerClaimRequest:
    return WorkerClaimRequest(claim_request_id=uuid4(), proof=LeaseProof.generate())


def observe(
    stored: StoredJobSubmission,
    authorization: WorkerAuthorization,
    *,
    age: timedelta = timedelta(0),
) -> PreparedJobClaim:
    return PreparedJobClaim(
        worker_id=authorization.worker_id,
        stored=stored,
        observed_at=datetime.now(UTC) - age,
    )


async def totals(engine: AsyncEngine) -> tuple[int, int, int, int, int]:
    async with engine.connect() as connection:
        return tuple(
            (
                await connection.execute(
                    text("""
          SELECT (SELECT count(*) FROM execution_attempts),
                 (SELECT count(*) FROM worker_claim_receipts),
                 (SELECT count(*) FROM worker_coordination), receipt_count, worker_count
          FROM dispatch_slots WHERE name = 'coding'
        """)
                )
            ).one()
        )


@pytest.mark.parametrize("check_count", [1, 32])
def test_atomic_assignment_recovery_and_submission_projection(
    database: DatabaseFactory, check_count: int
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            stored = await save_job(engine, check_count=check_count)
            await save_job(engine, repository=stored.intent.policy)
            authorization, pending = authority(stored), request()
            store = claim_store(engine, (stored.intent.policy,))
            identity = pending.identify(authorization)
            statements: list[str] = []

            def count(
                connection: Connection,
                cursor: object,
                statement: str,
                parameters: object,
                context: object,
                executemany: bool,
            ) -> None:
                statements.append(statement)

            event.listen(engine.sync_engine, "before_cursor_execute", count)
            try:
                result = await store.claim(
                    identity, authorization, observe(stored, authorization)
                )
            finally:
                event.remove(engine.sync_engine, "before_cursor_execute", count)

            assert result.attempt is not None and result.stored is not None
            assert result.stored.job == stored.job
            assert result.stored.workflow.status is JobStatus.CLAIMED
            assert result.stored.workflow.version == 1
            assert result.lease_authority_active and not result.recovered
            assert (
                result.attempt.lease.expires_at <= authorization.credential_expires_at
            )
            assert len(statements) == 17
            assert await totals(engine) == (1, 1, 1, 1, 1)
            recovered = await store.recover(identity, authorization)
            assert recovered is not None and recovered.attempt == result.attempt
            assert recovered.stored == result.stored and recovered.recovered
            current = await job_submission_reader(engine).find_by_key(
                stored.intent.submission_key
            )
            assert current == result.stored
            queued = await queued_job_reader(engine).find_candidate(
                repository_ids=authorization.allowed_repository_ids
            )
            assert queued is not None and queued.job.id != stored.job.id
            assert pending.proof.reveal() not in repr(result)
            assert identity.fingerprint not in repr(result)
            assert identity.verifier.digest.hex() not in repr(result)
            assert await totals(engine) == (1, 1, 1, 1, 1)

    run(scenario())


def test_concurrent_identical_claims_commit_one_attempt(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            stored = await save_job(engine)
            authorization, pending = authority(stored), request()
            identity, observation = (
                pending.identify(authorization),
                observe(stored, authorization),
            )
            stores = [claim_store(engine, (stored.intent.policy,)) for _ in range(4)]

            results = await asyncio.gather(
                *(store.claim(identity, authorization, observation) for store in stores)
            )

            assert len({result.attempt.id for result in results if result.attempt}) == 1
            assert sum(not result.recovered for result in results) == 1
            assert await totals(engine) == (1, 1, 1, 1, 1)

    run(scenario())


def test_two_workers_cannot_exceed_singleton_capacity(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            first = await save_job(engine)
            second = await save_job(engine, repository=first.intent.policy)
            workers = [authority(first), authority(second)]
            definitions = [first, second]
            store = claim_store(engine, (first.intent.policy,))

            results = await asyncio.gather(
                *(
                    store.claim(
                        request().identify(worker), worker, observe(definition, worker)
                    )
                    for worker, definition in zip(workers, definitions, strict=True)
                )
            )

            assert sum(result.attempt is not None for result in results) == 1
            assert (
                sum(result.no_work is NoWorkReason.OCCUPIED for result in results) == 1
            )
            assert await totals(engine) == (1, 2, 2, 2, 2)

    run(scenario())


def test_no_work_replay_stays_empty_after_jobs_arrive(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            stored = await save_job(engine)
            authorization, pending = authority(stored), request()
            store = claim_store(engine, (stored.intent.policy,))
            identity = pending.identify(authorization)
            original = await store.claim(identity, authorization, None)
            await save_job(engine, repository=stored.intent.policy)

            recovered = await store.claim(
                identity, authorization, observe(stored, authorization)
            )

            assert original.no_work is NoWorkReason.EMPTY
            assert recovered.no_work is NoWorkReason.EMPTY and recovered.recovered
            assert recovered.attempt is None
            assert await totals(engine) == (0, 1, 1, 1, 1)

    run(scenario())


def test_conflicting_proof_cannot_recover_at_quota(database: DatabaseFactory) -> None:
    async def scenario() -> None:
        async with database() as engine:
            stored = await save_job(engine)
            authorization, pending = authority(stored), request()
            store = claim_store(
                engine,
                (stored.intent.policy,),
                ClaimLimits(worker_receipts=1, global_receipts=1),
            )
            await store.claim(pending.identify(authorization), authorization, None)
            changed = WorkerClaimRequest(
                claim_request_id=pending.claim_request_id, proof=LeaseProof.generate()
            )

            with pytest.raises(ClaimReplayConflict):
                await store.recover(changed.identify(authorization), authorization)

            original = await store.recover(
                pending.identify(authorization), authorization
            )
            assert original is not None and original.recovered
            assert await totals(engine) == (0, 1, 1, 1, 1)

    run(scenario())


@pytest.mark.parametrize("boundary", ["worker", "global", "enrollment", "poll"])
def test_new_admission_denied_but_exact_recovery_survives(
    database: DatabaseFactory, boundary: str
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            stored = await save_job(engine)
            owner, pending = authority(stored), request()
            generous = claim_store(engine, (stored.intent.policy,))
            identity = pending.identify(owner)
            await generous.claim(identity, owner, None)
            limits = ClaimLimits(
                worker_receipts=1 if boundary == "worker" else 100_000,
                global_receipts=1 if boundary == "global" else 1_000_000,
                historical_workers=1 if boundary == "enrollment" else 256,
            )
            store = claim_store(engine, (stored.intent.policy,), limits)
            newcomer = (
                authority(stored) if boundary in ("global", "enrollment") else owner
            )

            with pytest.raises(ClaimAdmissionRejected):
                await store.claim(request().identify(newcomer), newcomer, None)

            recovered = await store.recover(identity, owner)
            assert recovered is not None and recovered.recovered
            assert await totals(engine) == (0, 1, 1, 1, 1)

    run(scenario())


@pytest.mark.parametrize("seconds", [31, -1])
def test_stale_or_future_observation_records_no_assignment(
    database: DatabaseFactory, seconds: int
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            stored = await save_job(
                engine, created_at=datetime.now(UTC) - timedelta(minutes=1)
            )
            owner, pending = authority(stored), request()
            store = claim_store(engine, (stored.intent.policy,))

            result = await store.claim(
                pending.identify(owner),
                owner,
                observe(stored, owner, age=timedelta(seconds=seconds)),
            )

            assert result.no_work is NoWorkReason.STALE
            assert await totals(engine) == (0, 1, 1, 1, 1)

    run(scenario())


def test_observed_candidate_race_records_changed_result(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            stored = await save_job(engine)
            first, second = authority(stored), authority(stored)
            store = claim_store(engine, (stored.intent.policy,))
            stale = observe(stored, second)
            await store.claim(request().identify(first), first, observe(stored, first))

            result = await store.claim(request().identify(second), second, stale)

            assert result.no_work is NoWorkReason.CHANGED
            assert await totals(engine) == (1, 2, 2, 2, 2)

    run(scenario())


def test_expired_assignment_recovery_retains_capacity_and_original_deadline(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            stored = await save_job(engine)
            owner = authority(stored, expiry=datetime.now(UTC) + timedelta(seconds=0.3))
            pending, store = request(), claim_store(engine, (stored.intent.policy,))
            result = await store.claim(
                pending.identify(owner), owner, observe(stored, owner)
            )
            await asyncio.sleep(0.35)
            rotated = replace(
                owner, credential_expires_at=datetime.now(UTC) + timedelta(hours=1)
            )

            recovered = await store.recover(pending.identify(rotated), rotated)

            assert recovered is not None and not recovered.lease_authority_active
            assert recovered.attempt == result.attempt
            other = authority(stored)
            blocked = await store.claim(request().identify(other), other, None)
            assert blocked.no_work is NoWorkReason.OCCUPIED
            assert await totals(engine) == (1, 2, 2, 2, 2)

    run(scenario())


@pytest.mark.parametrize(
    "lock_target", ["dispatch_slots", "source_tasks", "job_workflows"]
)
def test_expiry_is_rechecked_after_lock_wait(
    database: DatabaseFactory, lock_target: str
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            stored = await save_job(engine)
            owner = authority(stored, expiry=datetime.now(UTC) + timedelta(seconds=0.2))
            pending, store = request(), claim_store(engine, (stored.intent.policy,))
            observation = observe(stored, owner)
            async with engine.connect() as blocker:
                await blocker.begin()
                await blocker.execute(text(f"SELECT 1 FROM {lock_target} FOR UPDATE"))
                task = asyncio.create_task(
                    store.claim(pending.identify(owner), owner, observation)
                )
                await asyncio.sleep(0.3)
                await blocker.rollback()

                with pytest.raises(WorkerAuthorityExpired):
                    await task

            assert await totals(engine) == (0, 0, 0, 0, 0)

    run(scenario())


def test_lock_timeout_fails_without_consuming_receipt(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            stored = await save_job(engine)
            owner, store = (
                authority(stored),
                claim_store(engine, (stored.intent.policy,)),
            )
            async with engine.connect() as blocker:
                await blocker.begin()
                await blocker.execute(text("SELECT 1 FROM dispatch_slots FOR UPDATE"))

                with pytest.raises(ClaimPersistenceError) as raised:
                    await store.claim(request().identify(owner), owner, None)

                await blocker.rollback()
            assert "postgres" not in str(raised.value).lower()
            assert await totals(engine) == (0, 0, 0, 0, 0)

    run(scenario())


@pytest.mark.parametrize(
    "change", ["scope", "expiry", "identity", "observation", "policy", "disabled"]
)
def test_current_authority_and_policy_gate_new_claims(
    database: DatabaseFactory, change: str
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            stored = await save_job(engine)
            owner, pending = authority(stored), request()
            identity = pending.identify(owner)
            observation = observe(stored, owner)
            policy = stored.intent.policy
            expected: type[Exception] = ClaimScopeRejected
            if change == "scope":
                owner = replace(owner, allowed_repository_ids=frozenset({uuid4()}))
            elif change == "expiry":
                owner = replace(
                    owner,
                    credential_expires_at=datetime.now(UTC) - timedelta(seconds=1),
                )
                expected = WorkerAuthorityExpired
            elif change == "identity":
                identity = pending.identify(replace(owner, worker_id=uuid4()))
            elif change == "observation":
                observation = replace(observation, worker_id=uuid4())
            elif change == "policy":
                policy = replace(policy, target_branch="changed")
                expected = ClaimEligibilityRejected
            else:
                policy = replace(policy, enabled=False)
            store = claim_store(engine, (policy,))

            with pytest.raises(expected):
                await store.claim(identity, owner, observation)

            assert await totals(engine) == (0, 0, 0, 0, 0)

    run(scenario())


def test_scope_removal_denies_assignment_recovery(database: DatabaseFactory) -> None:
    async def scenario() -> None:
        async with database() as engine:
            stored = await save_job(engine)
            owner, pending = authority(stored), request()
            store = claim_store(engine, (stored.intent.policy,))
            await store.claim(pending.identify(owner), owner, observe(stored, owner))
            restricted = replace(owner, allowed_repository_ids=frozenset({uuid4()}))

            with pytest.raises(ClaimScopeRejected):
                await store.recover(pending.identify(restricted), restricted)

            assert await totals(engine) == (1, 1, 1, 1, 1)

    run(scenario())


def test_failure_before_receipt_rolls_back_all_claim_state(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            stored = await save_job(engine)
            owner, pending = authority(stored), request()
            store = claim_store(engine, (stored.intent.policy,))

            def fail(
                connection: Connection,
                cursor: object,
                statement: str,
                parameters: object,
                context: object,
                executemany: bool,
            ) -> None:
                if statement.startswith("INSERT INTO worker_claim_receipts"):
                    raise SQLAlchemyError("Test-only private failure detail.")

            event.listen(engine.sync_engine, "before_cursor_execute", fail)
            try:
                with pytest.raises(ClaimPersistenceError) as raised:
                    await store.claim(
                        pending.identify(owner), owner, observe(stored, owner)
                    )
            finally:
                event.remove(engine.sync_engine, "before_cursor_execute", fail)

            assert "private" not in str(raised.value)
            assert await totals(engine) == (0, 0, 0, 0, 0)
            assert (
                await job_submission_reader(engine).find_by_key(
                    stored.intent.submission_key
                )
                == stored
            )
            result = await store.claim(
                pending.identify(owner), owner, observe(stored, owner)
            )
            assert result.attempt is not None

    run(scenario())


def test_deferred_commit_failure_is_recoverable_without_partial_assignment(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            stored = await save_job(engine)
            owner, pending = authority(stored), request()
            store = claim_store(engine, (stored.intent.policy,))
            async with engine.begin() as connection:
                await connection.execute(
                    text("""
                  CREATE FUNCTION test_reject_receipt_commit() RETURNS trigger LANGUAGE plpgsql AS $$
                  BEGIN RAISE EXCEPTION 'Test-only private commit failure.' USING ERRCODE = '23514'; END $$;
                  CREATE CONSTRAINT TRIGGER test_commit_failure AFTER INSERT ON worker_claim_receipts
                    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION test_reject_receipt_commit();
                """)
                )

            with pytest.raises(ClaimPersistenceError) as raised:
                await store.claim(
                    pending.identify(owner), owner, observe(stored, owner)
                )

            assert "private" not in str(raised.value)
            assert await totals(engine) == (0, 0, 0, 0, 0)
            assert await store.recover(pending.identify(owner), owner) is None
            async with engine.begin() as connection:
                await connection.execute(
                    text("DROP TRIGGER test_commit_failure ON worker_claim_receipts")
                )
            result = await store.claim(
                pending.identify(owner), owner, observe(stored, owner)
            )
            assert result.attempt is not None

    run(scenario())


async def seed_idle_history(
    engine: AsyncEngine, owner: WorkerAuthorization, *, count: int
) -> tuple[ClaimRequestIdentity, ...]:
    identities = tuple(request().identify(owner) for _ in range(count))
    last = datetime.now(UTC) - timedelta(minutes=1)
    first = last - timedelta(seconds=30 * (count - 1))
    async with engine.begin() as connection:
        await connection.execute(
            insert(WorkerCoordinationRecord).values(
                worker_id=owner.worker_id,
                enrolled_at=first,
                next_poll_at=last + timedelta(seconds=30),
                receipt_count=count,
                version=count,
            )
        )
        await connection.execute(
            insert(ClaimReceiptRecord),
            [
                dict(
                    worker_id=identity.worker_id,
                    claim_request_id=identity.claim_request_id,
                    protocol_version=1,
                    fingerprint_version=1,
                    verifier_version=1,
                    fingerprint=identity.fingerprint,
                    verifier=identity.verifier.digest,
                    outcome="no_work",
                    no_work_reason="empty",
                    job_id=None,
                    attempt_id=None,
                    recorded_at=first + timedelta(seconds=30 * index),
                )
                for index, identity in enumerate(identities)
            ],
        )
        await connection.execute(
            text("""
          DO $$ DECLARE i integer; BEGIN
            FOR i IN 1..(SELECT receipt_count FROM worker_coordination) LOOP
              UPDATE dispatch_slots SET receipt_count = receipt_count + 1,
                version = version + 1, worker_count = 1 WHERE name = 'coding';
            END LOOP;
          END $$;
        """)
        )
    return identities


def test_existing_worker_new_poll_updates_monotonic_counters(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            stored = await save_job(engine)
            owner, pending = authority(stored), request()
            await seed_idle_history(engine, owner, count=1)
            store = claim_store(engine, (stored.intent.policy,))

            result = await store.claim(
                pending.identify(owner), owner, observe(stored, owner)
            )

            assert result.attempt is not None
            assert await totals(engine) == (1, 2, 1, 2, 1)
            async with engine.connect() as connection:
                count, version = (
                    await connection.execute(
                        text("SELECT receipt_count, version FROM worker_coordination")
                    )
                ).one()
            assert count == version == 2

    run(scenario())


def test_replay_uses_primary_key_with_bounded_history(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            stored = await save_job(engine)
            owner = authority(stored)
            identities = await seed_idle_history(engine, owner, count=1001)
            accepted = identities[501]
            async with engine.begin() as connection:
                await connection.execute(text("ANALYZE worker_claim_receipts"))
                rows = (
                    (
                        await connection.execute(
                            text("""
                  EXPLAIN SELECT * FROM worker_claim_receipts
                  WHERE worker_id = :worker AND claim_request_id = :request
                """),
                            {
                                "worker": owner.worker_id,
                                "request": accepted.claim_request_id,
                            },
                        )
                    )
                    .scalars()
                    .all()
                )
            store = claim_store(engine, (stored.intent.policy,))
            statements: list[str] = []

            def count(
                connection: Connection,
                cursor: object,
                statement: str,
                parameters: object,
                context: object,
                executemany: bool,
            ) -> None:
                statements.append(statement)

            event.listen(engine.sync_engine, "before_cursor_execute", count)
            try:
                recovered = await store.recover(accepted, owner)
            finally:
                event.remove(engine.sync_engine, "before_cursor_execute", count)

            assert any(
                "Index Scan" in row and "worker_claim_receipts_pkey" in row
                for row in rows
            )
            assert recovered is not None and recovered.no_work is NoWorkReason.EMPTY
            assert len(statements) == 6
            assert await totals(engine) == (0, 1001, 1, 1001, 1)

    run(scenario())


def test_bootstrap_claim_and_replay_release_database_before_provider_reads(
    database: DatabaseFactory,
    worker_configuration: ControlSettings,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            configuration = worker_configuration.model_copy(
                update={
                    "workers": tuple(
                        worker.model_copy(
                            update={
                                "expires_at": datetime.now(UTC) + timedelta(hours=1)
                            }
                        )
                        for worker in worker_configuration.workers
                    )
                }
            )
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

            def respond(incoming: httpx.Request) -> httpx.Response:
                assert isinstance(engine.sync_engine.pool, AsyncAdaptedQueuePool)
                assert engine.sync_engine.pool.checkedout() == 0
                assert incoming.method == "GET"
                if incoming.url.host == "api.atlassian.com":
                    calls.append("jira")
                    return httpx.Response(200, json=payload)
                calls.append("github")
                if incoming.url.path.endswith("/pulls"):
                    return httpx.Response(200, json=[])
                if "/git/ref/" in incoming.url.path:
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
            authorization = worker_authenticator(configuration.workers)(
                configuration.workers[0].auth_token.get_secret_value()
            )
            assert authorization is not None
            pending = request()
            calls.clear()

            async with open_job_claimer(
                configuration,
                engine,
                jira_transport=transport,
                github_transport=transport,
            ) as claimer:
                assigned = await claimer.execute(pending, authorization)
                recovered = await claimer.execute(pending, authorization)

            assert (
                assigned.attempt is not None and recovered.attempt == assigned.attempt
            )
            assert (
                assigned.stored is not None and assigned.stored.job == saved.stored.job
            )
            assert recovered.recovered
            assert calls == ["jira", "github", "github", "github", "github"]
            assert isinstance(engine.sync_engine.pool, AsyncAdaptedQueuePool)
            assert engine.sync_engine.pool.checkedout() == 0

    run(scenario())


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM dispatch_slots",
        "DELETE FROM worker_coordination",
        "DELETE FROM execution_attempts",
        "DELETE FROM worker_claim_receipts",
        "UPDATE worker_claim_receipts SET outcome = 'no_work'",
        "UPDATE dispatch_slots SET reserved_attempt_id = NULL, receipt_count = receipt_count + 1, version = version + 1",
        "UPDATE dispatch_slots SET receipt_count = 0, version = 0",
        "UPDATE worker_coordination SET receipt_count = 0, version = 0",
        "UPDATE execution_attempts SET expires_at = expires_at + interval '1 hour'",
        "UPDATE job_workflows SET status = 'queued', workflow_version = 0, active_attempt_id = NULL, last_attempt_id = NULL, last_attempt_number = 0",
    ],
)
def test_durable_history_and_reservation_cannot_reset(
    database: DatabaseFactory, sql: str
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            stored = await save_job(engine)
            owner, store = (
                authority(stored),
                claim_store(engine, (stored.intent.policy,)),
            )
            await store.claim(request().identify(owner), owner, observe(stored, owner))

            with pytest.raises(SQLAlchemyError):
                async with engine.begin() as connection:
                    await connection.execute(text(sql))

            assert await totals(engine) == (1, 1, 1, 1, 1)

    run(scenario())


@pytest.mark.parametrize("assigned", [True, False])
def test_downgrade_refuses_any_durable_claim_history(
    database: DatabaseFactory, assigned: bool
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            stored = await save_job(engine)
            owner, store = (
                authority(stored),
                claim_store(engine, (stored.intent.policy,)),
            )
            await store.claim(
                request().identify(owner),
                owner,
                observe(stored, owner) if assigned else None,
            )

            def downgrade(connection: Connection) -> None:
                config = Config("alembic.ini")
                config.attributes["connection"] = connection
                command.downgrade(config, "0005_queued_candidate_index")

            with pytest.raises(SQLAlchemyError):
                async with engine.begin() as connection:
                    await connection.run_sync(downgrade)

            assert (await totals(engine))[1:] == (1, 1, 1, 1)

    run(scenario())
