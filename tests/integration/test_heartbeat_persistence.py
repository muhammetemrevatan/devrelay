import asyncio
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import event, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from devrelay.application.contracts.assignments import (
    ClaimPersistenceError,
    ClaimScopeRejected,
)
from devrelay.application.contracts.claims import ClaimReplayConflict, LeaseProof
from devrelay.application.contracts.eligibility import WorkerAuthorityExpired
from devrelay.application.contracts.heartbeats import (
    HeartbeatAdmissionRejected,
    HeartbeatLimits,
    HeartbeatRejected,
    HeartbeatRequest,
    HeartbeatResult,
)
from devrelay.bootstrap.persistence import heartbeat_store, job_submission_reader
from devrelay.infrastructure.persistence.claim_store import SqlAlchemyClaimStore
from tests.integration.test_assignment_acknowledgement import Assigned, assigned
from tests.integration.test_atomic_claim_persistence import run

pytestmark = pytest.mark.postgres
type DatabaseFactory = Callable[[], AbstractAsyncContextManager[AsyncEngine]]


def request(setup: Assigned, *, revision: int = 1) -> HeartbeatRequest:
    return HeartbeatRequest(
        claim=setup.request.claim,
        heartbeat_request_id=uuid4(),
        attempt_id=setup.request.intent.attempt_id,
        generation=1,
        expected_lease_revision=revision,
    )


async def counts(engine: AsyncEngine) -> tuple[int, int, int, int, int, int]:
    async with engine.connect() as connection:
        row = (
            await connection.execute(
                text("""
                SELECT (SELECT count(*) FROM worker_heartbeat_receipts),
                       h.receipt_count, h.lease_revision,
                       s.heartbeat_receipt_count, c.heartbeat_receipt_count,
                       (SELECT count(*) FROM job_workflow_events)
                FROM attempt_lease_authorities h
                JOIN worker_coordination c ON c.worker_id=h.worker_id
                CROSS JOIN dispatch_slots s
                """)
            )
        ).one()
        return tuple(row)


class ObservedTimeStore(SqlAlchemyClaimStore):
    """Controlled storage-clock readings; real DB-clock lock tests are separate."""

    def __init__(
        self,
        engine: AsyncEngine,
        setup: Assigned,
        now: datetime,
        limits: HeartbeatLimits | None = None,
    ) -> None:
        super().__init__(
            async_sessionmaker(engine, expire_on_commit=False, autoflush=False),
            (setup.stored.intent.policy,),
            heartbeat_limits=limits,
        )
        self.now = now

    async def _clock(self, session: AsyncSession) -> datetime:
        return self.now


def test_recovery_after_original_expiry_uses_current_authority_without_repinning(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await acknowledged(engine)
            store = ObservedTimeStore(
                engine, setup, datetime.now(UTC) + timedelta(seconds=10)
            )
            pending = request(setup)
            first = await store.heartbeat(
                pending.identify(setup.authorization), setup.authorization
            )
            store.now = first.authority.reference.expires_at + timedelta(seconds=1)

            recovered = await store.recover(
                setup.request.claim.identify(setup.authorization), setup.authorization
            )
            acknowledgement = await store.acknowledge(
                setup.request.identify(setup.authorization),
                setup.authorization,
                definition_digest=setup.request.intent.definition_digest,
            )
            heartbeat = await store.heartbeat(
                pending.identify(setup.authorization), setup.authorization
            )

            assert recovered is not None and recovered.attempt is not None
            assert (
                recovered.lease_authority_active
                and acknowledgement.lease_authority_active
            )
            assert (
                recovered.attempt.lease.expires_at
                == first.authority.reference.expires_at
            )
            assert acknowledgement.expires_at == first.authority.reference.expires_at
            assert not heartbeat.renewed and not heartbeat.stop_required
            assert (
                heartbeat.authority.current_expires_at
                == first.authority.current_expires_at
            )
            assert await counts(engine) == (1, 1, 2, 1, 1, 3)

    run(scenario())


@pytest.mark.parametrize("revision", [2, 3])
def test_ahead_revision_cannot_create_a_receipt(
    database: DatabaseFactory, revision: int
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await acknowledged(engine)
            pending = request(setup, revision=revision)
            store = heartbeat_store(engine, (setup.stored.intent.policy,))

            with pytest.raises(HeartbeatRejected):
                await store.heartbeat(
                    pending.identify(setup.authorization), setup.authorization
                )

            assert await counts(engine) == (0, 0, 1, 0, 0, 3)

    run(scenario())


async def acknowledged(engine: AsyncEngine, *, check_count: int = 1) -> Assigned:
    setup = await assigned(engine, check_count=check_count)
    await setup.service.execute(setup.request, setup.authorization)
    return setup


@pytest.mark.parametrize("check_count", [1, 32])
def test_renewal_preserves_original_recovery_and_has_bounded_queries(
    database: DatabaseFactory, check_count: int
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await acknowledged(engine, check_count=check_count)
            pending = request(setup)
            store = heartbeat_store(engine, (setup.stored.intent.policy,))
            statements: list[str] = []

            def record(
                connection: object,
                cursor: object,
                statement: str,
                parameters: object,
                context: object,
                many: bool,
            ) -> None:
                statements.append(statement)

            event.listen(engine.sync_engine, "before_cursor_execute", record)
            try:
                result = await store.heartbeat(
                    pending.identify(setup.authorization), setup.authorization
                )
            finally:
                event.remove(engine.sync_engine, "before_cursor_execute", record)

            assert result.renewed and not result.stop_required
            assert result.authority.lease_revision == 2
            assert (
                result.authority.current_expires_at
                > result.authority.reference.expires_at
            )
            assert len(statements) <= 40
            assert not any("count(" in item.lower() for item in statements)
            recovered = await setup.claims.recover(
                setup.request.claim.identify(setup.authorization), setup.authorization
            )
            acknowledgement = await setup.service.execute(
                setup.request, setup.authorization
            )
            submission = await job_submission_reader(engine).find_by_key(
                setup.stored.intent.submission_key
            )
            assert recovered is not None and recovered.attempt is not None
            assert (
                recovered.attempt.lease.expires_at
                == result.authority.reference.expires_at
            )
            assert acknowledgement.expires_at == result.authority.reference.expires_at
            assert submission is not None and submission.intent == setup.stored.intent
            assert await counts(engine) == (1, 1, 2, 1, 1, 3)

    run(scenario())


@pytest.mark.parametrize("same_identity", [True, False])
def test_concurrent_renewals_mutate_once(
    database: DatabaseFactory, same_identity: bool
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await acknowledged(engine)
            first = request(setup)
            second = first if same_identity else request(setup)
            store = heartbeat_store(engine, (setup.stored.intent.policy,))

            results = await asyncio.gather(
                store.heartbeat(
                    first.identify(setup.authorization), setup.authorization
                ),
                store.heartbeat(
                    second.identify(setup.authorization), setup.authorization
                ),
                return_exceptions=True,
            )

            successes = [item for item in results if isinstance(item, HeartbeatResult)]
            assert sum(item.renewed for item in successes) == 1
            if same_identity:
                assert len(successes) == 2
            else:
                assert len(successes) == 1
                assert sum(isinstance(item, HeartbeatRejected) for item in results) == 1
            assert await counts(engine) == (1, 1, 2, 1, 1, 3)

    run(scenario())


def test_lost_response_replay_reports_current_revision_without_second_renewal(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await acknowledged(engine)
            store = ObservedTimeStore(
                engine, setup, datetime.now(UTC) + timedelta(seconds=10)
            )
            first = request(setup)
            accepted = await store.heartbeat(
                first.identify(setup.authorization), setup.authorization
            )
            store.now += timedelta(seconds=6)
            later = await store.heartbeat(
                request(setup, revision=2).identify(setup.authorization),
                setup.authorization,
            )

            replay = await store.heartbeat(
                first.identify(setup.authorization), setup.authorization
            )

            assert accepted.renewed and later.renewed
            assert not replay.renewed and not replay.stop_required
            assert replay.authority.lease_revision == 3
            assert (
                replay.authority.current_expires_at
                == later.authority.current_expires_at
            )
            assert await counts(engine) == (2, 2, 3, 2, 2, 3)

    run(scenario())


@pytest.mark.parametrize("changed", ["revision", "proof", "attempt"])
def test_retained_identity_rejects_changed_input_after_later_renewal(
    database: DatabaseFactory, changed: str
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await acknowledged(engine)
            store = ObservedTimeStore(
                engine, setup, datetime.now(UTC) + timedelta(seconds=10)
            )
            original = request(setup)
            await store.heartbeat(
                original.identify(setup.authorization), setup.authorization
            )
            store.now += timedelta(seconds=6)
            await store.heartbeat(
                request(setup, revision=2).identify(setup.authorization),
                setup.authorization,
            )
            invalid = (
                replace(original, expected_lease_revision=3)
                if changed == "revision"
                else replace(original, attempt_id=uuid4())
                if changed == "attempt"
                else replace(
                    original, claim=replace(original.claim, proof=LeaseProof.generate())
                )
            )

            with pytest.raises(ClaimReplayConflict):
                await store.heartbeat(
                    invalid.identify(setup.authorization), setup.authorization
                )

            assert await counts(engine) == (2, 2, 3, 2, 2, 3)

    run(scenario())


@pytest.mark.parametrize(
    "budget", ["attempt_receipts", "worker_receipts", "global_receipts"]
)
def test_exhausted_budget_preserves_exact_recovery(
    database: DatabaseFactory, budget: str
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await acknowledged(engine)
            limits = replace(HeartbeatLimits(), **{budget: 1})
            store = ObservedTimeStore(
                engine, setup, datetime.now(UTC) + timedelta(seconds=10), limits
            )
            original = request(setup)
            await store.heartbeat(
                original.identify(setup.authorization), setup.authorization
            )
            store.now += timedelta(seconds=6)

            with pytest.raises(HeartbeatAdmissionRejected):
                await store.heartbeat(
                    request(setup, revision=2).identify(setup.authorization),
                    setup.authorization,
                )

            replay = await store.heartbeat(
                original.identify(setup.authorization), setup.authorization
            )
            assert not replay.renewed and replay.authority.lease_revision == 2
            assert await counts(engine) == (1, 1, 2, 1, 1, 3)

    run(scenario())


def test_new_operation_rate_is_durable_and_replay_bypasses_it(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await acknowledged(engine)
            original = request(setup)
            store = heartbeat_store(engine, (setup.stored.intent.policy,))
            await store.heartbeat(
                original.identify(setup.authorization), setup.authorization
            )
            restarted = heartbeat_store(engine, (setup.stored.intent.policy,))

            with pytest.raises(HeartbeatAdmissionRejected):
                await restarted.heartbeat(
                    request(setup, revision=2).identify(setup.authorization),
                    setup.authorization,
                )

            replay = await restarted.heartbeat(
                original.identify(setup.authorization), setup.authorization
            )
            assert not replay.renewed and not replay.stop_required
            assert await counts(engine) == (1, 1, 2, 1, 1, 3)

    run(scenario())


def test_shortened_credential_cannot_reduce_current_lease_or_gain_new_time(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await acknowledged(engine)
            store = ObservedTimeStore(
                engine, setup, datetime.now(UTC) + timedelta(seconds=10)
            )
            original = request(setup)
            prior = await store.heartbeat(
                original.identify(setup.authorization), setup.authorization
            )
            store.now += timedelta(seconds=6)
            shortened = replace(
                setup.authorization,
                credential_expires_at=store.now + timedelta(seconds=10),
            )
            pending = request(setup, revision=2)

            result = await store.heartbeat(pending.identify(shortened), shortened)

            assert not result.renewed and not result.stop_required
            assert (
                result.authority.current_expires_at
                == prior.authority.current_expires_at
            )
            assert result.authority.lease_revision == prior.authority.lease_revision
            result.require_correlation(pending, shortened)
            assert await counts(engine) == (2, 2, 2, 2, 2, 3)

    run(scenario())


def test_expired_lease_keeps_reservation_and_replay_reports_stop(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await acknowledged(engine)
            store = ObservedTimeStore(
                engine, setup, datetime.now(UTC) + timedelta(seconds=10)
            )
            original = request(setup)
            accepted = await store.heartbeat(
                original.identify(setup.authorization), setup.authorization
            )
            store.now = accepted.authority.current_expires_at

            replay = await store.heartbeat(
                original.identify(setup.authorization), setup.authorization
            )

            assert replay.stop_required and not replay.renewed
            with pytest.raises(HeartbeatRejected):
                await store.heartbeat(
                    request(setup, revision=2).identify(setup.authorization),
                    setup.authorization,
                )
            async with engine.connect() as connection:
                slot = (
                    await connection.execute(
                        text("SELECT reserved_attempt_id FROM dispatch_slots")
                    )
                ).scalar_one()
            assert slot == original.attempt_id
            assert await counts(engine) == (1, 1, 2, 1, 1, 3)

    run(scenario())


@pytest.mark.parametrize("mismatch", ["scope", "policy", "credential"])
def test_current_authorization_gates_replay(
    database: DatabaseFactory, mismatch: str
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await acknowledged(engine)
            original = request(setup)
            store = heartbeat_store(engine, (setup.stored.intent.policy,))
            await store.heartbeat(
                original.identify(setup.authorization), setup.authorization
            )
            authorization = setup.authorization
            if mismatch == "scope":
                authorization = replace(
                    authorization, allowed_repository_ids=frozenset({uuid4()})
                )
            elif mismatch == "credential":
                authorization = replace(
                    authorization, credential_expires_at=datetime.now(UTC)
                )
            else:
                store = heartbeat_store(
                    engine, (replace(setup.stored.intent.policy, enabled=False),)
                )

            with pytest.raises((ClaimScopeRejected, WorkerAuthorityExpired)):
                await store.heartbeat(original.identify(authorization), authorization)

            assert await counts(engine) == (1, 1, 2, 1, 1, 3)

    run(scenario())


def test_unacknowledged_assignment_cannot_renew(database: DatabaseFactory) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await assigned(engine)
            pending = request(setup)
            store = heartbeat_store(engine, (setup.stored.intent.policy,))

            with pytest.raises(HeartbeatRejected):
                await store.heartbeat(
                    pending.identify(setup.authorization), setup.authorization
                )

            assert await counts(engine) == (0, 0, 1, 0, 0, 2)

    run(scenario())


def test_receipt_failure_rolls_back_authority_and_all_counters(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await acknowledged(engine)
            pending = request(setup)
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "CREATE FUNCTION reject_test_hb() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test rollback'; END $$"
                    )
                )
                await connection.execute(
                    text(
                        "CREATE TRIGGER reject_test_hb BEFORE INSERT ON worker_heartbeat_receipts FOR EACH ROW EXECUTE FUNCTION reject_test_hb()"
                    )
                )
            store = heartbeat_store(engine, (setup.stored.intent.policy,))

            with pytest.raises(ClaimPersistenceError):
                await store.heartbeat(
                    pending.identify(setup.authorization), setup.authorization
                )

            assert await counts(engine) == (0, 0, 1, 0, 0, 3)

    run(scenario())


@pytest.mark.parametrize(
    "mutation",
    [
        "UPDATE attempt_lease_authorities SET current_expires_at=current_expires_at+interval '1 second'",
        "UPDATE attempt_lease_authorities SET receipt_count=receipt_count+1",
        "DELETE FROM attempt_lease_authorities",
        "DELETE FROM worker_heartbeat_receipts",
        "UPDATE worker_heartbeat_receipts SET fingerprint=repeat('a',64)",
        "UPDATE dispatch_slots SET heartbeat_receipt_count=0",
        "UPDATE worker_coordination SET heartbeat_receipt_count=0",
        "UPDATE execution_attempts SET expires_at=expires_at+interval '1 second'",
    ],
)
def test_partial_mutations_or_history_erasure_cannot_commit(
    database: DatabaseFactory, mutation: str
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await acknowledged(engine)
            store = heartbeat_store(engine, (setup.stored.intent.policy,))
            await store.heartbeat(
                request(setup).identify(setup.authorization), setup.authorization
            )

            with pytest.raises(SQLAlchemyError):
                async with engine.begin() as connection:
                    await connection.execute(text(mutation))

            assert await counts(engine) == (1, 1, 2, 1, 1, 3)

    run(scenario())


@pytest.mark.parametrize("renewed", [False, True])
def test_downgrade_preserves_accepted_heartbeat_history(
    database: DatabaseFactory, renewed: bool
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await acknowledged(engine)
            if renewed:
                await heartbeat_store(engine, (setup.stored.intent.policy,)).heartbeat(
                    request(setup).identify(setup.authorization), setup.authorization
                )

            def downgrade(connection: Connection) -> None:
                config = Config("alembic.ini")
                config.attributes["connection"] = connection
                command.downgrade(config, "0007_assignment_acknowledgements")

            if renewed:
                with pytest.raises(SQLAlchemyError):
                    async with engine.begin() as connection:
                        await connection.run_sync(downgrade)
                assert await counts(engine) == (1, 1, 2, 1, 1, 3)
            else:
                async with engine.begin() as connection:
                    await connection.run_sync(downgrade)
                async with engine.connect() as connection:
                    acknowledgements = (
                        await connection.execute(
                            text("SELECT count(*) FROM worker_acknowledgement_receipts")
                        )
                    ).scalar_one()
                assert acknowledgements == 1

    run(scenario())


def test_downgrade_refuses_unrenewed_accepted_operation(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await acknowledged(engine)
            store = ObservedTimeStore(
                engine, setup, datetime.now(UTC) + timedelta(seconds=10)
            )
            shortened = replace(
                setup.authorization,
                credential_expires_at=store.now + timedelta(seconds=10),
            )
            pending = request(setup)
            result = await store.heartbeat(pending.identify(shortened), shortened)

            def downgrade(connection: Connection) -> None:
                config = Config("alembic.ini")
                config.attributes["connection"] = connection
                command.downgrade(config, "0007_assignment_acknowledgements")

            with pytest.raises(SQLAlchemyError):
                async with engine.begin() as connection:
                    await connection.run_sync(downgrade)

            assert not result.renewed and not result.stop_required
            assert await counts(engine) == (1, 1, 1, 1, 1, 3)

    run(scenario())


@pytest.mark.parametrize("phase", ["claimed", "running", "expired_running"])
def test_existing_assignments_backfill_original_authority(
    database: DatabaseFactory, phase: str
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await assigned(engine, short_lease=phase == "expired_running")
            if phase != "claimed":
                await setup.service.execute(setup.request, setup.authorization)
            if phase == "expired_running":
                await asyncio.sleep(
                    max(
                        0.0,
                        (
                            setup.authorization.credential_expires_at
                            - datetime.now(UTC)
                        ).total_seconds(),
                    )
                    + 0.03
                )
            async with engine.connect() as connection:
                original = tuple(
                    (
                        await connection.execute(
                            text(
                                "SELECT issued_at, expires_at, updated_at, acknowledged_at FROM execution_attempts"
                            )
                        )
                    ).one()
                )

            def migrate(connection: Connection) -> None:
                config = Config("alembic.ini")
                config.attributes["connection"] = connection
                command.downgrade(config, "0007_assignment_acknowledgements")
                command.upgrade(config, "head")

            async with engine.begin() as connection:
                await connection.run_sync(migrate)

            async with engine.connect() as connection:
                current = tuple(
                    (
                        await connection.execute(
                            text(
                                "SELECT issued_at, expires_at, updated_at, acknowledged_at FROM execution_attempts"
                            )
                        )
                    ).one()
                )
                authority = tuple(
                    (
                        await connection.execute(
                            text(
                                "SELECT updated_at, current_expires_at, lease_revision, receipt_count, next_heartbeat_at FROM attempt_lease_authorities"
                            )
                        )
                    ).one()
                )
            restored = await job_submission_reader(engine).find_by_key(
                setup.stored.intent.submission_key
            )
            assert current == original
            assert authority == (original[0], original[1], 1, 0, original[0])
            assert restored is not None and restored.intent == setup.stored.intent
            assert await counts(engine) == (
                0,
                0,
                1,
                0,
                0,
                2 if phase == "claimed" else 3,
            )

    run(scenario())


@pytest.mark.parametrize(
    "lock_target",
    ["dispatch_slots", "source_tasks", "job_workflows", "attempt_lease_authorities"],
)
def test_current_credential_expiry_is_rechecked_after_lock_wait(
    database: DatabaseFactory, lock_target: str
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await acknowledged(engine)
            pending = request(setup)
            authorization = replace(
                setup.authorization,
                credential_expires_at=datetime.now(UTC) + timedelta(seconds=1),
            )
            store = heartbeat_store(engine, (setup.stored.intent.policy,))
            async with engine.connect() as blocker:
                await blocker.begin()
                await blocker.execute(text(f"SELECT 1 FROM {lock_target} FOR UPDATE"))
                waiting = asyncio.Event()

                def record(
                    connection: object,
                    cursor: object,
                    statement: str,
                    parameters: object,
                    context: object,
                    many: bool,
                ) -> None:
                    if lock_target in statement and "FOR UPDATE" in statement:
                        waiting.set()

                event.listen(engine.sync_engine, "before_cursor_execute", record)
                task = asyncio.create_task(
                    store.heartbeat(pending.identify(authorization), authorization)
                )
                try:
                    await asyncio.wait_for(waiting.wait(), timeout=2)
                    await asyncio.sleep(
                        max(
                            0.0,
                            (
                                authorization.credential_expires_at - datetime.now(UTC)
                            ).total_seconds(),
                        )
                        + 0.05
                    )
                    assert not task.done()
                    await blocker.rollback()

                    with pytest.raises(WorkerAuthorityExpired):
                        await task
                finally:
                    event.remove(engine.sync_engine, "before_cursor_execute", record)
                    if not task.done():
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)

            assert await counts(engine) == (0, 0, 1, 0, 0, 3)

    run(scenario())


def test_expiry_during_receipt_insert_rolls_back_before_disclosure(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await acknowledged(engine)
            store = heartbeat_store(engine, (setup.stored.intent.policy,))
            pending = request(setup)
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "CREATE FUNCTION delay_test_hb() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(1.1); RETURN NEW; END $$"
                    )
                )
                await connection.execute(
                    text(
                        "CREATE TRIGGER delay_test_hb BEFORE INSERT ON worker_heartbeat_receipts FOR EACH ROW EXECUTE FUNCTION delay_test_hb()"
                    )
                )
            authorization = replace(
                setup.authorization,
                credential_expires_at=datetime.now(UTC) + timedelta(seconds=1),
            )

            with pytest.raises(WorkerAuthorityExpired):
                await store.heartbeat(pending.identify(authorization), authorization)

            assert await counts(engine) == (0, 0, 1, 0, 0, 3)

    run(scenario())


def test_cancelled_receipt_insert_preserves_exact_retry(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await acknowledged(engine)
            store = heartbeat_store(engine, (setup.stored.intent.policy,))
            pending = request(setup)
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "CREATE FUNCTION delay_test_hb() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(1); RETURN NEW; END $$"
                    )
                )
                await connection.execute(
                    text(
                        "CREATE TRIGGER delay_test_hb BEFORE INSERT ON worker_heartbeat_receipts FOR EACH ROW EXECUTE FUNCTION delay_test_hb()"
                    )
                )
            inserting = asyncio.Event()

            def record(
                connection: object,
                cursor: object,
                statement: str,
                parameters: object,
                context: object,
                many: bool,
            ) -> None:
                if statement.startswith("INSERT INTO worker_heartbeat_receipts"):
                    inserting.set()

            event.listen(engine.sync_engine, "before_cursor_execute", record)
            try:
                task = asyncio.create_task(
                    store.heartbeat(
                        pending.identify(setup.authorization), setup.authorization
                    )
                )
                await asyncio.wait_for(inserting.wait(), timeout=3)
                task.cancel()

                with pytest.raises(asyncio.CancelledError):
                    await task
            finally:
                event.remove(engine.sync_engine, "before_cursor_execute", record)

            assert await counts(engine) == (0, 0, 1, 0, 0, 3)
            async with engine.begin() as connection:
                await connection.execute(
                    text("DROP TRIGGER delay_test_hb ON worker_heartbeat_receipts")
                )
            recovered = await store.heartbeat(
                pending.identify(setup.authorization), setup.authorization
            )
            assert recovered.renewed
            assert await counts(engine) == (1, 1, 2, 1, 1, 3)

    run(scenario())
