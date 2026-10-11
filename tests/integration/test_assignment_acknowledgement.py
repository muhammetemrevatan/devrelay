import asyncio
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import event, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

from devrelay.application.acknowledgements import AcknowledgeAssignedJob
from devrelay.application.assignment_delivery import ReadAssignedJob
from devrelay.application.contracts.acknowledgements import (
    AcknowledgementRejected,
    WorkerAcknowledgementRequest,
)
from devrelay.application.contracts.assignment_delivery import (
    AssignmentDelivery,
    AssignmentUnavailable,
)
from devrelay.application.contracts.assignments import (
    ClaimPersistenceError,
    ClaimScopeRejected,
)
from devrelay.application.contracts.claims import ClaimReplayConflict, LeaseProof
from devrelay.application.contracts.eligibility import WorkerAuthorityExpired
from devrelay.application.contracts.submissions import StoredJobSubmission
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.claim_persistence import ClaimStore
from devrelay.bootstrap.persistence import (
    acknowledgement_store,
    claim_store,
    job_submission_reader,
)
from devrelay.domain.jobs.values import AttemptStatus, JobStatus
from tests.acknowledgement_fixtures import acknowledgement_intent, changed_intent
from tests.integration.job_fixtures import save_job
from tests.integration.test_atomic_claim_persistence import (
    authority,
    observe,
    request,
    run,
)

pytestmark = pytest.mark.postgres
type DatabaseFactory = Callable[[], AbstractAsyncContextManager[AsyncEngine]]


@dataclass(frozen=True)
class Assigned:
    service: AcknowledgeAssignedJob
    request: WorkerAcknowledgementRequest
    authorization: WorkerAuthorization
    claims: ClaimStore
    stored: StoredJobSubmission


async def assigned(
    engine: AsyncEngine, *, check_count: int = 1, short_lease: bool = False
) -> Assigned:
    stored = await save_job(engine, check_count=check_count)
    authorization = authority(
        stored, expiry=datetime.now(UTC) + timedelta(seconds=2) if short_lease else None
    )
    pending = request()
    claims = claim_store(engine, (stored.intent.policy,))
    result = await claims.claim(
        pending.identify(authorization), authorization, observe(stored, authorization)
    )
    delivery = AssignmentDelivery.from_result(
        result,
        worker_id=authorization.worker_id,
        claim_request_id=pending.claim_request_id,
    )
    return Assigned(
        service=AcknowledgeAssignedJob(
            ReadAssignedJob(claims, lambda proof: None),
            acknowledgement_store(engine, (stored.intent.policy,)),
        ),
        request=WorkerAcknowledgementRequest(
            intent=acknowledgement_intent(delivery), claim=pending
        ),
        authorization=authorization,
        claims=claims,
        stored=stored,
    )


async def counts(engine: AsyncEngine) -> tuple[int, int, str, int, int, int]:
    async with engine.connect() as connection:
        row = (
            await connection.execute(
                text("""
          SELECT (SELECT count(*) FROM worker_acknowledgement_receipts),
                 (SELECT count(*) FROM job_workflow_events WHERE workflow_version=2),
                 status, workflow_version, s.receipt_count, s.worker_count
          FROM job_workflows CROSS JOIN dispatch_slots s
        """)
            )
        ).one()
        return tuple(row)


@pytest.mark.parametrize("check_count", [1, 32])
def test_acknowledgement_commits_once_and_preserves_claim_delivery(
    database: DatabaseFactory, check_count: int
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await assigned(engine, check_count=check_count)
            original = await setup.claims.recover(
                setup.request.claim.identify(setup.authorization), setup.authorization
            )
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
                accepted = await setup.service.execute(
                    setup.request, setup.authorization
                )
            finally:
                event.remove(engine.sync_engine, "before_cursor_execute", record)

            assert not accepted.recovered and accepted.workflow_version == 2
            assert len(statements) == 30
            assert (
                accepted.intent == setup.request.intent
                and accepted.lease_authority_active
            )
            recovered = await setup.claims.recover(
                setup.request.claim.identify(setup.authorization), setup.authorization
            )
            assert (
                original is not None
                and original.attempt is not None
                and recovered is not None
                and recovered.attempt is not None
            )
            assert recovered.attempt.status is AttemptStatus.RUNNING
            assert (
                recovered.attempt.lease.expires_at == original.attempt.lease.expires_at
            )
            assert (
                recovered.attempt.lease.generation == original.attempt.lease.generation
            )
            assert recovered.recorded_at == original.recorded_at
            reader = ReadAssignedJob(setup.claims, lambda proof: None)
            delivery = await reader.execute(
                setup.request.intent.attempt_id,
                setup.request.claim,
                setup.authorization,
            )
            assert (
                delivery.definition.fingerprint()
                == setup.request.intent.definition_digest
            )
            replay = await setup.service.execute(setup.request, setup.authorization)
            assert (
                replay.recovered and replay.acknowledged_at == accepted.acknowledged_at
            )
            assert replay.expires_at == accepted.expires_at
            submission = await job_submission_reader(engine).find_by_key(
                setup.stored.intent.submission_key
            )
            assert (
                submission is not None
                and submission.workflow.status is JobStatus.RUNNING
            )
            assert (
                submission.job.status is JobStatus.QUEUED
                and submission.intent == setup.stored.intent
            )
            assert await counts(engine) == (1, 1, "running", 2, 1, 1)

    run(scenario())


@pytest.mark.parametrize("same_identity", [True, False])
def test_concurrent_acknowledgements_advance_one_history(
    database: DatabaseFactory, same_identity: bool
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await assigned(engine)
            second = (
                setup.request
                if same_identity
                else replace(
                    setup.request,
                    intent=replace(
                        setup.request.intent, acknowledgement_request_id=uuid4()
                    ),
                )
            )

            results = await asyncio.gather(
                setup.service.execute(setup.request, setup.authorization),
                setup.service.execute(second, setup.authorization),
                return_exceptions=True,
            )

            if same_identity:
                assert sorted(
                    result.recovered
                    for result in results
                    if not isinstance(result, BaseException)
                ) == [False, True]
            else:
                assert (
                    sum(
                        isinstance(result, AcknowledgementRejected)
                        for result in results
                    )
                    == 1
                )
            assert await counts(engine) == (1, 1, "running", 2, 1, 1)

    run(scenario())


@pytest.mark.parametrize(
    "field",
    [
        "snapshot_id",
        "base_revision",
        "definition_digest",
        "attempt_id",
        "claim_request_id",
    ],
)
def test_changed_inputs_cannot_acknowledge(
    database: DatabaseFactory, field: str
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await assigned(engine)
            intent = changed_intent(setup.request.intent, field)
            pending = (
                setup.request.claim
                if field != "claim_request_id"
                else replace(
                    setup.request.claim, claim_request_id=intent.claim_request_id
                )
            )
            invalid = WorkerAcknowledgementRequest(intent=intent, claim=pending)

            with pytest.raises((AcknowledgementRejected, AssignmentUnavailable)):
                await setup.service.execute(invalid, setup.authorization)

            assert await counts(engine) == (0, 0, "claimed", 1, 1, 1)

    run(scenario())


def test_wrong_proof_cannot_create_acknowledgement(database: DatabaseFactory) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await assigned(engine)
            invalid = replace(
                setup.request,
                claim=replace(setup.request.claim, proof=LeaseProof.generate()),
            )

            with pytest.raises(ClaimReplayConflict):
                await setup.service.execute(invalid, setup.authorization)

            assert await counts(engine) == (0, 0, "claimed", 1, 1, 1)

    run(scenario())


@pytest.mark.parametrize("removed", ["worker", "scope", "policy"])
def test_current_scope_gates_acknowledgement_recovery(
    database: DatabaseFactory, removed: str
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await assigned(engine)
            await setup.service.execute(setup.request, setup.authorization)
            authorization = setup.authorization
            service = setup.service
            if removed == "worker":
                authorization = replace(authorization, worker_id=uuid4())
            elif removed == "scope":
                authorization = replace(
                    authorization, allowed_repository_ids=frozenset({uuid4()})
                )
            else:
                policies = (replace(setup.stored.intent.policy, enabled=False),)
                service = AcknowledgeAssignedJob(
                    ReadAssignedJob(claim_store(engine, policies), lambda proof: None),
                    acknowledgement_store(engine, policies),
                )

            with pytest.raises((ClaimScopeRejected, AssignmentUnavailable)):
                await service.execute(setup.request, authorization)

            assert await counts(engine) == (1, 1, "running", 2, 1, 1)

    run(scenario())


@pytest.mark.parametrize("already_acknowledged", [True, False])
def test_expired_lease_recovers_only_original_acknowledgement(
    database: DatabaseFactory, already_acknowledged: bool
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await assigned(engine, short_lease=True)
            original = (
                await setup.service.execute(setup.request, setup.authorization)
                if already_acknowledged
                else None
            )
            await asyncio.sleep(
                max(
                    0,
                    (
                        setup.authorization.credential_expires_at - datetime.now(UTC)
                    ).total_seconds(),
                )
                + 0.03
            )
            renewed = replace(
                setup.authorization,
                credential_expires_at=datetime.now(UTC) + timedelta(hours=1),
            )

            if already_acknowledged:
                result = await setup.service.execute(setup.request, renewed)
                assert (
                    original is not None
                    and result.recovered
                    and not result.lease_authority_active
                )
                assert (
                    result.acknowledged_at == original.acknowledged_at
                    and result.expires_at == original.expires_at
                )
            else:
                with pytest.raises(AcknowledgementRejected):
                    await setup.service.execute(setup.request, renewed)
                assert await counts(engine) == (0, 0, "claimed", 1, 1, 1)

    run(scenario())


def test_receipt_failure_rolls_back_attempt_workflow_and_event(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await assigned(engine)
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "CREATE FUNCTION reject_test_ack() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test rollback'; END $$"
                    )
                )
                await connection.execute(
                    text(
                        "CREATE TRIGGER reject_test_ack BEFORE INSERT ON worker_acknowledgement_receipts FOR EACH ROW EXECUTE FUNCTION reject_test_ack()"
                    )
                )

            with pytest.raises(ClaimPersistenceError):
                await setup.service.execute(setup.request, setup.authorization)

            assert await counts(engine) == (0, 0, "claimed", 1, 1, 1)
            recovered = await setup.claims.recover(
                setup.request.claim.identify(setup.authorization), setup.authorization
            )
            assert (
                recovered is not None
                and recovered.attempt is not None
                and recovered.attempt.acknowledged_at is None
            )

    run(scenario())


@pytest.mark.parametrize(
    "mutation",
    [
        "UPDATE worker_acknowledgement_receipts SET fingerprint=repeat('a',64)",
        "DELETE FROM worker_acknowledgement_receipts",
        "UPDATE execution_attempts SET expires_at=expires_at+interval '1 minute'",
        "UPDATE dispatch_slots SET reserved_attempt_id=NULL",
        "UPDATE jobs SET base_revision=repeat('b',40)",
    ],
)
def test_acknowledgement_preserves_immutable_pins_and_reservation(
    database: DatabaseFactory, mutation: str
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await assigned(engine)
            await setup.service.execute(setup.request, setup.authorization)

            with pytest.raises(SQLAlchemyError):
                async with engine.begin() as connection:
                    await connection.execute(text(mutation))

            assert await counts(engine) == (1, 1, "running", 2, 1, 1)

    run(scenario())


@pytest.mark.parametrize("acknowledged", [True, False])
def test_downgrade_retains_acknowledgement_history(
    database: DatabaseFactory, acknowledged: bool
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await assigned(engine)
            if acknowledged:
                await setup.service.execute(setup.request, setup.authorization)

            def downgrade(connection: Connection) -> None:
                config = Config("alembic.ini")
                config.attributes["connection"] = connection
                command.downgrade(config, "0006_atomic_claims")

            if acknowledged:
                with pytest.raises(SQLAlchemyError):
                    async with engine.begin() as connection:
                        await connection.run_sync(downgrade)
                assert await counts(engine) == (1, 1, "running", 2, 1, 1)
            else:
                async with engine.begin() as connection:
                    await connection.run_sync(downgrade)

                def upgrade(connection: Connection) -> None:
                    config = Config("alembic.ini")
                    config.attributes["connection"] = connection
                    command.upgrade(config, "head")

                async with engine.begin() as connection:
                    await connection.run_sync(upgrade)
                recovered = await setup.claims.recover(
                    setup.request.claim.identify(setup.authorization),
                    setup.authorization,
                )
                assert recovered is not None and recovered.attempt is not None
                assert recovered.attempt.status is AttemptStatus.CLAIMED
                assert await counts(engine) == (0, 0, "claimed", 1, 1, 1)

    run(scenario())


def test_expiry_after_source_lock_wait_is_checked_with_fresh_db_time(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await assigned(engine)
            authorization = replace(
                setup.authorization,
                credential_expires_at=datetime.now(UTC) + timedelta(seconds=0.3),
            )
            async with engine.connect() as blocked:
                transaction = await blocked.begin()
                await blocked.execute(
                    text("SELECT id FROM source_tasks WHERE id=:id FOR UPDATE"),
                    {"id": setup.stored.job.source_task_id},
                )
                operation = asyncio.create_task(
                    acknowledgement_store(
                        engine, (setup.stored.intent.policy,)
                    ).acknowledge(
                        setup.request.identify(authorization),
                        authorization,
                        definition_digest=setup.request.intent.definition_digest,
                    )
                )
                await asyncio.sleep(0.4)
                await transaction.rollback()

                with pytest.raises(WorkerAuthorityExpired):
                    await operation

            assert await counts(engine) == (0, 0, "claimed", 1, 1, 1)

    run(scenario())


def test_expiry_during_receipt_write_rolls_back_before_disclosure(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await assigned(engine)
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "CREATE FUNCTION delay_test_ack() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(1.1); RETURN NEW; END $$"
                    )
                )
                await connection.execute(
                    text(
                        "CREATE TRIGGER delay_test_ack BEFORE INSERT ON worker_acknowledgement_receipts FOR EACH ROW EXECUTE FUNCTION delay_test_ack()"
                    )
                )
            authorization = replace(
                setup.authorization,
                credential_expires_at=datetime.now(UTC) + timedelta(seconds=1),
            )
            inserts: list[str] = []

            def record(
                connection: object,
                cursor: object,
                statement: str,
                parameters: object,
                context: object,
                many: bool,
            ) -> None:
                if statement.startswith("INSERT INTO worker_acknowledgement_receipts"):
                    inserts.append(statement)

            event.listen(engine.sync_engine, "before_cursor_execute", record)
            try:
                with pytest.raises(WorkerAuthorityExpired):
                    await acknowledgement_store(
                        engine, (setup.stored.intent.policy,)
                    ).acknowledge(
                        setup.request.identify(authorization),
                        authorization,
                        definition_digest=setup.request.intent.definition_digest,
                    )
            finally:
                event.remove(engine.sync_engine, "before_cursor_execute", record)

            assert len(inserts) == 1
            assert await counts(engine) == (0, 0, "claimed", 1, 1, 1)

    run(scenario())


def test_partial_raw_attempt_transition_cannot_commit_without_matching_history(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            await assigned(engine)

            with pytest.raises(
                SQLAlchemyError, match="Claim ownership must commit together"
            ):
                async with engine.begin() as connection:
                    await connection.execute(
                        text(
                            "WITH instant AS (SELECT clock_timestamp() AS now) UPDATE execution_attempts SET status='running', updated_at=instant.now, acknowledged_at=instant.now FROM instant"
                        )
                    )

            assert await counts(engine) == (0, 0, "claimed", 1, 1, 1)

    run(scenario())
