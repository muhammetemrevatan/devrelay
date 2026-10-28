import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import async_sessionmaker

from devrelay.application.coding_result_preparation import prepare_coding_result
from devrelay.application.contracts.assignments import ClaimScopeRejected
from devrelay.application.contracts.claims import ClaimReplayConflict, LeaseProof
from devrelay.application.contracts.coding_result_receipts import (
    CodingPublicationState,
    CodingPublicationStep,
    CodingResultRejected,
    CodingResultUnavailable,
    CodingWritePermit,
)
from devrelay.application.contracts.eligibility import WorkerAuthorityExpired
from devrelay.domain.jobs.attempt import LeaseExpired
from devrelay.infrastructure.persistence.result_store import SqlAlchemyResultStore
from tests.integration.job_fixtures import save_job
from tests.integration.test_atomic_claim_persistence import authority, observe, run
from tests.integration.test_atomic_claim_persistence import request as claim_request
from tests.integration.test_coding_result_store import (
    Context,
    DatabaseFactory,
    context,
    operation,
    projection,
    publication,
    write_plan,
)


def step() -> CodingPublicationStep:
    return CodingPublicationStep(
        result_id=uuid4(),
        invocation_id=uuid4(),
        expected_operation_version=1,
        step_id=uuid4(),
        step_kind="blob:" + "a" * 40,
        input_digest="b" * 64,
    )


def test_write_permit_temporal_metadata_is_paired_finite_and_never_replay_authority() -> (
    None
):
    original = step()
    at = datetime.now(UTC)
    until = at + timedelta(seconds=1)
    permit = CodingWritePermit(
        step=original,
        operation_version=2,
        created=True,
        authorized_at=at.astimezone(timezone(timedelta(hours=3))),
        authorized_until=until,
    )

    assert permit.authorized_at == at and permit.authorized_at.tzinfo is UTC
    assert permit.authorized_until == until
    assert replace(permit, authorized_at=None, authorized_until=None).created
    historical = CodingWritePermit(step=original, operation_version=2, created=False)
    assert historical.authorized_at is historical.authorized_until is None
    for changed in (
        {"created": False},
        {"authorized_at": None},
        {"authorized_until": None},
        {"authorized_until": at},
        {"authorized_until": at - timedelta(microseconds=1)},
        {"authorized_at": at.replace(tzinfo=None)},
        {"authorized_until": until.replace(tzinfo=None)},
    ):
        with pytest.raises(ValueError):
            replace(permit, **changed)


async def start(setup: Context) -> CodingPublicationStep:
    accepted = await setup.store.accept(setup.prepared, setup.authorization)
    invocation = uuid4()
    started = await setup.store.begin_publication(
        setup.prepared, setup.authorization, invocation_id=invocation
    )
    return replace(
        step(),
        result_id=accepted.receipt.result_id,
        invocation_id=invocation,
        expected_operation_version=started.operation_version,
    )


@pytest.mark.postgres
@pytest.mark.parametrize("credential_limited", [False, True])
def test_new_permit_uses_fresh_db_time_and_minimum_credential_or_lease_expiry(
    database: DatabaseFactory, credential_limited: bool
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine)
            planned = await start(setup)
            authorization = setup.authorization
            if credential_limited:
                authorization = replace(
                    authorization,
                    credential_expires_at=datetime.now(UTC) + timedelta(seconds=10),
                )
            async with engine.connect() as connection:
                before = (
                    await connection.execute(text("SELECT clock_timestamp()"))
                ).scalar_one()
                expires = (
                    await connection.execute(
                        text("SELECT current_expires_at FROM attempt_lease_authorities")
                    )
                ).scalar_one()

            permit = await setup.store.plan_write(
                setup.prepared, authorization, step=planned
            )
            replay = await setup.store.plan_write(
                setup.prepared, authorization, step=planned
            )

            async with engine.connect() as connection:
                after = (
                    await connection.execute(text("SELECT clock_timestamp()"))
                ).scalar_one()
            assert permit.created and not replay.created
            assert (
                permit.authorized_at is not None and permit.authorized_until is not None
            )
            assert before <= permit.authorized_at <= after
            assert (
                permit.authorized_at
                < permit.authorized_until
                == min(expires, authorization.credential_expires_at)
            )
            assert replay.authorized_at is replay.authorized_until is None

    run(scenario())


@pytest.mark.postgres
@pytest.mark.parametrize("expired_authority", ["lease", "credential"])
def test_write_plan_rechecks_current_authority_after_waiting_for_slot_lock(
    database: DatabaseFactory, expired_authority: str
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine, short_lease=True)
            planned = await start(setup)
            deadline = setup.authorization.credential_expires_at
            authorization = setup.authorization
            if expired_authority == "lease":
                authorization = replace(
                    authorization,
                    credential_expires_at=datetime.now(UTC) + timedelta(hours=1),
                )
            waiting = asyncio.Event()

            def observe_lock(
                connection: object,
                cursor: object,
                statement: str,
                parameters: object,
                execution_context: object,
                many: bool,
            ) -> None:
                if "FROM dispatch_slots" in statement and "FOR UPDATE" in statement:
                    waiting.set()

            async with engine.connect() as blocker, blocker.begin():
                await blocker.execute(
                    text(
                        "SELECT name FROM dispatch_slots WHERE name='coding' FOR UPDATE"
                    )
                )
                event.listen(engine.sync_engine, "before_cursor_execute", observe_lock)
                pending = asyncio.create_task(
                    setup.store.plan_write(setup.prepared, authorization, step=planned)
                )
                try:
                    await asyncio.wait_for(waiting.wait(), 1)
                    await asyncio.sleep(
                        max(0, (deadline - datetime.now(UTC)).total_seconds()) + 0.03
                    )
                finally:
                    event.remove(
                        engine.sync_engine, "before_cursor_execute", observe_lock
                    )

            with pytest.raises(
                LeaseExpired if expired_authority == "lease" else WorkerAuthorityExpired
            ):
                await pending
            assert await operation(engine) == ("publishing", 1, 2, 2)
            assert (await projection(engine))["release_count"] == 0

    run(scenario())


@pytest.mark.postgres
def test_history_covers_absent_accepted_started_planned_observed_and_sticky_uncertainty(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine)
            assert (
                await setup.store.read_publication(setup.prepared, setup.authorization)
                is None
            )
            accepted = await setup.store.accept(setup.prepared, setup.authorization)
            initial = await setup.store.read_publication(
                setup.prepared, setup.authorization
            )
            assert initial is not None and initial.status.receipt == accepted.receipt
            assert initial.status.state is CodingPublicationState.ACCEPTED
            assert all(
                value is None
                for value in (
                    initial.invocation_id,
                    initial.last_step,
                    initial.last_observation_digest,
                )
            )
            assert initial.last_event_kind == "accepted"
            planned = await start(setup)
            started = await setup.store.read_publication(
                setup.prepared, setup.authorization
            )
            assert (
                started is not None and started.last_event_kind == "publishing_started"
            )
            assert (
                started.invocation_id == planned.invocation_id
                and started.last_step is None
            )
            await setup.store.plan_write(
                setup.prepared, setup.authorization, step=planned
            )
            first = await setup.store.read_publication(
                setup.prepared, setup.authorization
            )
            assert first is not None and first.last_step == planned
            assert (
                first.last_event_kind == "write_planned"
                and first.last_observation_digest is None
            )
            observed = await setup.store.observe_write(
                setup.prepared,
                setup.authorization,
                step=planned,
                observation_digest="c" * 64,
            )
            settled = await setup.store.read_publication(
                setup.prepared, setup.authorization
            )
            assert settled is not None and settled.status == observed
            assert (
                settled.last_step == planned
                and settled.last_observation_digest == "c" * 64
            )
            assert settled.last_event_kind == "write_observed"
            next_step = replace(
                planned,
                step_id=uuid4(),
                step_kind="tree:" + "d" * 40,
                expected_operation_version=observed.operation_version,
            )
            await setup.store.plan_write(
                setup.prepared, setup.authorization, step=next_step
            )
            uncertain = await setup.store.mark_uncertain(
                setup.prepared, setup.authorization, step=next_step
            )
            pending = await setup.store.read_publication(
                setup.prepared, setup.authorization
            )
            assert pending is not None and pending.status == uncertain
            assert (
                pending.last_event_kind == "uncertain"
                and pending.last_step == next_step
            )
            assert pending.last_observation_digest is None

            await setup.store.observe_write(
                setup.prepared,
                setup.authorization,
                step=next_step,
                observation_digest="e" * 64,
            )
            late = await setup.store.read_publication(
                setup.prepared, setup.authorization
            )

            assert (
                late is not None
                and late.status.state is CodingPublicationState.UNCERTAIN
            )
            assert (
                late.last_event_kind == "write_observed" and late.last_step == next_step
            )
            assert late.last_observation_digest == "e" * 64
            assert (
                late.last_step.expected_operation_version == observed.operation_version
            )
            assert (await projection(engine))["release_count"] == 0

    run(scenario())


@pytest.mark.postgres
def test_expired_lease_history_and_replayed_plan_do_not_grant_fresh_write_authority(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine, short_lease=True)
            _, planned = await write_plan(setup)
            await asyncio.sleep(
                max(
                    0,
                    (
                        setup.authorization.credential_expires_at - datetime.now(UTC)
                    ).total_seconds(),
                )
                + 0.03
            )
            current = replace(
                setup.authorization,
                credential_expires_at=datetime.now(UTC) + timedelta(hours=1),
            )
            before = await projection(engine)

            history = await setup.store.read_publication(setup.prepared, current)
            replay = await setup.store.plan_write(setup.prepared, current, step=planned)

            assert history is not None and history.last_step == planned
            assert (
                not replay.created
                and replay.authorized_at is replay.authorized_until is None
            )
            with pytest.raises(LeaseExpired):
                await setup.store.plan_write(
                    setup.prepared,
                    current,
                    step=replace(
                        planned, step_id=uuid4(), expected_operation_version=2
                    ),
                )
            with pytest.raises(WorkerAuthorityExpired):
                await setup.store.read_publication(setup.prepared, setup.authorization)
            assert await projection(engine) == before

    run(scenario())


@pytest.mark.postgres
@pytest.mark.parametrize(
    "gate", ["worker", "scope", "credential", "disabled", "proof", "request"]
)
def test_history_requires_current_scoped_worker_and_exact_original_input(
    database: DatabaseFactory, gate: str
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine)
            await write_plan(setup)
            before = await projection(engine)
            store, prepared, authorization = (
                setup.store,
                setup.prepared,
                setup.authorization,
            )
            if gate == "worker":
                authorization = replace(authorization, worker_id=uuid4())
            elif gate == "scope":
                authorization = replace(
                    authorization, allowed_repository_ids=frozenset({uuid4()})
                )
            elif gate == "credential":
                authorization = replace(
                    authorization,
                    credential_expires_at=datetime.now(UTC) - timedelta(seconds=1),
                )
            elif gate == "disabled":
                store = SqlAlchemyResultStore(
                    async_sessionmaker(engine, expire_on_commit=False, autoflush=False),
                    (replace(setup.assigned.stored.intent.policy, enabled=False),),
                )
            else:
                request = setup.request
                if gate == "proof":
                    request = replace(
                        request,
                        claim=replace(request.claim, proof=LeaseProof.generate()),
                    )
                else:
                    request = replace(
                        request,
                        intent=replace(request.intent, result_request_id=uuid4()),
                    )
                prepared = await asyncio.to_thread(
                    prepare_coding_result,
                    request,
                    authorization,
                    setup.prepared.definition,
                )

            with pytest.raises(
                (
                    CodingResultRejected,
                    ClaimScopeRejected,
                    WorkerAuthorityExpired,
                    ClaimReplayConflict,
                )
            ):
                await store.read_publication(prepared, authorization)

            assert await projection(engine) == before

    run(scenario())


@pytest.mark.postgres
@pytest.mark.parametrize(
    "corruption",
    [
        "missing_plan",
        "plan_input",
        "plan_owner",
        "plan_kind",
        "plan_time",
        "latest_input",
        "latest_owner",
        "latest_step",
    ],
)
def test_history_rejects_corrupt_original_plan_latest_event_association(
    database: DatabaseFactory, corruption: str
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine)
            _, planned = await write_plan(setup)
            await setup.store.observe_write(
                setup.prepared,
                setup.authorization,
                step=planned,
                observation_digest="c" * 64,
            )
            mutations = {
                "missing_plan": "DELETE FROM delivery_operation_events WHERE kind='write_planned'",
                "plan_input": "UPDATE delivery_operation_events SET input_digest=repeat('d',64) WHERE kind='write_planned'",
                "plan_owner": "UPDATE delivery_operation_events SET invocation_id=:other WHERE kind='write_planned'",
                "plan_kind": "UPDATE delivery_operation_events SET step_kind='another_kind' WHERE kind='write_planned'",
                "plan_time": "UPDATE delivery_operation_events SET occurred_at=occurred_at+interval '1 hour' WHERE kind='write_planned'",
                "latest_input": "UPDATE delivery_operation_events SET input_digest=repeat('d',64) WHERE kind='write_observed'",
                "latest_owner": "UPDATE delivery_operation_events SET invocation_id=:other WHERE kind='write_observed'",
                "latest_step": "UPDATE delivery_operation_events SET step_id=:other WHERE kind='write_observed'",
            }
            async with engine.begin() as connection:
                await connection.execute(
                    text("SET LOCAL session_replication_role=replica")
                )
                await connection.execute(
                    text(mutations[corruption]), {"other": uuid4()}
                )

            with pytest.raises(CodingResultUnavailable):
                await setup.store.read_publication(setup.prepared, setup.authorization)

    run(scenario())


@pytest.mark.postgres
def test_finalized_history_survives_new_slot_assignment_without_releasing_it(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine)
            prepared = await publication(engine, setup)
            terminal = await setup.store.finalize_publication(
                setup.prepared, setup.authorization, publication=prepared.verified
            )
            next_job = await save_job(
                engine, repository=setup.assigned.stored.intent.policy
            )
            next_authorization = authority(next_job)
            request = claim_request()
            next_claim = await setup.assigned.claims.claim(
                request.identify(next_authorization),
                next_authorization,
                observe(next_job, next_authorization),
            )
            assert next_claim.attempt is not None
            slot_query = text(
                "SELECT reserved_attempt_id, receipt_count, release_count, version FROM dispatch_slots WHERE name='coding'"
            )
            async with engine.connect() as connection:
                before = (await connection.execute(slot_query)).one()
            assert before.reserved_attempt_id == next_claim.attempt.id

            history = await setup.store.read_publication(
                setup.prepared, setup.authorization
            )

            assert history is not None and history.status.terminal == terminal
            assert history.last_event_kind == "finalized" and history.last_step is None
            assert history.last_observation_digest is None
            assert history.invocation_id == prepared.invocation_id
            async with engine.connect() as connection:
                after = (await connection.execute(slot_query)).one()
            assert after == before

    run(scenario())


@pytest.mark.postgres
def test_history_query_count_and_selected_columns_do_not_grow_with_write_history(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine)
            planned = await start(setup)
            statements: list[str] = []

            def record(
                connection: object,
                cursor: object,
                statement: str,
                parameters: object,
                execution_context: object,
                many: bool,
            ) -> None:
                statements.append(statement)

            async def read_count() -> int:
                statements.clear()
                event.listen(engine.sync_engine, "before_cursor_execute", record)
                try:
                    history = await setup.store.read_publication(
                        setup.prepared, setup.authorization
                    )
                    assert history is not None
                    repr(history)
                finally:
                    event.remove(engine.sync_engine, "before_cursor_execute", record)
                assert all(
                    "attempt_results.payload_bytes" not in statement
                    for statement in statements
                )
                assert not any(
                    statement.lstrip()
                    .upper()
                    .startswith(("INSERT", "UPDATE", "DELETE"))
                    for statement in statements
                )
                return len(statements)

            first_count = await read_count()
            for _ in range(8):
                await setup.store.plan_write(
                    setup.prepared, setup.authorization, step=planned
                )
                observed = await setup.store.observe_write(
                    setup.prepared,
                    setup.authorization,
                    step=planned,
                    observation_digest="c" * 64,
                )
                planned = replace(
                    planned,
                    step_id=uuid4(),
                    expected_operation_version=observed.operation_version,
                )
            final_count = await read_count()

            assert first_count == final_count <= 24
            assert await operation(engine) == ("publishing", 17, 18, 18)

    run(scenario())
