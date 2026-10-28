import asyncio
import threading
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from devrelay.application.assignment_delivery import ReadAssignedJob
from devrelay.application.coding_result_preparation import prepare_coding_result
from devrelay.application.contracts.assignments import ClaimScopeRejected
from devrelay.application.contracts.candidates import CandidateSnapshot
from devrelay.application.contracts.claims import ClaimReplayConflict, LeaseProof
from devrelay.application.contracts.coding_result_acceptance import PreparedCodingResult
from devrelay.application.contracts.coding_result_receipts import (
    CodingPublicationState,
    CodingPublicationStep,
    CodingResultAcceptance,
    CodingResultRejected,
    CodingResultUnavailable,
    VerifiedCodingPublication,
)
from devrelay.application.contracts.coding_result_requests import (
    WorkerCodingResultIntent,
    WorkerCodingResultRequest,
)
from devrelay.application.contracts.eligibility import WorkerAuthorityExpired
from devrelay.application.contracts.executions import (
    ExecutionIsolation,
    ExecutionPolicy,
    ExecutionRuntime,
    WorkerExecutionIntent,
)
from devrelay.application.contracts.heartbeats import HeartbeatRejected, HeartbeatResult
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.domain.jobs.attempt import LeaseExpired
from devrelay.domain.jobs.values import PullRequestReference
from devrelay.infrastructure.persistence.result_store import SqlAlchemyResultStore
from tests.integration.job_fixtures import save_job
from tests.integration.test_assignment_acknowledgement import Assigned, assigned
from tests.integration.test_atomic_claim_persistence import authority, observe, run
from tests.integration.test_atomic_claim_persistence import request as claim_request
from tests.integration.test_heartbeat_persistence import request as heartbeat_request
from tests.unit.test_coding_results import context as report_context

pytestmark = pytest.mark.postgres
type DatabaseFactory = Callable[[], AbstractAsyncContextManager[AsyncEngine]]


@dataclass
class Context:
    assigned: Assigned
    request: WorkerCodingResultRequest
    prepared: PreparedCodingResult
    store: SqlAlchemyResultStore

    @property
    def authorization(self) -> WorkerAuthorization:
        return self.assigned.authorization


async def context(
    engine: AsyncEngine,
    *,
    repairs: int = 0,
    check_count: int = 1,
    short_lease: bool = False,
) -> Context:
    setup = await assigned(engine, check_count=check_count, short_lease=short_lease)
    await setup.service.execute(setup.request, setup.authorization)
    delivery = await ReadAssignedJob(setup.claims, lambda proof: None).execute(
        setup.request.intent.attempt_id,
        setup.request.claim,
        setup.authorization,
    )
    execution = WorkerExecutionIntent(
        execution_id=uuid4(),
        acknowledgement=setup.request.intent,
        policy=ExecutionPolicy(
            runtime=ExecutionRuntime.CODEX,
            isolation=ExecutionIsolation.WINDOWS_RESTRICTED,
            policy_digest="a" * 64,
        ),
    )
    report = await asyncio.to_thread(
        report_context,
        repairs,
        definition=delivery.definition,
        execution_id=execution.execution_id,
    )
    pending = WorkerCodingResultRequest(
        intent=WorkerCodingResultIntent(
            result_request_id=uuid4(), execution=execution, result=report.result
        ),
        claim=setup.request.claim,
    )
    prepared = await asyncio.to_thread(
        prepare_coding_result, pending, setup.authorization, delivery.definition
    )
    return Context(
        setup,
        pending,
        prepared,
        SqlAlchemyResultStore(
            async_sessionmaker(engine, expire_on_commit=False, autoflush=False),
            (setup.stored.intent.policy,),
        ),
    )


async def projection(engine: AsyncEngine) -> dict[str, Any]:
    async with engine.connect() as connection:
        row = (
            (
                await connection.execute(
                    text("""
            SELECT w.status, w.workflow_version, w.repair_cycles,
                   w.active_attempt_id, w.last_attempt_id, w.accepted_result_id,
                   w.candidate_revision, a.status AS attempt_status,
                   a.issued_at, a.acknowledged_at, a.ended_at,
                   l.updated_at AS authority_updated_at, l.revoked_at,
                   l.lease_revision, l.receipt_count AS heartbeat_receipts,
                   s.reserved_attempt_id, s.receipt_count AS claim_receipts,
                   s.release_count, s.version AS slot_version,
                   (SELECT count(*) FROM attempt_results) AS results,
                   (SELECT count(*) FROM delivery_operations) AS operations,
                   (SELECT count(*) FROM delivery_operation_events) AS operation_events,
                   (SELECT count(*) FROM job_workflow_events) AS workflow_events
            FROM job_workflows w JOIN execution_attempts a ON a.id=w.last_attempt_id
            JOIN attempt_lease_authorities l ON l.attempt_id=a.id
            CROSS JOIN dispatch_slots s WHERE s.name='coding'
        """)
                )
            )
            .mappings()
            .one()
        )
        return dict(row)


@pytest.mark.parametrize("repairs,check_count", [(0, 1), (2, 32)])
def test_acceptance_atomically_records_full_result_and_every_workflow_event(
    database: DatabaseFactory,
    repairs: int,
    check_count: int,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine, repairs=repairs, check_count=check_count)
            before = await projection(engine)

            accepted = await setup.store.accept(setup.prepared, setup.authorization)

            assert not accepted.recovered
            accepted.receipt.require_input(
                setup.prepared.identity, job_id=setup.prepared.definition.job_id
            )
            after = await projection(engine)
            assert (after["status"], after["attempt_status"]) == (
                "publishing_pr",
                "publishing_pr",
            )
            assert after["workflow_version"] == 4 + 2 * repairs
            assert after["repair_cycles"] == repairs
            assert after["accepted_result_id"] == accepted.receipt.result_id
            assert (
                after["candidate_revision"]
                == setup.request.intent.result.final_candidate.revision
            )
            assert (
                after["results"],
                after["operations"],
                after["operation_events"],
            ) == (1, 1, 1)
            assert after["workflow_events"] == 5 + 2 * repairs
            assert (
                after["reserved_attempt_id"]
                == after["active_attempt_id"]
                == after["last_attempt_id"]
            )
            assert after["ended_at"] is None and after["revoked_at"] is None
            assert (
                after["claim_receipts"],
                after["release_count"],
                after["slot_version"],
            ) == (1, 0, 1)
            for field in (
                "issued_at",
                "acknowledged_at",
                "authority_updated_at",
                "lease_revision",
                "heartbeat_receipts",
            ):
                assert after[field] == before[field]
            async with engine.connect() as connection:
                stored = (
                    await connection.execute(
                        text(
                            "SELECT payload_bytes, payload_sha256, recorded_at FROM attempt_results"
                        )
                    )
                ).one()
                events = (
                    await connection.execute(
                        text(
                            "SELECT workflow_version, previous_version, occurred_at, result_id FROM job_workflow_events WHERE workflow_version>2 ORDER BY workflow_version"
                        )
                    )
                ).all()
            assert bytes(stored.payload_bytes) == setup.prepared.payload_bytes
            assert stored.payload_sha256 == setup.prepared.payload_sha256
            assert (
                setup.request.claim.proof.reveal().encode() not in stored.payload_bytes
            )
            assert tuple(row.workflow_version for row in events) == tuple(
                range(3, 5 + 2 * repairs)
            )
            assert all(
                row.previous_version == row.workflow_version - 1
                and row.occurred_at == accepted.receipt.recorded_at
                and row.result_id == accepted.receipt.result_id
                for row in events
            )
            assert stored.recorded_at == accepted.receipt.recorded_at

    run(scenario())


def test_absent_result_recovery_is_read_only_and_cannot_advance_workflow(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine)
            before = await projection(engine)
            assert (
                await setup.store.recover_result(setup.prepared, setup.authorization)
                is None
            )
            assert await projection(engine) == before

    run(scenario())


def test_exact_replay_returns_original_receipt_without_duplicate_history(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine)
            accepted = await setup.store.accept(setup.prepared, setup.authorization)
            before = await projection(engine)

            replay = await setup.store.accept(setup.prepared, setup.authorization)
            recovered = await setup.store.recover_result(
                setup.prepared, setup.authorization
            )

            assert replay.recovered and replay.receipt == accepted.receipt
            assert recovered is not None and recovered.receipt == accepted.receipt
            assert recovered.state is CodingPublicationState.ACCEPTED
            assert recovered.operation_version == 0 and recovered.terminal is None
            assert await projection(engine) == before

    run(scenario())


@pytest.mark.parametrize("changed", ["request_id", "diagnostic", "proof"])
def test_changed_result_input_cannot_replace_one_accepted_attempt_result(
    database: DatabaseFactory, changed: str
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine)
            await setup.store.accept(setup.prepared, setup.authorization)
            before = await projection(engine)
            pending = setup.request
            if changed == "request_id":
                pending = replace(
                    pending, intent=replace(pending.intent, result_request_id=uuid4())
                )
            elif changed == "proof":
                pending = replace(
                    pending, claim=replace(pending.claim, proof=LeaseProof.generate())
                )
            else:
                report = pending.intent.result
                first = report.cycles[0]
                check = replace(first.checks[0], diagnostic_tail="changed report")
                report = replace(
                    report, cycles=(replace(first, checks=(check, *first.checks[1:])),)
                )
                pending = replace(
                    pending, intent=replace(pending.intent, result=report)
                )
            changed_input = await asyncio.to_thread(
                prepare_coding_result,
                pending,
                setup.authorization,
                setup.prepared.definition,
            )

            with pytest.raises(ClaimReplayConflict):
                await setup.store.accept(changed_input, setup.authorization)
            with pytest.raises(ClaimReplayConflict):
                await setup.store.recover_result(changed_input, setup.authorization)
            assert await projection(engine) == before

    run(scenario())


@pytest.mark.parametrize("same_request", [True, False])
def test_concurrent_result_submissions_commit_exactly_one_result(
    database: DatabaseFactory, same_request: bool
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine)
            second = setup.prepared
            if not same_request:
                pending = replace(
                    setup.request,
                    intent=replace(setup.request.intent, result_request_id=uuid4()),
                )
                second = await asyncio.to_thread(
                    prepare_coding_result,
                    pending,
                    setup.authorization,
                    setup.prepared.definition,
                )
            results = await asyncio.gather(
                setup.store.accept(setup.prepared, setup.authorization),
                setup.store.accept(second, setup.authorization),
                return_exceptions=True,
            )
            accepted = [
                result
                for result in results
                if isinstance(result, CodingResultAcceptance)
            ]
            assert sum(not result.recovered for result in accepted) == 1
            if same_request:
                assert (
                    len(accepted) == 2
                    and sum(result.recovered for result in accepted) == 1
                )
            else:
                assert len(accepted) == 1
                assert (
                    sum(isinstance(result, ClaimReplayConflict) for result in results)
                    == 1
                )
            after = await projection(engine)
            assert (
                after["results"],
                after["operations"],
                after["operation_events"],
                after["workflow_events"],
            ) == (1, 1, 1, 5)

    run(scenario())


@pytest.mark.parametrize("gate", ["worker", "scope", "policy", "credential"])
def test_current_authentication_scope_gates_new_and_historical_results(
    database: DatabaseFactory, gate: str
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine)
            await setup.store.accept(setup.prepared, setup.authorization)
            before = await projection(engine)
            authorization, store = setup.authorization, setup.store
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
            else:
                store = SqlAlchemyResultStore(
                    async_sessionmaker(engine, expire_on_commit=False, autoflush=False),
                    (replace(setup.assigned.stored.intent.policy, enabled=False),),
                )
            for operation in (store.accept, store.recover_result):
                with pytest.raises(
                    (CodingResultRejected, ClaimScopeRejected, WorkerAuthorityExpired)
                ):
                    await operation(setup.prepared, authorization)
            assert await projection(engine) == before

    run(scenario())


@pytest.mark.parametrize("accepted_before_expiry", [False, True])
def test_lease_expiry_preserves_history_but_cannot_accept_new_publication_intent(
    database: DatabaseFactory, accepted_before_expiry: bool
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine, short_lease=True)
            accepted = (
                await setup.store.accept(setup.prepared, setup.authorization)
                if accepted_before_expiry
                else None
            )
            before = await projection(engine)
            await asyncio.sleep(
                max(
                    0,
                    (
                        setup.authorization.credential_expires_at - datetime.now(UTC)
                    ).total_seconds(),
                )
                + 0.03
            )
            authorization = replace(
                setup.authorization,
                credential_expires_at=datetime.now(UTC) + timedelta(hours=1),
            )
            if accepted is None:
                with pytest.raises(LeaseExpired):
                    await setup.store.accept(setup.prepared, authorization)
                assert (
                    await setup.store.recover_result(setup.prepared, authorization)
                    is None
                )
            else:
                replay = await setup.store.accept(setup.prepared, authorization)
                recovered = await setup.store.recover_result(
                    setup.prepared, authorization
                )
                assert replay.recovered and replay.receipt == accepted.receipt
                assert recovered is not None and recovered.receipt == accepted.receipt
                assert (
                    recovered.state is CodingPublicationState.ACCEPTED
                    and recovered.terminal is None
                )
            assert await projection(engine) == before

    run(scenario())


def test_late_workflow_event_failure_rolls_back_result_operation_and_projection(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine)
            before = await projection(engine)

            def reject(
                connection: object,
                cursor: object,
                statement: str,
                parameters: object,
                context: object,
                many: bool,
            ) -> None:
                if statement.startswith("INSERT INTO job_workflow_events"):
                    raise SQLAlchemyError("fixture workflow event failure")

            event.listen(engine.sync_engine, "before_cursor_execute", reject)
            try:
                with pytest.raises(CodingResultUnavailable):
                    await setup.store.accept(setup.prepared, setup.authorization)
            finally:
                event.remove(engine.sync_engine, "before_cursor_execute", reject)
            assert await projection(engine) == before

    run(scenario())


@dataclass
class Publication:
    accepted: CodingResultAcceptance
    invocation_id: UUID
    step: CodingPublicationStep
    verified: VerifiedCodingPublication


async def publication(engine: AsyncEngine, setup: Context) -> Publication:
    accepted, step = await write_plan(setup)
    observed = await setup.store.observe_write(
        setup.prepared, setup.authorization, step=step, observation_digest="c" * 64
    )
    verified = await verified_publication(
        engine, setup, accepted, step, observed.operation_version
    )
    return Publication(accepted, step.invocation_id, step, verified)


async def write_plan(
    setup: Context,
) -> tuple[CodingResultAcceptance, CodingPublicationStep]:
    accepted = await setup.store.accept(setup.prepared, setup.authorization)
    invocation_id = uuid4()
    started = await setup.store.begin_publication(
        setup.prepared, setup.authorization, invocation_id=invocation_id
    )
    step = CodingPublicationStep(
        result_id=accepted.receipt.result_id,
        invocation_id=invocation_id,
        expected_operation_version=started.operation_version,
        step_id=uuid4(),
        step_kind="create_pr",
        input_digest="b" * 64,
    )
    permit = await setup.store.plan_write(
        setup.prepared, setup.authorization, step=step
    )
    assert permit.created
    return accepted, step


async def verified_publication(
    engine: AsyncEngine,
    setup: Context,
    accepted: CodingResultAcceptance,
    step: CodingPublicationStep,
    version: int,
) -> VerifiedCodingPublication:
    async with engine.connect() as connection:
        now = (await connection.execute(text("SELECT clock_timestamp()"))).scalar_one()
    assert isinstance(now, datetime)
    definition = setup.prepared.definition
    return VerifiedCodingPublication(
        result_id=accepted.receipt.result_id,
        invocation_id=step.invocation_id,
        expected_operation_version=version,
        reference=PullRequestReference(
            repository_id=definition.repository_id,
            number=42,
            source_branch=definition.authorized_branch,
            target_branch=definition.target_branch,
            head_revision=accepted.receipt.candidate_revision,
        ),
        provider_repository_id=definition.provider_repository_id,
        observed_at=now,
    )


async def operation(engine: AsyncEngine) -> tuple[str, int, int, int]:
    async with engine.connect() as connection:
        return tuple(
            (
                await connection.execute(
                    text(
                        "SELECT state, version, event_count, (SELECT count(*) FROM delivery_operation_events) FROM delivery_operations"
                    )
                )
            ).one()
        )


def test_publication_invocation_and_write_plan_are_create_once_replay_metadata(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine)
            accepted = await setup.store.accept(setup.prepared, setup.authorization)
            invocation_id = uuid4()
            first = await setup.store.begin_publication(
                setup.prepared, setup.authorization, invocation_id=invocation_id
            )
            replay = await setup.store.begin_publication(
                setup.prepared, setup.authorization, invocation_id=invocation_id
            )
            assert first == replay and first.operation_version == 1
            with pytest.raises(ClaimReplayConflict):
                await setup.store.begin_publication(
                    setup.prepared, setup.authorization, invocation_id=uuid4()
                )
            step = CodingPublicationStep(
                result_id=accepted.receipt.result_id,
                invocation_id=invocation_id,
                expected_operation_version=1,
                step_id=uuid4(),
                step_kind="create_branch",
                input_digest="b" * 64,
            )
            permit = await setup.store.plan_write(
                setup.prepared, setup.authorization, step=step
            )
            recovered = await setup.store.plan_write(
                setup.prepared, setup.authorization, step=step
            )
            assert permit.created and not recovered.created
            assert permit.operation_version == recovered.operation_version == 2
            with pytest.raises(ClaimReplayConflict):
                await setup.store.plan_write(
                    setup.prepared,
                    setup.authorization,
                    step=replace(step, input_digest="d" * 64),
                )
            with pytest.raises(CodingResultRejected):
                await setup.store.plan_write(
                    setup.prepared,
                    setup.authorization,
                    step=replace(step, step_id=uuid4(), expected_operation_version=2),
                )
            assert await operation(engine) == ("publishing", 2, 3, 3)
            state = await projection(engine)
            assert (
                state["release_count"] == 0 and state["reserved_attempt_id"] is not None
            )

    run(scenario())


def test_write_observation_replay_is_exact_and_conflicting_outcome_cannot_replace_it(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine)
            prepared = await publication(engine, setup)
            before = await operation(engine)
            repeated = await setup.store.observe_write(
                setup.prepared,
                setup.authorization,
                step=prepared.step,
                observation_digest="c" * 64,
            )
            assert repeated.operation_version == 3
            with pytest.raises(ClaimReplayConflict):
                await setup.store.observe_write(
                    setup.prepared,
                    setup.authorization,
                    step=prepared.step,
                    observation_digest="d" * 64,
                )
            with pytest.raises(CodingResultRejected):
                await setup.store.observe_write(
                    setup.prepared,
                    setup.authorization,
                    step=replace(prepared.step, step_id=uuid4()),
                    observation_digest="c" * 64,
                )
            assert await operation(engine) == before

    run(scenario())


@pytest.mark.parametrize("repairs", [0, 2])
def test_normal_publication_finalizes_exactly_once_and_releases_only_its_slot(
    database: DatabaseFactory, repairs: int
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine, repairs=repairs)
            prepared = await publication(engine, setup)
            before = await projection(engine)

            terminal = await setup.store.finalize_publication(
                setup.prepared, setup.authorization, publication=prepared.verified
            )

            after = await projection(engine)
            assert terminal.receipt == prepared.accepted.receipt
            assert terminal.reference == prepared.verified.reference
            assert (
                terminal.workflow_version == 5 + 2 * repairs
                and terminal.release_ordinal == 1
            )
            assert (after["status"], after["attempt_status"]) == (
                "awaiting_review",
                "succeeded",
            )
            assert (
                after["active_attempt_id"] is None
                and after["reserved_attempt_id"] is None
            )
            assert after["last_attempt_id"] == before["last_attempt_id"]
            assert after["ended_at"] == after["revoked_at"] == terminal.finalized_at
            assert (
                after["claim_receipts"],
                after["release_count"],
                after["slot_version"],
            ) == (1, 1, 2)
            assert after["workflow_version"] == terminal.workflow_version
            for field in (
                "issued_at",
                "acknowledged_at",
                "authority_updated_at",
                "lease_revision",
                "heartbeat_receipts",
            ):
                assert after[field] == before[field]
            assert await operation(engine) == ("finalized", 4, 5, 5)
            replay = await setup.store.finalize_publication(
                setup.prepared, setup.authorization, publication=prepared.verified
            )
            assert replay == terminal
            recovered = await setup.store.recover_result(
                setup.prepared, setup.authorization
            )
            assert recovered is not None and recovered.terminal == terminal
            assert (
                recovered.state is CodingPublicationState.FINALIZED
                and recovered.operation_version == 4
            )
            assert await projection(engine) == after

    run(scenario())


@pytest.mark.parametrize("late_observation", [False, True])
def test_uncertainty_stays_sticky_and_never_grants_another_write_or_slot_release(
    database: DatabaseFactory, late_observation: bool
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine)
            accepted, step = await write_plan(setup)
            uncertain = await setup.store.mark_uncertain(
                setup.prepared, setup.authorization, step=step
            )
            repeated = await setup.store.mark_uncertain(
                setup.prepared, setup.authorization, step=step
            )
            assert repeated == uncertain
            assert (
                uncertain.state is CodingPublicationState.UNCERTAIN
                and uncertain.operation_version == 3
            )
            if late_observation:
                uncertain = await setup.store.observe_write(
                    setup.prepared,
                    setup.authorization,
                    step=step,
                    observation_digest="c" * 64,
                )
                assert (
                    uncertain.state is CodingPublicationState.UNCERTAIN
                    and uncertain.operation_version == 4
                )
            recovered_plan = await setup.store.plan_write(
                setup.prepared, setup.authorization, step=step
            )
            assert not recovered_plan.created
            with pytest.raises(CodingResultRejected):
                await setup.store.plan_write(
                    setup.prepared,
                    setup.authorization,
                    step=replace(
                        step,
                        step_id=uuid4(),
                        expected_operation_version=uncertain.operation_version,
                    ),
                )
            observed = await verified_publication(
                engine, setup, accepted, step, uncertain.operation_version
            )
            with pytest.raises(CodingResultRejected):
                await setup.store.finalize_publication(
                    setup.prepared, setup.authorization, publication=observed
                )
            state = await projection(engine)
            assert state["status"] == state["attempt_status"] == "publishing_pr"
            assert (
                state["release_count"] == 0 and state["reserved_attempt_id"] is not None
            )
            assert state["ended_at"] is state["revoked_at"] is None
            assert await operation(engine) == (
                "uncertain",
                uncertain.operation_version,
                uncertain.operation_version + 1,
                uncertain.operation_version + 1,
            )

    run(scenario())


def test_late_write_observation_is_bookkeeping_after_expiry_without_new_write_rights(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine, short_lease=True)
            accepted, step = await write_plan(setup)
            await asyncio.sleep(
                max(
                    0,
                    (
                        setup.authorization.credential_expires_at - datetime.now(UTC)
                    ).total_seconds(),
                )
                + 0.03
            )

            observed = await setup.store.observe_write(
                setup.prepared,
                setup.authorization,
                step=step,
                observation_digest="c" * 64,
            )

            assert (
                observed.state is CodingPublicationState.PUBLISHING
                and observed.operation_version == 3
            )
            with pytest.raises(WorkerAuthorityExpired):
                await setup.store.plan_write(
                    setup.prepared,
                    setup.authorization,
                    step=replace(step, step_id=uuid4(), expected_operation_version=3),
                )
            publication_ = await verified_publication(
                engine, setup, accepted, step, observed.operation_version
            )
            renewed_credential = replace(
                setup.authorization,
                credential_expires_at=datetime.now(UTC) + timedelta(hours=1),
            )
            with pytest.raises(LeaseExpired):
                await setup.store.finalize_publication(
                    setup.prepared, renewed_credential, publication=publication_
                )
            state = await projection(engine)
            assert state["release_count"] == 0 and state["ended_at"] is None

    run(scenario())


def test_finalization_checks_exact_association_version_and_fresh_broker_time(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine)
            prepared = await publication(engine, setup)
            original = prepared.verified
            variants = (
                replace(original, result_id=uuid4()),
                replace(original, invocation_id=uuid4()),
                replace(
                    original, provider_repository_id=original.provider_repository_id + 1
                ),
                replace(
                    original,
                    reference=replace(original.reference, repository_id=uuid4()),
                ),
                replace(
                    original,
                    reference=replace(original.reference, source_branch="other/source"),
                ),
                replace(
                    original,
                    reference=replace(original.reference, target_branch="other/target"),
                ),
                replace(
                    original,
                    reference=replace(original.reference, head_revision="f" * 40),
                ),
                replace(original, expected_operation_version=1),
                replace(
                    original, observed_at=original.observed_at - timedelta(seconds=31)
                ),
                replace(
                    original, observed_at=original.observed_at + timedelta(hours=1)
                ),
            )
            before = await projection(engine)
            for invalid in variants:
                with pytest.raises((ClaimReplayConflict, CodingResultRejected)):
                    await setup.store.finalize_publication(
                        setup.prepared, setup.authorization, publication=invalid
                    )
            assert await projection(engine) == before
            assert await operation(engine) == ("publishing", 3, 4, 4)

    run(scenario())


def test_old_terminal_replay_cannot_release_a_newly_reserved_attempt(
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
            authorization = authority(next_job)
            pending = claim_request()
            next_claim = await setup.assigned.claims.claim(
                pending.identify(authorization),
                authorization,
                observe(next_job, authorization),
            )
            assert next_claim.attempt is not None
            async with engine.connect() as connection:
                before = (
                    await connection.execute(
                        text(
                            "SELECT reserved_attempt_id, receipt_count, release_count, version FROM dispatch_slots WHERE name='coding'"
                        )
                    )
                ).one()
            assert before.reserved_attempt_id == next_claim.attempt.id

            assert (
                await setup.store.finalize_publication(
                    setup.prepared, setup.authorization, publication=prepared.verified
                )
                == terminal
            )
            old_result = await setup.store.accept(setup.prepared, setup.authorization)
            assert old_result.recovered and old_result.receipt == terminal.receipt
            recovered = await setup.store.recover_result(
                setup.prepared, setup.authorization
            )
            assert recovered is not None and recovered.terminal == terminal
            with pytest.raises(ClaimReplayConflict):
                await setup.store.finalize_publication(
                    setup.prepared,
                    setup.authorization,
                    publication=replace(
                        prepared.verified,
                        reference=replace(prepared.verified.reference, number=43),
                    ),
                )
            async with engine.connect() as connection:
                after = (
                    await connection.execute(
                        text(
                            "SELECT reserved_attempt_id, receipt_count, release_count, version FROM dispatch_slots WHERE name='coding'"
                        )
                    )
                ).one()
            assert after == before and (
                after.receipt_count,
                after.release_count,
                after.version,
            ) == (2, 1, 3)

    run(scenario())


def test_heartbeat_and_finalization_serialize_without_losing_renewal_or_releasing_twice(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine)
            prepared = await publication(engine, setup)
            async with engine.connect() as connection:
                row = (
                    await connection.execute(
                        text(
                            "SELECT next_heartbeat_at, clock_timestamp() AS now FROM attempt_lease_authorities"
                        )
                    )
                ).one()
            await asyncio.sleep(
                max(0, (row.next_heartbeat_at - row.now).total_seconds()) + 0.03
            )
            heartbeat = heartbeat_request(setup.assigned)

            results = await asyncio.gather(
                setup.store.finalize_publication(
                    setup.prepared, setup.authorization, publication=prepared.verified
                ),
                setup.store.heartbeat(
                    heartbeat.identify(setup.authorization), setup.authorization
                ),
                return_exceptions=True,
            )

            assert not isinstance(results[0], BaseException)
            assert isinstance(results[1], (HeartbeatResult, HeartbeatRejected))
            after = await projection(engine)
            assert (
                after["status"],
                after["attempt_status"],
                after["release_count"],
                after["slot_version"],
            ) == ("awaiting_review", "succeeded", 1, 2)
            assert after["ended_at"] == after["revoked_at"]
            if isinstance(results[1], HeartbeatResult):
                assert after["heartbeat_receipts"] == 1
                assert after["lease_revision"] == results[1].authority.lease_revision
                assert after["authority_updated_at"] == results[1].authority.updated_at
            else:
                assert after["heartbeat_receipts"] == 0 and after["lease_revision"] == 1
            recovered = await setup.store.recover_result(
                setup.prepared, setup.authorization
            )
            assert recovered is not None and recovered.terminal == results[0]

    run(scenario())


def test_candidate_revalidation_finishes_off_loop_before_transaction_locks(
    database: DatabaseFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine)
            loop_thread = threading.get_ident()
            transactions = 0
            observations: list[tuple[int, int]] = []
            original = CandidateSnapshot.validate

            def begin(connection: object) -> None:
                nonlocal transactions
                transactions += 1

            def end(connection: object) -> None:
                nonlocal transactions
                transactions -= 1

            def validate(snapshot: CandidateSnapshot) -> None:
                observations.append((threading.get_ident(), transactions))
                original(snapshot)

            event.listen(engine.sync_engine, "begin", begin)
            event.listen(engine.sync_engine, "commit", end)
            event.listen(engine.sync_engine, "rollback", end)
            monkeypatch.setattr(CandidateSnapshot, "validate", validate)
            try:
                await setup.store.accept(setup.prepared, setup.authorization)
            finally:
                event.remove(engine.sync_engine, "begin", begin)
                event.remove(engine.sync_engine, "commit", end)
                event.remove(engine.sync_engine, "rollback", end)
            assert observations
            assert all(
                identity != loop_thread and active == 0
                for identity, active in observations
            )
            assert transactions == 0

    run(scenario())


def test_credential_expiry_while_waiting_for_slot_lock_cannot_accept_result(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine)
            before = await projection(engine)
            authorization = replace(
                setup.authorization,
                credential_expires_at=datetime.now(UTC) + timedelta(milliseconds=250),
            )
            waiting = asyncio.Event()

            def observe_lock(
                connection: object,
                cursor: object,
                statement: str,
                parameters: object,
                context: object,
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
                    setup.store.accept(setup.prepared, authorization)
                )
                try:
                    await asyncio.wait_for(waiting.wait(), 1)
                    await asyncio.sleep(
                        max(
                            0,
                            (
                                authorization.credential_expires_at - datetime.now(UTC)
                            ).total_seconds(),
                        )
                        + 0.03
                    )
                finally:
                    event.remove(
                        engine.sync_engine, "before_cursor_execute", observe_lock
                    )
            with pytest.raises(WorkerAuthorityExpired):
                await pending
            assert await projection(engine) == before

    run(scenario())
