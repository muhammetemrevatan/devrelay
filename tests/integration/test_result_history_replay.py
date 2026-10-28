"""Result publication leaves historical reads intact after real slot reuse."""

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncEngine

from devrelay.application.assignment_delivery import ReadAssignedJob
from devrelay.application.contracts.coding_result_receipts import CodingPublicationStep
from devrelay.application.contracts.heartbeats import HeartbeatRejected
from devrelay.bootstrap.persistence import heartbeat_store, job_submission_reader
from devrelay.domain.jobs.values import AttemptStatus, JobStatus
from tests.integration.job_fixtures import save_job
from tests.integration.test_atomic_claim_persistence import (
    authority,
    observe,
    request,
    run,
)
from tests.integration.test_coding_result_store import context, publication
from tests.integration.test_heartbeat_persistence import ObservedTimeStore
from tests.integration.test_heartbeat_persistence import request as heartbeat_request

pytestmark = pytest.mark.postgres
type DatabaseFactory = Callable[[], AbstractAsyncContextManager[AsyncEngine]]


@pytest.mark.parametrize("repairs", [0, 2])
def test_finalized_result_keeps_original_claim_ack_and_heartbeat_replay_after_slot_reuse(
    database: DatabaseFactory,
    repairs: int,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine, repairs=repairs)
            assigned = setup.assigned
            claims = assigned.claims
            original = await claims.recover(
                assigned.request.claim.identify(setup.authorization),
                setup.authorization,
            )
            assert original is not None and original.attempt is not None
            assert original.stored is not None
            heartbeat = heartbeat_request(assigned)
            heartbeats = heartbeat_store(engine, (assigned.stored.intent.policy,))
            prior = await heartbeats.heartbeat(
                heartbeat.identify(setup.authorization), setup.authorization
            )
            published = await publication(engine, setup)
            reader = job_submission_reader(engine)
            publishing = await reader.find_by_key(assigned.stored.intent.submission_key)
            assert (
                publishing is not None
                and publishing.workflow.status is JobStatus.PUBLISHING_PR
            )
            terminal = await setup.store.finalize_publication(
                setup.prepared, setup.authorization, publication=published.verified
            )

            next_job = await save_job(engine, repository=assigned.stored.intent.policy)
            next_authority, next_claim = authority(next_job), request()
            next_result = await claims.claim(
                next_claim.identify(next_authority),
                next_authority,
                observe(next_job, next_authority),
            )
            assert next_result.attempt is not None

            async def counters() -> tuple[object, ...]:
                async with engine.connect() as connection:
                    return tuple(
                        (
                            await connection.execute(
                                text("""
                        SELECT reserved_attempt_id, receipt_count, release_count, version,
                               heartbeat_receipt_count,
                               (SELECT count(*) FROM job_workflow_events),
                               (SELECT count(*) FROM worker_acknowledgement_receipts),
                               (SELECT count(*) FROM worker_heartbeat_receipts)
                        FROM dispatch_slots WHERE name='coding'
                    """)
                            )
                        ).one()
                    )

            before = await counters()
            recovered = await claims.recover(
                assigned.request.claim.identify(setup.authorization),
                setup.authorization,
            )
            ack = await assigned.service.execute(assigned.request, setup.authorization)
            late_store = ObservedTimeStore(
                engine,
                assigned,
                prior.authority.current_expires_at + timedelta(seconds=1),
            )
            repeated = await late_store.heartbeat(
                heartbeat.identify(setup.authorization), setup.authorization
            )
            assert recovered is not None and recovered.attempt is not None
            assert recovered.stored is not None
            assert recovered.stored.job.status is JobStatus.QUEUED
            assert recovered.stored.workflow.status is JobStatus.AWAITING_REVIEW
            assert recovered.attempt.status is AttemptStatus.SUCCEEDED
            assert recovered.recorded_at == original.recorded_at
            assert recovered.attempt.lease.issued_at == original.attempt.lease.issued_at
            assert (
                recovered.attempt.lease.expires_at == original.attempt.lease.expires_at
            )
            assert recovered.attempt.lease.updated_at == terminal.finalized_at
            assert not recovered.lease_authority_active
            assert ack.recovered and ack.workflow_version == 2
            assert ack.acknowledged_at == original.attempt.acknowledged_at
            assert not ack.lease_authority_active
            assert repeated.stop_required and not repeated.renewed
            assert repeated.authority.revoked_at == terminal.finalized_at
            assert repeated.authority.lease_revision == prior.authority.lease_revision
            assert repeated.authority.updated_at == prior.authority.updated_at
            historical = await ReadAssignedJob(claims, lambda proof: None).execute(
                assigned.request.intent.attempt_id,
                assigned.request.claim,
                setup.authorization,
            )
            assert not historical.lease_authority_active
            assert (
                historical.definition.fingerprint()
                == setup.prepared.definition.fingerprint()
            )
            with pytest.raises(HeartbeatRejected):
                await late_store.heartbeat(
                    heartbeat_request(
                        assigned, revision=prior.authority.lease_revision
                    ).identify(setup.authorization),
                    setup.authorization,
                )
            assert await counters() == before

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

            event.listen(engine.sync_engine, "before_cursor_execute", record)
            try:
                finalized = await reader.find_by_key(
                    assigned.stored.intent.submission_key
                )
            finally:
                event.remove(engine.sync_engine, "before_cursor_execute", record)
            assert (
                finalized is not None
                and finalized.workflow.status is JobStatus.AWAITING_REVIEW
            )
            assert finalized.job.candidate_revision is None
            assert (
                finalized.job.authorized_branch == original.stored.job.authorized_branch
            )
            assert len(statements) == 2
            assert all(
                "attempt_results.payload_bytes" not in statement
                for statement in statements
            )
            assert before[0] == next_result.attempt.id

    run(scenario())


def test_late_write_observation_keeps_uncertain_result_readable(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine)
            published = await publication(engine, setup)
            step = CodingPublicationStep(
                result_id=published.accepted.receipt.result_id,
                invocation_id=published.invocation_id,
                expected_operation_version=3,
                step_id=uuid4(),
                step_kind="create_pr",
                input_digest="e" * 64,
            )
            await setup.store.plan_write(setup.prepared, setup.authorization, step=step)
            await setup.store.mark_uncertain(
                setup.prepared, setup.authorization, step=step
            )
            expired = replace(
                setup.authorization,
                credential_expires_at=datetime.now(UTC) - timedelta(seconds=1),
            )
            result = await setup.store.observe_write(
                setup.prepared, expired, step=step, observation_digest="f" * 64
            )
            assert result.state.value == "uncertain" and result.operation_version == 6
            observed = await job_submission_reader(engine).find_by_key(
                setup.assigned.stored.intent.submission_key
            )
            assert (
                observed is not None
                and observed.workflow.status is JobStatus.PUBLISHING_PR
            )
            assert observed.job.status is JobStatus.QUEUED

    run(scenario())
