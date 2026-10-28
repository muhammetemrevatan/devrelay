"""Database invariants using synthetic, non-provider result rows.

These fixtures exercise SQL ownership/history constraints. Full wire payload and
check evidence validation belongs to the consumed result store/application tests.
"""

import hashlib
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from devrelay.infrastructure.persistence.result_models import (
    AttemptResultRecord,
    DeliveryOperationEventRecord,
    DeliveryOperationRecord,
)
from tests.integration.test_assignment_acknowledgement import Assigned, DatabaseFactory
from tests.integration.test_atomic_claim_persistence import run
from tests.integration.test_heartbeat_persistence import acknowledged

pytestmark = pytest.mark.postgres


@dataclass(frozen=True)
class Accepted:
    assignment: Assigned
    result_id: UUID
    attempt_id: UUID
    recorded_at: datetime
    repairs: int


async def accept_rows(
    engine: AsyncEngine,
    *,
    repairs: int = 0,
    changes: dict[str, Any] | None = None,
    omit_version: int | None = None,
) -> Accepted:
    setup = await acknowledged(engine)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions.begin() as session:
        anchors = (
            await session.execute(
                text("""
          SELECT a.id, j.id, j.repository_id, j.snapshot_id, c.fingerprint,
                 k.definition_digest, clock_timestamp()
          FROM execution_attempts a JOIN jobs j ON j.id = a.job_id
          JOIN worker_claim_receipts c ON c.attempt_id = a.id
          JOIN worker_acknowledgement_receipts k ON k.attempt_id = a.id
        """)
            )
        ).one()
        attempt, job, repository, snapshot, claim_hash, definition_hash, now = anchors
        result_id = uuid4()
        payload = b'{"fixture":"database constraints only"}'
        values: dict[str, Any] = dict(
            id=result_id,
            request_id=uuid4(),
            worker_id=setup.authorization.worker_id,
            attempt_id=attempt,
            job_id=job,
            claim_request_id=setup.request.claim.claim_request_id,
            repository_id=repository,
            snapshot_id=snapshot,
            execution_id=uuid4(),
            generation=1,
            definition_digest=definition_hash,
            claim_fingerprint=claim_hash,
            fingerprint="f" * 64,
            protocol_version=1,
            fingerprint_version=1,
            expected_workflow_version=2,
            publishing_workflow_version=4 + 2 * repairs,
            candidate_revision="b" * 40,
            repair_cycles=repairs,
            recorded_at=now,
            payload_bytes=payload,
            payload_sha256=hashlib.sha256(payload).hexdigest(),
        )
        values.update(changes or {})
        session.add(AttemptResultRecord(**values))
        session.add(
            DeliveryOperationRecord(
                result_id=result_id,
                version=0,
                event_count=1,
                state="accepted",
            )
        )
        session.add(
            DeliveryOperationEventRecord(
                result_id=result_id,
                sequence=0,
                kind="accepted",
                occurred_at=now,
            )
        )
        await session.flush()
        previous = "running"
        version = 2
        for cycle in range(repairs + 1):
            phases = (
                ("testing", "tested", "b" * 40),
                (
                    "publishing_pr" if cycle == repairs else "running",
                    "checks_accepted" if cycle == repairs else "repair_requested",
                    "b" * 40 if cycle == repairs else None,
                ),
            )
            for status, event_type, candidate in phases:
                version += 1
                projected_cycle = cycle + int(status == "running")
                if version != omit_version:
                    await session.execute(
                        text("""
                      INSERT INTO job_workflow_events
                        (job_id, workflow_version, status, event_type, occurred_at,
                         attempt_id, previous_version, previous_status, result_id, cycle, candidate_revision)
                      VALUES (:job, :version, :status, :kind, :now, :attempt,
                              :previous_version, :previous, :result, :cycle, :candidate)
                    """),
                        dict(
                            job=job,
                            version=version,
                            status=status,
                            kind=event_type,
                            now=now,
                            attempt=attempt,
                            previous_version=version - 1,
                            previous=previous,
                            result=result_id,
                            cycle=projected_cycle,
                            candidate=candidate,
                        ),
                    )
                await session.execute(
                    text("""
                  UPDATE job_workflows SET status=:status, workflow_version=:version,
                    updated_at=:now, repair_cycles=:cycle, candidate_revision=:candidate,
                    accepted_result_id=:result WHERE job_id=:job
                """),
                    dict(
                        status=status,
                        version=version,
                        now=now,
                        cycle=projected_cycle,
                        candidate=candidate,
                        result=result_id,
                        job=job,
                    ),
                )
                previous = status
        await session.execute(
            text("""
          UPDATE execution_attempts SET status='publishing_pr', updated_at=:now WHERE id=:attempt
        """),
            dict(now=now, attempt=attempt),
        )
    return Accepted(setup, result_id, attempt, now, repairs)


async def append_event(
    engine: AsyncEngine,
    accepted: Accepted,
    *,
    kind: str,
    invocation: UUID,
    step: UUID | None = None,
) -> None:
    async with engine.begin() as connection:
        row = (
            await connection.execute(
                text("""
          SELECT version, state, clock_timestamp() FROM delivery_operations WHERE result_id=:result
        """),
                dict(result=accepted.result_id),
            )
        ).one()
        version, state, now = row
        target_state = (
            "uncertain" if kind == "uncertain" or state == "uncertain" else "publishing"
        )
        await connection.execute(
            text("""
          INSERT INTO delivery_operation_events
            (result_id, sequence, kind, invocation_id, step_id, step_kind, input_digest, observation_digest, occurred_at)
          VALUES (:result, :sequence, :kind, :invocation, :step, :step_kind, :input, :observed, :now)
        """),
            dict(
                result=accepted.result_id,
                sequence=version + 1,
                kind=kind,
                invocation=invocation,
                step=step,
                step_kind="fixture:create_pr" if step else None,
                input="c" * 64 if step else None,
                observed="d" * 64 if kind == "write_observed" else None,
                now=now,
            ),
        )

        await connection.execute(
            text("""
          UPDATE delivery_operations SET version=:version, event_count=:count,
            state=:state, invocation_id=:invocation WHERE result_id=:result
        """),
            dict(
                version=version + 1,
                count=version + 2,
                state=target_state,
                invocation=invocation,
                result=accepted.result_id,
            ),
        )


async def finalize_rows(
    engine: AsyncEngine,
    accepted: Accepted,
    *,
    null_field: str | None = None,
) -> None:
    invocation, step = uuid4(), uuid4()
    await append_event(
        engine, accepted, kind="publishing_started", invocation=invocation
    )
    await append_event(
        engine, accepted, kind="write_planned", invocation=invocation, step=step
    )
    await append_event(
        engine, accepted, kind="write_observed", invocation=invocation, step=step
    )
    async with engine.begin() as connection:
        now = (await connection.execute(text("SELECT clock_timestamp()"))).scalar_one()
        params = dict(
            result=accepted.result_id,
            attempt=accepted.attempt_id,
            now=now,
            invocation=invocation,
            final_version=5 + 2 * accepted.repairs,
            null_field=null_field,
        )
        await connection.execute(
            text("""
          UPDATE delivery_operations o SET version=4,event_count=5,state='finalized',
            pr_number=CASE WHEN :null_field='pr_number' THEN NULL ELSE 7 END, pr_repository_id=j.repository_id,
            pr_provider_repository_id=j.provider_repository_id,
            pr_branch=j.authorized_branch,pr_target_branch=j.target_branch,
            pr_head_revision=CASE WHEN :null_field='pr_head_revision' THEN NULL ELSE r.candidate_revision END,
            completed_at=CASE WHEN :null_field='completed_at' THEN NULL ELSE :now END,
            final_workflow_version=:final_version,
            release_ordinal=CASE WHEN :null_field='release_ordinal' THEN NULL ELSE s.release_count+1 END
          FROM attempt_results r JOIN jobs j ON j.id=r.job_id CROSS JOIN dispatch_slots s
          WHERE o.result_id=r.id AND r.id=:result
        """),
            params,
        )
        await connection.execute(
            text("""
          INSERT INTO delivery_operation_events (result_id,sequence,kind,invocation_id,occurred_at)
          VALUES (:result,4,'finalized',:invocation,:now)
        """),
            params,
        )
        await connection.execute(
            text("""
          INSERT INTO job_workflow_events
            (job_id,workflow_version,status,event_type,occurred_at,attempt_id,
             previous_version,previous_status,result_id,cycle,candidate_revision)
          SELECT job_id,:final_version,'awaiting_review','published',:now,attempt_id,
            publishing_workflow_version,'publishing_pr',id,repair_cycles,candidate_revision
          FROM attempt_results WHERE id=:result
        """),
            params,
        )
        await connection.execute(
            text("""
          UPDATE job_workflows SET status='awaiting_review',workflow_version=:final_version,
            active_attempt_id=NULL,updated_at=:now WHERE accepted_result_id=:result
        """),
            params,
        )
        await connection.execute(
            text("""
          UPDATE execution_attempts SET status='succeeded',updated_at=:now,
            ended_at=:now,revoked_at=:now WHERE id=:attempt
        """),
            params,
        )
        await connection.execute(
            text("""
          UPDATE attempt_lease_authorities SET revoked_at=:now WHERE attempt_id=:attempt
        """),
            params,
        )
        await connection.execute(
            text("""
          UPDATE dispatch_slots SET reserved_attempt_id=NULL,version=version+1,
            release_count=release_count+1,last_released_result_id=:result
        """),
            params,
        )


@pytest.mark.parametrize("repairs", [0, 2])
def test_successful_fold_has_contiguous_history_and_immutable_original_anchors(
    database: DatabaseFactory,
    repairs: int,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            accepted = await accept_rows(engine, repairs=repairs)
            async with engine.connect() as connection:
                versions = (
                    (
                        await connection.execute(
                            text(
                                "SELECT workflow_version FROM job_workflow_events ORDER BY workflow_version"
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
                row = (
                    await connection.execute(
                        text("""
                  SELECT w.status, w.workflow_version, a.status, a.acknowledged_at=k.acknowledged_at,
                         a.expires_at>k.acknowledged_at, l.revoked_at, s.reserved_attempt_id=a.id,
                         s.release_count, o.state
                  FROM job_workflows w JOIN execution_attempts a ON a.id=w.last_attempt_id
                  JOIN worker_acknowledgement_receipts k ON k.attempt_id=a.id
                  JOIN attempt_lease_authorities l ON l.attempt_id=a.id
                  CROSS JOIN dispatch_slots s CROSS JOIN delivery_operations o
                """)
                    )
                ).one()
            assert versions == list(range(5 + 2 * repairs))
            assert tuple(row) == (
                "publishing_pr",
                4 + 2 * repairs,
                "publishing_pr",
                True,
                True,
                None,
                True,
                0,
                "accepted",
            )
            assert accepted.repairs == repairs

    run(scenario())


@pytest.mark.parametrize("missing", [3, 4])
def test_missing_middle_or_final_event_rolls_back_complete_result_transaction(
    database: DatabaseFactory,
    missing: int,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            with pytest.raises(SQLAlchemyError):
                await accept_rows(engine, omit_version=missing)
            async with engine.connect() as connection:
                assert (
                    await connection.execute(
                        text("SELECT count(*) FROM attempt_results")
                    )
                ).scalar_one() == 0
                assert (
                    await connection.execute(
                        text("SELECT count(*) FROM delivery_operations")
                    )
                ).scalar_one() == 0
                assert (
                    await connection.execute(
                        text("SELECT workflow_version FROM job_workflows")
                    )
                ).scalar_one() == 2

    run(scenario())


@pytest.mark.parametrize(
    "changes",
    [
        {"payload_sha256": "0" * 64},
        {"protocol_version": 2},
        {"generation": 2},
        {"candidate_revision": "b" * 64},
        {"repair_cycles": 1},
        {"claim_fingerprint": "0" * 64},
        {"definition_digest": "0" * 64},
        {"snapshot_id": UUID(int=4)},
    ],
)
def test_result_integrity_and_original_assignment_mismatches_fail_closed(
    database: DatabaseFactory,
    changes: dict[str, Any],
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            with pytest.raises(SQLAlchemyError):
                await accept_rows(engine, changes=changes)

    run(scenario())


@pytest.mark.parametrize("target", ["attempt_results", "delivery_operation_events"])
@pytest.mark.parametrize("operation", ["update", "delete"])
def test_accepted_result_and_append_only_events_cannot_be_mutated(
    database: DatabaseFactory,
    target: str,
    operation: str,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            await accept_rows(engine)
            statement = {
                (
                    "attempt_results",
                    "update",
                ): "UPDATE attempt_results SET fingerprint=fingerprint",
                ("attempt_results", "delete"): "DELETE FROM attempt_results",
                (
                    "delivery_operation_events",
                    "update",
                ): "UPDATE delivery_operation_events SET kind=kind",
                (
                    "delivery_operation_events",
                    "delete",
                ): "DELETE FROM delivery_operation_events",
            }[(target, operation)]
            with pytest.raises(SQLAlchemyError):
                async with engine.begin() as connection:
                    await connection.execute(text(statement))

    run(scenario())


@pytest.mark.parametrize("mutation", ["release", "revoke", "complete", "workflow"])
def test_partial_terminal_mutations_cannot_release_or_end_running_publication(
    database: DatabaseFactory,
    mutation: str,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            accepted = await accept_rows(engine)
            statement = {
                "release": "UPDATE dispatch_slots SET reserved_attempt_id=NULL, release_count=release_count+1, version=version+1, last_released_result_id=:result",
                "revoke": "UPDATE attempt_lease_authorities SET revoked_at=clock_timestamp()",
                "complete": "UPDATE execution_attempts SET status='succeeded', updated_at=clock_timestamp(), revoked_at=clock_timestamp(), ended_at=clock_timestamp()",
                "workflow": "UPDATE job_workflows SET status='awaiting_review', workflow_version=workflow_version+1, active_attempt_id=NULL, updated_at=clock_timestamp()",
            }[mutation]
            with pytest.raises(SQLAlchemyError):
                async with engine.begin() as connection:
                    await connection.execute(
                        text(statement), dict(result=accepted.result_id)
                    )
            async with engine.connect() as connection:
                row = (
                    await connection.execute(
                        text(
                            "SELECT reserved_attempt_id,release_count FROM dispatch_slots"
                        )
                    )
                ).one()
                assert tuple(row) == (accepted.attempt_id, 0)

    run(scenario())


def test_uncertain_publication_retains_capacity_and_disallows_new_write_or_invocation(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            accepted = await accept_rows(engine)
            invocation, step = uuid4(), uuid4()
            await append_event(
                engine, accepted, kind="publishing_started", invocation=invocation
            )
            await append_event(
                engine, accepted, kind="write_planned", invocation=invocation, step=step
            )
            await append_event(
                engine, accepted, kind="uncertain", invocation=invocation, step=step
            )
            await append_event(
                engine,
                accepted,
                kind="write_observed",
                invocation=invocation,
                step=step,
            )
            with pytest.raises(SQLAlchemyError):
                await append_event(
                    engine,
                    accepted,
                    kind="write_planned",
                    invocation=invocation,
                    step=uuid4(),
                )
            with pytest.raises(SQLAlchemyError):
                async with engine.begin() as connection:
                    await connection.execute(
                        text(
                            "UPDATE delivery_operations SET invocation_id=:invocation,version=version+1,event_count=event_count+1"
                        ),
                        dict(invocation=uuid4()),
                    )
            async with engine.connect() as connection:
                row = (
                    await connection.execute(
                        text("""
                  SELECT o.state,o.version,s.reserved_attempt_id,s.release_count
                  FROM delivery_operations o CROSS JOIN dispatch_slots s
                """)
                    )
                ).one()
                assert tuple(row) == ("uncertain", 4, accepted.attempt_id, 0)

    run(scenario())


def test_downgrade_refuses_accepted_result_and_retains_all_original_data(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            accepted = await accept_rows(engine)

            def downgrade(connection: Connection) -> None:
                config = Config("alembic.ini")
                config.attributes["connection"] = connection
                command.downgrade(config, "0008_heartbeat_authorities")

            with pytest.raises(
                SQLAlchemyError,
                match="Cannot discard accepted coding/publication history",
            ):
                async with engine.begin() as connection:
                    await connection.run_sync(downgrade)
            async with engine.connect() as connection:
                assert (
                    await connection.execute(text("SELECT id FROM attempt_results"))
                ).scalar_one() == accepted.result_id
                assert (
                    await connection.execute(
                        text("SELECT version_num FROM alembic_version")
                    )
                ).scalar_one() == "0009_coding_results"

    run(scenario())


@pytest.mark.parametrize("repairs", [0, 2])
def test_finalization_releases_once_and_retains_historical_lease_and_attempt(
    database: DatabaseFactory,
    repairs: int,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            accepted = await accept_rows(engine, repairs=repairs)
            async with engine.connect() as connection:
                original = tuple(
                    (
                        await connection.execute(
                            text("""
                  SELECT updated_at,current_expires_at,lease_revision,receipt_count,next_heartbeat_at
                  FROM attempt_lease_authorities
                """)
                        )
                    ).one()
                )
            await finalize_rows(engine, accepted)
            async with engine.connect() as connection:
                current = tuple(
                    (
                        await connection.execute(
                            text("""
                  SELECT updated_at,current_expires_at,lease_revision,receipt_count,next_heartbeat_at
                  FROM attempt_lease_authorities
                """)
                        )
                    ).one()
                )
                row = tuple(
                    (
                        await connection.execute(
                            text("""
                  SELECT w.status,w.workflow_version,w.active_attempt_id,w.last_attempt_id,
                    a.status,a.ended_at=a.revoked_at,a.ended_at=l.revoked_at,
                    s.reserved_attempt_id,s.release_count,s.version=s.receipt_count+s.release_count,
                    s.last_released_result_id,o.release_ordinal,o.state
                  FROM job_workflows w JOIN execution_attempts a ON a.id=w.last_attempt_id
                  JOIN attempt_lease_authorities l ON l.attempt_id=a.id
                  CROSS JOIN dispatch_slots s CROSS JOIN delivery_operations o
                """)
                        )
                    ).one()
                )
            assert current == original
            assert row == (
                "awaiting_review",
                5 + 2 * repairs,
                None,
                accepted.attempt_id,
                "succeeded",
                True,
                True,
                None,
                1,
                True,
                accepted.result_id,
                1,
                "finalized",
            )
            with pytest.raises(SQLAlchemyError):
                async with engine.begin() as connection:
                    await connection.execute(
                        text("""
                      UPDATE dispatch_slots SET release_count=release_count+1,version=version+1
                    """)
                    )
            replay = await accepted.assignment.claims.recover(
                accepted.assignment.request.claim.identify(
                    accepted.assignment.authorization
                ),
                accepted.assignment.authorization,
            )
            assert replay is not None and replay.attempt is not None
            assert replay.attempt.id == accepted.attempt_id
            assert not replay.lease_authority_active

    run(scenario())


@pytest.mark.parametrize(
    "null_field", ["pr_number", "pr_head_revision", "completed_at", "release_ordinal"]
)
def test_nullable_terminal_fields_cannot_bypass_database_evidence_constraints(
    database: DatabaseFactory,
    null_field: str,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            accepted = await accept_rows(engine)
            with pytest.raises(SQLAlchemyError):
                await finalize_rows(engine, accepted, null_field=null_field)
            async with engine.connect() as connection:
                assert (
                    await connection.execute(
                        text("SELECT reserved_attempt_id FROM dispatch_slots")
                    )
                ).scalar_one() == accepted.attempt_id
                assert (
                    await connection.execute(
                        text("SELECT state FROM delivery_operations")
                    )
                ).scalar_one() == "publishing"

    run(scenario())


@pytest.mark.parametrize(
    "mistake", ["observed_without_plan", "wrong_observed_step", "overlapping_plan"]
)
def test_external_write_evidence_requires_one_matching_unsettled_plan(
    database: DatabaseFactory,
    mistake: str,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            accepted = await accept_rows(engine)
            invocation = uuid4()
            await append_event(
                engine, accepted, kind="publishing_started", invocation=invocation
            )
            if mistake != "observed_without_plan":
                await append_event(
                    engine,
                    accepted,
                    kind="write_planned",
                    invocation=invocation,
                    step=uuid4(),
                )
            with pytest.raises(SQLAlchemyError):
                await append_event(
                    engine,
                    accepted,
                    kind="write_planned"
                    if mistake == "overlapping_plan"
                    else "write_observed",
                    invocation=invocation,
                    step=uuid4(),
                )

    run(scenario())
