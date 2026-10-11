import asyncio
import sys
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncEngine

from devrelay.application.assignment_delivery import ReadAssignedJob
from devrelay.application.contracts.assignment_delivery import AssignmentUnavailable
from devrelay.application.contracts.assignments import ClaimScopeRejected
from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    LeaseProof,
    WorkerClaimRequest,
)
from devrelay.application.contracts.eligibility import (
    PreparedJobClaim,
    WorkerAuthorityExpired,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.bootstrap.persistence import claim_store
from tests.integration.job_fixtures import save_job


@pytest.mark.postgres
@pytest.mark.parametrize("check_count", [1, 32])
def test_delivery_uses_bounded_recovery_queries_without_state_mutation(
    database: Callable[[], AbstractAsyncContextManager[AsyncEngine]],
    check_count: int,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            stored = await save_job(engine, check_count=check_count)
            authorization = WorkerAuthorization(
                worker_id=uuid4(),
                allowed_repository_ids=frozenset({stored.job.repository_id}),
                credential_expires_at=datetime.now(UTC) + timedelta(hours=1),
            )
            request = WorkerClaimRequest(
                claim_request_id=uuid4(), proof=LeaseProof.generate()
            )
            store = claim_store(engine, (stored.intent.policy,))
            result = await store.claim(
                request.identify(authorization),
                authorization,
                PreparedJobClaim(
                    worker_id=authorization.worker_id,
                    stored=stored,
                    observed_at=datetime.now(UTC),
                ),
            )
            assert result.attempt is not None
            reader = ReadAssignedJob(store, lambda proof: None)
            statements: list[str] = []

            def count(*args: object) -> None:
                statements.append(str(args[2]))

            event.listen(engine.sync_engine, "before_cursor_execute", count)
            try:
                delivery = await reader.execute(
                    result.attempt.id, request, authorization
                )
            finally:
                event.remove(engine.sync_engine, "before_cursor_execute", count)
            assert len(statements) == 10
            assert not any(
                statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
                for statement in statements
            )
            assert len(delivery.definition.checks) == check_count
            assert delivery.definition.snapshot == stored.intent.stored.snapshot
            assert delivery.definition.base_revision == stored.job.base_revision
            assert (
                delivery.outcome.assignment is not None
                and delivery.outcome.assignment.attempt_id == result.attempt.id
            )
            async with engine.connect() as connection:
                counters = (
                    await connection.execute(
                        text(
                            "SELECT (SELECT count(*) FROM worker_claim_receipts), (SELECT count(*) FROM execution_attempts), (SELECT receipt_count FROM dispatch_slots WHERE name = 'coding'), (SELECT workflow_version FROM job_workflows LIMIT 1)"
                        )
                    )
                ).one()
            assert tuple(counters) == (1, 1, 1, 1)

    asyncio.run(
        scenario(),
        loop_factory=asyncio.SelectorEventLoop if sys.platform == "win32" else None,
    )


@pytest.mark.postgres
@pytest.mark.parametrize(
    "change,error_type",
    [
        ("request", AssignmentUnavailable),
        ("attempt", AssignmentUnavailable),
        ("proof", ClaimReplayConflict),
        ("worker", AssignmentUnavailable),
        ("scope", ClaimScopeRejected),
        ("expiry", WorkerAuthorityExpired),
    ],
)
def test_assignment_disclosure_requires_original_identity_proof_and_current_scope(
    database: Callable[[], AbstractAsyncContextManager[AsyncEngine]],
    change: str,
    error_type: type[Exception],
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            stored = await save_job(engine)
            authorization = WorkerAuthorization(
                worker_id=uuid4(),
                allowed_repository_ids=frozenset({stored.job.repository_id}),
                credential_expires_at=datetime.now(UTC) + timedelta(hours=1),
            )
            original = WorkerClaimRequest(
                claim_request_id=uuid4(), proof=LeaseProof.generate()
            )
            store = claim_store(engine, (stored.intent.policy,))
            result = await store.claim(
                original.identify(authorization),
                authorization,
                PreparedJobClaim(
                    worker_id=authorization.worker_id,
                    stored=stored,
                    observed_at=datetime.now(UTC),
                ),
            )
            assert result.attempt is not None
            attempt_id = result.attempt.id
            presented = original
            if change == "request":
                presented = WorkerClaimRequest(
                    claim_request_id=uuid4(), proof=original.proof
                )
            elif change == "attempt":
                attempt_id = uuid4()
            elif change == "proof":
                presented = WorkerClaimRequest(
                    claim_request_id=original.claim_request_id,
                    proof=LeaseProof.generate(),
                )
            elif change == "worker":
                authorization = replace(authorization, worker_id=uuid4())
            elif change == "scope":
                authorization = replace(
                    authorization, allowed_repository_ids=frozenset({uuid4()})
                )
            else:
                authorization = replace(
                    authorization,
                    credential_expires_at=datetime.now(UTC) - timedelta(seconds=1),
                )
            reader = ReadAssignedJob(store, lambda proof: None)

            with pytest.raises(error_type):
                await reader.execute(attempt_id, presented, authorization)

            async with engine.connect() as connection:
                count = (
                    await connection.execute(
                        text("SELECT count(*) FROM worker_claim_receipts")
                    )
                ).scalar_one()
            assert count == 1

    asyncio.run(
        scenario(),
        loop_factory=asyncio.SelectorEventLoop if sys.platform == "win32" else None,
    )


@pytest.mark.postgres
def test_expired_original_assignment_remains_readable_without_active_authority(
    database: Callable[[], AbstractAsyncContextManager[AsyncEngine]],
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            stored = await save_job(engine)
            authorization = WorkerAuthorization(
                worker_id=uuid4(),
                allowed_repository_ids=frozenset({stored.job.repository_id}),
                credential_expires_at=datetime.now(UTC) + timedelta(seconds=1),
            )
            request = WorkerClaimRequest(
                claim_request_id=uuid4(), proof=LeaseProof.generate()
            )
            store = claim_store(engine, (stored.intent.policy,))
            result = await store.claim(
                request.identify(authorization),
                authorization,
                PreparedJobClaim(
                    worker_id=authorization.worker_id,
                    stored=stored,
                    observed_at=datetime.now(UTC),
                ),
            )
            assert result.attempt is not None
            await asyncio.sleep(1.05)
            renewed_authentication = replace(
                authorization,
                credential_expires_at=datetime.now(UTC) + timedelta(hours=1),
            )
            reader = ReadAssignedJob(store, lambda proof: None)

            delivery = await reader.execute(
                result.attempt.id, request, renewed_authentication
            )

            assert not delivery.lease_authority_active
            assert delivery.outcome.assignment is not None
            assert (
                delivery.outcome.assignment.expires_at
                == result.attempt.lease.expires_at
            )
            async with engine.connect() as connection:
                reserved = (
                    await connection.execute(
                        text(
                            "SELECT reserved_attempt_id FROM dispatch_slots WHERE name = 'coding'"
                        )
                    )
                ).scalar_one()
            assert reserved == result.attempt.id

    asyncio.run(
        scenario(),
        loop_factory=asyncio.SelectorEventLoop if sys.platform == "win32" else None,
    )
