import asyncio
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncEngine

from devrelay.application.base_source_delivery import ReadAssignedBase
from devrelay.application.contracts.assignment_delivery import AssignmentUnavailable
from devrelay.application.contracts.assignments import ClaimScopeRejected
from devrelay.application.contracts.base_sources import (
    BaseFileMode,
    BaseSourceFile,
    BaseSourceRequest,
    BaseSourceSnapshot,
    git_blob_sha,
)
from devrelay.application.contracts.claims import ClaimReplayConflict, LeaseProof
from devrelay.application.contracts.eligibility import WorkerAuthorityExpired
from tests.integration.test_assignment_acknowledgement import assigned, counts
from tests.integration.test_atomic_claim_persistence import run

pytestmark = pytest.mark.postgres
type DatabaseFactory = Callable[[], AbstractAsyncContextManager[AsyncEngine]]


class Source:
    def __init__(self) -> None:
        self.calls = 0
        self.wait_seconds = 0.0
        self.before_read: Callable[[], None] = lambda: None

    async def read(self, request: BaseSourceRequest) -> BaseSourceSnapshot:
        self.calls += 1
        self.before_read()
        if self.wait_seconds:
            await asyncio.sleep(self.wait_seconds)
        return BaseSourceSnapshot(
            request=request,
            root_tree_sha="c" * 40,
            files=(
                BaseSourceFile(
                    path="README.md",
                    mode=BaseFileMode.REGULAR,
                    content=b"Fixture base files\n",
                    blob_sha=git_blob_sha(b"Fixture base files\n"),
                ),
            ),
        )


def test_base_export_releases_read_session_before_provider_and_never_mutates(
    database: DatabaseFactory,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await assigned(engine)
            await setup.service.execute(setup.request, setup.authorization)
            original = await setup.claims.recover(
                setup.request.claim.identify(setup.authorization), setup.authorization
            )
            original_counts = await counts(engine)
            checked_out = 0

            def checkout(*args: object) -> None:
                nonlocal checked_out
                checked_out += 1

            def checkin(*args: object) -> None:
                nonlocal checked_out
                checked_out -= 1

            def verify_closed() -> None:
                assert checked_out == 0

            source = Source()
            source.before_read = verify_closed
            reader = ReadAssignedBase(setup.claims, source, lambda proof: None)
            event.listen(engine.sync_engine, "checkout", checkout)
            event.listen(engine.sync_engine, "checkin", checkin)
            try:
                export = await reader.execute(
                    setup.request.intent.attempt_id,
                    setup.request.claim,
                    setup.authorization,
                )
                await reader.require_current(
                    setup.request.intent.attempt_id,
                    setup.request.claim,
                    setup.authorization,
                    export,
                )
                assert checked_out == 0
            finally:
                event.remove(engine.sync_engine, "checkout", checkout)
                event.remove(engine.sync_engine, "checkin", checkin)
            current = await setup.claims.recover(
                setup.request.claim.identify(setup.authorization), setup.authorization
            )

            assert source.calls == 1 and await counts(engine) == original_counts
            assert current == original
            assert export.definition_digest == setup.request.intent.definition_digest
            assert (
                export.snapshot.request.base_revision == setup.stored.job.base_revision
            )

    run(scenario())


@pytest.mark.parametrize(
    "change,error_type",
    [
        ("unacknowledged", AssignmentUnavailable),
        ("attempt", AssignmentUnavailable),
        ("proof", ClaimReplayConflict),
        ("scope", ClaimScopeRejected),
        ("credential", WorkerAuthorityExpired),
    ],
)
def test_base_provider_is_not_contacted_for_ineligible_durable_receipts(
    database: DatabaseFactory, change: str, error_type: type[Exception]
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await assigned(engine)
            if change != "unacknowledged":
                await setup.service.execute(setup.request, setup.authorization)
            original_counts = await counts(engine)
            request, authorization = setup.request.claim, setup.authorization
            attempt_id = setup.request.intent.attempt_id
            if change == "attempt":
                attempt_id = uuid4()
            elif change == "proof":
                request = replace(request, proof=LeaseProof.generate())
            elif change == "scope":
                authorization = replace(
                    authorization, allowed_repository_ids=frozenset({uuid4()})
                )
            elif change == "credential":
                authorization = replace(
                    authorization,
                    credential_expires_at=datetime.now(UTC) - timedelta(seconds=1),
                )
            source = Source()
            reader = ReadAssignedBase(setup.claims, source, lambda proof: None)

            with pytest.raises(error_type):
                await reader.execute(attempt_id, request, authorization)

            assert source.calls == 0
            assert await counts(engine) == original_counts

    run(scenario())


@pytest.mark.parametrize("point", ["before", "fetch", "encoding"])
def test_base_disclosure_checks_current_db_authority_at_every_wait(
    database: DatabaseFactory, point: str
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await assigned(engine, short_lease=True)
            await setup.service.execute(setup.request, setup.authorization)
            original_counts = await counts(engine)
            # A renewed login cannot revive the assignment's original short lease.
            authorization = replace(
                setup.authorization,
                credential_expires_at=datetime.now(UTC) + timedelta(hours=1),
            )
            source = Source()
            reader = ReadAssignedBase(setup.claims, source, lambda proof: None)
            if point == "before":
                await asyncio.sleep(2.05)
            elif point == "fetch":
                source.wait_seconds = 2.05

            if point == "encoding":
                export = await reader.execute(
                    setup.request.intent.attempt_id, setup.request.claim, authorization
                )
                await asyncio.sleep(2.05)
                with pytest.raises(AssignmentUnavailable):
                    await reader.require_current(
                        setup.request.intent.attempt_id,
                        setup.request.claim,
                        authorization,
                        export,
                    )
            else:
                with pytest.raises(AssignmentUnavailable):
                    await reader.execute(
                        setup.request.intent.attempt_id,
                        setup.request.claim,
                        authorization,
                    )

            assert source.calls == (0 if point == "before" else 1)
            assert await counts(engine) == original_counts
            current = await setup.claims.recover(
                setup.request.claim.identify(authorization), authorization
            )
            assert current is not None and not current.lease_authority_active
            assert current.attempt is not None and current.attempt.reserves_execution

    run(scenario())
