"""Controlled provider effects with actual publication transactions; no live GitHub."""

import asyncio

import httpx
import pytest

from devrelay.application.contracts.coding_publications import (
    PublicationPolicy,
    PublicationWriteKind,
)
from devrelay.application.contracts.coding_result_receipts import (
    CodingPublicationState,
    CodingResultUnavailable,
)
from devrelay.application.plan_coding_publication import build_publication_plan
from devrelay.bootstrap.coding_publication import open_coding_publisher
from devrelay.config.control import ControlSettings
from tests.integration.test_atomic_claim_persistence import run
from tests.integration.test_coding_result_http import configuration
from tests.integration.test_coding_result_store import (
    Context,
    DatabaseFactory,
    context,
    projection,
)
from tests.unit.test_coding_publication_configuration import TOKEN
from tests.unit.test_github_coding_publication import GitHub

pytestmark = pytest.mark.postgres


def settings(
    original: ControlSettings, setup: Context, *, max_writes: int = 128
) -> ControlSettings:
    values = configuration(original, setup).model_dump()
    values["coding_publication"] = {
        "api_token": TOKEN,
        "allowed_repository_ids": frozenset({setup.prepared.definition.repository_id}),
        "max_writes": max_writes,
    }
    return ControlSettings.model_validate(values)


def test_real_broker_publisher_verifies_controlled_provider_and_releases_once(
    database: DatabaseFactory, worker_configuration: ControlSettings
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine, repairs=2, check_count=2)
            accepted = await setup.store.accept(setup.prepared, setup.authorization)
            plan = await asyncio.to_thread(
                build_publication_plan,
                setup.prepared,
                accepted.receipt,
                PublicationPolicy(),
            )
            github = GitHub(plan, token=TOKEN)
            async with open_coding_publisher(
                settings(worker_configuration, setup),
                engine,
                transport=httpx.MockTransport(github),
            ) as publisher:
                terminal = await publisher.execute(setup.prepared, setup.authorization)
                replay = await publisher.execute(setup.prepared, setup.authorization)
            state = await projection(engine)
            history = await setup.store.read_publication(
                setup.prepared, setup.authorization
            )

            assert replay == terminal and terminal.reference.number == 42
            assert terminal.reference.head_revision == plan.head_revision
            assert terminal.reference.source_branch == plan.source_branch
            assert terminal.reference.target_branch == plan.target_branch
            assert history is not None and history.status.terminal == terminal
            assert (
                state["status"],
                state["attempt_status"],
                state["release_count"],
            ) == ("awaiting_review", "succeeded", 1)
            assert state["reserved_attempt_id"] is None
            assert state["results"] == state["operations"] == 1
            assert state["operation_events"] == 3 + 2 * len(plan.writes)
            assert sum(request.method == "POST" for request in github.requests) == len(
                plan.writes
            )
            assert all(request.method in ("GET", "POST") for request in github.requests)

    run(scenario())


@pytest.mark.parametrize("kind", list(PublicationWriteKind))
def test_lost_provider_response_is_observed_without_another_write_or_release(
    database: DatabaseFactory,
    worker_configuration: ControlSettings,
    kind: PublicationWriteKind,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine)
            accepted = await setup.store.accept(setup.prepared, setup.authorization)
            plan = await asyncio.to_thread(
                build_publication_plan,
                setup.prepared,
                accepted.receipt,
                PublicationPolicy(),
            )
            github = GitHub(plan, token=TOKEN)
            target = next(write for write in plan.writes if write.kind is kind)
            lost = False

            def lose(
                request: httpx.Request, response: httpx.Response
            ) -> httpx.Response:
                nonlocal lost
                if (
                    not lost
                    and request.method == "POST"
                    and request.content == target.body
                ):
                    lost = True
                    raise httpx.ReadError(
                        "Controlled effect committed but response lost.",
                        request=request,
                    )
                return response

            github.hook = lose
            async with open_coding_publisher(
                settings(worker_configuration, setup),
                engine,
                transport=httpx.MockTransport(github),
            ) as publisher:
                with pytest.raises(CodingResultUnavailable):
                    await publisher.execute(setup.prepared, setup.authorization)
                writes_before = sum(
                    request.method == "POST" for request in github.requests
                )
                reconciled = await publisher.reconcile(
                    setup.prepared, setup.authorization
                )
                repeated = await publisher.reconcile(
                    setup.prepared, setup.authorization
                )
                with pytest.raises(CodingResultUnavailable):
                    await publisher.execute(setup.prepared, setup.authorization)
            state = await projection(engine)
            history = await setup.store.read_publication(
                setup.prepared, setup.authorization
            )

            assert lost and reconciled is not None and repeated == reconciled
            assert reconciled.state is CodingPublicationState.UNCERTAIN
            assert reconciled.terminal is None
            assert history is not None and history.last_step is not None
            assert history.last_step.step_id == target.write_id
            assert history.last_observation_digest is not None
            assert state["status"] == "publishing_pr" and state["release_count"] == 0
            assert state["reserved_attempt_id"] == accepted.receipt.attempt_id
            assert (
                sum(request.method == "POST" for request in github.requests)
                == writes_before
            )
            assert (
                sum(
                    request.method == "POST" and request.content == target.body
                    for request in github.requests
                )
                == 1
            )

    run(scenario())


def test_complete_plan_budget_rejection_precedes_owner_and_provider_writes(
    database: DatabaseFactory, worker_configuration: ControlSettings
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine)
            await setup.store.accept(setup.prepared, setup.authorization)
            requests: list[httpx.Request] = []

            def unexpected(request: httpx.Request) -> httpx.Response:
                requests.append(request)
                raise AssertionError("An oversized plan must make no provider request.")

            async with open_coding_publisher(
                settings(worker_configuration, setup, max_writes=3),
                engine,
                transport=httpx.MockTransport(unexpected),
            ) as publisher:
                with pytest.raises((ValueError, CodingResultUnavailable)):
                    await publisher.execute(setup.prepared, setup.authorization)
            history = await setup.store.read_publication(
                setup.prepared, setup.authorization
            )
            state = await projection(engine)

            assert requests == []
            assert (
                history is not None
                and history.status.state is CodingPublicationState.ACCEPTED
            )
            assert (
                history.invocation_id is None and history.status.operation_version == 0
            )
            assert state["release_count"] == 0

    run(scenario())
