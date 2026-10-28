from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncEngine

from devrelay.api.dependencies import ClaimOperation
from devrelay.application.contracts.assignments import ClaimResult
from devrelay.application.contracts.claims import WorkerClaimRequest
from devrelay.application.contracts.coding_result_wire import (
    encode_coding_result_request,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.bootstrap import control
from devrelay.bootstrap.control import create_app
from devrelay.config.control import ControlSettings
from devrelay.config.github import GitHubRepositoryId
from devrelay.config.repositories import CheckSettings
from devrelay.config.worker_authentication import WorkerRegistrationSettings
from tests.integration.test_atomic_claim_persistence import run
from tests.integration.test_coding_result_store import Context, context, publication

pytestmark = pytest.mark.postgres
type DatabaseFactory = Callable[[], AbstractAsyncContextManager[AsyncEngine]]
TOKEN = "test_only_worker_token_0123456789abcdefghijklmnop"


def configuration(original: ControlSettings, setup: Context) -> ControlSettings:
    definition = setup.prepared.definition
    values = original.model_dump()
    assert original.github is not None
    values["github"] = original.github.model_copy(
        update={
            "repository_ids": (
                GitHubRepositoryId(
                    repository_id=definition.repository_id,
                    github_id=definition.provider_repository_id,
                ),
            ),
        }
    )
    values["repositories"] = (
        original.repositories[0].model_copy(
            update={
                "repository_id": definition.repository_id,
                "checks": tuple(
                    CheckSettings(
                        id=c.id,
                        argv=c.argv,
                        working_directory=c.working_directory,
                        timeout_seconds=c.timeout_seconds,
                    )
                    for c in definition.checks
                ),
            }
        ),
    )
    values["workers"] = (
        WorkerRegistrationSettings(
            worker_id=setup.authorization.worker_id,
            auth_token=SecretStr(TOKEN),
            enabled=True,
            expires_at=setup.authorization.credential_expires_at,
            allowed_repository_ids=frozenset({definition.repository_id}),
        ),
    )
    values["worker_claims"] = {}
    values["worker_coding_results"] = {}
    return ControlSettings.model_validate(values)


def test_real_result_http_composition_accepts_recovers_and_reads_terminal_history(
    database: DatabaseFactory,
    worker_configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        async with database() as engine:
            setup = await context(engine, repairs=2, check_count=2)
            definition = setup.prepared.definition
            settings = configuration(worker_configuration, setup)

            async def claim(
                request: WorkerClaimRequest, authorization: WorkerAuthorization
            ) -> ClaimResult:
                raise AssertionError(
                    "Result delivery never requests another assignment."
                )

            @asynccontextmanager
            async def claims() -> AsyncIterator[ClaimOperation]:
                yield claim

            @asynccontextmanager
            async def existing_database(settings: object) -> AsyncIterator[AsyncEngine]:
                yield engine

            monkeypatch.setattr(control, "open_database_engine", existing_database)
            app = create_app(settings, claim_resources=claims)
            path = f"/api/v1/worker/attempts/{setup.request.intent.execution.acknowledgement.attempt_id}/results"
            body = encode_coding_result_request(setup.request)
            headers = {
                "Authorization": f"Bearer {TOKEN}",
                "Content-Type": "application/json",
            }
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app),
                    base_url="http://test",
                ) as client,
            ):
                unknown = await client.post(
                    path + "/recovery", headers=headers, content=body
                )
                assert unknown.status_code == 404
                accepted = await client.post(path, headers=headers, content=body)
                assert accepted.status_code == 202, accepted.text
                replay = await client.post(path, headers=headers, content=body)
                assert replay.status_code == 202 and replay.json()["recovered"]
                assert accepted.json()["receipt"] == replay.json()["receipt"]
                observed = await publication(engine, setup)
                terminal = await setup.store.finalize_publication(
                    setup.prepared, setup.authorization, publication=observed.verified
                )
                recovered = await client.post(
                    path + "/recovery", headers=headers, content=body
                )
                assert recovered.status_code == 200, recovered.text
                result = recovered.json()
                assert result["state"] == "finalized"
                assert result["terminal"]["number"] == terminal.reference.number
                assert result["terminal"]["branch"] == definition.authorized_branch
                assert result["terminal"]["release_ordinal"] == 1
                assert result["receipt"] == accepted.json()["receipt"]
                assert recovered.headers["Cache-Control"] == "no-store"
                assert (
                    TOKEN not in recovered.text
                    and setup.request.claim.proof.reveal() not in recovered.text
                )

    run(scenario())
