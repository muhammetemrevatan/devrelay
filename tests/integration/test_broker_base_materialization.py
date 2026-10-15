import asyncio
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import httpx
import pytest

from devrelay.api.dependencies import BaseSourceOperations, ClaimOperation
from devrelay.application.base_source_delivery import ReadAssignedBase
from devrelay.application.contracts.assignment_delivery import AssignmentDelivery
from devrelay.application.contracts.assignments import ClaimResult, NoWorkReason
from devrelay.application.contracts.claims import (
    ClaimRequestIdentity,
    WorkerClaimRequest,
)
from devrelay.application.contracts.executions import (
    ExecutionIsolation,
    ExecutionPolicy,
    ExecutionRuntime,
)
from devrelay.application.contracts.processes import ExecutionProcessBinding
from devrelay.application.contracts.worker_recovery import AcknowledgementCompletion
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.contracts.workspaces import WorkspaceBinding
from devrelay.application.worker_claims import RequestWorkerClaim
from devrelay.bootstrap import control
from devrelay.bootstrap.control import create_app
from devrelay.bootstrap.github import open_github_base_source
from devrelay.bootstrap.security import worker_authenticator
from devrelay.bootstrap.worker import open_worker_journal
from devrelay.bootstrap.workspaces import open_worker_workspace
from devrelay.config.control import ControlSettings
from devrelay.config.worker import WorkerSettings
from devrelay.infrastructure.control.base_sources import HttpControlBaseSource
from devrelay.infrastructure.control.gateway import HttpControlGateway
from devrelay.infrastructure.workspace.windows import WindowsWorkspacePolicy
from tests.assignment_fixtures import NOW, assigned_result
from tests.base_source_fixtures import source_fixture
from tests.integration.test_protected_workspace_native import fixture_supervisor

pytestmark = pytest.mark.skipif(
    sys.platform != "win32", reason="Native Windows protected source materialization"
)


@pytest.mark.parametrize("editable", [False, True])
def test_broker_fixture_to_http_worker_and_native_protected_workspace(
    tmp_path: Path,
    worker_configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
    editable: bool,
) -> None:
    values = worker_configuration.model_dump()
    values["worker_claims"] = {}
    values["worker_base_sources"] = {}
    settings = ControlSettings.model_validate(values)
    registration = settings.workers[0]
    authorization = WorkerAuthorization(
        worker_id=registration.worker_id,
        allowed_repository_ids=registration.allowed_repository_ids,
        credential_expires_at=registration.expires_at,
    )
    result = assigned_result(authorization)
    assert result.attempt is not None
    running = replace(
        result,
        attempt=result.attempt.acknowledge(
            worker_id=authorization.worker_id,
            generation=1,
            now=NOW + timedelta(seconds=1),
        ),
    )
    root = tmp_path / "workspaces"
    root.mkdir()
    private = tmp_path / "private"
    worker_settings = WorkerSettings(
        control_url="https://control.example/",
        auth_token=registration.auth_token,
        workspace_root=root,
    )
    monkeypatch.setattr(
        control,
        "worker_authenticator",
        lambda configured: worker_authenticator(configured, now=lambda: NOW),
    )
    provider_calls: list[str] = []

    async def exercise() -> None:
        async with open_worker_journal(
            worker_settings, worker_id=authorization.worker_id, state_directory=private
        ) as preparation:
            claim = await preparation.prepare()
            delivery = AssignmentDelivery.from_result(
                result,
                worker_id=authorization.worker_id,
                claim_request_id=claim.claim_request_id,
            )
            await preparation.record_outcome(claim, delivery.outcome)
            acknowledgement = await preparation.prepare_acknowledgement(delivery)
            await preparation.record_acknowledgement(
                claim,
                AcknowledgementCompletion(
                    intent=acknowledgement, acknowledged_at=NOW + timedelta(seconds=1)
                ),
            )
            reference = delivery.outcome.assignment
            assert reference is not None
            async with fixture_supervisor(delivery) as supervisor:
                execution = await preparation.prepare_execution(
                    delivery,
                    supervisor,
                    ExecutionPolicy(
                        runtime=ExecutionRuntime.CODEX,
                        isolation=ExecutionIsolation.WINDOWS_RESTRICTED,
                        policy_digest="a" * 64,
                    ),
                )
                binding = await asyncio.to_thread(
                    WorkspaceBinding,
                    execution=ExecutionProcessBinding(
                        intent=execution, reference=reference
                    ),
                    definition=delivery.definition,
                )
                fixture = source_fixture(binding)
                reads: list[ClaimRequestIdentity] = []

                class Store:
                    async def recover(
                        self,
                        identity: ClaimRequestIdentity,
                        current: WorkerAuthorization,
                    ) -> ClaimResult:
                        claim.identify(authorization).require_replay(identity)
                        assert current == authorization
                        reads.append(identity)
                        return running

                def provider_response(request: httpx.Request) -> httpx.Response:
                    provider_calls.append(request.url.path)
                    assert (
                        request.headers["authorization"]
                        == "Bearer " + settings.github.api_token.get_secret_value()
                        if settings.github
                        else False
                    )
                    return fixture.respond(request)

                async def unused_claim(
                    request: WorkerClaimRequest, current: WorkerAuthorization
                ) -> ClaimResult:
                    return ClaimResult(
                        recorded_at=NOW, recovered=True, no_work=NoWorkReason.EMPTY
                    )

                @asynccontextmanager
                async def claims() -> AsyncIterator[ClaimOperation]:
                    yield unused_claim

                @asynccontextmanager
                async def sources() -> AsyncIterator[BaseSourceOperations]:
                    assert settings.github is not None
                    async with open_github_base_source(
                        settings.github,
                        settings.repositories,
                        transport=httpx.MockTransport(provider_response),
                    ) as source:
                        reader = ReadAssignedBase(Store(), source, lambda proof: None)
                        yield BaseSourceOperations(
                            read_base=reader.execute, verify_base=reader.require_current
                        )

                app = create_app(
                    settings, claim_resources=claims, base_source_resources=sources
                )
                async with (
                    app.router.lifespan_context(app),
                    httpx.AsyncClient(
                        base_url="https://control.example/",
                        headers={
                            "Authorization": "Bearer "
                            + registration.auth_token.get_secret_value()
                        },
                        trust_env=False,
                        follow_redirects=False,
                        transport=httpx.ASGITransport(app),
                    ) as client,
                ):

                    async def bind(
                        request: WorkerClaimRequest,
                        current: WorkerAuthorization,
                        accepted: AssignmentDelivery,
                    ) -> HttpControlBaseSource:
                        return await asyncio.to_thread(
                            HttpControlBaseSource,
                            client,
                            request,
                            current,
                            accepted,
                            now=lambda: NOW,
                        )

                    gateway = HttpControlGateway(client, now=lambda: NOW)
                    worker = RequestWorkerClaim(
                        preparation,
                        gateway,
                        authorization,
                        heartbeat_gateway=gateway,
                        base_source_binding=bind,
                    )
                    monkeypatch.setattr(
                        worker, "heartbeat_supervisor", lambda: supervisor
                    )
                    policy = WindowsWorkspacePolicy(
                        root_directory=str(root), protected_directories=(str(private),)
                    )
                    async with open_worker_workspace(binding, policy) as workspace:
                        reservation = await preparation.reserve_workspace(
                            delivery, supervisor, workspace
                        )
                        history = await preparation.recover_workspace(delivery)
                        materialized = await worker.materialize_base(
                            delivery, workspace, reservation
                        )
                        assert (
                            materialized.manifest_digest
                            == fixture.snapshot.fingerprint()
                        )
                        assert await preparation.recover_workspace(delivery) == history
                        target = root / reservation.directory_name
                        assert {
                            p.relative_to(target).as_posix(): p.read_bytes()
                            for p in target.rglob("*")
                            if p.is_file()
                        } == {
                            item.path: item.content for item in fixture.snapshot.files
                        }
                        assert not (target / ".git").exists()
                        assert all(
                            claim.proof.reveal().encode() not in p.read_bytes()
                            for p in target.rglob("*")
                            if p.is_file()
                        )
                        if editable:
                            handed = await worker.handoff_materialized_base(
                                delivery, workspace, materialized
                            )
                            assert handed.materialization is materialized
                            assert handed.location == workspace.location
                            # Exercise literal filesystem edits without any command/model.
                            (target / "README.md").write_bytes(b"Edited fixture\n")
                            replacement = target / "replacement.txt"
                            replacement.write_bytes(b"Atomic replacement\n")
                            replacement.replace(target / "README.md")
                            (target / "README.md").unlink()
                            (target / "src").rename(target / "package")
                            (target / "package/app.py").unlink()
                            (target / "package").rmdir()
                            (target / "new.txt").write_bytes(b"New fixture\n")
                            assert (target / "new.txt").read_bytes() == b"New fixture\n"
                            assert (
                                await preparation.recover_workspace(delivery) == history
                            )
                            with pytest.raises(PermissionError):
                                target.rename(root / "must-stay-pinned")
                assert len(reads) >= 3
        assert (target / "data/payload.bin").read_bytes() == b"\x00\xffbinary\r\n"

    asyncio.run(exercise())
    assert provider_calls[0] == provider_calls[-1]
