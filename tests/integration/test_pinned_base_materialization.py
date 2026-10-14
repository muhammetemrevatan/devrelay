import asyncio
import sys
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
from pydantic import SecretStr

from devrelay.application.contracts.claims import LeaseProofVerifier
from devrelay.application.contracts.executions import (
    ExecutionIsolation,
    ExecutionPolicy,
    ExecutionRuntime,
)
from devrelay.application.contracts.processes import ExecutionProcessBinding
from devrelay.application.contracts.worker_recovery import AcknowledgementCompletion
from devrelay.application.contracts.workspaces import WorkspaceBinding
from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from devrelay.bootstrap.github import open_github_base_source
from devrelay.bootstrap.worker import open_worker_journal
from devrelay.bootstrap.workspaces import open_worker_workspace
from devrelay.config.github import GitHubRepositoryId, GitHubSettings
from devrelay.config.repositories import CheckSettings, RepositorySettings
from devrelay.config.worker import WorkerSettings
from devrelay.infrastructure.workspace.windows import WindowsWorkspacePolicy
from tests.acknowledgement_fixtures import acknowledgement_inputs
from tests.base_source_fixtures import source_fixture
from tests.integration.test_protected_workspace_native import fixture_supervisor
from tests.integration.test_worker_journal import TOKEN

pytestmark = pytest.mark.skipif(
    sys.platform != "win32", reason="Windows DPAPI, private journal and native files"
)


def test_protected_live_scope_materializes_broker_fixture_and_cannot_reuse_history(
    tmp_path: Path,
) -> None:
    _, original = acknowledgement_inputs()
    root = tmp_path / "workspaces"
    root.mkdir()
    directory = tmp_path / "private-state"
    settings = WorkerSettings(
        control_url="https://control.example/",
        auth_token=SecretStr(TOKEN),
        workspace_root=root,
    )
    provider_calls: list[str] = []

    async def exercise() -> None:
        async with open_worker_journal(
            settings, worker_id=original.outcome.worker_id, state_directory=directory
        ) as worker:
            request = await worker.prepare()
            outcome = replace(
                original.outcome, claim_request_id=request.claim_request_id
            )
            delivery = replace(original, outcome=outcome)
            await worker.record_outcome(request, outcome)
            acknowledgement = await worker.prepare_acknowledgement(delivery)
            await worker.record_acknowledgement(
                request,
                AcknowledgementCompletion(
                    intent=acknowledgement,
                    acknowledged_at=outcome.recorded_at + timedelta(seconds=1),
                ),
            )
            reference = outcome.assignment
            assert reference is not None
            async with fixture_supervisor(delivery) as supervisor:
                execution = await worker.prepare_execution(
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
                pins = fixture.snapshot.request
                provider = GitHubSettings(
                    api_token=SecretStr("test-only-broker-token"),
                    repository_ids=(
                        GitHubRepositoryId(
                            repository_id=pins.repository_id,
                            github_id=pins.provider_repository_id,
                        ),
                    ),
                )
                repository = RepositorySettings(
                    repository_id=pins.repository_id,
                    slug=pins.repository_slug,
                    enabled=True,
                    target_branch="main",
                    coding_branch_prefix="devrelay/jobs",
                    policy_version="v1",
                    allowed_project_keys=frozenset({"DRY"}),
                    checks=(
                        CheckSettings(id="check", argv=("unused",), timeout_seconds=1),
                    ),
                    max_repair_cycles=0,
                )

                def respond(http_request: httpx.Request) -> httpx.Response:
                    provider_calls.append(http_request.url.path)
                    return fixture.respond(http_request)

                policy = WindowsWorkspacePolicy(
                    root_directory=str(root), protected_directories=(str(directory),)
                )
                # A local fixture composes trusted broker reading with the worker;
                # this is not a deployed channel or credential transfer policy.
                async with (
                    open_worker_workspace(binding, policy) as workspace,
                    open_github_base_source(
                        provider, (repository,), transport=httpx.MockTransport(respond)
                    ) as source,
                ):
                    reservation = await worker.reserve_workspace(
                        delivery, supervisor, workspace
                    )
                    before = await worker.recover()
                    result = await worker.materialize_base(
                        delivery, supervisor, workspace, reservation, source
                    )
                    after = await worker.recover()
                    assert after.request is not None and before.request is not None
                    verifier = LeaseProofVerifier.from_proof(
                        worker_id=outcome.worker_id,
                        claim_request_id=before.request.claim_request_id,
                        proof=before.request.proof,
                    )
                    assert verifier.matches(
                        after.request.proof,
                        worker_id=outcome.worker_id,
                        claim_request_id=after.request.claim_request_id,
                    )
                    assert (
                        before.request.protocol_version
                        == after.request.protocol_version
                    )
                    assert replace(after, request=before.request) == before
                    assert result.reservation is reservation
                    assert result.manifest_digest == fixture.snapshot.fingerprint()
                    assert result.file_count == 3
                    target = root / reservation.directory_name
                    assert {
                        path.relative_to(target).as_posix(): path.read_bytes()
                        for path in target.rglob("*")
                        if path.is_file()
                    } == {item.path: item.content for item in fixture.snapshot.files}
                    assert not (target / ".git").exists()
                    count = len(provider_calls)
                    with pytest.raises(WorkspaceUnavailable):
                        await worker.materialize_base(
                            delivery, supervisor, workspace, reservation, source
                        )
                    assert len(provider_calls) == count
            historical = await worker.recover_workspace(delivery)
            assert historical is not None and historical.completion is not None
        async with open_worker_journal(
            settings, worker_id=outcome.worker_id, state_directory=directory
        ) as reopened:
            assert await reopened.recover_workspace(delivery) == historical
        assert (target / "data/payload.bin").read_bytes() == b"\x00\xffbinary\r\n"

    asyncio.run(exercise())
    assert provider_calls[0] == provider_calls[-1]
    assert all("/git/" in path for path in provider_calls[1:-1])
