import asyncio
from dataclasses import replace
from pathlib import Path
from time import monotonic
from typing import Any, cast

import pytest
from pydantic import SecretStr, ValidationError

from devrelay.application.contracts.executions import (
    ExecutionIsolation,
    ExecutionPolicy,
    ExecutionRuntime,
)
from devrelay.application.ports.candidate_workspace import CandidateWorkspacePort
from devrelay.application.ports.worker_journal import WorkerStateUnavailable
from devrelay.application.worker_claims import RequestWorkerClaim
from devrelay.bootstrap import worker as composition
from devrelay.bootstrap import worker_coding
from devrelay.bootstrap.worker_coding import drain_cleanup, validate_run_configuration
from devrelay.config.docker_runtime import DockerRuntimeSettings, WorkerRuntimeSettings
from devrelay.config.worker import WorkerSettings
from devrelay.infrastructure.containers import ContainerUnavailable, DockerEngine
from devrelay.infrastructure.docker_coding import (
    DockerCodingProfile,
    DockerCodingRuntime,
)
from devrelay.worker.main import main
from tests.git_source_fixtures import git_workspace_inputs
from tests.unit.test_worker_coding import Workspace
from tests.unit.test_worker_workspace_reservation import Guard

IMAGE = "sha256:" + "a" * 64


@pytest.mark.parametrize(
    "values",
    [
        {"image_id": "mutable:latest"},
        {"image_id": IMAGE, "endpoint": "tcp://remote:2375"},
        {"image_id": IMAGE, "model_calls_enabled": True},
        {"image_id": IMAGE, "model_calls_enabled": "true"},
        {"image_id": IMAGE, "auth_volume": "../personal"},
        {"image_id": IMAGE, "privileged": True},
        {"image_id": IMAGE, "coding_timeout_seconds": True},
        {"image_id": IMAGE, "coding_timeout_seconds": 10, "maximum_runtime_seconds": 9},
    ],
)
def test_runtime_configuration_rejects_unpinned_or_incomplete_boundaries(
    values: dict[str, Any],
) -> None:
    with pytest.raises(ValidationError):
        DockerRuntimeSettings.model_validate(values)


def test_diagnostic_settings_need_no_control_credential(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DEVRELAY_WORKER_AUTH_TOKEN", raising=False)
    monkeypatch.setenv("DEVRELAY_WORKER_DOCKER_RUNTIME", '{"image_id":"' + IMAGE + '"}')
    configured = WorkerRuntimeSettings().docker_runtime
    assert configured is not None and configured.image_id == IMAGE
    assert not configured.model_calls_enabled and configured.auth_volume is None


def test_disabled_runtime_rejected_before_control_identity_or_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = WorkerSettings(
        control_url="http://127.0.0.1:8000",
        auth_token=SecretStr("test_only_worker_token_0123456789abcdefghijklmnop"),
        docker_runtime=DockerRuntimeSettings(image_id=IMAGE),
    )
    monkeypatch.setattr(composition, "WorkerSettings", lambda **kwargs: settings)

    def unexpected(*args: object, **kwargs: object) -> None:
        raise AssertionError("Disabled execution must not contact control/Docker.")

    monkeypatch.setattr(composition, "open_worker_claim_client", unexpected)
    with pytest.raises(WorkerStateUnavailable):
        asyncio.run(composition.run_worker_once(None, Path("/private")))
    with pytest.raises(WorkerStateUnavailable):
        validate_run_configuration(settings, Path("/private"))


def test_cleanup_retains_original_task_through_repeated_cancellation() -> None:
    async def scenario() -> None:
        entered, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def cleanup() -> None:
            entered.set()
            await release.wait()
            finished.set()

        waiter = asyncio.create_task(drain_cleanup(cleanup()))
        await entered.wait()
        waiter.cancel()
        await asyncio.sleep(0)
        waiter.cancel()
        await asyncio.sleep(0)
        assert not waiter.done() and not finished.is_set()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert finished.is_set()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "argv",
    [
        ["--run-one"],
        ["--check-config", "--runtime-image", IMAGE],
        ["--run-one", "--check-config"],
    ],
)
def test_cli_rejects_ambiguous_actions_before_side_effects(
    monkeypatch: pytest.MonkeyPatch, argv: list[str]
) -> None:
    monkeypatch.setattr("sys.argv", ["devrelay-worker", *argv])
    with pytest.raises(SystemExit) as error:
        main()
    assert error.value.code == 2


@pytest.mark.parametrize("has_result", [False, True])
def test_existing_execution_never_becomes_another_claim_or_launch(
    monkeypatch: pytest.MonkeyPatch,
    has_result: bool,
) -> None:
    state, _, _, _ = git_workspace_inputs()
    profile = DockerCodingProfile(image_id=IMAGE)
    calls: list[str] = []

    class Existing:
        async def recover(self) -> Any:
            calls.append("recover")
            return state

        async def recover_coding_result(self) -> Any:
            calls.append("recover_result")
            return object() if has_result else None

        async def execute(self) -> Any:
            raise AssertionError("Historical execution cannot claim or launch again.")

    monkeypatch.setattr(
        worker_coding, "validate_run_configuration", lambda *args: profile
    )
    operation = worker_coding.run_claimed_task(
        cast(RequestWorkerClaim, Existing()),
        cast(WorkerSettings, object()),
        Path("/private"),
        cast(DockerEngine, object()),
        profile,
        monotonic,
    )
    if has_result:
        assert "no execution was restarted" in asyncio.run(operation)
    else:
        with pytest.raises(WorkerStateUnavailable):
            asyncio.run(operation)
    assert calls == ["recover", "recover_result"]


@pytest.mark.parametrize("enabled", [False, True])
def test_detached_workspace_values_or_disabled_model_cannot_reach_docker(
    enabled: bool,
) -> None:
    workspace = Workspace(0, [])
    profile = DockerCodingProfile(
        image_id=IMAGE,
        auth_volume="fixture-only" if enabled else None,
        model_calls_enabled=enabled,
    )
    binding = replace(
        workspace.binding.execution,
        intent=replace(
            workspace.binding.execution.intent,
            policy=ExecutionPolicy(
                runtime=ExecutionRuntime.CODEX,
                isolation=ExecutionIsolation.DOCKER_LINUX,
                policy_digest=profile.fingerprint(),
            ),
        ),
    )
    workspace.binding = replace(workspace.binding, execution=binding)
    guard = Guard(binding.reference, [])
    runtime = DockerCodingRuntime(
        binding,
        cast(DockerEngine, object()),
        profile,
        guard,
        monotonic,
        workspace=cast(CandidateWorkspacePort, workspace),
    )

    async def scenario() -> None:
        try:
            with pytest.raises(
                ContainerUnavailable, match="live workspace|not enabled"
            ):
                await runtime.code(
                    workspace.delivery,
                    replace(workspace.editable),
                    cycle=0,
                    previous=None,
                )
        finally:
            await runtime.aclose()

    asyncio.run(scenario())


def test_failed_runtime_readiness_never_opens_control_identity_or_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import devrelay.infrastructure.containers as containers
    import devrelay.infrastructure.docker_coding as docker_coding

    calls: list[str] = []
    settings = WorkerSettings(
        control_url="http://127.0.0.1:8000",
        auth_token=SecretStr("test_only_worker_token_0123456789abcdefghijklmnop"),
        docker_runtime=DockerRuntimeSettings(
            image_id=IMAGE, model_calls_enabled=True, auth_volume="fixture"
        ),
    )
    assert settings.docker_runtime is not None
    profile = worker_coding.runtime_profile(settings.docker_runtime)
    monkeypatch.setattr(composition, "WorkerSettings", lambda **kwargs: settings)
    monkeypatch.setattr(composition, "WindowsLeaseClock", lambda: monotonic)
    monkeypatch.setattr(
        worker_coding, "validate_run_configuration", lambda *args: profile
    )

    class Engine:
        def __init__(self, endpoint: str) -> None:
            pass

        async def aclose(self) -> None:
            calls.append("closed")

    async def check(*args: object) -> None:
        calls.append("engine")

    async def rejected(*args: object) -> None:
        calls.append("readiness")
        raise ContainerUnavailable("Required namespaces are unavailable.")

    def unexpected(*args: object, **kwargs: object) -> None:
        raise AssertionError(
            "Failed admission must not open control identity or claim."
        )

    monkeypatch.setattr(containers, "DockerEngine", Engine)
    monkeypatch.setattr(worker_coding, "check_engine", check)
    monkeypatch.setattr(docker_coding, "probe_runtime", rejected)
    monkeypatch.setattr(composition, "open_worker_claim_client", unexpected)
    with pytest.raises(ContainerUnavailable):
        asyncio.run(composition.run_worker_once(None, Path("/private")))
    assert calls == ["engine", "readiness", "closed"]
