"""Explicit one-task composition; persisted preparations never restart execution."""

import asyncio
import sys
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path

from devrelay.application.contracts.candidates import (
    CandidateCommitIdentity,
    CandidateCommitSpec,
)
from devrelay.application.contracts.executions import (
    ExecutionIsolation,
    ExecutionPolicy,
    ExecutionRuntime,
)
from devrelay.application.contracts.processes import ExecutionProcessBinding
from devrelay.application.contracts.workspaces import WorkspaceBinding
from devrelay.application.ports.worker_journal import WorkerStateUnavailable
from devrelay.application.worker_claims import RequestWorkerClaim
from devrelay.application.worker_coding import CodingRunPolicy, RunAssignedCoding
from devrelay.application.worker_heartbeats import HeartbeatSupervisor
from devrelay.bootstrap.workspaces import open_worker_workspace
from devrelay.config.docker_runtime import DockerRuntimeSettings
from devrelay.config.worker import WorkerSettings
from devrelay.infrastructure.containers import (
    ContainerUnavailable,
    DockerEngine,
    decode_json,
)
from devrelay.infrastructure.docker_coding import (
    DockerCodingProfile,
    DockerCodingRuntime,
    probe_runtime,
)
from devrelay.infrastructure.worker_state.clock import WindowsLeaseClock
from devrelay.infrastructure.workspace.windows import WindowsWorkspacePolicy


def runtime_profile(settings: DockerRuntimeSettings) -> DockerCodingProfile:
    return DockerCodingProfile(
        image_id=settings.image_id,
        auth_volume=settings.auth_volume,
        model=settings.model,
        model_calls_enabled=settings.model_calls_enabled,
        coding_timeout_seconds=settings.coding_timeout_seconds,
        maximum_runtime_seconds=settings.maximum_runtime_seconds,
    )


async def check_engine(engine: DockerEngine, profile: DockerCodingProfile) -> None:
    response = await engine.request("GET", "/info")
    info = decode_json(response.body)
    if (
        response.status != 200
        or not isinstance(info, dict)
        or info.get("OSType") != "linux"
        or type(info.get("MemTotal")) is not int
        or info["MemTotal"] < 2 * 1024**3
    ):
        raise ContainerUnavailable("The configured Linux Docker engine is unavailable.")
    response = await engine.request("GET", "/images/" + profile.image_id + "/json")
    image = decode_json(response.body)
    if (
        response.status != 200
        or not isinstance(image, dict)
        or image.get("Id") != profile.image_id
        or image.get("Os") != "linux"
        or image.get("Architecture") != "amd64"
    ):
        raise ContainerUnavailable(
            "The pinned Linux amd64 runtime image is unavailable."
        )
    if profile.model_calls_enabled:
        assert profile.auth_volume is not None
        response = await engine.request("GET", "/volumes/" + profile.auth_volume)
        volume = decode_json(response.body)
        labels = volume.get("Labels") if isinstance(volume, dict) else None
        if (
            response.status != 200
            or not isinstance(volume, dict)
            or volume.get("Name") != profile.auth_volume
            or not isinstance(labels, dict)
            or labels.get("devrelay.auth") != "1"
            or labels.get("devrelay.image") != profile.image_id
        ):
            raise ContainerUnavailable(
                "The dedicated authentication volume is unavailable."
            )


async def check_runtime(settings: DockerRuntimeSettings) -> None:
    profile = runtime_profile(settings)
    clock = await asyncio.to_thread(WindowsLeaseClock)
    engine = DockerEngine(settings.endpoint)
    try:
        await check_engine(engine, profile)
        await probe_runtime(engine, profile, clock)
    finally:
        await drain_cleanup(engine.aclose())


async def drain_cleanup(operation: Awaitable[None]) -> None:
    """Keep original resources owned until cleanup settles, including repeat cancel."""
    task = asyncio.ensure_future(operation)
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
    task.result()
    if cancelled:
        raise asyncio.CancelledError


async def _first_authority(
    supervisor: HeartbeatSupervisor, task: asyncio.Task[object]
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + supervisor.policy.exchange_timeout_seconds + 1
    while supervisor.deadline is None:
        if task.done() or supervisor.stop is not None or loop.time() >= deadline:
            raise WorkerStateUnavailable("Fresh heartbeat authority is unavailable.")
        await asyncio.sleep(supervisor.policy.monitor_interval_seconds)


async def run_claimed_task(
    worker: RequestWorkerClaim,
    settings: WorkerSettings,
    state_directory: Path,
    engine: DockerEngine,
    profile: DockerCodingProfile,
    clock: Callable[[], float],
) -> str:
    if validate_run_configuration(settings, state_directory) != profile:
        raise WorkerStateUnavailable(
            "Runtime profile changed its original configuration."
        )
    # Recover the old input first. Any existing execution might already have
    # created a container; neither journal status nor a reusable name can undo it.
    previous = await worker.recover()
    if (
        previous.execution is not None
        or previous.workspace_intent is not None
        or previous.workspace_history_unknown
    ):
        recovered = await worker.recover_coding_result()
        if recovered is not None:
            return "Existing coding result recovered; no execution was restarted."
        raise WorkerStateUnavailable(
            "An earlier execution requires trusted reconciliation."
        )
    exchange = await worker.execute()
    if exchange.outcome.assignment is None:
        return "No task is available."
    if not exchange.lease_authority_active:
        raise WorkerStateUnavailable(
            "The assigned task has no current lease authority."
        )
    delivery = await worker.fetch_assignment()
    # The image contains a pinned Python toolchain. Other platforms/toolchains
    # require their own explicit image profile, never host execution/translation.
    if any(
        check.argv[0]
        not in ("python", "python3", "pytest", "ruff", "mypy", "lint-imports")
        for check in delivery.definition.checks
    ):
        raise WorkerStateUnavailable(
            "This image requires explicit supported Linux Python check commands."
        )
    await worker.acknowledge()
    supervisor = worker.heartbeat_supervisor()
    heartbeat = asyncio.create_task(supervisor.run())
    runtime: DockerCodingRuntime | None = None
    try:
        await _first_authority(supervisor, heartbeat)
        reference = delivery.outcome.assignment
        assert reference is not None
        supervisor.require_current_assignment(reference)
        execution = await worker.prepare_execution(
            delivery,
            ExecutionPolicy(
                runtime=ExecutionRuntime.CODEX,
                isolation=ExecutionIsolation.DOCKER_LINUX,
                policy_digest=profile.fingerprint(),
            ),
        )
        binding = ExecutionProcessBinding(intent=execution, reference=reference)
        workspace_binding = WorkspaceBinding(
            execution=binding, definition=delivery.definition
        )
        policy = WindowsWorkspacePolicy(
            root_directory=str(settings.workspace_root),
            protected_directories=(str(state_directory), str(Path.home() / ".codex")),
        )
        async with open_worker_workspace(workspace_binding, policy) as workspace:
            runtime = DockerCodingRuntime(
                binding, engine, profile, supervisor, clock, workspace=workspace
            )
            try:
                reservation = await worker.reserve_workspace(delivery, workspace)
                materialization = await worker.materialize_base(
                    delivery, workspace, reservation
                )
                await worker.prepare_git_base(delivery, workspace, materialization)
                preparation = await worker.prepare_candidate_capture(
                    delivery, workspace, materialization
                )
                editable = await worker.handoff_materialized_base(
                    delivery, workspace, materialization
                )
                assert settings.docker_runtime is not None
                runner = RunAssignedCoding(
                    workspace,
                    runtime,
                    supervisor,
                    monotonic_clock=clock,
                    policy=CodingRunPolicy(
                        maximum_runtime_seconds=settings.docker_runtime.maximum_runtime_seconds,
                        coding_timeout_seconds=profile.coding_timeout_seconds,
                    ),
                )
                assert (
                    settings.commit_author_name is not None
                    and settings.commit_author_email is not None
                )
                identity = CandidateCommitIdentity(
                    name=settings.commit_author_name, email=settings.commit_author_email
                )
                commit = CandidateCommitSpec(
                    author=identity,
                    committer=identity,
                    timestamp=int(datetime.now(UTC).timestamp()),
                    message="Implement assigned task "
                    + str(delivery.definition.job_id)
                    + "\n",
                )
                await worker.run_coding_result(
                    delivery, runner, execution, preparation, editable, commit=commit
                )
                return "Checked coding result accepted; broker publication can proceed."
            finally:
                runtime.seal()
                # Drain actual containers while the original native workspace
                # handles are still held, including cancellation during creation.
                await drain_cleanup(runtime.aclose())
    finally:
        if runtime is not None:
            runtime.seal()
        supervisor.request_stop()

        async def stop_heartbeat() -> None:
            await asyncio.shield(heartbeat)

        await drain_cleanup(stop_heartbeat())


def validate_run_configuration(
    settings: WorkerSettings, state_directory: Path
) -> DockerCodingProfile:
    configured = settings.docker_runtime
    if (
        sys.platform != "win32"
        or configured is None
        or not configured.model_calls_enabled
        or not settings.coding_results_enabled
    ):
        raise WorkerStateUnavailable(
            "One-task execution requires an explicitly enabled Windows/Docker runtime and result delivery."
        )
    if not settings.workspace_root.is_absolute() or not state_directory.is_absolute():
        raise WorkerStateUnavailable(
            "Workspace and private state require explicit absolute directories."
        )
    if settings.commit_author_name is None or settings.commit_author_email is None:
        raise WorkerStateUnavailable(
            "Configure the human author name and email before claiming work."
        )
    CandidateCommitIdentity(
        name=settings.commit_author_name, email=settings.commit_author_email
    )
    WindowsWorkspacePolicy(
        root_directory=str(settings.workspace_root),
        protected_directories=(str(state_directory), str(Path.home() / ".codex")),
    )
    return runtime_profile(configured)
