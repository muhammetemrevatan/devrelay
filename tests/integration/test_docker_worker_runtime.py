"""Opt-in disposable Linux containers, fake data only; never account/model access."""

import asyncio
import json
import os
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic
from uuid import uuid4

import pytest

from devrelay.application.contracts.executions import (
    ExecutionIsolation,
    ExecutionPolicy,
    ExecutionRuntime,
)
from devrelay.application.contracts.git_sources import GitBaseSourceSnapshot
from devrelay.application.contracts.processes import ProcessStopStatus
from devrelay.application.contracts.submissions import CheckPolicy
from devrelay.bootstrap.workspaces import open_worker_workspace
from devrelay.infrastructure.containers import (
    ContainerUnavailable,
    ContainerSpec,
    DockerEngine,
    OwnedContainer,
)
from devrelay.infrastructure.docker_coding import (
    DockerCodingProfile,
    DockerCodingRuntime,
    probe_runtime,
)
from devrelay.infrastructure.workspace.windows import WindowsWorkspacePolicy
from tests.git_source_fixtures import git_workspace_inputs
from tests.unit.test_candidate_objects import commit
from tests.unit.test_worker_workspace_reservation import Guard

IMAGE = os.environ.get("DEVRELAY_TEST_WORKER_IMAGE", "")
pytestmark = [
    pytest.mark.docker_runtime,
    pytest.mark.skipif(not IMAGE, reason="Explicit disposable worker image required."),
]


def engine() -> DockerEngine:
    return DockerEngine(
        "npipe:////./pipe/dockerDesktopLinuxEngine"
        if sys.platform == "win32"
        else "unix:///var/run/docker.sock"
    )


def test_pinned_codex_runtime_preflight_without_model() -> None:
    async def scenario() -> None:
        docker = engine()
        try:
            await probe_runtime(docker, DockerCodingProfile(image_id=IMAGE), monotonic)
        except ContainerUnavailable as error:
            raise AssertionError(
                "Local Docker diagnostic transport failure: "
                + str(docker._last_failure_type)
            ) from error
        finally:
            await docker.aclose()

    asyncio.run(scenario())


@pytest.mark.skipif(
    sys.platform != "win32", reason="Actual Windows native candidate handles."
)
def test_actual_frozen_windows_candidate_checked_in_linux() -> None:
    async def scenario(root: Path) -> None:
        _, delivery, binding, fixture = git_workspace_inputs()
        fixed = CheckPolicy(
            id="readonly",
            argv=(
                "python",
                "-c",
                "exec("
                + repr(
                    "from pathlib import Path\np=Path('/workspace/README.md')\nassert p.read_bytes()\n"
                    "try:p.write_bytes(b'FORBIDDEN')\nexcept OSError:print('READONLY_CONFIRMED')\nelse:raise AssertionError('Writable candidate')\n"
                )
                + ")",
            ),
            working_directory=".",
            timeout_seconds=60,
        )
        definition = replace(delivery.definition, checks=(fixed,))
        profile = DockerCodingProfile(image_id=IMAGE)
        execution = replace(
            binding.execution.intent,
            policy=ExecutionPolicy(
                runtime=ExecutionRuntime.CODEX,
                isolation=ExecutionIsolation.DOCKER_LINUX,
                policy_digest=profile.fingerprint(),
            ),
            acknowledgement=replace(
                binding.execution.intent.acknowledgement,
                definition_digest=definition.fingerprint(),
            ),
        )
        binding = replace(
            binding,
            definition=definition,
            execution=replace(binding.execution, intent=execution),
        )
        guard = Guard(binding.execution.reference, [])
        docker = engine()
        try:
            async with open_worker_workspace(
                binding,
                WindowsWorkspacePolicy(
                    root_directory=str(root),
                    protected_directories=(str(root.parent / "private"),),
                ),
            ) as scope:
                location = await scope.prepare()
                reservation = await scope.reserve(location)
                materialized = await scope.materialize(reservation, fixture.snapshot)
                assert fixture.snapshot.raw_commit is not None
                source = GitBaseSourceSnapshot(
                    snapshot=fixture.snapshot, raw_commit=fixture.snapshot.raw_commit
                )
                await scope.prepare_git_metadata(
                    source, materialized, definition.authorized_branch
                )
                retained = await scope.prepare_candidate_capture(source)
                editable = await scope.handoff(materialized)
                await asyncio.to_thread(
                    (
                        Path(location.root_directory)
                        / reservation.directory_name
                        / "README.md"
                    ).write_text,
                    "Controlled synthetic candidate edit\n",
                )
                candidate = await scope.capture_candidate(
                    retained, editable, capture_id=uuid4(), cycle=0, commit=commit()
                )
                runtime = DockerCodingRuntime(
                    binding.execution,
                    docker,
                    profile,
                    guard,
                    monotonic,
                    workspace=scope,
                )
                try:
                    result = await runtime.check(candidate, fixed)
                    assert (
                        result.exit_code == 0 and b"READONLY_CONFIRMED" in result.stdout
                    )
                    assert result.stop_status is ProcessStopStatus.EMPTY_VERIFIED
                    await scope.verify_candidate(candidate)
                finally:
                    await runtime.aclose()
        finally:
            await docker.aclose()

    artifacts = Path(__file__).resolve().parents[4] / "artifacts"
    with TemporaryDirectory(prefix="dr-docker-native-", dir=artifacts) as directory:
        root = Path(directory) / "w"
        root.mkdir()
        asyncio.run(scenario(root))


def test_fixed_check_imports_actual_candidate_package(tmp_path: Path) -> None:
    execution = uuid4()
    workspace = tmp_path / str(execution)
    (workspace / "src" / "probe_candidate").mkdir(parents=True)
    (workspace / "src" / "probe_candidate" / "__init__.py").write_text(
        'VALUE="current-candidate"\n'
    )
    (workspace / "pyproject.toml").write_text(
        '[project]\nname="dr-probe-candidate"\nversion="9.8.7"\n'
        '[build-system]\nrequires=["uv_build>=0.12.24,<0.13"]\nbuild-backend="uv_build"\n'
        '[tool.uv.build-backend]\nmodule-name="probe_candidate"\n'
    )

    async def scenario() -> None:
        docker = engine()
        spec = ContainerSpec(
            image=IMAGE,
            execution_id=execution,
            invocation_id=uuid4(),
            mode="check",
            workspace=str(workspace),
        )
        owned = OwnedContainer(docker, spec, lambda: None, monotonic)
        try:
            result = await owned.run(
                json.dumps(
                    dict(
                        version=1,
                        mode="check",
                        execution_id=str(execution),
                        invocation_id=str(spec.invocation_id),
                        cycle=0,
                        timeout_seconds=60,
                        cwd=".",
                        argv=[
                            "python",
                            "-c",
                            "import importlib.metadata,probe_candidate; assert probe_candidate.VALUE=='current-candidate'; assert importlib.metadata.version('dr-probe-candidate')=='9.8.7'; assert probe_candidate.__file__.startswith('/tmp/candidate-site/'); print('CANDIDATE_PACKAGE_CONFIRMED')",
                        ],
                    )
                ).encode(),
                timeout_seconds=90,
            )
            assert result.exit_code == 0, result.stderr
            assert b"CANDIDATE_PACKAGE_CONFIRMED" in result.stdout
        finally:
            await owned.aclose()
            await docker.aclose()

    asyncio.run(scenario())


def test_cancellation_kills_original_container_with_descendants(tmp_path: Path) -> None:
    async def scenario() -> None:
        docker = engine()
        execution = uuid4()
        workspace = tmp_path / str(execution)
        workspace.mkdir()
        spec = ContainerSpec(
            image=IMAGE,
            execution_id=execution,
            invocation_id=uuid4(),
            mode="check",
            workspace=str(workspace),
        )
        owned = OwnedContainer(docker, spec, lambda: None, monotonic)
        task = asyncio.create_task(
            owned.run(
                json.dumps(
                    dict(
                        version=1,
                        mode="check",
                        execution_id=str(execution),
                        invocation_id=str(spec.invocation_id),
                        cycle=0,
                        timeout_seconds=60,
                        cwd=".",
                        argv=[
                            "python",
                            "-c",
                            "import subprocess,sys,time; child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)','DEVRELAY_SYNTHETIC_CHILD']); print('DESCENDANT_READY',child.pid,flush=True); time.sleep(60)",
                        ],
                    )
                ).encode(),
                timeout_seconds=90,
            )
        )
        try:
            # Wait for positive process evidence, not an arbitrary startup sleep.
            for _ in range(300):
                await asyncio.sleep(0.1)
                if task.done():
                    task.result()
                    pytest.fail("Expected the bounded sleeper to remain active.")
                if owned._id is not None:
                    # Separate trusted read-only diagnostic; never compete with
                    # the runtime's one-operation HTTP ownership lane.
                    top = await asyncio.to_thread(
                        subprocess.run,
                        [
                            "docker",
                            "--host",
                            "npipe:////./pipe/dockerDesktopLinuxEngine"
                            if sys.platform == "win32"
                            else "unix:///var/run/docker.sock",
                            "top",
                            owned._id,
                            "-eo",
                            "pid,ppid,args",
                        ],
                        capture_output=True,
                        timeout=5,
                        check=False,
                    )
                    if top.returncode == 0 and any(
                        line.strip().endswith(b"DEVRELAY_SYNTHETIC_CHILD")
                        for line in top.stdout.splitlines()
                    ):
                        break
            else:
                pytest.fail(
                    "No positive descendant startup evidence: "
                    + repr(top.stdout[-1024:])
                    + repr(top.stderr[-512:])
                )
            original = owned._id
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await owned.aclose()
            assert original is not None
            missing = await docker.request("GET", f"/containers/{original}/json")
            assert missing.status == 404
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await owned.aclose()
            await docker.aclose()

    asyncio.run(scenario())
