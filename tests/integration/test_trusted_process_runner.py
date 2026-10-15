import asyncio
import os
import sys
from pathlib import Path

import pytest

from devrelay.application.contracts.processes import ProcessStopStatus
from devrelay.application.contracts.trusted_processes import (
    TrustedProcessPolicy,
    TrustedProcessSpec,
    TrustedProcessStopReason,
)
from devrelay.application.contracts.worker_recovery import AssignmentReference
from devrelay.application.trusted_processes import RunTrustedProcess
from devrelay.bootstrap.trusted_processes import open_trusted_process_scope
from devrelay.infrastructure.worker_state.clock import WindowsLeaseClock
from tests.unit.test_process_bootstrap import binding, limits

pytestmark = pytest.mark.skipif(
    sys.platform != "win32", reason="Actual trusted Windows fixture process runner."
)


def command(tmp_path: Path, code: str) -> TrustedProcessSpec:
    return TrustedProcessSpec(
        executable=sys.executable,
        arguments=("-I", "-c", code),
        working_directory=str(tmp_path),
        environment=(("SystemRoot", os.environ["SystemRoot"]),),
    )


class Guard:
    def __init__(self, reference: AssignmentReference) -> None:
        self.reference = reference

    def require_current_assignment(self, reference: AssignmentReference) -> None:
        assert reference == self.reference


def test_runner_collects_binary_streams_exit_and_does_not_inherit_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DEVRELAY_FIXTURE_PRIVATE_CANARY", "must-not-be-inherited")
    spec = command(
        tmp_path,
        "import os,sys; "
        "assert 'DEVRELAY_FIXTURE_PRIVATE_CANARY' not in os.environ; "
        "assert os.getcwd() == sys.argv[1]; "
        "os.write(1,b'out\\x00\\xff'); os.write(2,b'err\\x00\\xfe'); sys.exit(7)",
    )
    spec = TrustedProcessSpec(
        executable=spec.executable,
        arguments=(*spec.arguments, str(tmp_path)),
        working_directory=spec.working_directory,
        environment=spec.environment,
    )
    association = binding()

    async def exercise() -> None:
        async with open_trusted_process_scope(association, limits()) as scope:
            runner = RunTrustedProcess(
                association, scope, Guard(association.reference), WindowsLeaseClock()
            )

            result = await runner.run(spec)

            assert result.reason is TrustedProcessStopReason.EXITED
            assert result.exit_code == 7
            assert result.stdout == b"out\x00\xff" and result.stderr == b"err\x00\xfe"
            assert result.stop_status is ProcessStopStatus.EMPTY_VERIFIED
            assert await scope.active_processes() == 0

    asyncio.run(exercise())


@pytest.mark.parametrize("stream", [1, 2])
def test_runner_stops_real_output_flood_with_bounded_capture(
    tmp_path: Path, stream: int
) -> None:
    spec = command(tmp_path, f"import os\nwhile True: os.write({stream}, b'x' * 8192)")
    policy = TrustedProcessPolicy(maximum_output_bytes=4096)
    association = binding()

    async def exercise() -> None:
        async with open_trusted_process_scope(association, limits()) as scope:
            runner = RunTrustedProcess(
                association,
                scope,
                Guard(association.reference),
                WindowsLeaseClock(),
                policy,
            )

            result = await runner.run(spec)

            assert result.reason is TrustedProcessStopReason.OUTPUT_LIMIT
            assert len(result.stdout) + len(result.stderr) <= 4096
            assert result.stop_status is ProcessStopStatus.EMPTY_VERIFIED
            assert await scope.active_processes() == 0

    asyncio.run(exercise())


def test_runner_runtime_stops_descendant_after_root_exit(tmp_path: Path) -> None:
    ready = tmp_path / "descendant-ready.txt"
    child = (
        "from pathlib import Path; import time; "
        f"Path({str(ready)!r}).write_text('ready'); time.sleep(60)"
    )
    spec = command(
        tmp_path,
        "import os,subprocess,sys; "
        f"subprocess.Popen([sys.executable,'-I','-c',{child!r}],"
        "stdout=sys.stdout,stderr=sys.stderr,close_fds=True,creationflags=0x08000000,"
        "env={'SystemRoot':os.environ['SystemRoot']})",
    )
    policy = TrustedProcessPolicy(maximum_runtime_seconds=3)
    association = binding()

    async def exercise() -> None:
        async with open_trusted_process_scope(association, limits()) as scope:
            runner = RunTrustedProcess(
                association,
                scope,
                Guard(association.reference),
                WindowsLeaseClock(),
                policy,
            )

            result = await runner.run(spec)

            assert ready.read_text() == "ready"
            assert result.exit_code == 0
            assert result.reason is TrustedProcessStopReason.RUNTIME_LIMIT
            assert result.stop_status is ProcessStopStatus.EMPTY_VERIFIED
            assert await scope.active_processes() == 0

    asyncio.run(exercise())


def test_runner_cancellation_terminates_actual_fixture_before_scope_close(
    tmp_path: Path,
) -> None:
    ready = tmp_path / "ready.txt"
    spec = command(
        tmp_path,
        "from pathlib import Path; import time; "
        f"Path({str(ready)!r}).write_text('ready'); time.sleep(60)",
    )
    association = binding()

    async def exercise() -> None:
        async with open_trusted_process_scope(association, limits()) as scope:
            runner = RunTrustedProcess(
                association, scope, Guard(association.reference), WindowsLeaseClock()
            )
            task = asyncio.create_task(runner.run(spec))
            try:
                async with asyncio.timeout(10):
                    while not ready.exists():
                        await asyncio.sleep(0.02)

                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task

                assert runner.result is not None
                assert runner.result.reason is TrustedProcessStopReason.CANCELLED
                assert await scope.active_processes() == 0
            finally:
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

    asyncio.run(exercise())
