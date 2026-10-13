import asyncio
import ctypes
import os
import subprocess
import sys
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from time import monotonic, sleep
from typing import cast

import pytest

from devrelay.application.contracts.processes import (
    ProcessManagementPolicy,
    ProcessStopStatus,
)
from devrelay.application.contracts.worker_recovery import AssignmentReference
from devrelay.application.ports.process_scope import ProcessScopeUnavailable
from devrelay.application.worker_processes import ManageExecutionProcesses
from devrelay.bootstrap.processes import open_worker_process_scope
from devrelay.infrastructure.worker_processes.windows import WindowsJobObject
from devrelay.infrastructure.worker_state.clock import WindowsLeaseClock
from tests.unit.test_windows_job_scope import LIMITS, binding

pytestmark = pytest.mark.skipif(
    sys.platform != "win32",
    reason="Native Windows Job Objects and suspended process fixtures.",
)

_DWORD = ctypes.c_uint32
_HANDLE = ctypes.c_void_p


class _Startup(ctypes.Structure):
    _fields_ = [
        ("cb", _DWORD),
        ("lpReserved", ctypes.c_wchar_p),
        ("lpDesktop", ctypes.c_wchar_p),
        ("lpTitle", ctypes.c_wchar_p),
        ("dwX", _DWORD),
        ("dwY", _DWORD),
        ("dwXSize", _DWORD),
        ("dwYSize", _DWORD),
        ("dwXCountChars", _DWORD),
        ("dwYCountChars", _DWORD),
        ("dwFillAttribute", _DWORD),
        ("dwFlags", _DWORD),
        ("wShowWindow", ctypes.c_uint16),
        ("cbReserved2", ctypes.c_uint16),
        ("lpReserved2", ctypes.c_void_p),
        ("hStdInput", _HANDLE),
        ("hStdOutput", _HANDLE),
        ("hStdError", _HANDLE),
    ]


class _Process(ctypes.Structure):
    _fields_ = [
        ("hProcess", _HANDLE),
        ("hThread", _HANDLE),
        ("dwProcessId", _DWORD),
        ("dwThreadId", _DWORD),
    ]


class FixtureProcess:
    """Own only a harmless suspended fixture and its two noninherited handles."""

    def __init__(self, code: str, cwd: Path) -> None:
        loader = getattr(ctypes, "WinDLL", None)
        assert loader is not None
        self.kernel = cast(
            ctypes.CDLL, loader("Kernel32.dll", use_last_error=True, winmode=0x800)
        )
        for name, args, result in (
            (
                "CreateProcessW",
                [
                    ctypes.c_wchar_p,
                    ctypes.c_wchar_p,
                    ctypes.c_void_p,
                    ctypes.c_void_p,
                    ctypes.c_int32,
                    _DWORD,
                    ctypes.c_void_p,
                    ctypes.c_wchar_p,
                    ctypes.POINTER(_Startup),
                    ctypes.POINTER(_Process),
                ],
                ctypes.c_int32,
            ),
            ("ResumeThread", [_HANDLE], _DWORD),
            ("TerminateProcess", [_HANDLE, _DWORD], ctypes.c_int32),
            ("WaitForSingleObject", [_HANDLE, _DWORD], _DWORD),
            ("CloseHandle", [_HANDLE], ctypes.c_int32),
        ):
            function = getattr(self.kernel, name)
            function.argtypes, function.restype = args, result
        startup = _Startup(cb=ctypes.sizeof(_Startup))
        process = _Process()
        command = ctypes.create_unicode_buffer(
            subprocess.list2cmdline([sys.executable, "-I", "-c", code])
        )
        environment = ctypes.create_unicode_buffer(
            "SystemRoot=" + os.environ["SystemRoot"] + "\x00\x00"
        )
        assert self.kernel.CreateProcessW(
            sys.executable,
            command,
            None,
            None,
            False,
            0x00000004 | 0x08000000 | 0x00000400,
            environment,
            str(cwd),
            ctypes.byref(startup),
            ctypes.byref(process),
        )
        self.process_handle = int(process.hProcess)
        self.thread_handle = int(process.hThread)
        self.resumes = 0

    def resume(self) -> None:
        assert self.kernel.ResumeThread(self.thread_handle) == 1
        self.resumes += 1

    def exited(self, milliseconds: int = 0) -> bool:
        value = self.kernel.WaitForSingleObject(self.process_handle, milliseconds)
        assert value in (0, 258)
        return bool(value == 0)

    def close(self) -> None:
        if not self.exited():
            assert self.kernel.TerminateProcess(self.process_handle, 1)
            assert self.exited(5000)
        assert self.kernel.CloseHandle(self.thread_handle)
        assert self.kernel.CloseHandle(self.process_handle)


@contextmanager
def process(code: str, cwd: Path) -> Iterator[FixtureProcess]:
    fixture = FixtureProcess(code, cwd)
    try:
        yield fixture
    finally:
        fixture.close()


def eventually(predicate: Callable[[], bool]) -> None:
    deadline = monotonic() + 10
    while not predicate():
        assert monotonic() < deadline, "Native fixture did not settle within its bound."
        sleep(0.02)


def test_native_descendants_remain_accounted_after_parent_exit_until_termination(
    tmp_path: Path,
) -> None:
    ready = tmp_path / "grandchild-ready.txt"
    grandchild = "import time; time.sleep(60)"
    child = (
        "import os,subprocess,sys,time; from pathlib import Path; "
        f"subprocess.Popen([sys.executable,'-I','-c',{grandchild!r}],close_fds=True,creationflags=0x08000000,env={{'SystemRoot':os.environ['SystemRoot']}},cwd={str(tmp_path)!r}); "
        f"Path({str(ready)!r}).write_text('ready'); time.sleep(60)"
    )
    parent = (
        "import os,subprocess,sys; "
        f"subprocess.Popen([sys.executable,'-I','-c',{child!r}],close_fds=True,creationflags=0x08000000,env={{'SystemRoot':os.environ['SystemRoot']}},cwd={str(tmp_path)!r})"
    )
    scope = WindowsJobObject(binding(), LIMITS)
    try:
        with process(parent, tmp_path) as root:
            scope.attach_suspended_process(root.process_handle)
            root.resume()
            eventually(ready.exists)
            eventually(root.exited)
            assert scope.active_processes() >= 2

            scope.terminate()

            eventually(lambda: scope.active_processes() == 0)
            assert root.resumes == 1
    finally:
        scope.close()


def test_native_last_job_close_kills_associated_root_and_never_reports_empty(
    tmp_path: Path,
) -> None:
    scope = WindowsJobObject(binding(), LIMITS)
    with process("import time; time.sleep(60)", tmp_path) as root:
        try:
            scope.attach_suspended_process(root.process_handle)
            root.resume()
            assert scope.active_processes() == 1

            scope.close()

            assert root.exited(5000)
            with pytest.raises(ProcessScopeUnavailable):
                scope.active_processes()
        finally:
            scope.close()


def test_native_sealed_assignment_failure_cleans_suspended_root_without_resume(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "must-not-run.txt"
    scope = WindowsJobObject(binding(), LIMITS)
    scope.seal()
    try:
        with process(
            f"from pathlib import Path; Path({str(marker)!r}).write_text('ran')",
            tmp_path,
        ) as root:
            with pytest.raises(ProcessScopeUnavailable):
                scope.attach_suspended_process(root.process_handle)

            assert root.resumes == 0 and not marker.exists()
            assert scope.active_processes() == 0
        assert not marker.exists()
    finally:
        scope.close()


def test_native_factory_management_stops_associated_scope_with_fresh_empty_evidence(
    tmp_path: Path,
) -> None:
    association = binding()

    class Guard:
        def require_current_assignment(self, reference: AssignmentReference) -> None:
            assert reference == association.reference

    async def exercise() -> None:
        async with open_worker_process_scope(association, LIMITS) as scope:
            with process("import time; time.sleep(60)", tmp_path) as root:
                await scope.attach_suspended_process(root.process_handle)
                root.resume()
                assert await scope.active_processes() == 1
                manager = ManageExecutionProcesses(
                    association,
                    scope,
                    Guard(),
                    WindowsLeaseClock(),
                    ProcessManagementPolicy(stop_timeout_seconds=3),
                )
                manager.request_stop()

                result = await manager.run()

                assert result.status is ProcessStopStatus.EMPTY_VERIFIED
                assert result.execution_id == association.intent.execution_id
                assert await scope.active_processes() == 0
                assert root.exited(5000)
                with pytest.raises(ProcessScopeUnavailable):
                    await scope.attach_suspended_process(root.process_handle)

    asyncio.run(exercise())
