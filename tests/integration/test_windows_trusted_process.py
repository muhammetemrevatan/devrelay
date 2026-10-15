import ctypes
import os
import sys
from pathlib import Path
from time import monotonic, sleep
from typing import cast

import pytest

from devrelay.application.contracts.trusted_processes import TrustedProcessSpec
from devrelay.application.ports.process_scope import ProcessScopeUnavailable
from devrelay.infrastructure.worker_processes.trusted_windows import (
    WindowsTrustedProcess,
)
from devrelay.infrastructure.worker_processes.windows import WindowsJobObject
from tests.unit.test_windows_job_scope import LIMITS, binding

pytestmark = pytest.mark.skipif(
    sys.platform != "win32",
    reason="Native Windows trusted suspended fixture processes.",
)


def specification(code: str, directory: Path) -> TrustedProcessSpec:
    return TrustedProcessSpec(
        executable=sys.executable,
        arguments=("-I", "-c", code),
        working_directory=str(directory),
        environment=(("SystemRoot", os.environ["SystemRoot"]),),
    )


def collect(process: WindowsTrustedProcess) -> tuple[int, bytes, bytes]:
    deadline = monotonic() + 10
    stdout, stderr = bytearray(), bytearray()
    while monotonic() < deadline:
        observation = process.observe()
        stdout.extend(observation.stdout)
        stderr.extend(observation.stderr)
        if (
            observation.exit_code is not None
            and observation.stdout_eof
            and observation.stderr_eof
        ):
            return observation.exit_code, bytes(stdout), bytes(stderr)
        sleep(0.01)
    pytest.fail("Trusted native fixture did not finish within its finite test budget.")


def test_native_fixture_is_suspended_attached_then_resumed_with_exact_binary_stdio(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    marker = tmp_path / "started.txt"
    monkeypatch.setenv("DEVRELAY_FIXTURE_AMBIENT", "must-not-inherit")
    code = (
        "import os,sys; from pathlib import Path; "
        f"Path({str(marker)!r}).write_text('started'); "
        "assert 'DEVRELAY_FIXTURE_AMBIENT' not in os.environ; "
        "assert sys.stdin.buffer.read()==b''; "
        "sys.stdout.buffer.write(bytes(range(256))*100); "
        "sys.stderr.buffer.write(b'fixture stderr\\x00\\xff')"
    )
    job = WindowsJobObject(binding(), LIMITS)
    process = WindowsTrustedProcess(job)
    try:
        process.prepare(specification(code, tmp_path))
        assert not marker.exists() and job.active_processes() == 1

        process.resume()
        exit_code, stdout, stderr = collect(process)

        assert exit_code == 0 and marker.read_text() == "started"
        assert stdout == bytes(range(256)) * 100
        assert stderr == b"fixture stderr\x00\xff"
        with pytest.raises(ProcessScopeUnavailable):
            process.resume()
    finally:
        process.close()
        job.close()


def test_native_genuine_exit259_does_not_look_like_running(tmp_path: Path) -> None:
    job = WindowsJobObject(binding(), LIMITS)
    process = WindowsTrustedProcess(job)
    try:
        process.prepare(specification("import os; os._exit(259)", tmp_path))
        process.resume()

        exit_code, stdout, stderr = collect(process)

        assert exit_code == 259 and stdout == b"" and stderr == b""
    finally:
        process.close()
        job.close()


def test_native_handle_list_excludes_an_unrelated_inheritable_event(
    tmp_path: Path,
) -> None:
    loader = getattr(ctypes, "WinDLL", None)
    assert loader
    kernel = cast(
        ctypes.CDLL, loader("Kernel32.dll", use_last_error=True, winmode=0x800)
    )

    class Security(ctypes.Structure):
        _fields_ = [
            ("length", ctypes.c_uint32),
            ("descriptor", ctypes.c_void_p),
            ("inherit", ctypes.c_int32),
        ]

    kernel.CreateEventW.argtypes = [
        ctypes.POINTER(Security),
        ctypes.c_int32,
        ctypes.c_int32,
        ctypes.c_wchar_p,
    ]
    kernel.CreateEventW.restype = ctypes.c_void_p
    kernel.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
    kernel.WaitForSingleObject.restype = ctypes.c_uint32
    kernel.CloseHandle.argtypes = [ctypes.c_void_p]
    kernel.CloseHandle.restype = ctypes.c_int32
    security = Security(length=ctypes.sizeof(Security), inherit=1)
    event = kernel.CreateEventW(ctypes.byref(security), True, False, None)
    assert event
    code = (
        "import ctypes; "
        "kernel=ctypes.WinDLL('Kernel32.dll',use_last_error=True,winmode=0x800); "
        "kernel.SetEvent.argtypes=[ctypes.c_void_p];kernel.SetEvent.restype=ctypes.c_int32; "
        f"print(int(bool(kernel.SetEvent({event}))))"
    )
    job = WindowsJobObject(binding(), LIMITS)
    process = WindowsTrustedProcess(job)
    try:
        process.prepare(specification(code, tmp_path))
        process.resume()

        exit_code, stdout, stderr = collect(process)

        assert exit_code == 0 and stdout.strip() == b"0" and stderr == b""
        assert kernel.WaitForSingleObject(event, 0) == 258
    finally:
        process.close()
        job.close()
        assert kernel.CloseHandle(event)


def test_native_seal_after_prepare_never_resumes_and_close_preserves_caller_job(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "must-not-start.txt"
    job = WindowsJobObject(binding(), LIMITS)
    process = WindowsTrustedProcess(job)
    try:
        process.prepare(
            specification(
                f"from pathlib import Path;Path({str(marker)!r}).write_text('started')",
                tmp_path,
            )
        )
        process.seal()

        with pytest.raises(ProcessScopeUnavailable):
            process.resume()
        process.close()

        assert not marker.exists() and job.active_processes() == 0
        process.close()
    finally:
        process.close()
        job.close()


def test_native_close_terminates_descendants_after_root_exit_without_closing_job(
    tmp_path: Path,
) -> None:
    ready = tmp_path / "descendant.txt"
    child = f"from pathlib import Path;import time;Path({str(ready)!r}).write_text('ready');time.sleep(60)"
    parent = f"import subprocess,sys;subprocess.Popen([sys.executable,'-I','-c',{child!r}],close_fds=True,creationflags=0x08000000)"
    job = WindowsJobObject(binding(), LIMITS)
    process = WindowsTrustedProcess(job)
    try:
        process.prepare(specification(parent, tmp_path))
        process.resume()
        deadline = monotonic() + 10
        while not ready.exists():
            assert monotonic() < deadline
            sleep(0.01)
        while process.observe().exit_code is None:
            assert monotonic() < deadline
            sleep(0.01)
        assert job.active_processes() >= 1

        process.close()

        deadline = monotonic() + 5
        while job.active_processes() != 0:
            assert monotonic() < deadline
            sleep(0.01)
        assert ready.read_text() == "ready"
    finally:
        process.close()
        job.close()
