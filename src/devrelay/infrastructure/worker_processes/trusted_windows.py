"""Explicit trusted host processes; this bridge supplies no identity sandbox."""

import ctypes
import subprocess
import sys
from threading import Event, RLock
from typing import cast

from devrelay.application.contracts.processes import ExecutionProcessBinding
from devrelay.application.contracts.trusted_processes import (
    TRUSTED_PROCESS_READ_SIZE,
    TrustedProcessObservation,
    TrustedProcessSpec,
)
from devrelay.application.ports.process_scope import ProcessScopeUnavailable
from devrelay.infrastructure.worker_processes.windows import WindowsJobObject

_FAILURE = "Trusted Windows process is unavailable."
_DWORD = ctypes.c_uint32
_BOOL = ctypes.c_int32
_HANDLE = ctypes.c_void_p
_INVALID_HANDLE = (1 << (ctypes.sizeof(_HANDLE) * 8)) - 1


class _Security(ctypes.Structure):
    _fields_ = [("length", _DWORD), ("descriptor", ctypes.c_void_p), ("inherit", _BOOL)]


class _Startup(ctypes.Structure):
    _fields_ = [
        ("cb", _DWORD),
        ("reserved", ctypes.c_wchar_p),
        ("desktop", ctypes.c_wchar_p),
        ("title", ctypes.c_wchar_p),
        ("x", _DWORD),
        ("y", _DWORD),
        ("x_size", _DWORD),
        ("y_size", _DWORD),
        ("x_count", _DWORD),
        ("y_count", _DWORD),
        ("fill", _DWORD),
        ("flags", _DWORD),
        ("show", ctypes.c_uint16),
        ("reserved_size", ctypes.c_uint16),
        ("reserved_bytes", ctypes.c_void_p),
        ("stdin", _HANDLE),
        ("stdout", _HANDLE),
        ("stderr", _HANDLE),
    ]


class _StartupEx(ctypes.Structure):
    _fields_ = [("startup", _Startup), ("attributes", ctypes.c_void_p)]


class _Process(ctypes.Structure):
    _fields_ = [
        ("process", _HANDLE),
        ("thread", _HANDLE),
        ("pid", _DWORD),
        ("tid", _DWORD),
    ]


def _load_kernel() -> ctypes.CDLL:
    loader = getattr(ctypes, "WinDLL", None)
    if sys.platform != "win32" or loader is None:
        raise ProcessScopeUnavailable(_FAILURE)
    return cast(ctypes.CDLL, loader("Kernel32.dll", use_last_error=True, winmode=0x800))


def _valid_handle(value: object) -> bool:
    return type(value) is int and 0 < value < _INVALID_HANDLE


class WindowsTrustedProcess:
    """Own one suspended trusted process and its explicitly inherited pipe ends.

    The caller retains ownership of the existing Job Object. There is no user,
    credential, filesystem or network isolation here. Only caller-selected trusted
    commands and harmless fixture processes may use this bridge.
    """

    def __init__(self, scope: WindowsJobObject) -> None:
        self._scope = scope
        self._original_scope = scope
        self._seal_scope = scope.seal
        self._terminate_scope = scope.terminate
        self._attach_scope = scope.attach_suspended_process
        self._count_scope = scope.active_processes
        self._binding = scope.binding
        self._binding.__post_init__()
        self._sealed = Event()
        self._lock = RLock()
        self._prepared = False
        self._ready = False
        self._resumed = False
        self._closed = False
        self._handles: list[int] = []
        self._process: int | None = None
        self._thread: int | None = None
        self._reads: dict[str, int] = {}
        self._eof = {"stdout": False, "stderr": False}
        try:
            self._kernel = _load_kernel()
            self._bind()
        except Exception:
            self.seal()
            raise ProcessScopeUnavailable(_FAILURE) from None

    @property
    def binding(self) -> ExecutionProcessBinding:
        return self._binding

    def seal(self) -> None:
        self._sealed.set()
        self._seal_scope()

    def _binding_matches(self) -> bool:
        return (
            self._scope is self._original_scope
            and self._original_scope.binding == self._binding
        )

    def _fence(self, *, launch: bool = False) -> None:
        if (
            self._closed
            or not self._binding_matches()
            or (launch and self._sealed.is_set())
        ):
            raise ProcessScopeUnavailable(_FAILURE)

    def _bind(self) -> None:
        pointer = ctypes.POINTER
        for name, args, result in (
            (
                "CreatePipe",
                [pointer(_HANDLE), pointer(_HANDLE), pointer(_Security), _DWORD],
                _BOOL,
            ),
            (
                "CreateFileW",
                [
                    ctypes.c_wchar_p,
                    _DWORD,
                    _DWORD,
                    pointer(_Security),
                    _DWORD,
                    _DWORD,
                    _HANDLE,
                ],
                _HANDLE,
            ),
            ("SetHandleInformation", [_HANDLE, _DWORD, _DWORD], _BOOL),
            ("GetHandleInformation", [_HANDLE, pointer(_DWORD)], _BOOL),
            (
                "InitializeProcThreadAttributeList",
                [ctypes.c_void_p, _DWORD, _DWORD, pointer(ctypes.c_size_t)],
                _BOOL,
            ),
            (
                "UpdateProcThreadAttribute",
                [
                    ctypes.c_void_p,
                    _DWORD,
                    ctypes.c_size_t,
                    ctypes.c_void_p,
                    ctypes.c_size_t,
                    ctypes.c_void_p,
                    ctypes.c_void_p,
                ],
                _BOOL,
            ),
            ("DeleteProcThreadAttributeList", [ctypes.c_void_p], None),
            (
                "CreateProcessW",
                [
                    ctypes.c_wchar_p,
                    ctypes.c_wchar_p,
                    ctypes.c_void_p,
                    ctypes.c_void_p,
                    _BOOL,
                    _DWORD,
                    ctypes.c_void_p,
                    ctypes.c_wchar_p,
                    pointer(_StartupEx),
                    pointer(_Process),
                ],
                _BOOL,
            ),
            ("ResumeThread", [_HANDLE], _DWORD),
            (
                "PeekNamedPipe",
                [
                    _HANDLE,
                    ctypes.c_void_p,
                    _DWORD,
                    ctypes.c_void_p,
                    pointer(_DWORD),
                    ctypes.c_void_p,
                ],
                _BOOL,
            ),
            (
                "ReadFile",
                [_HANDLE, ctypes.c_void_p, _DWORD, pointer(_DWORD), ctypes.c_void_p],
                _BOOL,
            ),
            ("GetExitCodeProcess", [_HANDLE, pointer(_DWORD)], _BOOL),
            ("WaitForSingleObject", [_HANDLE, _DWORD], _DWORD),
            ("TerminateProcess", [_HANDLE, _DWORD], _BOOL),
            ("CloseHandle", [_HANDLE], _BOOL),
            ("GetLastError", [], _DWORD),
        ):
            function = getattr(self._kernel, name)
            function.argtypes, function.restype = args, result

    def _retain(self, handle: object) -> int:
        if not _valid_handle(handle):
            raise ProcessScopeUnavailable(_FAILURE)
        value = cast(int, handle)
        if value in self._handles:
            raise ProcessScopeUnavailable(_FAILURE)
        self._handles.append(value)
        return value

    def _inheritance(self, handle: int, expected: bool) -> None:
        flags = _DWORD()
        if (
            not self._kernel.GetHandleInformation(handle, ctypes.byref(flags))
            or bool(flags.value & 1) != expected
        ):
            raise ProcessScopeUnavailable(_FAILURE)

    def _close_handle(self, handle: int) -> None:
        if not self._kernel.CloseHandle(handle):
            raise ProcessScopeUnavailable(_FAILURE)
        self._handles.remove(handle)

    def _pipe(self, name: str, security: _Security) -> int:
        read, write = _HANDLE(), _HANDLE()
        if not self._kernel.CreatePipe(
            ctypes.byref(read),
            ctypes.byref(write),
            ctypes.byref(security),
            TRUSTED_PROCESS_READ_SIZE,
        ):
            raise ProcessScopeUnavailable(_FAILURE)
        read_handle, write_handle = self._retain(read.value), self._retain(write.value)
        self._reads[name] = read_handle
        if not self._kernel.SetHandleInformation(read_handle, 1, 0):
            raise ProcessScopeUnavailable(_FAILURE)
        self._inheritance(read_handle, False)
        self._inheritance(write_handle, True)
        return write_handle

    def prepare(self, spec: TrustedProcessSpec) -> None:
        with self._lock:
            if self._prepared:
                raise ProcessScopeUnavailable(_FAILURE)
            self._prepared = True
            try:
                self._fence(launch=True)
                if not isinstance(spec, TrustedProcessSpec):
                    raise ProcessScopeUnavailable(_FAILURE)
                spec.__post_init__()
                command = subprocess.list2cmdline([spec.executable, *spec.arguments])
                if len(command.encode("utf-16-le")) // 2 >= 32767:
                    raise ProcessScopeUnavailable(_FAILURE)
                security = _Security(length=ctypes.sizeof(_Security), inherit=1)
                stdin = self._retain(
                    self._kernel.CreateFileW(
                        "NUL", 0x80000000, 3, ctypes.byref(security), 3, 0x80, None
                    )
                )
                self._inheritance(stdin, True)
                stdout, stderr = (
                    self._pipe("stdout", security),
                    self._pipe("stderr", security),
                )
                size = ctypes.c_size_t()
                if (
                    self._kernel.InitializeProcThreadAttributeList(
                        None, 1, 0, ctypes.byref(size)
                    )
                    or self._kernel.GetLastError() != 122
                    or not 0 < size.value <= 65536
                ):
                    raise ProcessScopeUnavailable(_FAILURE)
                attributes = ctypes.create_string_buffer(size.value)
                if not self._kernel.InitializeProcThreadAttributeList(
                    attributes, 1, 0, ctypes.byref(size)
                ):
                    raise ProcessScopeUnavailable(_FAILURE)
                try:
                    inherited = (_HANDLE * 3)(stdin, stdout, stderr)
                    if not self._kernel.UpdateProcThreadAttribute(
                        attributes,
                        0,
                        0x00020002,
                        inherited,
                        ctypes.sizeof(inherited),
                        None,
                        None,
                    ):
                        raise ProcessScopeUnavailable(_FAILURE)
                    startup = _StartupEx(
                        startup=_Startup(
                            cb=ctypes.sizeof(_StartupEx),
                            flags=0x100,
                            stdin=stdin,
                            stdout=stdout,
                            stderr=stderr,
                        ),
                        attributes=ctypes.cast(attributes, ctypes.c_void_p),
                    )
                    process = _Process()
                    environment = (
                        "\x00".join(
                            name + "=" + value
                            for name, value in sorted(
                                spec.environment, key=lambda pair: pair[0].casefold()
                            )
                        )
                        + "\x00\x00"
                    )
                    environment_buffer = ctypes.create_unicode_buffer(environment)
                    self._fence(launch=True)
                    if not self._kernel.CreateProcessW(
                        spec.executable,
                        ctypes.create_unicode_buffer(command),
                        None,
                        None,
                        True,
                        0x00000004 | 0x08000000 | 0x00000400 | 0x00080000,
                        environment_buffer,
                        spec.working_directory,
                        ctypes.byref(startup),
                        ctypes.byref(process),
                    ):
                        raise ProcessScopeUnavailable(_FAILURE)
                    self._process = self._retain(process.process)
                    self._thread = self._retain(process.thread)
                    self._inheritance(self._process, False)
                    self._inheritance(self._thread, False)
                    self._fence(launch=True)
                    self._attach_scope(self._process)
                    self._fence(launch=True)
                    for handle in (stdin, stdout, stderr):
                        self._close_handle(handle)
                    self._fence(launch=True)
                    self._ready = True
                finally:
                    self._kernel.DeleteProcThreadAttributeList(attributes)
            except Exception:
                self.seal()
                try:
                    self.close()
                except ProcessScopeUnavailable:
                    pass
                raise ProcessScopeUnavailable(_FAILURE) from None

    def resume(self) -> None:
        with self._lock:
            try:
                self._fence(launch=True)
                if not self._ready or self._resumed or self._thread is None:
                    raise ProcessScopeUnavailable(_FAILURE)
                self._resumed = True
                if self._kernel.ResumeThread(self._thread) != 1:
                    raise ProcessScopeUnavailable(_FAILURE)
                self._fence(launch=True)
            except Exception:
                self.seal()
                raise ProcessScopeUnavailable(_FAILURE) from None

    def _read(self, name: str) -> bytes:
        if self._eof[name]:
            return b""
        handle, available = self._reads[name], _DWORD()
        if not self._kernel.PeekNamedPipe(
            handle, None, 0, None, ctypes.byref(available), None
        ):
            if self._kernel.GetLastError() in (109, 232):
                self._eof[name] = True
                return b""
            raise ProcessScopeUnavailable(_FAILURE)
        count = min(available.value, TRUSTED_PROCESS_READ_SIZE)
        if count == 0:
            return b""
        buffer, received = ctypes.create_string_buffer(count), _DWORD()
        if not self._kernel.ReadFile(
            handle, buffer, count, ctypes.byref(received), None
        ):
            raise ProcessScopeUnavailable(_FAILURE)
        if not 0 < received.value <= count:
            raise ProcessScopeUnavailable(_FAILURE)
        return buffer.raw[: received.value]

    def observe(self) -> TrustedProcessObservation:
        with self._lock:
            try:
                self._fence()
                if not self._ready or self._process is None:
                    raise ProcessScopeUnavailable(_FAILURE)
                wait = self._kernel.WaitForSingleObject(self._process, 0)
                if wait not in (0, 258):
                    raise ProcessScopeUnavailable(_FAILURE)
                exit_code: int | None = None
                if wait == 0:
                    value = _DWORD()
                    if not self._kernel.GetExitCodeProcess(
                        self._process, ctypes.byref(value)
                    ):
                        raise ProcessScopeUnavailable(_FAILURE)
                    exit_code = value.value
                stdout, stderr = self._read("stdout"), self._read("stderr")
                self._fence()
                return TrustedProcessObservation(
                    exit_code=exit_code,
                    stdout=stdout,
                    stderr=stderr,
                    stdout_eof=self._eof["stdout"],
                    stderr_eof=self._eof["stderr"],
                )
            except Exception:
                self.seal()
                raise ProcessScopeUnavailable(_FAILURE) from None

    def terminate(self) -> None:
        self.seal()
        with self._lock:
            try:
                self._fence()
                self._terminate_scope()
            except Exception:
                raise ProcessScopeUnavailable(_FAILURE) from None

    def active_processes(self) -> int:
        with self._lock:
            try:
                self._fence()
                return self._count_scope()
            except Exception:
                raise ProcessScopeUnavailable(_FAILURE) from None

    def close(self) -> None:
        self.seal()
        with self._lock:
            if self._closed and not self._handles:
                return
            self._closed = True
            failed = False
            try:
                if not self._binding_matches():
                    raise ProcessScopeUnavailable(_FAILURE)
                self._terminate_scope()
            except Exception:
                failed = True
            root_stopped = self._process is None or self._process not in self._handles
            if self._process is not None and self._process in self._handles:
                try:
                    wait = self._kernel.WaitForSingleObject(
                        self._process, 1000 if not failed else 0
                    )
                    if wait == 258:
                        if not self._kernel.TerminateProcess(self._process, 1):
                            # Termination can complete between observation and
                            # the fallback request. Its fresh wait is decisive.
                            if self._kernel.WaitForSingleObject(self._process, 0) != 0:
                                raise ProcessScopeUnavailable(_FAILURE)
                        wait = self._kernel.WaitForSingleObject(self._process, 1000)
                    if wait != 0:
                        raise ProcessScopeUnavailable(_FAILURE)
                    root_stopped = True
                except Exception:
                    failed = True
            for handle in tuple(reversed(self._handles)):
                if not root_stopped and handle in (self._process, self._thread):
                    continue
                try:
                    self._close_handle(handle)
                except Exception:
                    failed = True
            if failed:
                raise ProcessScopeUnavailable(_FAILURE)
