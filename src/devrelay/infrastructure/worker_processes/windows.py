import ctypes
import sys
from threading import Event, RLock
from typing import cast

from devrelay.application.contracts.processes import (
    ExecutionProcessBinding,
    ProcessResourceLimits,
)
from devrelay.application.ports.process_scope import ProcessScopeUnavailable

_FAILURE = "Windows process scope is unavailable."
_DWORD = ctypes.c_uint32
_BOOL = ctypes.c_int32
_HANDLE = ctypes.c_void_p
_LIMIT_FLAGS = 0x00002000 | 0x00000200 | 0x00000008
_CPU_FLAGS = 0x00000001 | 0x00000004
_HANDLE_FLAG_INHERIT = 0x00000001
_BASIC_ACCOUNTING = 1
_EXTENDED_LIMIT = 9
_CPU_RATE = 15


class _BasicLimits(ctypes.Structure):
    _fields_ = [
        ("PerProcessUserTimeLimit", ctypes.c_int64),
        ("PerJobUserTimeLimit", ctypes.c_int64),
        ("LimitFlags", _DWORD),
        ("MinimumWorkingSetSize", ctypes.c_size_t),
        ("MaximumWorkingSetSize", ctypes.c_size_t),
        ("ActiveProcessLimit", _DWORD),
        ("Affinity", ctypes.c_size_t),
        ("PriorityClass", _DWORD),
        ("SchedulingClass", _DWORD),
    ]


class _IoCounters(ctypes.Structure):
    _fields_ = [
        ("ReadOperationCount", ctypes.c_uint64),
        ("WriteOperationCount", ctypes.c_uint64),
        ("OtherOperationCount", ctypes.c_uint64),
        ("ReadTransferCount", ctypes.c_uint64),
        ("WriteTransferCount", ctypes.c_uint64),
        ("OtherTransferCount", ctypes.c_uint64),
    ]


class _ExtendedLimits(ctypes.Structure):
    _fields_ = [
        ("BasicLimitInformation", _BasicLimits),
        ("IoInfo", _IoCounters),
        ("ProcessMemoryLimit", ctypes.c_size_t),
        ("JobMemoryLimit", ctypes.c_size_t),
        ("PeakProcessMemoryUsed", ctypes.c_size_t),
        ("PeakJobMemoryUsed", ctypes.c_size_t),
    ]


class _CpuLimits(ctypes.Structure):
    _fields_ = [("ControlFlags", _DWORD), ("CpuRate", _DWORD)]


class _Accounting(ctypes.Structure):
    _fields_ = [
        ("TotalUserTime", ctypes.c_int64),
        ("TotalKernelTime", ctypes.c_int64),
        ("ThisPeriodTotalUserTime", ctypes.c_int64),
        ("ThisPeriodTotalKernelTime", ctypes.c_int64),
        ("TotalPageFaultCount", _DWORD),
        ("TotalProcesses", _DWORD),
        ("ActiveProcesses", _DWORD),
        ("TotalTerminatedProcesses", _DWORD),
    ]


def _load_kernel() -> ctypes.CDLL:
    loader = getattr(ctypes, "WinDLL", None)
    if sys.platform != "win32" or loader is None:
        raise ProcessScopeUnavailable(_FAILURE)
    return cast(
        ctypes.CDLL,
        loader("Kernel32.dll", use_last_error=True, winmode=0x00000800),
    )


def _valid_handle(value: object) -> bool:
    return type(value) is int and 0 < value < (1 << (8 * ctypes.sizeof(_HANDLE))) - 1


class WindowsJobObject:
    """Private associated-process accounting and caps, not credential isolation.

    Only trusted suspended process handles may be attached. The caller owns their
    creation, suspension, final resume decision and failure cleanup. This adapter
    never opens by PID, resumes a thread, launches code or duplicates its job handle.
    """

    def __init__(
        self, binding: ExecutionProcessBinding, limits: ProcessResourceLimits
    ) -> None:
        if not isinstance(binding, ExecutionProcessBinding) or not isinstance(
            limits, ProcessResourceLimits
        ):
            raise ProcessScopeUnavailable(_FAILURE)
        self._binding = binding
        self._sealed = Event()
        self._lock = RLock()
        self._handle: int | None = None
        self._terminated = False
        try:
            self._kernel = _load_kernel()
            self._bind()
            handle = self._kernel.CreateJobObjectW(None, None)
            if not _valid_handle(handle):
                raise ProcessScopeUnavailable(_FAILURE)
            self._handle = handle
            if not self._kernel.SetHandleInformation(handle, _HANDLE_FLAG_INHERIT, 0):
                raise ProcessScopeUnavailable(_FAILURE)
            flags = _DWORD()
            if not self._kernel.GetHandleInformation(handle, ctypes.byref(flags)) or (
                flags.value & _HANDLE_FLAG_INHERIT
            ):
                raise ProcessScopeUnavailable(_FAILURE)
            extended = _ExtendedLimits()
            extended.BasicLimitInformation.LimitFlags = _LIMIT_FLAGS
            extended.BasicLimitInformation.ActiveProcessLimit = limits.maximum_processes
            extended.JobMemoryLimit = limits.committed_memory_bytes
            self._set(_EXTENDED_LIMIT, extended)
            cpu = _CpuLimits(ControlFlags=_CPU_FLAGS, CpuRate=limits.cpu_percent * 100)
            self._set(_CPU_RATE, cpu)
            effective, effective_cpu = _ExtendedLimits(), _CpuLimits()
            self._query(_EXTENDED_LIMIT, effective)
            self._query(_CPU_RATE, effective_cpu)
            if (
                effective.BasicLimitInformation.LimitFlags != _LIMIT_FLAGS
                or effective.BasicLimitInformation.ActiveProcessLimit
                != limits.maximum_processes
                or effective.JobMemoryLimit != limits.committed_memory_bytes
                or effective_cpu.ControlFlags != _CPU_FLAGS
                or effective_cpu.CpuRate != limits.cpu_percent * 100
            ):
                raise ProcessScopeUnavailable(_FAILURE)
        except Exception:
            self.seal()
            if self._handle is not None:
                try:
                    self.close()
                except ProcessScopeUnavailable:
                    pass
            raise ProcessScopeUnavailable(_FAILURE) from None

    def _bind(self) -> None:
        pointer = ctypes.POINTER
        for name, args, result in (
            ("CreateJobObjectW", [ctypes.c_void_p, ctypes.c_wchar_p], _HANDLE),
            ("SetHandleInformation", [_HANDLE, _DWORD, _DWORD], _BOOL),
            ("GetHandleInformation", [_HANDLE, pointer(_DWORD)], _BOOL),
            (
                "SetInformationJobObject",
                [_HANDLE, ctypes.c_int32, ctypes.c_void_p, _DWORD],
                _BOOL,
            ),
            (
                "QueryInformationJobObject",
                [_HANDLE, ctypes.c_int32, ctypes.c_void_p, _DWORD, pointer(_DWORD)],
                _BOOL,
            ),
            ("AssignProcessToJobObject", [_HANDLE, _HANDLE], _BOOL),
            ("IsProcessInJob", [_HANDLE, _HANDLE, pointer(_BOOL)], _BOOL),
            ("TerminateJobObject", [_HANDLE, _DWORD], _BOOL),
            ("CloseHandle", [_HANDLE], _BOOL),
        ):
            function = getattr(self._kernel, name)
            function.argtypes, function.restype = args, result

    @property
    def binding(self) -> ExecutionProcessBinding:
        return self._binding

    def seal(self) -> None:
        self._sealed.set()

    def _open(self) -> int:
        if self._handle is None:
            raise ProcessScopeUnavailable(_FAILURE)
        return self._handle

    def _set(self, information_class: int, data: ctypes.Structure) -> None:
        if not self._kernel.SetInformationJobObject(
            self._open(), information_class, ctypes.byref(data), ctypes.sizeof(data)
        ):
            raise ProcessScopeUnavailable(_FAILURE)

    def _query(self, information_class: int, data: ctypes.Structure) -> None:
        returned = _DWORD()
        if not self._kernel.QueryInformationJobObject(
            self._open(),
            information_class,
            ctypes.byref(data),
            ctypes.sizeof(data),
            ctypes.byref(returned),
        ) or returned.value != ctypes.sizeof(data):
            raise ProcessScopeUnavailable(_FAILURE)

    def attach_suspended_process(self, process_handle: int) -> None:
        with self._lock:
            try:
                if self._sealed.is_set() or not _valid_handle(process_handle):
                    raise ProcessScopeUnavailable(_FAILURE)
                handle = self._open()
                if not self._kernel.AssignProcessToJobObject(handle, process_handle):
                    raise ProcessScopeUnavailable(_FAILURE)
                member = _BOOL()
                if (
                    not self._kernel.IsProcessInJob(
                        process_handle, handle, ctypes.byref(member)
                    )
                    or member.value != 1
                    or self._sealed.is_set()
                ):
                    raise ProcessScopeUnavailable(_FAILURE)
            except Exception:
                self.seal()
                raise ProcessScopeUnavailable(_FAILURE) from None

    def terminate(self) -> None:
        self.seal()
        with self._lock:
            try:
                handle = self._open()
                if not self._terminated:
                    if not self._kernel.TerminateJobObject(handle, 1):
                        raise ProcessScopeUnavailable(_FAILURE)
                    self._terminated = True
            except Exception:
                raise ProcessScopeUnavailable(_FAILURE) from None

    def active_processes(self) -> int:
        with self._lock:
            try:
                accounting = _Accounting()
                self._query(_BASIC_ACCOUNTING, accounting)
                return int(accounting.ActiveProcesses)
            except Exception:
                raise ProcessScopeUnavailable(_FAILURE) from None

    def close(self) -> None:
        self.seal()
        with self._lock:
            if self._handle is not None:
                try:
                    if not self._kernel.CloseHandle(self._handle):
                        raise ProcessScopeUnavailable(_FAILURE)
                except Exception:
                    raise ProcessScopeUnavailable(_FAILURE) from None
                self._handle = None
