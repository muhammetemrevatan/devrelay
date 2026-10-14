import ctypes
import re
import sys
from dataclasses import dataclass
from threading import RLock
from typing import cast

from devrelay.application.contracts.workspaces import (
    WorkspaceBinding,
    WorkspaceDirectoryIdentity,
    WorkspaceReservation,
)
from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable

_FAILURE = "Windows workspace reservation is unavailable."
_DWORD = ctypes.c_uint32
_BOOL = ctypes.c_int32
_HANDLE = ctypes.c_void_p
_INVALID_HANDLE = (1 << (8 * ctypes.sizeof(_HANDLE))) - 1
_RESERVED = {"CON", "PRN", "AUX", "NUL", "CLOCK$", "CONIN$", "CONOUT$"} | {
    prefix + suffix for prefix in ("COM", "LPT") for suffix in "123456789¹²³"
}


def _path(value: object, *, root: bool) -> str:
    if (
        type(value) is not str
        or not 3 <= len(value) <= 200
        or not re.match(r"^[A-Za-z]:\\", value)
    ):
        raise WorkspaceUnavailable(_FAILURE)
    if value == value[:3]:
        if root:
            raise WorkspaceUnavailable(_FAILURE)
        return value
    for part in value[3:].split("\\"):
        if (
            not part
            or part in (".", "..")
            or part[-1] in ". "
            or part.split(".", 1)[0].rstrip(" .").upper() in _RESERVED
            or any(
                ord(char) < 32
                or 127 <= ord(char) <= 159
                or 0xD800 <= ord(char) <= 0xDFFF
                or char in '/:*?"<>|~'
                for char in part
            )
        ):
            raise WorkspaceUnavailable(_FAILURE)
    if len(value.encode("utf-16-le")) // 2 > 200:
        raise WorkspaceUnavailable(_FAILURE)
    return value


def _overlap(first: str, second: str) -> bool:
    first, second = first.casefold().rstrip("\\"), second.casefold().rstrip("\\")
    return (
        first == second
        or first.startswith(second + "\\")
        or second.startswith(first + "\\")
    )


@dataclass(frozen=True, slots=True, kw_only=True)
class WindowsWorkspacePolicy:
    root_directory: str
    protected_directories: tuple[str, ...]

    def __post_init__(self) -> None:
        root = _path(self.root_directory, root=True)
        if (
            type(self.protected_directories) is not tuple
            or not 1 <= len(self.protected_directories) <= 16
        ):
            raise WorkspaceUnavailable(_FAILURE)
        protected = tuple(
            _path(value, root=False) for value in self.protected_directories
        )
        if len({value.casefold() for value in protected}) != len(protected) or any(
            _overlap(root, value) for value in protected
        ):
            raise WorkspaceUnavailable(_FAILURE)


class _FileTime(ctypes.Structure):
    _fields_ = [("dwLowDateTime", _DWORD), ("dwHighDateTime", _DWORD)]


class _FileInformation(ctypes.Structure):
    _fields_ = [
        ("dwFileAttributes", _DWORD),
        ("ftCreationTime", _FileTime),
        ("ftLastAccessTime", _FileTime),
        ("ftLastWriteTime", _FileTime),
        ("dwVolumeSerialNumber", _DWORD),
        ("nFileSizeHigh", _DWORD),
        ("nFileSizeLow", _DWORD),
        ("nNumberOfLinks", _DWORD),
        ("nFileIndexHigh", _DWORD),
        ("nFileIndexLow", _DWORD),
    ]


class _FindData(ctypes.Structure):
    _fields_ = [
        ("dwFileAttributes", _DWORD),
        ("ftCreationTime", _FileTime),
        ("ftLastAccessTime", _FileTime),
        ("ftLastWriteTime", _FileTime),
        ("nFileSizeHigh", _DWORD),
        ("nFileSizeLow", _DWORD),
        ("dwReserved0", _DWORD),
        ("dwReserved1", _DWORD),
        ("cFileName", ctypes.c_uint16 * 260),
        ("cAlternateFileName", ctypes.c_uint16 * 14),
    ]


def _load_kernel() -> ctypes.CDLL:
    loader = getattr(ctypes, "WinDLL", None)
    if sys.platform != "win32" or loader is None:
        raise WorkspaceUnavailable(_FAILURE)
    return cast(ctypes.CDLL, loader("Kernel32.dll", use_last_error=True, winmode=0x800))


class WindowsWorkspaceReservation:
    """Reserve one new directory while retaining checked ancestor handles.

    The externally trusted coordinator owns the root. This observation supplies
    neither ACL isolation, hostile same-user race protection, Git provenance nor
    crash-safe uniqueness. Failed/cancelled attempts may leave a directory behind.
    """

    def __init__(
        self, binding: WorkspaceBinding, policy: WindowsWorkspacePolicy
    ) -> None:
        if not isinstance(binding, WorkspaceBinding) or not isinstance(
            policy, WindowsWorkspacePolicy
        ):
            raise WorkspaceUnavailable(_FAILURE)
        self._binding, self._policy = binding, policy
        self._used = False
        self._closed = False
        self._lock = RLock()
        self._handles: list[int] = []
        self._find_handles: list[int] = []
        self._kernel: ctypes.CDLL | None = None

    @property
    def binding(self) -> WorkspaceBinding:
        return self._binding

    def _native(self) -> ctypes.CDLL:
        if self._kernel is None:
            raise WorkspaceUnavailable(_FAILURE)
        return self._kernel

    def _bind(self) -> None:
        pointer = ctypes.POINTER
        for name, args, result in (
            (
                "CreateFileW",
                [
                    ctypes.c_wchar_p,
                    _DWORD,
                    _DWORD,
                    ctypes.c_void_p,
                    _DWORD,
                    _DWORD,
                    _HANDLE,
                ],
                _HANDLE,
            ),
            ("GetFileInformationByHandle", [_HANDLE, pointer(_FileInformation)], _BOOL),
            (
                "GetFinalPathNameByHandleW",
                [_HANDLE, ctypes.c_wchar_p, _DWORD, _DWORD],
                _DWORD,
            ),
            ("GetHandleInformation", [_HANDLE, pointer(_DWORD)], _BOOL),
            ("CreateDirectoryW", [ctypes.c_wchar_p, ctypes.c_void_p], _BOOL),
            ("FindFirstFileW", [ctypes.c_wchar_p, pointer(_FindData)], _HANDLE),
            ("FindNextFileW", [_HANDLE, pointer(_FindData)], _BOOL),
            ("FindClose", [_HANDLE], _BOOL),
            ("GetLastError", [], _DWORD),
            ("CloseHandle", [_HANDLE], _BOOL),
        ):
            function = getattr(self._native(), name)
            function.argtypes, function.restype = args, result

    def _verify(self, handle: int, path: str) -> WorkspaceDirectoryIdentity:
        native = self._native()
        information, flags = _FileInformation(), _DWORD()
        if not native.GetFileInformationByHandle(handle, ctypes.byref(information)) or (
            not information.dwFileAttributes & 0x10
            or information.dwFileAttributes & 0x400
        ):
            raise WorkspaceUnavailable(_FAILURE)
        if (
            not native.GetHandleInformation(handle, ctypes.byref(flags))
            or flags.value & 1
        ):
            raise WorkspaceUnavailable(_FAILURE)
        final = ctypes.create_unicode_buffer(261)
        count = native.GetFinalPathNameByHandleW(handle, final, len(final), 0)
        if type(count) is not int or not 0 < count < len(final):
            raise WorkspaceUnavailable(_FAILURE)
        normalized = final.value
        if (
            not normalized.startswith("\\\\?\\")
            or normalized[4:].casefold() != path.casefold()
        ):
            raise WorkspaceUnavailable(_FAILURE)
        return WorkspaceDirectoryIdentity(
            volume_serial_number=information.dwVolumeSerialNumber,
            file_index=(information.nFileIndexHigh << 32) | information.nFileIndexLow,
        )

    def _open_directory(self, path: str) -> tuple[int, WorkspaceDirectoryIdentity]:
        handle = self._native().CreateFileW(path, 0, 0x3, None, 3, 0x02200000, None)
        if type(handle) is not int or not 0 < handle < _INVALID_HANDLE:
            raise WorkspaceUnavailable(_FAILURE)
        self._handles.append(handle)
        return handle, self._verify(handle, path)

    def _empty(self, path: str) -> None:
        native, data = self._native(), _FindData()
        handle = native.FindFirstFileW(path + "\\*", ctypes.byref(data))
        if handle == _INVALID_HANDLE:
            if native.GetLastError() != 2:
                raise WorkspaceUnavailable(_FAILURE)
            return
        if type(handle) is not int or not 0 < handle < _INVALID_HANDLE:
            raise WorkspaceUnavailable(_FAILURE)
        self._find_handles.append(handle)
        names: set[str] = set()
        for _ in range(3):
            units = list(data.cFileName)
            if 0 not in units:
                raise WorkspaceUnavailable(_FAILURE)
            name = "".join(chr(unit) for unit in units[: units.index(0)])
            if name not in (".", "..") or name in names:
                raise WorkspaceUnavailable(_FAILURE)
            names.add(name)
            if not native.FindNextFileW(handle, ctypes.byref(data)):
                if native.GetLastError() != 18:
                    raise WorkspaceUnavailable(_FAILURE)
                if not native.FindClose(handle):
                    raise WorkspaceUnavailable(_FAILURE)
                self._find_handles.remove(handle)
                return
        raise WorkspaceUnavailable(_FAILURE)

    def reserve(self) -> WorkspaceReservation:
        with self._lock:
            if self._used or self._closed:
                raise WorkspaceUnavailable(_FAILURE)
            self._used = True
            try:
                directory_name = str(self._binding.execution.intent.execution_id)
                root = self._policy.root_directory
                target = root + "\\" + directory_name
                if len(target) >= 260:
                    raise WorkspaceUnavailable(_FAILURE)
                self._kernel = _load_kernel()
                self._bind()
                paths = [root[:3]]
                for part in root[3:].split("\\"):
                    paths.append(paths[-1].rstrip("\\") + "\\" + part)
                held = [(path, *self._open_directory(path)) for path in paths]
                if not self._native().CreateDirectoryW(target, None):
                    raise WorkspaceUnavailable(_FAILURE)
                handle, identity = self._open_directory(target)
                held.append((target, handle, identity))
                self._empty(target)
                for path, current, previous in held:
                    if self._verify(current, path) != previous:
                        raise WorkspaceUnavailable(_FAILURE)
                return WorkspaceReservation(
                    binding=self._binding,
                    directory_name=directory_name,
                    identity=identity,
                )
            except Exception:
                try:
                    self.close()
                except WorkspaceUnavailable:
                    pass
                raise WorkspaceUnavailable(_FAILURE) from None

    def close(self) -> None:
        with self._lock:
            self._closed = True
            failed = False
            for handles, operation in (
                (self._find_handles, "FindClose"),
                (self._handles, "CloseHandle"),
            ):
                for handle in tuple(reversed(handles)):
                    try:
                        if not getattr(self._native(), operation)(handle):
                            raise WorkspaceUnavailable(_FAILURE)
                    except Exception:
                        failed = True
                    else:
                        handles.remove(handle)
            if failed:
                raise WorkspaceUnavailable(_FAILURE)
