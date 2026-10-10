import ctypes
import os
import stat
from ctypes import wintypes
from hashlib import sha256
from pathlib import Path
from typing import cast

from devrelay.application.contracts.claims import LeaseProof
from devrelay.application.ports.worker_journal import (
    WorkerStateBusy,
    WorkerStateUnavailable,
)

_FAILURE = "Protected Windows worker state is unavailable."


class _Blob(ctypes.Structure):
    _fields_ = [("size", wintypes.DWORD), ("data", ctypes.c_void_p)]


class _SecurityAttributes(ctypes.Structure):
    _fields_ = [
        ("length", wintypes.DWORD),
        ("descriptor", ctypes.c_void_p),
        ("inherit", wintypes.BOOL),
    ]


class _TokenUser(ctypes.Structure):
    _fields_ = [("sid", ctypes.c_void_p), ("attributes", wintypes.DWORD)]


class _Acl(ctypes.Structure):
    _fields_ = [
        ("revision", ctypes.c_ubyte),
        ("padding", ctypes.c_ubyte),
        ("size", ctypes.c_ushort),
        ("count", ctypes.c_ushort),
        ("padding2", ctypes.c_ushort),
    ]


class _AllowedAce(ctypes.Structure):
    _fields_ = [
        ("kind", ctypes.c_ubyte),
        ("flags", ctypes.c_ubyte),
        ("size", ctypes.c_ushort),
        ("mask", wintypes.DWORD),
        ("sid_start", wintypes.DWORD),
    ]


def _library(name: str) -> ctypes.CDLL:
    loader = getattr(ctypes, "WinDLL", None)
    if os.name != "nt" or loader is None:
        raise WorkerStateUnavailable(_FAILURE)
    return cast(ctypes.CDLL, loader(name, use_last_error=True))


class WindowsWorkerSecurity:
    """User-scoped DPAPI and private storage; create/use/close on one thread."""

    def __init__(self) -> None:
        self._kernel = _library("kernel32")
        self._advapi = _library("advapi32")
        self._crypt = _library("crypt32")
        self._mutex: int | None = None
        self._bind()
        self._sid = self._current_sid()

    def _bind(self) -> None:
        void = ctypes.c_void_p
        pointer = ctypes.POINTER
        for name, args, result in (
            ("GetCurrentProcess", [], wintypes.HANDLE),
            ("GetLastError", [], wintypes.DWORD),
            ("CloseHandle", [wintypes.HANDLE], wintypes.BOOL),
            ("LocalFree", [void], void),
            (
                "CreateDirectoryW",
                [wintypes.LPCWSTR, pointer(_SecurityAttributes)],
                wintypes.BOOL,
            ),
            ("GetDriveTypeW", [wintypes.LPCWSTR], wintypes.UINT),
            (
                "CreateMutexW",
                [pointer(_SecurityAttributes), wintypes.BOOL, wintypes.LPCWSTR],
                wintypes.HANDLE,
            ),
            ("WaitForSingleObject", [wintypes.HANDLE, wintypes.DWORD], wintypes.DWORD),
            ("ReleaseMutex", [wintypes.HANDLE], wintypes.BOOL),
        ):
            function = getattr(self._kernel, name)
            function.argtypes, function.restype = args, result
        for name, args, result in (
            (
                "OpenProcessToken",
                [wintypes.HANDLE, wintypes.DWORD, pointer(wintypes.HANDLE)],
                wintypes.BOOL,
            ),
            (
                "GetTokenInformation",
                [
                    wintypes.HANDLE,
                    ctypes.c_int,
                    void,
                    wintypes.DWORD,
                    pointer(wintypes.DWORD),
                ],
                wintypes.BOOL,
            ),
            ("ConvertSidToStringSidW", [void, pointer(void)], wintypes.BOOL),
            (
                "ConvertStringSecurityDescriptorToSecurityDescriptorW",
                [wintypes.LPCWSTR, wintypes.DWORD, pointer(void), void],
                wintypes.BOOL,
            ),
            (
                "GetNamedSecurityInfoW",
                [
                    wintypes.LPWSTR,
                    ctypes.c_int,
                    wintypes.DWORD,
                    pointer(void),
                    void,
                    pointer(void),
                    void,
                    pointer(void),
                ],
                wintypes.DWORD,
            ),
            (
                "GetSecurityInfo",
                [
                    wintypes.HANDLE,
                    ctypes.c_int,
                    wintypes.DWORD,
                    pointer(void),
                    void,
                    pointer(void),
                    void,
                    pointer(void),
                ],
                wintypes.DWORD,
            ),
            (
                "GetSecurityDescriptorControl",
                [void, pointer(ctypes.c_ushort), pointer(wintypes.DWORD)],
                wintypes.BOOL,
            ),
            ("GetAce", [void, wintypes.DWORD, pointer(void)], wintypes.BOOL),
        ):
            function = getattr(self._advapi, name)
            function.argtypes, function.restype = args, result
        args = [
            ctypes.POINTER(_Blob),
            void,
            ctypes.POINTER(_Blob),
            void,
            void,
            wintypes.DWORD,
            ctypes.POINTER(_Blob),
        ]
        for name in ("CryptProtectData", "CryptUnprotectData"):
            function = getattr(self._crypt, name)
            function.argtypes, function.restype = args, wintypes.BOOL

    def _sid_text(self, sid: ctypes.c_void_p | int | None) -> str:
        value = ctypes.c_void_p()
        if not sid or not self._advapi.ConvertSidToStringSidW(sid, ctypes.byref(value)):
            raise WorkerStateUnavailable(_FAILURE)
        try:
            return ctypes.wstring_at(value)
        finally:
            self._kernel.LocalFree(value)

    def _current_sid(self) -> str:
        token = wintypes.HANDLE()
        if not self._advapi.OpenProcessToken(
            self._kernel.GetCurrentProcess(), 8, ctypes.byref(token)
        ):
            raise WorkerStateUnavailable(_FAILURE)
        try:
            size = wintypes.DWORD()
            self._advapi.GetTokenInformation(token, 1, None, 0, ctypes.byref(size))
            if not 1 <= size.value <= 65536:
                raise WorkerStateUnavailable(_FAILURE)
            buffer = ctypes.create_string_buffer(size.value)
            if not self._advapi.GetTokenInformation(
                token, 1, buffer, size, ctypes.byref(size)
            ):
                raise WorkerStateUnavailable(_FAILURE)
            user = _TokenUser.from_buffer(buffer)
            return self._sid_text(user.sid)
        finally:
            self._kernel.CloseHandle(token)

    def _descriptor(self, *, directory: bool) -> ctypes.c_void_p:
        flags, rights = ("OICI", "FA") if directory else ("", "GA")
        text = f"O:{self._sid}D:P(A;{flags};{rights};;;{self._sid})(A;{flags};{rights};;;SY)"
        result = ctypes.c_void_p()
        if not self._advapi.ConvertStringSecurityDescriptorToSecurityDescriptorW(
            text, 1, ctypes.byref(result), None
        ):
            raise WorkerStateUnavailable(_FAILURE)
        return result

    def _verify_security(
        self, target: str | int, *, directory: bool, root: bool
    ) -> None:
        owner, dacl, descriptor = (
            ctypes.c_void_p(),
            ctypes.c_void_p(),
            ctypes.c_void_p(),
        )
        function = (
            self._advapi.GetNamedSecurityInfoW
            if isinstance(target, str)
            else self._advapi.GetSecurityInfo
        )
        code = function(
            target,
            1 if isinstance(target, str) else 6,
            5,
            ctypes.byref(owner),
            None,
            ctypes.byref(dacl),
            None,
            ctypes.byref(descriptor),
        )
        if code:
            raise WorkerStateUnavailable(_FAILURE)
        try:
            if self._sid_text(owner) != self._sid or not dacl.value:
                raise WorkerStateUnavailable(_FAILURE)
            control, revision = ctypes.c_ushort(), wintypes.DWORD()
            if not self._advapi.GetSecurityDescriptorControl(
                descriptor, ctypes.byref(control), ctypes.byref(revision)
            ):
                raise WorkerStateUnavailable(_FAILURE)
            if root and not control.value & 0x1000:
                raise WorkerStateUnavailable(_FAILURE)
            acl = ctypes.cast(dacl, ctypes.POINTER(_Acl)).contents
            if acl.count != 2:
                raise WorkerStateUnavailable(_FAILURE)
            seen: set[str] = set()
            for index in range(acl.count):
                entry = ctypes.c_void_p()
                if (
                    not self._advapi.GetAce(dacl, index, ctypes.byref(entry))
                    or not entry.value
                ):
                    raise WorkerStateUnavailable(_FAILURE)
                ace = ctypes.cast(entry, ctypes.POINTER(_AllowedAce)).contents
                sid = self._sid_text(entry.value + _AllowedAce.sid_start.offset)
                allowed_mask = 0x1F01FF if isinstance(target, str) else 0x1F0001
                if (
                    ace.kind != 0
                    or ace.flags & 8
                    or ace.mask not in (allowed_mask, 0x10000000)
                    or sid in seen
                ):
                    raise WorkerStateUnavailable(_FAILURE)
                if directory and ace.flags & 3 != 3:
                    raise WorkerStateUnavailable(_FAILURE)
                seen.add(sid)
            if seen != {self._sid, "S-1-5-18"}:
                raise WorkerStateUnavailable(_FAILURE)
        finally:
            self._kernel.LocalFree(descriptor)

    def acquire(self, scope: bytes) -> None:
        if self._mutex is not None:
            raise WorkerStateBusy("Worker recovery state is already open.")
        descriptor = self._descriptor(directory=False)
        attributes = _SecurityAttributes(
            ctypes.sizeof(_SecurityAttributes), descriptor, False
        )
        try:
            handle = self._kernel.CreateMutexW(
                ctypes.byref(attributes),
                False,
                "Global\\DevRelay.worker." + sha256(scope).hexdigest(),
            )
        finally:
            self._kernel.LocalFree(descriptor)
        if not handle:
            raise WorkerStateUnavailable(_FAILURE)
        try:
            self._verify_security(handle, directory=False, root=True)
            result = self._kernel.WaitForSingleObject(handle, 0)
            if result == 258:
                raise WorkerStateBusy("Another process owns worker recovery state.")
            if result not in (0, 128):
                raise WorkerStateUnavailable(_FAILURE)
            self._mutex = int(handle)
        except BaseException:
            self._kernel.CloseHandle(handle)
            raise

    def close(self) -> None:
        if self._mutex is not None:
            handle, self._mutex = self._mutex, None
            released = self._kernel.ReleaseMutex(handle)
            self._kernel.CloseHandle(handle)
            if not released:
                raise WorkerStateUnavailable(_FAILURE)

    def ensure_private_directory(self, directory: Path) -> bool:
        if (
            not directory.is_absolute()
            or len(directory.drive) != 2
            or not directory.drive[0].isalpha()
            or directory.drive[1] != ":"
            or self._kernel.GetDriveTypeW(directory.anchor) != 3
        ):
            raise WorkerStateUnavailable(_FAILURE)
        for parent in directory.parents:
            if parent.is_symlink() or parent.is_junction():
                raise WorkerStateUnavailable(_FAILURE)
        descriptor = self._descriptor(directory=True)
        attributes = _SecurityAttributes(
            ctypes.sizeof(_SecurityAttributes), descriptor, False
        )
        try:
            created = self._kernel.CreateDirectoryW(
                str(directory), ctypes.byref(attributes)
            )
            if not created and self._kernel.GetLastError() != 183:
                raise WorkerStateUnavailable(_FAILURE)
        finally:
            self._kernel.LocalFree(descriptor)
        self._verify_path(directory, directory=True)
        self._verify_security(str(directory), directory=True, root=True)
        allowed = {"journal.sqlite3", "journal.sqlite3-wal", "journal.sqlite3-shm"}
        for child in directory.iterdir():
            if child.name not in allowed:
                raise WorkerStateUnavailable(_FAILURE)
            self.verify_file(child)
        return bool(created)

    def _verify_path(self, path: Path, *, directory: bool) -> None:
        info = path.lstat()
        if getattr(info, "st_file_attributes", 0) & 0x400 or not (
            stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
        ):
            raise WorkerStateUnavailable(_FAILURE)
        if not directory and (info.st_nlink != 1 or info.st_size > 1_048_576):
            raise WorkerStateUnavailable(_FAILURE)

    def verify_file(self, path: Path) -> None:
        self._verify_path(path, directory=False)
        self._verify_security(str(path), directory=False, root=False)

    def _transform(self, data: bytes, context: bytes, *, decrypt: bool) -> bytes:
        if (
            type(data) is not bytes
            or not 1 <= len(data) <= 4096
            or type(context) is not bytes
            or len(context) != 32
        ):
            raise WorkerStateUnavailable(_FAILURE)
        buffer, entropy = (
            ctypes.create_string_buffer(data, len(data)),
            ctypes.create_string_buffer(context, len(context)),
        )
        incoming = _Blob(len(data), ctypes.addressof(buffer))
        additional = _Blob(len(context), ctypes.addressof(entropy))
        output = _Blob()
        function = (
            self._crypt.CryptUnprotectData if decrypt else self._crypt.CryptProtectData
        )
        try:
            if not function(
                ctypes.byref(incoming),
                None,
                ctypes.byref(additional),
                None,
                None,
                1,
                ctypes.byref(output),
            ):
                raise WorkerStateUnavailable(_FAILURE)
            if not output.data or not 1 <= output.size <= 4096:
                raise WorkerStateUnavailable(_FAILURE)
            return ctypes.string_at(output.data, output.size)
        finally:
            ctypes.memset(ctypes.addressof(buffer), 0, len(data))
            ctypes.memset(ctypes.addressof(entropy), 0, len(context))
            if output.data:
                ctypes.memset(output.data, 0, output.size)
                self._kernel.LocalFree(output.data)

    def protect(self, proof: LeaseProof, *, context: bytes) -> bytes:
        return self._transform(proof.reveal().encode("ascii"), context, decrypt=False)

    def unprotect(self, ciphertext: bytes, *, context: bytes) -> LeaseProof:
        try:
            return LeaseProof(
                self._transform(ciphertext, context, decrypt=True).decode("ascii")
            )
        except ValueError, UnicodeError:
            raise WorkerStateUnavailable(_FAILURE) from None
