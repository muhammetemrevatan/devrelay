import ctypes
import hashlib
import json
import sys
from dataclasses import dataclass
from threading import Event, RLock
from typing import cast

from devrelay.application.contracts.base_sources import (
    BaseMaterialization,
    BaseSourceFile,
    BaseSourceSnapshot,
)
from devrelay.application.contracts.editable_workspaces import EditableWorkspace
from devrelay.application.contracts.git_sources import (
    GitBaseSourceSnapshot,
    GitWorkspacePreparation,
)
from devrelay.application.contracts.workspace_recovery import (
    WorkspaceLocation,
    canonical_workspace_path,
)
from devrelay.application.contracts.workspaces import (
    WorkspaceBinding,
    WorkspaceDirectoryIdentity,
    WorkspaceReservation,
)
from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from devrelay.infrastructure.workspace.git_metadata import (
    GitMetadata,
    GitMetadataFile,
    build_git_metadata,
)

_FAILURE = "Windows workspace reservation is unavailable."
_DWORD = ctypes.c_uint32
_BOOL = ctypes.c_int32
_HANDLE = ctypes.c_void_p
_INVALID_HANDLE = (1 << (8 * ctypes.sizeof(_HANDLE))) - 1


def _path(value: object, *, root: bool) -> str:
    try:
        return canonical_workspace_path(value, allow_volume_root=not root)
    except ValueError:
        raise WorkspaceUnavailable(_FAILURE) from None


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

    def fingerprint(self) -> str:
        payload = {
            "version": 1,
            "root_directory": self.root_directory,
            "protected_directories": sorted(self.protected_directories),
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()


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
        self._prepared = False
        self._closed = False
        self._lock = RLock()
        self._handles: list[int] = []
        self._find_handles: list[int] = []
        self._kernel: ctypes.CDLL | None = None
        self._location: WorkspaceLocation | None = None
        self._held: list[tuple[str, int, WorkspaceDirectoryIdentity]] = []
        self._reservation: WorkspaceReservation | None = None
        self._materialization_used = False
        self._materialization_sealed = Event()
        self._materialization: BaseMaterialization | None = None
        self._materialization_snapshot: BaseSourceSnapshot | None = None
        self._materialization_files: tuple[
            tuple[
                str, int, WorkspaceDirectoryIdentity, BaseSourceFile | GitMetadataFile
            ],
            ...,
        ] = ()
        self._materialization_nodes: dict[str, bool] = {}
        self._materialization_digest: str | None = None
        self._materialization_handles: tuple[int, ...] = ()
        self._handoff_used = False
        self._git_metadata_used = False
        self._git_metadata: GitMetadata | None = None
        self._git_preparation: GitWorkspacePreparation | None = None
        self._git_metadata_digest: str | None = None

    @property
    def binding(self) -> WorkspaceBinding:
        return self._binding

    @property
    def reservation(self) -> WorkspaceReservation | None:
        return self._reservation

    @property
    def location(self) -> WorkspaceLocation | None:
        return self._location

    @property
    def materialization(self) -> BaseMaterialization | None:
        return self._materialization

    @property
    def git_preparation(self) -> GitWorkspacePreparation | None:
        return self._git_preparation

    def seal_materialization(self) -> None:
        self._materialization_sealed.set()

    def _check_materialization(self) -> None:
        if self._materialization_sealed.is_set() or self._closed:
            raise WorkspaceUnavailable(_FAILURE)

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

    def _verify(
        self, handle: int, path: str, *, directory: bool = True
    ) -> WorkspaceDirectoryIdentity:
        native = self._native()
        information, flags = _FileInformation(), _DWORD()
        if not native.GetFileInformationByHandle(handle, ctypes.byref(information)) or (
            bool(information.dwFileAttributes & 0x10) != directory
            or information.dwFileAttributes & 0x400
            or (not directory and information.nNumberOfLinks != 1)
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
        # Explicit directory read access participates in Windows share checks;
        # metadata-only access does not prevent a directory rename.
        handle = self._native().CreateFileW(path, 1, 0x3, None, 3, 0x02200000, None)
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

    def _recheck_root(self) -> None:
        for path, current, previous in self._held:
            if self._verify(current, path) != previous:
                raise WorkspaceUnavailable(_FAILURE)

    def prepare(self) -> WorkspaceLocation:
        """Retain checked ancestors before any durable creation decision."""
        with self._lock:
            if self._prepared or self._closed:
                raise WorkspaceUnavailable(_FAILURE)
            self._prepared = True
            try:
                root = self._policy.root_directory
                self._kernel = _load_kernel()
                self._bind()
                paths = [root[:3]]
                for part in root[3:].split("\\"):
                    paths.append(paths[-1].rstrip("\\") + "\\" + part)
                self._held = [(path, *self._open_directory(path)) for path in paths]
                self._recheck_root()
                self._location = WorkspaceLocation(
                    root_directory=root,
                    policy_digest=self._policy.fingerprint(),
                    identity=self._held[-1][2],
                )
                return self._location
            except Exception:
                try:
                    self.close()
                except WorkspaceUnavailable:
                    pass
                raise WorkspaceUnavailable(_FAILURE) from None

    def reserve(
        self, location: WorkspaceLocation | None = None
    ) -> WorkspaceReservation:
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
                # The low-level convenience call remains useful for isolated fixtures.
                # The application port always supplies the newly journaled location.
                if location is None:
                    location = self._location if self._prepared else self.prepare()
                if (
                    not isinstance(location, WorkspaceLocation)
                    or location != self._location
                ):
                    raise WorkspaceUnavailable(_FAILURE)
                self._recheck_root()
                if not self._native().CreateDirectoryW(target, None):
                    raise WorkspaceUnavailable(_FAILURE)
                handle, identity = self._open_directory(target)
                self._held.append((target, handle, identity))
                self._empty(target)
                self._recheck_root()
                self._reservation = WorkspaceReservation(
                    binding=self._binding,
                    directory_name=directory_name,
                    identity=identity,
                )
                return self._reservation
            except Exception:
                try:
                    self.close()
                except WorkspaceUnavailable:
                    pass
                raise WorkspaceUnavailable(_FAILURE) from None

    def verify_reservation(self, reservation: WorkspaceReservation) -> None:
        with self._lock:
            try:
                self._check_materialization()
                if (
                    not isinstance(reservation, WorkspaceReservation)
                    or reservation is not self._reservation
                ):
                    raise WorkspaceUnavailable(_FAILURE)
                self._recheck_root()
                target = self._policy.root_directory + "\\" + reservation.directory_name
                if not any(
                    path == target and identity == reservation.identity
                    for path, handle, identity in self._held
                ):
                    raise WorkspaceUnavailable(_FAILURE)
                self._empty(target)
                self._check_materialization()
            except Exception:
                self.seal_materialization()
                raise WorkspaceUnavailable(_FAILURE) from None

    def _bind_materialization(self) -> None:
        pointer = ctypes.POINTER
        for name, args, result in (
            (
                "WriteFile",
                [_HANDLE, ctypes.c_void_p, _DWORD, pointer(_DWORD), ctypes.c_void_p],
                _BOOL,
            ),
            (
                "ReadFile",
                [_HANDLE, ctypes.c_void_p, _DWORD, pointer(_DWORD), ctypes.c_void_p],
                _BOOL,
            ),
            ("FlushFileBuffers", [_HANDLE], _BOOL),
            (
                "SetFilePointerEx",
                [_HANDLE, ctypes.c_int64, ctypes.c_void_p, _DWORD],
                _BOOL,
            ),
        ):
            function = getattr(self._native(), name)
            function.argtypes, function.restype = args, result

    def _write_file(
        self, target: str, item: BaseSourceFile | GitMetadataFile
    ) -> tuple[int, WorkspaceDirectoryIdentity]:
        self._check_materialization()
        native = self._native()
        handle = native.CreateFileW(target, 0xC0000000, 1, None, 1, 0x00200080, None)
        if type(handle) is not int or not 0 < handle < _INVALID_HANDLE:
            raise WorkspaceUnavailable(_FAILURE)
        self._handles.append(handle)
        identity = self._verify(handle, target, directory=False)
        for offset in range(0, len(item.content), 64 * 1024):
            self._check_materialization()
            chunk = item.content[offset : offset + 64 * 1024]
            buffer, written = ctypes.create_string_buffer(chunk), _DWORD()
            if not native.WriteFile(
                handle, buffer, len(chunk), ctypes.byref(written), None
            ) or written.value != len(chunk):
                raise WorkspaceUnavailable(_FAILURE)
            self._check_materialization()
        self._check_materialization()
        if not native.FlushFileBuffers(handle):
            raise WorkspaceUnavailable(_FAILURE)
        self._verify_file_bytes(handle, target, item, identity)
        return handle, identity

    def _verify_file_bytes(
        self,
        handle: int,
        target: str,
        item: BaseSourceFile | GitMetadataFile,
        identity: WorkspaceDirectoryIdentity,
    ) -> None:
        self._check_materialization()
        native, information = self._native(), _FileInformation()
        if not native.GetFileInformationByHandle(handle, ctypes.byref(information)) or (
            (information.nFileSizeHigh << 32) | information.nFileSizeLow
        ) != len(item.content):
            raise WorkspaceUnavailable(_FAILURE)
        if not native.SetFilePointerEx(handle, 0, None, 0):
            raise WorkspaceUnavailable(_FAILURE)
        digest = hashlib.sha256()
        for offset in range(0, len(item.content), 64 * 1024):
            self._check_materialization()
            expected = item.content[offset : offset + 64 * 1024]
            buffer, read = ctypes.create_string_buffer(len(expected)), _DWORD()
            if not native.ReadFile(
                handle, buffer, len(expected), ctypes.byref(read), None
            ) or read.value != len(expected):
                raise WorkspaceUnavailable(_FAILURE)
            actual = buffer.raw
            if actual != expected:
                raise WorkspaceUnavailable(_FAILURE)
            digest.update(actual)
        buffer, read = ctypes.create_string_buffer(1), _DWORD()
        self._check_materialization()
        if (
            not native.ReadFile(handle, buffer, 1, ctypes.byref(read), None)
            or read.value != 0
        ):
            raise WorkspaceUnavailable(_FAILURE)
        if (
            digest.hexdigest() != hashlib.sha256(item.content).hexdigest()
            or self._verify(handle, target, directory=False) != identity
        ):
            raise WorkspaceUnavailable(_FAILURE)
        self._check_materialization()

    def _verify_tree(self, target: str, nodes: dict[str, bool]) -> None:
        native = self._native()
        directories = [""] + sorted(
            path for path, directory in nodes.items() if directory
        )
        observed: dict[str, bool] = {}
        for directory in directories:
            self._check_materialization()
            prefix = directory + "/" if directory else ""
            expected = {
                path: kind
                for path, kind in nodes.items()
                if path.startswith(prefix) and "/" not in path[len(prefix) :]
            }
            data = _FindData()
            absolute = target + (
                "\\" + directory.replace("/", "\\") if directory else ""
            )
            handle = native.FindFirstFileW(absolute + "\\*", ctypes.byref(data))
            if handle == _INVALID_HANDLE:
                if native.GetLastError() != 2 or expected:
                    raise WorkspaceUnavailable(_FAILURE)
                continue
            if type(handle) is not int or not 0 < handle < _INVALID_HANDLE:
                raise WorkspaceUnavailable(_FAILURE)
            self._find_handles.append(handle)
            names: set[str] = set()
            for _ in range(len(expected) + 3):
                self._check_materialization()
                units = list(data.cFileName)
                if 0 not in units:
                    raise WorkspaceUnavailable(_FAILURE)
                raw = bytes(data.cFileName)[: units.index(0) * 2]
                name = raw.decode("utf-16-le")
                if name in names:
                    raise WorkspaceUnavailable(_FAILURE)
                names.add(name)
                if name not in (".", ".."):
                    path = prefix + name
                    kind = bool(data.dwFileAttributes & 0x10)
                    if (
                        data.dwFileAttributes & 0x400
                        or path not in expected
                        or kind != expected[path]
                    ):
                        raise WorkspaceUnavailable(_FAILURE)
                    observed[path] = kind
                if not native.FindNextFileW(handle, ctypes.byref(data)):
                    if native.GetLastError() != 18:
                        raise WorkspaceUnavailable(_FAILURE)
                    if not native.FindClose(handle):
                        raise WorkspaceUnavailable(_FAILURE)
                    self._find_handles.remove(handle)
                    break
            else:
                raise WorkspaceUnavailable(_FAILURE)
        if observed != nodes:
            raise WorkspaceUnavailable(_FAILURE)

    def materialize(
        self, reservation: WorkspaceReservation, snapshot: BaseSourceSnapshot
    ) -> BaseMaterialization:
        with self._lock:
            if self._materialization_used:
                raise WorkspaceUnavailable(_FAILURE)
            self._materialization_used = True
            try:
                self._check_materialization()
                if not isinstance(snapshot, BaseSourceSnapshot):
                    raise WorkspaceUnavailable(_FAILURE)
                snapshot.validate()
                self._binding.__post_init__()
                target = self._policy.root_directory + "\\" + reservation.directory_name
                nodes: dict[str, bool] = {}
                for item in snapshot.files:
                    parts = item.path.split("/")
                    for index in range(1, len(parts) + 1):
                        relative = "/".join(parts[:index])
                        if (
                            len(
                                (target + "\\" + relative.replace("/", "\\")).encode(
                                    "utf-16-le"
                                )
                            )
                            // 2
                            >= 260
                        ):
                            raise WorkspaceUnavailable(_FAILURE)
                        nodes[relative] = index < len(parts)
                if any(
                    check.working_directory != "."
                    and not nodes.get(check.working_directory, False)
                    for check in self._binding.definition.checks
                ):
                    raise WorkspaceUnavailable(_FAILURE)
                result = BaseMaterialization(
                    reservation=reservation,
                    source=snapshot.request,
                    root_tree_sha=snapshot.root_tree_sha,
                    manifest_digest=snapshot.fingerprint(),
                    file_count=len(snapshot.files),
                    total_bytes=sum(len(item.content) for item in snapshot.files),
                )
                self.verify_reservation(reservation)
                self._bind_materialization()
                retained_count = len(self._handles)
                for relative in sorted(
                    (path for path, directory in nodes.items() if directory),
                    key=lambda path: (path.count("/"), path),
                ):
                    self._check_materialization()
                    path = target + "\\" + relative.replace("/", "\\")
                    if not self._native().CreateDirectoryW(path, None):
                        raise WorkspaceUnavailable(_FAILURE)
                    handle, identity = self._open_directory(path)
                    self._held.append((path, handle, identity))
                files: list[
                    tuple[str, int, WorkspaceDirectoryIdentity, BaseSourceFile]
                ] = []
                for item in snapshot.files:
                    self._check_materialization()
                    path = target + "\\" + item.path.replace("/", "\\")
                    handle, identity = self._write_file(path, item)
                    files.append((path, handle, identity, item))
                self._recheck_root()
                self._verify_tree(target, nodes)
                for path, handle, identity, item in files:
                    self._verify_file_bytes(handle, path, item, identity)
                self._recheck_root()
                self._check_materialization()
                self._materialization_snapshot = snapshot
                self._materialization_files = tuple(files)
                self._materialization_nodes = nodes
                self._materialization_digest = result.manifest_digest
                self._materialization_handles = tuple(self._handles[retained_count:])
                self._materialization = result
                return result
            except Exception:
                self.seal_materialization()
                raise WorkspaceUnavailable(_FAILURE) from None

    def _verify_materialized_base(self, result: BaseMaterialization) -> None:
        self._check_materialization()
        snapshot = self._materialization_snapshot
        if (
            not isinstance(result, BaseMaterialization)
            or result is not self._materialization
            or result.reservation is not self._reservation
            or self._location is None
            or snapshot is None
            or self._find_handles
        ):
            raise WorkspaceUnavailable(_FAILURE)
        result.__post_init__()
        self._binding.__post_init__()
        snapshot.validate()
        if (
            snapshot.fingerprint() != self._materialization_digest
            or result.manifest_digest != self._materialization_digest
            or result.source != snapshot.request
            or result.root_tree_sha != snapshot.root_tree_sha
            or result.file_count != len(snapshot.files)
            or result.total_bytes != sum(len(item.content) for item in snapshot.files)
        ):
            raise WorkspaceUnavailable(_FAILURE)
        if self._git_metadata is not None:
            self._git_metadata.validate()
            if (
                self._git_metadata.fingerprint() != self._git_metadata_digest
                or self._git_preparation is None
                or self._git_preparation.materialization is not result
                or self._git_preparation.branch
                != self._binding.definition.authorized_branch
                or self._git_preparation.metadata_digest != self._git_metadata_digest
            ):
                raise WorkspaceUnavailable(_FAILURE)
        target = self._policy.root_directory + "\\" + result.reservation.directory_name
        self._recheck_root()
        self._verify_tree(target, self._materialization_nodes)
        for path, handle, identity, item in self._materialization_files:
            self._verify_file_bytes(handle, path, item, identity)
        self._recheck_root()
        self._check_materialization()

    def prepare_git_metadata(
        self,
        source: GitBaseSourceSnapshot,
        result: BaseMaterialization,
        branch: str,
    ) -> GitWorkspacePreparation:
        """Create genuine shallow metadata inside this exact retained live scope."""
        with self._lock:
            if self._git_metadata_used or self._handoff_used:
                raise WorkspaceUnavailable(_FAILURE)
            self._git_metadata_used = True
            try:
                self._verify_materialized_base(result)
                if (
                    not isinstance(source, GitBaseSourceSnapshot)
                    or branch != self._binding.definition.authorized_branch
                ):
                    raise WorkspaceUnavailable(_FAILURE)
                source.validate()
                if source.snapshot.fingerprint() != self._materialization_digest:
                    raise WorkspaceUnavailable(_FAILURE)
                metadata = build_git_metadata(source, branch)
                digest = metadata.fingerprint()
                target = (
                    self._policy.root_directory
                    + "\\"
                    + result.reservation.directory_name
                )
                nodes = dict(self._materialization_nodes)
                nodes.update((path, True) for path in metadata.directories)
                nodes.update((item.path, False) for item in metadata.files)
                for path in (
                    *metadata.directories,
                    *(item.path for item in metadata.files),
                ):
                    if (
                        len(
                            (target + "\\" + path.replace("/", "\\")).encode(
                                "utf-16-le"
                            )
                        )
                        // 2
                        >= 260
                    ):
                        raise WorkspaceUnavailable(_FAILURE)
                preparation = GitWorkspacePreparation(
                    materialization=result, branch=branch, metadata_digest=digest
                )
                self._verify_materialized_base(result)
                retained_count = len(self._handles)
                for relative in sorted(
                    metadata.directories, key=lambda path: (path.count("/"), path)
                ):
                    self._check_materialization()
                    path = target + "\\" + relative.replace("/", "\\")
                    if not self._native().CreateDirectoryW(path, None):
                        raise WorkspaceUnavailable(_FAILURE)
                    handle, identity = self._open_directory(path)
                    self._held.append((path, handle, identity))
                files = list(self._materialization_files)
                for item in metadata.files:
                    self._check_materialization()
                    path = target + "\\" + item.path.replace("/", "\\")
                    handle, identity = self._write_file(path, item)
                    files.append((path, handle, identity, item))
                self._materialization_files = tuple(files)
                self._materialization_nodes = nodes
                self._materialization_handles += tuple(self._handles[retained_count:])
                self._git_metadata = metadata
                self._git_metadata_digest = digest
                self._git_preparation = preparation
                self._verify_materialized_base(result)
                return preparation
            except Exception:
                self.seal_materialization()
                raise WorkspaceUnavailable(_FAILURE) from None

    def handoff_materialized_base(
        self, result: BaseMaterialization
    ) -> EditableWorkspace:
        """Release descendant guards once, retaining the reserved UUID root.

        The returned object is same-process evidence only. Releasing guards makes
        previous byte observations historical; it grants no process launch,
        isolation, Git, restart adoption or capacity-release authority.
        """
        with self._lock:
            if self._handoff_used:
                raise WorkspaceUnavailable(_FAILURE)
            self._handoff_used = True
            try:
                self._verify_materialized_base(result)
                if self._location is None:
                    raise WorkspaceUnavailable(_FAILURE)
                target = (
                    self._policy.root_directory
                    + "\\"
                    + result.reservation.directory_name
                )
                handles = self._materialization_handles
                if (
                    len(handles) != len(self._materialization_nodes)
                    or len(set(handles)) != len(handles)
                    or any(handle not in self._handles for handle in handles)
                    or any(
                        path == target and handle in handles
                        for path, handle, _ in self._held
                    )
                ):
                    raise WorkspaceUnavailable(_FAILURE)
                handoff = EditableWorkspace(
                    materialization=result, location=self._location
                )
                for handle in reversed(handles):
                    self._check_materialization()
                    if not self._native().CloseHandle(handle):
                        raise WorkspaceUnavailable(_FAILURE)
                    self._handles.remove(handle)
                    self._held = [entry for entry in self._held if entry[1] != handle]
                    self._check_materialization()
                self._recheck_root()
                self._check_materialization()
                self._materialization_snapshot = None
                self._materialization_files = ()
                self._materialization_nodes = {}
                self._materialization_digest = None
                self._materialization_handles = ()
                self._git_metadata = None
                self._git_metadata_digest = None
                return handoff
            except Exception:
                self.seal_materialization()
                raise WorkspaceUnavailable(_FAILURE) from None

    def close(self) -> None:
        self.seal_materialization()
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
