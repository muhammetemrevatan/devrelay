"""Freeze exact live Windows files and freshly generated candidate Git metadata.

Directory guards prevent rename, not new children. The caller must first verify
coder settlement and use a separately enforced read-only check profile. These
observations never attest process containment, lease authority or restart rights.
"""

import ctypes
from collections.abc import Iterable
from dataclasses import dataclass
from uuid import UUID

from devrelay.application.contracts.base_sources import (
    BaseFileMode,
    BaseSourceFile,
    canonical_base_path,
    git_blob_sha,
)
from devrelay.application.contracts.candidates import CandidateCommitSpec
from devrelay.application.contracts.editable_workspaces import EditableWorkspace
from devrelay.application.contracts.frozen_candidates import FrozenCandidate
from devrelay.application.contracts.git_sources import (
    GitBaseSourceSnapshot,
    GitWorkspacePreparation,
)
from devrelay.application.contracts.workspaces import WorkspaceDirectoryIdentity
from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from devrelay.infrastructure.workspace.candidate_objects import (
    build_candidate_metadata,
    build_candidate_snapshot,
)
from devrelay.infrastructure.workspace.git_metadata import GitMetadataFile
from devrelay.infrastructure.workspace.windows import (
    _BOOL,
    _DWORD,
    _FAILURE,
    _HANDLE,
    _INVALID_HANDLE,
    WindowsWorkspaceReservation,
    _FileInformation,
    _FindData,
)

_MAX_ENTRIES = 2048
_MAX_FILES = 512
_MAX_FILE_BYTES = 2 * 1024**2
_MAX_TOTAL_BYTES = 8 * 1024**2


class _StreamData(ctypes.Structure):
    _fields_ = [("StreamSize", ctypes.c_int64), ("cStreamName", ctypes.c_uint16 * 296)]


@dataclass(frozen=True, slots=True)
class _GuardedFile:
    path: str
    handle: int
    identity: WorkspaceDirectoryIdentity
    item: BaseSourceFile | GitMetadataFile


def _name(units: Iterable[int]) -> str:
    values = list(units)
    if 0 not in values:
        raise WorkspaceUnavailable(_FAILURE)
    return b"".join(
        int(value).to_bytes(2, "little") for value in values[: values.index(0)]
    ).decode("utf-16-le")


class WindowsCandidateWorkspace:
    """One opt-in capability over the original reservation and bounded cycles.

    Unlike ordinary editable handoff, this explicit pre-handoff consumer retains
    bounded original source bytes to reconstruct parent objects without reading
    coder-controlled Git metadata. It clears those bytes when its owner closes.
    """

    def __init__(
        self, owner: WindowsWorkspaceReservation, source: GitBaseSourceSnapshot
    ) -> None:
        self._owner = owner
        self._source: GitBaseSourceSnapshot | None = None
        self._source_digest = ""
        self._frozen: FrozenCandidate | None = None
        self._fingerprint = ""
        self._capture_identity: tuple[UUID, str, WorkspaceDirectoryIdentity] | None = (
            None
        )
        self._files: tuple[_GuardedFile, ...] = ()
        self._nodes: dict[str, bool] = {}
        self._metadata_nodes: dict[str, bool] = {}
        self._cycle_handles: list[int] = []
        self._capture_ids: set[UUID] = set()
        self._next_cycle = 0
        self._capture_started = False
        self._parent: tuple[int, WorkspaceDirectoryIdentity] | None = None
        with owner._lock:
            if owner._candidate_capture_used:
                raise WorkspaceUnavailable(_FAILURE)
            owner._candidate_capture_used = True
            owner._candidate_capture = self
            try:
                owner._check_materialization()
                if (
                    not isinstance(source, GitBaseSourceSnapshot)
                    or owner._handoff_used
                    or owner.materialization is None
                    or owner.git_preparation is None
                    or source.snapshot is not owner._materialization_snapshot
                ):
                    raise WorkspaceUnavailable(_FAILURE)
                source.validate()
                if (
                    len(source.snapshot.files) > _MAX_FILES
                    or any(
                        len(item.content) > _MAX_FILE_BYTES
                        for item in source.snapshot.files
                    )
                    or sum(len(item.content) for item in source.snapshot.files)
                    > _MAX_TOTAL_BYTES
                    or source.snapshot.fingerprint()
                    != owner.materialization.manifest_digest
                ):
                    raise WorkspaceUnavailable(_FAILURE)
                owner._verify_materialized_base(owner.materialization)
                self._worktree = (
                    owner._policy.root_directory
                    + "\\"
                    + owner.materialization.reservation.directory_name
                )
                self._git_path = self._worktree + "\\.git"
                original = next(
                    identity
                    for path, _, identity in owner._held
                    if path == self._git_path
                )
                handle, identity = owner._open_directory(self._git_path)
                owner._held.append((self._git_path, handle, identity))
                if identity != original:
                    raise WorkspaceUnavailable(_FAILURE)
                self._git_guard = (handle, identity)
                self._bind_streams()
                self._source, self._source_digest = (
                    source,
                    source.snapshot.fingerprint(),
                )
            except Exception:
                owner.seal_materialization()
                raise WorkspaceUnavailable(_FAILURE) from None

    def clear_source(self) -> None:
        self._source = None
        self._files = ()

    def _bind_streams(self) -> None:
        for name, args, result in (
            (
                "FindFirstStreamW",
                [ctypes.c_wchar_p, _DWORD, ctypes.POINTER(_StreamData), _DWORD],
                _HANDLE,
            ),
            ("FindNextStreamW", [_HANDLE, ctypes.POINTER(_StreamData)], _BOOL),
        ):
            function = getattr(self._owner._native(), name)
            function.argtypes, function.restype = args, result

    def _streams(self, path: str, *, directory: bool = False) -> None:
        owner, data = self._owner, _StreamData()
        owner._check_materialization()
        handle = owner._native().FindFirstStreamW(path, 0, ctypes.byref(data), 0)
        if (
            handle == _INVALID_HANDLE
            and directory
            and owner._native().GetLastError() == 38
        ):
            return
        if type(handle) is not int or not 0 < handle < _INVALID_HANDLE:
            raise WorkspaceUnavailable(_FAILURE)
        owner._find_handles.append(handle)
        if directory or _name(data.cStreamName) != "::$DATA":
            raise WorkspaceUnavailable(_FAILURE)
        if (
            owner._native().FindNextStreamW(handle, ctypes.byref(data))
            or owner._native().GetLastError() != 38
        ):
            raise WorkspaceUnavailable(_FAILURE)
        if not owner._native().FindClose(handle):
            raise WorkspaceUnavailable(_FAILURE)
        owner._find_handles.remove(handle)
        owner._check_materialization()

    def _live(
        self, preparation: GitWorkspacePreparation, editable: EditableWorkspace
    ) -> GitBaseSourceSnapshot:
        owner, source = self._owner, self._source
        owner._check_materialization()
        if (
            owner._candidate_capture is not self
            or preparation is not owner.git_preparation
            or editable is not owner.editable_workspace
            or not isinstance(editable, EditableWorkspace)
            or editable.materialization is not owner.materialization
            or preparation.materialization is not editable.materialization
            or source is None
            or source.snapshot.fingerprint() != self._source_digest
        ):
            raise WorkspaceUnavailable(_FAILURE)
        owner._recheck_root()
        owner.binding.__post_init__()
        preparation.__post_init__()
        editable.__post_init__()
        source.validate()
        handle, identity = self._git_guard
        if owner._verify(handle, self._git_path) != identity:
            raise WorkspaceUnavailable(_FAILURE)
        return source

    def _enumerate(self) -> dict[str, bool]:
        owner = self._owner
        observed: dict[str, bool] = {}
        pending = [""]
        while pending:
            relative = pending.pop()
            absolute = self._worktree + (
                "\\" + relative.replace("/", "\\") if relative else ""
            )
            data = _FindData()
            owner._check_materialization()
            handle = owner._native().FindFirstFileW(
                absolute + "\\*", ctypes.byref(data)
            )
            if handle == _INVALID_HANDLE:
                if owner._native().GetLastError() != 2:
                    raise WorkspaceUnavailable(_FAILURE)
                continue
            if type(handle) is not int or not 0 < handle < _INVALID_HANDLE:
                raise WorkspaceUnavailable(_FAILURE)
            owner._find_handles.append(handle)
            names: set[str] = set()
            for _ in range(_MAX_ENTRIES + 3):
                owner._check_materialization()
                name = _name(data.cFileName)
                if name in names:
                    raise WorkspaceUnavailable(_FAILURE)
                names.add(name)
                if name not in (".", ".."):
                    path = relative + "/" + name if relative else name
                    if not relative and name == ".git":
                        if (
                            not data.dwFileAttributes & 0x10
                            or data.dwFileAttributes & 0x400
                        ):
                            raise WorkspaceUnavailable(_FAILURE)
                    else:
                        canonical_base_path(path)
                        if (
                            len(observed) >= _MAX_ENTRIES
                            or data.dwFileAttributes & 0x400
                            or path.casefold() in {item.casefold() for item in observed}
                        ):
                            raise WorkspaceUnavailable(_FAILURE)
                        directory = bool(data.dwFileAttributes & 0x10)
                        observed[path] = directory
                        if directory:
                            pending.append(path)
                if not owner._native().FindNextFileW(handle, ctypes.byref(data)):
                    if (
                        owner._native().GetLastError() != 18
                        or not owner._native().FindClose(handle)
                    ):
                        raise WorkspaceUnavailable(_FAILURE)
                    owner._find_handles.remove(handle)
                    break
            else:
                raise WorkspaceUnavailable(_FAILURE)
        return observed

    def _identity(
        self, handle: int, path: str, *, directory: bool
    ) -> WorkspaceDirectoryIdentity:
        identity = self._owner._verify(handle, path, directory=directory)
        final = ctypes.create_unicode_buffer(261)
        count = self._owner._native().GetFinalPathNameByHandleW(
            handle, final, len(final), 0
        )
        if (
            not 0 < count < len(final)
            or final.value[4 + len(self._worktree) :] != path[len(self._worktree) :]
        ):
            raise WorkspaceUnavailable(_FAILURE)
        return identity

    def _read_file(
        self, path: str, *, maximum: int = _MAX_FILE_BYTES
    ) -> tuple[int, WorkspaceDirectoryIdentity, bytes]:
        owner, native = self._owner, self._owner._native()
        owner._check_materialization()
        if len(path.encode("utf-16-le")) // 2 >= 260:
            raise WorkspaceUnavailable(_FAILURE)
        handle = native.CreateFileW(path, 0x80000000, 1, None, 3, 0x00200080, None)
        if type(handle) is not int or not 0 < handle < _INVALID_HANDLE:
            raise WorkspaceUnavailable(_FAILURE)
        owner._handles.append(handle)
        self._cycle_handles.append(handle)
        identity = self._identity(handle, path, directory=False)
        self._streams(path)
        information = _FileInformation()
        if not native.GetFileInformationByHandle(handle, ctypes.byref(information)):
            raise WorkspaceUnavailable(_FAILURE)
        size = (information.nFileSizeHigh << 32) | information.nFileSizeLow
        if size > maximum:
            raise WorkspaceUnavailable(_FAILURE)
        content = bytearray()
        for _ in range(size // 65536 + 2):
            owner._check_materialization()
            count = min(65536, size - len(content) + 1)
            buffer, received = ctypes.create_string_buffer(count), _DWORD()
            if (
                not native.ReadFile(handle, buffer, count, ctypes.byref(received), None)
                or received.value > count
            ):
                raise WorkspaceUnavailable(_FAILURE)
            content.extend(buffer.raw[: received.value])
            if len(content) > size:
                raise WorkspaceUnavailable(_FAILURE)
            if received.value == 0:
                if (
                    len(content) != size
                    or self._identity(handle, path, directory=False) != identity
                ):
                    raise WorkspaceUnavailable(_FAILURE)
                return handle, identity, bytes(content)
        raise WorkspaceUnavailable(_FAILURE)

    def capture(
        self,
        preparation: GitWorkspacePreparation,
        editable: EditableWorkspace,
        *,
        capture_id: UUID,
        cycle: int,
        commit: CandidateCommitSpec,
    ) -> FrozenCandidate:
        owner = self._owner
        with owner._lock:
            if self._capture_started or self._frozen is not None:
                raise WorkspaceUnavailable(_FAILURE)
            self._capture_started = True
            try:
                source = self._live(preparation, editable)
                if (
                    not isinstance(capture_id, UUID)
                    or capture_id in self._capture_ids
                    or type(cycle) is not int
                    or cycle != self._next_cycle
                    or not 0 <= cycle <= 5
                    or cycle > owner.binding.definition.max_repair_cycles
                ):
                    raise WorkspaceUnavailable(_FAILURE)
                self._capture_ids.add(capture_id)
                nodes = self._enumerate()
                if sum(not kind for kind in nodes.values()) > _MAX_FILES:
                    raise WorkspaceUnavailable(_FAILURE)
                for relative in sorted(
                    (path for path, directory in nodes.items() if directory),
                    key=lambda path: (path.count("/"), path),
                ):
                    path = self._worktree + "\\" + relative.replace("/", "\\")
                    handle, identity = owner._open_directory(path)
                    self._cycle_handles.append(handle)
                    owner._held.append((path, handle, identity))
                    self._streams(path, directory=True)
                modes = {item.path: item.mode for item in source.snapshot.files}
                files: list[BaseSourceFile] = []
                guards: list[_GuardedFile] = []
                identities: set[WorkspaceDirectoryIdentity] = set()
                total = 0
                for relative in sorted(
                    path for path, directory in nodes.items() if not directory
                ):
                    path = self._worktree + "\\" + relative.replace("/", "\\")
                    handle, identity, content = self._read_file(path)
                    total += len(content)
                    if total > _MAX_TOTAL_BYTES or identity in identities:
                        raise WorkspaceUnavailable(_FAILURE)
                    identities.add(identity)
                    item = BaseSourceFile(
                        path=relative,
                        blob_sha=git_blob_sha(content),
                        mode=modes.get(relative, BaseFileMode.REGULAR),
                        content=content,
                    )
                    files.append(item)
                    guards.append(_GuardedFile(path, handle, identity, item))
                candidate = build_candidate_snapshot(
                    execution_id=owner.binding.execution.intent.execution_id,
                    cycle=cycle,
                    source=source,
                    branch=preparation.branch,
                    files=tuple(files),
                    commit=commit,
                )
                parent = self._git_path + "\\devrelay"
                if self._parent is None:
                    if not owner._native().CreateDirectoryW(parent, None):
                        raise WorkspaceUnavailable(_FAILURE)
                    handle, identity = owner._open_directory(parent)
                    owner._held.append((parent, handle, identity))
                    self._parent = handle, identity
                elif owner._verify(self._parent[0], parent) != self._parent[1]:
                    raise WorkspaceUnavailable(_FAILURE)
                git_directory = parent + "\\" + str(capture_id)
                metadata = build_candidate_metadata(
                    source,
                    candidate,
                    git_directory=git_directory,
                    worktree=self._worktree,
                )
                metadata_nodes: dict[str, bool] = {}
                git_identity: WorkspaceDirectoryIdentity | None = None
                for relative in sorted(
                    metadata.directories, key=lambda path: (path.count("/"), path)
                ):
                    path = git_directory + relative.removeprefix(".git").replace(
                        "/", "\\"
                    )
                    if len(
                        path.encode("utf-16-le")
                    ) // 2 >= 260 or not owner._native().CreateDirectoryW(path, None):
                        raise WorkspaceUnavailable(_FAILURE)
                    handle, identity = owner._open_directory(path)
                    self._cycle_handles.append(handle)
                    owner._held.append((path, handle, identity))
                    if relative == ".git":
                        git_identity = identity
                    else:
                        metadata_nodes[relative.removeprefix(".git/")] = True
                for metadata_item in metadata.files:
                    relative = metadata_item.path.removeprefix(".git/")
                    path = git_directory + "\\" + relative.replace("/", "\\")
                    if len(path.encode("utf-16-le")) // 2 >= 260:
                        raise WorkspaceUnavailable(_FAILURE)
                    handle, identity = owner._write_file(path, metadata_item)
                    self._cycle_handles.append(handle)
                    if not owner._native().CloseHandle(handle):
                        raise WorkspaceUnavailable(_FAILURE)
                    owner._handles.remove(handle)
                    self._cycle_handles.remove(handle)
                    retained, observed, content = self._read_file(
                        path, maximum=len(metadata_item.content)
                    )
                    if observed != identity or content != metadata_item.content:
                        raise WorkspaceUnavailable(_FAILURE)
                    guards.append(_GuardedFile(path, retained, observed, metadata_item))
                    metadata_nodes[relative] = False
                if git_identity is None:
                    raise WorkspaceUnavailable(_FAILURE)
                result = FrozenCandidate(
                    capture_id=capture_id,
                    editable=editable,
                    preparation=preparation,
                    snapshot=candidate,
                    git_directory=git_directory,
                    git_identity=git_identity,
                )
                self._files, self._nodes, self._metadata_nodes = (
                    tuple(guards),
                    nodes,
                    metadata_nodes,
                )
                self._frozen, self._fingerprint = result, candidate.fingerprint()
                self._capture_identity = capture_id, git_directory, git_identity
                self.verify(result)
                return result
            except Exception:
                owner.seal_materialization()
                raise WorkspaceUnavailable(_FAILURE) from None

    def verify(self, frozen: FrozenCandidate) -> None:
        owner = self._owner
        with owner._lock:
            try:
                if frozen is not self._frozen:
                    raise WorkspaceUnavailable(_FAILURE)
                frozen.__post_init__()
                self._live(frozen.preparation, frozen.editable)
                if (
                    self._capture_identity
                    != (frozen.capture_id, frozen.git_directory, frozen.git_identity)
                    or frozen.snapshot.fingerprint() != self._fingerprint
                    or self._enumerate() != self._nodes
                ):
                    raise WorkspaceUnavailable(_FAILURE)
                owner._verify_tree(frozen.git_directory, self._metadata_nodes)
                for guarded in self._files:
                    if (
                        self._identity(guarded.handle, guarded.path, directory=False)
                        != guarded.identity
                    ):
                        raise WorkspaceUnavailable(_FAILURE)
                    owner._verify_file_bytes(
                        guarded.handle, guarded.path, guarded.item, guarded.identity
                    )
                    self._streams(guarded.path)
                owner._recheck_root()
                for path, handle, _ in owner._held:
                    if path == self._worktree or path.startswith(self._worktree + "\\"):
                        self._identity(handle, path, directory=True)
                        self._streams(path, directory=True)
            except Exception:
                owner.seal_materialization()
                raise WorkspaceUnavailable(_FAILURE) from None

    def release_for_repair(self, frozen: FrozenCandidate) -> EditableWorkspace:
        owner = self._owner
        with owner._lock:
            try:
                self.verify(frozen)
                if frozen.snapshot.cycle >= owner.binding.definition.max_repair_cycles:
                    raise WorkspaceUnavailable(_FAILURE)
                for handle in reversed(tuple(self._cycle_handles)):
                    owner._check_materialization()
                    if not owner._native().CloseHandle(handle):
                        raise WorkspaceUnavailable(_FAILURE)
                    owner._handles.remove(handle)
                    owner._held = [entry for entry in owner._held if entry[1] != handle]
                    self._cycle_handles.remove(handle)
                owner._recheck_root()
                owner._check_materialization()
                self._files, self._nodes, self._metadata_nodes = (), {}, {}
                self._frozen = None
                self._capture_started = False
                self._next_cycle += 1
                return frozen.editable
            except Exception:
                owner.seal_materialization()
                raise WorkspaceUnavailable(_FAILURE) from None
