import ctypes
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Event
from typing import Any

import pytest

from devrelay.application.contracts.workspaces import WorkspaceReservation
from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from devrelay.infrastructure.workspace import windows
from tests.integration.test_windows_base_materialization import snapshot
from tests.unit.test_windows_workspace import Function
from tests.unit.test_windows_workspace import Kernel as ReservationKernel
from tests.workspace_fixtures import workspace_binding


class Kernel(ReservationKernel):
    def __init__(self) -> None:
        super().__init__()
        self.entries: dict[str, bool] = {}
        self.contents: dict[int, bytes] = {}
        self.positions: dict[int, int] = {}
        self.searches: dict[int, tuple[list[tuple[str, bool]], int]] = {}
        self.writes: list[tuple[int, bytes]] = []
        self.on_write: Callable[[], None] = lambda: None
        self.WriteFile = Function(self.write)
        self.ReadFile = Function(self.read)
        self.FlushFileBuffers = Function(lambda handle: int(self.fail != "flush"))
        self.SetFilePointerEx = Function(self.seek)

    def open(self, path: str, *args: Any) -> int:
        new_file = args[3] == 1
        if new_file and (path in self.entries or self.fail == "create_file"):
            return windows._INVALID_HANDLE
        handle = super().open(path, *args)
        self.entries[path] = not new_file
        if new_file:
            self.contents[handle], self.positions[handle] = b"", 0
        return handle

    def create(self, path: str, security: object) -> int:
        if path in self.entries:
            return 0
        success = super().create(path, security)
        if success:
            self.entries[path] = True
        return success

    def information(self, handle: int, pointer: Any) -> int:
        success = super().information(handle, pointer)
        if handle in self.contents:
            pointer._obj.dwFileAttributes = (
                0x480 if self.damage == "file_reparse" else 0x80
            )
            pointer._obj.nNumberOfLinks = 2 if self.damage == "hardlink" else 1
            pointer._obj.nFileSizeLow = len(self.contents[handle])
            if self.damage == "file_identity" and self.writes:
                pointer._obj.nFileIndexLow += 1
        return success

    def first(self, path: str, pointer: Any) -> int:
        if self.fail == "find_first":
            self.error = 5
            return windows._INVALID_HANDLE
        directory = path[:-2]
        children = [
            (entry[len(directory) + 1 :], kind)
            for entry, kind in self.entries.items()
            if entry.startswith(directory + "\\")
            and "\\" not in entry[len(directory) + 1 :]
        ]
        names = [(".", True), ("..", True)] + sorted(children)
        if self.damage == "extra_entry" and self.writes:
            names.append(("unexpected.txt", False))
        if self.damage == "duplicate_entry" and self.writes:
            names.append(names[-1])
        if self.damage == "case_alias" and self.writes:
            names[-1] = (names[-1][0].upper(), names[-1][1])
        handle = 10000 + len(self.searches)
        self.searches[handle] = (names, 0)
        self.fill(pointer, names[0][0])
        pointer._obj.dwFileAttributes = 0x10
        return handle

    def next(self, handle: int, pointer: Any) -> int:
        if self.fail == "find_next":
            self.error = 5
            return 0
        names, index = self.searches[handle]
        index += 1
        self.searches[handle] = (names, index)
        if index >= len(names):
            self.error = 18
            return 0
        name, kind = names[index]
        self.fill(pointer, name)
        pointer._obj.dwFileAttributes = 0x10 if kind else 0x80
        if (
            self.damage == "enumeration_reparse"
            and self.writes
            and name not in (".", "..")
        ):
            pointer._obj.dwFileAttributes |= 0x400
        return 1

    def write(
        self, handle: int, buffer: Any, count: int, written: Any, overlapped: object
    ) -> int:
        chunk = ctypes.string_at(buffer, count)
        self.writes.append((handle, chunk))
        self.contents[handle] += chunk
        written._obj.value = count - 1 if self.fail == "short_write" else count
        self.on_write()
        return int(self.fail != "write")

    def seek(self, handle: int, offset: int, output: object, method: int) -> int:
        self.positions[handle] = offset
        return int(self.fail != "seek")

    def read(
        self, handle: int, buffer: Any, count: int, received: Any, overlapped: object
    ) -> int:
        offset = self.positions[handle]
        chunk = self.contents[handle][offset : offset + count]
        self.positions[handle] += len(chunk)
        if self.damage == "readback" and chunk:
            chunk = b"!" + chunk[1:]
        ctypes.memmove(buffer, chunk, len(chunk))
        received._obj.value = (
            len(chunk) - 1 if self.fail == "short_read" and chunk else len(chunk)
        )
        return int(self.fail != "read")


@pytest.fixture
def setup(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[windows.WindowsWorkspaceReservation, WorkspaceReservation, Kernel]:
    kernel = Kernel()
    monkeypatch.setattr(windows, "_load_kernel", lambda: kernel)
    resource = windows.WindowsWorkspaceReservation(
        workspace_binding(),
        windows.WindowsWorkspacePolicy(
            root_directory="C:\\coordinator\\workspaces",
            protected_directories=("C:\\private-state",),
        ),
    )
    reservation = resource.reserve()
    return resource, reservation, kernel


def test_complete_manifest_is_staged_with_bounded_chunks_and_exact_evidence(
    setup: tuple[windows.WindowsWorkspaceReservation, WorkspaceReservation, Kernel],
) -> None:
    resource, reservation, kernel = setup
    source = snapshot(
        resource.binding, {"empty.txt": b"", "src/main.py": b"x" * (2 * 64 * 1024 + 1)}
    )

    result = resource.materialize(reservation, source)

    assert result.reservation is reservation
    assert result.manifest_digest == source.fingerprint()
    assert result.file_count == 2 and result.total_bytes == 2 * 64 * 1024 + 1
    assert [len(chunk) for handle, chunk in kernel.writes] == [64 * 1024, 64 * 1024, 1]
    assert kernel.closed == []
    assert all(
        kind != "open"
        or args[1][3] != 1
        or args[1] == (0xC0000000, 1, None, 1, 0x00200080, None)
        for kind, args in kernel.calls
    )


@pytest.mark.parametrize(
    "failure",
    [
        "create_file",
        "write",
        "short_write",
        "flush",
        "seek",
        "read",
        "short_read",
        "find_first",
        "find_next",
        "find_close",
    ],
)
def test_native_file_failure_is_sanitized_and_never_retries_or_deletes(
    setup: tuple[windows.WindowsWorkspaceReservation, WorkspaceReservation, Kernel],
    failure: str,
) -> None:
    resource, reservation, kernel = setup
    source = snapshot(resource.binding, {"file.txt": b"literal data"})
    kernel.fail = failure

    with pytest.raises(WorkspaceUnavailable) as error:
        resource.materialize(reservation, source)

    assert str(error.value) == "Windows workspace reservation is unavailable."
    assert kernel.closed == []
    count = len(kernel.writes)
    with pytest.raises(WorkspaceUnavailable):
        resource.materialize(reservation, source)
    assert len(kernel.writes) == count


@pytest.mark.parametrize(
    "damage",
    [
        "readback",
        "file_reparse",
        "hardlink",
        "file_identity",
        "extra_entry",
        "duplicate_entry",
        "case_alias",
        "enumeration_reparse",
        "identity",
    ],
)
def test_native_readback_identity_and_exact_tree_mismatches_fail_closed(
    setup: tuple[windows.WindowsWorkspaceReservation, WorkspaceReservation, Kernel],
    damage: str,
) -> None:
    resource, reservation, kernel = setup
    source = snapshot(resource.binding, {"file.txt": b"literal data"})
    kernel.damage = damage

    with pytest.raises(WorkspaceUnavailable):
        resource.materialize(reservation, source)

    assert kernel.closed == []


@pytest.mark.parametrize("mutation", ["path", "content", "files", "source"])
def test_manifest_and_source_revalidation_finish_before_any_native_writes(
    setup: tuple[windows.WindowsWorkspaceReservation, WorkspaceReservation, Kernel],
    mutation: str,
) -> None:
    resource, reservation, kernel = setup
    source = snapshot(resource.binding, {"file.txt": b"literal data"})
    if mutation == "path":
        object.__setattr__(source.files[0], "path", ".git/config")
    elif mutation == "content":
        object.__setattr__(source.files[0], "content", b"changed without new Git hash")
    elif mutation == "files":
        object.__setattr__(source, "files", source.files + source.files)
    else:
        source = replace(
            source, request=replace(source.request, repository_slug="other/repo")
        )
    original_directories = list(kernel.created)

    with pytest.raises(WorkspaceUnavailable):
        resource.materialize(reservation, source)

    assert kernel.writes == [] and kernel.created == original_directories


def test_missing_check_working_directory_is_not_created_to_satisfy_policy(
    setup: tuple[windows.WindowsWorkspaceReservation, WorkspaceReservation, Kernel],
) -> None:
    resource, reservation, kernel = setup
    check = resource.binding.definition.checks[0]
    object.__setattr__(check, "working_directory", "missing")
    # Preserve the pin only to isolate the native manifest-directory gate.
    object.__setattr__(
        resource.binding.execution.intent.acknowledgement,
        "definition_digest",
        resource.binding.definition.fingerprint(),
    )
    source = snapshot(resource.binding, {"file.txt": b"literal data"})
    original = list(kernel.created)

    with pytest.raises(WorkspaceUnavailable):
        resource.materialize(reservation, source)

    assert kernel.writes == [] and kernel.created == original


def test_seal_after_first_chunk_stops_before_next_chunk_without_close(
    setup: tuple[windows.WindowsWorkspaceReservation, WorkspaceReservation, Kernel],
) -> None:
    resource, reservation, kernel = setup
    source = snapshot(resource.binding, {"file.txt": b"x" * (64 * 1024 + 1)})
    kernel.on_write = resource.seal_materialization

    with pytest.raises(WorkspaceUnavailable):
        resource.materialize(reservation, source)

    assert len(kernel.writes) == 1 and len(kernel.writes[0][1]) == 64 * 1024
    assert kernel.closed == []


def test_seal_is_nonblocking_during_actual_pending_native_write(
    setup: tuple[windows.WindowsWorkspaceReservation, WorkspaceReservation, Kernel],
) -> None:
    resource, reservation, kernel = setup
    source = snapshot(resource.binding, {"file.txt": b"x" * (64 * 1024 + 1)})
    entered, release = Event(), Event()

    def pending_write() -> None:
        entered.set()
        if not release.wait(5):
            raise RuntimeError("Fixture native write timeout.")

    kernel.on_write = pending_write
    with ThreadPoolExecutor(max_workers=1) as executor:
        operation = executor.submit(resource.materialize, reservation, source)
        assert entered.wait(5)
        try:
            resource.seal_materialization()
            assert kernel.closed == []
        finally:
            release.set()
        with pytest.raises(WorkspaceUnavailable):
            operation.result(timeout=5)

    assert len(kernel.writes) == 1 and kernel.closed == []


def test_verify_fences_exact_live_reservation_without_spending_stage_latch(
    setup: tuple[windows.WindowsWorkspaceReservation, WorkspaceReservation, Kernel],
) -> None:
    resource, reservation, kernel = setup
    source = snapshot(resource.binding, {"file.txt": b"literal data"})
    resource.verify_reservation(reservation)

    result = resource.materialize(reservation, source)

    assert result.reservation is reservation and len(kernel.writes) == 1


def test_equal_copy_cannot_verify_or_write_against_live_native_reservation(
    setup: tuple[windows.WindowsWorkspaceReservation, WorkspaceReservation, Kernel],
) -> None:
    resource, reservation, kernel = setup
    copied = replace(reservation)

    with pytest.raises(WorkspaceUnavailable):
        resource.verify_reservation(copied)

    assert kernel.writes == []


def test_create_new_fences_entry_inserted_after_empty_observation(
    setup: tuple[windows.WindowsWorkspaceReservation, WorkspaceReservation, Kernel],
) -> None:
    resource, reservation, kernel = setup
    source = snapshot(resource.binding, {"file.txt": b"never overwrite"})
    original_open = kernel.open

    def racing_open(path: str, *args: Any) -> int:
        if args[3] == 1:
            kernel.entries[path] = False
        return original_open(path, *args)

    kernel.CreateFileW.body = racing_open

    with pytest.raises(WorkspaceUnavailable):
        resource.materialize(reservation, source)

    assert kernel.writes == [] and kernel.closed == []


def test_empty_manifest_produces_empty_evidence_without_creating_directories(
    setup: tuple[windows.WindowsWorkspaceReservation, WorkspaceReservation, Kernel],
) -> None:
    resource, reservation, kernel = setup
    source = snapshot(resource.binding, {})
    original = list(kernel.created)

    result = resource.materialize(reservation, source)

    assert result.file_count == 0 and result.total_bytes == 0
    assert result.manifest_digest == source.fingerprint()
    assert kernel.writes == [] and kernel.created == original
