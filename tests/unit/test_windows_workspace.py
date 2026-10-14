import ctypes
import sys
from collections.abc import Callable
from dataclasses import replace
from typing import Any

import pytest

from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from devrelay.infrastructure.workspace import windows
from tests.workspace_fixtures import workspace_binding


class Function:
    def __init__(self, body: Callable[..., Any]) -> None:
        self.body = body
        self.argtypes: list[object] = []
        self.restype: object = None

    def __call__(self, *args: Any) -> Any:
        return self.body(*args)


class Kernel:
    def __init__(self) -> None:
        self.paths: dict[int, str] = {}
        self.calls: list[tuple[str, Any]] = []
        self.created: list[str] = []
        self.closed: list[int] = []
        self.search_closed: list[int] = []
        self.fail = ""
        self.damage = ""
        self.created_directory = False
        self.names = [".", ".."]
        self.name_index = 0
        self.error = 0
        self.CreateFileW = Function(self.open)
        self.GetFileInformationByHandle = Function(self.information)
        self.GetFinalPathNameByHandleW = Function(self.final_path)
        self.GetHandleInformation = Function(self.handle_information)
        self.CreateDirectoryW = Function(self.create)
        self.FindFirstFileW = Function(self.first)
        self.FindNextFileW = Function(self.next)
        self.FindClose = Function(self.find_close)
        self.GetLastError = Function(lambda: self.error)
        self.CloseHandle = Function(self.close)

    def open(self, path: str, *args: Any) -> int:
        self.calls.append(("open", (path, args)))
        if self.fail == "open":
            return windows._INVALID_HANDLE
        handle = len(self.paths) + 100
        self.paths[handle] = path
        return handle

    def information(self, handle: int, pointer: Any) -> int:
        pointer._obj.dwFileAttributes = (
            0x410
            if self.damage == "reparse"
            else 0
            if self.damage == "not_directory"
            else 0x10
        )
        pointer._obj.dwVolumeSerialNumber = 42
        pointer._obj.nFileIndexLow = handle + int(
            self.damage == "identity" and self.created_directory
        )
        return int(self.fail != "information")

    def final_path(self, handle: int, output: Any, length: int, flags: int) -> int:
        path = self.paths[handle]
        output.value = "\\\\?\\" + (
            path.replace("workspaces", "alias") if self.damage == "alias" else path
        )
        return (
            0
            if self.fail == "final_path"
            else length
            if self.damage == "oversized_path"
            else len(output.value)
        )

    def handle_information(self, handle: int, output: Any) -> int:
        output._obj.value = int(self.damage == "inherit")
        return int(self.fail != "handle_info")

    def create(self, path: str, security: object) -> int:
        self.calls.append(("create", (path, security)))
        self.created.append(path)
        self.created_directory = self.fail != "create"
        return int(self.created_directory)

    def fill(self, pointer: Any, name: str) -> None:
        for index in range(260):
            pointer._obj.cFileName[index] = ord(name[index]) if index < len(name) else 0

    def first(self, path: str, pointer: Any) -> int:
        self.calls.append(("find_first", path))
        self.name_index = 0
        if self.fail == "find_first" or not self.names:
            self.error = 5 if self.fail else 2
            return windows._INVALID_HANDLE
        self.fill(pointer, self.names[0])
        return 999

    def next(self, handle: int, pointer: Any) -> int:
        self.name_index += 1
        if self.fail == "find_next":
            self.error = 5
            return 0
        if self.name_index >= len(self.names):
            self.error = 18
            return 0
        self.fill(pointer, self.names[self.name_index])
        return 1

    def find_close(self, handle: int) -> int:
        self.search_closed.append(handle)
        return int(self.fail != "find_close")

    def close(self, handle: int) -> int:
        self.closed.append(handle)
        return int(self.fail != "close")


@pytest.fixture
def policy() -> windows.WindowsWorkspacePolicy:
    return windows.WindowsWorkspacePolicy(
        root_directory="C:\\coordinator\\workspaces",
        protected_directories=("C:\\coordinator\\private-state",),
    )


@pytest.fixture
def kernel(monkeypatch: pytest.MonkeyPatch) -> Kernel:
    result = Kernel()
    monkeypatch.setattr(windows, "_load_kernel", lambda: result)
    return result


@pytest.mark.parametrize(
    "path",
    [
        "",
        "C:\\",
        "C:relative",
        "relative",
        "\\\\server\\share\\work",
        "\\\\?\\C:\\work",
        "\\\\.\\C:\\work",
        "C:/work",
        "C:\\work\\",
        "C:\\work\\.\\x",
        "C:\\work\\..\\x",
        "C:\\work:stream",
        "C:\\work\\two..",
        "C:\\work\\space ",
        "C:\\work\\NUL.txt",
        "C:\\work\\com1",
        "C:\\work\\LPT².log",
        "C:\\work\\CONOUT$",
        "C:\\work\\bad*",
        "C:\\work\\bad?",
        "C:\\work\\bad\x00",
        "C:\\work\\bad\x1f",
        "C:\\work\\bad\x7f",
        "C:\\PROGRA~1\\work",
        "C:\\work\\CON .txt",
        "C:\\" + "😀" * 99,
        "C:\\work\\a\\\\b",
        "C:\\" + "a" * 198,
    ],
)
def test_root_policy_rejects_noncanonical_or_unsafe_dos_paths(path: str) -> None:
    with pytest.raises(WorkspaceUnavailable):
        windows.WindowsWorkspacePolicy(
            root_directory=path, protected_directories=("D:\\private",)
        )


@pytest.mark.parametrize(
    "protected",
    [
        ("C:\\",),
        ("C:\\COORDINATOR",),
        ("c:\\coordinator\\WORKSPACES\\nested",),
        ("C:\\coordinator\\workspaces",),
        ("C:\\secret", "c:\\SECRET"),
        (),
        tuple("D:\\private" + str(i) for i in range(17)),
    ],
)
def test_policy_rejects_protected_overlap_duplicates_or_unbounded_scope(
    protected: tuple[str, ...],
) -> None:
    with pytest.raises(WorkspaceUnavailable):
        windows.WindowsWorkspacePolicy(
            root_directory="C:\\coordinator\\workspaces",
            protected_directories=protected,
        )


def test_policy_accepts_adjacent_prefix_and_independent_drive() -> None:
    value = windows.WindowsWorkspacePolicy(
        root_directory="C:\\work",
        protected_directories=("C:\\workspace", "D:\\private"),
    )

    assert value.root_directory == "C:\\work"


def test_prepare_retains_root_without_creating_execution_directory(
    kernel: Kernel, policy: windows.WindowsWorkspacePolicy
) -> None:
    resource = windows.WindowsWorkspaceReservation(workspace_binding(), policy)

    location = resource.prepare()

    assert kernel.created == []
    assert location.root_directory == policy.root_directory
    assert location.identity.file_index == max(kernel.paths)
    assert kernel.closed == []


@pytest.mark.parametrize(
    "field,value", [("root_directory", "C:\\different"), ("policy_digest", "b" * 64)]
)
def test_reserve_rejects_changed_precreation_location(
    kernel: Kernel, policy: windows.WindowsWorkspacePolicy, field: str, value: str
) -> None:
    resource = windows.WindowsWorkspaceReservation(workspace_binding(), policy)
    location = resource.prepare()
    changes: dict[str, Any] = {field: value}
    changed = replace(location, **changes)

    with pytest.raises(WorkspaceUnavailable):
        resource.reserve(changed)

    assert kernel.created == []


def test_reserve_rechecks_root_reparse_state_before_mkdir(
    kernel: Kernel, policy: windows.WindowsWorkspacePolicy
) -> None:
    resource = windows.WindowsWorkspaceReservation(workspace_binding(), policy)
    location = resource.prepare()
    kernel.damage = "reparse"

    with pytest.raises(WorkspaceUnavailable):
        resource.reserve(location)

    assert kernel.created == []


def test_policy_digest_binds_protected_namespace_configuration() -> None:
    original = windows.WindowsWorkspacePolicy(
        root_directory="C:\\work", protected_directories=("C:\\private",)
    )
    changed = replace(original, protected_directories=("C:\\other-private",))

    original_digest, changed_digest = original.fingerprint(), changed.fingerprint()

    assert original_digest != changed_digest


def test_reservation_checks_and_retains_all_ancestors_and_exact_uuid_directory(
    kernel: Kernel, policy: windows.WindowsWorkspacePolicy
) -> None:
    binding = workspace_binding()
    resource = windows.WindowsWorkspaceReservation(binding, policy)

    reservation = resource.reserve()

    target = policy.root_directory + "\\" + str(binding.execution.intent.execution_id)
    assert list(kernel.paths.values()) == [
        "C:\\",
        "C:\\coordinator",
        policy.root_directory,
        target,
    ]
    assert kernel.created == [target]
    assert reservation.binding is binding and resource.binding is binding
    assert reservation.directory_name == str(binding.execution.intent.execution_id)
    assert reservation.identity.volume_serial_number == 42
    assert reservation.identity.file_index == max(kernel.paths)
    assert kernel.closed == [] and kernel.search_closed == [999]
    assert all(
        args[1] == (0, 3, None, 3, 0x02200000, None)
        for operation, args in kernel.calls
        if operation == "open"
    )


@pytest.mark.parametrize(
    "failure",
    [
        "open",
        "information",
        "final_path",
        "handle_info",
        "create",
        "find_first",
        "find_next",
        "find_close",
    ],
)
def test_native_failure_is_sanitized_one_shot_and_closes_own_handles(
    kernel: Kernel, policy: windows.WindowsWorkspacePolicy, failure: str
) -> None:
    kernel.fail = failure
    resource = windows.WindowsWorkspaceReservation(workspace_binding(), policy)

    with pytest.raises(WorkspaceUnavailable) as error:
        resource.reserve()

    assert str(error.value) == "Windows workspace reservation is unavailable."
    assert error.value.__cause__ is None
    assert set(kernel.closed) == set(kernel.paths)
    with pytest.raises(WorkspaceUnavailable):
        resource.reserve()
    assert len(kernel.created) <= 1


@pytest.mark.parametrize(
    "damage",
    ["reparse", "not_directory", "alias", "inherit", "oversized_path", "identity"],
)
def test_reparse_alias_inheritance_and_identity_changes_fail_closed(
    kernel: Kernel, policy: windows.WindowsWorkspacePolicy, damage: str
) -> None:
    kernel.damage = damage
    resource = windows.WindowsWorkspaceReservation(workspace_binding(), policy)

    with pytest.raises(WorkspaceUnavailable):
        resource.reserve()

    assert set(kernel.closed) == set(kernel.paths)
    assert len(kernel.created) <= 1


@pytest.mark.parametrize("names", [["file.txt"], [".", "..", ".git"], [".", "."], []])
def test_empty_observation_is_bounded_and_rejects_existing_entries(
    kernel: Kernel, policy: windows.WindowsWorkspacePolicy, names: list[str]
) -> None:
    kernel.names = names
    resource = windows.WindowsWorkspaceReservation(workspace_binding(), policy)

    if names:
        with pytest.raises(WorkspaceUnavailable):
            resource.reserve()
    else:
        reservation = resource.reserve()
        assert reservation.binding is resource.binding

    assert kernel.name_index <= 2


def test_close_never_deletes_directory_and_is_idempotent(
    kernel: Kernel, policy: windows.WindowsWorkspacePolicy
) -> None:
    resource = windows.WindowsWorkspaceReservation(workspace_binding(), policy)
    resource.reserve()

    resource.close()
    resource.close()

    assert set(kernel.closed) == set(kernel.paths)
    assert len(kernel.closed) == len(kernel.paths)
    assert kernel.created_directory


def test_failed_close_retains_resources_for_cleanup_retry(
    kernel: Kernel, policy: windows.WindowsWorkspacePolicy
) -> None:
    resource = windows.WindowsWorkspaceReservation(workspace_binding(), policy)
    resource.reserve()
    kernel.fail = "close"

    with pytest.raises(WorkspaceUnavailable):
        resource.close()

    kernel.fail = ""
    resource.close()
    assert len(kernel.closed) == 2 * len(kernel.paths)


def test_close_before_reserve_prevents_native_action(
    kernel: Kernel, policy: windows.WindowsWorkspacePolicy
) -> None:
    resource = windows.WindowsWorkspaceReservation(workspace_binding(), policy)
    resource.close()

    with pytest.raises(WorkspaceUnavailable):
        resource.reserve()

    assert kernel.calls == []


def test_constructor_does_not_load_native_api_and_unsupported_platform_has_no_fallback(
    monkeypatch: pytest.MonkeyPatch, policy: windows.WindowsWorkspacePolicy
) -> None:
    calls: list[str] = []
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(
        ctypes, "WinDLL", lambda *args, **kwargs: calls.append("load"), raising=False
    )
    resource = windows.WindowsWorkspaceReservation(workspace_binding(), policy)

    with pytest.raises(WorkspaceUnavailable):
        resource.reserve()

    assert calls == []


def test_native_metadata_structures_use_fixed_windows_widths() -> None:
    sizes = tuple(
        ctypes.sizeof(kind)
        for kind in (
            windows._DWORD,
            windows._FileTime,
            windows._FileInformation,
            windows._FindData,
        )
    )

    assert sizes == (4, 8, 52, 592)
