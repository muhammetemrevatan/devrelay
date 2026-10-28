from collections.abc import Iterator
from dataclasses import dataclass, replace
from typing import Any
from uuid import uuid4

import pytest

from devrelay.application.contracts.editable_workspaces import EditableWorkspace
from devrelay.application.contracts.frozen_candidates import FrozenCandidate
from devrelay.application.contracts.git_sources import (
    GitBaseSourceSnapshot,
    GitWorkspacePreparation,
)
from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from devrelay.infrastructure.workspace import windows
from devrelay.infrastructure.workspace.windows_candidates import (
    WindowsCandidateWorkspace,
)
from tests.unit.test_candidate_objects import commit
from tests.unit.test_windows_base_materialization import Kernel as MaterializationKernel
from tests.unit.test_windows_git_metadata import git_inputs
from tests.unit.test_windows_workspace import Function


class Kernel(MaterializationKernel):
    def __init__(self) -> None:
        super().__init__()
        self.identities: dict[str, int] = {}
        self.capture_failure = ""
        self.FindFirstStreamW = Function(self.first_stream)
        self.FindNextStreamW = Function(self.next_stream)

    def open(self, path: str, *args: Any) -> int:
        if self.capture_failure == "read_open" and args[0] == 0x80000000:
            return windows._INVALID_HANDLE
        existing = self.entries.get(path)
        old = next(
            (
                handle
                for handle, value in self.paths.items()
                if value == path and handle in self.contents
            ),
            None,
        )
        handle = super().open(path, *args)
        if handle == windows._INVALID_HANDLE:
            return handle
        self.identities.setdefault(path, len(self.identities) + 100)
        if existing is not None:
            self.entries[path] = existing
        if old is not None and args[3] != 1:
            self.contents[handle] = self.contents[old]
            self.positions[handle] = 0
        return handle

    def information(self, handle: int, pointer: Any) -> int:
        result = super().information(handle, pointer)
        pointer._obj.nFileIndexLow = self.identities[self.paths[handle]]
        if self.capture_failure == "file_identity" and handle in self.contents:
            pointer._obj.nFileIndexLow += handle
        return result

    def first_stream(self, path: str, level: int, data: Any, flags: int) -> int:
        if self.capture_failure == "streams":
            self.error = 5
            return windows._INVALID_HANDLE
        if self.entries[path]:
            self.error = 38
            return windows._INVALID_HANDLE
        for index, char in enumerate("::$DATA\0"):
            data._obj.cStreamName[index] = ord(char)
        return 50000

    def next_stream(self, handle: int, data: Any) -> int:
        self.error = 38
        return int(self.capture_failure == "extra_stream")

    def change(self, path: str, value: bytes) -> None:
        for handle, selected in self.paths.items():
            if selected == path and handle in self.contents:
                self.contents[handle] = value


@dataclass
class Setup:
    owner: windows.WindowsWorkspaceReservation
    source: GitBaseSourceSnapshot
    helper: WindowsCandidateWorkspace
    preparation: GitWorkspacePreparation
    editable: EditableWorkspace
    kernel: Kernel
    target: str

    def capture(self) -> FrozenCandidate:
        return self.helper.capture(
            self.preparation,
            self.editable,
            capture_id=uuid4(),
            cycle=0,
            commit=commit(),
        )


@pytest.fixture
def setup(monkeypatch: pytest.MonkeyPatch) -> Iterator[Setup]:
    binding, source = git_inputs()
    kernel = Kernel()
    monkeypatch.setattr(windows, "_load_kernel", lambda: kernel)
    owner = windows.WindowsWorkspaceReservation(
        binding,
        windows.WindowsWorkspacePolicy(
            root_directory="C:\\workspaces", protected_directories=("C:\\private",)
        ),
    )
    reservation = owner.reserve()
    materialization = owner.materialize(reservation, source.snapshot)
    preparation = owner.prepare_git_metadata(
        source, materialization, binding.definition.authorized_branch
    )
    helper = owner.prepare_candidate_capture(source)
    editable = owner.handoff_materialized_base(materialization)
    target = "C:\\workspaces\\" + reservation.directory_name
    kernel.change(target + "\\README.md", b"changed fixture source\n")
    try:
        yield Setup(owner, source, helper, preparation, editable, kernel, target)
    finally:
        kernel.fail, kernel.capture_failure, kernel.damage = "", "", ""
        owner.close()


def test_native_capture_uses_readonly_file_handles_and_original_cached_objects(
    setup: Setup,
) -> None:
    frozen = setup.capture()

    assert frozen.preparation is setup.preparation
    assert frozen.editable is setup.editable
    assert setup.owner._materialization_snapshot is None
    assert setup.helper._source is setup.source
    guarded = {item.handle for item in setup.helper._files}
    opens = {
        handle: next(
            args
            for kind, (path, args) in setup.kernel.calls
            if kind == "open"
            and path == setup.kernel.paths[handle]
            and args[0] == 0x80000000
        )
        for handle in guarded
    }
    assert all(
        args == (0x80000000, 1, None, 3, 0x00200080, None) for args in opens.values()
    )


@pytest.mark.parametrize(
    "failure", ["read_open", "streams", "extra_stream", "file_identity"]
)
def test_capture_failure_retains_partial_resources_and_sticky_seal(
    setup: Setup, failure: str
) -> None:
    setup.kernel.capture_failure = failure
    existing = tuple(setup.owner._handles)

    with pytest.raises(WorkspaceUnavailable):
        setup.capture()

    assert setup.owner._materialization_sealed.is_set()
    assert all(handle in setup.owner._handles for handle in existing)
    assert setup.helper._frozen is None


@pytest.mark.parametrize(
    "failure",
    [
        "read",
        "short_read",
        "information",
        "handle_info",
        "final_path",
        "find_next",
        "find_close",
        "create_file",
        "write",
        "flush",
    ],
)
def test_native_operation_failures_never_grant_candidate_or_release(
    setup: Setup, failure: str
) -> None:
    setup.kernel.fail = failure

    with pytest.raises(WorkspaceUnavailable):
        setup.capture()

    assert setup.helper._frozen is None
    assert setup.owner._materialization_sealed.is_set()


@pytest.mark.parametrize(
    "damage",
    [
        "file_reparse",
        "hardlink",
        "enumeration_reparse",
        "duplicate_entry",
        "alias",
        "inherit",
    ],
)
def test_native_hostile_shapes_fail_closed(setup: Setup, damage: str) -> None:
    setup.kernel.damage = damage

    with pytest.raises(WorkspaceUnavailable):
        setup.capture()

    assert setup.owner._materialization_sealed.is_set()


def test_partial_repair_release_keeps_unclosed_handles_and_seals(setup: Setup) -> None:
    frozen = setup.capture()
    original = tuple(setup.helper._cycle_handles)
    underlying = setup.kernel.CloseHandle
    calls = 0

    def close(handle: int) -> int:
        nonlocal calls
        calls += 1
        return 0 if calls == 2 else underlying(handle)

    setup.kernel.CloseHandle = Function(close)

    with pytest.raises(WorkspaceUnavailable):
        setup.helper.release_for_repair(frozen)

    assert original[-1] not in setup.owner._handles
    assert original[-2] in setup.owner._handles
    assert setup.helper._frozen is frozen
    assert setup.owner._materialization_sealed.is_set()


@pytest.mark.parametrize(
    "field", ["capture_id", "git_identity", "git_directory", "snapshot"]
)
def test_mutated_frozen_observation_cannot_redirect_verification(
    setup: Setup, field: str
) -> None:
    frozen = setup.capture()
    value: object
    if field == "capture_id":
        value = uuid4()
    elif field == "git_identity":
        value = replace(
            frozen.git_identity, file_index=frozen.git_identity.file_index + 1
        )
    elif field == "git_directory":
        value = frozen.git_directory + "\\other"
    else:
        value = replace(frozen.snapshot, cycle=1)
    object.__setattr__(frozen, field, value)

    with pytest.raises(WorkspaceUnavailable):
        setup.helper.verify(frozen)

    assert setup.owner._materialization_sealed.is_set()


def test_no_change_candidate_fails_before_creating_private_namespace(
    setup: Setup,
) -> None:
    original = next(
        item.content for item in setup.source.snapshot.files if item.path == "README.md"
    )
    setup.kernel.change(setup.target + "\\README.md", original)

    with pytest.raises(WorkspaceUnavailable):
        setup.capture()

    assert setup.target + "\\.git\\devrelay" not in setup.kernel.entries


def test_successful_owner_close_clears_explicit_original_source_bytes(
    setup: Setup,
) -> None:
    setup.owner.close()

    assert setup.helper._source is None
    assert setup.owner._handles == []


@pytest.mark.parametrize(
    "kind", ["file_count", "file_bytes", "total_bytes", "entry_count", "nested_git"]
)
def test_source_capture_budgets_and_namespace_fail_before_candidate_metadata(
    setup: Setup, kind: str
) -> None:
    if kind == "file_count":
        for index in range(513):
            setup.kernel.entries[setup.target + f"\\added-{index}.txt"] = False
    elif kind == "entry_count":
        for index in range(2049):
            setup.kernel.entries[setup.target + f"\\dir-{index}"] = True
    elif kind == "nested_git":
        setup.kernel.entries[setup.target + "\\src\\.git"] = True
    elif kind == "file_bytes":
        setup.kernel.change(setup.target + "\\README.md", b"x" * (2 * 1024**2 + 1))
    else:
        for index in range(5):
            path = setup.target + f"\\large-{index}.bin"
            handle = setup.kernel.open(path, 0xC0000000, 1, None, 1, 0x00200080, None)
            setup.kernel.contents[handle] = b"x" * (2 * 1024**2)

    with pytest.raises(WorkspaceUnavailable):
        setup.capture()

    assert setup.owner._materialization_sealed.is_set()
    assert setup.target + "\\.git\\devrelay" not in setup.kernel.entries


def test_replayed_equal_frozen_value_has_no_verification_right(setup: Setup) -> None:
    frozen = setup.capture()
    replay = replace(frozen)

    with pytest.raises(WorkspaceUnavailable):
        setup.helper.verify(replay)

    assert setup.owner._materialization_sealed.is_set()


def test_capture_helper_cannot_be_registered_twice(setup: Setup) -> None:
    original = setup.owner._candidate_capture

    with pytest.raises(WorkspaceUnavailable):
        setup.owner.prepare_candidate_capture(setup.source)

    assert setup.owner._candidate_capture is original


def test_final_accepted_cycle_cannot_release_for_an_extra_repair(setup: Setup) -> None:
    maximum = setup.owner.binding.definition.max_repair_cycles
    for cycle in range(maximum + 1):
        setup.kernel.change(
            setup.target + "\\README.md", f"candidate {cycle}\n".encode()
        )
        frozen = setup.helper.capture(
            setup.preparation,
            setup.editable,
            capture_id=uuid4(),
            cycle=cycle,
            commit=commit(),
        )
        if cycle < maximum:
            setup.helper.release_for_repair(frozen)
    handles = tuple(setup.helper._cycle_handles)

    with pytest.raises(WorkspaceUnavailable):
        setup.helper.release_for_repair(frozen)

    assert tuple(setup.helper._cycle_handles) == handles
    assert setup.owner._materialization_sealed.is_set()


@pytest.mark.parametrize("stage", ["equal_source", "after_handoff"])
def test_initialization_requires_exact_original_source_before_handoff(
    monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    binding, source = git_inputs()
    kernel = Kernel()
    monkeypatch.setattr(windows, "_load_kernel", lambda: kernel)
    owner = windows.WindowsWorkspaceReservation(
        binding,
        windows.WindowsWorkspacePolicy(
            root_directory="C:\\workspaces", protected_directories=("C:\\private",)
        ),
    )
    reservation = owner.reserve()
    materialization = owner.materialize(reservation, source.snapshot)
    owner.prepare_git_metadata(
        source, materialization, binding.definition.authorized_branch
    )
    if stage == "equal_source":
        source = replace(source, snapshot=replace(source.snapshot))
    else:
        owner.handoff_materialized_base(materialization)
    try:
        with pytest.raises(WorkspaceUnavailable):
            owner.prepare_candidate_capture(source)

        assert owner._materialization_sealed.is_set()
        assert owner._candidate_capture_used
    finally:
        owner.close()
