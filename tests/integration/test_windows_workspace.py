import ctypes
import os
import subprocess
import sys
from pathlib import Path

import pytest

from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from devrelay.infrastructure.workspace.windows import (
    WindowsWorkspacePolicy,
    WindowsWorkspaceReservation,
)
from tests.workspace_fixtures import workspace_binding

pytestmark = pytest.mark.skipif(
    sys.platform != "win32",
    reason="Native Windows directory handles and canonical identities.",
)


def policy(tmp_path: Path) -> WindowsWorkspacePolicy:
    root = tmp_path / "workspaces"
    root.mkdir()
    return WindowsWorkspacePolicy(
        root_directory=str(root),
        protected_directories=(str(tmp_path / "private-state"),),
    )


def test_native_reservation_returns_empty_uuid_directory_and_close_preserves_it(
    tmp_path: Path,
) -> None:
    configuration = policy(tmp_path)
    binding = workspace_binding()
    native = WindowsWorkspaceReservation(binding, configuration)
    try:
        reservation = native.reserve()
        target = Path(configuration.root_directory) / reservation.directory_name

        assert reservation.binding is binding
        assert reservation.directory_name == str(binding.execution.intent.execution_id)
        assert reservation.identity.file_index > 0
        assert target.is_dir() and list(target.iterdir()) == []
        assert not (target / ".git").exists()
        with pytest.raises(WorkspaceUnavailable):
            native.reserve()
    finally:
        native.close()
    assert target.is_dir() and list(target.iterdir()) == []


def test_native_existing_execution_directory_is_never_adopted(tmp_path: Path) -> None:
    configuration = policy(tmp_path)
    binding = workspace_binding()
    target = Path(configuration.root_directory) / str(
        binding.execution.intent.execution_id
    )
    target.mkdir()
    marker = target / "existing.txt"
    marker.write_text("preserve")
    native = WindowsWorkspaceReservation(binding, configuration)
    try:
        with pytest.raises(WorkspaceUnavailable) as error:
            native.reserve()

        assert str(error.value) == "Windows workspace reservation is unavailable."
        assert marker.read_text() == "preserve"
        with pytest.raises(WorkspaceUnavailable):
            native.reserve()
    finally:
        native.close()


def test_native_retained_handles_block_renaming_root_until_close(
    tmp_path: Path,
) -> None:
    configuration = policy(tmp_path)
    root, renamed = Path(configuration.root_directory), tmp_path / "renamed-workspaces"
    assert root.resolve().is_relative_to(tmp_path.resolve())
    assert renamed.resolve().is_relative_to(tmp_path.resolve())
    native = WindowsWorkspaceReservation(workspace_binding(), configuration)
    reservation = native.reserve()
    try:
        with pytest.raises(PermissionError):
            os.rename(root, renamed)

        assert root.is_dir() and not renamed.exists()
    finally:
        native.close()
    os.rename(root, renamed)
    assert (renamed / reservation.directory_name).is_dir()


def test_native_junction_ancestor_is_refused_before_directory_creation(
    tmp_path: Path,
) -> None:
    actual, link = tmp_path / "actual", tmp_path / "junction"
    actual.mkdir()
    (actual / "workspaces").mkdir()
    assert actual.resolve().is_relative_to(tmp_path.resolve())
    assert link.resolve().is_relative_to(tmp_path.resolve())
    command = subprocess.run(
        [
            str(Path(os.environ["SystemRoot"]) / "System32" / "cmd.exe"),
            "/c",
            "mklink",
            "/J",
            str(link),
            str(actual),
        ],
        capture_output=True,
        check=False,
        timeout=10,
        shell=False,
        creationflags=0x08000000,
        env={"SystemRoot": os.environ["SystemRoot"]},
    )
    if command.returncode != 0:
        pytest.skip(
            "Fixture junction creation is unavailable on this Windows filesystem."
        )
    configuration = WindowsWorkspacePolicy(
        root_directory=str(link / "workspaces"),
        protected_directories=(str(tmp_path / "private-state"),),
    )
    binding = workspace_binding()
    native = WindowsWorkspaceReservation(binding, configuration)
    try:
        with pytest.raises(WorkspaceUnavailable):
            native.reserve()

        assert not (
            actual / "workspaces" / str(binding.execution.intent.execution_id)
        ).exists()
    finally:
        native.close()


def test_native_missing_root_is_not_created_implicitly(tmp_path: Path) -> None:
    root = tmp_path / "missing-workspaces"
    configuration = WindowsWorkspacePolicy(
        root_directory=str(root),
        protected_directories=(str(tmp_path / "private-state"),),
    )
    native = WindowsWorkspaceReservation(workspace_binding(), configuration)
    try:
        with pytest.raises(WorkspaceUnavailable):
            native.reserve()

        assert not root.exists()
    finally:
        native.close()


def test_native_directory_handles_are_noninheritable_and_retained(
    tmp_path: Path,
) -> None:
    configuration = policy(tmp_path)
    native = WindowsWorkspaceReservation(workspace_binding(), configuration)
    native.reserve()
    loader = getattr(ctypes, "WinDLL", None)
    assert loader is not None
    kernel = loader("Kernel32.dll", use_last_error=True, winmode=0x800)
    kernel.GetHandleInformation.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_uint32),
    ]
    kernel.GetHandleInformation.restype = ctypes.c_int32
    try:
        observations = []
        for handle in native._handles:
            flags = ctypes.c_uint32()
            assert kernel.GetHandleInformation(handle, ctypes.byref(flags))
            observations.append(flags.value)

        assert len(observations) >= 3
        assert all(flags & 1 == 0 for flags in observations)
    finally:
        native.close()
