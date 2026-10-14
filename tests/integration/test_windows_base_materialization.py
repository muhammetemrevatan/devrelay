import sys
from dataclasses import replace
from pathlib import Path

import pytest

from devrelay.application.contracts.base_sources import (
    BaseFileMode,
    BaseSourceFile,
    BaseSourceRequest,
    BaseSourceSnapshot,
    git_blob_sha,
)
from devrelay.application.contracts.workspaces import WorkspaceBinding
from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from devrelay.infrastructure.workspace.windows import (
    WindowsWorkspacePolicy,
    WindowsWorkspaceReservation,
)
from tests.workspace_fixtures import workspace_binding

pytestmark = pytest.mark.skipif(
    sys.platform != "win32", reason="Native Windows literal base-file materialization."
)


def snapshot(
    binding: WorkspaceBinding, content: dict[str, bytes]
) -> BaseSourceSnapshot:
    definition = binding.definition
    request = BaseSourceRequest(
        repository_id=definition.repository_id,
        provider_repository_id=definition.provider_repository_id,
        repository_slug=definition.repository_slug,
        base_revision=definition.base_revision,
    )
    return BaseSourceSnapshot(
        request=request,
        root_tree_sha="a" * 40,
        files=tuple(
            BaseSourceFile(
                path=path,
                blob_sha=git_blob_sha(data),
                mode=BaseFileMode.REGULAR,
                content=data,
            )
            for path, data in sorted(
                content.items(), key=lambda item: item[0].encode("utf-8")
            )
        ),
    )


def resource(tmp_path: Path) -> WindowsWorkspaceReservation:
    root = tmp_path / "workspaces"
    root.mkdir()
    return WindowsWorkspaceReservation(
        workspace_binding(),
        WindowsWorkspacePolicy(
            root_directory=str(root),
            protected_directories=(str(tmp_path / "private-state"),),
        ),
    )


def test_native_materialization_preserves_exact_bytes_and_literal_tree_without_git(
    tmp_path: Path,
) -> None:
    native = resource(tmp_path)
    reservation = native.reserve()
    contents = {
        "README.md": b"line one\r\nline two\n",
        "assets/café.bin": bytes(range(256)) * 600,
        "assets/😀.txt": b"literal UTF-16 filename",
        "empty.txt": b"",
        "src/package/main.py": b"print('data only')\n",
    }
    source = snapshot(native.binding, contents)
    target = (
        Path(native.location.root_directory) / reservation.directory_name
        if native.location
        else None
    )
    assert target
    try:
        result = native.materialize(reservation, source)

        assert result.reservation is reservation
        assert result.file_count == len(contents)
        assert result.total_bytes == sum(map(len, contents.values()))
        assert result.manifest_digest == source.fingerprint()
        assert {
            path.relative_to(target).as_posix(): path.read_bytes()
            for path in target.rglob("*")
            if path.is_file()
        } == contents
        assert not (target / ".git").exists()
        with pytest.raises(PermissionError):
            (target / "README.md").write_bytes(b"cannot replace retained bytes")
        with pytest.raises(PermissionError):
            (target / "README.md").unlink()
        with pytest.raises(WorkspaceUnavailable):
            native.materialize(reservation, source)
    finally:
        native.close()
    assert (target / "README.md").read_bytes() == contents["README.md"]


def test_native_equal_historical_reservation_copy_cannot_materialize(
    tmp_path: Path,
) -> None:
    native = resource(tmp_path)
    reservation = native.reserve()
    copied = replace(reservation)
    source = snapshot(native.binding, {"README.md": b"must not write"})
    assert native.location
    target = Path(native.location.root_directory) / reservation.directory_name
    try:
        with pytest.raises(WorkspaceUnavailable):
            native.materialize(copied, source)

        assert list(target.iterdir()) == []
    finally:
        native.close()


def test_native_existing_entry_is_never_overwritten_or_removed(tmp_path: Path) -> None:
    native = resource(tmp_path)
    reservation = native.reserve()
    assert native.location
    target = Path(native.location.root_directory) / reservation.directory_name
    marker = target / "existing.txt"
    marker.write_bytes(b"preserve")
    source = snapshot(
        native.binding, {"existing.txt": b"replacement", "new.txt": b"must not write"}
    )
    try:
        with pytest.raises(WorkspaceUnavailable):
            native.materialize(reservation, source)

        assert marker.read_bytes() == b"preserve"
        assert not (target / "new.txt").exists()
    finally:
        native.close()


def test_native_sealed_materialization_leaves_reserved_directory_empty(
    tmp_path: Path,
) -> None:
    native = resource(tmp_path)
    reservation = native.reserve()
    source = snapshot(native.binding, {"file.txt": b"must not write"})
    native.seal_materialization()
    assert native.location
    target = Path(native.location.root_directory) / reservation.directory_name
    try:
        with pytest.raises(WorkspaceUnavailable):
            native.materialize(reservation, source)

        assert list(target.iterdir()) == []
    finally:
        native.close()


def test_native_absolute_path_budget_rejects_manifest_before_any_writes(
    tmp_path: Path,
) -> None:
    native = resource(tmp_path)
    reservation = native.reserve()
    source = snapshot(
        native.binding, {"a.txt": b"must not write", "b" * 170 + ".txt": b"too long"}
    )
    assert native.location
    target = Path(native.location.root_directory) / reservation.directory_name
    try:
        with pytest.raises(WorkspaceUnavailable):
            native.materialize(reservation, source)

        assert list(target.iterdir()) == []
    finally:
        native.close()
