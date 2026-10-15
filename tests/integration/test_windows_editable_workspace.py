import os
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from tests.integration.test_windows_base_materialization import resource, snapshot

pytestmark = pytest.mark.skipif(
    sys.platform != "win32", reason="Native Windows editable workspace handoff."
)


def test_native_handoff_allows_edit_delete_replace_and_directory_rename_with_reserved_root(
    tmp_path: Path,
) -> None:
    native = resource(tmp_path)
    reservation = native.reserve()
    source = snapshot(
        native.binding,
        {
            "edit.txt": b"original",
            "remove.txt": b"remove",
            "replace.txt": b"replace",
            "src/nested/file.txt": b"nested",
        },
    )
    assert native.location
    target = Path(native.location.root_directory) / reservation.directory_name
    renamed = target.with_name(target.name + "-moved")
    try:
        result = native.materialize(reservation, source)
        with pytest.raises(PermissionError):
            target.rename(renamed)
        with pytest.raises(PermissionError):
            target.parent.rename(target.parent.with_name("moved-workspaces"))
        with pytest.raises(PermissionError):
            (target / "edit.txt").write_bytes(b"blocked")
        with pytest.raises(PermissionError):
            (target / "src").rename(target / "other")

        handoff = native.handoff_materialized_base(result)

        assert handoff.materialization is result
        (target / "edit.txt").write_bytes(b"edited")
        (target / "remove.txt").unlink()
        temporary = target / "replacement.tmp"
        temporary.write_bytes(b"atomically replaced")
        os.replace(temporary, target / "replace.txt")
        (target / "src").rename(target / "renamed")
        assert (target / "edit.txt").read_bytes() == b"edited"
        assert not (target / "remove.txt").exists()
        assert (target / "replace.txt").read_bytes() == b"atomically replaced"
        assert (target / "renamed/nested/file.txt").read_bytes() == b"nested"
        with pytest.raises(PermissionError):
            target.rename(renamed)
        with pytest.raises(PermissionError):
            target.parent.rename(target.parent.with_name("moved-workspaces"))
        with pytest.raises(WorkspaceUnavailable):
            native.handoff_materialized_base(result)
        assert not (target / ".git").exists()
    finally:
        native.close()
    target.rename(renamed)
    assert (renamed / "edit.txt").read_bytes() == b"edited"


def test_native_copied_materialization_never_releases_file_guards(
    tmp_path: Path,
) -> None:
    native = resource(tmp_path)
    reservation = native.reserve()
    assert native.location
    target = Path(native.location.root_directory) / reservation.directory_name
    try:
        result = native.materialize(
            reservation, snapshot(native.binding, {"file.txt": b"retained"})
        )

        with pytest.raises(WorkspaceUnavailable):
            native.handoff_materialized_base(replace(result))

        with pytest.raises(PermissionError):
            (target / "file.txt").write_bytes(b"blocked")
        assert (target / "file.txt").read_bytes() == b"retained"
    finally:
        native.close()
