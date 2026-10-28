"""Harmless native freeze fixtures; no model, provider or untrusted check launch."""

import os
import shutil
import subprocess
import sys
from collections.abc import Iterator
from dataclasses import dataclass, replace
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import pytest

from devrelay.application.contracts.base_sources import BaseFileMode
from devrelay.application.contracts.editable_workspaces import EditableWorkspace
from devrelay.application.contracts.frozen_candidates import FrozenCandidate
from devrelay.application.contracts.git_sources import (
    GitBaseSourceSnapshot,
    GitWorkspacePreparation,
)
from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from devrelay.infrastructure.workspace.windows import (
    WindowsWorkspacePolicy,
    WindowsWorkspaceReservation,
)
from devrelay.infrastructure.workspace.windows_candidates import (
    WindowsCandidateWorkspace,
)
from tests.integration.test_windows_git_metadata import conversion_inputs
from tests.unit.test_candidate_objects import commit

pytestmark = pytest.mark.skipif(
    sys.platform != "win32", reason="Actual Windows candidate handles."
)


@dataclass
class Native:
    owner: WindowsWorkspaceReservation
    capture: WindowsCandidateWorkspace
    source: GitBaseSourceSnapshot
    preparation: GitWorkspacePreparation
    editable: EditableWorkspace
    target: Path

    def freeze(self, cycle: int = 0) -> FrozenCandidate:
        return self.capture.capture(
            self.preparation,
            self.editable,
            capture_id=uuid4(),
            cycle=cycle,
            commit=commit(),
        )


@pytest.fixture
def native() -> Iterator[Native]:
    # A deliberately short, owned path leaves room for the explicit safe GIT_DIR.
    artifacts = Path(__file__).resolve().parents[4] / "artifacts"
    with TemporaryDirectory(prefix="dr-candidate-", dir=artifacts) as directory:
        binding, source = conversion_inputs()
        root = Path(directory) / "w"
        root.mkdir()
        owner = WindowsWorkspaceReservation(
            binding,
            WindowsWorkspacePolicy(
                root_directory=str(root),
                protected_directories=(str(Path(directory) / "private"),),
            ),
        )
        reservation = owner.reserve()
        try:
            materialization = owner.materialize(reservation, source.snapshot)
            preparation = owner.prepare_git_metadata(
                source, materialization, binding.definition.authorized_branch
            )
            capture = owner.prepare_candidate_capture(source)
            editable = owner.handoff_materialized_base(materialization)
            yield Native(
                owner,
                capture,
                source,
                preparation,
                editable,
                root / reservation.directory_name,
            )
        finally:
            owner.close()


def fixture_git(frozen: FrozenCandidate, *arguments: str) -> bytes:
    executable = shutil.which("git")
    assert executable
    result = subprocess.run(
        [executable, "--no-optional-locks", *arguments],
        env={
            "SystemRoot": os.environ["SystemRoot"],
            "GIT_DIR": frozen.git_directory,
            "GIT_WORK_TREE": frozen.worktree,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_NO_REPLACE_OBJECTS": "1",
        },
        stdin=subprocess.DEVNULL,
        capture_output=True,
        timeout=10,
        creationflags=0x08000000,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


def test_native_freeze_has_real_candidate_parent_clean_index_and_readonly_source_guards(
    native: Native,
) -> None:
    (native.target / "README.md").write_bytes(b"changed executable source\n")
    (native.target / "data/payload.bin").unlink()
    (native.target / "new.txt").write_bytes(b"new literal source\n")
    # These are coder-controlled and must never influence trusted reconstruction.
    (native.target / ".git/config").write_bytes(b"invalid coder configuration\n")
    (native.target / ".git/HEAD").write_bytes(b"untrusted HEAD\n")
    (native.target / ".git/index").write_bytes(b"untrusted index\n")

    frozen = native.freeze()

    assert frozen.editable is native.editable
    files = {item.path: item for item in frozen.snapshot.files}
    assert "data/payload.bin" not in files
    assert files["README.md"].mode is BaseFileMode.EXECUTABLE
    assert files["new.txt"].mode is BaseFileMode.REGULAR
    assert (
        fixture_git(frozen, "rev-parse", "HEAD").strip()
        == frozen.snapshot.revision.encode()
    )
    assert (
        fixture_git(frozen, "cat-file", "commit", "HEAD") == frozen.snapshot.raw_commit
    )
    assert (
        fixture_git(frozen, "cat-file", "commit", "HEAD^") == native.source.raw_commit
    )
    assert (
        fixture_git(frozen, "status", "--porcelain=v1", "-z", "--untracked-files=all")
        == b""
    )
    assert fixture_git(frozen, "fsck", "--connectivity-only", "--no-reflogs") == b""
    with pytest.raises(PermissionError):
        (native.target / "README.md").write_bytes(b"forbidden while frozen")
    with pytest.raises(PermissionError):
        (native.target / "src").rename(native.target / "renamed")
    with pytest.raises(PermissionError):
        (Path(frozen.git_directory) / "HEAD").write_bytes(b"forbidden metadata write")
    native.capture.verify(frozen)


def test_native_release_allows_bounded_repair_with_fresh_metadata(
    native: Native,
) -> None:
    (native.target / "README.md").write_bytes(b"first candidate\n")
    frozen = native.freeze()
    native.capture.release_for_repair(frozen)
    (native.target / "README.md").write_bytes(b"repaired candidate\n")

    repaired = native.freeze(cycle=1)

    assert repaired.snapshot.revision != frozen.snapshot.revision
    assert repaired.git_directory != frozen.git_directory
    assert Path(frozen.git_directory).is_dir()
    assert repaired.snapshot.cycle == 1
    native.capture.verify(repaired)


def test_native_empty_candidate_keeps_real_parent_and_empty_index(
    native: Native,
) -> None:
    for item in native.source.snapshot.files:
        (native.target / item.path).unlink()

    frozen = native.freeze()

    assert frozen.snapshot.files == ()
    assert fixture_git(frozen, "ls-files", "-z") == b""
    assert (
        fixture_git(frozen, "cat-file", "commit", "HEAD^") == native.source.raw_commit
    )
    native.capture.verify(frozen)


@pytest.mark.parametrize("kind", ["file", "directory"])
def test_native_new_child_after_freeze_is_detected_and_seals_capture(
    native: Native, kind: str
) -> None:
    (native.target / "README.md").write_bytes(b"candidate\n")
    frozen = native.freeze()
    extra = native.target / "extra"
    if kind == "file":
        extra.write_bytes(b"not in checked candidate")
    else:
        extra.mkdir()

    with pytest.raises(WorkspaceUnavailable):
        native.capture.verify(frozen)

    assert native.owner._materialization_sealed.is_set()
    assert extra.exists()


def test_native_new_metadata_child_is_detected_before_frozen_reuse(
    native: Native,
) -> None:
    (native.target / "README.md").write_bytes(b"candidate\n")
    frozen = native.freeze()
    unexpected = Path(frozen.git_directory) / "unexpected"
    unexpected.write_bytes(b"not generated metadata")

    with pytest.raises(WorkspaceUnavailable):
        native.capture.verify(frozen)

    assert native.owner._materialization_sealed.is_set()
    assert unexpected.read_bytes() == b"not generated metadata"


@pytest.mark.parametrize("kind", ["file", "directory", "worktree"])
def test_native_alternate_streams_are_rejected(native: Native, kind: str) -> None:
    (native.target / "README.md").write_bytes(b"candidate\n")
    path = (
        native.target / "README.md"
        if kind == "file"
        else native.target / "src"
        if kind == "directory"
        else native.target
    )
    Path(str(path) + ":hidden").write_bytes(b"unrepresented alternate input")

    with pytest.raises(WorkspaceUnavailable):
        native.freeze()

    assert Path(str(path) + ":hidden").read_bytes() == b"unrepresented alternate input"


def test_native_hardlink_source_rejected_without_deletion(native: Native) -> None:
    (native.target / "README.md").write_bytes(b"candidate\n")
    linked = native.target / "linked.txt"
    os.link(native.target / "README.md", linked)

    with pytest.raises(WorkspaceUnavailable):
        native.freeze()

    assert linked.read_bytes() == b"candidate\n"


@pytest.mark.parametrize("kind", ["parent", "capture"])
def test_native_foreign_metadata_namespace_is_never_adopted(
    native: Native, kind: str
) -> None:
    (native.target / "README.md").write_bytes(b"candidate\n")
    capture_id = uuid4()
    parent = native.target / ".git/devrelay"
    parent.mkdir()
    if kind == "capture":
        (parent / str(capture_id)).mkdir()
    marker = parent / "marker"
    marker.write_bytes(b"preserve foreign bytes")

    with pytest.raises(WorkspaceUnavailable):
        native.capture.capture(
            native.preparation,
            native.editable,
            capture_id=capture_id,
            cycle=0,
            commit=commit(),
        )

    assert marker.read_bytes() == b"preserve foreign bytes"


@pytest.mark.parametrize("kind", ["editable", "preparation"])
def test_native_value_equal_observations_supply_no_capture_right(
    native: Native, kind: str
) -> None:
    (native.target / "README.md").write_bytes(b"candidate\n")
    editable = replace(native.editable) if kind == "editable" else native.editable
    preparation = (
        replace(native.preparation) if kind == "preparation" else native.preparation
    )

    with pytest.raises(WorkspaceUnavailable):
        native.capture.capture(
            preparation, editable, capture_id=uuid4(), cycle=0, commit=commit()
        )

    assert not (native.target / ".git/devrelay").exists()
