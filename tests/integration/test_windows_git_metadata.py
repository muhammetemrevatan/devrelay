import hashlib
import os
import shutil
import subprocess
import sys
import zlib
from dataclasses import replace
from pathlib import Path

import pytest

from devrelay.application.contracts.base_sources import (
    BaseFileMode,
    BaseSourceFile,
    git_blob_sha,
)
from devrelay.application.contracts.git_sources import GitBaseSourceSnapshot
from devrelay.application.contracts.workspaces import WorkspaceBinding
from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from devrelay.infrastructure.workspace.git_metadata import build_git_metadata
from devrelay.infrastructure.workspace.windows import (
    WindowsWorkspacePolicy,
    WindowsWorkspaceReservation,
)
from tests.unit.test_windows_git_metadata import git_inputs

pytestmark = pytest.mark.skipif(
    sys.platform != "win32",
    reason="Native Windows verified shallow Git workspace metadata.",
)


def git(directory: Path, *arguments: str) -> bytes:
    executable = shutil.which("git")
    assert executable, (
        "Installed Git is required for shallow metadata compatibility evidence."
    )
    result = subprocess.run(
        [executable, "--no-optional-locks", "-C", str(directory), *arguments],
        env={
            "SystemRoot": os.environ["SystemRoot"],
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_NO_REPLACE_OBJECTS": "1",
        },
        stdin=subprocess.DEVNULL,
        capture_output=True,
        timeout=10,
        check=False,
        creationflags=0x08000000,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


def conversion_inputs() -> tuple[WorkspaceBinding, GitBaseSourceSnapshot]:
    binding, source = git_inputs()
    metadata = build_git_metadata(source, binding.definition.authorized_branch)
    root = source.snapshot.root_tree_sha
    compressed = next(
        item.content
        for item in metadata.files
        if item.path == f".git/objects/{root[:2]}/{root[2:]}"
    )
    original_tree = (
        zlib.decompress(compressed)
        .split(b"\x00", 1)[1]
        .replace(b"100644 README.md\x00", b"100755 README.md\x00", 1)
    )
    content = b"* text eol=crlf filter=fixture ident working-tree-encoding=UTF-16\n"
    attributes = BaseSourceFile(
        path=".gitattributes",
        blob_sha=git_blob_sha(content),
        mode=BaseFileMode.REGULAR,
        content=content,
    )
    tree = (
        b"100644 .gitattributes\x00"
        + bytes.fromhex(attributes.blob_sha)
        + original_tree
    )
    root = hashlib.sha1(
        b"tree " + str(len(tree)).encode() + b"\x00" + tree, usedforsecurity=False
    ).hexdigest()
    raw = b"tree " + root.encode() + b"\n" + source.raw_commit.split(b"\n", 1)[1]
    sha = hashlib.sha1(
        b"commit " + str(len(raw)).encode() + b"\x00" + raw, usedforsecurity=False
    ).hexdigest()
    snapshot = replace(
        source.snapshot,
        files=(
            attributes,
            *(
                replace(item, mode=BaseFileMode.EXECUTABLE)
                if item.path == "README.md"
                else item
                for item in source.snapshot.files
            ),
        ),
        root_tree_sha=root,
        raw_commit=raw,
        request=replace(source.snapshot.request, base_revision=sha),
    )
    definition = replace(binding.definition, base_revision=sha)
    acknowledgement = replace(
        binding.execution.intent.acknowledgement,
        base_revision=sha,
        definition_digest=definition.fingerprint(),
    )
    binding = replace(
        binding,
        definition=definition,
        execution=replace(
            binding.execution,
            intent=replace(binding.execution.intent, acknowledgement=acknowledgement),
        ),
    )
    return binding, GitBaseSourceSnapshot(snapshot=snapshot, raw_commit=raw)


@pytest.mark.parametrize("conversion_attributes", [False, True])
def test_native_metadata_is_actual_clean_shallow_git_at_original_commit_and_authorized_branch(
    tmp_path: Path,
    conversion_attributes: bool,
) -> None:
    binding, source = conversion_inputs() if conversion_attributes else git_inputs()
    root = tmp_path / "workspaces"
    root.mkdir()
    native = WindowsWorkspaceReservation(
        binding,
        WindowsWorkspacePolicy(
            root_directory=str(root), protected_directories=(str(tmp_path / "private"),)
        ),
    )
    reservation = native.reserve()
    target = root / reservation.directory_name
    try:
        materialization = native.materialize(reservation, source.snapshot)

        prepared = native.prepare_git_metadata(
            source, materialization, binding.definition.authorized_branch
        )

        assert prepared.materialization is materialization
        with pytest.raises(PermissionError):
            (target / ".git/HEAD").write_bytes(b"ref: refs/heads/other\n")
        native.handoff_materialized_base(materialization)
        assert (
            git(target, "rev-parse", "HEAD").strip()
            == source.snapshot.request.base_revision.encode()
        )
        assert (
            git(target, "symbolic-ref", "--short", "HEAD").strip()
            == binding.definition.authorized_branch.encode()
        )
        assert git(target, "rev-parse", "--is-shallow-repository").strip() == b"true"
        assert (
            git(target, "status", "--porcelain=v1", "-z", "--untracked-files=all")
            == b""
        )
        assert git(target, "cat-file", "commit", "HEAD") == source.raw_commit
        assert git(target, "log", "--format=%H", "--max-count=5").splitlines() == [
            source.snapshot.request.base_revision.encode()
        ]
        assert git(target, "fsck", "--connectivity-only", "--no-reflogs") == b""
        assert git(target, "remote") == b""
        (target / "README.md").write_bytes(b"edited candidate\n")
        assert git(target, "status", "--porcelain=v1", "-z") == b" M README.md\x00"
        with pytest.raises(PermissionError):
            target.rename(target.with_name("renamed-reservation"))
        assert not (target / ".git/objects/info/alternates").exists()
    finally:
        native.close()


def test_native_existing_git_entry_is_retained_and_never_adopted(
    tmp_path: Path,
) -> None:
    binding, source = git_inputs()
    root = tmp_path / "workspaces"
    root.mkdir()
    native = WindowsWorkspaceReservation(
        binding,
        WindowsWorkspacePolicy(
            root_directory=str(root), protected_directories=(str(tmp_path / "private"),)
        ),
    )
    reservation = native.reserve()
    target = root / reservation.directory_name
    try:
        materialization = native.materialize(reservation, source.snapshot)
        (target / ".git").mkdir()
        (target / ".git/marker").write_bytes(b"preserve foreign metadata")

        with pytest.raises(WorkspaceUnavailable):
            native.prepare_git_metadata(
                source, materialization, binding.definition.authorized_branch
            )

        assert (target / ".git/marker").read_bytes() == b"preserve foreign metadata"
        assert not (target / ".git/HEAD").exists()
    finally:
        native.close()
