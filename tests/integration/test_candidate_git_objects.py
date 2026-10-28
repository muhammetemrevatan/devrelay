"""Actual Windows Git evidence for the pure canonical candidate builder."""

import os
import shutil
import subprocess
import sys
from pathlib import Path
from uuid import UUID

import pytest

from devrelay.infrastructure.workspace.candidate_objects import (
    build_candidate_metadata,
    build_candidate_snapshot,
)
from tests.unit.test_candidate_objects import commit, files, source

_EXECUTION = UUID("11111111-1111-4111-8111-111111111111")
_CAPTURE = UUID("22222222-2222-4222-8222-222222222222")


@pytest.mark.skipif(sys.platform != "win32", reason="Actual Windows Git fixture")
def test_actual_git_observes_candidate_parent_tree_clean_index_literal_bytes_and_depth_two(
    tmp_path: Path,
) -> None:
    executable = shutil.which("git")
    assert executable is not None, (
        "Trusted installed Git fixture is required on Windows."
    )
    original = source()
    worktree = tmp_path / "work"
    worktree.mkdir()
    gitdir = worktree / ".git" / "devrelay" / str(_CAPTURE)
    candidate = build_candidate_snapshot(
        execution_id=_EXECUTION,
        cycle=0,
        source=original,
        branch="devrelay/fixture",
        files=files(),
        commit=commit(),
    )
    metadata = build_candidate_metadata(
        original, candidate, git_directory=str(gitdir), worktree=str(worktree)
    )
    for directory in metadata.directories:
        relative = directory.removeprefix(".git").lstrip("/")
        (gitdir / relative).mkdir(parents=True, exist_ok=True)
    for item in metadata.files:
        (gitdir / item.path.removeprefix(".git/")).write_bytes(item.content)
    for source_file in candidate.files:
        path = worktree / source_file.path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(source_file.content)
    env = {
        "SystemRoot": os.environ["SystemRoot"],
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_DIR": str(gitdir),
        "GIT_WORK_TREE": str(worktree),
        "GIT_NO_LAZY_FETCH": "1",
    }
    creation_flags = 0
    if sys.platform == "win32":
        creation_flags = subprocess.CREATE_NO_WINDOW

    def git(*args: str) -> bytes:
        result = subprocess.run(
            [executable, *args],
            cwd=worktree,
            env=env,
            capture_output=True,
            stdin=subprocess.DEVNULL,
            creationflags=creation_flags,
            timeout=10,
            check=False,
        )
        assert result.returncode == 0, result.stderr.decode(errors="replace")
        return result.stdout

    assert git("rev-parse", "HEAD").strip() == candidate.revision.encode()
    assert (
        git("rev-parse", "HEAD^").strip()
        == original.snapshot.request.base_revision.encode()
    )
    assert git("cat-file", "commit", "HEAD") == candidate.raw_commit
    assert git("cat-file", "commit", "HEAD^") == original.raw_commit
    assert git("write-tree").strip() == candidate.root_tree_sha.encode()
    assert git("rev-list", "--count", "HEAD").strip() == b"2"
    assert git("status", "--porcelain") == b""
    assert git("remote") == b""
    assert b"100755 " in git("ls-files", "--stage")
    assert git("show", "HEAD:changed.txt") == b"literal\n$Id$\n"
    git("fsck", "--full")
