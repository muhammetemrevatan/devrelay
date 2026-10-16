from dataclasses import replace
from typing import Any

import pytest

from devrelay.application.contracts.base_sources import (
    BaseFileMode,
    BaseMaterializationPolicy,
    BaseSourceFile,
    BaseSourceLimits,
    BaseSourceSnapshot,
    git_blob_sha,
)
from tests.base_source_fixtures import source_fixture
from tests.workspace_fixtures import workspace_binding


@pytest.mark.parametrize(
    "path",
    [
        "../escape",
        "/absolute",
        "C:/device",
        "foo\\bar",
        ".git/config",
        "src/.GIT/hooks",
        "file:stream",
        "NUL",
        "src./file",
        "a//b",
        "a/../b",
        "a~1/file",
        "e\u0301.txt",
    ],
)
def test_base_file_rejects_unsafe_relative_path(path: str) -> None:
    content = b"fixture"

    with pytest.raises(ValueError):
        BaseSourceFile(
            path=path,
            blob_sha=git_blob_sha(content),
            mode=BaseFileMode.REGULAR,
            content=content,
        )


@pytest.mark.parametrize(
    "paths", [("A/file", "a/other"), ("file", "file/child"), ("file", "file")]
)
def test_manifest_rejects_directory_aliases_and_file_conflicts(
    paths: tuple[str, ...],
) -> None:
    original = source_fixture(workspace_binding()).snapshot
    files = tuple(
        BaseSourceFile(
            path=path,
            blob_sha=git_blob_sha(b"x"),
            mode=BaseFileMode.REGULAR,
            content=b"x",
        )
        for path in sorted(paths)
    )

    with pytest.raises(ValueError):
        BaseSourceSnapshot(
            request=original.request, root_tree_sha=original.root_tree_sha, files=files
        )


def test_git_blob_hash_matches_known_git_object_format() -> None:
    content = b"hello\n"

    sha = git_blob_sha(content)

    assert sha == "ce013625030ba8dba906f756967f9e9ca394464a"


@pytest.mark.parametrize("value", [True, float("nan"), float("inf"), 10**1000, -1])
def test_source_and_materialization_time_budgets_reject_nonfinite_or_unbounded(
    value: Any,
) -> None:
    changes: dict[str, Any] = {"total_timeout_seconds": value}

    with pytest.raises(ValueError):
        replace(BaseSourceLimits(), **changes)
    with pytest.raises(ValueError):
        replace(BaseMaterializationPolicy(), **changes)


def test_manifest_revalidates_corrupt_blob_bytes_before_stage() -> None:
    snapshot = source_fixture(workspace_binding()).snapshot
    damaged = object.__new__(BaseSourceFile)
    for field in ("path", "blob_sha", "mode", "content"):
        object.__setattr__(damaged, field, getattr(snapshot.files[0], field))
    object.__setattr__(damaged, "content", b"changed")
    candidate = object.__new__(BaseSourceSnapshot)
    object.__setattr__(candidate, "request", snapshot.request)
    object.__setattr__(candidate, "root_tree_sha", snapshot.root_tree_sha)
    object.__setattr__(candidate, "files", (damaged, *snapshot.files[1:]))
    object.__setattr__(candidate, "raw_commit", None)

    with pytest.raises(ValueError, match="verified ordinary blob"):
        candidate.validate()


@pytest.mark.parametrize("ending", [b"\n", b"\r\n"])
def test_base_file_rejects_lfs_pointer(ending: bytes) -> None:
    content = (
        b"version https://git-lfs.github.com/spec/v1"
        + ending
        + b"oid sha256:0000\nsize 0\n"
    )

    with pytest.raises(ValueError):
        BaseSourceFile(
            path="pointer",
            blob_sha=git_blob_sha(content),
            mode=BaseFileMode.REGULAR,
            content=content,
        )
