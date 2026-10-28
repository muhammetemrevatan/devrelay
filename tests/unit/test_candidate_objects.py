import hashlib
import struct
import zlib
from dataclasses import replace
from uuid import UUID

import pytest

from devrelay.application.contracts.base_sources import (
    BaseFileMode,
    BaseSourceFile,
    git_blob_sha,
)
from devrelay.application.contracts.candidates import (
    CandidateCommitIdentity,
    CandidateCommitSpec,
)
from devrelay.application.contracts.git_objects import GitObject
from devrelay.application.contracts.git_sources import GitBaseSourceSnapshot
from devrelay.infrastructure.workspace.candidate_objects import (
    build_candidate_metadata,
    build_candidate_snapshot,
)
from tests.git_source_fixtures import git_source_fixture
from tests.workspace_fixtures import workspace_binding

_EXECUTION = UUID("11111111-1111-4111-8111-111111111111")
_CAPTURE = UUID("22222222-2222-4222-8222-222222222222")


def source() -> GitBaseSourceSnapshot:
    snapshot = git_source_fixture(
        workspace_binding(), b"gpgsig fixture-signature\n continued signature\n"
    ).snapshot
    assert snapshot.raw_commit is not None
    return GitBaseSourceSnapshot(snapshot=snapshot, raw_commit=snapshot.raw_commit)


def commit() -> CandidateCommitSpec:
    human = CandidateCommitIdentity(name="Fixture Human", email="human@example.invalid")
    return CandidateCommitSpec(
        author=human,
        committer=human,
        timestamp=1700000000,
        message="Apply fixture candidate\n",
    )


def files() -> tuple[BaseSourceFile, ...]:
    return tuple(
        BaseSourceFile(path=path, content=data, blob_sha=git_blob_sha(data), mode=mode)
        for path, data, mode in (
            (
                ".gitattributes",
                b"* text eol=crlf filter=hostile ident\n",
                BaseFileMode.REGULAR,
            ),
            ("changed.txt", b"literal\n$Id$\n", BaseFileMode.REGULAR),
            ("nested/tool.py", b"print('fixture')\n", BaseFileMode.EXECUTABLE),
        )
    )


def test_metadata_contains_exact_base_and_candidate_objects_head_index_and_shallow() -> (
    None
):
    original = source()
    candidate = build_candidate_snapshot(
        execution_id=_EXECUTION,
        cycle=2,
        source=original,
        branch="devrelay/fixture",
        files=files(),
        commit=commit(),
    )
    metadata = build_candidate_metadata(
        original,
        candidate,
        git_directory=f"C:\\fixture\\.git\\devrelay\\{_CAPTURE}",
        worktree="C:\\fixture",
    )
    values = {item.path: item.content for item in metadata.files}
    for sha, raw in (
        (original.snapshot.request.base_revision, original.raw_commit),
        (candidate.revision, candidate.raw_commit),
    ):
        assert (
            zlib.decompress(values[f".git/objects/{sha[:2]}/{sha[2:]}"])
            == GitObject(kind="commit", data=raw).raw()
        )
    assert (
        values[".git/shallow"]
        == original.snapshot.request.base_revision.encode() + b"\n"
    )
    assert values[".git/HEAD"] == b"ref: refs/heads/devrelay/fixture\n"
    assert (
        values[".git/refs/heads/devrelay/fixture"]
        == candidate.revision.encode() + b"\n"
    )
    index = values[".git/index"]
    assert index[:12] == b"DIRC" + struct.pack("!II", 2, 3)
    assert hashlib.sha1(index[:-20], usedforsecurity=False).digest() == index[-20:]
    offset = 12
    for item in candidate.files:
        length = struct.unpack("!H", index[offset + 60 : offset + 62])[0]
        assert index[offset + 62 : offset + 62 + length] == item.path.encode()
        assert index[offset + 40 : offset + 60] == bytes.fromhex(item.blob_sha)
        assert struct.unpack("!I", index[offset + 24 : offset + 28])[0] == int(
            item.mode.value, 8
        )
        offset += ((62 + length) // 8 + 1) * 8
    assert offset == len(index) - 20
    config = values[".git/config"]
    assert (
        f'hooksPath = "C:/fixture/.git/devrelay/{_CAPTURE}/hooks-disabled"'.encode()
        in config
    )
    assert b'worktree = "C:/fixture"' in config
    assert b"remote" not in config and b"include" not in config
    assert b"helper =\n" in config and b"allow = never" in config
    assert (
        values[".git/info/attributes"]
        == b"* -text -filter -ident -working-tree-encoding\n"
    )
    for path, compressed in values.items():
        if path.startswith(".git/objects/"):
            assert hashlib.sha1(
                zlib.decompress(compressed), usedforsecurity=False
            ).hexdigest() == path.removeprefix(".git/objects/").replace("/", "")


def test_builder_refuses_no_change() -> None:
    original = source()
    with pytest.raises(ValueError, match="no-change"):
        build_candidate_snapshot(
            execution_id=_EXECUTION,
            cycle=0,
            source=original,
            branch="devrelay/fixture",
            files=original.snapshot.files,
            commit=commit(),
        )


def test_builder_rejects_original_raw_commit_pinned_to_a_false_manifest_tree() -> None:
    original = source()
    raw = original.raw_commit.replace(
        original.snapshot.root_tree_sha.encode(), b"0" * 40, 1
    )
    sha = GitObject(kind="commit", data=raw).sha
    forged = GitBaseSourceSnapshot(
        snapshot=replace(
            original.snapshot,
            request=replace(original.snapshot.request, base_revision=sha),
            root_tree_sha="0" * 40,
            raw_commit=raw,
        ),
        raw_commit=raw,
    )
    with pytest.raises(ValueError, match="tree"):
        build_candidate_snapshot(
            execution_id=_EXECUTION,
            cycle=0,
            source=forged,
            branch="devrelay/fixture",
            files=files(),
            commit=commit(),
        )


def test_builder_rejects_corrupted_original_blob_bytes() -> None:
    original = source()
    corrupt = original.snapshot.files[0]
    object.__setattr__(corrupt, "content", b"tamper")
    with pytest.raises(ValueError):
        build_candidate_snapshot(
            execution_id=_EXECUTION,
            cycle=0,
            source=original,
            branch="devrelay/fixture",
            files=files(),
            commit=commit(),
        )


def test_empty_candidate_is_real_empty_tree_and_index_with_exact_parent() -> None:
    original = source()
    candidate = build_candidate_snapshot(
        execution_id=_EXECUTION,
        cycle=0,
        source=original,
        branch="devrelay/fixture",
        files=(),
        commit=commit(),
    )
    metadata = build_candidate_metadata(
        original,
        candidate,
        git_directory=f"C:\\fixture\\.git\\devrelay\\{_CAPTURE}",
        worktree="C:\\fixture",
    )
    assert candidate.root_tree_sha == "4b825dc642cb6eb9a060e54bf8d69288fbee4904"
    assert (
        b"parent " + original.snapshot.request.base_revision.encode() + b"\n"
        in candidate.raw_commit
    )
    assert (
        len(next(item.content for item in metadata.files if item.path == ".git/index"))
        == 32
    )


@pytest.mark.parametrize(
    "gitdir,worktree",
    [
        ("C:\\elsewhere\\.git\\devrelay\\" + str(_CAPTURE), "C:\\fixture"),
        ("C:\\fixture\\.git\\devrelay\\not-uuid", "C:\\fixture"),
        ("C:\\fixture\\.git\\devrelay\\" + str(_CAPTURE) + "\\child", "C:\\fixture"),
        ("C:\\fixture\\.git\\devrelay\\..\\escape", "C:\\fixture"),
        ("C:\\fixture\\.git\\devrelay\\" + str(_CAPTURE), "C:\\fixture\nattack"),
    ],
)
def test_metadata_requires_exact_private_capture_and_safe_absolute_config_paths(
    gitdir: str, worktree: str
) -> None:
    original = source()
    candidate = build_candidate_snapshot(
        execution_id=_EXECUTION,
        cycle=0,
        source=original,
        branch="devrelay/fixture",
        files=files(),
        commit=commit(),
    )
    with pytest.raises(ValueError):
        build_candidate_metadata(
            original, candidate, git_directory=gitdir, worktree=worktree
        )


def test_metadata_rejects_candidate_from_different_original_numeric_repository() -> (
    None
):
    original = source()
    candidate = build_candidate_snapshot(
        execution_id=_EXECUTION,
        cycle=0,
        source=original,
        branch="devrelay/fixture",
        files=files(),
        commit=commit(),
    )
    candidate = replace(
        candidate,
        request=replace(
            candidate.request,
            provider_repository_id=candidate.request.provider_repository_id + 1,
        ),
    )
    with pytest.raises(ValueError, match="source identity"):
        build_candidate_metadata(
            original,
            candidate,
            git_directory=f"C:\\fixture\\.git\\devrelay\\{_CAPTURE}",
            worktree="C:\\fixture",
        )
