import hashlib
from collections.abc import Callable
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
    CandidateSnapshot,
    parse_candidate_commit,
    validate_candidate_branch,
)
from devrelay.application.contracts.git_objects import (
    GitObject,
    build_git_tree,
    validate_candidate_files,
)
from tests.git_source_fixtures import git_source_fixture
from tests.workspace_fixtures import workspace_binding


def identity() -> CandidateCommitIdentity:
    return CandidateCommitIdentity(name="Fixture Human", email="human@example.invalid")


def spec() -> CandidateCommitSpec:
    return CandidateCommitSpec(
        author=identity(),
        committer=identity(),
        timestamp=1700000000,
        message="Apply fixture change\n\nPreserve exact bytes.\n",
    )


def file(path: str, content: bytes = b"fixture\n") -> BaseSourceFile:
    return BaseSourceFile(
        path=path,
        content=content,
        blob_sha=git_blob_sha(content),
        mode=BaseFileMode.REGULAR,
    )


def snapshot() -> CandidateSnapshot:
    source = git_source_fixture(workspace_binding()).snapshot
    files = (file("edited.txt"),)
    tree = build_git_tree(files).root_tree_sha
    raw = spec().raw_commit(parent=source.request.base_revision, tree=tree)
    return CandidateSnapshot(
        execution_id=UUID("11111111-1111-4111-8111-111111111111"),
        cycle=0,
        request=source.request,
        authorized_branch="devrelay/fixture",
        root_tree_sha=tree,
        revision=GitObject(kind="commit", data=raw).sha,
        raw_commit=raw,
        files=files,
    )


def test_git_object_known_independent_empty_tree_and_blob_vectors() -> None:
    assert (
        GitObject(kind="tree", data=b"").sha
        == "4b825dc642cb6eb9a060e54bf8d69288fbee4904"
    )
    assert (
        GitObject(kind="blob", data=b"hello\n").sha
        == "ce013625030ba8dba906f756967f9e9ca394464a"
    )
    assert (
        build_git_tree(()).root_tree_sha == "4b825dc642cb6eb9a060e54bf8d69288fbee4904"
    )


def test_tree_sorts_directories_using_git_slash_order_and_preserves_modes() -> None:
    files = (file("a.c"), replace(file("a/z"), mode=BaseFileMode.EXECUTABLE))
    result = build_git_tree(files)
    root = next(item for item in result.objects if item.sha == result.root_tree_sha)
    assert root.data.startswith(b"100644 a.c\x00")
    assert b"40000 a\x00" in root.data
    subtree = next(
        item for item in result.objects if item.kind == "tree" and item != root
    )
    assert subtree.data == b"100755 z\x00" + bytes.fromhex(files[1].blob_sha)
    assert len([item for item in result.objects if item.kind == "blob"]) == 1


def test_canonical_commit_has_exact_headers_parent_identity_date_and_message() -> None:
    raw = spec().raw_commit(parent="1" * 40, tree="2" * 40)
    assert raw == (
        b"tree "
        + b"2" * 40
        + b"\nparent "
        + b"1" * 40
        + b"\nauthor Fixture Human <human@example.invalid> 1700000000 +0000"
        + b"\ncommitter Fixture Human <human@example.invalid> 1700000000 +0000"
        + b"\n\nApply fixture change\n\nPreserve exact bytes.\n"
    )
    assert parse_candidate_commit(raw, parent="1" * 40, tree="2" * 40) == spec()


@pytest.mark.parametrize("timestamp", [0, 253402300799])
def test_commit_profile_accepts_explicit_boundary_dates_and_utf8_human_identity(
    timestamp: int,
) -> None:
    profile = replace(
        spec(),
        author=CandidateCommitIdentity(
            name="Fixture İnsan", email="human@example.invalid"
        ),
        timestamp=timestamp,
        message="Çalışma alanı\n",
    )
    raw = profile.raw_commit(parent="1" * 40, tree="2" * 40)
    assert parse_candidate_commit(raw, parent="1" * 40, tree="2" * 40) == profile


def test_commit_message_allows_exact_byte_bound_and_refuses_wrong_parent_tree() -> None:
    profile = replace(spec(), message="x" * 16383 + "\n")
    raw = profile.raw_commit(parent="1" * 40, tree="2" * 40)
    assert parse_candidate_commit(raw, parent="1" * 40, tree="2" * 40) == profile
    for parent, tree in (("0" * 40, "2" * 40), ("1" * 40, "0" * 40)):
        with pytest.raises(ValueError):
            parse_candidate_commit(raw, parent=parent, tree=tree)


@pytest.mark.parametrize(
    "name,email",
    [
        ("", "a@b"),
        (" a", "a@b"),
        ("a\nb", "a@b"),
        ("a<b", "a@b"),
        ("a", "bad"),
        ("a", "a@b\ncommitter evil"),
        ("a", "a@b>"),
        ("e\u0301", "a@b"),
        ("a" * 257, "a@b"),
        ("a", "a" * 253 + "@b"),
    ],
)
def test_commit_identity_refuses_header_injection_aliases_and_bounds(
    name: str, email: str
) -> None:
    with pytest.raises(ValueError):
        CandidateCommitIdentity(name=name, email=email)


@pytest.mark.parametrize("timestamp", [-1, True, 1.5, 253402300800])
def test_commit_timestamp_is_one_bounded_whole_utc_second(timestamp: object) -> None:
    with pytest.raises(ValueError):
        replace(spec(), timestamp=timestamp)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "message",
    ["", " \n", "missing LF", "CR\r\n", "nul\x00\n", "x\n\n", "x" * 16384 + "\n"],
)
def test_commit_message_requires_bounded_utf8_lf_canonical_bytes(message: str) -> None:
    with pytest.raises(ValueError):
        replace(spec(), message=message)


@pytest.mark.parametrize(
    "change",
    [
        lambda raw: raw.replace(b"+0000", b"+0100"),
        lambda raw: raw.replace(b"1700000000", b"01700000000"),
        lambda raw: raw.replace(b"\n\n", b"\nencoding UTF-8\n\n", 1),
        lambda raw: raw.replace(b"\n\n", b"\ngpgsig signature\n\n", 1),
        lambda raw: raw.replace(b"\nauthor ", b"\nparent " + b"3" * 40 + b"\nauthor "),
        lambda raw: raw.replace(
            b"committer Fixture Human", b"committer Other Human"
        ).replace(b"1700000000 +0000\n\n", b"1700000001 +0000\n\n"),
    ],
)
def test_parser_rejects_noncanonical_extra_headers_and_different_dates(
    change: Callable[[bytes], bytes],
) -> None:
    raw = spec().raw_commit(parent="1" * 40, tree="2" * 40)
    with pytest.raises(ValueError):
        parse_candidate_commit(change(raw), parent="1" * 40, tree="2" * 40)


def test_snapshot_independently_binds_commit_and_tree_and_hides_content() -> None:
    candidate = snapshot()
    assert candidate.fingerprint() == replace(candidate).fingerprint()
    assert candidate.raw_commit.decode() not in repr(candidate)
    assert "fixture\\n" not in repr(candidate)
    assert replace(candidate, cycle=1).fingerprint() != candidate.fingerprint()
    assert (
        replace(candidate, authorized_branch="devrelay/other").fingerprint()
        != candidate.fingerprint()
    )
    for changes in (
        {"root_tree_sha": "0" * 40},
        {"revision": "0" * 40},
        {"raw_commit": candidate.raw_commit + b"tamper"},
        {"files": (file("edited.txt", b"tamper"),)},
        {"request": replace(candidate.request, base_revision="0" * 40)},
        {"cycle": 6},
        {"cycle": True},
    ):
        with pytest.raises(ValueError):
            replace(candidate, **changes)


@pytest.mark.parametrize(
    "branch",
    [
        "../escape",
        "bad.lock",
        "bad name",
        "a//b",
        ".hidden",
        "a@{b",
        "a\\b",
        "a~b",
        "a:b",
    ],
)
def test_authorized_branch_refuses_git_and_windows_unsafe_names(branch: str) -> None:
    with pytest.raises(ValueError):
        validate_candidate_branch(branch)


def test_file_manifest_rejects_order_case_directory_conflicts_and_corruption() -> None:
    for files in (
        (file("b"), file("a")),
        (file("a"), file("a")),
        (file("A"), file("a")),
        (file("a"), file("a/b")),
        (file("A/a"), file("a/b")),
    ):
        with pytest.raises(ValueError):
            validate_candidate_files(files)
    corrupted = file("a")
    object.__setattr__(corrupted, "content", b"tamper")
    with pytest.raises(ValueError):
        validate_candidate_files((corrupted,))


def test_file_manifest_enforces_count_file_and_total_byte_limits() -> None:
    with pytest.raises(ValueError):
        validate_candidate_files(tuple(file(f"f{i:04d}", b"") for i in range(513)))
    with pytest.raises(ValueError):
        validate_candidate_files((file("large", b"x" * (2 * 1024**2 + 1)),))
    with pytest.raises(ValueError):
        validate_candidate_files(
            tuple(file(f"f{i}", b"x" * (2 * 1024**2)) for i in range(5))
        )
    validate_candidate_files(
        tuple(file(f"f{i}", b"x" * (2 * 1024**2)) for i in range(4))
    )


def test_candidate_revision_is_hash_of_complete_git_object_not_just_message() -> None:
    candidate = snapshot()
    raw = (
        b"commit "
        + str(len(candidate.raw_commit)).encode()
        + b"\x00"
        + candidate.raw_commit
    )
    assert hashlib.sha1(raw, usedforsecurity=False).hexdigest() == candidate.revision
    assert (
        hashlib.sha1(candidate.raw_commit, usedforsecurity=False).hexdigest()
        != candidate.revision
    )
