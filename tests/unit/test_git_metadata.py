import hashlib
import struct
import zlib
from dataclasses import replace

import pytest

from devrelay.application.contracts.base_sources import git_blob_sha
from devrelay.application.contracts.git_sources import GitBaseSourceSnapshot
from devrelay.infrastructure.workspace.git_metadata import build_git_metadata
from tests.base_source_fixtures import source_fixture
from tests.workspace_fixtures import workspace_binding


def git_source() -> GitBaseSourceSnapshot:
    source = source_fixture(workspace_binding()).snapshot
    raw = (
        b"tree "
        + source.root_tree_sha.encode("ascii")
        + b"\n"
        + b"parent "
        + b"b" * 40
        + b"\n"
        + b"author Fixture Author <fixture@example.invalid> 1700000000 +0545\n"
        + b"committer Fixture Author <fixture@example.invalid> 1700000000 -0330\n"
        + b"encoding ISO-8859-1\n"
        + b"gpgsig -----BEGIN PGP SIGNATURE-----\n fixture-signature\n -----END PGP SIGNATURE-----\n\n"
        + b"Original caf\xe9 commit bytes\n"
    )
    sha = hashlib.sha1(
        b"commit " + str(len(raw)).encode("ascii") + b"\x00" + raw,
        usedforsecurity=False,
    ).hexdigest()
    snapshot = replace(
        source, request=replace(source.request, base_revision=sha), raw_commit=raw
    )
    return GitBaseSourceSnapshot(snapshot=snapshot, raw_commit=raw)


def test_builder_preserves_true_original_commit_and_matching_complete_object_graph() -> (
    None
):
    source = git_source()

    metadata = build_git_metadata(source, "devrelay/fixture-task")

    files = {item.path: item.content for item in metadata.files}
    commit_sha = source.snapshot.request.base_revision
    assert (
        zlib.decompress(files[f".git/objects/{commit_sha[:2]}/{commit_sha[2:]}"])
        == b"commit "
        + str(len(source.raw_commit)).encode("ascii")
        + b"\x00"
        + source.raw_commit
    )
    assert files[".git/HEAD"] == b"ref: refs/heads/devrelay/fixture-task\n"
    assert files[".git/refs/heads/devrelay/fixture-task"] == commit_sha.encode() + b"\n"
    assert files[".git/shallow"] == commit_sha.encode() + b"\n"
    assert ".git/hooks-disabled" in metadata.directories
    for path, compressed in files.items():
        if path.startswith(".git/objects/"):
            raw = zlib.decompress(compressed)
            assert hashlib.sha1(
                raw, usedforsecurity=False
            ).hexdigest() == path.removeprefix(".git/objects/").replace("/", "")
    for item in source.snapshot.files:
        assert (
            zlib.decompress(
                files[f".git/objects/{item.blob_sha[:2]}/{item.blob_sha[2:]}"]
            )
            == b"blob " + str(len(item.content)).encode() + b"\x00" + item.content
        )


def test_index_v2_has_exact_sorted_paths_modes_blob_shas_sizes_padding_and_checksum() -> (
    None
):
    source = git_source()
    metadata = build_git_metadata(source, "devrelay/fixture-task")
    index = next(item.content for item in metadata.files if item.path == ".git/index")

    assert index[:12] == b"DIRC" + struct.pack("!II", 2, len(source.snapshot.files))
    assert hashlib.sha1(index[:-20], usedforsecurity=False).digest() == index[-20:]
    offset = 12
    for item in source.snapshot.files:
        start = offset
        fields = struct.unpack("!10I", index[offset : offset + 40])
        assert fields[:6] == (0,) * 6 and fields[6] == int(item.mode.value, 8)
        assert fields[7:] == (0, 0, len(item.content))
        assert index[offset + 40 : offset + 60] == bytes.fromhex(item.blob_sha)
        length = struct.unpack("!H", index[offset + 60 : offset + 62])[0]
        path = item.path.encode("utf-8")
        assert length == len(path)
        assert index[offset + 62 : offset + 62 + length] == path
        offset = start + ((62 + length) // 8 + 1) * 8
        assert not any(index[start + 62 + length : offset])
    assert offset == len(index) - 20


def test_generated_profile_disables_literal_conversions_and_has_no_remote_configuration() -> (
    None
):
    metadata = build_git_metadata(git_source(), "devrelay/fixture-task")
    files = {item.path: item.content for item in metadata.files}

    assert (
        files[".git/info/attributes"]
        == b"* -text -filter -ident -working-tree-encoding\n"
    )
    assert b"autocrlf = false" in files[".git/config"]
    assert b"fsmonitor = false" in files[".git/config"]
    assert b"allow = never" in files[".git/config"]
    assert not any(
        word in files[".git/config"] for word in (b"remote", b"include", b"url")
    )
    assert not any(path.endswith("alternates") or "/hooks/" in path for path in files)


@pytest.mark.parametrize(
    "branch",
    [
        "../escape",
        "/root",
        "root/",
        "a//b",
        "a..b",
        "a.lock",
        "a.LOCK/b",
        ".hidden/name",
        "a/.hidden",
        "a@{b",
        "a?b",
        "a*b",
        "a[b",
        "a:b",
        "a\\b",
        "a b",
        "a\nb",
        "a.",
        "@",
        "CON",
        "a/AUX.txt",
        "a" * 257,
    ],
)
def test_invalid_git_or_windows_reference_paths_fail_before_metadata_output(
    branch: str,
) -> None:
    source = git_source()

    with pytest.raises(ValueError):
        build_git_metadata(source, branch)


def test_authorized_branch_can_exceed_coding_prefix_length_budget() -> None:
    branch = "a" * 144 + "/12345678-1234-1234-1234-123456789abc"

    metadata = build_git_metadata(git_source(), branch)

    files = {item.path: item.content for item in metadata.files}
    assert len(branch) == 181
    assert files[".git/HEAD"] == b"ref: refs/heads/" + branch.encode("ascii") + b"\n"
    assert ".git/refs/heads/" + branch in files


def test_raw_commit_damage_is_revalidated_instead_of_reconstructing_an_object() -> None:
    source = git_source()
    object.__setattr__(source, "raw_commit", source.raw_commit + b"changed")

    with pytest.raises(ValueError):
        build_git_metadata(source, "devrelay/fixture-task")


def test_manifest_root_mismatch_cannot_become_a_checkout() -> None:
    source = git_source()
    first = source.snapshot.files[0]
    changed = replace(first, content=b"changed", blob_sha=git_blob_sha(b"changed"))
    snapshot = replace(source.snapshot, files=(changed, *source.snapshot.files[1:]))
    source = replace(source, snapshot=snapshot)

    with pytest.raises(ValueError, match="root tree"):
        build_git_metadata(source, "devrelay/fixture-task")


def test_metadata_is_deterministic_for_identical_pinned_input() -> None:
    source = git_source()

    first = build_git_metadata(source, "devrelay/fixture-task")
    second = build_git_metadata(source, "devrelay/fixture-task")

    assert first == second and first.fingerprint() == second.fingerprint()


def test_empty_root_still_has_real_tree_commit_and_empty_index() -> None:
    original = git_source()
    root = hashlib.sha1(b"tree 0\x00", usedforsecurity=False).hexdigest()
    raw = b"tree " + root.encode() + b"\n" + original.raw_commit.split(b"\n", 1)[1]
    sha = hashlib.sha1(
        b"commit " + str(len(raw)).encode() + b"\x00" + raw, usedforsecurity=False
    ).hexdigest()
    snapshot = replace(
        original.snapshot,
        files=(),
        root_tree_sha=root,
        request=replace(original.snapshot.request, base_revision=sha),
        raw_commit=raw,
    )
    source = GitBaseSourceSnapshot(snapshot=snapshot, raw_commit=raw)

    metadata = build_git_metadata(source, "devrelay/empty")

    files = {item.path: item.content for item in metadata.files}
    assert (
        zlib.decompress(files[f".git/objects/{root[:2]}/{root[2:]}"]) == b"tree 0\x00"
    )
    assert files[".git/index"][:12] == b"DIRC" + struct.pack("!II", 2, 0)
    assert len(files[".git/index"]) == 32
