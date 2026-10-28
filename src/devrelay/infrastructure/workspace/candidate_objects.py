"""Pure candidate construction and safe generated metadata, without executing Git."""

import hashlib
import struct
import zlib
from uuid import UUID

from devrelay.application.contracts.base_sources import BaseSourceFile
from devrelay.application.contracts.candidates import (
    CandidateCommitSpec,
    CandidateSnapshot,
)
from devrelay.application.contracts.git_objects import GitObject, build_git_tree
from devrelay.application.contracts.git_sources import GitBaseSourceSnapshot
from devrelay.application.contracts.workspace_recovery import canonical_workspace_path
from devrelay.infrastructure.workspace.git_metadata import (
    GitMetadata,
    GitMetadataFile,
    build_git_metadata,
)


def build_candidate_snapshot(
    *,
    execution_id: UUID,
    cycle: int,
    source: GitBaseSourceSnapshot,
    branch: str,
    files: tuple[BaseSourceFile, ...],
    commit: CandidateCommitSpec,
) -> CandidateSnapshot:
    if not isinstance(source, GitBaseSourceSnapshot) or not isinstance(
        commit, CandidateCommitSpec
    ):
        raise ValueError(
            "Candidate builder requires typed original source and identity."
        )
    source.validate()
    # Also independently verify the accepted base manifest's claimed tree.
    build_git_metadata(source, branch)
    tree = build_git_tree(files)
    if tree.root_tree_sha == source.snapshot.root_tree_sha:
        raise ValueError("A no-change candidate is unsupported by this profile.")
    raw = commit.raw_commit(
        parent=source.snapshot.request.base_revision, tree=tree.root_tree_sha
    )
    return CandidateSnapshot(
        execution_id=execution_id,
        cycle=cycle,
        request=source.snapshot.request,
        authorized_branch=branch,
        root_tree_sha=tree.root_tree_sha,
        revision=GitObject(kind="commit", data=raw).sha,
        raw_commit=raw,
        files=files,
    )


def _index(files: tuple[BaseSourceFile, ...]) -> bytes:
    # Same literal v2 format as base preparation; no inherited stat-cache claims.
    body = bytearray(b"DIRC" + struct.pack("!II", 2, len(files)))
    for item in files:
        path = item.path.encode("utf-8")
        entry = struct.pack(
            "!10I", 0, 0, 0, 0, 0, 0, int(item.mode.value, 8), 0, 0, len(item.content)
        )
        entry += bytes.fromhex(item.blob_sha) + struct.pack("!H", len(path)) + path
        entry += b"\x00" * (8 - len(entry) % 8)
        body.extend(entry)
    return bytes(body) + hashlib.sha1(body, usedforsecurity=False).digest()


def _config(*, git_directory: str, worktree: str) -> bytes:
    canonical_workspace_path(worktree)
    canonical_workspace_path(git_directory)
    prefix = worktree + "\\.git\\devrelay\\"
    if not git_directory.startswith(prefix):
        raise ValueError(
            "Candidate metadata requires its exact private capture directory."
        )
    suffix = git_directory.removeprefix(prefix)
    try:
        if str(UUID(suffix)) != suffix:
            raise ValueError
    except ValueError:
        raise ValueError(
            "Candidate metadata requires a canonical capture UUID."
        ) from None
    root = git_directory.replace("\\", "/")
    work = worktree.replace("\\", "/")
    return (
        "[core]\n"
        "    repositoryFormatVersion = 0\n"
        "    bare = false\n"
        '    worktree = "' + work + '"\n'
        "    filemode = false\n"
        "    ignorecase = true\n"
        "    autocrlf = false\n"
        "    safecrlf = false\n"
        "    eol = lf\n"
        "    symlinks = false\n"
        "    protectNTFS = true\n"
        "    fsmonitor = false\n"
        "    untrackedCache = false\n"
        "    logallrefupdates = false\n"
        '    hooksPath = "' + root + '/hooks-disabled"\n'
        "[gc]\n    auto = 0\n"
        "[maintenance]\n    auto = false\n"
        "[protocol]\n    allow = never\n"
        "[credential]\n    helper =\n"
    ).encode("utf-8")


def build_candidate_metadata(
    source: GitBaseSourceSnapshot,
    candidate: CandidateSnapshot,
    *,
    git_directory: str,
    worktree: str,
) -> GitMetadata:
    """Generate metadata for remapping into a fresh retained capture GIT_DIR.

    No supplied .git content is read. Native consumers must create these bytes
    under the exact guarded git_directory and pass explicit GIT_DIR/GIT_WORK_TREE
    plus their fixed config/environment policy to checks. Paths alone grant no
    filesystem capability, isolation, replay or process launch permission.
    """
    if not isinstance(source, GitBaseSourceSnapshot) or not isinstance(
        candidate, CandidateSnapshot
    ):
        raise ValueError(
            "Candidate metadata requires original source and checked value."
        )
    candidate.validate()
    if candidate.request != source.snapshot.request:
        raise ValueError("Candidate metadata changed the accepted source identity.")
    base = build_git_metadata(source, candidate.authorized_branch)
    if candidate.root_tree_sha == source.snapshot.root_tree_sha:
        raise ValueError("A no-change candidate is unsupported by this profile.")
    files = {item.path: item.content for item in base.files}
    tree = build_git_tree(candidate.files)
    for item in (*tree.objects, GitObject(kind="commit", data=candidate.raw_commit)):
        path = ".git/objects/" + item.sha[:2] + "/" + item.sha[2:]
        compressed = zlib.compress(item.raw())
        if path in files and zlib.decompress(files[path]) != item.raw():
            raise ValueError("Candidate object conflicts with accepted original bytes.")
        files[path] = compressed
    files[".git/refs/heads/" + candidate.authorized_branch] = (
        candidate.revision.encode("ascii") + b"\n"
    )
    files[".git/index"] = _index(candidate.files)
    files[".git/config"] = _config(git_directory=git_directory, worktree=worktree)
    # The original base remains the shallow boundary; candidate has its real parent.
    directories = {".git/hooks-disabled"}
    for path in files:
        parts = path.split("/")
        directories.update("/".join(parts[:index]) for index in range(1, len(parts)))
    result = GitMetadata(
        files=tuple(
            GitMetadataFile(path=path, content=content)
            for path, content in sorted(
                files.items(), key=lambda pair: pair[0].encode()
            )
        ),
        directories=tuple(sorted(directories)),
    )
    result.validate()
    return result
