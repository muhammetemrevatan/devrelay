"""Bounded ordinary Git objects shared by candidate builders and verifiers."""

import hashlib
from dataclasses import dataclass, field
from typing import Literal

from devrelay.application.contracts.base_sources import BaseSourceFile

MAX_CANDIDATE_FILES = 512
MAX_CANDIDATE_FILE_BYTES = 2 * 1024**2
MAX_CANDIDATE_TOTAL_BYTES = 8 * 1024**2
_MAX_OBJECT_BYTES = MAX_CANDIDATE_TOTAL_BYTES + 1024**2


@dataclass(frozen=True, slots=True, kw_only=True)
class GitObject:
    kind: Literal["blob", "tree", "commit"]
    data: bytes = field(repr=False)

    def __post_init__(self) -> None:
        if (
            type(self.kind) is not str
            or self.kind not in ("blob", "tree", "commit")
            or (type(self.data) is not bytes or len(self.data) > _MAX_OBJECT_BYTES)
        ):
            raise ValueError("Git object requires bounded typed bytes and kind.")

    def raw(self) -> bytes:
        self.__post_init__()
        return (
            self.kind.encode("ascii")
            + b" "
            + str(len(self.data)).encode("ascii")
            + b"\x00"
            + self.data
        )

    @property
    def sha(self) -> str:
        return hashlib.sha1(self.raw(), usedforsecurity=False).hexdigest()


def validate_candidate_files(files: tuple[BaseSourceFile, ...]) -> None:
    if type(files) is not tuple or len(files) > MAX_CANDIDATE_FILES:
        raise ValueError("Candidate exceeds its bounded ordinary file count.")
    names: dict[str, str] = {}
    leaves: set[str] = set()
    directories: set[str] = set()
    total = 0
    previous: bytes | None = None
    for item in files:
        if not isinstance(item, BaseSourceFile):
            raise ValueError("Candidate requires typed ordinary source files.")
        item.__post_init__()
        if len(item.content) > MAX_CANDIDATE_FILE_BYTES:
            raise ValueError("Candidate file exceeds its byte budget.")
        total += len(item.content)
        if total > MAX_CANDIDATE_TOTAL_BYTES:
            raise ValueError("Candidate exceeds its total byte budget.")
        encoded = item.path.encode("utf-8")
        if previous is not None and encoded <= previous:
            raise ValueError("Candidate files require unique canonical ordering.")
        previous = encoded
        parts = item.path.split("/")
        for index in range(1, len(parts) + 1):
            path = "/".join(parts[:index])
            folded = path.casefold()
            if folded in names and names[folded] != path:
                raise ValueError("Candidate paths contain ambiguous aliases.")
            names[folded] = path
            if index < len(parts):
                directories.add(folded)
        leaves.add(item.path.casefold())
    if leaves & directories:
        raise ValueError("Candidate file conflicts with a directory.")


@dataclass(frozen=True, slots=True, kw_only=True)
class GitTreeObjects:
    root_tree_sha: str
    objects: tuple[GitObject, ...] = field(repr=False)


def build_git_tree(files: tuple[BaseSourceFile, ...]) -> GitTreeObjects:
    """Reconstruct literal blob/tree identities, including the empty root tree."""
    validate_candidate_files(files)
    objects: dict[str, GitObject] = {}
    entries: dict[str, dict[str, tuple[str, str]]] = {"": {}}

    def add(item: GitObject) -> str:
        sha = item.sha
        if sha in objects and objects[sha] != item:
            raise ValueError("Git object identity conflicts with content.")
        objects[sha] = item
        return sha

    for item in files:
        if add(GitObject(kind="blob", data=item.content)) != item.blob_sha:
            raise ValueError("Candidate blob differs from its actual bytes.")
        parent = ""
        parts = item.path.split("/")
        for part in parts[:-1]:
            child = parent + part + "/"
            entries.setdefault(child, {})
            entries[parent][part] = ("40000", child)
            parent = child
        entries[parent][parts[-1]] = (item.mode.value, item.blob_sha)

    def tree(prefix: str) -> str:
        body = bytearray()
        for name, (mode, value) in sorted(
            entries[prefix].items(),
            key=lambda pair: (
                pair[0].encode("utf-8") + (b"/" if pair[1][0] == "40000" else b"")
            ),
        ):
            sha = tree(value) if mode == "40000" else value
            body.extend(
                mode.encode("ascii")
                + b" "
                + name.encode("utf-8")
                + b"\x00"
                + bytes.fromhex(sha)
            )
        return add(GitObject(kind="tree", data=bytes(body)))

    root = tree("")
    return GitTreeObjects(
        root_tree_sha=root,
        objects=tuple(objects[sha] for sha in sorted(objects)),
    )
