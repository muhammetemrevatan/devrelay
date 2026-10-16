"""Build verified shallow Git metadata without running Git or repository code."""

import hashlib
import json
import re
import struct
import zlib
from dataclasses import dataclass, field

from devrelay.application.contracts.base_sources import (
    MAX_BASE_DEPTH,
    MAX_BASE_FILES,
    MAX_BASE_TOTAL_BYTES,
    BaseSourceFile,
)
from devrelay.application.contracts.git_sources import GitBaseSourceSnapshot
from devrelay.application.contracts.workspace_recovery import canonical_workspace_path

_MAX_METADATA_FILES = MAX_BASE_FILES * (MAX_BASE_DEPTH + 1) + 8
_MAX_METADATA_BYTES = MAX_BASE_TOTAL_BYTES + 8 * 1024**2
_CONFIG = b"""[core]
    repositoryFormatVersion = 0
    bare = false
    filemode = false
    ignorecase = true
    autocrlf = false
    safecrlf = false
    eol = lf
    symlinks = false
    protectNTFS = true
    fsmonitor = false
    untrackedCache = false
    logallrefupdates = false
    hooksPath = .git/hooks-disabled
[gc]
    auto = 0
[maintenance]
    auto = false
[protocol]
    allow = never
[credential]
    helper =
"""


def _metadata_path(path: str, *, directory: bool = False) -> None:
    if (
        type(path) is not str
        or not (path.startswith(".git/") or (directory and path == ".git"))
        or "\\" in path
    ):
        raise ValueError("Git metadata requires a generated literal internal path.")
    canonical_workspace_path("C:\\" + path.replace("/", "\\"))


def _branch(branch: str) -> None:
    if (
        type(branch) is not str
        or not 1 <= len(branch) <= 256
        or branch.startswith("/")
        or branch.endswith(("/", "."))
        or "//" in branch
        or ".." in branch
        or "@{" in branch
        or branch == "@"
        or re.search(r"[\x00-\x20\x7f~^:?*\[\\]", branch)
        or any(
            part.startswith(".") or part.casefold().endswith(".lock")
            for part in branch.split("/")
        )
    ):
        raise ValueError("Git workspace requires a safe authorized branch reference.")
    _metadata_path(".git/refs/heads/" + branch)


@dataclass(frozen=True, slots=True, kw_only=True)
class GitMetadataFile:
    path: str
    content: bytes = field(repr=False)

    def __post_init__(self) -> None:
        _metadata_path(self.path)
        if type(self.content) is not bytes or len(self.content) > _MAX_METADATA_BYTES:
            raise ValueError("Git metadata file exceeds its bounded representation.")


@dataclass(frozen=True, slots=True, kw_only=True)
class GitMetadata:
    files: tuple[GitMetadataFile, ...] = field(repr=False)
    directories: tuple[str, ...]

    def validate(self) -> None:
        if (
            type(self.files) is not tuple
            or not 1 <= len(self.files) <= _MAX_METADATA_FILES
        ):
            raise ValueError("Git metadata exceeds its bounded file count.")
        if (
            type(self.directories) is not tuple
            or len(self.directories) > _MAX_METADATA_FILES
        ):
            raise ValueError("Git metadata exceeds its bounded directory count.")
        if any(not isinstance(item, GitMetadataFile) for item in self.files):
            raise ValueError("Git metadata files require typed generated values.")
        paths = [item.path for item in self.files]
        if paths != sorted(set(paths), key=lambda item: item.encode("utf-8")) or list(
            self.directories
        ) != sorted(set(self.directories)):
            raise ValueError("Git metadata paths require canonical unique ordering.")
        seen: set[str] = set()
        total = 0
        for item in self.files:
            if not isinstance(item, GitMetadataFile):
                raise ValueError("Git metadata files require typed generated values.")
            item.__post_init__()
            total += len(item.content)
            if total > _MAX_METADATA_BYTES:
                raise ValueError("Git metadata exceeds its finite byte budget.")
        for path, directory in [(item.path, False) for item in self.files] + [
            (path, True) for path in self.directories
        ]:
            _metadata_path(path, directory=directory)
            folded = path.casefold()
            if folded in seen:
                raise ValueError("Git metadata paths cannot collide.")
            seen.add(folded)
        directories = set(self.directories)
        if any(
            "/" in path and path.rsplit("/", 1)[0] not in directories
            for path in (*paths, *self.directories)
        ):
            raise ValueError(
                "Git metadata paths require all generated parent directories."
            )

    def fingerprint(self) -> str:
        self.validate()
        payload = {
            "version": 1,
            "directories": self.directories,
            "files": [
                (item.path, len(item.content), hashlib.sha256(item.content).hexdigest())
                for item in self.files
            ],
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii")
        ).hexdigest()


def _index(files: tuple[BaseSourceFile, ...]) -> bytes:
    body = bytearray(b"DIRC" + struct.pack("!II", 2, len(files)))
    for item in files:
        path = item.path.encode("utf-8")
        entry = struct.pack(
            "!10I", 0, 0, 0, 0, 0, 0, int(item.mode.value, 8), 0, 0, len(item.content)
        )
        entry += (
            bytes.fromhex(item.blob_sha)
            + struct.pack("!H", min(len(path), 0xFFF))
            + path
        )
        entry += b"\x00" * (8 - len(entry) % 8)
        body.extend(entry)
    return bytes(body) + hashlib.sha1(body, usedforsecurity=False).digest()


def build_git_metadata(source: GitBaseSourceSnapshot, branch: str) -> GitMetadata:
    """Create exact loose objects and initial v2 index for a depth-one checkout.

    The actual original commit bytes are retained; no commit is reconstructed.
    Literal attributes deliberately disable conversion/filter/ident processing.
    History, remotes, hooks and executable policy are outside this local profile.
    """
    if not isinstance(source, GitBaseSourceSnapshot):
        raise ValueError("Git workspace requires a verified original commit export.")
    source.validate()
    _branch(branch)
    snapshot = source.snapshot
    objects: dict[str, bytes] = {}

    def add(kind: bytes, data: bytes) -> str:
        raw = kind + b" " + str(len(data)).encode("ascii") + b"\x00" + data
        sha = hashlib.sha1(raw, usedforsecurity=False).hexdigest()
        compressed = zlib.compress(raw)
        if sha in objects and objects[sha] != compressed:
            raise ValueError("Git object identity conflicts with original content.")
        objects[sha] = compressed
        return sha

    entries: dict[str, dict[str, tuple[str, str]]] = {"": {}}
    for item in snapshot.files:
        if add(b"blob", item.content) != item.blob_sha:
            raise ValueError("Git blob differs from original source pins.")
        parts = item.path.split("/")
        parent = ""
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
        return add(b"tree", bytes(body))

    if tree("") != snapshot.root_tree_sha:
        raise ValueError("Git source tree differs from accepted root tree.")
    if add(b"commit", source.raw_commit) != snapshot.request.base_revision:
        raise ValueError("Git original commit differs from accepted base.")
    files = {
        ".git/HEAD": b"ref: refs/heads/" + branch.encode("utf-8") + b"\n",
        ".git/config": _CONFIG,
        ".git/shallow": snapshot.request.base_revision.encode("ascii") + b"\n",
        ".git/refs/heads/" + branch: snapshot.request.base_revision.encode("ascii")
        + b"\n",
        ".git/index": _index(snapshot.files),
        ".git/info/attributes": b"* -text -filter -ident -working-tree-encoding\n",
        **{
            ".git/objects/" + sha[:2] + "/" + sha[2:]: content
            for sha, content in objects.items()
        },
    }
    directories = {".git/hooks-disabled"}
    for path in files:
        parts = path.split("/")
        directories.update("/".join(parts[:index]) for index in range(1, len(parts)))
    result = GitMetadata(
        files=tuple(
            GitMetadataFile(path=path, content=content)
            for path, content in sorted(
                files.items(), key=lambda pair: pair[0].encode("utf-8")
            )
        ),
        directories=tuple(sorted(directories)),
    )
    result.validate()
    return result
