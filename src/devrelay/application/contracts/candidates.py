"""Canonical real candidate commit values; no process or publication authority."""

import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass, field
from uuid import UUID

from devrelay.application.contracts.base_sources import (
    BaseSourceFile,
    BaseSourceRequest,
)
from devrelay.application.contracts.git_objects import GitObject, build_git_tree
from devrelay.application.contracts.workspace_recovery import canonical_workspace_path

MAX_CANDIDATE_MESSAGE_BYTES = 16 * 1024


def _sha(value: object) -> None:
    if type(value) is not str or re.fullmatch(r"[0-9a-f]{40}", value) is None:
        raise ValueError("Candidate requires canonical SHA-1 object identities.")


def validate_candidate_branch(branch: str) -> None:
    if (
        type(branch) is not str
        or not 1 <= len(branch) <= 256
        or unicodedata.normalize("NFC", branch) != branch
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
        raise ValueError("Candidate requires a safe authorized branch.")
    canonical_workspace_path("C:\\.git\\refs\\heads\\" + branch.replace("/", "\\"))


@dataclass(frozen=True, slots=True, kw_only=True)
class CandidateCommitIdentity:
    name: str
    email: str

    def __post_init__(self) -> None:
        for value, bound in ((self.name, 256), (self.email, 254)):
            if (
                type(value) is not str
                or not value.strip()
                or value != value.strip()
                or len(value.encode("utf-8")) > bound
                or unicodedata.normalize("NFC", value) != value
                or any(unicodedata.category(char).startswith("C") for char in value)
                or any(char in value for char in "<>")
            ):
                raise ValueError("Candidate commit identity must be explicit and safe.")
        if re.fullmatch(r"[^\s@]+@[^\s@]+", self.email) is None:
            raise ValueError("Candidate commit identity requires an email address.")

    def encoded(self) -> bytes:
        self.__post_init__()
        return (self.name + " <" + self.email + ">").encode("utf-8")


@dataclass(frozen=True, slots=True, kw_only=True)
class CandidateCommitSpec:
    author: CandidateCommitIdentity
    committer: CandidateCommitIdentity
    timestamp: int
    message: str = field(repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.author, CandidateCommitIdentity) or not isinstance(
            self.committer, CandidateCommitIdentity
        ):
            raise ValueError("Candidate requires configured author and committer.")
        self.author.__post_init__()
        self.committer.__post_init__()
        if (
            type(self.timestamp) is not int
            or not 0 <= self.timestamp <= 253402300799
            or type(self.message) is not str
            or not self.message.strip()
            or not self.message.endswith("\n")
            or self.message.endswith("\n\n")
            or "\r" in self.message
            or "\x00" in self.message
            or len(self.message.encode("utf-8")) > MAX_CANDIDATE_MESSAGE_BYTES
        ):
            raise ValueError(
                "Candidate requires whole UTC seconds and bounded LF message."
            )

    def raw_commit(self, *, parent: str, tree: str) -> bytes:
        self.__post_init__()
        _sha(parent)
        _sha(tree)
        date = str(self.timestamp).encode("ascii") + b" +0000"
        return (
            b"tree "
            + tree.encode("ascii")
            + b"\nparent "
            + parent.encode("ascii")
            + b"\nauthor "
            + self.author.encoded()
            + b" "
            + date
            + b"\ncommitter "
            + self.committer.encoded()
            + b" "
            + date
            + b"\n\n"
            + self.message.encode("utf-8")
        )


def parse_candidate_commit(
    raw: bytes, *, parent: str, tree: str
) -> CandidateCommitSpec:
    """Accept exactly the canonical unsigned profile, then rebuild byte-for-byte."""
    _sha(parent)
    _sha(tree)
    if type(raw) is not bytes or len(raw) > MAX_CANDIDATE_MESSAGE_BYTES + 2048:
        raise ValueError("Candidate raw commit exceeds its canonical profile.")
    try:
        header, message = raw.decode("utf-8").split("\n\n", 1)
        lines = header.split("\n")
        if len(lines) != 4 or lines[:2] != ["tree " + tree, "parent " + parent]:
            raise ValueError
        identities: list[CandidateCommitIdentity] = []
        timestamps: list[int] = []
        for line, prefix in zip(lines[2:], ("author ", "committer "), strict=True):
            match = re.fullmatch(
                re.escape(prefix) + r"([^<>]+) <([^<>]+)> (0|[1-9][0-9]{0,11}) \+0000",
                line,
            )
            if match is None:
                raise ValueError
            identities.append(CandidateCommitIdentity(name=match[1], email=match[2]))
            timestamps.append(int(match[3]))
        if timestamps[0] != timestamps[1]:
            raise ValueError
        spec = CandidateCommitSpec(
            author=identities[0],
            committer=identities[1],
            timestamp=timestamps[0],
            message=message,
        )
        if spec.raw_commit(parent=parent, tree=tree) != raw:
            raise ValueError
        return spec
    except UnicodeError, ValueError, TypeError:
        raise ValueError(
            "Candidate commit is outside the canonical unsigned profile."
        ) from None


@dataclass(frozen=True, slots=True, kw_only=True)
class CandidateSnapshot:
    execution_id: UUID
    cycle: int
    request: BaseSourceRequest
    authorized_branch: str
    root_tree_sha: str
    revision: str
    raw_commit: bytes = field(repr=False)
    files: tuple[BaseSourceFile, ...] = field(repr=False)

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        if (
            not isinstance(self.execution_id, UUID)
            or type(self.cycle) is not int
            or not 0 <= self.cycle <= 5
            or not isinstance(self.request, BaseSourceRequest)
        ):
            raise ValueError(
                "Candidate requires exact execution and original source pins."
            )
        self.request.__post_init__()
        validate_candidate_branch(self.authorized_branch)
        _sha(self.root_tree_sha)
        _sha(self.revision)
        if build_git_tree(self.files).root_tree_sha != self.root_tree_sha:
            raise ValueError(
                "Candidate tree differs from independently verified files."
            )
        parse_candidate_commit(
            self.raw_commit, parent=self.request.base_revision, tree=self.root_tree_sha
        )
        if GitObject(kind="commit", data=self.raw_commit).sha != self.revision:
            raise ValueError("Candidate revision differs from actual raw commit bytes.")

    def fingerprint(self) -> str:
        self.validate()
        payload = (
            "devrelay.candidate.v1",
            str(self.execution_id),
            self.cycle,
            str(self.request.repository_id),
            self.request.provider_repository_id,
            self.request.repository_slug,
            self.request.base_revision,
            self.authorized_branch,
            self.root_tree_sha,
            self.revision,
            hashlib.sha256(self.raw_commit).hexdigest(),
            [(item.path, item.mode.value, item.blob_sha) for item in self.files],
        )
        return hashlib.sha256(
            json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode(
                "ascii"
            )
        ).hexdigest()
