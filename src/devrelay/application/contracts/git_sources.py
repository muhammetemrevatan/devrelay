import re
from dataclasses import dataclass, field

from devrelay.application.contracts.base_sources import (
    BaseMaterialization,
    BaseSourceSnapshot,
)
from devrelay.application.contracts.git_commits import (
    MAX_GIT_COMMIT_BYTES as MAX_GIT_COMMIT_BYTES,
)
from devrelay.application.contracts.git_commits import (
    validate_git_commit as validate_git_commit,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class GitBaseSourceSnapshot:
    snapshot: BaseSourceSnapshot
    raw_commit: bytes = field(repr=False)

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        if not isinstance(self.snapshot, BaseSourceSnapshot):
            raise ValueError("Git source requires a verified source manifest.")
        self.snapshot.validate()
        validate_git_commit(
            self.raw_commit,
            self.snapshot.request.base_revision,
            self.snapshot.root_tree_sha,
        )
        if (
            self.snapshot.raw_commit is not None
            and self.snapshot.raw_commit != self.raw_commit
        ):
            raise ValueError("Git source changed original commit bytes.")


@dataclass(frozen=True, slots=True, kw_only=True)
class GitWorkspacePreparation:
    """Process-local metadata observation; no launch, restart or release authority."""

    materialization: BaseMaterialization
    branch: str
    metadata_digest: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.materialization, BaseMaterialization)
            or type(self.branch) is not str
            or not 1 <= len(self.branch) <= 256
            or type(self.metadata_digest) is not str
            or re.fullmatch(r"[0-9a-f]{64}", self.metadata_digest) is None
        ):
            raise ValueError("Git preparation requires original bounded pins.")
