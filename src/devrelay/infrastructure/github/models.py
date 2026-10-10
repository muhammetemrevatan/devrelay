from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

type Revision = Annotated[str, Field(pattern=r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")]


class RepositoryIdentity(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore", hide_input_in_errors=True)
    id: int = Field(gt=0, le=2**63 - 1)
    full_name: str = Field(min_length=3, max_length=256)


class RepositoryMetadata(RepositoryIdentity):
    archived: bool
    disabled: bool


class CommitObject(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore", hide_input_in_errors=True)
    type: Literal["commit"]
    sha: Revision


class GitReference(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore", hide_input_in_errors=True)
    ref: str = Field(min_length=1, max_length=300)
    object: CommitObject


class PullRequestBranch(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore", hide_input_in_errors=True)
    ref: str = Field(min_length=1, max_length=256)
    sha: Revision
    repo: RepositoryIdentity | None


class PullRequest(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore", hide_input_in_errors=True)
    number: int = Field(gt=0, le=2**63 - 1)
    state: Literal["open"]
    head: PullRequestBranch
    base: PullRequestBranch


PULL_REQUEST_PAGE = TypeAdapter(Annotated[list[PullRequest], Field(max_length=100)])
