"""Broker publication values; no provider credential or recovered write right."""

import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from uuid import UUID

from devrelay.application.contracts.coding_result_receipts import (
    CodingPublicationState,
    CodingPublicationStep,
    CodingResultStatus,
    _digest,
)
from devrelay.application.contracts.git_objects import GitObject
from devrelay.domain.jobs.values import PullRequestReference, _branch, _utc, _uuid


class PublicationWriteKind(StrEnum):
    BLOB = "blob"
    TREE = "tree"
    COMMIT = "commit"
    BRANCH = "branch"
    PULL_REQUEST = "pull_request"


@dataclass(frozen=True, slots=True, kw_only=True)
class PublicationPolicy:
    max_writes: int = 128
    max_requests: int = 1024
    max_request_bytes: int = 16 * 1024**2
    max_response_bytes: int = 4 * 1024**2
    max_total_response_bytes: int = 32 * 1024**2
    total_timeout_seconds: float = 90
    request_timeout_seconds: float = 15
    dispatch_margin_seconds: float = 0.25

    def __post_init__(self) -> None:
        from math import isfinite

        for value, minimum, maximum in (
            (self.max_writes, 3, 512),
            (self.max_requests, 16, 4096),
            (self.max_request_bytes, 1024, 32 * 1024**2),
            (self.max_response_bytes, 1024, 4 * 1024**2),
            (self.max_total_response_bytes, 4096, 64 * 1024**2),
        ):
            if type(value) is not int or not minimum <= value <= maximum:
                raise ValueError("Publication budgets require bounded integers.")
        for duration, lower, upper in (
            (self.total_timeout_seconds, 1, 120),
            (self.request_timeout_seconds, 0.1, 60),
            (self.dispatch_margin_seconds, 0.01, 5),
        ):
            if (
                type(duration) not in (int, float)
                or not isfinite(duration)
                or not lower <= duration <= upper
            ):
                raise ValueError("Publication time budgets must be finite.")
        if self.request_timeout_seconds > self.total_timeout_seconds:
            raise ValueError("Publication request timeout exceeds its total budget.")


@dataclass(frozen=True, slots=True, kw_only=True)
class PublicationWrite:
    write_id: UUID
    kind: PublicationWriteKind
    path: str
    body: bytes = field(repr=False)
    expected_sha: str | None = None
    input_digest: str = field(init=False)

    def __post_init__(self) -> None:
        _uuid(self.write_id)
        if (
            not isinstance(self.kind, PublicationWriteKind)
            or type(self.path) is not str
            or re.fullmatch(
                r"repos/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/(git/(blobs|trees|commits|refs)|pulls)",
                self.path,
            )
            is None
            or type(self.body) is not bytes
            or not 1 <= len(self.body) <= 4 * 1024**2
            or (self.expected_sha is None)
            != (self.kind is PublicationWriteKind.PULL_REQUEST)
        ):
            raise ValueError("Publication write requires bounded fixed input.")
        if self.expected_sha is not None:
            _digest(self.expected_sha, 40)
        material = (
            self.kind.value.encode("ascii")
            + b"\0"
            + self.path.encode("ascii")
            + b"\0"
            + self.body
        )
        object.__setattr__(self, "input_digest", hashlib.sha256(material).hexdigest())


@dataclass(frozen=True, slots=True, kw_only=True)
class PublicationPlan:
    result_id: UUID
    repository_id: UUID
    provider_repository_id: int
    repository_slug: str
    source_branch: str
    target_branch: str
    head_revision: str
    base_revision: str
    objects: tuple[GitObject, ...] = field(repr=False)
    writes: tuple[PublicationWrite, ...] = field(repr=False)
    policy: PublicationPolicy

    def __post_init__(self) -> None:
        for identity in (self.result_id, self.repository_id):
            _uuid(identity)
        if (
            type(self.provider_repository_id) is not int
            or not 1 <= self.provider_repository_id < 2**63
            or type(self.repository_slug) is not str
            or re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", self.repository_slug)
            is None
            or not isinstance(self.policy, PublicationPolicy)
            or type(self.objects) is not tuple
            or type(self.writes) is not tuple
        ):
            raise ValueError("Publication plan requires pinned repository input.")
        self.policy.__post_init__()
        _branch(self.source_branch)
        _branch(self.target_branch)
        _digest(self.head_revision, 40)
        _digest(self.base_revision, 40)
        if (
            self.source_branch == self.target_branch
            or not 3 <= len(self.writes) <= self.policy.max_writes
        ):
            raise ValueError("Publication plan exceeds its allowed write scope.")
        ids: set[UUID] = set()
        for write in self.writes:
            if not isinstance(write, PublicationWrite):
                raise ValueError("Publication plan requires typed writes.")
            write.__post_init__()
            if write.write_id in ids or not write.path.startswith(
                f"repos/{self.repository_slug}/"
            ):
                raise ValueError("Publication plan changed its fixed repository.")
            ids.add(write.write_id)
        if (
            tuple(write.kind for write in self.writes[-3:])
            != (
                PublicationWriteKind.COMMIT,
                PublicationWriteKind.BRANCH,
                PublicationWriteKind.PULL_REQUEST,
            )
            or any(
                write.kind not in (PublicationWriteKind.BLOB, PublicationWriteKind.TREE)
                for write in self.writes[:-3]
            )
            or sum(len(write.body) for write in self.writes)
            > self.policy.max_request_bytes
            or 4 * len(self.writes) + 8 > self.policy.max_requests
            or 2 * len(self.writes) + 4 > 2047
        ):
            raise ValueError(
                "Publication plan cannot fit its complete operation budget."
            )
        for item in self.objects:
            if not isinstance(item, GitObject):
                raise ValueError("Publication plan requires exact Git objects.")
            item.__post_init__()


@dataclass(frozen=True, slots=True, kw_only=True)
class PublicationObservation:
    write_id: UUID
    input_digest: str
    observation_digest: str
    pull_request_number: int | None = None

    def __post_init__(self) -> None:
        _uuid(self.write_id)
        _digest(self.input_digest)
        _digest(self.observation_digest)
        if self.pull_request_number is not None and (
            type(self.pull_request_number) is not int
            or not 1 <= self.pull_request_number < 2**63
        ):
            raise ValueError("Publication observation requires a valid PR number.")


@dataclass(frozen=True, slots=True, kw_only=True)
class ProviderPublication:
    reference: PullRequestReference
    provider_repository_id: int
    observed_at: datetime

    def __post_init__(self) -> None:
        if (
            not isinstance(self.reference, PullRequestReference)
            or type(self.provider_repository_id) is not int
            or not 1 <= self.provider_repository_id < 2**63
        ):
            raise ValueError("Publication requires verified provider association.")
        self.reference.__post_init__()
        object.__setattr__(self, "observed_at", _utc(self.observed_at))


@dataclass(frozen=True, slots=True, kw_only=True)
class PublicationHistory:
    status: CodingResultStatus
    invocation_id: UUID | None
    last_step: CodingPublicationStep | None = None
    last_observation_digest: str | None = None
    last_event_kind: str = "accepted"

    def __post_init__(self) -> None:
        if not isinstance(self.status, CodingResultStatus):
            raise ValueError("Publication history requires typed status.")
        self.status.__post_init__()
        if self.invocation_id is not None:
            _uuid(self.invocation_id)
        if self.last_step is not None:
            if not isinstance(self.last_step, CodingPublicationStep):
                raise ValueError("Publication history requires a typed original step.")
            self.last_step.__post_init__()
            if (self.last_step.result_id, self.last_step.invocation_id) != (
                self.status.receipt.result_id,
                self.invocation_id,
            ):
                raise ValueError("Publication history changed its original owner.")
        if self.last_observation_digest is not None:
            _digest(self.last_observation_digest)
        if self.last_event_kind not in (
            "accepted",
            "publishing_started",
            "write_planned",
            "write_observed",
            "uncertain",
            "finalized",
        ):
            raise ValueError("Publication history has an unsupported event.")
        event, state = self.last_event_kind, self.status.state
        if event in ("accepted", "publishing_started", "finalized"):
            expected = {
                "accepted": CodingPublicationState.ACCEPTED,
                "publishing_started": CodingPublicationState.PUBLISHING,
                "finalized": CodingPublicationState.FINALIZED,
            }[event]
            if (
                state is not expected
                or (self.invocation_id is None) != (event == "accepted")
                or self.last_step is not None
                or self.last_observation_digest is not None
                or (
                    event == "publishing_started" and self.status.operation_version != 1
                )
            ):
                raise ValueError(
                    "Publication history changed its lifecycle association."
                )
        else:
            step = self.last_step
            if (
                step is None
                or self.invocation_id is None
                or state
                not in (
                    CodingPublicationState.PUBLISHING,
                    CodingPublicationState.UNCERTAIN,
                )
                or (
                    event == "write_planned"
                    and state is not CodingPublicationState.PUBLISHING
                )
                or (
                    event == "uncertain"
                    and state is not CodingPublicationState.UNCERTAIN
                )
                or (self.last_observation_digest is not None)
                != (event == "write_observed")
            ):
                raise ValueError(
                    "Publication history lost its original write association."
                )
            offset = (
                1
                if event == "write_planned"
                else 3
                if event == "write_observed"
                and state is CodingPublicationState.UNCERTAIN
                else 2
            )
            if (
                self.status.operation_version
                != step.expected_operation_version + offset
            ):
                raise ValueError(
                    "Publication history changed its original write sequence."
                )
