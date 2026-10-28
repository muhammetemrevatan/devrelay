import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from uuid import UUID

from devrelay.application.contracts.coding_result_requests import CodingResultIdentity
from devrelay.domain.jobs.values import PullRequestReference, _utc, _uuid


class CodingResultUnavailable(Exception):
    """Result storage cannot safely complete or disclose the requested operation."""


class CodingResultRejected(Exception):
    """New result input does not match the current accepted assignment."""


def _digest(value: object, length: int = 64) -> None:
    if (
        type(value) is not str
        or re.fullmatch(r"[0-9a-f]{" + str(length) + "}", value) is None
    ):
        raise ValueError("Coding result metadata requires canonical digests.")


class CodingPublicationState(StrEnum):
    ACCEPTED = "accepted"
    PUBLISHING = "publishing"
    UNCERTAIN = "uncertain"
    FINALIZED = "finalized"


@dataclass(frozen=True, slots=True, kw_only=True)
class CodingResultReceipt:
    """Immutable acceptance history; never terminal acknowledgement or work rights."""

    result_id: UUID
    worker_id: UUID
    request_id: UUID
    claim_request_id: UUID
    job_id: UUID
    attempt_id: UUID
    repository_id: UUID
    execution_id: UUID
    definition_digest: str
    request_fingerprint: str
    candidate_revision: str
    repair_cycles: int
    publishing_workflow_version: int
    recorded_at: datetime
    protocol_version: int = 1

    def __post_init__(self) -> None:
        for identity in (
            self.result_id,
            self.worker_id,
            self.request_id,
            self.claim_request_id,
            self.job_id,
            self.attempt_id,
            self.repository_id,
            self.execution_id,
        ):
            _uuid(identity)
        _digest(self.definition_digest)
        _digest(self.request_fingerprint)
        _digest(self.candidate_revision, 40)
        if (
            type(self.repair_cycles) is not int
            or not 0 <= self.repair_cycles <= 5
            or type(self.publishing_workflow_version) is not int
            or self.publishing_workflow_version != 4 + 2 * self.repair_cycles
            or type(self.protocol_version) is not int
            or self.protocol_version != 1
            or not isinstance(self.recorded_at, datetime)
        ):
            raise ValueError(
                "Coding receipt requires bounded original acceptance pins."
            )
        object.__setattr__(self, "recorded_at", _utc(self.recorded_at))

    def require_input(self, identity: CodingResultIdentity, *, job_id: UUID) -> None:
        """Correlate a transport receipt, without deriving current authority."""
        self.__post_init__()
        if not isinstance(identity, CodingResultIdentity):
            raise ValueError("Coding receipt requires the original scoped request.")
        intent = identity.intent
        _uuid(job_id)
        identity.require_replay(self.request_fingerprint)
        if (
            self.worker_id != identity.claim.worker_id
            or self.job_id != job_id
            or self.request_id != intent.result_request_id
            or self.claim_request_id != identity.claim.claim_request_id
            or self.attempt_id != intent.execution.acknowledgement.attempt_id
            or self.execution_id != intent.execution.execution_id
            or self.repository_id != intent.result.final_candidate.request.repository_id
            or self.definition_digest != intent.result.definition_digest
            or self.candidate_revision != intent.result.final_candidate.revision
            or self.repair_cycles != len(intent.result.cycles) - 1
        ):
            raise ValueError("Coding receipt changed original result pins.")


@dataclass(frozen=True, slots=True, kw_only=True)
class CodingResultAcceptance:
    receipt: CodingResultReceipt
    recovered: bool

    def __post_init__(self) -> None:
        if (
            not isinstance(self.receipt, CodingResultReceipt)
            or type(self.recovered) is not bool
        ):
            raise ValueError("Coding acceptance requires typed receipt metadata.")
        self.receipt.__post_init__()


@dataclass(frozen=True, slots=True, kw_only=True)
class VerifiedCodingPublication:
    """Trusted broker observation; worker input never constructs this value.

    This correlation is not provider verification, worker authentication or a
    native stop capability. The broker must independently verify the exact PR,
    branch and checked head before asking the atomic store to finalize them.
    """

    result_id: UUID
    invocation_id: UUID
    expected_operation_version: int
    reference: PullRequestReference
    provider_repository_id: int
    observed_at: datetime

    def __post_init__(self) -> None:
        _uuid(self.result_id)
        _uuid(self.invocation_id)
        if (
            type(self.expected_operation_version) is not int
            or not 1 <= self.expected_operation_version < 2047
            or not isinstance(self.reference, PullRequestReference)
            or type(self.provider_repository_id) is not int
            or not 1 <= self.provider_repository_id < 2**63
            or not isinstance(self.observed_at, datetime)
        ):
            raise ValueError(
                "Publication requires bounded verified association metadata."
            )
        self.reference.__post_init__()
        object.__setattr__(self, "observed_at", _utc(self.observed_at))


@dataclass(frozen=True, slots=True, kw_only=True)
class TerminalCodingResult:
    """Historical normal publication acknowledgement, not Job delivery success.

    The control transaction records revocation and release at finalized_at. A
    worker still needs its own verified local settlement before advancing; this
    receipt never restores handles, lease authority or permission to run again.
    """

    receipt: CodingResultReceipt
    reference: PullRequestReference
    finalized_at: datetime
    workflow_version: int
    release_ordinal: int

    def __post_init__(self) -> None:
        if not isinstance(self.receipt, CodingResultReceipt) or not isinstance(
            self.reference, PullRequestReference
        ):
            raise ValueError("Terminal result requires original publication metadata.")
        self.receipt.__post_init__()
        self.reference.__post_init__()
        if (
            self.reference.repository_id != self.receipt.repository_id
            or self.reference.head_revision != self.receipt.candidate_revision
            or type(self.workflow_version) is not int
            or self.workflow_version != self.receipt.publishing_workflow_version + 1
            or type(self.release_ordinal) is not int
            or not 1 <= self.release_ordinal < 2**63
            or not isinstance(self.finalized_at, datetime)
        ):
            raise ValueError("Terminal result changed its accepted result association.")
        object.__setattr__(self, "finalized_at", _utc(self.finalized_at))
        if self.finalized_at < self.receipt.recorded_at:
            raise ValueError("Publication cannot precede result acceptance.")


@dataclass(frozen=True, slots=True, kw_only=True)
class CodingResultStatus:
    receipt: CodingResultReceipt
    state: CodingPublicationState
    operation_version: int
    terminal: TerminalCodingResult | None = field(default=None)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.receipt, CodingResultReceipt)
            or not isinstance(self.state, CodingPublicationState)
            or type(self.operation_version) is not int
            or not 0 <= self.operation_version < 2048
            or (self.state is CodingPublicationState.FINALIZED)
            != (self.terminal is not None)
        ):
            raise ValueError(
                "Coding status requires coherent bounded operation metadata."
            )
        self.receipt.__post_init__()
        if (
            self.state is CodingPublicationState.ACCEPTED
            and self.operation_version != 0
        ):
            raise ValueError("Initial coding operation must have version zero.")
        if (
            self.state is not CodingPublicationState.ACCEPTED
            and self.operation_version == 0
        ):
            raise ValueError("Advanced coding operations require recorded history.")
        if self.terminal is not None:
            if (
                not isinstance(self.terminal, TerminalCodingResult)
                or self.operation_version < 2
            ):
                raise ValueError(
                    "Finalized coding status requires typed terminal history."
                )
            self.terminal.__post_init__()
            if self.terminal.receipt != self.receipt:
                raise ValueError("Terminal result changed original acceptance receipt.")


@dataclass(frozen=True, slots=True, kw_only=True)
class CodingPublicationStep:
    """A stable broker write input, persisted before any provider mutation."""

    result_id: UUID
    invocation_id: UUID
    expected_operation_version: int
    step_id: UUID
    step_kind: str
    input_digest: str

    def __post_init__(self) -> None:
        for identity in (self.result_id, self.invocation_id, self.step_id):
            _uuid(identity)
        if (
            type(self.expected_operation_version) is not int
            or not 1 <= self.expected_operation_version < 2045
            or type(self.step_kind) is not str
            or re.fullmatch(r"[a-z][a-z0-9_:-]{0,127}", self.step_kind) is None
        ):
            raise ValueError("Publication steps require bounded stable broker input.")
        _digest(self.input_digest)


@dataclass(frozen=True, slots=True, kw_only=True)
class CodingWritePermit:
    """Only created=True authorizes the owning caller's one immediate write.

    Historical recovery does not repeat a provider write, even when its outcome
    is absent. A transport failure must be recorded as uncertainty and reconciled.
    Undated created permits remain compatible with old metadata fixtures; they
    are insufficient for dispatch. Fresh store permits carry a bounded DB window.
    """

    step: CodingPublicationStep
    operation_version: int
    created: bool
    authorized_at: datetime | None = None
    authorized_until: datetime | None = None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.step, CodingPublicationStep)
            or type(self.created) is not bool
        ):
            raise ValueError("Write permits require typed broker metadata.")
        self.step.__post_init__()
        if (
            type(self.operation_version) is not int
            or self.operation_version != self.step.expected_operation_version + 1
        ):
            raise ValueError("Write permit must name the committed planning event.")
        if (self.authorized_at is None) != (self.authorized_until is None):
            raise ValueError("Write authorization requires both DB timestamps.")
        if self.authorized_at is not None and self.authorized_until is not None:
            if (
                not self.created
                or not isinstance(self.authorized_at, datetime)
                or not isinstance(self.authorized_until, datetime)
            ):
                raise ValueError("Recovered metadata cannot carry write authority.")
            object.__setattr__(self, "authorized_at", _utc(self.authorized_at))
            object.__setattr__(self, "authorized_until", _utc(self.authorized_until))
            if self.authorized_until <= self.authorized_at:
                raise ValueError("Write authorization requires an unexpired window.")
