from dataclasses import dataclass, field, replace
from datetime import datetime
from enum import StrEnum
from typing import Literal
from uuid import UUID

from devrelay.application.contracts.acknowledgements import (
    AcknowledgementResult,
    WorkerAcknowledgementIntent,
)
from devrelay.application.contracts.assignments import ClaimResult, NoWorkReason
from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    WorkerClaimRequest,
)
from devrelay.application.contracts.executions import WorkerExecutionIntent
from devrelay.application.contracts.workspace_recovery import (
    WorkerWorkspaceCompletion,
    WorkerWorkspaceIntent,
)
from devrelay.domain.jobs.values import _utc, _uuid


@dataclass(frozen=True, slots=True, kw_only=True)
class AssignmentReference:
    """Stable claim recovery anchors; not acknowledgement or launch permission."""

    worker_id: UUID
    job_id: UUID
    repository_id: UUID
    attempt_id: UUID
    generation: int
    issued_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        for value in (self.worker_id, self.job_id, self.repository_id, self.attempt_id):
            _uuid(value)
        if type(self.generation) is not int or self.generation != 1:
            raise ValueError("Initial assignment generation must be one.")
        for name in ("issued_at", "expires_at"):
            object.__setattr__(self, name, _utc(getattr(self, name)))
        if self.expires_at <= self.issued_at:
            raise ValueError("Assignment expiry must follow issuance.")


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerClaimOutcome:
    """Trusted control response projection for the local journal, with no secret.

    Transport must authenticate control and validate the complete response before
    deriving this projection. Only stable references are stored; accepted task and
    check payloads can be recovered from control using the original request/proof.
    """

    worker_id: UUID
    claim_request_id: UUID
    recorded_at: datetime
    assignment: AssignmentReference | None = None
    no_work: NoWorkReason | None = None
    protocol_version: Literal[1] = 1

    def __post_init__(self) -> None:
        _uuid(self.worker_id)
        _uuid(self.claim_request_id)
        object.__setattr__(self, "recorded_at", _utc(self.recorded_at))
        if type(self.protocol_version) is not int or self.protocol_version != 1:
            raise ValueError("Claim outcome protocol is unsupported.")
        if self.assignment is None:
            if not isinstance(self.no_work, NoWorkReason):
                raise ValueError("No-work requires a typed recorded reason.")
        elif (
            not isinstance(self.assignment, AssignmentReference)
            or self.no_work is not None
            or self.assignment.worker_id != self.worker_id
            or self.assignment.issued_at != self.recorded_at
        ):
            raise ValueError("Assignment outcome must match the recorded worker claim.")

    @classmethod
    def from_result(
        cls, result: ClaimResult, *, worker_id: UUID, claim_request_id: UUID
    ) -> WorkerClaimOutcome:
        reference = None
        if result.attempt is not None and result.stored is not None:
            lease = result.attempt.lease
            reference = AssignmentReference(
                worker_id=lease.worker_id,
                job_id=result.attempt.job_id,
                repository_id=result.stored.job.repository_id,
                attempt_id=result.attempt.id,
                generation=lease.generation,
                issued_at=lease.issued_at,
                expires_at=lease.expires_at,
            )
        return cls(
            worker_id=worker_id,
            claim_request_id=claim_request_id,
            recorded_at=result.recorded_at,
            assignment=reference,
            no_work=result.no_work,
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class ClaimExchange:
    """Recovery references only; payload retrieval and execution are separate gates."""

    outcome: WorkerClaimOutcome
    recovered: bool
    lease_authority_active: bool

    def __post_init__(self) -> None:
        if (
            not isinstance(self.outcome, WorkerClaimOutcome)
            or type(self.recovered) is not bool
            or type(self.lease_authority_active) is not bool
            or (self.outcome.assignment is None and self.lease_authority_active)
        ):
            raise ValueError("Claim exchange requires coherent recovery metadata.")


class WorkerRecoveryPhase(StrEnum):
    IDLE = "idle"
    PENDING = "pending"
    ASSIGNED = "assigned"
    ACKNOWLEDGEMENT_PENDING = "acknowledgement_pending"
    ACKNOWLEDGED = "acknowledged"


@dataclass(frozen=True, slots=True, kw_only=True)
class AcknowledgementCompletion:
    """Historical completion only; current authority is never persisted."""

    intent: WorkerAcknowledgementIntent
    acknowledged_at: datetime
    workflow_version: int = 2

    def __post_init__(self) -> None:
        object.__setattr__(self, "acknowledged_at", _utc(self.acknowledged_at))
        if (
            not isinstance(self.intent, WorkerAcknowledgementIntent)
            or type(self.workflow_version) is not int
            or self.workflow_version != 2
        ):
            raise ValueError("Completion requires typed acknowledgement evidence.")

    @classmethod
    def from_result(
        cls,
        result: AcknowledgementResult,
        outcome: WorkerClaimOutcome,
        intent: WorkerAcknowledgementIntent,
    ) -> AcknowledgementCompletion:
        reference = outcome.assignment
        if (
            reference is None
            or result.intent != intent
            or result.worker_id != outcome.worker_id
            or result.job_id != reference.job_id
            or result.repository_id != reference.repository_id
            or result.expires_at != reference.expires_at
            or not reference.issued_at <= result.acknowledged_at < reference.expires_at
        ):
            raise ValueError("Completion changed original assignment anchors.")
        return cls(
            intent=intent,
            acknowledged_at=result.acknowledged_at,
            workflow_version=result.workflow_version,
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerHeartbeatIntent:
    """Durable operation input only; the expected revision is never live authority."""

    heartbeat_request_id: UUID
    claim_request_id: UUID
    attempt_id: UUID
    generation: int
    expected_lease_revision: int
    protocol_version: int = 1

    def __post_init__(self) -> None:
        for identity in (
            self.heartbeat_request_id,
            self.claim_request_id,
            self.attempt_id,
        ):
            _uuid(identity)
        if (
            type(self.generation) is not int
            or self.generation != 1
            or type(self.protocol_version) is not int
            or self.protocol_version != 1
            or type(self.expected_lease_revision) is not int
            or not 1 <= self.expected_lease_revision < 1 << 63
        ):
            raise ValueError("Heartbeat intent requires supported bounded input.")

    def require_same_input(self, candidate: WorkerHeartbeatIntent) -> None:
        if not isinstance(candidate, WorkerHeartbeatIntent) or self != replace(
            candidate, heartbeat_request_id=self.heartbeat_request_id
        ):
            raise ClaimReplayConflict("Heartbeat intent conflicts with original input.")


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerRecoveryState:
    phase: WorkerRecoveryPhase
    request: WorkerClaimRequest | None = field(default=None, repr=False)
    assignment: WorkerClaimOutcome | None = None
    last_no_work: WorkerClaimOutcome | None = None
    acknowledgement: WorkerAcknowledgementIntent | None = None
    completion: AcknowledgementCompletion | None = None
    pending_heartbeat: WorkerHeartbeatIntent | None = None
    last_heartbeat: WorkerHeartbeatIntent | None = None
    execution: WorkerExecutionIntent | None = None
    workspace_intent: WorkerWorkspaceIntent | None = None
    workspace_completion: WorkerWorkspaceCompletion | None = None
    workspace_history_unknown: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.phase, WorkerRecoveryPhase):
            raise ValueError("Worker recovery phase must be typed.")
        if self.last_no_work is not None and (
            not isinstance(self.last_no_work, WorkerClaimOutcome)
            or self.last_no_work.no_work is None
        ):
            raise ValueError("Completed no-work requires a recorded no-work outcome.")
        if self.phase is WorkerRecoveryPhase.IDLE:
            if self.request is not None or self.assignment is not None:
                raise ValueError("Idle recovery cannot contain an active claim.")
        elif not isinstance(self.request, WorkerClaimRequest):
            raise ValueError("Unresolved recovery requires the original request.")
        if self.phase is WorkerRecoveryPhase.PENDING and self.assignment is not None:
            raise ValueError("Pending recovery cannot contain an assignment.")
        if self.phase in (
            WorkerRecoveryPhase.ASSIGNED,
            WorkerRecoveryPhase.ACKNOWLEDGEMENT_PENDING,
            WorkerRecoveryPhase.ACKNOWLEDGED,
        ):
            if (
                not isinstance(self.assignment, WorkerClaimOutcome)
                or self.assignment.assignment is None
                or self.request is None
                or self.assignment.claim_request_id != self.request.claim_request_id
            ):
                raise ValueError("Assigned recovery requires matching stable anchors.")
        if self.phase in (
            WorkerRecoveryPhase.ACKNOWLEDGEMENT_PENDING,
            WorkerRecoveryPhase.ACKNOWLEDGED,
        ):
            reference = self.assignment.assignment if self.assignment else None
            if (
                not isinstance(self.acknowledgement, WorkerAcknowledgementIntent)
                or self.request is None
                or reference is None
                or self.acknowledgement.claim_request_id
                != self.request.claim_request_id
                or self.acknowledgement.attempt_id != reference.attempt_id
                or self.acknowledgement.generation != reference.generation
            ):
                raise ValueError("Pending acknowledgement requires original anchors.")
        elif self.acknowledgement is not None:
            raise ValueError("Acknowledgement input requires its durable phase.")
        if self.phase is WorkerRecoveryPhase.ACKNOWLEDGED:
            reference = self.assignment.assignment if self.assignment else None
            if (
                not isinstance(self.completion, AcknowledgementCompletion)
                or self.completion.intent != self.acknowledgement
                or reference is None
                or not reference.issued_at
                <= self.completion.acknowledged_at
                < reference.expires_at
            ):
                raise ValueError("Completed acknowledgement requires original anchors.")
        elif self.completion is not None:
            raise ValueError("Completion requires its durable phase.")
        if self.execution is not None and (
            self.phase is not WorkerRecoveryPhase.ACKNOWLEDGED
            or not isinstance(self.execution, WorkerExecutionIntent)
            or self.completion is None
            or self.execution.acknowledgement != self.acknowledgement
        ):
            raise ValueError(
                "Execution preparation requires the original completed acknowledgement."
            )
        if type(self.workspace_history_unknown) is not bool or (
            self.workspace_history_unknown
            and (
                self.execution is None
                or self.workspace_intent is not None
                or self.workspace_completion is not None
            )
        ):
            raise ValueError(
                "Unknown workspace history requires retained execution only."
            )
        if self.workspace_intent is not None and (
            not isinstance(self.workspace_intent, WorkerWorkspaceIntent)
            or self.execution is None
            or self.workspace_intent.execution != self.execution
        ):
            raise ValueError("Workspace preparation requires original execution input.")
        if self.workspace_completion is not None and (
            not isinstance(self.workspace_completion, WorkerWorkspaceCompletion)
            or self.workspace_intent is None
            or self.workspace_completion.intent != self.workspace_intent
        ):
            raise ValueError(
                "Workspace completion requires original preparation input."
            )
        for heartbeat in (self.pending_heartbeat, self.last_heartbeat):
            if heartbeat is None:
                continue
            reference = self.assignment.assignment if self.assignment else None
            if (
                self.phase is not WorkerRecoveryPhase.ACKNOWLEDGED
                or not isinstance(heartbeat, WorkerHeartbeatIntent)
                or self.request is None
                or reference is None
                or heartbeat.claim_request_id != self.request.claim_request_id
                or heartbeat.attempt_id != reference.attempt_id
                or heartbeat.generation != reference.generation
                or heartbeat.protocol_version != self.request.protocol_version
            ):
                raise ValueError(
                    "Heartbeat input requires acknowledged original anchors."
                )
        if (
            self.pending_heartbeat is not None
            and self.last_heartbeat is not None
            and (
                self.pending_heartbeat.heartbeat_request_id
                == self.last_heartbeat.heartbeat_request_id
                or self.pending_heartbeat.expected_lease_revision
                < self.last_heartbeat.expected_lease_revision
            )
        ):
            raise ValueError("Heartbeat input cannot reuse or regress completed input.")
