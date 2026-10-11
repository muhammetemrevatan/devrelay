import hashlib
import json
import re
from dataclasses import dataclass, field, replace
from datetime import datetime
from secrets import compare_digest
from uuid import UUID

from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    ClaimRequestIdentity,
    WorkerClaimRequest,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.domain.jobs.values import _revision, _utc, _uuid


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerAcknowledgementIntent:
    """Durable initial acknowledgement input; no proof or execution permission.

    The original claim journal supplies the proof at a future transport boundary.
    One request identity pins the delivered definition before any acknowledgement
    HTTP call; a restart must reuse this exact input rather than repin it.
    """

    acknowledgement_request_id: UUID
    claim_request_id: UUID
    attempt_id: UUID
    generation: int
    snapshot_id: UUID
    base_revision: str
    definition_digest: str
    expected_workflow_version: int = 1
    definition_digest_version: int = 1
    protocol_version: int = 1

    def __post_init__(self) -> None:
        for identity in (
            self.acknowledgement_request_id,
            self.claim_request_id,
            self.attempt_id,
            self.snapshot_id,
        ):
            _uuid(identity)
        for version in (
            self.generation,
            self.expected_workflow_version,
            self.definition_digest_version,
            self.protocol_version,
        ):
            if type(version) is not int or version != 1:
                raise ValueError("Initial acknowledgement version must be one.")
        _revision(self.base_revision)
        if (
            not isinstance(self.definition_digest, str)
            or re.fullmatch(r"[0-9a-f]{64}", self.definition_digest) is None
        ):
            raise ValueError("Acknowledgement requires a canonical definition digest.")

    def require_same_input(self, candidate: WorkerAcknowledgementIntent) -> None:
        """Ignore a fresh candidate ID only when every accepted pin is identical."""
        if not isinstance(candidate, WorkerAcknowledgementIntent) or self != replace(
            candidate, acknowledgement_request_id=self.acknowledgement_request_id
        ):
            raise ClaimReplayConflict("Acknowledgement conflicts with original input.")


class AcknowledgementRejected(Exception):
    """Initial acknowledgement cannot change current assignment state."""


@dataclass(frozen=True, slots=True, kw_only=True, eq=False)
class WorkerAcknowledgementRequest:
    intent: WorkerAcknowledgementIntent
    claim: WorkerClaimRequest = field(repr=False)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.intent, WorkerAcknowledgementIntent)
            or not isinstance(self.claim, WorkerClaimRequest)
            or self.intent.claim_request_id != self.claim.claim_request_id
            or self.intent.protocol_version != self.claim.protocol_version
        ):
            raise ValueError("Acknowledgement requires the original claim input.")

    def identify(self, authorization: WorkerAuthorization) -> AcknowledgementIdentity:
        return AcknowledgementIdentity(
            intent=self.intent, claim=self.claim.identify(authorization)
        )


@dataclass(frozen=True, slots=True, kw_only=True, eq=False)
class AcknowledgementIdentity:
    """Scoped operation input; raw proof never belongs to its durable receipt."""

    intent: WorkerAcknowledgementIntent
    claim: ClaimRequestIdentity = field(repr=False)
    fingerprint: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.intent, WorkerAcknowledgementIntent)
            or not isinstance(self.claim, ClaimRequestIdentity)
            or self.intent.claim_request_id != self.claim.claim_request_id
            or self.intent.protocol_version != self.claim.protocol_version
        ):
            raise ValueError("Acknowledgement identity requires the original claim.")
        intent = self.intent
        payload = {
            "version": 1,
            "operation": "acknowledgement",
            "route": f"/api/v1/worker/attempts/{intent.attempt_id}/acknowledgements",
            "worker_id": str(self.claim.worker_id),
            "acknowledgement_request_id": str(intent.acknowledgement_request_id),
            "claim_request_id": str(intent.claim_request_id),
            "claim_fingerprint": self.claim.fingerprint,
            "attempt_id": str(intent.attempt_id),
            "generation": intent.generation,
            "expected_workflow_version": intent.expected_workflow_version,
            "snapshot_id": str(intent.snapshot_id),
            "base_revision": intent.base_revision,
            "definition_digest": intent.definition_digest,
            "definition_digest_version": intent.definition_digest_version,
            "protocol_version": intent.protocol_version,
        }
        object.__setattr__(
            self,
            "fingerprint",
            hashlib.sha256(
                json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
                    "ascii"
                )
            ).hexdigest(),
        )

    def require_replay(self, fingerprint: str) -> None:
        if (
            not isinstance(fingerprint, str)
            or re.fullmatch(r"[0-9a-f]{64}", fingerprint) is None
            or not compare_digest(self.fingerprint, fingerprint)
        ):
            raise ClaimReplayConflict(
                "Acknowledgement identity conflicts with original input."
            )


@dataclass(frozen=True, slots=True, kw_only=True)
class AcknowledgementResult:
    worker_id: UUID
    job_id: UUID
    repository_id: UUID
    intent: WorkerAcknowledgementIntent
    acknowledged_at: datetime
    expires_at: datetime
    recovered: bool
    lease_authority_active: bool
    workflow_version: int = 2

    def __post_init__(self) -> None:
        for identity in (self.worker_id, self.job_id, self.repository_id):
            _uuid(identity)
        for name in ("acknowledged_at", "expires_at"):
            object.__setattr__(self, name, _utc(getattr(self, name)))
        if (
            not isinstance(self.intent, WorkerAcknowledgementIntent)
            or type(self.workflow_version) is not int
            or self.workflow_version != 2
            or type(self.recovered) is not bool
            or type(self.lease_authority_active) is not bool
            or self.acknowledged_at >= self.expires_at
        ):
            raise ValueError("Acknowledgement result requires stable bounded evidence.")
