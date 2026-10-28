"""Proof-bound successful coding input; no current authority or release rights."""

import hashlib
import json
import re
from dataclasses import dataclass, field
from secrets import compare_digest
from uuid import UUID

from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    ClaimRequestIdentity,
    WorkerClaimRequest,
)
from devrelay.application.contracts.coding_results import SuccessfulCodingResult
from devrelay.application.contracts.executions import WorkerExecutionIntent
from devrelay.application.contracts.workers import WorkerAuthorization

MAX_CODING_RESULT_REQUEST_BYTES = 16 * 1024**2


def _fingerprint(payload: dict[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(
            payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        ).encode("ascii")
    ).hexdigest()


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerCodingResultIntent:
    """Durable bounded successful report input, independent of the raw lease proof.

    Acceptance, replay persistence and current authority are external decisions.
    The request byte cap belongs to the separate canonical transport codec; this
    contract validates the already decoded bounded report and its original pins.
    """

    result_request_id: UUID
    execution: WorkerExecutionIntent = field(repr=False)
    result: SuccessfulCodingResult = field(repr=False)
    expected_workflow_version: int = 2
    protocol_version: int = 1

    def __post_init__(self) -> None:
        if (
            not isinstance(self.result_request_id, UUID)
            or not isinstance(self.execution, WorkerExecutionIntent)
            or not isinstance(self.result, SuccessfulCodingResult)
            or type(self.expected_workflow_version) is not int
            or self.expected_workflow_version != 2
            or type(self.protocol_version) is not int
            or self.protocol_version != 1
        ):
            raise ValueError(
                "Coding result intent requires original typed versioned input."
            )
        self.execution.__post_init__()
        self.execution.acknowledgement.__post_init__()
        self.execution.policy.__post_init__()
        self.result.__post_init__()
        acknowledgement = self.execution.acknowledgement
        if (
            self.result.execution_id != self.execution.execution_id
            or self.result.definition_digest != acknowledgement.definition_digest
            or self.result.final_candidate.request.base_revision
            != acknowledgement.base_revision
            or self.protocol_version != acknowledgement.protocol_version
        ):
            raise ValueError(
                "Coding result changed its original acknowledged execution."
            )

    def require_same_input(self, candidate: WorkerCodingResultIntent) -> None:
        """A fresh UUID may be ignored only before retaining the original intent."""
        if not isinstance(candidate, WorkerCodingResultIntent):
            raise ClaimReplayConflict("Coding result conflicts with original input.")
        original = _intent_payload(self, include_request_id=False)
        proposed = _intent_payload(candidate, include_request_id=False)
        if not compare_digest(_fingerprint(original), _fingerprint(proposed)):
            raise ClaimReplayConflict("Coding result conflicts with original input.")


def _intent_payload(
    intent: WorkerCodingResultIntent, *, include_request_id: bool = True
) -> dict[str, object]:
    intent.__post_init__()
    execution = intent.execution
    acknowledgement = execution.acknowledgement
    payload: dict[str, object] = {
        "expected_workflow_version": intent.expected_workflow_version,
        "protocol_version": intent.protocol_version,
        "execution_id": str(execution.execution_id),
        "acknowledgement": {
            "acknowledgement_request_id": str(
                acknowledgement.acknowledgement_request_id
            ),
            "claim_request_id": str(acknowledgement.claim_request_id),
            "attempt_id": str(acknowledgement.attempt_id),
            "generation": acknowledgement.generation,
            "snapshot_id": str(acknowledgement.snapshot_id),
            "base_revision": acknowledgement.base_revision,
            "definition_digest": acknowledgement.definition_digest,
            "expected_workflow_version": acknowledgement.expected_workflow_version,
            "definition_digest_version": acknowledgement.definition_digest_version,
            "protocol_version": acknowledgement.protocol_version,
        },
        "execution_policy": {
            "runtime": execution.policy.runtime.value,
            "isolation": execution.policy.isolation.value,
            "policy_digest": execution.policy.policy_digest,
        },
        "result_fingerprint": intent.result.fingerprint(),
    }
    if include_request_id:
        payload["result_request_id"] = str(intent.result_request_id)
    return payload


@dataclass(frozen=True, slots=True, kw_only=True, eq=False)
class WorkerCodingResultRequest:
    intent: WorkerCodingResultIntent
    claim: WorkerClaimRequest = field(repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.intent, WorkerCodingResultIntent) or not isinstance(
            self.claim, WorkerClaimRequest
        ):
            raise ValueError("Coding result request requires original claim input.")
        self.intent.__post_init__()
        self.claim.__post_init__()
        if (
            self.intent.execution.acknowledgement.claim_request_id
            != self.claim.claim_request_id
            or self.intent.protocol_version != self.claim.protocol_version
        ):
            raise ValueError("Coding result request changed its original claim.")

    def identify(self, authorization: WorkerAuthorization) -> CodingResultIdentity:
        self.__post_init__()
        return CodingResultIdentity(
            intent=self.intent, claim=self.claim.identify(authorization)
        )


def coding_result_identity_payload(
    intent: WorkerCodingResultIntent, claim: ClaimRequestIdentity
) -> dict[str, object]:
    """Canonical nonsecret fingerprint input, not a wire report or durable receipt."""
    if not isinstance(intent, WorkerCodingResultIntent) or not isinstance(
        claim, ClaimRequestIdentity
    ):
        raise ValueError("Coding result identity requires typed original claim input.")
    intent.__post_init__()
    claim.__post_init__()
    if (
        intent.execution.acknowledgement.claim_request_id != claim.claim_request_id
        or intent.protocol_version != claim.protocol_version
    ):
        raise ValueError("Coding result identity changed its original scoped claim.")
    return {
        "version": 1,
        "operation": "coding_result",
        "route": f"/api/v1/worker/attempts/{intent.execution.acknowledgement.attempt_id}/results",
        "worker_id": str(claim.worker_id),
        "claim_request_id": str(claim.claim_request_id),
        "claim_fingerprint": claim.fingerprint,
        "intent": _intent_payload(intent),
    }


@dataclass(frozen=True, slots=True, kw_only=True, eq=False)
class CodingResultIdentity:
    """Scoped operation fingerprint; the raw proof never belongs to its receipt."""

    intent: WorkerCodingResultIntent = field(repr=False)
    claim: ClaimRequestIdentity = field(repr=False)
    fingerprint: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "fingerprint",
            _fingerprint(coding_result_identity_payload(self.intent, self.claim)),
        )

    def require_replay(self, fingerprint: str) -> None:
        if (
            type(fingerprint) is not str
            or re.fullmatch(r"[0-9a-f]{64}", fingerprint) is None
            or not compare_digest(self.fingerprint, fingerprint)
        ):
            raise ClaimReplayConflict(
                "Coding result identity conflicts with original input."
            )
