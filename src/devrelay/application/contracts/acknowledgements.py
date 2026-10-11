import re
from dataclasses import dataclass, replace
from typing import Literal
from uuid import UUID

from devrelay.application.contracts.claims import ClaimReplayConflict
from devrelay.domain.jobs.values import _revision, _uuid


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
    definition_digest_version: Literal[1] = 1
    protocol_version: Literal[1] = 1

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
