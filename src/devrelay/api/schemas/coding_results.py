from dataclasses import asdict
from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from devrelay.application.contracts.coding_result_receipts import (
    CodingPublicationState,
    CodingResultAcceptance,
    CodingResultReceipt,
    CodingResultStatus,
)


class CodingResultReceiptResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

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
    protocol_version: int

    @classmethod
    def from_receipt(cls, value: CodingResultReceipt) -> CodingResultReceiptResponse:
        value.__post_init__()
        return cls.model_validate(asdict(value))


class CodingResultAcceptanceResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    receipt: CodingResultReceiptResponse
    recovered: bool

    @classmethod
    def from_result(
        cls, value: CodingResultAcceptance
    ) -> CodingResultAcceptanceResponse:
        value.__post_init__()
        return cls(
            receipt=CodingResultReceiptResponse.from_receipt(value.receipt),
            recovered=value.recovered,
        )


class TerminalCodingResultResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    repository_id: UUID
    number: int
    branch: str
    target_branch: str
    head_revision: str
    finalized_at: datetime
    workflow_version: int
    release_ordinal: int


class CodingResultStatusResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    receipt: CodingResultReceiptResponse
    state: CodingPublicationState
    operation_version: int
    terminal: TerminalCodingResultResponse | None = None

    @classmethod
    def from_result(cls, value: CodingResultStatus) -> CodingResultStatusResponse:
        value.__post_init__()
        terminal = value.terminal
        reference = terminal.reference if terminal else None
        return cls(
            receipt=CodingResultReceiptResponse.from_receipt(value.receipt),
            state=value.state,
            operation_version=value.operation_version,
            terminal=TerminalCodingResultResponse(
                repository_id=reference.repository_id,
                number=reference.number,
                branch=reference.source_branch,
                target_branch=reference.target_branch,
                head_revision=reference.head_revision,
                finalized_at=terminal.finalized_at,
                workflow_version=terminal.workflow_version,
                release_ordinal=terminal.release_ordinal,
            )
            if terminal is not None and reference is not None
            else None,
        )
