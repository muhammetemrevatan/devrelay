from dataclasses import dataclass, field

from devrelay.application.contracts.coding_result_receipts import (
    CodingPublicationState,
    CodingResultReceipt,
    CodingResultStatus,
)
from devrelay.application.contracts.coding_result_requests import (
    CodingResultIdentity,
    WorkerCodingResultIntent,
    WorkerCodingResultRequest,
)
from devrelay.application.contracts.worker_recovery import (
    WorkerRecoveryPhase,
    WorkerRecoveryState,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.domain.jobs.values import _branch


def historical_result_identity(
    original: WorkerRecoveryState, intent: WorkerCodingResultIntent
) -> CodingResultIdentity:
    """Compute original operation correlation only; never current authentication."""
    reference = original.assignment.assignment if original.assignment else None
    if reference is None or original.request is None:
        raise ValueError("Result recovery requires the original assigned claim.")
    # This historical value supplies a worker UUID to fingerprint construction.
    # No caller may use its old expiry/scope as renewed or current authority.
    historical = WorkerAuthorization(
        worker_id=reference.worker_id,
        allowed_repository_ids=frozenset({reference.repository_id}),
        credential_expires_at=reference.expires_at,
    )
    return WorkerCodingResultRequest(intent=intent, claim=original.request).identify(
        historical
    )


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerCodingResultState:
    """Durable original result and receipts, with no restart or launch rights.

    In particular, a terminal server receipt does not clear the original claim,
    adopt native handles or prove current process settlement after a restart.
    """

    original: WorkerRecoveryState = field(repr=False)
    intent: WorkerCodingResultIntent | None = field(default=None, repr=False)
    target_branch: str | None = None
    receipt: CodingResultReceipt | None = None
    terminal_status: CodingResultStatus | None = None
    history_unknown: bool = False

    def __post_init__(self) -> None:
        if (
            not isinstance(self.original, WorkerRecoveryState)
            or type(self.history_unknown) is not bool
        ):
            raise ValueError("Result recovery requires typed original history.")
        self.original.__post_init__()
        if self.history_unknown and (
            self.original.execution is None or self.intent is not None
        ):
            raise ValueError(
                "Unknown result history cannot create new result evidence."
            )
        if self.intent is None:
            if (
                self.target_branch is not None
                or self.receipt is not None
                or self.terminal_status is not None
            ):
                raise ValueError(
                    "Result receipts require the retained original report."
                )
            return
        original, intent = self.original, self.intent
        if not isinstance(intent, WorkerCodingResultIntent):
            raise ValueError("Result recovery requires typed original input.")
        reference = original.assignment.assignment if original.assignment else None
        intent.__post_init__()
        if (
            original.phase is not WorkerRecoveryPhase.ACKNOWLEDGED
            or original.execution != intent.execution
            or original.workspace_completion is None
            or original.workspace_history_unknown
            or reference is None
            or intent.execution.acknowledgement.attempt_id != reference.attempt_id
            or intent.result.final_candidate.request.repository_id
            != reference.repository_id
            or self.target_branch is None
        ):
            raise ValueError(
                "Result recovery changed original execution or workspace history."
            )
        _branch(self.target_branch)
        if self.target_branch == intent.result.final_candidate.authorized_branch:
            raise ValueError(
                "Result target must differ from the authorized coding branch."
            )
        if self.receipt is not None:
            if not isinstance(self.receipt, CodingResultReceipt):
                raise ValueError("Result recovery requires typed receipt metadata.")
            self.receipt.require_input(
                historical_result_identity(original, intent), job_id=reference.job_id
            )
            if (
                original.completion is None
                or self.receipt.recorded_at < original.completion.acknowledged_at
            ):
                raise ValueError(
                    "Result acceptance cannot precede its original acknowledgement."
                )
        if self.terminal_status is not None:
            status = self.terminal_status
            if not isinstance(status, CodingResultStatus):
                raise ValueError("Terminal result recovery requires typed metadata.")
            status.__post_init__()
            terminal = status.terminal
            if (
                self.receipt is None
                or status.receipt != self.receipt
                or status.state is not CodingPublicationState.FINALIZED
                or terminal is None
                or terminal.reference.source_branch
                != intent.result.final_candidate.authorized_branch
                or terminal.reference.target_branch != self.target_branch
            ):
                raise ValueError(
                    "Terminal result changed its retained publication association."
                )
