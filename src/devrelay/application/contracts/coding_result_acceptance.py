"""Trusted preflight data for result storage, not current acceptance authority."""

import re
from dataclasses import dataclass, field
from uuid import UUID

from devrelay.application.contracts.assignment_delivery import AssignedJobDefinition
from devrelay.application.contracts.coding_result_requests import (
    MAX_CODING_RESULT_REQUEST_BYTES,
    CodingResultIdentity,
)
from devrelay.application.contracts.coding_results import (
    MAX_CODING_CHECKS,
    MAX_CODING_CYCLES,
    check_policy_digest,
)
from devrelay.application.contracts.processes import ProcessStopStatus
from devrelay.application.contracts.trusted_processes import TrustedProcessStopReason
from devrelay.domain.jobs.values import CheckResult, VerificationOutcome

_ERROR = "Invalid coding result preparation."


@dataclass(frozen=True, slots=True, kw_only=True, eq=False)
class PreparedCodingResult:
    """Consume only from the trusted preflight, before opening the write UOW.

    Construction checks bounded shape and correlation without re-decoding payload
    bytes or rehashing candidate files. The factory binds canonical bytes, their
    digest and the owned identity graph. This value authenticates no caller and
    supplies no current lease, admission, publication or reservation-release right.
    """

    identity: CodingResultIdentity = field(repr=False)
    definition: AssignedJobDefinition = field(repr=False)
    payload_bytes: bytes = field(repr=False)
    payload_sha256: str
    cycle_checks: tuple[tuple[CheckResult, ...], ...] = field(repr=False)

    def __post_init__(self) -> None:
        try:
            _validate_prepared(self)
        except ValueError, TypeError, AttributeError, KeyError, OverflowError:
            raise ValueError(_ERROR) from None


def _validate_prepared(value: PreparedCodingResult) -> None:
    if (
        not isinstance(value.identity, CodingResultIdentity)
        or not isinstance(value.definition, AssignedJobDefinition)
        or type(value.payload_bytes) is not bytes
        or not 1 <= len(value.payload_bytes) <= MAX_CODING_RESULT_REQUEST_BYTES
        or type(value.payload_sha256) is not str
        or re.fullmatch(r"[0-9a-f]{64}", value.payload_sha256) is None
        or type(value.cycle_checks) is not tuple
        or not 1 <= len(value.cycle_checks) <= MAX_CODING_CYCLES
    ):
        raise ValueError(_ERROR)
    identity, definition = value.identity, value.definition
    definition.__post_init__()
    identity.claim.__post_init__()
    intent = identity.intent
    execution = intent.execution
    execution.__post_init__()
    execution.acknowledgement.__post_init__()
    execution.policy.__post_init__()
    acknowledgement, report = execution.acknowledgement, intent.result
    candidate = report.final_candidate
    request = candidate.request
    definition_digest = definition.fingerprint()
    if (
        not isinstance(intent.result_request_id, UUID)
        or type(intent.expected_workflow_version) is not int
        or intent.expected_workflow_version != 2
        or type(intent.protocol_version) is not int
        or intent.protocol_version != 1
        or acknowledgement.claim_request_id != identity.claim.claim_request_id
        or acknowledgement.snapshot_id != definition.snapshot.id
        or acknowledgement.base_revision != definition.base_revision
        or acknowledgement.definition_digest != definition_digest
        or report.definition_digest != definition_digest
        or report.check_policy_digest != check_policy_digest(definition)
        or report.execution_id != execution.execution_id
        or candidate.execution_id != execution.execution_id
        or request.repository_id != definition.repository_id
        or request.provider_repository_id != definition.provider_repository_id
        or request.repository_slug != definition.repository_slug
        or request.base_revision != definition.base_revision
        or candidate.authorized_branch != definition.authorized_branch
        or type(report.cycles) is not tuple
        or len(report.cycles) != len(value.cycle_checks)
        or len(report.cycles) - 1 > definition.max_repair_cycles
        or candidate.cycle != len(report.cycles) - 1
        or candidate.revision != report.cycles[-1].revision
        or candidate.root_tree_sha != report.cycles[-1].root_tree_sha
        or type(identity.fingerprint) is not str
        or re.fullmatch(r"[0-9a-f]{64}", identity.fingerprint) is None
    ):
        raise ValueError(_ERROR)
    check_ids = tuple(check.id for check in definition.checks)
    for index, (cycle, checks) in enumerate(
        zip(report.cycles, value.cycle_checks, strict=True)
    ):
        cycle.coding.__post_init__()
        if (
            type(cycle.cycle) is not int
            or cycle.cycle != index
            or type(checks) is not tuple
            or not 1 <= len(checks) <= MAX_CODING_CHECKS
            or len(checks) != len(cycle.checks)
            or tuple(check.check_id for check in checks) != check_ids
            or tuple(check.check_id for check in cycle.checks) != check_ids
            or cycle.coding.execution_id != execution.execution_id
            or cycle.coding.exit_code != 0
            or cycle.coding.reason is not TrustedProcessStopReason.EXITED
            or cycle.coding.stop_status is not ProcessStopStatus.EMPTY_VERIFIED
        ):
            raise ValueError(_ERROR)
        for check, observed in zip(checks, cycle.checks, strict=True):
            if not isinstance(check, CheckResult):
                raise ValueError(_ERROR)
            check.__post_init__()
            observed.settlement.__post_init__()
            if (
                check.revision != cycle.revision
                or observed.revision != cycle.revision
                or observed.candidate_digest != cycle.candidate_digest
                or check.exit_code != observed.settlement.exit_code
                or check.outcome
                is not (
                    VerificationOutcome.PASSED
                    if check.exit_code == 0
                    else VerificationOutcome.FAILED
                )
                or observed.settlement.execution_id != execution.execution_id
                or observed.settlement.reason is not TrustedProcessStopReason.EXITED
                or observed.settlement.stop_status
                is not ProcessStopStatus.EMPTY_VERIFIED
            ):
                raise ValueError(_ERROR)
        if all(check.exit_code == 0 for check in checks) != (
            index == len(report.cycles) - 1
        ):
            raise ValueError(_ERROR)
