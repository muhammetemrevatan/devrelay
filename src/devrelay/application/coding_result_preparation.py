"""Synchronous full-byte result preflight; callers schedule it outside SQL/off-loop."""

import hashlib
from dataclasses import replace
from secrets import compare_digest

from devrelay.application.contracts.assignment_delivery import AssignedJobDefinition
from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    WorkerClaimRequest,
)
from devrelay.application.contracts.coding_result_acceptance import PreparedCodingResult
from devrelay.application.contracts.coding_result_requests import (
    WorkerCodingResultRequest,
)
from devrelay.application.contracts.coding_result_wire import (
    decode_coding_result_intent,
    encode_coding_result_intent,
)
from devrelay.application.contracts.coding_results import SuccessfulCodingResult
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.domain.jobs.values import CheckResult, VerificationOutcome

_ERROR = "Invalid coding result preparation."


def _capture_definition(definition: AssignedJobDefinition) -> AssignedJobDefinition:
    if not isinstance(definition, AssignedJobDefinition):
        raise ValueError(_ERROR)
    definition.__post_init__()
    snapshot = replace(
        definition.snapshot, identity=replace(definition.snapshot.identity)
    )
    captured = replace(
        definition,
        snapshot=snapshot,
        checks=tuple(replace(check) for check in definition.checks),
    )
    if not compare_digest(definition.fingerprint(), captured.fingerprint()):
        raise ValueError(_ERROR)
    return captured


def _validate_original(
    request: WorkerCodingResultRequest, definition: AssignedJobDefinition
) -> None:
    request.__post_init__()
    acknowledgement = request.intent.execution.acknowledgement
    if (
        acknowledgement.snapshot_id != definition.snapshot.id
        or acknowledgement.base_revision != definition.base_revision
        or acknowledgement.definition_digest != definition.fingerprint()
    ):
        raise ValueError(_ERROR)
    request.intent.result.validate(definition)


def compile_coding_checks(
    report: SuccessfulCodingResult,
) -> tuple[tuple[CheckResult, ...], ...]:
    """Preserve passed and failed settled cycle checks as acceptance history."""
    return tuple(
        tuple(
            CheckResult(
                check_id=check.check_id,
                revision=cycle.revision,
                outcome=(
                    VerificationOutcome.PASSED
                    if check.settlement.exit_code == 0
                    else VerificationOutcome.FAILED
                ),
                exit_code=check.settlement.exit_code,
            )
            for check in cycle.checks
        )
        for cycle in report.cycles
    )


def prepare_coding_result(
    request: WorkerCodingResultRequest,
    authorization: WorkerAuthorization,
    definition: AssignedJobDefinition,
) -> PreparedCodingResult:
    """Capture one owned canonical input without opening a UOW or asserting expiry.

    Full candidate decoding/hashing and definition checks belong here. The store
    must separately authenticate current scope, resolve replay and recheck actual
    database ownership/version/authority after locks. Returned history never
    fabricates process timestamps or releases capacity.
    """
    try:
        if not isinstance(request, WorkerCodingResultRequest) or not isinstance(
            authorization, WorkerAuthorization
        ):
            raise ValueError(_ERROR)
        captured_authorization = replace(authorization)
        original = request.identify(captured_authorization)
        captured_definition = _capture_definition(definition)
        _validate_original(request, captured_definition)
        payload = encode_coding_result_intent(request.intent)
        captured_intent = decode_coding_result_intent(payload)
        claim = WorkerClaimRequest(
            claim_request_id=request.claim.claim_request_id,
            proof=request.claim.proof,
            protocol_version=request.claim.protocol_version,
        )
        rebound = WorkerCodingResultRequest(intent=captured_intent, claim=claim)
        identity = rebound.identify(captured_authorization)
        original.require_replay(identity.fingerprint)
        _validate_original(rebound, captured_definition)
        original.require_replay(request.identify(captured_authorization).fingerprint)
        final_definition = _capture_definition(definition)
        if (
            not compare_digest(
                captured_definition.fingerprint(), final_definition.fingerprint()
            )
            or captured_authorization != authorization
        ):
            raise ValueError(_ERROR)
        return PreparedCodingResult(
            identity=identity,
            definition=captured_definition,
            payload_bytes=payload,
            payload_sha256=hashlib.sha256(payload).hexdigest(),
            cycle_checks=compile_coding_checks(captured_intent.result),
        )
    except ClaimReplayConflict:
        raise
    except (
        ValueError,
        TypeError,
        AttributeError,
        KeyError,
        OverflowError,
        RecursionError,
    ):
        raise ValueError(_ERROR) from None
