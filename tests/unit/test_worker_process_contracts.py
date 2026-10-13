from dataclasses import replace
from typing import Any

import pytest

from devrelay.application.contracts.processes import (
    ExecutionProcessBinding,
    ProcessManagementPolicy,
    ProcessResourceLimits,
    ProcessScopeResult,
    ProcessStopReason,
    ProcessStopStatus,
)
from tests.unit.test_worker_process_management import binding


def test_process_binding_rejects_another_attempt() -> None:
    original = binding()
    changed = replace(original.reference, attempt_id=original.reference.job_id)

    with pytest.raises(ValueError):
        ExecutionProcessBinding(intent=original.intent, reference=changed)


@pytest.mark.parametrize("value", [True, 0, 101, 1.0])
def test_resource_limits_reject_invalid_cpu_policy(value: Any) -> None:
    with pytest.raises(ValueError):
        ProcessResourceLimits(
            cpu_percent=value, committed_memory_bytes=64 * 1024**2, maximum_processes=1
        )


@pytest.mark.parametrize("value", [True, float("nan"), float("inf"), 10**1000])
def test_management_policy_rejects_nonfinite_or_coerced_timing(value: Any) -> None:
    with pytest.raises(ValueError):
        ProcessManagementPolicy(operation_timeout_seconds=value)


def test_scope_result_requires_typed_stop_status() -> None:
    original = binding()
    malformed: Any = "empty_verified"

    with pytest.raises(ValueError):
        ProcessScopeResult(
            execution_id=original.intent.execution_id,
            status=malformed,
            reason=ProcessStopReason.EMPTY,
        )


def test_scope_result_is_only_execution_associated_observation() -> None:
    original = binding()

    result = ProcessScopeResult(
        execution_id=original.intent.execution_id,
        status=ProcessStopStatus.UNKNOWN,
        reason=ProcessStopReason.OBSERVATION_FAILED,
    )

    assert result.execution_id == original.intent.execution_id
    assert result.status is ProcessStopStatus.UNKNOWN
