from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta, timezone
from uuid import UUID

import pytest

from devrelay.application.accept_coding_result import fold_successful_coding_result
from devrelay.application.contracts.assignment_delivery import AssignedJobDefinition
from devrelay.application.contracts.coding_results import (
    SuccessfulCodingResult,
)
from devrelay.application.contracts.processes import (
    ExecutionProcessBinding,
)
from devrelay.domain.jobs.job import Job
from devrelay.domain.jobs.values import JobStatus
from tests.git_source_fixtures import git_workspace_inputs
from tests.unit.test_coding_results import context as result_context


@dataclass
class Context:
    job: Job
    definition: AssignedJobDefinition
    execution: ExecutionProcessBinding
    result: SuccessfulCodingResult
    accepted_at: datetime


def context(repairs: int = 0) -> Context:
    state, delivery, binding, fixture = git_workspace_inputs()
    definition, execution = delivery.definition, binding.execution
    definition = replace(definition, max_repair_cycles=5)
    execution = replace(
        execution,
        intent=replace(
            execution.intent,
            acknowledgement=replace(
                execution.intent.acknowledgement,
                definition_digest=definition.fingerprint(),
            ),
        ),
    )
    reference = execution.reference
    assert state.completion is not None
    job = Job(
        id=definition.job_id,
        source_task_id=definition.source_task_id,
        snapshot_id=definition.snapshot.id,
        repository_id=definition.repository_id,
        authorized_branch=definition.authorized_branch,
        target_branch=definition.target_branch,
        base_revision=definition.base_revision,
        policy_version=definition.policy_version,
        required_check_ids=tuple(check.id for check in definition.checks),
        max_repair_cycles=definition.max_repair_cycles,
        created_at=definition.created_at,
        updated_at=definition.created_at,
    ).claim(reference.attempt_id, now=reference.issued_at)
    job = job.begin_execution(
        reference.attempt_id, now=state.completion.acknowledged_at
    )
    result = result_context(
        repairs, definition=definition, execution_id=execution.intent.execution_id
    ).result
    return Context(
        job, definition, execution, result, job.updated_at + timedelta(seconds=3)
    )


@pytest.mark.parametrize("repairs", [0, 1, 5])
def test_fold_returns_every_contiguous_acceptance_state_with_final_checked_head(
    repairs: int,
) -> None:
    setup = context(repairs)
    original = setup.job

    states = fold_successful_coding_result(
        setup.job,
        setup.definition,
        setup.execution,
        setup.result,
        accepted_at=setup.accepted_at,
    )

    assert len(states) == 2 * (repairs + 1)
    assert tuple(state.workflow_version for state in states) == tuple(
        range(3, 5 + 2 * repairs)
    )
    assert all(state.updated_at == setup.accepted_at for state in states)
    assert all(state.id == setup.job.id for state in states)
    assert tuple(state.status for state in states) == (
        *(
            status
            for _ in range(repairs)
            for status in (JobStatus.TESTING, JobStatus.RUNNING)
        ),
        JobStatus.TESTING,
        JobStatus.PUBLISHING_PR,
    )
    final = states[-1]
    assert final.workflow_version == 4 + 2 * repairs
    assert final.repair_cycles == repairs
    assert final.candidate_revision == setup.result.final_candidate.revision
    assert final.passed_checks == setup.result.cycles[-1].domain_checks()
    assert final.active_attempt_id == setup.execution.reference.attempt_id
    assert final.pull_request is None and final.finished_at is None
    assert setup.job is original and original.workflow_version == 2
    assert original.candidate_revision is None and original.passed_checks == ()


@pytest.mark.parametrize(
    "field,value",
    [
        ("id", UUID(int=1001)),
        ("source_task_id", UUID(int=1002)),
        ("snapshot_id", UUID(int=1003)),
        ("repository_id", UUID(int=1004)),
        ("authorized_branch", "other/candidate"),
        ("target_branch", "other-target"),
        ("base_revision", "a" * 40),
        ("policy_version", "other-policy"),
        ("required_check_ids", ("other-check",)),
        ("max_repair_cycles", 4),
    ],
)
def test_fold_rejects_changed_immutable_job_policy(field: str, value: object) -> None:
    setup = context()
    changed = replace(setup.job, **{field: value})  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="original running assignment"):
        fold_successful_coding_result(
            changed,
            setup.definition,
            setup.execution,
            setup.result,
            accepted_at=setup.accepted_at,
        )


@pytest.mark.parametrize("field", ["attempt", "execution", "acknowledgement"])
def test_fold_rejects_another_attempt_or_execution(field: str) -> None:
    setup = context()
    execution = setup.execution
    if field == "execution":
        execution = replace(
            execution, intent=replace(execution.intent, execution_id=UUID(int=1901))
        )
    elif field == "acknowledgement":
        execution = replace(
            execution,
            intent=replace(
                execution.intent,
                acknowledgement=replace(
                    execution.intent.acknowledgement, definition_digest="a" * 64
                ),
            ),
        )
    else:
        foreign = UUID(int=1902)
        execution = replace(
            execution,
            reference=replace(execution.reference, attempt_id=foreign),
            intent=replace(
                execution.intent,
                acknowledgement=replace(
                    execution.intent.acknowledgement, attempt_id=foreign
                ),
            ),
        )

    with pytest.raises(ValueError, match="original running assignment"):
        fold_successful_coding_result(
            setup.job,
            setup.definition,
            execution,
            setup.result,
            accepted_at=setup.accepted_at,
        )


def test_fold_cannot_accept_the_same_report_from_an_advanced_projection() -> None:
    setup = context()
    publishing = fold_successful_coding_result(
        setup.job,
        setup.definition,
        setup.execution,
        setup.result,
        accepted_at=setup.accepted_at,
    )[-1]

    with pytest.raises(ValueError, match="original running assignment"):
        fold_successful_coding_result(
            publishing,
            setup.definition,
            setup.execution,
            setup.result,
            accepted_at=setup.accepted_at,
        )


@pytest.mark.parametrize(
    "clock", ["naive", "before_running", "equivalent_zone", "after_original_expiry"]
)
def test_only_acceptance_time_is_recorded_without_restoring_original_lease_authority(
    clock: str,
) -> None:
    setup = context()
    accepted_at = setup.accepted_at
    if clock == "naive":
        accepted_at = accepted_at.replace(tzinfo=None)
    elif clock == "before_running":
        accepted_at = setup.job.updated_at - timedelta(seconds=1)
    elif clock == "equivalent_zone":
        accepted_at = accepted_at.astimezone(timezone(timedelta(hours=3)))
    else:
        accepted_at = setup.execution.reference.expires_at + timedelta(hours=1)

    if clock in {"naive", "before_running"}:
        with pytest.raises(ValueError):
            fold_successful_coding_result(
                setup.job,
                setup.definition,
                setup.execution,
                setup.result,
                accepted_at=accepted_at,
            )
    else:
        states = fold_successful_coding_result(
            setup.job,
            setup.definition,
            setup.execution,
            setup.result,
            accepted_at=accepted_at,
        )

        assert all(state.updated_at == accepted_at.astimezone(UTC) for state in states)
        if clock == "after_original_expiry":
            assert setup.execution.reference.expires_at < accepted_at
