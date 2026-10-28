from datetime import datetime

from devrelay.application.contracts.assignment_delivery import AssignedJobDefinition
from devrelay.application.contracts.coding_results import SuccessfulCodingResult
from devrelay.application.contracts.processes import ExecutionProcessBinding
from devrelay.domain.jobs.job import Job
from devrelay.domain.jobs.values import JobStatus, _utc


def _validate_original(
    job: Job,
    definition: AssignedJobDefinition,
    execution: ExecutionProcessBinding,
    result: SuccessfulCodingResult,
) -> None:
    if (
        not isinstance(job, Job)
        or not isinstance(definition, AssignedJobDefinition)
        or not isinstance(execution, ExecutionProcessBinding)
        or not isinstance(result, SuccessfulCodingResult)
    ):
        raise ValueError("Coding acceptance requires typed original execution pins.")
    job.__post_init__()
    definition.__post_init__()
    execution.__post_init__()
    execution.intent.__post_init__()
    execution.intent.policy.__post_init__()
    execution.intent.acknowledgement.__post_init__()
    execution.reference.__post_init__()
    reference, acknowledgement = execution.reference, execution.intent.acknowledgement
    if (
        job.status is not JobStatus.RUNNING
        or job.workflow_version != 2
        or job.repair_cycles != 0
        or job.last_attempt_number != 1
        or job.active_attempt_id != reference.attempt_id
        or job.last_attempt_id != reference.attempt_id
        or job.candidate_revision is not None
        or job.passed_checks != ()
        or job.pull_request is not None
        or job.failure is not None
        or job.release_revision is not None
        or job.release_ci_run_id is not None
        or job.deployment is not None
        or job.finished_at is not None
        or job.health_verified_at is not None
        or job.id != reference.job_id
        or job.repository_id != reference.repository_id
        or job.created_at > reference.issued_at
        or result.execution_id != execution.intent.execution_id
        or acknowledgement.snapshot_id != definition.snapshot.id
        or acknowledgement.base_revision != definition.base_revision
        or acknowledgement.definition_digest != definition.fingerprint()
        or (
            job.id,
            job.source_task_id,
            job.snapshot_id,
            job.repository_id,
            job.authorized_branch,
            job.target_branch,
            job.base_revision,
            job.policy_version,
            job.required_check_ids,
            job.max_repair_cycles,
            job.created_at,
        )
        != (
            definition.job_id,
            definition.source_task_id,
            definition.snapshot.id,
            definition.repository_id,
            definition.authorized_branch,
            definition.target_branch,
            definition.base_revision,
            definition.policy_version,
            tuple(check.id for check in definition.checks),
            definition.max_repair_cycles,
            definition.created_at,
        )
    ):
        raise ValueError("Coding acceptance changed the original running assignment.")
    result.validate(definition)


def fold_successful_coding_result(
    job: Job,
    definition: AssignedJobDefinition,
    execution: ExecutionProcessBinding,
    result: SuccessfulCodingResult,
    *,
    accepted_at: datetime,
) -> tuple[Job, ...]:
    """Plan every contiguous transition for one atomic control-side acceptance.

    The caller owns authentication, proof/receipt replay, fresh current authority
    and transactional persistence. Returned timestamps mean evidence acceptance;
    they are never reconstructed process times. No provider write, slot release
    or Job delivery completion occurs here. All original Job values stay immutable.
    """
    _validate_original(job, definition, execution, result)
    if not isinstance(accepted_at, datetime):
        raise ValueError("Coding acceptance requires a trusted current timestamp.")
    timestamp = _utc(accepted_at)
    attempt_id = execution.reference.attempt_id
    states: list[Job] = []
    current = job
    for cycle in result.cycles:
        current = current.begin_testing(attempt_id, cycle.revision, now=timestamp)
        states.append(current)
        if cycle.passed:
            current = current.accept_checks(
                attempt_id, cycle.domain_checks(), now=timestamp
            )
        else:
            current = current.request_repair(attempt_id, now=timestamp)
        states.append(current)
    return tuple(states)
