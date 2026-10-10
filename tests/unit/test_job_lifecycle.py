from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta, timezone
from typing import cast
from uuid import UUID, uuid4

import pytest

from devrelay.domain.jobs.job import Job
from devrelay.domain.jobs.values import (
    AttemptStatus,
    CheckResult,
    DeploymentReference,
    FailureCategory,
    HumanMergeEvidence,
    InvalidJobEvidence,
    InvalidJobTransition,
    JobStatus,
    PullRequestReference,
    ReleaseCIEvidence,
    RepairLimitExceeded,
    StaleAttempt,
    VerificationOutcome,
)

BASE = "a" * 40
CANDIDATE = "b" * 40
RELEASE = "c" * 40


@pytest.fixture
def job() -> Job:
    now = datetime(2026, 10, 9, 10, 0, tzinfo=UTC)
    return Job(
        id=uuid4(),
        source_task_id=uuid4(),
        snapshot_id=uuid4(),
        repository_id=uuid4(),
        authorized_branch="devrelay/task-5",
        target_branch="main",
        base_revision=BASE,
        policy_version="policy-v1",
        required_check_ids=("lint", "tests"),
        max_repair_cycles=2,
        created_at=now,
        updated_at=now,
    )


def next_time(job: Job) -> datetime:
    return job.updated_at + timedelta(seconds=1)


def _testing(job: Job) -> tuple[Job, UUID]:
    attempt = uuid4()
    claimed = job.claim(attempt, now=next_time(job))
    running = claimed.begin_execution(attempt, now=next_time(claimed))
    return running.begin_testing(attempt, CANDIDATE, now=next_time(running)), attempt


def checks(job: Job) -> tuple[CheckResult, ...]:
    assert job.candidate_revision is not None
    return tuple(
        CheckResult(
            check_id=check_id,
            revision=job.candidate_revision,
            outcome=VerificationOutcome.PASSED,
            exit_code=0,
        )
        for check_id in job.required_check_ids
    )


def reference(job: Job, *, number: int = 7) -> PullRequestReference:
    assert job.candidate_revision is not None
    return PullRequestReference(
        repository_id=job.repository_id,
        number=number,
        source_branch=job.authorized_branch,
        target_branch=job.target_branch,
        head_revision=job.candidate_revision,
    )


def awaiting_review(job: Job) -> Job:
    tested, attempt = _testing(job)
    publishing = tested.accept_checks(attempt, checks(tested), now=next_time(tested))
    return publishing.record_publication(
        attempt, reference(publishing), now=next_time(publishing)
    )


def awaiting_release(job: Job) -> Job:
    review = awaiting_review(job)
    evidence = HumanMergeEvidence(
        repository_id=review.repository_id,
        pull_request_number=7,
        reviewed_head_revision=CANDIDATE,
        release_revision=RELEASE,
    )
    return review.record_human_merge(evidence, now=next_time(review))


def release_ci(job: Job) -> ReleaseCIEvidence:
    return ReleaseCIEvidence(
        repository_id=job.repository_id,
        run_id="release-run-1",
        revision=RELEASE,
        outcome=VerificationOutcome.PASSED,
    )


def deployment(job: Job) -> DeploymentReference:
    return DeploymentReference(
        repository_id=job.repository_id,
        deployment_id="deployment-1",
        revision=RELEASE,
        artifact_id="release-image-1",
    )


def test_success_requires_human_merge_release_ci_and_matching_healthy_artifact(
    job: Job,
) -> None:
    merged = awaiting_release(job)
    started = merged.begin_deployment(
        release_ci(merged), deployment(merged), now=next_time(merged)
    )
    evidence, now = deployment(started), next_time(started)

    complete = started.complete_deployment(
        evidence, VerificationOutcome.PASSED, now=now
    )

    assert complete.status is JobStatus.SUCCEEDED
    assert complete.workflow_version == 8
    assert complete.finished_at == complete.updated_at
    assert not complete.blocks_new_submission
    assert complete.snapshot_id == job.snapshot_id
    assert complete.source_task_id == job.source_task_id


def test_publication_waits_for_review_and_releases_execution_reservation(
    job: Job,
) -> None:
    review = awaiting_review(job)

    assert review.status is JobStatus.AWAITING_REVIEW
    assert review.blocks_new_submission and not review.reserves_execution
    assert review.finished_at is None


def test_deployment_cannot_complete_before_human_merge(job: Job) -> None:
    review = awaiting_review(job)
    evidence, now = deployment(review), next_time(review)

    with pytest.raises(InvalidJobTransition):
        review.complete_deployment(evidence, VerificationOutcome.PASSED, now=now)


@pytest.mark.parametrize(
    "invalid", ["missing", "duplicate", "unknown", "failed", "wrong_revision"]
)
def test_checks_cannot_omit_or_substitute_required_passing_results(
    job: Job, invalid: str
) -> None:
    tested, attempt = _testing(job)
    evidence = checks(tested)
    if invalid == "missing":
        evidence = evidence[:1]
    elif invalid == "duplicate":
        evidence = (evidence[0], evidence[0])
    elif invalid == "unknown":
        evidence = (evidence[0], replace(evidence[1], check_id="unconfigured"))
    elif invalid == "failed":
        evidence = (
            evidence[0],
            replace(evidence[1], outcome=VerificationOutcome.FAILED, exit_code=1),
        )
    else:
        evidence = (evidence[0], replace(evidence[1], revision=BASE))
    with pytest.raises(InvalidJobEvidence):
        tested.accept_checks(attempt, evidence, now=next_time(tested))
    assert tested.status is JobStatus.TESTING
    assert tested.passed_checks == ()


@pytest.mark.parametrize(
    "mismatch", ["repository", "source_branch", "target_branch", "head"]
)
def test_publication_is_correlated_to_authorized_repository_branches_and_checks(
    job: Job, mismatch: str
) -> None:
    tested, attempt = _testing(job)
    publishing = tested.accept_checks(attempt, checks(tested), now=next_time(tested))
    evidence = reference(publishing)
    if mismatch == "repository":
        evidence = replace(evidence, repository_id=uuid4())
    elif mismatch == "source_branch":
        evidence = replace(evidence, source_branch="unauthorized/branch")
    elif mismatch == "target_branch":
        evidence = replace(evidence, target_branch="different-main")
    else:
        evidence = replace(evidence, head_revision=BASE)
    with pytest.raises(InvalidJobEvidence):
        publishing.record_publication(attempt, evidence, now=next_time(publishing))
    assert publishing.reserves_execution


def test_repair_clears_candidate_within_the_same_attempt(
    job: Job,
) -> None:
    tested, attempt = _testing(replace(job, max_repair_cycles=1))

    repairing = tested.request_repair(attempt, now=next_time(tested))

    assert repairing.candidate_revision is None
    assert repairing.last_attempt_number == 1
    assert repairing.active_attempt_id == attempt


def repaired_job(job: Job) -> tuple[Job, UUID, tuple[CheckResult, ...]]:
    tested, attempt = _testing(replace(job, max_repair_cycles=1))
    old_checks = checks(tested)
    repairing = tested.request_repair(attempt, now=next_time(tested))
    return (
        repairing.begin_testing(attempt, RELEASE, now=next_time(repairing)),
        attempt,
        old_checks,
    )


def test_repair_invalidates_old_checks(job: Job) -> None:
    repaired, attempt, old_checks = repaired_job(job)

    with pytest.raises(InvalidJobEvidence):
        repaired.accept_checks(attempt, old_checks, now=next_time(repaired))


def test_repair_budget_is_bounded_within_one_attempt(job: Job) -> None:
    repaired, attempt, _ = repaired_job(job)

    with pytest.raises(RepairLimitExceeded):
        repaired.request_repair(attempt, now=next_time(repaired))


def test_known_stopped_failure_can_retry_with_a_new_attempt_but_same_snapshot(
    job: Job,
) -> None:
    tested, old_attempt = _testing(job)
    failed = tested.record_failure(
        FailureCategory.TEST_FAILURE,
        "Configured tests failed.",
        stopped_attempt_id=old_attempt,
        now=next_time(tested),
    )
    queued = failed.retry_execution(now=next_time(failed))
    new_attempt = uuid4()

    claimed = queued.claim(new_attempt, now=next_time(queued))

    assert claimed.last_attempt_number == 2
    assert claimed.snapshot_id == job.snapshot_id
    assert claimed.candidate_revision is None


def stopped_failure(job: Job) -> tuple[Job, UUID]:
    tested, attempt = _testing(job)
    return tested.record_failure(
        FailureCategory.TEST_FAILURE,
        "Configured tests failed.",
        stopped_attempt_id=attempt,
        now=next_time(tested),
    ), attempt


def test_failure_requires_proof_of_the_stopped_attempt(job: Job) -> None:
    tested, _ = _testing(job)

    with pytest.raises(StaleAttempt):
        tested.record_failure(
            FailureCategory.TEST_FAILURE,
            "Configured tests failed.",
            now=next_time(tested),
        )


def test_stopped_failure_releases_execution_but_blocks_new_submission(job: Job) -> None:
    tested, attempt = _testing(job)

    failed = tested.record_failure(
        FailureCategory.TEST_FAILURE,
        "Configured tests failed.",
        stopped_attempt_id=attempt,
        now=next_time(tested),
    )

    assert failed.failure is not None and failed.failure.stage is JobStatus.TESTING
    assert not failed.reserves_execution and failed.blocks_new_submission


def test_retry_cannot_reuse_the_previous_attempt(job: Job) -> None:
    failed, previous = stopped_failure(job)
    queued = failed.retry_execution(now=next_time(failed))

    with pytest.raises(StaleAttempt):
        queued.claim(previous, now=next_time(queued))


def test_previous_attempt_cannot_execute_a_new_claim(job: Job) -> None:
    failed, previous = stopped_failure(job)
    queued = failed.retry_execution(now=next_time(failed))
    claimed = queued.claim(uuid4(), now=next_time(queued))

    with pytest.raises(StaleAttempt):
        claimed.begin_execution(previous, now=next_time(claimed))


@pytest.mark.parametrize(
    "category", [FailureCategory.LEASE_LOST, FailureCategory.UNCERTAIN_SIDE_EFFECT]
)
def test_uncertainty_preserves_execution_reservation_and_blocks_reassignment(
    job: Job, category: FailureCategory
) -> None:
    tested, attempt = _testing(job)

    paused = tested.require_recovery(
        category, "Execution outcome is uncertain.", now=next_time(tested)
    )
    assert paused.status is JobStatus.RECOVERY_REQUIRED
    assert paused.active_attempt_id == attempt
    assert paused.reserves_execution and paused.blocks_new_submission


@pytest.mark.parametrize(
    "category", [FailureCategory.LEASE_LOST, FailureCategory.UNCERTAIN_SIDE_EFFECT]
)
def test_uncertain_failure_cannot_be_recorded_as_stopped(
    job: Job, category: FailureCategory
) -> None:
    tested, attempt = _testing(job)

    with pytest.raises(InvalidJobTransition):
        tested.record_failure(
            category,
            "Execution outcome is uncertain.",
            stopped_attempt_id=attempt,
            now=next_time(tested),
        )


@pytest.mark.parametrize(
    "category", [FailureCategory.LEASE_LOST, FailureCategory.UNCERTAIN_SIDE_EFFECT]
)
@pytest.mark.parametrize("operation", ["retry", "claim", "rehydrate_failure"])
def test_recovery_cannot_bypass_execution_reservation(
    job: Job, category: FailureCategory, operation: str
) -> None:
    tested, _ = _testing(job)
    paused = tested.require_recovery(
        category, "Execution outcome is uncertain.", now=next_time(tested)
    )
    error = (
        InvalidJobEvidence if operation == "rehydrate_failure" else InvalidJobTransition
    )

    with pytest.raises(error):
        if operation == "retry":
            paused.retry_execution(now=next_time(paused))
        elif operation == "claim":
            paused.claim(uuid4(), now=next_time(paused))
        else:
            replace(paused, status=JobStatus.FAILED, active_attempt_id=None)


def test_human_review_follow_up_preserves_existing_pr_and_pins_approved_current_head(
    job: Job,
) -> None:
    review = awaiting_review(job)
    current = replace(reference(review), head_revision=RELEASE)

    follow_up = review.approve_review_follow_up(current, now=next_time(review))

    assert follow_up.base_revision == RELEASE
    assert follow_up.snapshot_id == job.snapshot_id
    assert follow_up.pull_request == current


def follow_up_publication(job: Job) -> tuple[Job, UUID]:
    review = awaiting_review(job)
    assert review.pull_request is not None
    current = replace(review.pull_request, head_revision=RELEASE)
    follow_up = review.approve_review_follow_up(current, now=next_time(review))
    tested, attempt = _testing(follow_up)
    publishing = tested.accept_checks(attempt, checks(tested), now=next_time(tested))
    return publishing, attempt


def test_review_follow_up_cannot_publish_another_pr(job: Job) -> None:
    publishing, attempt = follow_up_publication(job)
    evidence = reference(publishing, number=8)

    with pytest.raises(InvalidJobEvidence):
        publishing.record_publication(attempt, evidence, now=next_time(publishing))


def test_review_follow_up_updates_the_existing_pr(job: Job) -> None:
    publishing, attempt = follow_up_publication(job)
    evidence = reference(publishing)

    updated = publishing.record_publication(
        attempt, evidence, now=next_time(publishing)
    )

    assert updated.last_attempt_number == 2
    assert updated.pull_request is not None and updated.pull_request.number == 7


@pytest.mark.parametrize("mismatch", ["repository", "number", "head"])
def test_merge_of_another_repository_pr_or_head_cannot_release_job(
    job: Job, mismatch: str
) -> None:
    review = awaiting_review(job)
    evidence = HumanMergeEvidence(
        repository_id=job.repository_id,
        pull_request_number=7,
        reviewed_head_revision=CANDIDATE,
        release_revision=RELEASE,
    )
    if mismatch == "repository":
        evidence = replace(evidence, repository_id=uuid4())
    elif mismatch == "number":
        evidence = replace(evidence, pull_request_number=8)
    else:
        evidence = replace(evidence, reviewed_head_revision=BASE)
    with pytest.raises(InvalidJobEvidence):
        review.record_human_merge(evidence, now=next_time(review))
    assert review.release_revision is None


@pytest.mark.parametrize(
    "mismatch",
    [
        "failed_ci",
        "ci_revision",
        "ci_repository",
        "deployment_revision",
        "deployment_repository",
    ],
)
def test_release_ci_and_deployment_start_must_match_the_merged_revision(
    job: Job, mismatch: str
) -> None:
    merged = awaiting_release(job)
    ci, deployed = release_ci(merged), deployment(merged)
    if mismatch == "failed_ci":
        ci = replace(ci, outcome=VerificationOutcome.FAILED)
    elif mismatch == "ci_revision":
        ci = replace(ci, revision=BASE)
    elif mismatch == "ci_repository":
        ci = replace(ci, repository_id=uuid4())
    elif mismatch == "deployment_revision":
        deployed = replace(deployed, revision=BASE)
    else:
        deployed = replace(deployed, repository_id=uuid4())
    with pytest.raises(InvalidJobEvidence):
        merged.begin_deployment(ci, deployed, now=next_time(merged))
    assert merged.deployment is None


@pytest.mark.parametrize(
    "mismatch", ["unhealthy", "deployment", "artifact", "revision", "repository"]
)
def test_another_or_unhealthy_deployment_cannot_complete_job(
    job: Job, mismatch: str
) -> None:
    merged = awaiting_release(job)
    deployed = deployment(merged)
    started = merged.begin_deployment(
        release_ci(merged), deployed, now=next_time(merged)
    )
    observed, health = deployed, VerificationOutcome.PASSED
    if mismatch == "unhealthy":
        health = VerificationOutcome.FAILED
    elif mismatch == "deployment":
        observed = replace(observed, deployment_id="another-deployment")
    elif mismatch == "artifact":
        observed = replace(observed, artifact_id="another-image")
    elif mismatch == "revision":
        observed = replace(observed, revision=BASE)
    else:
        observed = replace(observed, repository_id=uuid4())
    with pytest.raises(InvalidJobEvidence):
        started.complete_deployment(observed, health, now=next_time(started))
    assert started.finished_at is None


@pytest.mark.parametrize("operation", ["retry", "cancel"])
def test_post_merge_failure_cannot_restart_coding_or_be_cancelled_as_rollback(
    job: Job,
    operation: str,
) -> None:
    merged = awaiting_release(job)
    failed = merged.record_failure(
        FailureCategory.TEST_FAILURE, "Release CI failed.", now=next_time(merged)
    )

    with pytest.raises(InvalidJobTransition):
        if operation == "retry":
            failed.retry_execution(now=next_time(failed))
        else:
            failed.request_cancellation(now=next_time(failed))

    assert failed.blocks_new_submission


def test_post_merge_job_has_no_coding_lease_to_recover(job: Job) -> None:
    merged = awaiting_release(job)

    with pytest.raises(InvalidJobEvidence):
        merged.require_recovery(
            FailureCategory.LEASE_LOST, "No coding lease exists.", now=next_time(merged)
        )


def test_cancellation_keeps_active_reservation_until_containment_is_confirmed(
    job: Job,
) -> None:
    tested, _ = _testing(job)

    requested = tested.request_cancellation(now=next_time(tested))

    assert requested.status is JobStatus.CANCEL_REQUESTED
    assert requested.reserves_execution


def test_cancellation_request_stops_accepting_checks(job: Job) -> None:
    tested, attempt = _testing(job)
    requested = tested.request_cancellation(now=next_time(tested))
    evidence = checks(tested)

    with pytest.raises(InvalidJobTransition):
        requested.accept_checks(attempt, evidence, now=next_time(requested))


def test_cancellation_requires_proof_of_the_stopped_attempt(job: Job) -> None:
    tested, _ = _testing(job)
    requested = tested.request_cancellation(now=next_time(tested))

    with pytest.raises(StaleAttempt):
        requested.confirm_cancellation(now=next_time(requested))


def test_confirmed_containment_releases_cancelled_execution(job: Job) -> None:
    tested, attempt = _testing(job)
    requested = tested.request_cancellation(now=next_time(tested))

    cancelled = requested.confirm_cancellation(
        stopped_attempt_id=attempt, now=next_time(requested)
    )

    assert cancelled.status is JobStatus.CANCELLED
    assert not cancelled.reserves_execution and not cancelled.blocks_new_submission


def test_unconfirmed_cancellation_retains_execution_reservation(job: Job) -> None:
    tested, _ = _testing(job)
    requested = tested.request_cancellation(now=next_time(tested))

    unknown = requested.require_recovery(
        FailureCategory.UNCERTAIN_SIDE_EFFECT,
        "Containment is not confirmed.",
        now=next_time(requested),
    )

    assert unknown.reserves_execution and unknown.finished_at is None


def test_queued_cancellation_is_immediately_terminal(job: Job) -> None:
    queued_cancelled = job.request_cancellation(now=next_time(job))

    assert queued_cancelled.status is JobStatus.CANCELLED


def test_cancellation_without_execution_retains_existing_pr_reference(job: Job) -> None:
    review = awaiting_review(job)
    requested = review.request_cancellation(now=next_time(review))

    cancelled = requested.confirm_cancellation(now=next_time(requested))

    assert cancelled.pull_request == review.pull_request
    assert cancelled.finished_at is not None


def test_job_cannot_be_mutated(job: Job) -> None:
    terminal = job.request_cancellation(now=next_time(job))

    with pytest.raises(FrozenInstanceError):
        terminal.status = JobStatus.QUEUED  # type: ignore[misc]  # Deliberate mutation attempt.


@pytest.mark.parametrize("operation", ["claim", "retry"])
def test_terminal_workflow_cannot_be_restarted(job: Job, operation: str) -> None:
    terminal = job.request_cancellation(now=next_time(job))

    with pytest.raises(InvalidJobTransition):
        if operation == "claim":
            terminal.claim(uuid4(), now=next_time(terminal))
        else:
            terminal.retry_execution(now=next_time(terminal))


def test_job_cannot_be_rehydrated_as_success_without_delivery_evidence(
    job: Job,
) -> None:
    with pytest.raises(InvalidJobEvidence):
        replace(job, status=JobStatus.SUCCEEDED, finished_at=job.updated_at)


def test_timestamps_are_utc_monotonic_and_versions_change_only_on_accepted_commands(
    job: Job,
) -> None:
    local = job.updated_at.astimezone(timezone(timedelta(hours=3)))

    claimed = job.claim(uuid4(), now=local)

    assert claimed.updated_at.tzinfo is UTC
    assert claimed.workflow_version == 1
    assert job.workflow_version == 0


def test_backwards_time_cannot_change_the_job_version(job: Job) -> None:
    claimed = job.claim(uuid4(), now=job.updated_at)
    attempt = cast(UUID, claimed.active_attempt_id)
    earlier = job.updated_at - timedelta(seconds=1)

    with pytest.raises(InvalidJobEvidence):
        claimed.begin_execution(attempt, now=earlier)

    assert claimed.workflow_version == 1
    assert job.workflow_version == 0


def test_naive_job_timestamp_is_rejected(job: Job) -> None:
    naive = datetime(2026, 10, 9)

    with pytest.raises(InvalidJobEvidence):
        job.claim(uuid4(), now=naive)


def test_enum_values_are_explicit_and_distinct_for_jobs_and_attempts() -> None:
    job_values = {status.value for status in JobStatus}
    attempt_values = {status.value for status in AttemptStatus}

    assert job_values == {
        "queued",
        "claimed",
        "running",
        "testing",
        "publishing_pr",
        "awaiting_review",
        "awaiting_release_ci",
        "deploying",
        "succeeded",
        "failed",
        "recovery_required",
        "cancel_requested",
        "cancelled",
    }
    assert attempt_values == {
        "claimed",
        "running",
        "testing",
        "publishing_pr",
        "succeeded",
        "failed",
        "lease_expired",
        "cancelled",
    }


def test_rehydrated_success_requires_a_health_verification_record(job: Job) -> None:
    merged = awaiting_release(job)
    started = merged.begin_deployment(
        release_ci(merged), deployment(merged), now=next_time(merged)
    )
    with pytest.raises(InvalidJobEvidence, match="health verification"):
        replace(started, status=JobStatus.SUCCEEDED, finished_at=started.updated_at)


def test_skipping_assignment_does_not_start_execution(job: Job) -> None:
    with pytest.raises(InvalidJobTransition):
        job.begin_execution(uuid4(), now=next_time(job))


def test_skipping_checks_does_not_allow_publication(job: Job) -> None:
    tested, attempt = _testing(job)
    evidence = reference(tested)

    with pytest.raises(InvalidJobTransition):
        tested.record_publication(attempt, evidence, now=next_time(tested))


@pytest.mark.parametrize(
    "reason",
    ["identity", "missing_checks", "duplicate_checks", "same_branch", "budget"],
)
def test_invalid_job_policy_and_identity_are_rejected(job: Job, reason: str) -> None:
    with pytest.raises(InvalidJobEvidence):
        if reason == "identity":
            replace(job, repository_id=cast(UUID, "unparsed-repository-id"))
        elif reason == "missing_checks":
            replace(job, required_check_ids=())
        elif reason == "duplicate_checks":
            replace(job, required_check_ids=("tests", "tests"))
        elif reason == "same_branch":
            replace(job, target_branch=job.authorized_branch)
        else:
            replace(job, max_repair_cycles=-1)


@pytest.mark.parametrize(
    "revision,outcome,exit_code",
    [
        (CANDIDATE, VerificationOutcome.PASSED, 1),
        (CANDIDATE, VerificationOutcome.FAILED, 0),
        ("short-ref", VerificationOutcome.PASSED, 0),
    ],
)
def test_check_evidence_rejects_mismatched_exit_code_or_invalid_revision(
    revision: str, outcome: VerificationOutcome, exit_code: int
) -> None:
    with pytest.raises(InvalidJobEvidence):
        CheckResult(
            check_id="tests",
            revision=revision,
            outcome=outcome,
            exit_code=exit_code,
        )
