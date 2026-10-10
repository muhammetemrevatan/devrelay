from dataclasses import dataclass, replace
from datetime import datetime
from typing import TypedDict, Unpack
from uuid import UUID

from devrelay.domain.jobs.values import (
    CheckResult,
    DeploymentReference,
    FailureCategory,
    HumanMergeEvidence,
    InvalidJobEvidence,
    InvalidJobTransition,
    JobFailure,
    JobStatus,
    PullRequestReference,
    ReleaseCIEvidence,
    RepairLimitExceeded,
    StaleAttempt,
    VerificationOutcome,
    _branch,
    _revision,
    _text,
    _utc,
    _uuid,
)


class _JobChanges(TypedDict, total=False):
    status: JobStatus
    active_attempt_id: UUID | None
    last_attempt_id: UUID | None
    last_attempt_number: int
    repair_cycles: int
    base_revision: str
    candidate_revision: str | None
    passed_checks: tuple[CheckResult, ...]
    pull_request: PullRequestReference | None
    release_revision: str | None
    release_ci_run_id: str | None
    deployment: DeploymentReference | None
    failure: JobFailure | None
    finished_at: datetime | None
    health_verified_at: datetime | None


_EXECUTION_STATES = frozenset(
    {JobStatus.CLAIMED, JobStatus.RUNNING, JobStatus.TESTING, JobStatus.PUBLISHING_PR}
)
_DELIVERY_STATES = frozenset(
    {
        JobStatus.AWAITING_REVIEW,
        JobStatus.AWAITING_RELEASE_CI,
        JobStatus.DEPLOYING,
        JobStatus.SUCCEEDED,
    }
)
_UNCERTAIN_FAILURES = frozenset(
    {FailureCategory.LEASE_LOST, FailureCategory.UNCERTAIN_SIDE_EFFECT}
)


@dataclass(frozen=True, slots=True, kw_only=True)
class Job:
    """One pinned delivery, independent of worker lease and persistence mechanics.

    These methods validate lifecycle and evidence correlation. Application must
    authenticate/authorize callers, verify remote evidence and execution containment,
    and apply DB version/claim/idempotency guards before exposing commands.
    """

    id: UUID
    source_task_id: UUID
    snapshot_id: UUID
    repository_id: UUID
    authorized_branch: str
    target_branch: str
    base_revision: str
    policy_version: str
    required_check_ids: tuple[str, ...]
    max_repair_cycles: int
    created_at: datetime
    updated_at: datetime
    status: JobStatus = JobStatus.QUEUED
    workflow_version: int = 0
    active_attempt_id: UUID | None = None
    last_attempt_id: UUID | None = None
    last_attempt_number: int = 0
    repair_cycles: int = 0
    candidate_revision: str | None = None
    passed_checks: tuple[CheckResult, ...] = ()
    pull_request: PullRequestReference | None = None
    release_revision: str | None = None
    release_ci_run_id: str | None = None
    deployment: DeploymentReference | None = None
    failure: JobFailure | None = None
    finished_at: datetime | None = None
    health_verified_at: datetime | None = None

    def __post_init__(self) -> None:
        for identity in (
            self.id,
            self.source_task_id,
            self.snapshot_id,
            self.repository_id,
        ):
            _uuid(identity)
        if self.active_attempt_id is not None:
            _uuid(self.active_attempt_id)
        if self.last_attempt_id is not None:
            _uuid(self.last_attempt_id)
        if not isinstance(self.status, JobStatus):
            raise InvalidJobEvidence("Job status must be typed.")
        _branch(self.authorized_branch)
        _branch(self.target_branch)
        if self.authorized_branch == self.target_branch:
            raise InvalidJobEvidence("Coding and target branches must be distinct.")
        _revision(self.base_revision)
        _text(self.policy_version, "Policy version", 64)
        if (
            not isinstance(self.required_check_ids, tuple)
            or not 1 <= len(self.required_check_ids) <= 32
        ):
            raise InvalidJobEvidence(
                "Required checks must be a bounded immutable tuple."
            )
        for check_id in self.required_check_ids:
            _text(check_id, "Required check ID", 128)
        if len(set(self.required_check_ids)) != len(self.required_check_ids):
            raise InvalidJobEvidence("Required check IDs must be unique.")
        for count in (
            self.workflow_version,
            self.last_attempt_number,
            self.repair_cycles,
            self.max_repair_cycles,
        ):
            if type(count) is not int or count < 0:
                raise InvalidJobEvidence(
                    "Lifecycle counters must be nonnegative integers."
                )
        if self.repair_cycles > self.max_repair_cycles:
            raise InvalidJobEvidence("Repair budget cannot be exceeded.")
        object.__setattr__(self, "created_at", _utc(self.created_at))
        object.__setattr__(self, "updated_at", _utc(self.updated_at))
        if self.updated_at < self.created_at:
            raise InvalidJobEvidence("Job timestamps cannot move backwards.")
        if (self.last_attempt_id is None) != (self.last_attempt_number == 0):
            raise InvalidJobEvidence("Attempt identity and counter must agree.")
        if (
            self.active_attempt_id is not None
            and self.active_attempt_id != self.last_attempt_id
        ):
            raise InvalidJobEvidence("Active attempt must match the latest assignment.")
        if self.status in _EXECUTION_STATES and self.active_attempt_id is None:
            raise InvalidJobEvidence("Execution requires an assigned attempt.")
        if (
            self.active_attempt_id is not None
            and self.status
            not in _EXECUTION_STATES
            | {JobStatus.CANCEL_REQUESTED, JobStatus.RECOVERY_REQUIRED}
        ):
            raise InvalidJobEvidence("This state cannot retain active execution.")
        if self.candidate_revision is not None:
            _revision(self.candidate_revision)
        if (
            self.status in {JobStatus.TESTING, JobStatus.PUBLISHING_PR}
            and self.candidate_revision is None
        ):
            raise InvalidJobEvidence(
                "Testing/publication requires a candidate revision."
            )
        if not isinstance(self.passed_checks, tuple):
            raise InvalidJobEvidence("Check evidence must be immutable.")
        if self.passed_checks:
            self._check_evidence(self.passed_checks)
        if self.status is JobStatus.PUBLISHING_PR or self.status in _DELIVERY_STATES:
            self._check_evidence(self.passed_checks)
        if self.pull_request is not None:
            self._check_pr_scope(self.pull_request)
        if self.status in _DELIVERY_STATES:
            if (
                self.pull_request is None
                or self.pull_request.head_revision != self.candidate_revision
            ):
                raise InvalidJobEvidence(
                    "Delivery requires the checked published head."
                )
        if self.release_revision is not None:
            _revision(self.release_revision)
            if self.pull_request is None:
                raise InvalidJobEvidence("Release evidence requires a pull request.")
        if (
            self.status
            in {JobStatus.AWAITING_RELEASE_CI, JobStatus.DEPLOYING, JobStatus.SUCCEEDED}
            and self.release_revision is None
        ):
            raise InvalidJobEvidence("Release requires confirmed merge evidence.")
        if self.release_ci_run_id is not None:
            _text(self.release_ci_run_id, "Release CI run ID")
        if self.deployment is not None:
            if (
                self.deployment.repository_id != self.repository_id
                or self.deployment.revision != self.release_revision
                or self.release_ci_run_id is None
            ):
                raise InvalidJobEvidence("Deployment must match the verified release.")
        if (
            self.status in {JobStatus.DEPLOYING, JobStatus.SUCCEEDED}
            and self.deployment is None
        ):
            raise InvalidJobEvidence("Deployment identity is required.")
        if (
            self.status in {JobStatus.FAILED, JobStatus.RECOVERY_REQUIRED}
            and self.failure is None
        ):
            raise InvalidJobEvidence("Failure/recovery requires classified evidence.")
        if self.failure is not None:
            if not self.created_at <= self.failure.recorded_at <= self.updated_at:
                raise InvalidJobEvidence(
                    "Failure timestamp must be within Job history."
                )
            if self.status is JobStatus.FAILED and (
                self.failure.category in _UNCERTAIN_FAILURES
                or self.failure.stage
                not in _EXECUTION_STATES
                | {JobStatus.AWAITING_RELEASE_CI, JobStatus.DEPLOYING}
            ):
                raise InvalidJobEvidence(
                    "Uncertain failure cannot be restored as retryable execution."
                )
            if (
                self.status is JobStatus.RECOVERY_REQUIRED
                and self.failure.category not in _UNCERTAIN_FAILURES
            ):
                raise InvalidJobEvidence(
                    "Recovery must retain its uncertainty category."
                )
            if (
                self.failure.category is FailureCategory.LEASE_LOST
                and self.active_attempt_id is None
            ):
                raise InvalidJobEvidence(
                    "Lost lease recovery must retain its attempt reservation."
                )
        if self.finished_at is not None:
            object.__setattr__(self, "finished_at", _utc(self.finished_at))
            if not self.created_at <= self.finished_at <= self.updated_at:
                raise InvalidJobEvidence(
                    "Completion timestamp must be within Job history."
                )
        if (self.finished_at is not None) != (
            self.status in {JobStatus.SUCCEEDED, JobStatus.CANCELLED}
        ):
            raise InvalidJobEvidence(
                "Only terminal workflows have a completion timestamp."
            )
        if self.health_verified_at is not None:
            object.__setattr__(
                self, "health_verified_at", _utc(self.health_verified_at)
            )
            if not self.created_at <= self.health_verified_at <= self.updated_at:
                raise InvalidJobEvidence(
                    "Health verification must be within Job history."
                )
            if self.finished_at is None or self.health_verified_at > self.finished_at:
                raise InvalidJobEvidence(
                    "Delivery cannot finish before health verification."
                )
        if (self.health_verified_at is not None) != (
            self.status is JobStatus.SUCCEEDED
        ):
            raise InvalidJobEvidence(
                "Successful delivery requires recorded health verification."
            )

    @property
    def blocks_new_submission(self) -> bool:
        return self.status not in {JobStatus.SUCCEEDED, JobStatus.CANCELLED}

    @property
    def reserves_execution(self) -> bool:
        return self.active_attempt_id is not None

    def _require_state(self, *states: JobStatus) -> None:
        if self.status not in states:
            raise InvalidJobTransition(
                "Command is not allowed in the current Job state."
            )

    def _require_attempt(self, attempt_id: UUID) -> None:
        if self.active_attempt_id is None or attempt_id != self.active_attempt_id:
            raise StaleAttempt("The current attempt identity is required.")

    def _changed(self, now: datetime, **changes: Unpack[_JobChanges]) -> Job:
        timestamp = _utc(now)
        if timestamp < self.updated_at:
            raise InvalidJobEvidence("Lifecycle timestamps cannot move backwards.")
        return replace(
            self,
            updated_at=timestamp,
            workflow_version=self.workflow_version + 1,
            **changes,
        )

    def _check_evidence(self, checks: tuple[CheckResult, ...]) -> None:
        if (
            not isinstance(checks, tuple)
            or len(checks) != len(self.required_check_ids)
            or {check.check_id for check in checks} != set(self.required_check_ids)
        ):
            raise InvalidJobEvidence("Every configured check is required exactly once.")
        if any(
            check.outcome is not VerificationOutcome.PASSED
            or check.revision != self.candidate_revision
            for check in checks
        ):
            raise InvalidJobEvidence("All checks must pass for the candidate revision.")

    def _check_pr_scope(self, reference: PullRequestReference) -> None:
        if (
            reference.repository_id != self.repository_id
            or reference.source_branch != self.authorized_branch
            or reference.target_branch != self.target_branch
        ):
            raise InvalidJobEvidence(
                "Pull request must match the authorized repository/branches."
            )

    def claim(self, attempt_id: UUID, *, now: datetime) -> Job:
        self._require_state(JobStatus.QUEUED)
        if attempt_id == self.last_attempt_id:
            raise StaleAttempt("A new assignment requires a new attempt identity.")
        return self._changed(
            now,
            status=JobStatus.CLAIMED,
            active_attempt_id=attempt_id,
            last_attempt_id=attempt_id,
            last_attempt_number=self.last_attempt_number + 1,
            repair_cycles=0,
            failure=None,
        )

    def begin_execution(self, attempt_id: UUID, *, now: datetime) -> Job:
        self._require_state(JobStatus.CLAIMED)
        self._require_attempt(attempt_id)
        return self._changed(now, status=JobStatus.RUNNING)

    def begin_testing(self, attempt_id: UUID, revision: str, *, now: datetime) -> Job:
        self._require_state(JobStatus.RUNNING)
        self._require_attempt(attempt_id)
        _revision(revision)
        return self._changed(
            now, status=JobStatus.TESTING, candidate_revision=revision, passed_checks=()
        )

    def request_repair(self, attempt_id: UUID, *, now: datetime) -> Job:
        self._require_state(JobStatus.TESTING)
        self._require_attempt(attempt_id)
        if self.repair_cycles >= self.max_repair_cycles:
            raise RepairLimitExceeded("The configured repair budget is exhausted.")
        return self._changed(
            now,
            status=JobStatus.RUNNING,
            repair_cycles=self.repair_cycles + 1,
            candidate_revision=None,
            passed_checks=(),
        )

    def accept_checks(
        self, attempt_id: UUID, checks: tuple[CheckResult, ...], *, now: datetime
    ) -> Job:
        self._require_state(JobStatus.TESTING)
        self._require_attempt(attempt_id)
        self._check_evidence(checks)
        return self._changed(now, status=JobStatus.PUBLISHING_PR, passed_checks=checks)

    def record_publication(
        self, attempt_id: UUID, reference: PullRequestReference, *, now: datetime
    ) -> Job:
        self._require_state(JobStatus.PUBLISHING_PR)
        self._require_attempt(attempt_id)
        self._check_pr_scope(reference)
        if reference.head_revision != self.candidate_revision or (
            self.pull_request is not None
            and reference.number != self.pull_request.number
        ):
            raise InvalidJobEvidence(
                "Publication must preserve the checked head and existing PR identity."
            )
        return self._changed(
            now,
            status=JobStatus.AWAITING_REVIEW,
            pull_request=reference,
            active_attempt_id=None,
        )

    def approve_review_follow_up(
        self, current_pr: PullRequestReference, *, now: datetime
    ) -> Job:
        self._require_state(JobStatus.AWAITING_REVIEW)
        self._check_pr_scope(current_pr)
        if self.pull_request is None or current_pr.number != self.pull_request.number:
            raise InvalidJobEvidence("Review follow-up must use the existing PR.")
        return self._changed(
            now,
            status=JobStatus.QUEUED,
            base_revision=current_pr.head_revision,
            pull_request=current_pr,
            candidate_revision=None,
            passed_checks=(),
        )

    def record_human_merge(self, evidence: HumanMergeEvidence, *, now: datetime) -> Job:
        self._require_state(JobStatus.AWAITING_REVIEW)
        reference = self.pull_request
        if (
            reference is None
            or evidence.repository_id != self.repository_id
            or evidence.pull_request_number != reference.number
            or evidence.reviewed_head_revision != reference.head_revision
        ):
            raise InvalidJobEvidence("Merge must match the reviewed pull request head.")
        return self._changed(
            now,
            status=JobStatus.AWAITING_RELEASE_CI,
            release_revision=evidence.release_revision,
        )

    def begin_deployment(
        self, ci: ReleaseCIEvidence, deployment: DeploymentReference, *, now: datetime
    ) -> Job:
        self._require_state(JobStatus.AWAITING_RELEASE_CI)
        if (
            ci.outcome is not VerificationOutcome.PASSED
            or ci.repository_id != self.repository_id
            or ci.revision != self.release_revision
            or deployment.repository_id != self.repository_id
            or deployment.revision != self.release_revision
        ):
            raise InvalidJobEvidence(
                "Deployment requires passed release CI for the matching revision."
            )
        return self._changed(
            now,
            status=JobStatus.DEPLOYING,
            release_ci_run_id=ci.run_id,
            deployment=deployment,
        )

    def complete_deployment(
        self,
        observed: DeploymentReference,
        health: VerificationOutcome,
        *,
        now: datetime,
    ) -> Job:
        self._require_state(JobStatus.DEPLOYING)
        if observed != self.deployment or health is not VerificationOutcome.PASSED:
            raise InvalidJobEvidence(
                "Completion requires the matching healthy deployment/artifact."
            )
        return self._changed(
            now,
            status=JobStatus.SUCCEEDED,
            finished_at=_utc(now),
            health_verified_at=_utc(now),
        )

    def record_failure(
        self,
        category: FailureCategory,
        summary: str,
        *,
        stopped_attempt_id: UUID | None = None,
        now: datetime,
    ) -> Job:
        self._require_state(
            *_EXECUTION_STATES, JobStatus.AWAITING_RELEASE_CI, JobStatus.DEPLOYING
        )
        if category in _UNCERTAIN_FAILURES:
            raise InvalidJobTransition(
                "Uncertain execution/effects require recovery, not a retryable failure."
            )
        if self.active_attempt_id != stopped_attempt_id:
            raise StaleAttempt(
                "Failure requires the matching confirmed stopped attempt."
            )
        failure = JobFailure(
            stage=self.status, category=category, summary=summary, recorded_at=now
        )
        return self._changed(
            now, status=JobStatus.FAILED, active_attempt_id=None, failure=failure
        )

    def retry_execution(self, *, now: datetime) -> Job:
        self._require_state(JobStatus.FAILED)
        if (
            self.release_revision is not None
            or self.failure is None
            or self.failure.stage not in _EXECUTION_STATES
        ):
            raise InvalidJobTransition("Post-merge failures cannot restart coding.")
        return self._changed(
            now,
            status=JobStatus.QUEUED,
            candidate_revision=None,
            passed_checks=(),
            failure=None,
            repair_cycles=0,
        )

    def require_recovery(
        self, category: FailureCategory, summary: str, *, now: datetime
    ) -> Job:
        self._require_state(
            *_EXECUTION_STATES,
            JobStatus.AWAITING_RELEASE_CI,
            JobStatus.DEPLOYING,
            JobStatus.CANCEL_REQUESTED,
        )
        if category not in _UNCERTAIN_FAILURES:
            raise InvalidJobEvidence(
                "Recovery requires uncertain effects or lost lease authority."
            )
        failure = JobFailure(
            stage=self.status, category=category, summary=summary, recorded_at=now
        )
        return self._changed(now, status=JobStatus.RECOVERY_REQUIRED, failure=failure)

    def request_cancellation(self, *, now: datetime) -> Job:
        self._require_state(
            JobStatus.QUEUED,
            *_EXECUTION_STATES,
            JobStatus.AWAITING_REVIEW,
            JobStatus.FAILED,
        )
        if self.release_revision is not None:
            raise InvalidJobTransition("Post-merge cancellation cannot undo a release.")
        if self.status is JobStatus.QUEUED:
            return self._changed(now, status=JobStatus.CANCELLED, finished_at=_utc(now))
        return self._changed(now, status=JobStatus.CANCEL_REQUESTED)

    def confirm_cancellation(
        self, *, stopped_attempt_id: UUID | None = None, now: datetime
    ) -> Job:
        self._require_state(JobStatus.CANCEL_REQUESTED)
        if self.active_attempt_id != stopped_attempt_id:
            raise StaleAttempt("Cancellation requires the matching contained attempt.")
        return self._changed(
            now,
            status=JobStatus.CANCELLED,
            active_attempt_id=None,
            finished_at=_utc(now),
        )
