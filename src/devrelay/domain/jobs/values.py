import re
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID


class JobStatus(StrEnum):
    QUEUED = "queued"
    CLAIMED = "claimed"
    RUNNING = "running"
    TESTING = "testing"
    PUBLISHING_PR = "publishing_pr"
    AWAITING_REVIEW = "awaiting_review"
    AWAITING_RELEASE_CI = "awaiting_release_ci"
    DEPLOYING = "deploying"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    RECOVERY_REQUIRED = "recovery_required"
    CANCEL_REQUESTED = "cancel_requested"
    CANCELLED = "cancelled"


class AttemptStatus(StrEnum):
    CLAIMED = "claimed"
    RUNNING = "running"
    TESTING = "testing"
    PUBLISHING_PR = "publishing_pr"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    LEASE_EXPIRED = "lease_expired"
    CANCELLED = "cancelled"


class FailureCategory(StrEnum):
    CONFIGURATION = "configuration"
    AUTHENTICATION = "authentication"
    VALIDATION = "validation"
    TEST_FAILURE = "test_failure"
    TIMEOUT = "timeout"
    TRANSIENT_DEPENDENCY = "transient_dependency"
    LEASE_LOST = "lease_lost"
    UNCERTAIN_SIDE_EFFECT = "uncertain_side_effect"
    INTERNAL = "internal"


class VerificationOutcome(StrEnum):
    PASSED = "passed"
    FAILED = "failed"


class InvalidJobTransition(ValueError):
    """The command cannot run in the current delivery state."""


class InvalidJobEvidence(ValueError):
    """Evidence is incomplete, failed or correlated to a different delivery."""


class StaleAttempt(InvalidJobEvidence):
    """The command does not name the currently assigned attempt."""


class RepairLimitExceeded(InvalidJobTransition):
    """The current attempt has exhausted its configured repair budget."""


def _text(value: str, name: str, limit: int = 256) -> None:
    if not value.strip() or value != value.strip() or len(value) > limit:
        raise InvalidJobEvidence(f"{name} requires bounded nonblank text.")


def _revision(value: str) -> None:
    if re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", value) is None:
        raise InvalidJobEvidence("A full lowercase commit revision is required.")


def _uuid(value: UUID) -> None:
    if not isinstance(value, UUID):
        raise InvalidJobEvidence("Lifecycle identity must be a UUID.")


def _utc(value: datetime) -> datetime:
    if value.utcoffset() is None:
        raise InvalidJobEvidence("Lifecycle timestamps must be timezone-aware.")
    return value.astimezone(UTC)


def _branch(value: str) -> None:
    _text(value, "Branch")
    if any(character.isspace() for character in value):
        raise InvalidJobEvidence("Branch must not contain whitespace.")


@dataclass(frozen=True, slots=True, kw_only=True)
class CheckResult:
    check_id: str
    revision: str
    outcome: VerificationOutcome
    exit_code: int

    def __post_init__(self) -> None:
        _text(self.check_id, "Check ID", 128)
        _revision(self.revision)
        if (
            not isinstance(self.outcome, VerificationOutcome)
            or type(self.exit_code) is not int
        ):
            raise InvalidJobEvidence("Check outcome and exit code must be typed.")
        if (self.outcome is VerificationOutcome.PASSED) != (self.exit_code == 0):
            raise InvalidJobEvidence("Check outcome must agree with its exit code.")


@dataclass(frozen=True, slots=True, kw_only=True)
class PullRequestReference:
    repository_id: UUID
    number: int
    source_branch: str
    target_branch: str
    head_revision: str

    def __post_init__(self) -> None:
        _uuid(self.repository_id)
        if type(self.number) is not int or self.number <= 0:
            raise InvalidJobEvidence("Pull request number must be positive.")
        _branch(self.source_branch)
        _branch(self.target_branch)
        _revision(self.head_revision)
        if self.source_branch == self.target_branch:
            raise InvalidJobEvidence("Pull request branches must be distinct.")


@dataclass(frozen=True, slots=True, kw_only=True)
class HumanMergeEvidence:
    """Construct only from trusted control observations, never worker claims."""

    repository_id: UUID
    pull_request_number: int
    reviewed_head_revision: str
    release_revision: str

    def __post_init__(self) -> None:
        _uuid(self.repository_id)
        if type(self.pull_request_number) is not int or self.pull_request_number <= 0:
            raise InvalidJobEvidence("Merge evidence requires a pull request number.")
        _revision(self.reviewed_head_revision)
        _revision(self.release_revision)


@dataclass(frozen=True, slots=True, kw_only=True)
class ReleaseCIEvidence:
    repository_id: UUID
    run_id: str
    revision: str
    outcome: VerificationOutcome

    def __post_init__(self) -> None:
        _uuid(self.repository_id)
        _text(self.run_id, "Release CI run ID")
        _revision(self.revision)
        if not isinstance(self.outcome, VerificationOutcome):
            raise InvalidJobEvidence("Release CI outcome must be typed.")


@dataclass(frozen=True, slots=True, kw_only=True)
class DeploymentReference:
    repository_id: UUID
    deployment_id: str
    revision: str
    artifact_id: str

    def __post_init__(self) -> None:
        _uuid(self.repository_id)
        _text(self.deployment_id, "Deployment ID")
        _text(self.artifact_id, "Release artifact ID")
        _revision(self.revision)


@dataclass(frozen=True, slots=True, kw_only=True)
class JobFailure:
    stage: JobStatus
    category: FailureCategory
    summary: str
    recorded_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.stage, JobStatus) or not isinstance(
            self.category, FailureCategory
        ):
            raise InvalidJobEvidence("Failure stage and category must be typed.")
        _text(self.summary, "Failure summary", 512)
        object.__setattr__(self, "recorded_at", _utc(self.recorded_at))
