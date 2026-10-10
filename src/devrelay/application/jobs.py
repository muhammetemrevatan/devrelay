from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID, uuid4

from devrelay.application.contracts.submissions import (
    PreparedJobSubmission,
    RepositoryPolicy,
    SavedJobSubmission,
    StoredJobSubmission,
    SubmissionAuthorization,
)
from devrelay.application.ports.job_persistence import (
    JobSubmissionReader,
    JobSubmissionUnitOfWork,
)
from devrelay.application.ports.repository_source import RepositorySubmissionSource
from devrelay.application.ports.task_persistence import TaskSnapshotReader
from devrelay.application.ports.task_source import TaskReadinessSource
from devrelay.application.tasks import (
    RequirementGap,
    TaskImportRejected,
    missing_requirements,
)
from devrelay.domain.jobs.job import Job
from devrelay.domain.jobs.values import PullRequestReference


class JobSubmissionRejected(Exception):
    """A selected task or repository cannot be submitted under current policy."""


class JobSubmissionConflict(JobSubmissionRejected):
    """A replay conflicts with pinned policy or an unresolved delivery/PR."""


class TaskRequirementsChanged(JobSubmissionRejected):
    """Re-import edited requirements rather than silently changing the pinned task."""


class TaskNotReady(JobSubmissionRejected):
    def __init__(self, gaps: tuple[RequirementGap, ...]) -> None:
        super().__init__("Task requirements are not ready for submission.")
        self.gaps = gaps


class PrepareJobSubmission:
    def __init__(
        self,
        snapshots: TaskSnapshotReader,
        source: TaskReadinessSource,
        policies: tuple[RepositoryPolicy, ...],
    ) -> None:
        if len(policies) > 32 or len({p.repository_id for p in policies}) != len(
            policies
        ):
            raise ValueError(
                "Repository policies must be bounded and uniquely identified."
            )
        if len({p.slug.casefold() for p in policies}) != len(policies):
            raise ValueError("A repository cannot have multiple policy identities.")
        self._snapshots = snapshots
        self._source = source
        self._policies = {policy.repository_id: policy for policy in policies}

    async def execute(
        self,
        *,
        snapshot_id: UUID,
        repository_id: UUID,
        authorization: SubmissionAuthorization,
    ) -> PreparedJobSubmission:
        intent = await self.resolve_intent(
            snapshot_id=snapshot_id,
            repository_id=repository_id,
            authorization=authorization,
        )
        await self.check_readiness(intent)
        return intent

    async def resolve_intent(
        self,
        *,
        snapshot_id: UUID,
        repository_id: UUID,
        authorization: SubmissionAuthorization,
    ) -> PreparedJobSubmission:
        # Fail before any DB/provider read for an unauthorized repository.
        policy = self._policies.get(repository_id)
        if (
            repository_id not in authorization.allowed_repository_ids
            or policy is None
            or not policy.enabled
        ):
            raise JobSubmissionRejected("Repository is not authorized for submission.")
        stored = await self._snapshots.get_by_id(snapshot_id)
        if stored.snapshot.id != snapshot_id:
            raise JobSubmissionRejected("Stored snapshot identity does not match.")
        identity = stored.snapshot.identity
        if (
            identity.provider != policy.task_scope.provider
            or identity.site_id != policy.task_scope.site_id
        ):
            raise JobSubmissionRejected(
                "Stored task is outside repository source scope."
            )
        return PreparedJobSubmission(
            stored=stored, policy=policy, actor_id=authorization.actor_id
        )

    async def check_readiness(self, intent: PreparedJobSubmission) -> None:
        stored, policy = intent.stored, intent.policy
        identity = stored.snapshot.identity
        observed = await self._source.read_by_identity(identity)
        if observed.snapshot.identity != identity:
            raise JobSubmissionRejected("Task source returned a different identity.")
        try:
            policy.task_scope.check_scope(observed)
        except TaskImportRejected:
            raise JobSubmissionRejected(
                "Task is outside repository source scope."
            ) from None
        if (
            observed.snapshot.fingerprint_version != stored.snapshot.fingerprint_version
            or observed.snapshot.content_fingerprint
            != stored.snapshot.content_fingerprint
        ):
            raise TaskRequirementsChanged(
                "Task requirements changed; import them again."
            )
        gaps = missing_requirements(observed, policy.task_scope.readiness_label)
        if gaps:
            raise TaskNotReady(gaps)


@dataclass(frozen=True, slots=True, kw_only=True)
class RecordedJobSubmission:
    job: Job
    submission_key: str
    submission_fingerprint: str


@dataclass(frozen=True, slots=True, kw_only=True)
class UnresolvedTaskPullRequest:
    source_task_id: UUID
    reference: PullRequestReference


@dataclass(frozen=True, slots=True, kw_only=True)
class SubmissionState:
    """Trusted scoped lookup projection; absence must be checked atomically later."""

    existing: RecordedJobSubmission | None = None
    blocking_job: Job | None = None
    unresolved_pull_request: UnresolvedTaskPullRequest | None = None


class AdmissionAction(StrEnum):
    NEW = "new"
    EXISTING = "existing"


@dataclass(frozen=True, slots=True, kw_only=True)
class SubmissionDecision:
    action: AdmissionAction
    existing_job: Job | None = None


def decide_submission(
    prepared: PreparedJobSubmission, state: SubmissionState
) -> SubmissionDecision:
    """Evaluate rules only; a Python precheck is not concurrent replay protection.

    The future transaction must repeat these checks under task/repository locks
    and unique constraints. Remote PR reconciliation is also required before
    execution; an empty local projection does not prove remote absence.
    """
    source_id = prepared.stored.source_task_id
    repository_id = prepared.policy.repository_id
    for job in (state.existing.job if state.existing else None, state.blocking_job):
        if job is not None and (
            job.source_task_id != source_id or job.repository_id != repository_id
        ):
            raise JobSubmissionConflict("Submission lookup returned a different scope.")
    pr = state.unresolved_pull_request
    if pr is not None and (
        pr.source_task_id != source_id or pr.reference.repository_id != repository_id
    ):
        raise JobSubmissionConflict("Pull request lookup returned a different scope.")
    if state.existing is not None:
        existing = state.existing
        if (
            existing.submission_key != prepared.submission_key
            or existing.submission_fingerprint != prepared.submission_fingerprint
            or existing.job.snapshot_id != prepared.stored.snapshot.id
            or existing.job.target_branch != prepared.policy.target_branch
            or existing.job.authorized_branch
            != f"{prepared.policy.coding_branch_prefix}/{existing.job.id.hex}"
            or existing.job.policy_version != prepared.policy.version
            or existing.job.required_check_ids
            != tuple(check.id for check in prepared.policy.checks)
            or existing.job.max_repair_cycles != prepared.policy.max_repair_cycles
        ):
            raise JobSubmissionConflict(
                "Submission replay conflicts with pinned intent."
            )
        return SubmissionDecision(
            action=AdmissionAction.EXISTING,
            existing_job=existing.job,
        )
    if state.blocking_job is not None and state.blocking_job.blocks_new_submission:
        raise JobSubmissionConflict("Task already has an unresolved delivery.")
    if pr is not None:
        raise JobSubmissionConflict("Task already has an unresolved pull request.")
    return SubmissionDecision(action=AdmissionAction.NEW)


def recorded_submission(stored: StoredJobSubmission) -> RecordedJobSubmission:
    return RecordedJobSubmission(
        job=stored.job,
        submission_key=stored.intent.submission_key,
        submission_fingerprint=stored.intent.submission_fingerprint,
    )


class SubmitJob:
    """Persist one default delivery, with provider reads outside write transactions."""

    def __init__(
        self,
        preparer: PrepareJobSubmission,
        reader: JobSubmissionReader,
        unit_of_work: Callable[[], JobSubmissionUnitOfWork],
        repository_source: RepositorySubmissionSource,
        *,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        new_id: Callable[[], UUID] = uuid4,
    ) -> None:
        self._preparer = preparer
        self._reader = reader
        self._unit_of_work = unit_of_work
        self._repository_source = repository_source
        self._now = now
        self._new_id = new_id

    async def execute(
        self,
        *,
        snapshot_id: UUID,
        repository_id: UUID,
        authorization: SubmissionAuthorization,
    ) -> SavedJobSubmission:
        intent = await self._preparer.resolve_intent(
            snapshot_id=snapshot_id,
            repository_id=repository_id,
            authorization=authorization,
        )
        existing = await self._reader.find_by_key(intent.submission_key)
        if existing is not None:
            decide_submission(
                intent, SubmissionState(existing=recorded_submission(existing))
            )
            return SavedJobSubmission(stored=existing, created=False)
        await self._preparer.check_readiness(intent)
        observation = await self._repository_source.observe(
            repository_id=repository_id,
            target_branch=intent.policy.target_branch,
            source_task_id=intent.stored.source_task_id,
        )
        if observation.unresolved_pull_request is not None:
            decide_submission(
                intent,
                SubmissionState(
                    unresolved_pull_request=UnresolvedTaskPullRequest(
                        source_task_id=intent.stored.source_task_id,
                        reference=observation.unresolved_pull_request,
                    )
                ),
            )
        candidate = intent.build_candidate(
            job_id=self._new_id(),
            head=observation.head,
            now=self._now(),
        )
        async with self._unit_of_work() as unit:
            saved = await unit.jobs.admit(intent, candidate)
            await unit.commit()
        return saved
