from collections.abc import Callable
from datetime import UTC, datetime

from devrelay.application.contracts.eligibility import (
    ClaimEligibilityRejected,
    PreparedJobClaim,
    WorkerAuthorityExpired,
)
from devrelay.application.contracts.submissions import (
    PreparedJobSubmission,
    RepositoryHead,
    RepositoryPolicy,
    StoredJobSubmission,
    repository_policy_map,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.claim_eligibility import QueuedJobReader
from devrelay.application.ports.repository_source import RepositorySubmissionSource
from devrelay.application.ports.task_source import TaskReadinessSource
from devrelay.application.tasks import TaskImportRejected, missing_requirements
from devrelay.domain.jobs.values import _utc


class PrepareJobClaim:
    """Observe one authorized candidate without holding a DB session over I/O."""

    def __init__(
        self,
        reader: QueuedJobReader,
        source: TaskReadinessSource,
        repository_source: RepositorySubmissionSource,
        policies: tuple[RepositoryPolicy, ...],
        *,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._policies = repository_policy_map(policies)
        self._reader = reader
        self._source = source
        self._repository_source = repository_source
        self._now = now

    def _require_authority(self, authorization: WorkerAuthorization) -> datetime:
        instant = _utc(self._now())
        if instant >= authorization.credential_expires_at:
            raise WorkerAuthorityExpired("Worker authority has expired.")
        return instant

    async def execute(
        self, authorization: WorkerAuthorization
    ) -> PreparedJobClaim | None:
        self._require_authority(authorization)
        scope = frozenset(
            identity
            for identity, policy in self._policies.items()
            if policy.enabled and identity in authorization.allowed_repository_ids
        )
        if not scope:
            raise ClaimEligibilityRejected("Worker has no enabled repository scope.")
        stored = await self._reader.find_candidate(repository_ids=scope)
        observed_at = self._require_authority(authorization)
        if stored is None:
            return None
        repository_id = stored.job.repository_id
        if repository_id not in scope:
            raise ClaimEligibilityRejected("Candidate is outside worker scope.")
        policy = self._policies[repository_id]
        if policy.provider_repository_id is None:
            raise ClaimEligibilityRejected(
                "Claim requires a pinned repository identity."
            )
        current_intent = PreparedJobSubmission(
            stored=stored.intent.stored, policy=policy, actor_id=stored.intent.actor_id
        )
        if (
            current_intent.submission_key != stored.intent.submission_key
            or current_intent.submission_fingerprint
            != stored.intent.submission_fingerprint
        ):
            raise ClaimEligibilityRejected(
                "Repository policy changed after submission."
            )
        expected = current_intent.build_candidate(
            job_id=stored.job.id,
            head=RepositoryHead(
                repository_id=repository_id,
                provider_repository_id=policy.provider_repository_id,
                target_branch=policy.target_branch,
                revision=stored.job.base_revision,
            ),
            now=stored.job.created_at,
        )
        if expected != stored.job:
            raise ClaimEligibilityRejected("Candidate definition is inconsistent.")
        result = PreparedJobClaim(
            worker_id=authorization.worker_id, stored=stored, observed_at=observed_at
        )
        identity = stored.intent.stored.snapshot.identity
        task = await self._source.read_by_identity(identity)
        try:
            policy.task_scope.check_scope(task)
        except TaskImportRejected:
            raise ClaimEligibilityRejected(
                "Task is outside repository scope."
            ) from None
        snapshot = stored.intent.stored.snapshot
        if (
            task.snapshot.identity != identity
            or task.snapshot.fingerprint_version != snapshot.fingerprint_version
            or task.snapshot.content_fingerprint != snapshot.content_fingerprint
            or missing_requirements(task, policy.task_scope.readiness_label)
        ):
            raise ClaimEligibilityRejected("Task requirements are no longer eligible.")
        result.require_fresh(now=self._require_authority(authorization))
        observation = await self._repository_source.observe(
            repository_id=repository_id,
            target_branch=policy.target_branch,
            source_task_id=stored.job.source_task_id,
        )
        if (
            observation.head != expected_head(stored)
            or observation.unresolved_pull_request is not None
        ):
            raise ClaimEligibilityRejected("Repository state is no longer eligible.")
        result.require_fresh(now=self._require_authority(authorization))
        return result


def expected_head(stored: StoredJobSubmission) -> RepositoryHead:
    provider_id = stored.intent.policy.provider_repository_id
    if provider_id is None:
        raise ClaimEligibilityRejected("Claim requires a pinned repository identity.")
    return RepositoryHead(
        repository_id=stored.job.repository_id,
        provider_repository_id=provider_id,
        target_branch=stored.job.target_branch,
        revision=stored.job.base_revision,
    )
