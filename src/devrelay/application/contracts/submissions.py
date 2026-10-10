import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID

from devrelay.application.contracts.tasks import StoredTaskSnapshot
from devrelay.application.tasks import TaskImportPolicy
from devrelay.domain.jobs.job import Job
from devrelay.domain.jobs.values import PullRequestReference


@dataclass(frozen=True, slots=True, kw_only=True)
class CheckPolicy:
    """Server-configured argv; task text never supplies executable commands."""

    id: str
    argv: tuple[str, ...]
    working_directory: str
    timeout_seconds: int

    def __post_init__(self) -> None:
        if not self.id.strip() or len(self.id) > 128:
            raise ValueError("Check policy requires a bounded identifier.")
        if not isinstance(self.argv, tuple) or not 1 <= len(self.argv) <= 64:
            raise ValueError("Check policy requires a bounded immutable argv.")
        if any(not item or len(item) > 4096 or "\x00" in item for item in self.argv):
            raise ValueError("Check policy contains an invalid argument.")
        if not self.working_directory or len(self.working_directory) > 256:
            raise ValueError("Check policy requires a bounded working directory.")
        if (
            type(self.timeout_seconds) is not int
            or not 1 <= self.timeout_seconds <= 3600
        ):
            raise ValueError("Check policy requires a bounded timeout.")


@dataclass(frozen=True, slots=True, kw_only=True)
class RepositoryPolicy:
    """Bootstrap-owned allowlist. These values contain no service credentials."""

    repository_id: UUID
    slug: str
    enabled: bool
    target_branch: str
    coding_branch_prefix: str
    version: str
    task_scope: TaskImportPolicy
    checks: tuple[CheckPolicy, ...]
    max_repair_cycles: int

    def __post_init__(self) -> None:
        if not isinstance(self.repository_id, UUID) or type(self.enabled) is not bool:
            raise ValueError("Repository policy requires typed identity and admission.")
        for value, limit in (
            (self.slug, 256),
            (self.target_branch, 256),
            (self.coding_branch_prefix, 180),
            (self.version, 64),
        ):
            if not value.strip() or value != value.strip() or len(value) > limit:
                raise ValueError("Repository policy requires bounded explicit values.")
        if not isinstance(self.checks, tuple) or not 1 <= len(self.checks) <= 32:
            raise ValueError("Repository policy requires bounded immutable checks.")
        if len({check.id for check in self.checks}) != len(self.checks):
            raise ValueError("Repository check identifiers must be unique.")
        if (
            type(self.max_repair_cycles) is not int
            or not 0 <= self.max_repair_cycles <= 5
        ):
            raise ValueError("Repository repair budget must be explicit and bounded.")


@dataclass(frozen=True, slots=True, kw_only=True)
class SubmissionAuthorization:
    """Trusted caller scope, constructed after separate submission authentication.

    This value does not authenticate itself. Import credentials and Jira labels
    must never be mapped to this scope. The actor is for later durable audit.
    """

    actor_id: str
    allowed_repository_ids: frozenset[UUID]

    def __post_init__(self) -> None:
        if not self.actor_id.strip() or len(self.actor_id) > 128:
            raise ValueError("Submission authority requires a bounded actor.")
        if (
            not isinstance(self.allowed_repository_ids, frozenset)
            or not 1 <= len(self.allowed_repository_ids) <= 32
            or any(not isinstance(value, UUID) for value in self.allowed_repository_ids)
        ):
            raise ValueError("Submission authority requires explicit repository scope.")


@dataclass(frozen=True, slots=True, kw_only=True)
class RepositoryHead:
    """Trusted Git observation, not a client-provided base commit."""

    repository_id: UUID
    target_branch: str
    revision: str


@dataclass(frozen=True, slots=True, kw_only=True)
class JobBranchAssociation:
    """Materialized storage evidence for associating a remote PR with a task."""

    job_id: UUID
    repository_id: UUID
    source_task_id: UUID
    authorized_branch: str


@dataclass(frozen=True, slots=True, kw_only=True)
class RepositorySubmissionObservation:
    """Trusted scoped Git head and associated unresolved PR observation."""

    head: RepositoryHead
    unresolved_pull_request: PullRequestReference | None = None


def _digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True, slots=True, kw_only=True)
class PreparedJobSubmission:
    """Canonical intent; freshness is checked separately for new admission only."""

    stored: StoredTaskSnapshot
    policy: RepositoryPolicy
    actor_id: str
    submission_key: str = field(init=False)
    submission_fingerprint: str = field(init=False)

    def __post_init__(self) -> None:
        snapshot = self.stored.snapshot
        identity = snapshot.identity
        key = {
            "version": 1,
            "provider": identity.provider,
            "site_id": identity.site_id,
            "external_id": identity.external_id,
            "fingerprint_version": snapshot.fingerprint_version,
            "content_fingerprint": snapshot.content_fingerprint,
            "repository_id": str(self.policy.repository_id),
            "generation": 1,
        }
        object.__setattr__(self, "submission_key", _digest(key))
        # Actor, capture metadata, candidate ID and moving Git head are not intent.
        # Retried requests resolve the original pinned Job; policy edits conflict.
        object.__setattr__(
            self,
            "submission_fingerprint",
            _digest(
                {
                    "key": key,
                    "slug": self.policy.slug,
                    "target_branch": self.policy.target_branch,
                    "coding_branch_prefix": self.policy.coding_branch_prefix,
                    "policy_version": self.policy.version,
                    "provider": self.policy.task_scope.provider,
                    "site_id": self.policy.task_scope.site_id,
                    "allowed_scope_keys": sorted(
                        self.policy.task_scope.allowed_scope_keys
                    ),
                    "readiness_label": self.policy.task_scope.readiness_label,
                    "max_repair_cycles": self.policy.max_repair_cycles,
                    "checks": [
                        {
                            "id": check.id,
                            "argv": check.argv,
                            "working_directory": check.working_directory,
                            "timeout_seconds": check.timeout_seconds,
                        }
                        for check in self.policy.checks
                    ],
                }
            ),
        )

    def build_candidate(
        self, *, job_id: UUID, head: RepositoryHead, now: datetime
    ) -> Job:
        """Build an unpersisted candidate after trusted head observation.

        The future transaction must persist this intent's complete check policy,
        audit actor and fingerprints alongside the Job, not only check IDs.
        """
        if (
            head.repository_id != self.policy.repository_id
            or head.target_branch != self.policy.target_branch
        ):
            raise ValueError("Repository head does not match submission policy.")
        return Job(
            id=job_id,
            source_task_id=self.stored.source_task_id,
            snapshot_id=self.stored.snapshot.id,
            repository_id=self.policy.repository_id,
            authorized_branch=f"{self.policy.coding_branch_prefix}/{job_id.hex}",
            target_branch=self.policy.target_branch,
            base_revision=head.revision,
            policy_version=self.policy.version,
            required_check_ids=tuple(check.id for check in self.policy.checks),
            max_repair_cycles=self.policy.max_repair_cycles,
            created_at=now,
            updated_at=now,
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class StoredJobSubmission:
    """Materialized durable definition and the originally accepted operator."""

    intent: PreparedJobSubmission
    job: Job


@dataclass(frozen=True, slots=True, kw_only=True)
class SavedJobSubmission:
    stored: StoredJobSubmission
    created: bool
