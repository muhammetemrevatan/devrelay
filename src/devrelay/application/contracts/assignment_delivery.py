import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID

from devrelay.application.contracts.assignments import ClaimResult
from devrelay.application.contracts.submissions import CheckPolicy
from devrelay.application.contracts.worker_recovery import WorkerClaimOutcome
from devrelay.domain.jobs.values import _branch, _revision, _text, _utc, _uuid
from devrelay.domain.tasks import TaskSnapshot

MAX_ASSIGNMENT_BYTES = 2 * 1024 * 1024
MAX_CHECKS_BYTES = 512 * 1024


class AssignmentUnavailable(Exception):
    """No matching assigned receipt can be disclosed to this worker."""


class InvalidAssignmentDefinition(Exception):
    """Stored assignment cannot be delivered under the bounded initial protocol."""


def check_payload(checks: tuple[CheckPolicy, ...]) -> list[dict[str, object]]:
    return [
        {
            "id": check.id,
            "argv": check.argv,
            "working_directory": check.working_directory,
            "timeout_seconds": check.timeout_seconds,
        }
        for check in checks
    ]


@dataclass(frozen=True, slots=True, kw_only=True)
class AssignedJobDefinition:
    """Accepted execution inputs as data; no credentials or launch authority."""

    job_id: UUID
    source_task_id: UUID
    repository_id: UUID
    provider_repository_id: int
    repository_slug: str
    snapshot: TaskSnapshot = field(repr=False)
    authorized_branch: str
    target_branch: str
    base_revision: str
    policy_version: str
    checks: tuple[CheckPolicy, ...] = field(repr=False)
    max_repair_cycles: int
    created_at: datetime

    def __post_init__(self) -> None:
        for value in (self.job_id, self.source_task_id, self.repository_id):
            _uuid(value)
        if (
            type(self.provider_repository_id) is not int
            or not 1 <= self.provider_repository_id < 2**63
            or not isinstance(self.snapshot, TaskSnapshot)
            or not isinstance(self.checks, tuple)
            or not 1 <= len(self.checks) <= 32
            or any(not isinstance(check, CheckPolicy) for check in self.checks)
            or len({check.id for check in self.checks}) != len(self.checks)
            or type(self.max_repair_cycles) is not int
            or not 0 <= self.max_repair_cycles <= 5
        ):
            raise ValueError("Assignment requires bounded pinned inputs.")
        _uuid(self.snapshot.id)
        if re.fullmatch(
            r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,38})/[A-Za-z0-9_][A-Za-z0-9_.-]{0,99}",
            self.repository_slug,
        ) is None or self.repository_slug.endswith(".git"):
            raise ValueError("Assignment requires a literal repository slug.")
        _branch(self.authorized_branch)
        _branch(self.target_branch)
        if self.authorized_branch == self.target_branch:
            raise ValueError("Assignment branches must be distinct.")
        _revision(self.base_revision)
        _text(self.policy_version, "Policy version", 64)
        object.__setattr__(self, "created_at", _utc(self.created_at))
        for check in self.checks:
            if (
                re.fullmatch(r"[a-z][a-z0-9_-]*", check.id) is None
                or check.argv[0] != check.argv[0].strip()
                or not check.argv[0].strip()
                or any(
                    any(ord(char) < 32 or ord(char) == 127 for char in argument)
                    for argument in check.argv
                )
                or (
                    check.working_directory != "."
                    and re.fullmatch(
                        r"[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+)*", check.working_directory
                    )
                    is None
                )
            ):
                raise ValueError("Assignment checks require literal bounded policy.")

    def fingerprint(self) -> str:
        checks = check_payload(self.checks)
        if (
            len(
                json.dumps(checks, ensure_ascii=False, separators=(",", ":")).encode(
                    "utf-8"
                )
            )
            > MAX_CHECKS_BYTES
        ):
            raise InvalidAssignmentDefinition(
                "Assignment check policy exceeds delivery bounds."
            )
        value = {
            "version": 1,
            "job_id": str(self.job_id),
            "source_task_id": str(self.source_task_id),
            "repository_id": str(self.repository_id),
            "provider_repository_id": self.provider_repository_id,
            "repository_slug": self.repository_slug,
            "snapshot_id": str(self.snapshot.id),
            "source_identity": {
                "provider": self.snapshot.identity.provider,
                "site_id": self.snapshot.identity.site_id,
                "external_id": self.snapshot.identity.external_id,
            },
            "display_key": self.snapshot.display_key,
            "captured_at": self.snapshot.captured_at.isoformat(),
            "source_updated_at": self.snapshot.source_updated_at.isoformat()
            if self.snapshot.source_updated_at
            else None,
            "content_fingerprint": self.snapshot.content_fingerprint,
            "fingerprint_version": self.snapshot.fingerprint_version,
            "authorized_branch": self.authorized_branch,
            "target_branch": self.target_branch,
            "base_revision": self.base_revision,
            "policy_version": self.policy_version,
            "checks": checks,
            "max_repair_cycles": self.max_repair_cycles,
            "created_at": self.created_at.isoformat(),
        }
        return hashlib.sha256(
            json.dumps(
                value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
        ).hexdigest()


@dataclass(frozen=True, slots=True, kw_only=True)
class AssignmentDelivery:
    outcome: WorkerClaimOutcome
    definition: AssignedJobDefinition = field(repr=False)
    lease_authority_active: bool

    def __post_init__(self) -> None:
        reference = self.outcome.assignment
        if (
            reference is None
            or not isinstance(self.definition, AssignedJobDefinition)
            or type(self.lease_authority_active) is not bool
            or self.definition.job_id != reference.job_id
            or self.definition.repository_id != reference.repository_id
            or self.definition.created_at > reference.issued_at
        ):
            raise ValueError("Assignment delivery requires matching recovery anchors.")

    @classmethod
    def from_result(
        cls,
        result: ClaimResult,
        *,
        worker_id: UUID,
        claim_request_id: UUID,
    ) -> AssignmentDelivery:
        if result.stored is None or result.attempt is None:
            raise AssignmentUnavailable("Assigned receipt is unavailable.")
        stored, job = result.stored, result.stored.job
        policy = stored.intent.policy
        if (
            policy.provider_repository_id is None
            or job.repository_id != policy.repository_id
            or job.source_task_id != stored.intent.stored.source_task_id
            or job.snapshot_id != stored.intent.stored.snapshot.id
            or job.target_branch != policy.target_branch
            or job.policy_version != policy.version
            or job.required_check_ids != tuple(check.id for check in policy.checks)
            or job.max_repair_cycles != policy.max_repair_cycles
        ):
            raise InvalidAssignmentDefinition(
                "Stored assignment pins are inconsistent."
            )
        return cls(
            outcome=WorkerClaimOutcome.from_result(
                result, worker_id=worker_id, claim_request_id=claim_request_id
            ),
            definition=AssignedJobDefinition(
                job_id=job.id,
                source_task_id=job.source_task_id,
                repository_id=job.repository_id,
                provider_repository_id=policy.provider_repository_id,
                repository_slug=policy.slug,
                snapshot=stored.intent.stored.snapshot,
                authorized_branch=job.authorized_branch,
                target_branch=job.target_branch,
                base_revision=job.base_revision,
                policy_version=job.policy_version,
                checks=policy.checks,
                max_repair_cycles=job.max_repair_cycles,
                created_at=job.created_at,
            ),
            lease_authority_active=result.lease_authority_active,
        )
