import asyncio
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from devrelay.application.claim_eligibility import PrepareJobClaim
from devrelay.application.contracts.eligibility import (
    ClaimEligibilityRejected,
    PreparedJobClaim,
    WorkerAuthorityExpired,
)
from devrelay.application.contracts.submissions import (
    CheckPolicy,
    PreparedJobSubmission,
    RepositoryHead,
    RepositoryPolicy,
    RepositorySubmissionObservation,
    StoredJobSubmission,
)
from devrelay.application.contracts.tasks import StoredTaskSnapshot, TaskRead
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.contracts.workflows import JobWorkflowState
from devrelay.application.ports.job_persistence import JobPersistenceError
from devrelay.application.ports.repository_source import RepositorySourceUnavailable
from devrelay.application.ports.task_source import TaskSourceUnavailable
from devrelay.application.tasks import TaskImportPolicy
from devrelay.domain.jobs.values import JobStatus, PullRequestReference
from devrelay.domain.tasks import TaskIdentity, TaskSnapshot

NOW = datetime(2026, 10, 11, tzinfo=UTC)


@dataclass
class Clock:
    instant: datetime = NOW

    def __call__(self) -> datetime:
        return self.instant


class Queue:
    def __init__(self, stored: StoredJobSubmission, clock: Clock) -> None:
        self.stored: StoredJobSubmission | None = stored
        self.clock = clock
        self.scopes: list[frozenset[UUID]] = []
        self.delay = timedelta(0)
        self.error: Exception | None = None

    async def find_candidate(
        self, *, repository_ids: frozenset[UUID]
    ) -> StoredJobSubmission | None:
        self.scopes.append(repository_ids)
        self.clock.instant += self.delay
        if self.error is not None:
            raise self.error
        return self.stored


class Source:
    def __init__(self, snapshot: TaskSnapshot, clock: Clock) -> None:
        self.observation = TaskRead(
            snapshot=snapshot, scope_key="DRY", labels=frozenset({"agent-ready"})
        )
        self.clock = clock
        self.identities: list[TaskIdentity] = []
        self.delay = timedelta(0)
        self.error: Exception | None = None

    async def read_by_identity(self, identity: TaskIdentity) -> TaskRead:
        self.identities.append(identity)
        self.clock.instant += self.delay
        if self.error is not None:
            raise self.error
        return self.observation


class Git:
    def __init__(self, head: RepositoryHead, clock: Clock) -> None:
        self.observation = RepositorySubmissionObservation(head=head)
        self.clock = clock
        self.calls: list[tuple[UUID, str, UUID]] = []
        self.delay = timedelta(0)
        self.error: Exception | None = None

    async def observe(
        self, *, repository_id: UUID, target_branch: str, source_task_id: UUID
    ) -> RepositorySubmissionObservation:
        self.calls.append((repository_id, target_branch, source_task_id))
        self.clock.instant += self.delay
        if self.error is not None:
            raise self.error
        return self.observation


@dataclass
class Scenario:
    stored: StoredJobSubmission
    authorization: WorkerAuthorization
    clock: Clock
    queue: Queue
    source: Source
    git: Git

    def service(self, policy: RepositoryPolicy | None = None) -> PrepareJobClaim:
        return PrepareJobClaim(
            self.queue,
            self.source,
            self.git,
            (policy or self.stored.intent.policy,),
            now=self.clock,
        )


@pytest.fixture
def scenario() -> Scenario:
    snapshot = TaskSnapshot(
        id=uuid4(),
        identity=TaskIdentity(
            provider="jira", site_id="test-site", external_id="10005"
        ),
        display_key="DRY-5",
        title="Implement a feature",
        description="Keep requirements stable.",
        acceptance_criteria=("Pass configured checks.",),
        captured_at=NOW,
    )
    policy = RepositoryPolicy(
        repository_id=uuid4(),
        provider_repository_id=123,
        slug="example/devrelay",
        enabled=True,
        target_branch="master",
        coding_branch_prefix="devrelay/jobs",
        version="v1",
        task_scope=TaskImportPolicy(
            provider="jira", site_id="test-site", allowed_scope_keys=frozenset({"DRY"})
        ),
        checks=(
            CheckPolicy(
                id="pytest",
                argv=("pytest",),
                working_directory=".",
                timeout_seconds=300,
            ),
        ),
        max_repair_cycles=2,
    )
    intent = PreparedJobSubmission(
        stored=StoredTaskSnapshot(source_task_id=uuid4(), snapshot=snapshot),
        policy=policy,
        actor_id="test-operator",
    )
    head = RepositoryHead(
        repository_id=policy.repository_id,
        provider_repository_id=123,
        target_branch="master",
        revision="a" * 40,
    )
    job = intent.build_candidate(job_id=uuid4(), head=head, now=NOW)
    stored = StoredJobSubmission(
        intent=intent, job=job, workflow=JobWorkflowState.initial(job)
    )
    clock = Clock()
    return Scenario(
        stored=stored,
        authorization=WorkerAuthorization(
            worker_id=uuid4(),
            allowed_repository_ids=frozenset({policy.repository_id}),
            credential_expires_at=NOW + timedelta(minutes=2),
        ),
        clock=clock,
        queue=Queue(stored, clock),
        source=Source(snapshot, clock),
        git=Git(head, clock),
    )


def test_preparation_preserves_pins_and_uses_stable_task_identity(
    scenario: Scenario,
) -> None:
    scenario.source.observation = replace(
        scenario.source.observation,
        snapshot=replace(
            scenario.stored.intent.stored.snapshot, display_key="DRY-99", id=uuid4()
        ),
    )
    service = scenario.service()

    result = asyncio.run(service.execute(scenario.authorization))

    assert result is not None
    assert result.stored is scenario.stored
    assert result.worker_id == scenario.authorization.worker_id
    assert result.observed_at == NOW
    assert scenario.queue.scopes == [scenario.authorization.allowed_repository_ids]
    assert scenario.source.identities == [
        scenario.stored.intent.stored.snapshot.identity
    ]
    assert scenario.git.calls == [
        (
            scenario.stored.job.repository_id,
            "master",
            scenario.stored.job.source_task_id,
        )
    ]
    assert result.stored.workflow.status is JobStatus.QUEUED


def test_empty_queue_does_not_call_providers(scenario: Scenario) -> None:
    scenario.queue.stored = None
    service = scenario.service()

    result = asyncio.run(service.execute(scenario.authorization))

    assert result is None
    assert scenario.source.identities == []
    assert scenario.git.calls == []


@pytest.mark.parametrize("change", ["legacy", "assigned", "version", "foreign_scope"])
def test_invalid_candidate_never_reaches_provider_io(
    scenario: Scenario, change: str
) -> None:
    stored = scenario.stored
    if change == "legacy":
        stored = replace(
            stored,
            intent=replace(
                stored.intent,
                policy=replace(stored.intent.policy, provider_repository_id=None),
            ),
        )
    elif change == "assigned":
        stored = replace(
            stored,
            workflow=replace(stored.workflow, status=JobStatus.CLAIMED, version=1),
        )
    elif change == "version":
        stored = replace(stored, workflow=replace(stored.workflow, version=1))
    else:
        stored = replace(stored, job=replace(stored.job, repository_id=uuid4()))
    scenario.queue.stored = stored
    service = scenario.service()

    with pytest.raises(ClaimEligibilityRejected):
        asyncio.run(service.execute(scenario.authorization))

    assert scenario.source.identities == []
    assert scenario.git.calls == []


@pytest.mark.parametrize("stage", ["before", "queue", "source", "git"])
def test_credential_expiry_stops_preparation_at_every_io_boundary(
    scenario: Scenario, stage: str
) -> None:
    if stage == "before":
        scenario.clock.instant = scenario.authorization.credential_expires_at
    else:
        getattr(scenario, stage).delay = timedelta(minutes=2)
    service = scenario.service()

    with pytest.raises(WorkerAuthorityExpired):
        asyncio.run(service.execute(scenario.authorization))

    if stage in {"before", "queue"}:
        assert scenario.source.identities == []
    if stage != "git":
        assert scenario.git.calls == []


@pytest.mark.parametrize("stage", ["source", "git"])
def test_slow_reads_do_not_refresh_original_observation_time(
    scenario: Scenario, stage: str
) -> None:
    getattr(scenario, stage).delay = timedelta(seconds=30)
    service = scenario.service()

    with pytest.raises(ClaimEligibilityRejected, match="stale"):
        asyncio.run(service.execute(scenario.authorization))

    if stage == "source":
        assert scenario.git.calls == []


@pytest.mark.parametrize(
    "change", ["disabled", "scope", "policy", "provider_id", "checks"]
)
def test_configuration_changes_reject_before_provider_io(
    scenario: Scenario, change: str
) -> None:
    policy = scenario.stored.intent.policy
    if change == "disabled":
        policy = replace(policy, enabled=False)
    elif change == "scope":
        scenario.authorization = replace(
            scenario.authorization, allowed_repository_ids=frozenset({uuid4()})
        )
    elif change == "policy":
        policy = replace(policy, version="v2")
    elif change == "provider_id":
        policy = replace(policy, provider_repository_id=456)
    else:
        policy = replace(policy, checks=(replace(policy.checks[0], argv=("other",)),))
    service = scenario.service(policy)

    with pytest.raises(ClaimEligibilityRejected):
        asyncio.run(service.execute(scenario.authorization))

    assert scenario.source.identities == []
    assert scenario.git.calls == []


@pytest.mark.parametrize(
    "change", ["identity", "content", "label", "project", "description", "criteria"]
)
def test_changed_jira_requirements_stop_before_github(
    scenario: Scenario, change: str
) -> None:
    observed = scenario.source.observation
    snapshot = observed.snapshot
    if change == "identity":
        observed = replace(
            observed,
            snapshot=replace(
                snapshot, identity=replace(snapshot.identity, external_id="10006")
            ),
        )
    elif change == "content":
        observed = replace(observed, snapshot=replace(snapshot, title="Changed task"))
    elif change == "label":
        observed = replace(observed, labels=frozenset())
    elif change == "project":
        observed = replace(observed, scope_key="OTHER")
    elif change == "description":
        observed = replace(observed, snapshot=replace(snapshot, description=""))
    else:
        observed = replace(observed, snapshot=replace(snapshot, acceptance_criteria=()))
    scenario.source.observation = observed
    service = scenario.service()

    with pytest.raises(ClaimEligibilityRejected):
        asyncio.run(service.execute(scenario.authorization))

    assert scenario.git.calls == []


@pytest.mark.parametrize(
    "change", ["repository", "provider_id", "branch", "revision", "pull_request"]
)
def test_changed_repository_observation_cannot_authorize_work(
    scenario: Scenario, change: str
) -> None:
    observed = scenario.git.observation
    if change == "pull_request":
        observed = replace(
            observed,
            unresolved_pull_request=PullRequestReference(
                repository_id=scenario.stored.job.repository_id,
                number=1,
                source_branch=scenario.stored.job.authorized_branch,
                target_branch=scenario.stored.job.target_branch,
                head_revision="b" * 40,
            ),
        )
    else:
        head = observed.head
        heads = {
            "repository": replace(head, repository_id=uuid4()),
            "provider_id": replace(head, provider_repository_id=456),
            "branch": replace(head, target_branch="other"),
            "revision": replace(head, revision="b" * 40),
        }
        observed = replace(observed, head=heads[change])
    scenario.git.observation = observed
    service = scenario.service()

    with pytest.raises(ClaimEligibilityRejected, match="Repository state"):
        asyncio.run(service.execute(scenario.authorization))


@pytest.mark.parametrize("stage", ["queue", "source", "git"])
def test_classified_failures_remain_failures_not_no_work(
    scenario: Scenario, stage: str
) -> None:
    error = {
        "queue": JobPersistenceError("Storage unavailable."),
        "source": TaskSourceUnavailable("Source unavailable."),
        "git": RepositorySourceUnavailable("Repository unavailable."),
    }[stage]
    getattr(scenario, stage).error = error
    service = scenario.service()

    with pytest.raises(type(error)) as caught:
        asyncio.run(service.execute(scenario.authorization))

    assert caught.value is error


@pytest.mark.parametrize("seconds", [-1, 30, 31])
def test_observation_rejects_clock_regression_and_exact_deadline(
    scenario: Scenario, seconds: int
) -> None:
    result = PreparedJobClaim(
        worker_id=scenario.authorization.worker_id,
        stored=scenario.stored,
        observed_at=NOW,
    )

    with pytest.raises(ClaimEligibilityRejected):
        result.require_fresh(now=NOW + timedelta(seconds=seconds))


def test_observation_accepts_time_before_deadline(scenario: Scenario) -> None:
    result = PreparedJobClaim(
        worker_id=scenario.authorization.worker_id,
        stored=scenario.stored,
        observed_at=NOW,
    )

    result.require_fresh(now=NOW + timedelta(seconds=29, microseconds=999999))

    assert result.observed_at == NOW


def test_cancellation_propagates_without_github_io(scenario: Scenario) -> None:
    async def cancelled(identity: TaskIdentity) -> TaskRead:
        raise asyncio.CancelledError

    scenario.source.read_by_identity = cancelled  # type: ignore[method-assign] # Inject cancellation at the port boundary.
    service = scenario.service()

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(service.execute(scenario.authorization))

    assert scenario.git.calls == []
