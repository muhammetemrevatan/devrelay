import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import TracebackType
from typing import Self, cast
from uuid import UUID, uuid4

import pytest

from devrelay.application.contracts.submissions import (
    CheckPolicy,
    PreparedJobSubmission,
    RepositoryHead,
    RepositoryPolicy,
    RepositorySubmissionObservation,
    SavedJobSubmission,
    StoredJobSubmission,
    SubmissionAuthorization,
)
from devrelay.application.contracts.tasks import StoredTaskSnapshot, TaskRead
from devrelay.application.jobs import (
    AdmissionAction,
    JobSubmissionConflict,
    JobSubmissionRejected,
    PrepareJobSubmission,
    RecordedJobSubmission,
    SubmissionState,
    SubmitJob,
    TaskNotReady,
    TaskRequirementsChanged,
    UnresolvedTaskPullRequest,
    decide_submission,
)
from devrelay.application.ports.job_persistence import JobPersistenceError
from devrelay.application.ports.task_persistence import TaskSnapshotNotFound
from devrelay.application.ports.task_source import TaskSourceUnavailable
from devrelay.application.tasks import RequirementGap, TaskImportPolicy
from devrelay.domain.jobs.job import Job
from devrelay.domain.jobs.values import PullRequestReference
from devrelay.domain.tasks import TaskIdentity, TaskSnapshot

NOW = datetime(2026, 10, 9, tzinfo=UTC)


@pytest.mark.parametrize("provider_id", [True, False, 0, -1, 2**63, 1.5, "123"])
def test_repository_policy_rejects_invalid_numeric_identity(
    provider_id: object,
) -> None:
    policy = repository()

    with pytest.raises(ValueError):
        replace(policy, provider_repository_id=cast(int, provider_id))


@pytest.mark.parametrize("provider_id", [None, True, 0, -1, 2**63, "123"])
def test_repository_observation_requires_strict_numeric_identity(
    provider_id: object,
) -> None:
    with pytest.raises(ValueError):
        RepositoryHead(
            repository_id=uuid4(),
            provider_repository_id=cast(int, provider_id),
            target_branch="main",
            revision="a" * 40,
        )


def test_legacy_definition_cannot_build_a_new_candidate() -> None:
    prepared = prepare(stored_task(), repository())
    legacy = replace(
        prepared, policy=replace(prepared.policy, provider_repository_id=None)
    )
    head = RepositoryHead(
        repository_id=prepared.policy.repository_id,
        provider_repository_id=123,
        target_branch="main",
        revision="a" * 40,
    )

    with pytest.raises(ValueError):
        legacy.build_candidate(job_id=uuid4(), head=head, now=NOW)


def test_legacy_submission_hashes_match_version_one_golden_reference() -> None:
    stored = stored_task()
    policy = replace(
        repository(),
        repository_id=UUID("11111111-2222-3333-4444-555555555555"),
        provider_repository_id=None,
    )

    legacy = PreparedJobSubmission(
        stored=stored, policy=policy, actor_id="test-operator"
    )

    assert (
        legacy.submission_key
        == "7daaacc662d581422007946b43c53f6db926e41bff85e4eeafa40de656b003ce"
    )
    assert (
        legacy.submission_fingerprint
        == "897e6b398d6972f2c6b8853fb8b8cd5fec629f28383b698df1cc6baadef625bf"
    )


def stored_task() -> StoredTaskSnapshot:
    return StoredTaskSnapshot(
        source_task_id=uuid4(),
        snapshot=TaskSnapshot(
            id=uuid4(),
            identity=TaskIdentity(
                provider="jira",
                site_id="test-site",
                external_id="10005",
            ),
            display_key="DRY-5",
            title="Import a task",
            description="Preserve requirements.",
            acceptance_criteria=("Save once.",),
            captured_at=NOW,
        ),
    )


def repository() -> RepositoryPolicy:
    return RepositoryPolicy(
        provider_repository_id=123,
        repository_id=uuid4(),
        slug="example/devrelay",
        enabled=True,
        target_branch="main",
        coding_branch_prefix="devrelay/jobs",
        version="v1",
        task_scope=TaskImportPolicy(
            provider="jira",
            site_id="test-site",
            allowed_scope_keys=frozenset({"DRY"}),
        ),
        checks=(
            CheckPolicy(
                id="pytest",
                argv=("uv", "run", "--locked", "pytest"),
                working_directory=".",
                timeout_seconds=300,
            ),
        ),
        max_repair_cycles=2,
    )


class Reader:
    def __init__(self, stored: StoredTaskSnapshot, events: list[str]) -> None:
        self.stored = stored
        self.events = events

    async def get_by_id(self, snapshot_id: UUID) -> StoredTaskSnapshot:
        self.events.append("stored")
        if snapshot_id != self.stored.snapshot.id:
            raise TaskSnapshotNotFound("Stored task snapshot was not found.")
        return self.stored


class Source:
    def __init__(self, read: TaskRead, events: list[str]) -> None:
        self.read = read
        self.events = events

    async def read_by_identity(self, identity: TaskIdentity) -> TaskRead:
        self.events.append("fresh")
        return self.read


def authority(policy: RepositoryPolicy) -> SubmissionAuthorization:
    return SubmissionAuthorization(
        actor_id="test-operator",
        allowed_repository_ids=frozenset({policy.repository_id}),
    )


def prepare(
    stored: StoredTaskSnapshot,
    policy: RepositoryPolicy,
    observed: TaskRead | None = None,
    events: list[str] | None = None,
    authorization: SubmissionAuthorization | None = None,
    selected_repository: UUID | None = None,
) -> PreparedJobSubmission:
    events = events if events is not None else []
    source = Source(
        observed
        or TaskRead(
            snapshot=replace(
                stored.snapshot, id=uuid4(), captured_at=NOW + timedelta(seconds=1)
            ),
            scope_key="DRY",
            labels=frozenset({"agent-ready"}),
        ),
        events,
    )
    return asyncio.run(
        PrepareJobSubmission(Reader(stored, events), source, (policy,)).execute(
            snapshot_id=stored.snapshot.id,
            repository_id=selected_repository or policy.repository_id,
            authorization=authorization or authority(policy),
        )
    )


def candidate(prepared: PreparedJobSubmission) -> Job:
    return prepared.build_candidate(
        job_id=uuid4(),
        head=RepositoryHead(
            provider_repository_id=123,
            repository_id=prepared.policy.repository_id,
            target_branch=prepared.policy.target_branch,
            revision="a" * 40,
        ),
        now=NOW,
    )


def recorded(prepared: PreparedJobSubmission) -> RecordedJobSubmission:
    return RecordedJobSubmission(
        job=candidate(prepared),
        submission_key=prepared.submission_key,
        submission_fingerprint=prepared.submission_fingerprint,
    )


def test_preparation_keeps_canonical_snapshot_and_uses_fresh_readiness() -> None:
    stored, policy = stored_task(), repository()
    events: list[str] = []
    prepared = prepare(stored, policy, events=events)
    assert events == ["stored", "fresh"]
    assert prepared.stored is stored
    assert prepared.policy is policy
    assert prepared.actor_id == "test-operator"


def test_candidate_pins_the_canonical_snapshot_and_repository_policy() -> None:
    stored, policy = stored_task(), repository()
    prepared = prepare(stored, policy)

    job = candidate(prepared)

    assert job.snapshot_id == stored.snapshot.id
    assert job.source_task_id == stored.source_task_id
    assert job.required_check_ids == ("pytest",)
    assert job.base_revision == "a" * 40
    assert job.authorized_branch == f"devrelay/jobs/{job.id.hex}"


@pytest.mark.parametrize("reason", ["foreign", "disabled", "unknown"])
def test_repository_authority_fails_before_reads(reason: str) -> None:
    stored, policy = stored_task(), repository()
    events: list[str] = []
    authorization = authority(policy)
    selection = policy.repository_id
    if reason == "foreign":
        authorization = replace(
            authorization, allowed_repository_ids=frozenset({uuid4()})
        )
    elif reason == "disabled":
        policy = replace(policy, enabled=False)
    else:
        selection = uuid4()
        authorization = replace(
            authorization, allowed_repository_ids=frozenset({selection})
        )
    with pytest.raises(JobSubmissionRejected):
        prepare(
            stored,
            policy,
            events=events,
            authorization=authorization,
            selected_repository=selection,
        )
    assert events == []


def test_stored_site_mismatch_fails_before_provider_read() -> None:
    stored, policy = stored_task(), repository()
    events: list[str] = []
    stored = replace(
        stored,
        snapshot=replace(
            stored.snapshot,
            identity=replace(
                stored.snapshot.identity,
                site_id="other-site",
            ),
        ),
    )
    with pytest.raises(JobSubmissionRejected):
        prepare(stored, policy, events=events)
    assert events == ["stored"]


@pytest.mark.parametrize(
    "change", ["identity", "scope", "title", "description", "criteria"]
)
def test_changed_identity_scope_or_content_rejects_preparation(change: str) -> None:
    stored, policy = stored_task(), repository()
    snapshot = stored.snapshot
    scope = "DRY"
    if change == "identity":
        snapshot = replace(
            snapshot, identity=replace(snapshot.identity, external_id="10006")
        )
    elif change == "scope":
        scope = "OTHER"
    elif change == "title":
        snapshot = replace(snapshot, title="Different requirements")
    elif change == "description":
        snapshot = replace(snapshot, description="Edited requirements")
    else:
        snapshot = replace(snapshot, acceptance_criteria=("Edited criterion.",))
    observed = TaskRead(
        snapshot=snapshot, scope_key=scope, labels=frozenset({"agent-ready"})
    )
    expected = (
        JobSubmissionRejected
        if change in {"identity", "scope"}
        else TaskRequirementsChanged
    )
    with pytest.raises(expected):
        prepare(stored, policy, observed)


@pytest.mark.parametrize("gap", list(RequirementGap))
def test_fresh_requirements_must_be_complete(gap: RequirementGap) -> None:
    stored, policy = stored_task(), repository()
    labels = frozenset({"agent-ready"})
    if gap is RequirementGap.READINESS_LABEL:
        labels = frozenset()
    elif gap is RequirementGap.DESCRIPTION:
        stored = replace(stored, snapshot=replace(stored.snapshot, description=" "))
    else:
        stored = replace(
            stored, snapshot=replace(stored.snapshot, acceptance_criteria=())
        )
    observed = TaskRead(snapshot=stored.snapshot, scope_key="DRY", labels=labels)

    with pytest.raises(TaskNotReady) as failure:
        prepare(stored, policy, observed)
    assert failure.value.gaps == (gap,)


def test_rename_timestamp_newlines_labels_and_actor_do_not_change_intent() -> None:
    stored, policy = stored_task(), repository()
    first = prepare(stored, policy)
    metadata = replace(
        stored,
        snapshot=replace(
            stored.snapshot,
            id=uuid4(),
            display_key="DRY-20",
            captured_at=NOW + timedelta(days=1),
            source_updated_at=NOW,
            description="Preserve requirements.",
        ),
    )
    authorization = replace(authority(policy), actor_id="another-operator")

    second = prepare(
        metadata,
        policy,
        authorization=authorization,
    )
    assert (first.submission_key, first.submission_fingerprint) == (
        second.submission_key,
        second.submission_fingerprint,
    )


@pytest.mark.parametrize(
    "change",
    [
        "version",
        "argv",
        "timeout",
        "directory",
        "target",
        "budget",
        "scope",
        "prefix",
        "provider_id",
    ],
)
def test_policy_edits_keep_identity_but_conflict_with_pinned_fingerprint(
    change: str,
) -> None:
    stored, policy = stored_task(), repository()
    first = prepare(stored, policy)
    if change == "version":
        policy = replace(policy, version="v2")
    elif change == "provider_id":
        policy = replace(policy, provider_repository_id=456)
    elif change in {"argv", "timeout", "directory"}:
        check = policy.checks[0]
        if change == "argv":
            check = replace(check, argv=("uv", "run", "pytest", "-q"))
        elif change == "timeout":
            check = replace(check, timeout_seconds=200)
        else:
            check = replace(check, working_directory="tests")
        policy = replace(policy, checks=(check,))
    elif change == "target":
        policy = replace(policy, target_branch="release")
    elif change == "budget":
        policy = replace(policy, max_repair_cycles=1)
    elif change == "prefix":
        policy = replace(policy, coding_branch_prefix="automation")
    else:
        policy = replace(
            policy,
            task_scope=replace(
                policy.task_scope, allowed_scope_keys=frozenset({"DRY", "OTHER"})
            ),
        )
    second = prepare(stored, policy)
    receipt = recorded(first)
    state = SubmissionState(existing=receipt)

    with pytest.raises(JobSubmissionConflict):
        decide_submission(second, state)

    assert first.submission_key == second.submission_key
    assert first.submission_fingerprint != second.submission_fingerprint


def test_exact_replay_returns_original_even_if_cancelled_or_head_has_moved() -> None:
    prepared = prepare(stored_task(), repository())
    original = recorded(prepared)
    cancelled = original.job.request_cancellation(now=NOW)
    receipt = replace(original, job=cancelled)
    newer = prepared.build_candidate(
        job_id=uuid4(),
        head=RepositoryHead(
            provider_repository_id=123,
            repository_id=prepared.policy.repository_id,
            target_branch="main",
            revision="b" * 40,
        ),
        now=NOW,
    )
    state = SubmissionState(existing=receipt)

    decision = decide_submission(prepared, state)

    assert decision.action is AdmissionAction.EXISTING
    assert decision.existing_job is cancelled
    assert newer.base_revision != cancelled.base_revision


def test_new_requirements_cannot_bypass_unresolved_job() -> None:
    stored, policy = stored_task(), repository()
    first = prepare(stored, policy)
    changed = replace(
        stored, snapshot=replace(stored.snapshot, id=uuid4(), title="New scope")
    )
    second = prepare(changed, policy)
    state = SubmissionState(blocking_job=candidate(first))

    with pytest.raises(JobSubmissionConflict):
        decide_submission(second, state)

    assert second.submission_key != first.submission_key


def test_cancelled_delivery_with_unresolved_pr_still_blocks_new_submission() -> None:
    prepared = prepare(stored_task(), repository())
    cancelled = candidate(prepared).request_cancellation(now=NOW)
    pr = UnresolvedTaskPullRequest(
        source_task_id=prepared.stored.source_task_id,
        reference=PullRequestReference(
            repository_id=prepared.policy.repository_id,
            number=12,
            source_branch=cancelled.authorized_branch,
            target_branch="main",
            head_revision="a" * 40,
        ),
    )
    state = SubmissionState(blocking_job=cancelled, unresolved_pull_request=pr)

    with pytest.raises(JobSubmissionConflict):
        decide_submission(prepared, state)


def test_cancelled_delivery_without_unresolved_pr_allows_new_submission() -> None:
    prepared = prepare(stored_task(), repository())
    cancelled = candidate(prepared).request_cancellation(now=NOW)
    state = SubmissionState(blocking_job=cancelled)

    decision = decide_submission(prepared, state)

    assert decision.action is AdmissionAction.NEW


@pytest.mark.parametrize("field", ["repository_id", "source_task_id", "snapshot_id"])
def test_wrong_scope_or_snapshot_cannot_return_existing_job(field: str) -> None:
    prepared = prepare(stored_task(), repository())
    receipt = recorded(prepared)
    job = receipt.job
    if field == "repository_id":
        job = replace(job, repository_id=uuid4())
    elif field == "source_task_id":
        job = replace(job, source_task_id=uuid4())
    else:
        job = replace(job, snapshot_id=uuid4())
    with pytest.raises(JobSubmissionConflict):
        decide_submission(prepared, SubmissionState(existing=replace(receipt, job=job)))


@pytest.mark.parametrize("field", ["repository", "branch", "revision", "provider_id"])
def test_candidate_requires_matching_trusted_head(field: str) -> None:
    prepared = prepare(stored_task(), repository())
    head = RepositoryHead(
        provider_repository_id=123,
        repository_id=prepared.policy.repository_id,
        target_branch="main",
        revision="a" * 40,
    )
    if field == "repository":
        head = replace(head, repository_id=uuid4())
    elif field == "branch":
        head = replace(head, target_branch="other")
    elif field == "provider_id":
        head = replace(head, provider_repository_id=456)
    else:
        head = replace(head, revision="short")
    candidate_id = uuid4()

    with pytest.raises(ValueError):
        prepared.build_candidate(job_id=candidate_id, head=head, now=NOW)


@pytest.mark.parametrize("field", ["branch", "check", "budget", "version", "key"])
def test_receipt_cannot_hide_mismatched_job_policy(field: str) -> None:
    prepared = prepare(stored_task(), repository())
    receipt = recorded(prepared)
    if field == "branch":
        receipt = replace(receipt, job=replace(receipt.job, target_branch="other"))
    elif field == "check":
        receipt = replace(
            receipt, job=replace(receipt.job, required_check_ids=("other",))
        )
    elif field == "budget":
        receipt = replace(receipt, job=replace(receipt.job, max_repair_cycles=1))
    elif field == "version":
        receipt = replace(receipt, job=replace(receipt.job, policy_version="other"))
    else:
        receipt = replace(receipt, submission_key="a" * 64)
    with pytest.raises(JobSubmissionConflict):
        decide_submission(prepared, SubmissionState(existing=receipt))


def test_source_failure_propagates_without_retry_or_candidate() -> None:
    stored, policy = stored_task(), repository()
    events: list[str] = []

    class Unavailable(Source):
        async def read_by_identity(self, identity: TaskIdentity) -> TaskRead:
            self.events.append("fresh")
            raise TaskSourceUnavailable("Provider is unavailable.")

    service = PrepareJobSubmission(
        Reader(stored, events),
        Unavailable(
            TaskRead(snapshot=stored.snapshot, scope_key="DRY", labels=frozenset()),
            events,
        ),
        (policy,),
    )
    with pytest.raises(TaskSourceUnavailable):
        asyncio.run(
            service.execute(
                snapshot_id=stored.snapshot.id,
                repository_id=policy.repository_id,
                authorization=authority(policy),
            )
        )
    assert events == ["stored", "fresh"]


@pytest.mark.parametrize(
    "reason", ["duplicate_id", "duplicate_slug", "duplicate_provider_id", "unbound"]
)
def test_duplicate_repository_identities_are_rejected(reason: str) -> None:
    stored, policy = stored_task(), repository()
    reader = Reader(stored, [])
    source = Source(
        TaskRead(snapshot=stored.snapshot, scope_key="DRY", labels=frozenset()), []
    )
    policies: tuple[RepositoryPolicy, ...]
    if reason == "duplicate_id":
        policies = (policy, policy)
    elif reason == "duplicate_slug":
        policies = (
            policy,
            replace(
                policy,
                repository_id=uuid4(),
                slug=policy.slug.upper(),
                provider_repository_id=456,
            ),
        )
    elif reason == "duplicate_provider_id":
        policies = (
            policy,
            replace(policy, repository_id=uuid4(), slug="example/other"),
        )
    else:
        policies = (replace(policy, provider_repository_id=None),)

    with pytest.raises(ValueError):
        PrepareJobSubmission(reader, source, policies)


class SubmissionReader:
    def __init__(
        self, events: list[str], stored: StoredJobSubmission | None = None
    ) -> None:
        self.events, self.stored = events, stored

    async def find_by_key(self, submission_key: str) -> StoredJobSubmission | None:
        self.events.append("existing")
        return self.stored


class RepositorySource:
    def __init__(
        self, events: list[str], observation: RepositorySubmissionObservation
    ) -> None:
        self.events, self.observation = events, observation

    async def observe(
        self, *, repository_id: UUID, target_branch: str, source_task_id: UUID
    ) -> RepositorySubmissionObservation:
        self.events.append("git")
        return self.observation


class SubmissionUnit:
    def __init__(self, events: list[str], fail_commit: bool = False) -> None:
        self.events, self.fail_commit = events, fail_commit

    @property
    def jobs(self) -> Self:
        return self

    async def admit(
        self, intent: PreparedJobSubmission, candidate: Job
    ) -> SavedJobSubmission:
        self.events.append("admit")
        return SavedJobSubmission(
            stored=StoredJobSubmission(intent=intent, job=candidate), created=True
        )

    async def __aenter__(self) -> Self:
        self.events.append("begin")
        return self

    async def commit(self) -> None:
        self.events.append("commit")
        if self.fail_commit:
            raise JobPersistenceError("Commit failed.")

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.events.append("rollback" if exc_type else "close")


def submitter(
    stored: StoredTaskSnapshot,
    policy: RepositoryPolicy,
    events: list[str],
    *,
    existing: StoredJobSubmission | None = None,
    ready: bool = True,
    fail_commit: bool = False,
    observation: RepositorySubmissionObservation | None = None,
) -> SubmitJob:
    return SubmitJob(
        PrepareJobSubmission(
            Reader(stored, events),
            Source(
                TaskRead(
                    snapshot=stored.snapshot,
                    scope_key="DRY",
                    labels=frozenset({"agent-ready"}) if ready else frozenset(),
                ),
                events,
            ),
            (policy,),
        ),
        SubmissionReader(events, existing),
        lambda: SubmissionUnit(events, fail_commit),
        RepositorySource(
            events,
            observation
            or RepositorySubmissionObservation(
                head=RepositoryHead(
                    provider_repository_id=123,
                    repository_id=policy.repository_id,
                    target_branch="main",
                    revision="a" * 40,
                ),
            ),
        ),
        now=lambda: NOW,
    )


def test_new_submission_checks_sources_before_transaction_and_returns_after_commit() -> (
    None
):
    stored, policy = stored_task(), repository()
    events: list[str] = []
    service = submitter(stored, policy, events)
    authorization = authority(policy)

    result = asyncio.run(
        service.execute(
            snapshot_id=stored.snapshot.id,
            repository_id=policy.repository_id,
            authorization=authorization,
        )
    )

    assert result.created
    assert result.stored.intent.policy.checks == policy.checks
    assert events == [
        "stored",
        "existing",
        "fresh",
        "git",
        "begin",
        "admit",
        "commit",
        "close",
    ]


def test_exact_replay_skips_freshness_and_git_and_keeps_original_actor() -> None:
    stored, policy = stored_task(), repository()
    prepared = prepare(stored, policy)
    existing = StoredJobSubmission(intent=prepared, job=candidate(prepared))
    events: list[str] = []
    service = submitter(stored, policy, events, existing=existing, ready=False)
    authorization = replace(authority(policy), actor_id="second-operator")

    result = asyncio.run(
        service.execute(
            snapshot_id=stored.snapshot.id,
            repository_id=policy.repository_id,
            authorization=authorization,
        )
    )

    assert not result.created and result.stored is existing
    assert result.stored.intent.actor_id == "test-operator"
    assert events == ["stored", "existing"]


@pytest.mark.parametrize("change", ["policy", "provider_id", "legacy"])
def test_replayed_policy_conflict_fails_before_freshness_or_transaction(
    change: str,
) -> None:
    stored, policy = stored_task(), repository()
    prepared = prepare(stored, policy)
    existing = StoredJobSubmission(intent=prepared, job=candidate(prepared))
    changed = (
        replace(policy, version="v2")
        if change == "policy"
        else replace(policy, provider_repository_id=456)
    )
    if change == "legacy":
        existing = replace(
            existing,
            intent=replace(
                prepared, policy=replace(policy, provider_repository_id=None)
            ),
        )
        changed = policy
    events: list[str] = []
    service = submitter(stored, changed, events, existing=existing)
    authorization = authority(policy)

    with pytest.raises(JobSubmissionConflict):
        asyncio.run(
            service.execute(
                snapshot_id=stored.snapshot.id,
                repository_id=policy.repository_id,
                authorization=authorization,
            )
        )

    assert events == ["stored", "existing"]


@pytest.mark.parametrize("failure", ["readiness", "pr", "head", "commit"])
def test_failed_submission_does_not_return_success(failure: str) -> None:
    stored, policy = stored_task(), repository()
    events: list[str] = []
    observation = RepositorySubmissionObservation(
        head=RepositoryHead(
            provider_repository_id=123,
            repository_id=policy.repository_id,
            target_branch="main",
            revision="a" * 40,
        )
    )
    if failure == "pr":
        observation = replace(
            observation,
            unresolved_pull_request=PullRequestReference(
                repository_id=policy.repository_id,
                number=5,
                source_branch="previous-job",
                target_branch="main",
                head_revision="b" * 40,
            ),
        )
    elif failure == "head":
        observation = replace(
            observation, head=replace(observation.head, repository_id=uuid4())
        )
    expected = {
        "readiness": TaskNotReady,
        "pr": JobSubmissionConflict,
        "head": ValueError,
        "commit": JobPersistenceError,
    }[failure]
    service = submitter(
        stored,
        policy,
        events,
        ready=failure != "readiness",
        fail_commit=failure == "commit",
        observation=observation,
    )
    authorization = authority(policy)

    with pytest.raises(expected):
        asyncio.run(
            service.execute(
                snapshot_id=stored.snapshot.id,
                repository_id=policy.repository_id,
                authorization=authorization,
            )
        )

    if failure == "commit":
        assert events[-1] == "rollback"
    else:
        assert "begin" not in events
