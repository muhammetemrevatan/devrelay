from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID

import pytest

from devrelay.application.contracts.workflows import JobWorkflowState
from devrelay.domain.jobs.job import Job
from devrelay.domain.jobs.values import JobStatus


@pytest.fixture
def job() -> Job:
    now = datetime(2026, 10, 10, tzinfo=UTC)
    return Job(
        id=UUID(int=1),
        source_task_id=UUID(int=2),
        snapshot_id=UUID(int=3),
        repository_id=UUID(int=4),
        authorized_branch="devrelay/jobs/example",
        target_branch="master",
        base_revision="a" * 40,
        policy_version="v1",
        required_check_ids=("pytest",),
        max_repair_cycles=2,
        created_at=now,
        updated_at=now,
    )


def test_initial_projection_preserves_definition_identity_and_time(job: Job) -> None:
    state = JobWorkflowState.initial(job)

    assert state.job_id == job.id
    assert state.status is JobStatus.QUEUED and state.version == 0
    assert state.blocks_new_submission
    assert state.created_at == state.updated_at == job.created_at


@pytest.mark.parametrize("version", [True, False, -1, 2**63, "0", 1.5])
def test_projection_rejects_unbounded_or_untyped_version(
    job: Job, version: object
) -> None:
    state = JobWorkflowState.initial(job)

    with pytest.raises(ValueError):
        replace(state, version=cast(int, version))


@pytest.mark.parametrize("status", [None, "queued", "unknown"])
def test_projection_requires_typed_status(job: Job, status: object) -> None:
    state = JobWorkflowState.initial(job)

    with pytest.raises(ValueError):
        replace(state, status=cast(JobStatus, status))


@pytest.mark.parametrize("guard", [False, 1, None])
def test_queued_projection_requires_a_typed_reservation(
    job: Job, guard: object
) -> None:
    state = JobWorkflowState.initial(job)

    with pytest.raises(ValueError):
        replace(state, blocks_new_submission=cast(bool, guard))


def test_projection_rejects_time_before_creation(job: Job) -> None:
    state = JobWorkflowState.initial(job)

    with pytest.raises(ValueError):
        replace(state, updated_at=job.created_at - timedelta(seconds=1))


def test_initial_factory_rejects_already_claimed_job(job: Job) -> None:
    claimed = job.claim(UUID(int=5), now=job.created_at)

    with pytest.raises(ValueError):
        JobWorkflowState.initial(claimed)


def test_initial_factory_rejects_changed_creation_chronology(job: Job) -> None:
    changed = replace(job, updated_at=job.created_at + timedelta(seconds=1))

    with pytest.raises(ValueError):
        JobWorkflowState.initial(changed)


def test_projection_is_immutable(job: Job) -> None:
    state = JobWorkflowState.initial(job)

    with pytest.raises(FrozenInstanceError):
        state.version = 1  # type: ignore[misc]  # Exercise runtime immutability.
