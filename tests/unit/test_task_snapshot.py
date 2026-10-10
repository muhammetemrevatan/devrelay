from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta, timezone
from typing import TypedDict, cast
from uuid import UUID, uuid4

import pytest

from devrelay.domain.tasks import TaskIdentity, TaskSnapshot


class _SnapshotChanges(TypedDict, total=False):
    title: str
    display_key: str
    description: str
    acceptance_criteria: tuple[str, ...]


class _TimestampChanges(TypedDict, total=False):
    captured_at: datetime
    source_updated_at: datetime


@pytest.fixture
def snapshot() -> TaskSnapshot:
    return TaskSnapshot(
        id=UUID("97c240c0-e6e5-4877-9cfe-4b2f64b22a8e"),
        identity=TaskIdentity(provider="jira", site_id="site-1", external_id="10005"),
        display_key="DRY-5",
        title="Read a task",
        description="Preserve task requirements.",
        acceptance_criteria=("Map stable identity.", "Reject invalid data."),
        captured_at=datetime(2026, 10, 9, 10, 0, tzinfo=UTC),
    )


@pytest.mark.parametrize("target_name", ["snapshot", "identity"])
def test_snapshot_and_identity_cannot_be_mutated(
    snapshot: TaskSnapshot, target_name: str
) -> None:
    target, name, value = (
        (snapshot, "title", "Changed requirements")
        if target_name == "snapshot"
        else (snapshot.identity, "external_id", "another-task")
    )

    with pytest.raises(FrozenInstanceError):
        setattr(target, name, value)


def test_mutable_criteria_are_rejected(snapshot: TaskSnapshot) -> None:
    mutable = ["This list could change after capture."]
    with pytest.raises(TypeError, match="immutable tuple"):
        replace(snapshot, acceptance_criteria=cast(tuple[str, ...], mutable))


def test_renamed_display_key_preserves_stable_identity(snapshot: TaskSnapshot) -> None:
    renamed = replace(snapshot, display_key="NEW-5")

    assert renamed.identity == snapshot.identity
    assert renamed.content_fingerprint == snapshot.content_fingerprint


def test_same_external_id_in_another_site_has_a_distinct_identity(
    snapshot: TaskSnapshot,
) -> None:
    identity = TaskIdentity(
        provider="jira", site_id="another-site", external_id="10005"
    )

    assert identity != snapshot.identity


def test_capture_metadata_does_not_create_new_business_content(
    snapshot: TaskSnapshot,
) -> None:
    captured_again = replace(
        snapshot,
        id=uuid4(),
        captured_at=snapshot.captured_at + timedelta(days=1),
        source_updated_at=snapshot.captured_at,
    )
    assert captured_again.content_fingerprint == snapshot.content_fingerprint
    assert captured_again.fingerprint_version == snapshot.fingerprint_version == 1


def test_version_one_fingerprint_has_a_stable_reference_value(
    snapshot: TaskSnapshot,
) -> None:
    fingerprint = snapshot.content_fingerprint

    assert fingerprint == (
        "913758edbbab963304d4d8e85e6876fc55912835f751b0f31cc7c25be14b7d90"
    )


@pytest.mark.parametrize("field", ["title", "description", "criteria_order"])
def test_changed_requirements_and_criteria_order_change_fingerprint(
    snapshot: TaskSnapshot, field: str
) -> None:
    original = snapshot.content_fingerprint
    if field == "title":
        changes: _SnapshotChanges = {"title": "Read a different task"}
    elif field == "description":
        changes = {"description": "New requirements."}
    else:
        changes = {"acceptance_criteria": tuple(reversed(snapshot.acceptance_criteria))}

    changed = replace(snapshot, **changes)

    assert changed.content_fingerprint != original


def test_newlines_are_normalized_without_discarding_content(
    snapshot: TaskSnapshot,
) -> None:
    unix = replace(snapshot, description="First line.\nSecond line.")

    windows = replace(unix, description="First line.\r\nSecond line.")

    assert unix.content_fingerprint == windows.content_fingerprint
    assert windows.description == "First line.\r\nSecond line."


def test_trailing_whitespace_changes_content_fingerprint(
    snapshot: TaskSnapshot,
) -> None:
    original = replace(snapshot, description="First line.\nSecond line.")

    changed = replace(original, description="First line.\nSecond line. ")

    assert changed.content_fingerprint != original.content_fingerprint


def test_unicode_content_is_preserved(snapshot: TaskSnapshot) -> None:
    content = "User-provided content: café, 東京, görev."
    captured = replace(snapshot, description=content)
    assert captured.description == content
    assert captured.content_fingerprint != snapshot.content_fingerprint


def test_empty_requirements_can_be_captured_before_eligibility_review(
    snapshot: TaskSnapshot,
) -> None:
    captured = replace(snapshot, description="", acceptance_criteria=())
    assert captured.acceptance_criteria == ()


@pytest.mark.parametrize(
    "changes",
    [
        {"title": " "},
        {"title": "x" * 513},
        {"display_key": ""},
        {"description": "x" * 65_537},
        {"acceptance_criteria": ("",)},
        {"acceptance_criteria": ("x" * 4_097,)},
        {"acceptance_criteria": ("Valid criterion",) * 33},
    ],
)
def test_invalid_or_unbounded_requirements_are_rejected(
    snapshot: TaskSnapshot, changes: _SnapshotChanges
) -> None:
    with pytest.raises(ValueError):
        replace(snapshot, **changes)


@pytest.mark.parametrize("external_id", ["", "  ", " 10005", "10005 ", "x" * 257])
def test_external_identity_is_validated_without_silent_rewriting(
    external_id: str,
) -> None:
    with pytest.raises(ValueError):
        TaskIdentity(provider="jira", site_id="site-1", external_id=external_id)


def test_timestamps_are_aware_and_normalized_to_utc(snapshot: TaskSnapshot) -> None:
    offset = timezone(timedelta(hours=3))
    local = snapshot.captured_at.astimezone(offset)

    captured = replace(snapshot, captured_at=local, source_updated_at=local)

    assert captured.captured_at == snapshot.captured_at
    assert captured.captured_at.tzinfo is UTC
    assert captured.source_updated_at is not None
    assert captured.source_updated_at.tzinfo is UTC


@pytest.mark.parametrize("field", ["captured_at", "source_updated_at"])
def test_naive_timestamps_are_rejected(snapshot: TaskSnapshot, field: str) -> None:
    naive = datetime(2026, 10, 9, 10, 0)
    changes: _TimestampChanges = (
        {"captured_at": naive}
        if field == "captured_at"
        else {"source_updated_at": naive}
    )

    with pytest.raises(ValueError, match="timezone-aware"):
        replace(snapshot, **changes)
