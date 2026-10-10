from devrelay.application.ports.task_persistence import TaskPersistenceError
from devrelay.domain.tasks import TaskIdentity, TaskSnapshot
from devrelay.infrastructure.persistence.models import TaskSnapshotRecord


def materialize_snapshot(
    record: TaskSnapshotRecord, identity: TaskIdentity
) -> TaskSnapshot:
    stored = TaskSnapshot(
        id=record.id,
        identity=identity,
        display_key=record.display_key,
        title=record.title,
        description=record.description,
        acceptance_criteria=tuple(record.acceptance_criteria),
        source_updated_at=record.source_updated_at,
        captured_at=record.captured_at,
    )
    if (
        stored.fingerprint_version != record.fingerprint_version
        or stored.content_fingerprint != record.content_fingerprint
    ):
        raise TaskPersistenceError("Stored snapshot failed integrity validation.")
    return stored
