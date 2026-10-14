from dataclasses import replace
from typing import Any
from uuid import uuid4

import pytest

from devrelay.application.contracts.workspaces import (
    WorkspaceBinding,
    WorkspaceDirectoryIdentity,
    WorkspaceReservation,
)
from tests.workspace_fixtures import workspace_binding


@pytest.mark.parametrize(
    "field,value",
    [
        ("job_id", uuid4()),
        ("repository_id", uuid4()),
        ("provider_repository_id", 1),
        ("base_revision", "b" * 40),
        ("authorized_branch", "task/changed"),
        ("target_branch", "other-main"),
        ("policy_version", "changed-policy"),
        ("max_repair_cycles", 5),
    ],
)
def test_binding_rejects_changed_accepted_definition(field: str, value: Any) -> None:
    original = workspace_binding()
    definition = replace(original.definition, **{field: value})

    with pytest.raises(ValueError, match="accepted definition pins"):
        WorkspaceBinding(execution=original.execution, definition=definition)


@pytest.mark.parametrize("field", ["execution", "definition"])
def test_binding_requires_typed_inputs(field: str) -> None:
    original = workspace_binding()
    changes: dict[str, Any] = {field: None}

    with pytest.raises(ValueError, match="typed execution"):
        replace(original, **changes)


@pytest.mark.parametrize(
    "volume,index",
    [
        (True, 1),
        (-1, 1),
        (2**32, 1),
        (1, True),
        (1, 0),
        (1, -1),
        (1, 2**64),
    ],
)
def test_directory_identity_requires_native_bounds(volume: int, index: int) -> None:
    with pytest.raises(ValueError, match="native identity"):
        WorkspaceDirectoryIdentity(volume_serial_number=volume, file_index=index)


@pytest.mark.parametrize("name", ["..", "other", "", "C:\\escape", str(uuid4())])
def test_reservation_rejects_names_outside_execution_identity(name: str) -> None:
    binding = workspace_binding()
    identity = WorkspaceDirectoryIdentity(volume_serial_number=1, file_index=2)

    with pytest.raises(ValueError, match="exact execution pins"):
        WorkspaceReservation(binding=binding, directory_name=name, identity=identity)


def test_reservation_preserves_full_binding_and_native_identity() -> None:
    binding = workspace_binding()
    identity = WorkspaceDirectoryIdentity(volume_serial_number=0, file_index=2**64 - 1)
    directory = str(binding.execution.intent.execution_id)

    result = WorkspaceReservation(
        binding=binding, directory_name=directory, identity=identity
    )

    assert result.binding is binding
    assert result.identity is identity
    assert result.directory_name == directory
    assert binding.definition.snapshot.title not in repr(binding)
