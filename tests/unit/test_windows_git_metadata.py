from dataclasses import replace

import pytest

from devrelay.application.contracts.git_sources import GitBaseSourceSnapshot
from devrelay.application.contracts.workspaces import WorkspaceBinding
from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from devrelay.infrastructure.workspace import windows
from tests.base_source_fixtures import source_fixture
from tests.unit.test_git_metadata import git_source
from tests.unit.test_windows_base_materialization import Kernel
from tests.workspace_fixtures import workspace_binding


def git_inputs() -> tuple[WorkspaceBinding, GitBaseSourceSnapshot]:
    original = workspace_binding()
    raw_source = git_source()
    definition = replace(
        original.definition, base_revision=raw_source.snapshot.request.base_revision
    )
    acknowledgement = replace(
        original.execution.intent.acknowledgement,
        base_revision=definition.base_revision,
        definition_digest=definition.fingerprint(),
    )
    intent = replace(original.execution.intent, acknowledgement=acknowledgement)
    binding = replace(
        original,
        definition=definition,
        execution=replace(original.execution, intent=intent),
    )
    snapshot = replace(
        source_fixture(binding).snapshot, raw_commit=raw_source.raw_commit
    )
    return binding, GitBaseSourceSnapshot(
        snapshot=snapshot, raw_commit=raw_source.raw_commit
    )


@pytest.fixture
def setup(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[windows.WindowsWorkspaceReservation, GitBaseSourceSnapshot, Kernel]:
    binding, source = git_inputs()
    kernel = Kernel()
    monkeypatch.setattr(windows, "_load_kernel", lambda: kernel)
    resource = windows.WindowsWorkspaceReservation(
        binding,
        windows.WindowsWorkspacePolicy(
            root_directory="C:\\coordinator\\workspaces",
            protected_directories=("C:\\private-state",),
        ),
    )
    reservation = resource.reserve()
    resource.materialize(reservation, source.snapshot)
    return resource, source, kernel


def test_native_git_preparation_extends_exact_inventory_then_handoff_releases_all_descendants(
    setup: tuple[windows.WindowsWorkspaceReservation, GitBaseSourceSnapshot, Kernel],
) -> None:
    resource, source, kernel = setup
    result = resource.materialization
    assert result

    prepared = resource.prepare_git_metadata(
        source, result, resource.binding.definition.authorized_branch
    )

    assert prepared.materialization is result and resource.git_preparation is prepared
    assert (
        resource._git_metadata
        and prepared.metadata_digest == resource._git_metadata.fingerprint()
    )
    assert any(path.endswith("\\.git\\HEAD") for path in kernel.entries)
    original = tuple(resource._handles)
    descendant_count = len(resource._materialization_nodes)
    resource.handoff_materialized_base(result)
    assert len(kernel.closed) == descendant_count
    assert len(resource._handles) < len(original)
    assert all("\\.git" not in path for path, _, _ in resource._held)


@pytest.mark.parametrize("change", ["result", "branch", "raw", "snapshot"])
def test_git_pin_mismatch_fails_before_any_metadata_creation(
    setup: tuple[windows.WindowsWorkspaceReservation, GitBaseSourceSnapshot, Kernel],
    change: str,
) -> None:
    resource, source, kernel = setup
    result = resource.materialization
    assert result
    branch = resource.binding.definition.authorized_branch
    if change == "result":
        result = replace(result)
    elif change == "branch":
        branch = "devrelay/other"
    elif change == "raw":
        object.__setattr__(source, "raw_commit", source.raw_commit + b"changed")
    else:
        object.__setattr__(source.snapshot.files[0], "content", b"changed")
    created, writes = list(kernel.created), list(kernel.writes)

    with pytest.raises(WorkspaceUnavailable):
        resource.prepare_git_metadata(source, result, branch)

    assert kernel.created == created and kernel.writes == writes
    assert resource.git_preparation is None and kernel.closed == []
    with pytest.raises(WorkspaceUnavailable):
        resource.handoff_materialized_base(result)


@pytest.mark.parametrize(
    "failure",
    [
        "create_file",
        "write",
        "short_write",
        "flush",
        "read",
        "short_read",
        "find_first",
    ],
)
def test_metadata_native_failure_preserves_partial_resources_and_denies_handoff(
    setup: tuple[windows.WindowsWorkspaceReservation, GitBaseSourceSnapshot, Kernel],
    failure: str,
) -> None:
    resource, source, kernel = setup
    result = resource.materialization
    assert result
    kernel.fail = failure

    with pytest.raises(WorkspaceUnavailable):
        resource.prepare_git_metadata(
            source, result, resource.binding.definition.authorized_branch
        )

    assert kernel.closed == []
    with pytest.raises(WorkspaceUnavailable):
        resource.prepare_git_metadata(
            source, result, resource.binding.definition.authorized_branch
        )
    with pytest.raises(WorkspaceUnavailable):
        resource.handoff_materialized_base(result)


def test_metadata_byte_tampering_is_rejected_before_any_handoff_handle_close(
    setup: tuple[windows.WindowsWorkspaceReservation, GitBaseSourceSnapshot, Kernel],
) -> None:
    resource, source, kernel = setup
    result = resource.materialization
    assert result
    resource.prepare_git_metadata(
        source, result, resource.binding.definition.authorized_branch
    )
    head = next(
        handle for handle, path in kernel.paths.items() if path.endswith("\\.git\\HEAD")
    )
    kernel.contents[head] = b"ref: refs/heads/other\n"

    with pytest.raises(WorkspaceUnavailable):
        resource.handoff_materialized_base(result)

    assert kernel.closed == []


def test_retained_metadata_manifest_tampering_is_rejected_before_handoff(
    setup: tuple[windows.WindowsWorkspaceReservation, GitBaseSourceSnapshot, Kernel],
) -> None:
    resource, source, kernel = setup
    result = resource.materialization
    assert result
    resource.prepare_git_metadata(
        source, result, resource.binding.definition.authorized_branch
    )
    assert resource._git_metadata
    object.__setattr__(resource._git_metadata.files[0], "content", b"changed")

    with pytest.raises(WorkspaceUnavailable):
        resource.handoff_materialized_base(result)

    assert kernel.closed == []


def test_seal_during_metadata_write_stops_remaining_writes(
    setup: tuple[windows.WindowsWorkspaceReservation, GitBaseSourceSnapshot, Kernel],
) -> None:
    resource, source, kernel = setup
    result = resource.materialization
    assert result
    original = len(kernel.writes)
    kernel.on_write = resource.seal_materialization

    with pytest.raises(WorkspaceUnavailable):
        resource.prepare_git_metadata(
            source, result, resource.binding.definition.authorized_branch
        )

    assert len(kernel.writes) == original + 1 and kernel.closed == []


def test_metadata_absolute_path_budget_is_checked_before_any_git_creation(
    setup: tuple[windows.WindowsWorkspaceReservation, GitBaseSourceSnapshot, Kernel],
) -> None:
    _, source, kernel = setup
    binding, _ = git_inputs()
    definition = replace(
        binding.definition,
        base_revision=source.snapshot.request.base_revision,
        repository_id=source.snapshot.request.repository_id,
    )
    reference = replace(
        binding.execution.reference, repository_id=definition.repository_id
    )
    acknowledgement = replace(
        binding.execution.intent.acknowledgement,
        definition_digest=definition.fingerprint(),
    )
    binding = replace(
        binding,
        definition=definition,
        execution=replace(
            binding.execution,
            reference=reference,
            intent=replace(binding.execution.intent, acknowledgement=acknowledgement),
        ),
    )
    resource = windows.WindowsWorkspaceReservation(
        binding,
        windows.WindowsWorkspacePolicy(
            root_directory="C:\\" + "a" * 180,
            protected_directories=("C:\\private-state",),
        ),
    )
    reservation = resource.reserve()
    result = resource.materialize(reservation, source.snapshot)
    created, writes = list(kernel.created), list(kernel.writes)

    with pytest.raises(WorkspaceUnavailable):
        resource.prepare_git_metadata(
            source, result, binding.definition.authorized_branch
        )

    assert kernel.created == created and kernel.writes == writes
