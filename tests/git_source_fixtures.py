import hashlib
from dataclasses import replace

from devrelay.application.contracts.assignment_delivery import AssignmentDelivery
from devrelay.application.contracts.worker_recovery import WorkerRecoveryState
from devrelay.application.contracts.workspaces import WorkspaceBinding
from tests.base_source_fixtures import BaseFixture, source_fixture
from tests.workspace_fixtures import workspace_inputs


def git_source_fixture(
    binding: WorkspaceBinding, extra_headers: bytes = b""
) -> BaseFixture:
    literal = source_fixture(binding)
    raw = (
        b"tree "
        + literal.snapshot.root_tree_sha.encode("ascii")
        + b"\nparent "
        + b"1" * 40
        + b"\nauthor Fixture <fixture@example.invalid> 1000000000 +0530"
        + b"\ncommitter Fixture <fixture@example.invalid> 1000000001 -0700\n"
        + extra_headers
        + b"\nOriginal fixture commit\n"
    )
    sha = hashlib.sha1(
        b"commit " + str(len(raw)).encode("ascii") + b"\x00" + raw,
        usedforsecurity=False,
    ).hexdigest()
    snapshot = replace(
        literal.snapshot,
        request=replace(literal.snapshot.request, base_revision=sha),
        raw_commit=raw,
    )
    return replace(literal, snapshot=snapshot)


def git_workspace_inputs() -> tuple[
    WorkerRecoveryState, AssignmentDelivery, WorkspaceBinding, BaseFixture
]:
    state, delivery, binding = workspace_inputs()
    fixture = git_source_fixture(binding)
    definition = replace(
        delivery.definition, base_revision=fixture.snapshot.request.base_revision
    )
    delivery = replace(delivery, definition=definition)
    assert state.acknowledgement is not None and state.execution is not None
    assert state.completion is not None
    acknowledgement = replace(
        state.acknowledgement,
        base_revision=definition.base_revision,
        definition_digest=definition.fingerprint(),
    )
    execution = replace(state.execution, acknowledgement=acknowledgement)
    state = replace(
        state,
        acknowledgement=acknowledgement,
        completion=replace(state.completion, intent=acknowledgement),
        execution=execution,
    )
    binding = replace(
        binding,
        definition=definition,
        execution=replace(binding.execution, intent=execution),
    )
    return state, delivery, binding, fixture
