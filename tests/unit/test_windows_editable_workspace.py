from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Event

import pytest

from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from devrelay.infrastructure.workspace import windows
from tests.integration.test_windows_base_materialization import snapshot
from tests.unit.test_windows_base_materialization import Kernel
from tests.workspace_fixtures import workspace_binding


def prepared(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[windows.WindowsWorkspaceReservation, Kernel]:
    kernel = Kernel()
    monkeypatch.setattr(windows, "_load_kernel", lambda: kernel)
    native = windows.WindowsWorkspaceReservation(
        workspace_binding(),
        windows.WindowsWorkspacePolicy(
            root_directory="C:\\coordinator\\workspaces",
            protected_directories=("C:\\private-state",),
        ),
    )
    native.reserve()
    return native, kernel


def test_handoff_releases_only_descendants_and_keeps_root_guards(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    native, kernel = prepared(monkeypatch)
    assert native.reservation
    root_handles = tuple(native._handles)
    result = native.materialize(
        native.reservation,
        snapshot(native.binding, {"a.txt": b"a", "src/nested/b.txt": b"b"}),
    )
    descendant_handles = tuple(native._handles[len(root_handles) :])
    assert native.materialization is result

    handoff = native.handoff_materialized_base(result)

    assert handoff.materialization is result and handoff.location is native.location
    assert tuple(native._handles) == root_handles
    assert all(
        args[1] == (1, 3, None, 3, 0x02200000, None)
        for kind, args in kernel.calls
        if kind == "open" and args[1][3] != 1
    )
    assert kernel.closed == list(reversed(descendant_handles))
    assert all(handle in root_handles for _, handle, _ in native._held)
    assert native._materialization_snapshot is None
    assert native._materialization_files == () and native._materialization_nodes == {}
    with pytest.raises(WorkspaceUnavailable):
        native.handoff_materialized_base(result)
    assert kernel.closed == list(reversed(descendant_handles))


@pytest.mark.parametrize(
    "damage",
    [
        "readback",
        "file_identity",
        "extra_entry",
        "duplicate_entry",
        "case_alias",
        "enumeration_reparse",
        "identity",
        "hardlink",
        "file_reparse",
    ],
)
def test_handoff_rechecks_bytes_tree_and_native_identity_before_releasing(
    monkeypatch: pytest.MonkeyPatch, damage: str
) -> None:
    native, kernel = prepared(monkeypatch)
    assert native.reservation
    result = native.materialize(
        native.reservation, snapshot(native.binding, {"file.txt": b"literal bytes"})
    )
    kernel.damage = damage

    with pytest.raises(WorkspaceUnavailable):
        native.handoff_materialized_base(result)

    assert kernel.closed == []
    assert native._materialization_sealed.is_set()


def test_equal_copy_cannot_release_original_guards(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    native, kernel = prepared(monkeypatch)
    assert native.reservation
    result = native.materialize(
        native.reservation, snapshot(native.binding, {"file.txt": b"data"})
    )

    with pytest.raises(WorkspaceUnavailable):
        native.handoff_materialized_base(replace(result))

    assert kernel.closed == []
    with pytest.raises(WorkspaceUnavailable):
        native.handoff_materialized_base(result)


@pytest.mark.parametrize("mutation", ["source", "manifest", "count", "bytes", "tree"])
def test_handoff_revalidates_retained_manifest_and_result(
    monkeypatch: pytest.MonkeyPatch, mutation: str
) -> None:
    native, kernel = prepared(monkeypatch)
    assert native.reservation
    source = snapshot(native.binding, {"file.txt": b"data"})
    result = native.materialize(native.reservation, source)
    if mutation == "source":
        object.__setattr__(source.files[0], "content", b"changed")
    else:
        field, value = {
            "manifest": ("manifest_digest", "0" * 64),
            "count": ("file_count", 2),
            "bytes": ("total_bytes", 7),
            "tree": ("root_tree_sha", "b" * 40),
        }[mutation]
        object.__setattr__(result, field, value)

    with pytest.raises(WorkspaceUnavailable):
        native.handoff_materialized_base(result)

    assert kernel.closed == []


def test_partial_close_failure_keeps_remaining_inventory_and_denies_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    native, kernel = prepared(monkeypatch)
    assert native.reservation
    result = native.materialize(
        native.reservation,
        snapshot(native.binding, {"src/a.txt": b"a", "src/b.txt": b"b"}),
    )
    original = tuple(native._handles)
    calls: list[int] = []

    def close(handle: int) -> int:
        calls.append(handle)
        return int(len(calls) != 2)

    kernel.CloseHandle.body = close

    with pytest.raises(WorkspaceUnavailable):
        native.handoff_materialized_base(result)

    assert calls == list(reversed(original[-2:]))
    assert tuple(native._handles) == original[:-1]
    assert native._materialization_sealed.is_set()
    with pytest.raises(WorkspaceUnavailable):
        native.handoff_materialized_base(result)
    assert len(calls) == 2
    kernel.CloseHandle.body = kernel.close
    native.close()
    assert native._handles == []


def test_seal_during_close_stops_before_next_handle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    native, kernel = prepared(monkeypatch)
    assert native.reservation
    result = native.materialize(
        native.reservation,
        snapshot(native.binding, {"src/a.txt": b"a", "src/b.txt": b"b"}),
    )
    original = tuple(native._handles)

    def close(handle: int) -> int:
        kernel.closed.append(handle)
        native.seal_materialization()
        return 1

    kernel.CloseHandle.body = close

    with pytest.raises(WorkspaceUnavailable):
        native.handoff_materialized_base(result)

    assert kernel.closed == [original[-1]]
    assert tuple(native._handles) == original[:-1]


def test_empty_base_handoff_retains_all_root_guards(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    native, kernel = prepared(monkeypatch)
    assert native.reservation
    result = native.materialize(native.reservation, snapshot(native.binding, {}))
    original = tuple(native._handles)

    handoff = native.handoff_materialized_base(result)

    assert handoff.materialization is result
    assert tuple(native._handles) == original and kernel.closed == []


def test_sealed_scope_cannot_handoff(monkeypatch: pytest.MonkeyPatch) -> None:
    native, kernel = prepared(monkeypatch)
    assert native.reservation
    result = native.materialize(
        native.reservation, snapshot(native.binding, {"file.txt": b"data"})
    )
    native.seal_materialization()

    with pytest.raises(WorkspaceUnavailable):
        native.handoff_materialized_base(result)

    assert kernel.closed == []


def test_seal_is_nonblocking_while_native_close_is_unsettled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    native, kernel = prepared(monkeypatch)
    assert native.reservation
    result = native.materialize(
        native.reservation,
        snapshot(native.binding, {"src/a.txt": b"a", "src/b.txt": b"b"}),
    )
    entered, release = Event(), Event()

    def close(handle: int) -> int:
        entered.set()
        if not release.wait(5):
            raise RuntimeError("Fixture close did not settle.")
        kernel.closed.append(handle)
        return 1

    kernel.CloseHandle.body = close
    with ThreadPoolExecutor(max_workers=1) as executor:
        operation = executor.submit(native.handoff_materialized_base, result)
        assert entered.wait(5)
        try:
            native.seal_materialization()
            assert kernel.closed == []
        finally:
            release.set()
        with pytest.raises(WorkspaceUnavailable):
            operation.result(timeout=5)

    assert len(kernel.closed) == 1
    with pytest.raises(WorkspaceUnavailable):
        native.handoff_materialized_base(result)
