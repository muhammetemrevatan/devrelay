"""Lossless local v8->v9 fixtures; no live coordination database or runtime."""

import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from devrelay.application.contracts.claims import ClaimReplayConflict
from devrelay.application.contracts.executions import ExecutionIsolation
from devrelay.application.ports.worker_journal import WorkerStateUnavailable
from devrelay.infrastructure.worker_state.journal import SqliteClaimStore
from tests.integration.test_worker_journal import TestStorage
from tests.integration.test_worker_result_journal import create, normalized, retain


def make_v8(path: Path) -> tuple[bytes, bytes]:
    """Recreate the exact old CHECK constraints without changing protected rows."""
    with sqlite3.connect(path / "journal.sqlite3") as connection:
        metadata = connection.execute("SELECT * FROM metadata").fetchone()
        execution = connection.execute("SELECT * FROM execution_input").fetchone()
        assert metadata is not None and execution is not None
        metadata_sql = connection.execute(
            "SELECT sql FROM sqlite_master WHERE name='metadata'"
        ).fetchone()[0]
        execution_sql = connection.execute(
            "SELECT sql FROM sqlite_master WHERE name='execution_input'"
        ).fetchone()[0]
        connection.execute("DROP TABLE metadata")
        connection.execute(metadata_sql.replace("version = 9", "version = 8"))
        connection.execute(
            "INSERT INTO metadata VALUES (?,?,?,?,?,?,?,?)",
            (metadata[0], 8, *metadata[2:]),
        )
        connection.execute("DROP TABLE execution_input")
        connection.execute(
            execution_sql.replace(
                "isolation IN ('windows_restricted', 'docker_linux')",
                "isolation = 'windows_restricted'",
            )
        )
        connection.execute("INSERT INTO execution_input VALUES (?,?,?,?,?)", execution)
        cipher = connection.execute("SELECT ciphertext FROM pending_claim").fetchone()[
            0
        ]
        integrity = connection.execute("SELECT integrity FROM assignment").fetchone()[0]
        return cipher, integrity


@pytest.mark.parametrize("stage", ["empty", "intent", "accepted", "terminal"])
def test_v8_upgrade_preserves_proof_pins_and_complete_result_history(
    tmp_path: Path, stage: str
) -> None:
    setup = create(tmp_path)
    if stage != "empty":
        retain(setup)
    if stage in ("accepted", "terminal"):
        setup.store.record_result_acceptance(setup.request, setup.receipt)
    if stage == "terminal":
        setup.store.record_result_terminal(setup.request, setup.terminal)
    before = setup.store.load_state()
    result = setup.store.load_result_state()
    setup.store.close()
    cipher, integrity = make_v8(tmp_path)
    reopened = SqliteClaimStore(
        tmp_path, setup.store._scope, setup.store._protector, TestStorage()
    )
    try:
        after = reopened.load_state()
        assert after.execution == before.execution
        assert after.workspace_intent == before.workspace_intent
        assert after.workspace_completion == before.workspace_completion
        assert after.request is not None and before.request is not None
        assert after.request.proof.reveal() == before.request.proof.reveal()
        assert normalized(reopened.load_result_state(), result) == result
        assert after.execution is not None
        changed = replace(
            after.execution,
            policy=replace(
                after.execution.policy, isolation=ExecutionIsolation.DOCKER_LINUX
            ),
        )
        with pytest.raises(ClaimReplayConflict):
            reopened.get_or_create_execution(setup.request, changed)
        with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
            assert connection.execute("SELECT version FROM metadata").fetchone() == (9,)
            assert connection.execute(
                "SELECT ciphertext FROM pending_claim"
            ).fetchone() == (cipher,)
            assert connection.execute(
                "SELECT integrity FROM assignment"
            ).fetchone() == (integrity,)
    finally:
        reopened.close()


def test_damaged_v8_fails_before_any_schema_or_pin_change(tmp_path: Path) -> None:
    setup = create(tmp_path)
    retain(setup)
    setup.store.close()
    make_v8(tmp_path)
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        connection.execute("UPDATE assignment SET integrity=?", (b"x" * 32,))
        original = connection.execute(
            "SELECT sql FROM sqlite_master WHERE name='execution_input'"
        ).fetchone()
    with pytest.raises(WorkerStateUnavailable):
        SqliteClaimStore(
            tmp_path, setup.store._scope, setup.store._protector, TestStorage()
        )
    with sqlite3.connect(tmp_path / "journal.sqlite3") as connection:
        assert connection.execute("SELECT version FROM metadata").fetchone() == (8,)
        assert (
            connection.execute(
                "SELECT sql FROM sqlite_master WHERE name='execution_input'"
            ).fetchone()
            == original
        )
