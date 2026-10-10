import asyncio
import json
import os
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import SecretStr

from devrelay.application.contracts.claims import LeaseProof, LeaseProofVerifier
from devrelay.application.ports.worker_journal import (
    WorkerStateBusy,
    WorkerStateUnavailable,
)
from devrelay.bootstrap.worker import open_worker_journal
from devrelay.config.worker import WorkerSettings
from devrelay.infrastructure.worker_state.windows import WindowsWorkerSecurity

pytestmark = pytest.mark.skipif(
    os.name != "nt", reason="Requires real Windows DPAPI, ACLs and mutexes."
)
TOKEN = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"


@pytest.fixture
def settings(tmp_path: Path) -> WorkerSettings:
    return WorkerSettings(
        control_url="https://control.example/",
        auth_token=SecretStr("test_only_worker_token_0123456789abcdefghijklmnop"),
        workspace_root=tmp_path / "workspaces",
    )


def test_windows_dpapi_round_trip_has_no_plaintext() -> None:
    security = WindowsWorkerSecurity()
    proof = LeaseProof(TOKEN)
    context = b"x" * 32
    ciphertext = security.protect(proof, context=context)

    result = security.unprotect(ciphertext, context=context)

    assert result.reveal() == TOKEN
    assert TOKEN.encode() not in ciphertext


@pytest.mark.parametrize("change", ["context", "ciphertext"])
def test_windows_dpapi_tampering_is_rejected(change: str) -> None:
    security = WindowsWorkerSecurity()
    context = b"x" * 32
    ciphertext = security.protect(LeaseProof(TOKEN), context=context)
    presented = (
        ciphertext[:-1] + bytes((ciphertext[-1] ^ 1,))
        if change == "ciphertext"
        else ciphertext
    )
    entropy = b"y" * 32 if change == "context" else context

    with pytest.raises(WorkerStateUnavailable) as caught:
        security.unprotect(presented, context=entropy)

    assert TOKEN not in str(caught.value)


def test_real_windows_journal_restores_request_after_reopen(
    tmp_path: Path, settings: WorkerSettings
) -> None:
    identity = uuid4()
    path = tmp_path / "private"

    async def scenario() -> tuple[bool, bool]:
        async with open_worker_journal(
            settings, worker_id=identity, state_directory=path
        ) as service:
            first = await service.prepare()
        async with open_worker_journal(
            settings, worker_id=identity, state_directory=path
        ) as service:
            restored = await service.prepare()
        verifier = LeaseProofVerifier.from_proof(
            worker_id=identity,
            claim_request_id=first.claim_request_id,
            proof=first.proof,
        )
        return first.claim_request_id == restored.claim_request_id, verifier.matches(
            restored.proof,
            worker_id=identity,
            claim_request_id=restored.claim_request_id,
        )

    same_id, same_proof = asyncio.run(scenario())

    assert same_id and same_proof


def test_another_process_owner_cannot_open_same_worker_in_other_directory(
    tmp_path: Path, settings: WorkerSettings
) -> None:
    identity = uuid4()

    async def scenario() -> None:
        async with open_worker_journal(
            settings, worker_id=identity, state_directory=tmp_path / "private"
        ):
            async with open_worker_journal(
                settings, worker_id=identity, state_directory=tmp_path / "other"
            ):
                pytest.fail("A second worker owner was admitted.")

    with pytest.raises(WorkerStateBusy):
        asyncio.run(scenario())

    assert not (tmp_path / "other").exists()


def test_insecure_existing_directory_is_not_repaired(
    tmp_path: Path, settings: WorkerSettings
) -> None:
    path = tmp_path / "insecure"
    path.mkdir()

    async def scenario() -> None:
        async with open_worker_journal(
            settings, worker_id=uuid4(), state_directory=path
        ):
            pytest.fail("Insecure directory was admitted.")

    with pytest.raises(WorkerStateUnavailable):
        asyncio.run(scenario())

    assert not (path / "journal.sqlite3").exists()


def test_state_inside_agent_workspace_is_rejected(
    tmp_path: Path, settings: WorkerSettings
) -> None:
    path = settings.workspace_root / "private"

    async def scenario() -> None:
        async with open_worker_journal(
            settings, worker_id=uuid4(), state_directory=path
        ):
            pytest.fail("Agent workspace was admitted.")

    with pytest.raises(WorkerStateUnavailable):
        asyncio.run(scenario())


def test_state_inside_checkout_is_rejected(
    tmp_path: Path, settings: WorkerSettings
) -> None:
    (tmp_path / ".git").mkdir()

    async def scenario() -> None:
        async with open_worker_journal(
            settings, worker_id=uuid4(), state_directory=tmp_path / "private"
        ):
            pytest.fail("Repository checkout was admitted.")

    with pytest.raises(WorkerStateUnavailable):
        asyncio.run(scenario())


def test_corrupted_real_ciphertext_does_not_create_another_request(
    tmp_path: Path, settings: WorkerSettings
) -> None:
    identity = uuid4()
    path = tmp_path / "private"

    async def prepare() -> None:
        async with open_worker_journal(
            settings, worker_id=identity, state_directory=path
        ) as service:
            await service.prepare()

    asyncio.run(prepare())
    with sqlite3.connect(path / "journal.sqlite3") as connection:
        connection.execute("UPDATE pending_claim SET ciphertext = ?", (b"corrupted",))

    with pytest.raises(WorkerStateUnavailable):
        asyncio.run(prepare())

    with sqlite3.connect(path / "journal.sqlite3") as connection:
        count = connection.execute("SELECT COUNT(*) FROM pending_claim").fetchone()
    assert count == (1,)


def test_process_death_releases_mutex_but_preserves_request(
    tmp_path: Path, settings: WorkerSettings
) -> None:
    identity = uuid4()
    path = tmp_path / "private"
    script = """
import asyncio, json, sys
from pathlib import Path
from uuid import UUID
from pydantic import SecretStr
from devrelay.bootstrap.worker import open_worker_journal
from devrelay.config.worker import WorkerSettings
async def main():
    settings = WorkerSettings(control_url='https://control.example/', auth_token=SecretStr('test_only_worker_token_0123456789abcdefghijklmnop'))
    async with open_worker_journal(settings, worker_id=UUID(sys.argv[2]), state_directory=Path(sys.argv[1])) as service:
        request = await service.prepare()
        print(json.dumps({'request_id': str(request.claim_request_id)}), flush=True)
        await asyncio.sleep(60)
asyncio.run(main())
"""
    child = subprocess.Popen(
        [sys.executable, "-c", script, str(path), str(identity)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    reader = ThreadPoolExecutor(max_workers=1)
    try:
        assert child.stdout is not None
        message = reader.submit(child.stdout.readline).result(timeout=15)
        original = json.loads(message)["request_id"]

        async def busy() -> None:
            async with open_worker_journal(
                settings, worker_id=identity, state_directory=tmp_path / "other"
            ):
                pytest.fail("A second process was admitted.")

        with pytest.raises(WorkerStateBusy):
            asyncio.run(busy())
        child.terminate()
        child.wait(timeout=5)

        async def recover() -> str:
            async with open_worker_journal(
                settings, worker_id=identity, state_directory=path
            ) as service:
                restored = await service.prepare()
                return str(restored.claim_request_id)

        recovered = asyncio.run(recover())

        assert recovered == original
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=5)
        reader.shutdown(wait=True, cancel_futures=True)
        for stream in (child.stdout, child.stderr):
            if stream is not None:
                stream.close()


def test_hardlinked_state_file_is_rejected(tmp_path: Path) -> None:
    security = WindowsWorkerSecurity()
    path = tmp_path / "private"
    security.ensure_private_directory(path)
    source = tmp_path / "other.sqlite3"
    source.write_bytes(b"Dummy test data.")
    os.link(source, path / "journal.sqlite3")

    with pytest.raises(WorkerStateUnavailable):
        security.verify_file(path / "journal.sqlite3")


def test_lost_existing_journal_is_not_recreated(
    tmp_path: Path, settings: WorkerSettings
) -> None:
    identity = uuid4()
    path = tmp_path / "private"

    async def scenario() -> None:
        async with open_worker_journal(
            settings, worker_id=identity, state_directory=path
        ) as service:
            await service.prepare()

    asyncio.run(scenario())
    (path / "journal.sqlite3").unlink()

    with pytest.raises(WorkerStateUnavailable):
        asyncio.run(scenario())

    assert not (path / "journal.sqlite3").exists()
