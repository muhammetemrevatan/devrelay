from pathlib import Path
from types import SimpleNamespace
from typing import cast
from uuid import UUID, uuid4

import pytest
import uvicorn

from devrelay.application.contracts.coding_result_receipts import (
    CodingPublicationState,
    CodingResultStatus,
    TerminalCodingResult,
)
from devrelay.bootstrap import accepted_coding_results as publication
from devrelay.bootstrap import control
from devrelay.config.control import ControlSettings


@pytest.mark.parametrize(
    "option", ["--publish-result", "--reconcile-result", "--worker-id"]
)
def test_publication_arguments_require_the_explicit_pair_before_configuration(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], option: str
) -> None:
    def unexpected(**kwargs: object) -> ControlSettings:
        raise AssertionError("Invalid arguments must not load broker configuration.")

    monkeypatch.setattr("sys.argv", ["devrelay-control", option, str(uuid4())])
    monkeypatch.setattr(control, "ControlSettings", unexpected)

    with pytest.raises(SystemExit) as error:
        control.main()

    assert error.value.code == 2
    assert "must be supplied together" in capsys.readouterr().err


def test_result_commands_are_mutually_exclusive(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "devrelay-control",
            "--publish-result",
            str(uuid4()),
            "--reconcile-result",
            str(uuid4()),
            "--worker-id",
            str(uuid4()),
        ],
    )

    with pytest.raises(SystemExit) as error:
        control.main()

    assert error.value.code == 2
    assert "not allowed with argument" in capsys.readouterr().err


def test_publication_command_routes_only_one_result_and_prints_no_credentials(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    settings = ControlSettings()
    result_id, worker_id = uuid4(), uuid4()
    configuration_files: list[object] = []
    calls: list[tuple[ControlSettings, UUID, UUID]] = []
    terminal = cast(
        TerminalCodingResult,
        SimpleNamespace(
            receipt=SimpleNamespace(result_id=result_id),
            reference=SimpleNamespace(number=7, head_revision="a" * 40),
        ),
    )

    def configuration(**kwargs: object) -> ControlSettings:
        configuration_files.append(kwargs["_env_file"])
        return settings

    async def publish(
        received: ControlSettings, result: UUID, worker: UUID
    ) -> TerminalCodingResult:
        calls.append((received, result, worker))
        return terminal

    def unexpected(*args: object, **kwargs: object) -> None:
        raise AssertionError("A one-result command must not start HTTP.")

    monkeypatch.setattr(
        "sys.argv",
        [
            "devrelay-control",
            "--env-file",
            "explicit.env",
            "--publish-result",
            str(result_id),
            "--worker-id",
            str(worker_id),
        ],
    )
    monkeypatch.setattr(control, "ControlSettings", configuration)
    monkeypatch.setattr(publication, "publish_accepted_result", publish)
    monkeypatch.setattr(uvicorn, "run", unexpected)

    control.main()

    output = capsys.readouterr()
    assert calls == [(settings, result_id, worker_id)]
    assert configuration_files == [Path("explicit.env")]
    assert output.err == ""
    assert output.out == (
        f"Published accepted result {result_id}: pull request #7, "
        f"head {'a' * 40}; awaiting human review.\n"
    )


@pytest.mark.parametrize("mode", ["publication", "reconciliation"])
def test_failed_result_command_reports_a_fixed_error_without_sensitive_values(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], mode: str
) -> None:
    secret = "test-only-provider-database-secret"

    async def failed(
        settings: ControlSettings, result_id: UUID, worker_id: UUID
    ) -> TerminalCodingResult:
        raise RuntimeError(secret)

    async def failed_reconciliation(
        settings: ControlSettings, result_id: UUID, worker_id: UUID
    ) -> CodingResultStatus | None:
        raise RuntimeError(secret)

    monkeypatch.setattr(
        "sys.argv",
        [
            "devrelay-control",
            "--publish-result" if mode == "publication" else "--reconcile-result",
            str(uuid4()),
            "--worker-id",
            str(uuid4()),
        ],
    )
    monkeypatch.setattr(publication, "publish_accepted_result", failed)
    monkeypatch.setattr(publication, "reconcile_accepted_result", failed_reconciliation)

    with pytest.raises(SystemExit) as error:
        control.main()

    output = capsys.readouterr()
    assert error.value.code == 1
    assert output.out == ""
    assert output.err == (
        "Coding result operation did not complete; inspect retained history before retrying.\n"
    )
    assert secret not in output.err


@pytest.mark.parametrize("state", [None, *CodingPublicationState])
def test_reconciliation_command_only_observes_and_reports_bounded_status(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    state: CodingPublicationState | None,
) -> None:
    result_id, worker_id = uuid4(), uuid4()
    calls: list[tuple[UUID, UUID]] = []
    reference = SimpleNamespace(number=7, head_revision="a" * 40)
    status = (
        None
        if state is None
        else cast(
            CodingResultStatus,
            SimpleNamespace(
                receipt=SimpleNamespace(result_id=result_id),
                state=state,
                terminal=SimpleNamespace(reference=reference)
                if state is CodingPublicationState.FINALIZED
                else None,
            ),
        )
    )

    async def reconcile(
        settings: ControlSettings, result: UUID, worker: UUID
    ) -> CodingResultStatus | None:
        calls.append((result, worker))
        return status

    def unexpected(*args: object, **kwargs: object) -> None:
        raise AssertionError("Reconciliation must neither publish nor start HTTP.")

    monkeypatch.setattr(
        "sys.argv",
        [
            "devrelay-control",
            "--reconcile-result",
            str(result_id),
            "--worker-id",
            str(worker_id),
        ],
    )
    monkeypatch.setattr(publication, "reconcile_accepted_result", reconcile)
    monkeypatch.setattr(publication, "publish_accepted_result", unexpected)
    monkeypatch.setattr(uvicorn, "run", unexpected)

    control.main()

    output = capsys.readouterr()
    assert calls == [(result_id, worker_id)]
    assert output.err == ""
    if state is None:
        assert output.out == "No accepted result was available for reconciliation.\n"
    elif state is CodingPublicationState.FINALIZED:
        assert output.out == (
            f"Reconciled accepted result {result_id}: finalized, pull request #7, "
            f"head {'a' * 40}; awaiting human review.\n"
        )
    else:
        assert output.out == (
            f"Reconciled accepted result {result_id}: {state.value}; "
            "no new publication write authorized.\n"
        )


def test_default_control_command_still_starts_the_http_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    def server(app: object, **kwargs: object) -> None:
        calls.append(kwargs)

    monkeypatch.setattr("sys.argv", ["devrelay-control"])
    monkeypatch.setattr(uvicorn, "run", server)

    control.main()

    assert len(calls) == 1
    assert calls[0]["loop"] == "devrelay.bootstrap.control:control_event_loop"
    assert calls[0]["access_log"] is False
