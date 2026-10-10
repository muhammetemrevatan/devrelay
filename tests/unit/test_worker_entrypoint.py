import pytest

from devrelay.worker.main import main


def test_worker_execution_is_explicitly_unavailable(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr("sys.argv", ["devrelay-worker"])

    with pytest.raises(SystemExit) as error:
        main()

    assert error.value.code == 2
    assert "Job execution is not implemented" in capsys.readouterr().err


def test_configuration_check_does_not_print_credentials(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    token = "test-only-worker-token-0123456789abcdef"
    monkeypatch.setenv("DEVRELAY_WORKER_CONTROL_URL", "http://127.0.0.1:8000")
    monkeypatch.setenv("DEVRELAY_WORKER_AUTH_TOKEN", token)
    monkeypatch.setattr("sys.argv", ["devrelay-worker", "--check-config"])

    main()

    output = capsys.readouterr().out
    assert "Worker configuration is valid" in output
    assert token not in output
