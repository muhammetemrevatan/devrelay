from pathlib import Path

import pytest
from pydantic import ValidationError

from devrelay.config.control import ControlSettings, RuntimeEnvironment
from devrelay.config.worker import WorkerSettings

TEST_TOKEN = "test-only-worker-token-0123456789abcdef"


def test_process_environment_namespaces_are_separate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DEVRELAY_CONTROL_PORT", "8123")
    monkeypatch.setenv("DEVRELAY_WORKER_CONTROL_URL", "https://control.example.test")
    monkeypatch.setenv("DEVRELAY_WORKER_AUTH_TOKEN", TEST_TOKEN)

    control = ControlSettings()
    worker = WorkerSettings()

    assert control.port == 8123
    assert worker.control_url.host == "control.example.test"
    assert TEST_TOKEN not in repr(worker)
    assert "auth_token" not in ControlSettings.model_fields


@pytest.mark.parametrize("port", [0, 65536])
def test_invalid_control_ports_are_rejected(port: int) -> None:
    with pytest.raises(ValidationError):
        ControlSettings(port=port)


def test_unknown_environment_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEVRELAY_CONTROL_ENVIRONMENT", "unknown")

    with pytest.raises(ValidationError):
        ControlSettings()


def test_worker_requires_explicit_credentials() -> None:
    with pytest.raises(ValidationError):
        WorkerSettings()


def test_validation_error_does_not_print_invalid_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    invalid_token = "short-secret"
    monkeypatch.setenv("DEVRELAY_WORKER_CONTROL_URL", "https://control.example.test")
    monkeypatch.setenv("DEVRELAY_WORKER_AUTH_TOKEN", invalid_token)

    with pytest.raises(ValidationError) as error:
        WorkerSettings()

    assert invalid_token not in str(error.value)


@pytest.mark.parametrize(
    "url",
    [
        "http://control.example.test",
        "https://user:password@control.example.test",
        "https://control.example.test?token=secret",
        "https://control.example.test#fragment",
    ],
)
def test_unsafe_worker_control_urls_are_rejected(
    url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DEVRELAY_WORKER_CONTROL_URL", url)
    monkeypatch.setenv("DEVRELAY_WORKER_AUTH_TOKEN", TEST_TOKEN)

    with pytest.raises(ValidationError):
        WorkerSettings()


@pytest.mark.parametrize(
    "url",
    ["http://127.0.0.1:8000", "http://[::1]:8000", "https://control.example.test"],
)
def test_loopback_or_tls_transport_is_accepted(
    url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DEVRELAY_WORKER_CONTROL_URL", url)
    monkeypatch.setenv("DEVRELAY_WORKER_AUTH_TOKEN", TEST_TOKEN)

    settings = WorkerSettings()

    assert str(settings.control_url).rstrip("/") == url


def test_environment_file_is_not_loaded_implicitly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    file = tmp_path / ".env"
    file.write_text("DEVRELAY_CONTROL_ENVIRONMENT=production\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    settings = ControlSettings()

    assert settings.environment == RuntimeEnvironment.DEVELOPMENT


def test_environment_file_is_loaded_when_requested(tmp_path: Path) -> None:
    file = tmp_path / ".env"
    file.write_text("DEVRELAY_CONTROL_ENVIRONMENT=production\n", encoding="utf-8")

    # BaseSettings accepts _env_file, absent from Pylance's synthesized signature.
    settings = ControlSettings(_env_file=file)  # pyright: ignore[reportCallIssue]

    assert settings.environment == RuntimeEnvironment.PRODUCTION
