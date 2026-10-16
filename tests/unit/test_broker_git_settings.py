import pytest
from pydantic import ValidationError

from devrelay.config.broker_git import BrokerGitSettings
from devrelay.config.control import ControlSettings


def values() -> dict[str, object]:
    return {
        "executable": "/usr/bin/git",
        "executable_sha256": "a" * 64,
        "version": "git version 2.47.3",
        "exec_directory": "/usr/lib/git-core",
        "https_helper_sha256": "b" * 64,
        "private_directory": "/var/lib/devrelay/git-reads",
    }


def test_broker_git_is_disabled_without_explicit_configuration() -> None:
    settings = ControlSettings(_env_file=None)

    assert settings.broker_git is None


def test_explicit_broker_git_profile_preserves_executable_and_resource_pins() -> None:
    configuration = values()

    settings = BrokerGitSettings.model_validate(configuration)

    assert settings.executable == configuration["executable"]
    assert settings.version == configuration["version"]
    assert settings.executable_sha256 == "a" * 64
    assert settings.https_helper_sha256 == "b" * 64
    assert settings.maximum_processes == 8


@pytest.mark.parametrize(
    "path",
    [
        "git",
        "relative/git",
        "//host/git",
        "/usr/../bin/git",
        "/usr/./bin/git",
        "/usr//bin/git",
        "/usr/bin/",
        "/usr/\\git",
        "/usr/\x00git",
    ],
)
@pytest.mark.parametrize("field", ["executable", "exec_directory", "private_directory"])
def test_broker_git_rejects_ambiguous_or_nonlocal_paths(field: str, path: str) -> None:
    configuration = values() | {field: path}

    with pytest.raises(ValidationError):
        BrokerGitSettings.model_validate(configuration)


@pytest.mark.parametrize(
    "field,value",
    [
        ("executable_sha256", "A" * 64),
        ("https_helper_sha256", "wrong"),
        ("version", "latest"),
        ("maximum_processes", True),
        ("maximum_processes", 9),
        ("maximum_disk_bytes", 0),
        ("operation_timeout_seconds", float("inf")),
        ("total_timeout_seconds", float("nan")),
        ("unknown", "value"),
    ],
)
def test_broker_git_rejects_missing_pins_or_unbounded_policy(
    field: str, value: object
) -> None:
    configuration = values() | {field: value}

    with pytest.raises(ValidationError):
        BrokerGitSettings.model_validate(configuration)


def test_broker_git_requires_separately_enabled_base_delivery() -> None:
    configuration = {"broker_git": values()}

    with pytest.raises(ValidationError, match="explicitly enabled base delivery"):
        ControlSettings.model_validate(configuration)
