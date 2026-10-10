import pytest
from pydantic import ValidationError

from devrelay.config.control import ControlSettings


def _set_jira_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    values = {
        "SITE_URL": "https://example.atlassian.net",
        "CLOUD_ID": "11111111-2222-3333-4444-555555555555",
        "EMAIL": "reader@example.test",
        "API_TOKEN": "test-only-secret-token",
        "ALLOWED_PROJECT_KEYS": '["DRY"]',
    }
    for name, value in values.items():
        monkeypatch.setenv(f"DEVRELAY_CONTROL_JIRA__{name}", value)


def test_jira_configuration_is_optional() -> None:
    settings = ControlSettings()

    assert settings.jira is None


def test_nested_jira_settings_are_control_owned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_jira_environment(monkeypatch)

    jira = ControlSettings().jira

    assert jira is not None
    assert jira.allowed_project_keys == frozenset({"DRY"})
    assert "test-only-secret-token" not in repr(jira)


@pytest.mark.parametrize(
    "name,value",
    [
        ("SITE_URL", "http://example.atlassian.net"),
        ("SITE_URL", "https://user:password@example.atlassian.net"),
        ("SITE_URL", "https://example.atlassian.net/path"),
        ("SITE_URL", "https://example.atlassian.net?token=secret"),
        ("SITE_URL", "https://untrusted.example"),
        ("ALLOWED_PROJECT_KEYS", "[]"),
        ("TOKEN_MODE", "oauth"),
        ("API_TOKEN", "secret\n"),
        ("ACCEPTANCE_CRITERIA_FIELD", "arbitrary-field"),
        ("REQUEST_TIMEOUT_SECONDS", "0"),
    ],
)
def test_invalid_connection_policy_is_rejected_at_startup(
    monkeypatch: pytest.MonkeyPatch, name: str, value: str
) -> None:
    _set_jira_environment(monkeypatch)
    monkeypatch.setenv(f"DEVRELAY_CONTROL_JIRA__{name}", value)
    with pytest.raises(ValidationError) as error:
        ControlSettings()
    assert "test-only-secret-token" not in str(error.value)


def test_partial_credentials_do_not_silently_disable_jira(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DEVRELAY_CONTROL_JIRA__API_TOKEN", "test-only-secret-token")
    with pytest.raises(ValidationError) as error:
        ControlSettings()
    assert "test-only-secret-token" not in str(error.value)
