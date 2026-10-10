import asyncio
import sys
from uuid import UUID

import pytest
from pydantic import SecretStr, ValidationError
from uvicorn import Config

from devrelay.bootstrap.control import control_event_loop, create_app
from devrelay.bootstrap.security import token_verifier
from devrelay.config.control import ControlSettings
from devrelay.config.database import DatabaseSettings
from devrelay.config.jira import JiraSettings
from devrelay.config.task_import import TaskImportSettings

TOKEN = "test_only_import_token_0123456789abcdefghijklmnop"


@pytest.mark.parametrize(
    "token",
    [
        "",
        "tiny-secret",
        "x" * 129,
        " " + "x" * 43,
        "x" * 43 + "=",
        "replace-with-" + "x" * 43,
    ],
)
def test_invalid_tokens_do_not_appear_in_validation_errors(token: str) -> None:
    with pytest.raises(ValidationError) as error:
        TaskImportSettings(auth_token=SecretStr(token))
    if token:
        assert token not in str(error.value)


def test_import_cannot_be_enabled_without_integrations() -> None:
    settings = TaskImportSettings(auth_token=SecretStr(TOKEN))

    with pytest.raises(ValidationError, match="complete Jira and database"):
        ControlSettings(task_import=settings)


@pytest.mark.parametrize("shared", ["jira", "database"])
def test_import_credentials_cannot_reuse_integration_secrets(shared: str) -> None:
    task_import = TaskImportSettings(auth_token=SecretStr(TOKEN))
    jira = JiraSettings(
        site_url="https://example.atlassian.net",
        cloud_id=UUID("11111111-2222-3333-4444-555555555555"),
        email="reader@example.test",
        api_token=SecretStr(TOKEN if shared == "jira" else "test-only-jira"),
        allowed_project_keys=frozenset({"DRY"}),
    )
    database = DatabaseSettings(
        host="127.0.0.1",
        database="devrelay_test",
        username="devrelay_test",
        password=SecretStr(TOKEN if shared == "database" else "test-only-db"),
    )

    with pytest.raises(ValidationError, match="separate credential") as error:
        ControlSettings(task_import=task_import, jira=jira, database=database)

    assert TOKEN not in str(error.value)


@pytest.mark.parametrize(
    "token,expected",
    [
        (TOKEN, True),
        (TOKEN[:-1], False),
        ("wrong-token", False),
        ("Invalid Unicode credential: ü", False),
    ],
)
def test_verifier_accepts_only_exact_token_and_handles_unicode(
    token: str, expected: bool
) -> None:
    configuration = TaskImportSettings(auth_token=SecretStr(TOKEN))
    verify = token_verifier(configuration.auth_token)

    accepted = verify(token)

    assert accepted is expected
    assert TOKEN not in repr(configuration)


def test_control_uses_psycopg_compatible_windows_event_loop() -> None:
    config = Config(create_app(), loop="devrelay.bootstrap.control:control_event_loop")

    factory = config.get_loop_factory()
    loop = control_event_loop()

    try:
        assert factory is control_event_loop
        if sys.platform == "win32":
            assert isinstance(loop, asyncio.SelectorEventLoop)
    finally:
        loop.close()
