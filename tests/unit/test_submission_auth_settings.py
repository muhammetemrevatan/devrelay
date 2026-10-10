import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import uuid4

import pytest
from pydantic import ValidationError

from devrelay.api.dependencies import SubmissionOperation
from devrelay.bootstrap.control import create_app
from devrelay.config.control import ControlSettings
from devrelay.config.job_submission import JobSubmissionSettings


def test_submission_is_disabled_by_default() -> None:
    configuration = ControlSettings()

    application = create_app(configuration)

    assert configuration.job_submission is None
    assert "/api/v1/jobs" not in application.openapi()["paths"]


@pytest.mark.parametrize("integration", ["jira", "database", "github"])
def test_submission_requires_all_integrations(
    submission_configuration: ControlSettings, integration: str
) -> None:
    values = submission_configuration.model_dump()
    values[integration] = None

    with pytest.raises(ValidationError, match="complete Jira, database and GitHub"):
        ControlSettings.model_validate(values)


@pytest.mark.parametrize("scope", ["disabled", "unknown"])
def test_submission_scope_must_be_enabled_and_configured(
    submission_configuration: ControlSettings, scope: str
) -> None:
    values = submission_configuration.model_dump()
    if scope == "disabled":
        values["repositories"][0]["enabled"] = False
    else:
        values["job_submission"]["allowed_repository_ids"] = frozenset({uuid4()})

    with pytest.raises(ValidationError, match="enabled repository policies"):
        ControlSettings.model_validate(values)


@pytest.mark.parametrize("integration", ["jira", "database", "github", "task_import"])
def test_submission_token_cannot_reuse_other_credentials(
    submission_configuration: ControlSettings, integration: str
) -> None:
    values = submission_configuration.model_dump()
    token = values["job_submission"]["auth_token"]
    if integration == "task_import":
        values[integration] = {"auth_token": token}
    else:
        name = "password" if integration == "database" else "api_token"
        values[integration][name] = token

    with pytest.raises(ValidationError, match="separate credential") as error:
        ControlSettings.model_validate(values)

    assert token.get_secret_value() not in str(error.value)


@pytest.mark.parametrize(
    "updates",
    [
        {"auth_token": "tiny-token"},
        {"auth_token": "x" * 129},
        {"auth_token": "replace-with-" + "x" * 43},
        {"auth_token": "x" * 43 + "="},
        {"actor_id": ""},
        {"actor_id": "spoof\noperator"},
        {"actor_id": " operator"},
        {"actor_id": "x" * 129},
        {"allowed_repository_ids": []},
        {"allowed_repository_ids": [uuid4() for _ in range(33)]},
        {"max_concurrent_requests": 0},
        {"max_concurrent_requests": 5},
        {"body_timeout_seconds": 0},
        {"body_timeout_seconds": 16},
    ],
)
def test_submission_settings_reject_unbounded_or_invalid_values(
    submission_configuration: ControlSettings, updates: dict[str, object]
) -> None:
    assert submission_configuration.job_submission is not None
    values = submission_configuration.job_submission.model_dump()
    values.update(updates)

    with pytest.raises(ValidationError):
        JobSubmissionSettings.model_validate(values)


def test_submission_scope_loads_from_nested_environment(
    submission_configuration: ControlSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert submission_configuration.job_submission is not None
    expected = submission_configuration.job_submission
    monkeypatch.setenv(
        "DEVRELAY_CONTROL_JOB_SUBMISSION__AUTH_TOKEN",
        expected.auth_token.get_secret_value(),
    )
    monkeypatch.setenv("DEVRELAY_CONTROL_JOB_SUBMISSION__ACTOR_ID", expected.actor_id)
    monkeypatch.setenv(
        "DEVRELAY_CONTROL_JOB_SUBMISSION__ALLOWED_REPOSITORY_IDS",
        json.dumps([str(item) for item in expected.allowed_repository_ids]),
    )
    values = submission_configuration.model_dump(exclude={"job_submission"})

    configuration = ControlSettings(**values)

    assert configuration.job_submission == expected
    assert expected.auth_token.get_secret_value() not in repr(configuration)


def test_resources_cannot_implicitly_enable_submission() -> None:
    @asynccontextmanager
    async def resource() -> AsyncIterator[SubmissionOperation]:
        raise AssertionError("Disabled submission must not open resources.")
        yield

    with pytest.raises(ValueError, match="explicit submission authentication"):
        create_app(ControlSettings(), submission_resources=resource)
