import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from pydantic import SecretStr, ValidationError

from devrelay.config.control import ControlSettings
from devrelay.config.worker import WorkerSettings
from devrelay.config.worker_authentication import WorkerRegistrationSettings

TOKEN = "test_only_worker_token_0123456789abcdefghijklmnop"


def test_worker_registration_is_disabled_by_default(
    worker_configuration: ControlSettings,
) -> None:
    values = worker_configuration.workers[0].model_dump(exclude={"enabled"})

    registration = WorkerRegistrationSettings.model_validate(values)

    assert not registration.enabled


@pytest.mark.parametrize(
    "updates",
    [
        {"auth_token": "short"},
        {"auth_token": "x" * 129},
        {"auth_token": "x" * 44 + " "},
        {"auth_token": "replace-with-" + "x" * 40},
        {"expires_at": "2026-10-10T00:00:00"},
        {"expires_at": None},
        {"enabled": "true"},
        {"allowed_repository_ids": []},
        {"allowed_repository_ids": [str(uuid4()) for _ in range(33)]},
        {"extra_authority": True},
    ],
)
def test_registration_rejects_incomplete_or_unsafe_values(
    worker_configuration: ControlSettings, updates: dict[str, object]
) -> None:
    values = worker_configuration.workers[0].model_dump() | updates

    with pytest.raises(ValidationError) as error:
        WorkerRegistrationSettings.model_validate(values)

    assert TOKEN not in str(error.value)


def test_expiry_with_offset_is_normalized_to_utc(
    worker_configuration: ControlSettings,
) -> None:
    values = worker_configuration.workers[0].model_dump()
    values["expires_at"] = "2026-10-10T04:00:00+03:00"

    registration = WorkerRegistrationSettings.model_validate(values)

    assert registration.expires_at == datetime(2026, 10, 10, 1, tzinfo=UTC)
    assert registration.expires_at.utcoffset() == timedelta(0)


def test_past_expiry_can_load_without_granting_authority(
    worker_configuration: ControlSettings,
) -> None:
    values = worker_configuration.model_dump()
    values["workers"][0]["expires_at"] = datetime(2000, 1, 1, tzinfo=UTC)

    configuration = ControlSettings.model_validate(values)

    assert configuration.workers[0].expires_at.year == 2000


@pytest.mark.parametrize(
    "reason",
    [
        "unknown",
        "disabled",
        "missing_github",
        "duplicate_id",
        "duplicate_token",
        "too_many",
    ],
)
def test_control_enforces_explicit_bounded_worker_scope(
    worker_configuration: ControlSettings, reason: str
) -> None:
    values = worker_configuration.model_dump()
    workers = values["workers"]
    if reason == "unknown":
        workers[0]["allowed_repository_ids"] = frozenset({uuid4()})
    elif reason == "disabled":
        values["repositories"][0]["enabled"] = False
    elif reason == "missing_github":
        values["github"] = None
    else:
        second = dict(workers[0])
        if reason != "duplicate_id":
            second["worker_id"] = uuid4()
        if reason != "duplicate_token":
            second["auth_token"] = SecretStr(TOKEN + "x")
        values["workers"] = (
            (workers[0], second) if reason != "too_many" else (workers[0],) * 33
        )

    with pytest.raises(ValidationError) as error:
        ControlSettings.model_validate(values)

    assert TOKEN not in str(error.value)


@pytest.mark.parametrize(
    "integration", ["jira", "database", "github", "task_import", "job_submission"]
)
def test_worker_token_must_be_distinct_from_all_control_credentials(
    worker_configuration: ControlSettings,
    submission_configuration: ControlSettings,
    integration: str,
) -> None:
    values = worker_configuration.model_dump()
    if integration == "task_import":
        values[integration] = {"auth_token": SecretStr(TOKEN)}
    elif integration == "job_submission":
        values[integration] = (
            submission_configuration.job_submission.model_dump()
            if submission_configuration.job_submission
            else {}
        )
        values[integration]["auth_token"] = SecretStr(TOKEN)
    else:
        key = "password" if integration == "database" else "api_token"
        values[integration][key] = SecretStr(TOKEN)

    with pytest.raises(ValidationError, match="separate credential") as error:
        ControlSettings.model_validate(values)

    assert TOKEN not in str(error.value)


def test_worker_registrations_load_from_explicit_json_environment(
    worker_configuration: ControlSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    registration = worker_configuration.workers[0].model_dump(mode="json")
    registration["auth_token"] = TOKEN
    monkeypatch.setenv("DEVRELAY_CONTROL_WORKERS", json.dumps([registration]))
    values = worker_configuration.model_dump(exclude={"workers"})

    configuration = ControlSettings(**values)

    assert configuration.workers == worker_configuration.workers
    assert TOKEN not in repr(configuration)
    assert TOKEN not in configuration.model_dump_json()


@pytest.mark.parametrize(
    "token",
    ["test-secret-too-small", "x" * 129, "replace-with-" + "x" * 40, "x" * 44 + " "],
)
def test_local_worker_token_uses_the_registration_format(token: str) -> None:
    with pytest.raises(ValidationError) as error:
        WorkerSettings(
            control_url="https://control.example.test", auth_token=SecretStr(token)
        )

    assert token not in str(error.value)
