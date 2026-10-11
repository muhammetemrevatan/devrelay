import pytest
from pydantic import ValidationError

from devrelay.config.control import ControlSettings
from devrelay.config.worker_claims import WorkerClaimsSettings


@pytest.mark.parametrize(
    "missing", ["workers", "database", "jira", "github", "enabled"]
)
def test_claims_require_explicit_complete_configuration(
    worker_configuration: ControlSettings,
    missing: str,
) -> None:
    values = worker_configuration.model_dump()
    values["worker_claims"] = {}
    if missing == "enabled":
        values["workers"][0]["enabled"] = False
    else:
        values[missing] = () if missing == "workers" else None

    with pytest.raises(ValidationError):
        ControlSettings.model_validate(values)


@pytest.mark.parametrize(
    "values",
    [
        {"max_concurrent_requests": 0},
        {"max_concurrent_requests": 5},
        {"max_concurrent_requests": True},
        {"body_timeout_seconds": 0},
        {"body_timeout_seconds": 16},
        {"unknown": True},
    ],
)
def test_claim_http_limits_are_bounded(values: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        WorkerClaimsSettings.model_validate(values)


def test_claims_can_be_enabled_separately_from_operator_submission(
    worker_configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    values = worker_configuration.model_dump(exclude={"worker_claims"})
    monkeypatch.setenv("DEVRELAY_CONTROL_WORKER_CLAIMS", "{}")

    configuration = ControlSettings(**values)

    assert configuration.worker_claims == WorkerClaimsSettings()
    assert configuration.job_submission is None and configuration.task_import is None


def test_claim_enablement_rejects_undeliverable_check_policy(
    worker_configuration: ControlSettings,
) -> None:
    values = worker_configuration.model_dump()
    values["worker_claims"] = {}
    large = dict(values["repositories"][0]["checks"][0])
    large["argv"] = ("python",) + ("x" * 4096,) * 63
    values["repositories"][0]["checks"] = tuple(
        {**large, "id": f"check_{index}"} for index in range(3)
    )

    with pytest.raises(ValidationError, match="delivery bounds"):
        ControlSettings.model_validate(values)
