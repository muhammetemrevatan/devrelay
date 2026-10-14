import pytest
from pydantic import ValidationError

from devrelay.config.control import ControlSettings
from devrelay.config.worker_base_sources import WorkerBaseSourcesSettings


def test_base_sources_are_disabled_without_explicit_gate() -> None:
    assert ControlSettings().worker_base_sources is None


def test_base_sources_require_claims(worker_configuration: ControlSettings) -> None:
    values = worker_configuration.model_dump()
    values["worker_base_sources"] = {}

    with pytest.raises(ValidationError, match="explicitly enabled claims"):
        ControlSettings.model_validate(values)


def test_base_sources_can_be_enabled_separately(
    worker_configuration: ControlSettings,
) -> None:
    values = worker_configuration.model_dump()
    values["worker_claims"] = {}
    values["worker_base_sources"] = {}

    settings = ControlSettings.model_validate(values)

    assert settings.worker_base_sources == WorkerBaseSourcesSettings()
    assert settings.worker_claims is not None


@pytest.mark.parametrize(
    "values",
    [
        {"max_concurrent_requests": 0},
        {"max_concurrent_requests": 2},
        {"max_concurrent_requests": True},
        {"body_timeout_seconds": True},
        {"body_timeout_seconds": 0},
        {"body_timeout_seconds": 16},
        {"body_timeout_seconds": float("nan")},
        {"request_timeout_seconds": False},
        {"request_timeout_seconds": 0},
        {"request_timeout_seconds": 91},
        {"request_timeout_seconds": float("inf")},
        {"unknown": "value"},
    ],
)
def test_base_source_limits_fail_closed(values: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        WorkerBaseSourcesSettings.model_validate(values)
