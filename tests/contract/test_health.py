from fastapi.testclient import TestClient

from devrelay.bootstrap.control import create_app
from devrelay.config.control import ControlSettings, RuntimeEnvironment


def test_liveness_response_is_minimal_and_does_not_imply_db_readiness() -> None:
    with TestClient(create_app()) as client:
        response = client.get("/health/live")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_production_does_not_expose_interactive_docs() -> None:
    settings = ControlSettings(environment=RuntimeEnvironment.PRODUCTION)
    with TestClient(create_app(settings)) as client:
        assert client.get("/docs").status_code == 404
        assert client.get("/openapi.json").status_code == 404
        assert client.get("/health/live").status_code == 200
