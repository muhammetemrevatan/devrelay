from pathlib import Path

from devrelay.config.worker import WorkerSettings


def validate_worker_configuration(env_file: Path | None = None) -> None:
    # Pylance omits _env_file and treats environment-sourced fields as required args.
    WorkerSettings(_env_file=env_file)  # pyright: ignore[reportCallIssue]
