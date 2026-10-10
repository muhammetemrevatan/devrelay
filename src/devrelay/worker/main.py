import argparse
from pathlib import Path

from devrelay.bootstrap.worker import validate_worker_configuration


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate local worker configuration.")
    parser.add_argument("--check-config", action="store_true")
    parser.add_argument(
        "--env-file", type=Path, help="Explicit local environment file."
    )
    arguments = parser.parse_args()
    if not arguments.check_config:
        parser.error("Job execution is not implemented; use --check-config.")
    validate_worker_configuration(arguments.env_file)
    print("Worker configuration is valid. Job execution is not implemented.")
