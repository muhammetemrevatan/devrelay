import argparse
import asyncio
from pathlib import Path

from devrelay.bootstrap.worker import (
    check_worker_runtime,
    run_worker_once,
    validate_worker_configuration,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate or explicitly run one Windows worker task."
    )
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument("--check-config", action="store_true")
    actions.add_argument(
        "--check-runtime",
        action="store_true",
        help="Run synthetic Linux isolation checks without account/model access.",
    )
    actions.add_argument(
        "--run-one",
        action="store_true",
        help="Explicitly run one new task; never restart an old execution.",
    )
    parser.add_argument(
        "--runtime-image", help="Pinned sha256 image for model-free diagnostics only."
    )
    parser.add_argument(
        "--state-directory",
        type=Path,
        help="Explicit private Windows journal directory.",
    )
    parser.add_argument(
        "--env-file", type=Path, help="Explicit local environment file."
    )
    arguments = parser.parse_args()
    if not any((arguments.check_config, arguments.check_runtime, arguments.run_one)):
        parser.error("Choose --check-config, --check-runtime or --run-one.")
    if arguments.runtime_image is not None and not arguments.check_runtime:
        parser.error("--runtime-image is available only with --check-runtime.")
    if arguments.run_one and arguments.state_directory is None:
        parser.error("--run-one requires --state-directory.")
    try:
        if arguments.check_config:
            validate_worker_configuration(arguments.env_file)
            print("Worker configuration is valid.")
        elif arguments.check_runtime:
            asyncio.run(
                check_worker_runtime(arguments.env_file, arguments.runtime_image)
            )
            print(
                "Synthetic Linux runtime isolation checks passed. No model was called."
            )
        else:
            print(
                asyncio.run(
                    run_worker_once(arguments.env_file, arguments.state_directory)
                )
            )
    except Exception:
        parser.exit(
            1,
            "Worker operation unavailable; verify explicit configuration and runtime readiness.\n",
        )
