"""Execute the fixed check against the actual readonly candidate package."""

import json
import os
import subprocess
from pathlib import Path

spec = json.loads(os.environ.pop("DEVRELAY_FIXED_CHECK"))
environment = dict(os.environ)
if (Path("/workspace") / "pyproject.toml").is_file():
    result = subprocess.run(
        [
            "/usr/local/bin/uv",
            "pip",
            "install",
            "--python",
            "/opt/deps/.venv/bin/python",
            "--offline",
            "--no-deps",
            "--no-build-isolation",
            "--target",
            "/tmp/candidate-site",
            "/workspace",
        ],
        stdin=subprocess.DEVNULL,
        env=environment,
        check=False,
    )
    if result.returncode:
        raise SystemExit(125)
    environment["PYTHONPATH"] = "/tmp/candidate-site"
else:
    environment["PYTHONPATH"] = "/workspace/src"
os.chdir("/workspace" + ("/" + spec["cwd"] if spec["cwd"] != "." else ""))
os.execvpe(spec["argv"][0], spec["argv"], environment)
