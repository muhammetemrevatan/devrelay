# syntax=docker/dockerfile:1
FROM python:3.14.8-slim-trixie AS base

FROM base AS builder
COPY --from=ghcr.io/astral-sh/uv:0.12.24 /uv /usr/local/bin/uv
ENV UV_PYTHON_DOWNLOADS=0 \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1
WORKDIR /app
COPY pyproject.toml uv.lock .python-version README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-install-project
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable

FROM base AS runtime
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DEVRELAY_CONTROL_ENVIRONMENT=production \
    DEVRELAY_CONTROL_HOST=0.0.0.0 \
    DEVRELAY_CONTROL_PORT=8000
RUN groupadd --gid 10001 devrelay \
    && useradd --uid 10001 --gid devrelay --no-create-home \
        --shell /usr/sbin/nologin devrelay
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY alembic.ini ./
COPY migrations ./migrations
USER 10001:10001
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import json, os, urllib.request; port = int(os.environ.get('DEVRELAY_CONTROL_PORT', '8000')); response = urllib.request.urlopen(f'http://127.0.0.1:{port}/health/live', timeout=3); assert response.status == 200 and json.load(response) == {'status': 'ok'}"]
CMD ["devrelay-control"]

FROM runtime AS broker-git
USER root
ARG DEVRELAY_GIT_PACKAGE_VERSION=1:2.47.3-0+deb13u1
RUN apt-get update -qq \
    && apt-get install -y --no-install-recommends git="${DEVRELAY_GIT_PACKAGE_VERSION}" \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /var/lib/devrelay/git-reads \
    && chown 10001:10001 /var/lib/devrelay/git-reads \
    && chmod 0700 /var/lib/devrelay/git-reads
RUN python -c 'import hashlib,json,subprocess; from pathlib import Path; executable=Path("/usr/bin/git"); helpers=Path("/usr/lib/git-core"); profile={"executable":str(executable),"executable_sha256":hashlib.sha256(executable.read_bytes()).hexdigest(),"version":subprocess.check_output([str(executable),"--version"],text=True).strip(),"exec_directory":str(helpers),"https_helper_sha256":hashlib.sha256((helpers/"git-remote-https").read_bytes()).hexdigest(),"private_directory":"/var/lib/devrelay/git-reads"}; Path("/app/broker-git-profile.json").write_text(json.dumps(profile,sort_keys=True)+"\n")'
USER 10001:10001

FROM runtime AS control
