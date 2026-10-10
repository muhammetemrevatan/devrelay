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
