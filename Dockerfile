FROM node:22-alpine AS frontend-build

WORKDIR /frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV PATH="/app/.venv/bin:$PATH"

COPY pyproject.toml uv.lock ./
# The project metadata references README.md, but dependency installation does not need its content.
# Keeping documentation out of this layer avoids reinstalling OpenCV and the Python stack for a
# README-only change, which is especially expensive on the supported mechanical-disk staging host.
# QuickJS publishes an x86_64 wheel but is compiled from source on ARM64. Keep the compiler out of
# the runtime image while retaining one dependency layer that works on both the staging host and Pi.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && touch README.md \
    && uv sync --frozen --no-dev --no-install-project \
    && apt-get purge -y --auto-remove build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY app ./app
COPY manga_manager ./manga_manager
COPY README.md ./
COPY alembic.v2.ini ./
COPY scripts ./scripts
COPY --from=frontend-build /frontend/dist ./frontend/dist
RUN uv sync --frozen --no-dev

EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 CMD ["python", "-c", "import json, sys, urllib.request; response = urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=3); sys.exit(0 if json.load(response).get('ok') else 1)"]
CMD ["uvicorn", "manga_manager.web.app:app", "--host", "0.0.0.0", "--port", "8000"]
