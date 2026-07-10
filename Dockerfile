FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY app ./app
COPY manga_manager ./manga_manager
COPY alembic.ini ./
COPY alembic.v2.ini ./
COPY alembic ./alembic
RUN uv sync --frozen --no-dev

EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 CMD ["uv", "run", "--frozen", "python", "-c", "import json, sys, urllib.request; response = urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=3); sys.exit(0 if json.load(response).get('ok') else 1)"]
CMD ["uv", "run", "--frozen", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
