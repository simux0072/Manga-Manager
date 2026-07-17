# Repository Guidelines

## Project Structure & Module Organization

Provider adapters and transport records live in `app/`. Core code is under `manga_manager/`:
`domain/` defines catalog and job contracts, `application/` contains use cases, `infrastructure/`
owns PostgreSQL, queue, and storage code, `worker/` runs durable pools, and `web/` exposes FastAPI.
Alembic revisions are in `manga_manager/migrations/versions/`. The React client is in
`frontend/src/`; browser tests are in `frontend/tests/`. Python tests live in `tests/`, operational
scripts in `scripts/`, and deployment guidance in `docs/`.

## Build, Test, and Development Commands

```bash
UV_CACHE_DIR=.uv-cache uv sync --frozen --extra dev
UV_CACHE_DIR=.uv-cache uv run pytest -q
UV_CACHE_DIR=.uv-cache uv run ruff check .
cd frontend && npm ci && npm test && npm run build
scripts/test-environment.sh validate
```

The first command installs locked Python dependencies. The next two run Python tests and linting.
Frontend commands run Vitest and create the production Vite bundle. The final command performs the
isolated PostgreSQL/Kavita/browser staging rehearsal; it is intentionally slower. For persistent
local use, run `scripts/kavita-local.sh up` (Manga Manager plus Kavita) or
`scripts/stage-local.sh serve --build` (Manga Manager only), never both launchers concurrently.

## Coding Style & Naming Conventions

Use four-space Python indentation, type annotations, Ruff-compatible imports, and `snake_case` for
functions/modules. Classes and validated payloads use `PascalCase`; constants use `UPPER_SNAKE_CASE`.
Keep database changes in a new ordered Alembic revision. TypeScript components use `PascalCase`,
hooks/functions use `camelCase`, and server state belongs in React Query. Preserve existing public
URLs, job payload versions, and storage formats.

## Testing Guidelines

Pytest discovers `test_*.py`; asynchronous tests use `@pytest.mark.asyncio`. Add regression tests
beside the affected subsystem and PostgreSQL tests for leases, migrations, or constraints. UI work
should include Vitest behavior coverage and Playwright checks when layout or navigation changes.
Do not commit generated databases, media, reports, logs, `frontend/dist`, or credentials.

## Commit & Pull Request Guidelines

Follow the history’s concise imperative subjects, such as `Optimize catalog worker hot paths`.
Keep commits scoped by subsystem. Pull requests should explain behavior and migration impact, list
commands run, link relevant issues, and include screenshots for visible UI changes. Call out any
deferred Docker, browser, ARM64, or Raspberry Pi validation explicitly.
