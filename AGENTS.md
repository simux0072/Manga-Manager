# Repository Guidelines

## Project Structure & Module Organization

`manga_manager/` is the PostgreSQL v2 service: domain models, application workflows, SQLAlchemy
infrastructure, Alembic migrations, workers, CLI, and FastAPI API layer. `app/` retains source
adapters and legacy-compatibility code. The React/Vite client lives in `frontend/src/`; its browser
tests are in `frontend/e2e/`. Python tests live in `tests/`. Operational scripts are in `scripts/`,
and deployment, repair, staging, and rollback guides are in `docs/`.

Keep schema changes in a new, ordered file under `manga_manager/migrations/versions/`. Do not edit
generated storage, reports, databases, backups, or `.env` files.

## Build, Test, and Development Commands

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run pytest -q       # Python suite
UV_CACHE_DIR=/tmp/uv-cache uv run ruff check .    # Python lint
cd frontend && npm test -- --run                  # React/Vitest tests
cd frontend && npm run build                      # Type-check and production bundle
scripts/stage-local.sh                            # Direct-Docker local rehearsal
scripts/stage-local.sh serve --build              # Persistent local web/worker/PostgreSQL
scripts/kavita-local.sh up                        # Provision local Kavita integration
```

Use `V2_DATABASE_URL=postgresql+psycopg://... uv run manga-manager migrate` to upgrade a PostgreSQL
database. Prefer the staging script for end-to-end checks; it enforces Pi-sized memory limits and
storage safeguards.

## Coding Style & Naming Conventions

Python targets 3.12+, uses four-space indentation, type hints, `snake_case` functions/modules, and
`PascalCase` classes. Ruff enforces a 100-character line limit. Keep database mutations transactional
and preserve lease/permit semantics in worker code. React uses TypeScript, functional components,
`PascalCase` component names, and camelCase props/state. Avoid adding a second UI framework.

## Testing Guidelines

Name Python tests `test_<behavior>.py` and async tests with `pytest.mark.asyncio` where needed. Add
fixture coverage for parser changes, migrations, retry/fallback paths, and queue concurrency. Run
targeted tests during development, then the full Python and frontend suites. Browser tests require a
running server: `PLAYWRIGHT_BASE_URL=http://127.0.0.1:18000 npm run test:browser`.

## Commit & Pull Request Guidelines

Use concise imperative commit subjects, as in `Harden PostgreSQL scheduling and provider reliability`.
Keep commits focused by concern (catalog, worker/database, UI, or staging/docs). PRs should explain
behavioral changes, list validation commands, link the relevant issue, include UI screenshots for
visual changes, and call out migrations, configuration changes, or operational rollback effects.

## Security & Configuration

Never commit `.env`, API keys, storage, database/WAL files, reports, or backups. Treat source limits
as provider-global; do not bypass access controls or raise concurrency limits without benchmark data.
