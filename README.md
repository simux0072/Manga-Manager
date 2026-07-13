# Manga Manager

Manga Manager is a private PostgreSQL-backed catalog, downloader, and Kavita synchronization
service for Asura Scans, King of Shojo, and MangaFire. It provides a responsive React interface,
durable provider-aware workers, content-addressed CBZ storage, and standalone legacy repair/import
tools.

## Quick start

The supported local workflow uses Docker directly and does not require the Compose plugin:

```bash
scripts/stage-local.sh serve --build
# Manga Manager: http://127.0.0.1:18000
scripts/stage-local.sh down
```

To run Manga Manager with a locally provisioned Kavita instance:

```bash
scripts/kavita-local.sh up
# Manga Manager: http://127.0.0.1:18000
# Kavita: http://127.0.0.1:15000
scripts/kavita-local.sh credentials
scripts/kavita-local.sh down
```

Kavita credentials are generated once in `.local/kavita.env` with mode `0600`. The library is
created automatically and mapped to the staged Manga Manager library. Use the Operations action or
`manga-manager enqueue-kavita-pending` to synchronize existing downloaded series.

## Development

```bash
UV_CACHE_DIR=.uv-cache uv sync --frozen
V2_DATABASE_URL=postgresql+psycopg://manga:manga@localhost/manga_manager \
  UV_CACHE_DIR=.uv-cache uv run manga-manager migrate
UV_CACHE_DIR=.uv-cache uv run pytest -q
UV_CACHE_DIR=.uv-cache uv run ruff check .
cd frontend && npm test -- --run && npm run build
```

Run the API with `uv run uvicorn manga_manager.web.app:app --reload` and the durable worker with
`uv run manga-manager worker`; both require `V2_DATABASE_URL`. Production reserves 5 GiB of free
storage by default. Local staging uses 1 GiB (`STAGE_MIN_FREE_BYTES` overrides it).

## Operations

- `manga-manager enqueue-pull asura` queues a bounded listing scan. Changed entries fan out into
  independently retryable `source_refresh` jobs.
- Tracking queues the first two and latest two chapters, then bounded backfill.
- Chapter jobs reserve shared storage before network work. A low-space pause does not consume an
  attempt and clears automatically when storage recovers.
- Provider request limits, cooldowns, fallbacks, and leased permits are global across workers.
- `manga-manager enqueue-kavita-pending --limit 100` queues downloaded tracked series missing a
  current Kavita mapping.

Legacy SQLite is not an application runtime. `audit-legacy`, `repair-legacy`, `validate-legacy`,
`migrate-legacy-library`, and `import-cbz` remain available for one-time recovery.

See [local staging](docs/local-staging.md), [catalog repair](docs/catalog-repair.md),
[concurrency tuning](docs/concurrency-tuning.md), [backup/restore](docs/postgresql-backup-restore.md),
and [Pi deployment/rollback](docs/pi-deployment-and-rollback.md).
