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

`kavita-local.sh up` starts both services; do not run `stage-local.sh serve` concurrently.

For fast, deterministic development without downloading a real catalog:

```bash
scripts/test-environment.sh up
scripts/test-environment.sh check
scripts/test-environment.sh scale-check
scripts/test-environment.sh reset --yes
```

This isolated stack generates two synthetic manga and tiny valid CBZs at runtime. The scale check
uses 2,000 database-only series and 25,000 jobs, then removes its disposable volume. No fixture media
or databases are stored in Git.

Kavita credentials are generated once in `.local/kavita.env` with mode `0600`. The library is
created automatically and mapped to the tracked-only `storage-v2-stage/kavita-library` projection.
Use the Operations action or `manga-manager enqueue-kavita-pending` to synchronize existing
downloaded series. Manga Manager rewrites canonical `ComicInfo.xml`, waits for Kavita's asynchronous
scan to finish, then uploads the chosen series cover to both the series and every chapter. It also
imports Kavita chapter progress on each synchronization; the scheduler refreshes tracked series at
most every ten minutes, while the Operations action requests an immediate refresh.

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
- `manga-manager enqueue-library-repair --all-tracked` queues a resumable canonical metadata and
  tracked-only Kavita projection pass without needing the legacy SQLite catalog.
- The scheduler automatically queues the same bounded repair for tracked artifacts missing their
  Kavita projection, so upgrades heal incrementally without a one-off command.
- Cover evidence jobs stop after a bounded failed attempt for a specific cover URL and become
  eligible again when that URL changes. A temporarily unavailable cover no longer prevents Kavita
  series/chapter mapping or reading-progress synchronization.
- Matches are review-only. Suggested matches combine title evidence with crop hashes and local ORB
  geometry; Manual merge ranks the whole tracked library and accepts two or more provider-distinct
  records (up to the configured provider count). Reviewed labels can be exported with
  `export-match-training` without requiring a model.
- `manga-manager diagnostic-bundle --output diagnostics.json` writes a bounded, credential-redacted
  database/provider/worker/storage snapshot suitable for issue reports.

Legacy SQLite is not an application runtime. `audit-legacy`, `repair-legacy`, `validate-legacy`,
`migrate-legacy-library`, `audit-catalog-recovery`, `repair-catalog-recovery`, and `import-cbz`
remain available for one-time recovery.

See [local staging](docs/local-staging.md), [catalog repair](docs/catalog-repair.md),
[concurrency tuning](docs/concurrency-tuning.md), [backup/restore](docs/postgresql-backup-restore.md),
[Pi deployment/rollback](docs/pi-deployment-and-rollback.md), and the
[architecture and optimization audit](docs/codebase-architecture-and-optimization-audit.md).

### Provider identity and grouped queue rollout

After migration, audit rotating provider identities before applying repairs:

```bash
uv run manga-manager repair-provider-identities --report reports/provider-identities-dry-run.json
uv run manga-manager repair-provider-identities --report reports/provider-identities-applied.json --apply
uv run manga-manager reconcile-refresh-queue --report reports/refresh-queue-dry-run.json
uv run manga-manager reconcile-refresh-queue --report reports/refresh-queue-applied.json --apply
```

The Job Center reports both logical groups and raw tasks. A large refresh count normally represents
distinct series discovered by one provider poll, not duplicate homepage requests. Active refreshes
are deduplicated by provider identity and only materially newer observations are coalesced into the
existing job. Refresh reconciliation preserves compatible v1/v2 work, regroups legacy rows, and
rebuilds only unsupported payload versions; leased work receives one deferred replacement.
