# Local direct-Docker staging

The host does not need the Compose plugin:

Set `STAGE_PROJECT` to isolate all container, network, image, and volume names when another staging
stack is running. For example, use
`STAGE_PROJECT=manga-manager-check STAGE_STORAGE_ROOT=/tmp/manga-manager-check scripts/stage-local.sh`.
Use the same project value for teardown: `STAGE_PROJECT=manga-manager-check
scripts/stage-local.sh down --volumes`.

For a persistent development stack without the full rehearsal:

```bash
scripts/stage-local.sh serve --build
scripts/stage-local.sh down
```

The persistent web and worker containers use `unless-stopped` restart policies. A transient process
failure is recovered automatically; `stage-local.sh down` still stops and removes them explicitly.

For normal feature work, prefer the small deterministic environment so provider traffic and a large
archive tree do not slow builds:

```bash
scripts/test-environment.sh up
scripts/test-environment.sh check
scripts/test-environment.sh scale-check
scripts/test-environment.sh down       # preserve its small database
scripts/test-environment.sh reset --yes
```

Run the entire fresh small-stack, Kavita/browser, backup/restore, and disposable scale acceptance
sequence unattended with `scripts/test-environment.sh validate`. It always stops its isolated
services afterward, including when a check fails. Orchestration locks live under
`.local/test-locks`, outside the disposable state tree, and Playwright artifacts live under the
test environment root. A container UID therefore cannot make a later host-side validation fail to
open a lock or replace `frontend/test-results`.

On the Raspberry Pi, run `scripts/test-environment.sh performance-check` once before production
rollout. It uses the same disposable scale project but expands it to 2,000 series, 100,000 synthetic
chapters, and 100,000 jobs. The regular `scale-check` intentionally stays at 25,000 jobs and zero
chapters for quicker development feedback on the slower staging PC. Staging builds use the host's
native architecture unless `STAGE_PLATFORM` is explicitly set for a cross-platform rehearsal.

It uses project `manga-manager-test`, ports 18001/15001, an isolated Kavita configuration, disabled
live sources, two generated manga, and four tiny generated CBZs. `scale-check` uses a separate
temporary project on port 18002, stops its worker before inserting 2,000 series and 25,000 jobs,
checks cursor completeness/grouping/latency and per-route SQL-query ceilings, and always tears
itself down. Every HTTP response includes `Server-Timing` and `X-SQL-Query-Count`; `/metrics`
provides bounded Prometheus-compatible request, duration, SQL, response-size, queue-age,
job-duration, and provider-latency counters. `/livez` is process-only and `/readyz` verifies the
database; `/healthz` remains a readiness-compatible alias.
The small check also downloads each tracked fixture's series and chapter covers from Kavita and
requires their response bytes to match, rather than trusting only Manga Manager's stored checksums.
`scripts/test-environment.sh up` rebuilds its application image by default so it cannot validate
stale code; set `TEST_ENV_BUILD=false` only when restarting an unchanged checkout.
Kavita is temporarily pinned to `jvmilazz0/kavita:0.8.9` because `0.9.0.2` cannot initialize an
empty local-test database reliably. This isolated instance is not public-facing. Set `KAVITA_IMAGE`
explicitly to retest a future fixed release before updating the pin.

Run only one launcher at a time. `scripts/kavita-local.sh up` provisions Kavita and then starts
Manga Manager itself, so it replaces (rather than accompanies) `stage-local.sh serve --build`.
Launch scripts use locks and reject a concurrent startup before it can restart PostgreSQL.
Normal `serve` mode skips provider/queue repair; full rehearsals still run repairs. Set
`STAGE_RUN_REPAIRS=true` only for an intentional, backed-up maintenance startup with workers quiet.

For an automatically provisioned local Kavita integration:

```bash
scripts/kavita-local.sh up
scripts/kavita-local.sh credentials
scripts/kavita-local.sh status
scripts/kavita-local.sh down
```

Credentials default to the project-scoped ignored file
`.local/<STAGE_PROJECT>-kavita.env`; an existing legacy `.local/kavita.env` remains supported. If the
disposable Kavita volume remains but its ignored credentials file was deleted, `up` detects
the administrator mismatch and recreates only the Kavita config volume. It preserves PostgreSQL,
content-addressed blobs, and both manga projection trees. When a credentials file exists, an
authentication failure remains non-destructive; explicitly reset test metadata with
`scripts/kavita-local.sh reset-config --yes` before running `up` again.

Manga Manager is exposed on port 18000 and Kavita on port 15000. Both default to `0.0.0.0`, so a
trusted device on the same network can use `http://<host-ip>:18000` and
`http://<host-ip>:15000`. The launcher prints the detected address. Open those TCP ports in the
host firewall if needed, but do not forward them from the router: this staging stack has no
application authentication. Set `STAGE_BIND_ADDRESS=127.0.0.1` and
`KAVITA_BIND_ADDRESS=127.0.0.1` for host-only access. The isolated automated test environment
always uses loopback regardless of these staging defaults. Kavita reads only
`kavita-library/`; untracked series are removed from that projection while content-addressed blobs
remain intact. `STAGE_MIN_FREE_BYTES` overrides the 1 GiB staging reserve. Web readiness retries
quietly for 120 seconds; set `STAGE_WEB_WAIT_ATTEMPTS` to a larger number on unusually slow storage.

`KAVITA_WAIT_SECONDS` controls the provisioning readiness deadline (900 seconds by default for
mechanical disks).
The launcher recreates the container when the configured image changes, while preserving its named
configuration volume. It completes the I/O-heavy Manga Manager image build before starting Kavita,
so first-run SQLite journal writes do not compete with image export on a mechanical disk. Reset the
isolated volume before switching between incompatible versions.
Staging gives PostgreSQL up to 300 seconds for a graceful checkpoint before replacing its container;
`STAGE_POSTGRES_STOP_SECONDS` can raise that limit on an unusually slow disk. Do not interrupt this
stop phase: forcing it causes PostgreSQL to repeat the same work as crash recovery at next startup.

For the full import and rollback rehearsal:

```bash
STAGE_LEGACY_DATABASE="$PWD/manga_manager.db" \
STAGE_LEGACY_STORAGE_ROOT="$PWD/storage" \
STAGE_STORAGE_ROOT="$PWD/storage-v2-stage" \
scripts/stage-local.sh
```

The preflight refuses legacy import unless a source CBZ can be hardlinked into staging and at least
1 GiB remains free. This prevents a copy-based import from filling a mechanical disk. Import reports
are resumable: if a run stops, execute the same command and already activated identities are skipped.
The legacy import container disables the normal 5 GiB production watermark only after this hardlink
preflight; staged web/worker downloads retain the 1 GiB reserve.

The rehearsal builds the selected platform image, migrates PostgreSQL, imports/reconciles content,
recovers legacy tracking, runs provider-identity and refresh-payload repair in dry-run/apply pairs,
normalizes canonical archive metadata, starts web and worker under Pi-sized memory limits, runs
health/queue/memory checks, rehearses
backup/restore, restarts services, and proves lease recovery. Set `STAGE_SMOKE_SOURCE=mangafire` for
an optional live pull. The integrity step reads every active CBZ and can take a long time on a large
library stored on a mechanical disk. Tear down containers with `scripts/stage-local.sh down`; append
`--volumes` only when the staged database is no longer needed.

Full rehearsals disable scheduled live sources unless `STAGE_SMOKE_SOURCE` or
`STAGE_ENABLE_SOURCES=true` is set. Persistent `serve` mode enables them by default.

The full rehearsal waits for `library_repair` jobs before its integrity check. On a mechanical disk,
raise `STAGE_REPAIR_WAIT_ATTEMPTS` if canonical metadata rewriting legitimately takes over two hours.
`serve` mode does not wait; the Operations page shows repair and Kavita progress in the background.
The standalone `stage-check` prints archive-validation progress to stderr and one final JSON result
to stdout. It now fails fast with `"busy": true` while chapter downloads, `library_repair`, or
Kavita synchronization jobs are queued, leased, or waiting to retry; those jobs can change files or
projections during a slow scan. Wait for them to settle, briefly stop the worker, and run the check
from the web container for a stable result. Failure output includes counts and at most 25 paths per
category by default. Pass `--full-details` only when a complete machine-readable failure manifest is
needed.

Before the archive scan, run the bounded relational audit without touching CBZ contents:

```bash
docker exec manga-manager-stage-worker uv run --frozen manga-manager database-audit \
  --json --report /tmp/database-audit.json
```

It checks migration `0019` aggregates, provider uniqueness, projections, reading state, leases,
permits, reservations, workload cycles, source hygiene, and PostgreSQL table/index/dead-tuple
statistics under a short statement timeout.

Before deleting a staging catalog, use the preview-first reset workflow:

```bash
scripts/reset-local-data.sh preview
scripts/reset-local-data.sh archive
scripts/reset-local-data.sh apply --archive-dir local-archives/pre-reset-TIMESTAMP \
  --include-legacy --yes
```

The archive command stops writers, creates a custom PostgreSQL dump and redacted diagnostics, then
restores the dump into a disposable database. Apply refuses to run without the verified dump and
checksum. It removes only known project resources; the local archive remains ignored and must be
deleted manually after Raspberry Pi acceptance. `--include-legacy` also removes ignored legacy
SQLite/storage data and generated browser/build output, while preserving `.env`, `.venv`,
`frontend/node_modules`, and package caches.

After the reset, reclaim old global Docker build cache separately only if this host is not relying on
it for other projects: `docker builder prune --filter until=24h`. This is intentionally not part of
the reset script because Docker build cache is host-global rather than Manga Manager-owned.

Run frontend browser checks against the staged server with:

```bash
cd frontend
PLAYWRIGHT_BASE_URL=http://127.0.0.1:18000 npm run test:browser
```

For an unattended final rehearsal plus ARM64 image/runtime check, run
`scripts/final-validation.sh`. It uses isolated staging names, writes timestamped files under
`logs/`, continues to the ARM check if staging fails, captures container diagnostics, and cleans up
only its isolated staging containers and volume.

After a repair and Kavita sync completes, an individual series/chapter cover pair can be checked
byte-for-byte with the IDs shown by Kavita's API:

```bash
set -a
. .local/manga-manager-stage-kavita.env
set +a
UV_CACHE_DIR=/tmp/uv-cache uv run python scripts/kavita-cover-check.py \
  --url http://127.0.0.1:15000 --api-key "$KAVITA_API_KEY" \
  --series-id SERIES_ID --chapter-id CHAPTER_ID
```

The check fails if Kavita still exposes its generated first-page thumbnail for the chapter.
