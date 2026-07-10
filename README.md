# Manga Manager

Private manga discovery, tracking, download, and Kavita import service.

This project is designed for personal use. It does not bypass paywalls, CAPTCHAs, accounts,
or other access controls. Use only where you have the right to archive the content.

## Quick Start

```bash
UV_CACHE_DIR=.uv-cache uv sync --extra dev
UV_CACHE_DIR=.uv-cache uv run uvicorn app.main:app --reload
```

Open <http://127.0.0.1:8000>.

When `DATABASE_URL` is unset, local development uses the built-in SQLite default
`sqlite:///./manga_manager.db`.

## Deployment Safety

Manga Manager is still designed for private deployments. It supports optional env-based Basic Auth,
but you should still run it on a private network or behind a reverse proxy before exposing it beyond
a trusted host.

Recommended reverse-proxy setup:

- Terminate HTTPS at Caddy, nginx, Traefik, or your existing home-lab proxy.
- Require proxy-level authentication such as Basic Auth, Authelia, OAuth2 Proxy, or Tailscale/Zero
  Trust access before traffic reaches the app.
- Forward only the app port to trusted clients; do not expose Postgres, Kavita, or the storage volume
  over the public internet.
- Keep the proxy request body limit large enough for normal pages, but downloads happen server-side,
  so no special upload tuning is usually needed.

Optional built-in auth:

```env
BASIC_AUTH_USERNAME=<username>
BASIC_AUTH_PASSWORD=<password>
AUTH_PROTECT_HEALTHZ=false
```

When username or password is set, the app protects HTML and mutating routes with HTTP Basic Auth.
Set both username and password together; an incomplete Basic Auth configuration is rejected.
Authenticated form posts include CSRF tokens. `/healthz` remains public by default for container
health checks; set `AUTH_PROTECT_HEALTHZ=true` if your deployment does not need unauthenticated
health probes.

## Docker

```bash
cp .env.example .env
docker compose up --build
```

Leave `DATABASE_URL` unset in `.env` for the default Docker Compose Postgres database. Set
`DATABASE_URL` only when you intentionally want to use a different database.

Database migrations run automatically on service startup using Alembic.
The Docker image includes a `/healthz` healthcheck; containers are unhealthy if scheduler creation,
startup, or job inspection fails.

For backups, keep both the Postgres volume and the `storage/` directory together. The database stores
metadata and file paths, while `storage/` contains covers, CBZ files, and replacement archives.

### PostgreSQL v2 staging

The v2 profile separates migration, web, worker, and PostgreSQL processes with Pi-oriented memory
limits. It publishes the v2 web application on port 8001:

```bash
docker compose --profile v2 up --build
```

On a host without Compose, the direct-Docker rehearsal builds, migrates, starts web/worker/database,
and runs the staged health checks:

```bash
scripts/stage-local.sh
scripts/stage-local.sh down
scripts/stage-local.sh down --volumes  # also delete the staging database
```

Set `STAGE_PLATFORM=linux/arm64` when ARM64 binfmt/QEMU support is installed. This rehearsal does not
change Raspberry Pi or Traefik state.

Set `STAGE_IMPORT_ROOT=/path/to/repaired-cbz` to import and reconcile a repaired library during the
rehearsal. Set `STAGE_SMOKE_SOURCE=asura`, `mangafire`, or `kingofshojo` only when a live provider
smoke pull is intended. Every run performs a logical dump/restore into a disposable database and
restarts web and worker containers before its final health check.

### Legacy audit and repair

Audit and the default repair mode are read-only. `--apply` creates a consistent SQLite backup before
applying deterministic normalizations; evidence-dependent splits and quarantines remain reported.

```bash
uv run manga-manager audit-legacy manga_manager.db --storage-root storage --report reports/audit.json
uv run manga-manager repair-legacy manga_manager.db --storage-root storage --report reports/dry-run.md
uv run manga-manager repair-legacy manga_manager.db --storage-root storage \
  --backup-dir backups --report reports/applied.json --apply
```

Reports contain stable action keys, row evidence, before/after values, rollback instructions, and a
SHA-256 storage manifest. A second applied run has no repeatable changes. V2 operational checks are
available through `migrate`, `doctor`, and `stage-check`.

Repair archives are retained by default. Delete only archives older than the 30-day rollback window:

```bash
uv run manga-manager cleanup-repair-archives storage --retain-days 30
```

`benchmark-workers` reports the effective pool limits. Asura concurrency two must be explicitly
requested and is automatically reduced to one while its shared source state is in cooldown:

```bash
uv run manga-manager benchmark-workers --asura-concurrency 2
```

Backup checklist:

1. Stop Manga Manager or pause scheduled work from your orchestrator.
2. Dump Postgres with `pg_dump` or snapshot the Compose Postgres volume.
3. Copy the entire `storage/` directory at the same point in time.
4. Keep the database dump/snapshot and `storage/` copy as one backup set.

Restore checklist:

1. Restore the Postgres dump or volume before starting the app.
2. Restore `storage/` to the same host path or update `LIBRARY_ROOT`, `STAGING_ROOT`, and
   `ARCHIVE_ROOT` consistently.
3. Start Manga Manager and check `/healthz` or the Library Operations section.
4. If Kavita paths changed, update `KAVITA_LIBRARY_ROOT` and run Kavita sync from the Library page.

For Raspberry Pi 4 deployments, use a 64-bit OS and keep the Postgres data, Kavita data, and
`storage/` directory on an external SSD. The app keeps download work sequential by default and
prefers targeted Kavita folder scans so it remains responsive on small hardware.

## Kavita

Set `KAVITA_URL` and `KAVITA_API_KEY` in `.env`. Chapters are written as CBZ files under
`LIBRARY_ROOT`, using one folder per series. After a successful download, Manga Manager queues a
Kavita sync job that scans the affected series folder and maps local series/chapters to Kavita IDs
when possible.

If Kavita is configured after chapters have already been downloaded, `Run Kavita sync` and the
scheduled Kavita drain discover tracked series with downloaded but unmapped chapters and queue sync
jobs for them. If Kavita is later unconfigured, queued sync jobs remain queued instead of being
skipped, so they can run after configuration is restored. Sync jobs prefer the downloaded chapter's
folder for targeted scans and fall back to the expected `LIBRARY_ROOT/Manga/<series>` folder.

If Kavita sees the same files at a different path, set `KAVITA_LIBRARY_ROOT` to Kavita's view of
`LIBRARY_ROOT`. For example, Manga Manager can write to `/data/library` while Kavita has that same
SSD mounted as `/manga`; in that case use `LIBRARY_ROOT=/data/library` and
`KAVITA_LIBRARY_ROOT=/manga`. If both containers use the same mount path, leave
`KAVITA_LIBRARY_ROOT` unset.

Path-mapping troubleshooting:

- If sync jobs finish but series stay unmapped, confirm Kavita can see the CBZ files under the path
  shown in the sync job folder.
- If Manga Manager writes `/data/library/Manga/Example` and Kavita scans `/manga/Manga/Example`, set
  `KAVITA_LIBRARY_ROOT=/manga`.
- If both containers mount the same folder at `/library`, use `LIBRARY_ROOT=/library` and leave
  `KAVITA_LIBRARY_ROOT` blank.
- After changing paths, run `Run Kavita drain` from Library to rescan immediately runnable work.

Discovery cards link into Kavita when a series or chapter is already mapped. If a newly discovered
series is not in Kavita yet, clicking the bookmark/download control queues the newest configured
missing chapters first, then lower-priority backfill jobs for older chapters. Clicking a chapter
queues that exact chapter first. The scheduler or manual `Run download drain` action performs
downloads, and `Run Kavita drain` performs scan/map work.
Use `Run next download` or `Run Kavita sync` when you only want to process one job.

The `New` page lists unread downloaded chapters for tracked series, newest first. When Kavita has
mapped a chapter, the row includes a Kavita reader link; otherwise it still links to the local open
route and will map after the next successful Kavita scan. Cover images are used as series/CBZ
metadata only; Manga Manager does not create fake chapter archives from covers when page extraction
fails.

Optional Kavita settings:

- `KAVITA_SYNC_WANT_TO_READ`: enables scheduler-oriented Want to Read intent sync when wired into
  scheduled work; the Library page also has a manual `Sync Want to Read` action.
- `KAVITA_LIBRARY_ROOT`: optional Kavita-visible path for files under `LIBRARY_ROOT`.
- `KAVITA_SERIES_URL_TEMPLATE`: controls series deep links.
- `KAVITA_CHAPTER_URL_TEMPLATE`: controls chapter reader deep links.

## Library Operations

The Library page includes operational controls and local reading state:

- Filters for ready, pending sync, failed, unread, reading, and caught-up series.
- List and cover-wall views.
- Per-series and per-chapter progress controls that work without Kavita.
- Manual queue drains, retry controls, source rescans, Want to Read sync, repair, and retention cleanup.
- Activity history at `/activity` and an RSS feed at `/notifications/rss.xml`.

`Repair known manga` runs a non-destructive repair pass. It rescans known source series, prioritizing
tracked manga, refreshes detail metadata and aliases, removes legacy bad placeholder chapter rows
that have no downloaded files, normalizes obviously polluted titles, refreshes missing or broken
covers, regenerates pending cross-source match candidates, and recovers stale download jobs. Use it
after parser or matcher fixes, or after importing data created by an older version. It does not flush
or rebuild the database.

`Rebuild matches` regenerates text, alias, and description-based candidates without doing a metadata
rescan. `Build visual fingerprints` extracts conservative perceptual hashes from locally downloaded
CBZ pages, and `Rebuild visual matches` uses those cached fingerprints to add pending candidates with
reason `visual chapter match`. Visual matches are never auto-merged.

`Recover stale jobs` requeues old `running` download jobs whose heartbeat is older than
`DOWNLOAD_STALE_MINUTES`. Use it when a worker was interrupted and jobs remain stuck as running.
Interrupted source pulls are handled automatically on application startup: any `queued` or `running`
source pull job left behind by a restart is reset to the beginning and scheduled again. Scheduled
source polls still recover truly stale active pull rows before deciding whether to skip a duplicate
poll.

The job drawer is intentionally compact and only shows recent grouped work. The Info page is the
authoritative operations view: it shows true queued/running/delayed/failed/completed counts and a
pageable in-place browser for active, failed, and completed job sections. Switching Info job pages
uses JSON and does not refresh the page or jump back to the top.

Source cooldowns appear in Info when a website or CDN rate-limits downloads. Rate-limited jobs are
delayed until `Retry-After` when the source provides it, or until the configured fallback cooldown
expires. During cooldown, Manga Manager can temporarily download the same chapter from a lower
priority source and later replace it with the higher-priority source.

Download scheduling is source- and series-aware. `DOWNLOAD_CONCURRENCY` limits total running
download jobs, `DOWNLOAD_PER_SERIES_CONCURRENCY` prevents one manga from occupying every worker, and
`ASURA_DOWNLOAD_CONCURRENCY`, `MANGAFIRE_DOWNLOAD_CONCURRENCY`, and
`KINGOFSHOJO_DOWNLOAD_CONCURRENCY` cap each website independently. Asura defaults to a conservative
single job, while MangaFire and KingOfShojo can run more work in parallel. Page image fetching is
also bounded per source with `ASURA_PAGE_CONCURRENCY`, `MANGAFIRE_PAGE_CONCURRENCY`, and
`KINGOFSHOJO_PAGE_CONCURRENCY`; fetched images are still written to CBZ files in page order. Running
downloads update a heartbeat while pages are staged so healthy long downloads are not treated as
stale.
`DOWNLOAD_DRAIN_INTERVAL_MINUTES` controls how often the scheduler drains queued downloads.

Retention cleanup is disabled by default. Set `RETENTION_REPLACED_DAYS` or
`RETENTION_REPLACED_MAX_PER_CHAPTER` to remove inactive replaced files during manual cleanup.

Notifications are disabled unless configured and are best-effort: delivery failures are logged but
do not fail downloads, syncs, polling, or cleanup. `NOTIFICATION_WEBHOOK_URL` posts important
activity events to a webhook. SMTP email requires `SMTP_HOST`, `SMTP_FROM`, and `SMTP_TO`;
username/password are optional for authenticated SMTP. Use `NOTIFICATION_TIMEOUT_SECONDS` to keep
external notification calls bounded.

## Discovery

The Discovery page is a two-column update list on desktop and one column on mobile. Each item shows
cover art, a truncated title, source badges, combined stats when available, a short description,
aliases/genres, and the newest 2-3 known chapter releases with relative ages. The cover/title area
opens the series; chapter rows open the specific chapter. Bookmark/follow counts are summed across
merged sources. Rating uses the highest-priority source that has a rating rather than averaging
different sites.

Source tabs filter Discovery server-side and show counts for `All`, Asura, MangaFire, and
KingOfShojo. `All` interleaves recent series by source so a very active source cannot dominate the
first page. The `Visible websites` checkboxes still hide source badges, chapter rows, and any
Discovery or Library card with no remaining visible source rows client-side.

Potential Matches only suggests cross-source matches. Exact external IDs and exact normalized titles
can still auto-match, but weak title matches are kept for manual review. The matcher also boosts
manual candidates when different sources share distinctive title tokens or aliases, which helps
catch titles with different translations.

Manual source refresh actions are labeled `Pull`. Pulls run in the background and the top bar shows
pull progress. Discovery is paginated with a `Load more` control using `DISCOVERY_PAGE_SIZE`.

Source discovery uses adaptive frontier scanning. Before each poll, Manga Manager takes the newest
known source rows as sentinels and keeps paging until enough sentinels are seen with no newer
chapter, or the source hard cap is reached. Configure this with `SOURCE_FRONTIER_SENTINELS`,
`SOURCE_FRONTIER_REQUIRED_HITS`, `ASURA_RECENT_PAGES`, `MANGAFIRE_RECENT_PAGES`, and
`KINGOFSHOJO_RECENT_PAGES`. Empty sources scan a small initial frontier.

MangaFire defaults to the latest/new updates feed via `MANGAFIRE_DISCOVERY_MODE=new`. Discovery uses
the current homepage/latest-update cards and supports `/manga/<slug>.<id>` links as well as legacy
`/title/...` links; detail and chapter API calls happen later during enrichment/rescan so item-level
MangaFire throttling does not hide the whole updated list. Set `MANGAFIRE_DISCOVERY_MODE=hot` only if
you explicitly want MangaFire's hot feed for the legacy API parser/tests.

MangaFire is treated as English-only. MangaFire series with no English chapters are skipped during
Discovery so they do not appear as broken cards.

## Rate Limits And Retries

Source adapters respect per-source request intervals. Asura defaults to a conservative request
interval because its CDN may return `429 Too Many Requests` during bursts. Relevant settings:

- `ASURA_REQUEST_INTERVAL_SECONDS`: delay between Asura HTTP requests.
- `DOWNLOAD_CONCURRENCY`: maximum total running chapter download jobs.
- `DOWNLOAD_PER_SERIES_CONCURRENCY`: maximum running download jobs for a single canonical series.
- `DOWNLOAD_DRAIN_INTERVAL_MINUTES`: scheduler interval for draining queued download jobs.
- `ASURA_DOWNLOAD_CONCURRENCY`, `MANGAFIRE_DOWNLOAD_CONCURRENCY`,
  `KINGOFSHOJO_DOWNLOAD_CONCURRENCY`: per-source running job caps.
- `ASURA_PAGE_CONCURRENCY`, `MANGAFIRE_PAGE_CONCURRENCY`, `KINGOFSHOJO_PAGE_CONCURRENCY`: per-source
  page image fetch caps.
- `RATE_LIMIT_COOLDOWN_MINUTES`: default cooldown when a non-Asura source is rate-limited without a
  `Retry-After` header.
- `ASURA_RATE_LIMIT_COOLDOWN_MINUTES`: default Asura cooldown when no `Retry-After` header exists.
- `JOB_STATUS_GROUP_LIMIT`: number of grouped jobs shown per status slice in the live jobs API.

Rate-limit errors do not consume normal download attempts. Temporary content/CDN errors such as
incomplete image bodies, invalid image bytes, or too few page images delay the current job and can
queue a lower-priority fallback source for the same chapter. Temporary CDN/server failures also set a
source download cooldown so other ready providers can use workers while the source recovers. Asura
reader extraction supports both `asura-images/chapters/` and older
`asura-images/chapters-stitched/` page URLs.

Individual incomplete page bodies are retried up to three times before the chapter job is delayed.
Running jobs older than `DOWNLOAD_STALE_MINUTES`, or running jobs already carrying temporary
partial-body/network error text, are recovered by startup, Info `Recover stale jobs`, queueing, and
repair.

## Local Testing

Start with the automated checks:

```bash
UV_CACHE_DIR=.uv-cache uv sync --extra dev
UV_CACHE_DIR=.uv-cache uv run pytest -q
UV_CACHE_DIR=.uv-cache uv run ruff check .
UV_CACHE_DIR=.uv-cache uv run python -m compileall -q app tests
git diff --check
```

You can test most of the service without Kavita. Leave `KAVITA_URL` and `KAVITA_API_KEY` empty, run
the app, and open Discovery, Library, and `/healthz`:

```bash
UV_CACHE_DIR=.uv-cache uv run uvicorn app.main:app --reload
```

Expected no-Kavita behavior:

- `/healthz` returns scheduler, source, download job, and Kavita configuration state.
- Library shows Today and Operations panels.
- Library shows Kavita as not configured.
- `Run Kavita sync` and `Run Kavita drain` do not crash; queued sync jobs remain available for later.
- Discovery, Matches, Info pagination, source pulls, local downloads, repair, match rebuild, and
  stale recovery all work without Kavita.
- Source pull controls render. Only pull/download content you have the right to archive.

To test the Kavita integration end to end, run Kavita separately and mount this repo's
`storage/library` into Kavita as `/manga`. Add a Kavita Manga library pointed at `/manga/Manga` or
`/manga`, then set:

```env
LIBRARY_ROOT=./storage/library
KAVITA_LIBRARY_ROOT=/manga
KAVITA_URL=http://127.0.0.1:5000
KAVITA_API_KEY=<your-api-key>
```

Restart Manga Manager after editing `.env`. Then:

1. Pull a source or use an already discovered series.
2. Mark one series `Interested` or `Reading`.
3. Click `Queue downloads`, then `Run download drain`.
4. Confirm a CBZ appears under `storage/library/Manga/<series>/`.
5. Click `Run Kavita drain`.
6. Confirm Library's `Downloaded` count increases when CBZs exist and `Ready` increases after Kavita
   maps the series.
7. Confirm `Pick something` links only after a series is Kavita-mapped; downloaded but unmapped
   content shows `Downloaded, awaiting Kavita sync`.

If Manga Manager runs in Docker instead of host uvicorn, `KAVITA_URL=http://127.0.0.1:5000` points
inside the Manga Manager container, not at host Kavita. Put both services on the same Compose network
and use a service URL such as `http://kavita:5000`, or use a host-reachable URL appropriate for your
Docker setup.

## Checks

```bash
UV_CACHE_DIR=.uv-cache uv run pytest -q
UV_CACHE_DIR=.uv-cache uv run ruff check .
UV_CACHE_DIR=.uv-cache uv run python -m compileall -q app tests
git diff --check
```
