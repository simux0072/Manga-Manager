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
series is not in Kavita yet, clicking the series queues the newest 3 missing chapters and returns to
Discovery with a pending state. Clicking a chapter queues that exact chapter first. The scheduler or
manual `Run download drain` action performs downloads, and `Run Kavita drain` performs scan/map work.
Use `Run next download` or `Run Kavita sync` when you only want to process one job.

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
- Manual queue drains, retry controls, source rescans, Want to Read sync, and retention cleanup.
- Activity history at `/activity` and an RSS feed at `/notifications/rss.xml`.

Retention cleanup is disabled by default. Set `RETENTION_REPLACED_DAYS` or
`RETENTION_REPLACED_MAX_PER_CHAPTER` to remove inactive replaced files during manual cleanup.

Notifications are disabled unless configured and are best-effort: delivery failures are logged but
do not fail downloads, syncs, polling, or cleanup. `NOTIFICATION_WEBHOOK_URL` posts important
activity events to a webhook. SMTP email requires `SMTP_HOST`, `SMTP_FROM`, and `SMTP_TO`;
username/password are optional for authenticated SMTP. Use `NOTIFICATION_TIMEOUT_SECONDS` to keep
external notification calls bounded.

## Discovery

The Discovery page is a two-column update list on desktop and one column on mobile. Each item shows
cover art, a truncated title, source badges, and the newest 2-3 known chapter releases with relative
ages. The cover/title area opens the series; chapter rows open the specific chapter.

MangaFire defaults to the latest/new updates feed via `MANGAFIRE_DISCOVERY_MODE=new`. Set
`MANGAFIRE_DISCOVERY_MODE=hot` only if you explicitly want MangaFire's hot feed.

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
- Source polling controls render. Only poll/download content you have the right to archive.

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

1. Poll a source or use an already discovered series.
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
