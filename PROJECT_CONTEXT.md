# Manga Manager Project Context

Last updated: 2026-07-10

## PostgreSQL v2 transition

- `manga_manager/` contains the durable PostgreSQL catalog, leased queue, workers,
  content-addressed storage, migrations, CLI, and separate FastAPI/Jinja v2 web surface.
- V2 discovery uses an indexed ID cursor and database-side filtering. Delta SSE emits `job` and
  `counts` events; heartbeats are comments.
- `audit-legacy` is read-only. `repair-legacy` defaults to dry-run and requires `--apply`; applied
  runs create a SQLite backup before deterministic changes.
- `scripts/stage-local.sh` is the local rehearsal path when Compose is unavailable. Pi and Traefik
  cutover remains deferred.
- PostgreSQL jobs carry indexed source, series, and pool routing. Renewable permit leases enforce
  provider and global capacities across worker processes, while a partial unique index and advisory
  lock enforce one leased chapter job per canonical series.
- The v2 web application includes Discovery, Library, Updates, Matches, Activity, and Operations.
  Match acceptance validates both complete provider sets before transactional reassignment.

## Purpose

Manga Manager is a private personal-use manga discovery, tracking, download, and Kavita import
service. It discovers recent chapters from configured sources, groups releases into canonical series,
lets the user decide which series to track, downloads eligible chapters as CBZ files, and optionally
syncs downloaded series and chapters into Kavita after successful downloads.

The project is explicitly not intended to bypass paywalls, CAPTCHAs, accounts, premium locks, or other
access controls. It should only be used where the user has the right to archive the content.

## Current Product Shape

The application is a FastAPI server with server-rendered HTML pages:

- `/` shows newly discovered series as a compact update list with covers, recent chapters, and source
  health.
- `/library` shows a Today panel, tracked/interested series, source filters, and local reading state.
- `/new-chapters` shows unread downloaded chapters for tracked series, with local and Kavita links
  when available.
- `/info` shows operations health, source cooldowns, live job counts, grouped job details, and
  maintenance actions.
- `/matches` shows possible cross-source series matches for manual merge/separation.
- `/healthz` returns JSON with app, scheduler, and source health.
- POST routes handle polling, queueing downloads, running downloads, retrying failed jobs, status
  changes, and match decisions.
- GET routes handle Kavita-oriented series/chapter opening. Mapped items redirect to Kavita; unmapped
  items queue focused imports and return with a pending state if Kavita is not ready.

The UI is intentionally small and operational. The backend services contain most of the project logic.

## Main Plan

The high-level plan for the project is:

1. Discover recent manga/manhwa series from enabled source adapters.
2. Normalize and merge source-specific series records into canonical `Series` rows.
3. Detect chapters and choose the best source by configured priority.
4. Let the user mark series as `reading` or `interested`.
5. Queue download jobs only for tracked series and best-source releases.
6. Download eligible chapters, write CBZ files, archive replaced files when configured, and record file
   metadata.
7. Trigger targeted Kavita folder scanning after successful downloads when Kavita is configured.
8. Map local series and chapters to Kavita IDs so Discovery can deep-link to Kavita series and
   chapter reader pages.
9. Keep polling and download processing running through APScheduler.
10. Provide enough health information to operate the service on a server.

## Project Layout

- `app/main.py`: FastAPI app, lifespan startup/shutdown, routes, templates, health endpoint.
- `app/settings.py`: Pydantic settings loaded from `.env`.
- `app/db.py`: SQLAlchemy engine/session setup, Alembic startup migration handling, SQLite
  compatibility migration helpers.
- `app/models.py`: SQLAlchemy models.
- `app/domain.py`: Dataclasses and normalization/priority helpers.
- `app/services.py`: Core application workflows for merging series, polling, queueing/running
  downloads, writing CBZs, match handling, stale job recovery, and metadata merging.
- `app/scheduler.py`: APScheduler jobs for source polling and download draining.
- `app/kavita.py`: Kavita API client for scanning, series/chapter lookup, Want to Read endpoints, and
  deep-link URL rendering.
- `app/adapters/`: Source adapter implementations and shared HTTP/parsing helpers.
- `app/templates/`: Jinja templates.
- `app/static/`: CSS.
- `alembic/`: Alembic migration environment and revisions.
- `tests/`: Unit/regression tests for services, adapters, domain logic, Kavita, DB migrations, and
  health/lifespan behavior.

## Runtime Architecture

On startup, `app.main.lifespan`:

1. Runs `init_db()`.
2. Recovers stale `running` download and Kavita sync jobs.
3. Resets interrupted `queued` or `running` source pull jobs and schedules them to restart from the
   beginning.
4. Attempts to create the scheduler and records any creation failure.
5. Schedules scheduler startup on the current event loop when creation succeeds.
6. Shuts the scheduler down on app shutdown.

`init_db()` runs Alembic migrations for normal database URLs. In-memory SQLite is special-cased for
tests and uses `Base.metadata.create_all()`.

The scheduler:

- Polls Asura, MangaFire, and King of Shojo at configurable intervals when enabled.
- Recovers stale active source pull rows before creating scheduled pull jobs, so old in-process rows
  cannot make scheduled polling skip forever.
- Calls `queue_downloads()` after each scheduled source poll.
- Periodically drains queued downloads by repeatedly calling `run_next_download()`.
- Periodically discovers pending Kavita sync work for tracked downloaded/unmapped series, then drains
  queued Kavita sync jobs by repeatedly calling `run_next_kavita_sync()`.

## Data Model Summary

- `Series`: canonical user-facing series.
- `SourceSeries`: a source-specific series row linked to one canonical `Series`.
- `Chapter`: canonical chapter row for a series and normalized chapter number.
- `ChapterRelease`: one source-specific release for a chapter.
- `DownloadJob`: one job per chapter release; `chapter_release_id` is unique.
- `SourcePullJob`: one source scan job. `queued` and `running` are active statuses and are unique per
  source.
- `DownloadedFile`: historical file metadata for downloaded CBZs.
- `SourceHealth`: per-source enabled state, last poll time, last error, and failure count.
  Download cooldown fields track source/CDN rate limits separately from poll health.
- `ManualMatchRule`: records manual merge/separate decisions.
- `MatchCandidate`: possible source-series-to-series matches pending user review.
- `ChapterFingerprint`: perceptual image segment hashes built from downloaded CBZ pages for
  conservative visual match candidates.

Kavita mapping fields:

- `Series.kavita_series_id`, `Series.kavita_library_id`, `Series.kavita_synced_at` store the mapped
  Kavita series and library.
- `Chapter.kavita_chapter_id`, `Chapter.kavita_volume_id`, `Chapter.kavita_mapped_at` store the
  mapped Kavita chapter/volume after a Kavita folder scan and series-detail lookup.

Important uniqueness constraints:

- `SourceSeries`: `(source, source_id)`.
- `Chapter`: `(series_id, number)`.
- `ChapterRelease`: `(source_series_id, number)`.
- `DownloadJob`: `chapter_release_id`.
- `MatchCandidate`: `(source_series_id, candidate_series_id)`.

## Source Priority

Source replacement priority is defined in `app/domain.py`:

- `asura`: 100
- `mangafire`: 50
- `kingofshojo`: 10

Higher priority sources can replace lower priority sources for `Chapter.best_source` and downloaded
files.

Download fallback keeps this order per chapter. The best available source is tried first. If that
source has a failed job at `MAX_DOWNLOAD_ATTEMPTS`, the queue may select the next lower-priority
available release for the same chapter. If the best source is temporarily rate-limited, the source
is put on download cooldown and lower-priority releases may be used until the cooldown expires. When
the higher-priority source becomes available again, later queueing can replace lower-priority files.

## Settings

Settings are loaded from `.env` through `app/settings.py`.

Important settings:

- `DATABASE_URL`: defaults to `sqlite:///./manga_manager.db` in app settings when unset. Docker
  Compose uses its Postgres fallback when `.env` does not explicitly set this value.
- `LIBRARY_ROOT`: final library output root.
- `STAGING_ROOT`: reserved temporary workspace setting. Final CBZ replacement is staged beside the
  destination file so `Path.replace()` stays atomic across deployment layouts.
- `ARCHIVE_ROOT`: replaced-file archive location.
- `KAVITA_URL`, `KAVITA_API_KEY`: optional Kavita integration.
- `KAVITA_SYNC_WANT_TO_READ`: reserved toggle for syncing Kavita Want to Read intent.
- `KAVITA_SERIES_URL_TEMPLATE`: configurable Kavita series deep-link template.
- `KAVITA_CHAPTER_URL_TEMPLATE`: configurable Kavita chapter reader deep-link template.
- `ENABLE_ASURA`, `ENABLE_MANGAFIRE`, `ENABLE_KINGOFSHOJO`: source toggles.
- `ASURA_POLL_MINUTES`, `MANGAFIRE_POLL_MINUTES`, `KINGOFSHOJO_POLL_MINUTES`: scheduler intervals.
- `ASURA_DELAY_HOURS`: delay for Asura releases that may still be premium/locked.
- `REQUEST_TIMEOUT_SECONDS`: normal HTTP timeout.
- `MAX_PAGE_BYTES`: per-page image byte cap for downloads.
- `MAX_COVER_BYTES`: per-cover image byte cap.
- `*_REQUEST_INTERVAL_SECONDS`: per-source request pacing. Asura defaults to conservative pacing.
- `RATE_LIMIT_COOLDOWN_MINUTES`: default source cooldown when a 429 has no `Retry-After`.
- `ASURA_RATE_LIMIT_COOLDOWN_MINUTES`: Asura-specific 429 cooldown fallback.
- `JOB_STATUS_GROUP_LIMIT`: grouped jobs shown per live status slice before Info reports more exist.
- `DOWNLOADS_ENABLED`: global download queue toggle.
- `MIN_PAGES_PER_CHAPTER`: rejects suspicious downloads with too few pages.
- `MAX_DOWNLOAD_ATTEMPTS`: retry limit.
- `DOWNLOAD_RETRY_BASE_MINUTES`: exponential backoff base.
- `DOWNLOAD_STALE_MINUTES`: stale `running` job recovery threshold.
- `KEEP_REPLACED_FILES`: archive old CBZs before replacement.
- `MANGAFIRE_DISCOVERY_MODE`: defaults to `new`; set to `hot` only to request MangaFire's hot feed.
- `MANGAFIRE_RECENT_LIMIT`: number of MangaFire recent titles to request.
- `SOURCE_FRONTIER_SENTINELS`, `SOURCE_FRONTIER_REQUIRED_HITS`: adaptive source-paging stop
  controls.
- `ASURA_RECENT_PAGES`, `MANGAFIRE_RECENT_PAGES`, `KINGOFSHOJO_RECENT_PAGES`: hard source-paging
  caps.
- `VISUAL_MATCH_*`: manual visual fingerprint and visual match thresholds.
- `DOWNLOAD_CONCURRENCY`: total running download job cap.
- `DOWNLOAD_PER_SERIES_CONCURRENCY`: running download cap for one canonical series.
- `ASURA_DOWNLOAD_CONCURRENCY`, `MANGAFIRE_DOWNLOAD_CONCURRENCY`,
  `KINGOFSHOJO_DOWNLOAD_CONCURRENCY`: per-source running download caps.
- `ASURA_PAGE_CONCURRENCY`, `MANGAFIRE_PAGE_CONCURRENCY`, `KINGOFSHOJO_PAGE_CONCURRENCY`:
  source-wide page image request caps.

`.env.example` includes the runtime safety settings currently defined in `app/settings.py`, but does
not set an active `DATABASE_URL` by default.

## Database And Migrations

Alembic is the authoritative migration path for normal persistent databases.

Current behavior:

- Fresh DBs run Alembic normally.
- Existing SQLite DBs without `alembic_version` keep a compatibility path for local/prototype use.
- Existing non-SQLite DBs without `alembic_version` fail loudly instead of being blindly stamped as
  current. This is intentional: server databases should be migrated explicitly or manually inspected
  before stamping.
- SQLite compatibility migrations add missing columns, populate `first_seen_at`, remove duplicate
  `download_job` rows for the same release, and add a unique index on
  `download_job.chapter_release_id`.
- Versioned DBs have an Alembic revision that deduplicates download jobs and creates the unique
  download-job index if the DB was already upgraded before that invariant existed.
- Later Alembic revisions add Kavita mapping columns, source-series metadata, and source download
  cooldown fields.

## Download Workflow

`queue_downloads(session)`:

1. Returns immediately if downloads are disabled.
2. Recovers stale running jobs.
3. Finds tracked series with status `reading` or `interested`.
4. Finds eligible chapter releases for those series.
5. Queues the selected source release for each chapter. Selection follows source priority, skips
   sources currently in download cooldown, and can fall back after content failures.
6. Handles uniqueness races gracefully if another queue attempt already created the job.

`run_next_download(session)`:

1. Recovers stale running jobs.
2. Atomically claims one due queued or due delayed job with a conditional `queued/delayed -> running`
   update.
3. Revalidates that the release is still due, still the chapter best source, and can replace any
   existing downloaded source.
4. Marks obsolete jobs as `skipped`, future-due jobs as `delayed`, and removes orphaned jobs whose
   release/chapter no longer exists.
5. Builds a `ChapterItem`.
6. Streams page bytes from the source adapter into a same-directory temporary CBZ with job/release
   identity in the staging filename.
7. Rejects corrupt or non-image page bytes and chapters below `MIN_PAGES_PER_CHAPTER`.
8. Archives the old CBZ when replacement is configured.
9. Atomically replaces the final file after the staged CBZ passes validation.
10. Updates `Chapter`, `ChapterRelease`, `DownloadedFile`, and `DownloadJob`.
11. Triggers Kavita folder scan after successful download and refreshes local Kavita series/chapter
    mappings.
12. Applies delay/retry/failure state depending on the exception.

HTTP 429s are represented as source rate limits, not content failures. They do not consume normal
download attempts. The source receives a cooldown from `Retry-After` or the configured fallback, the
current job becomes `delayed`, and a lower-priority fallback release is queued when available.
Temporary content/CDN errors such as incomplete image bodies, invalid image bytes, or too few pages
also delay the current job without consuming a normal attempt and can queue a lower-priority
fallback release. Cover images are used for metadata/covers only, not as fake chapter pages.

The download workflow does not keep every page in memory at once. Each page is fetched, validated by
size and image decodability, and written to the staged CBZ before the next page is fetched. Temporary
CBZs live beside their final path to avoid cross-filesystem rename failures.

Focused imports:

- Clicking an unmapped series in Discovery marks it `interested`, queues the newest 3 missing
  chapters, and returns to Discovery with a pending notice.
- Clicking an unmapped chapter row queues that exact chapter first and returns to Discovery with a
  pending notice.
- The scheduler or manual Library queue-drain controls perform downloads and Kavita scan/map work.

## Polling Workflow

`poll_source(session, source)`:

1. Gets only the requested enabled source adapter.
2. Loads or creates `SourceHealth`.
3. Skips disabled sources before constructing an adapter.
4. Fetches recent source items with adaptive frontier scanning when the adapter supports it.
5. For each item, merges series metadata, caches cover image, fetches chapters, and upserts releases.
6. Handles item-level failures without aborting the whole source poll.
7. Records partial item failures as warning health/activity instead of clean success.
8. Disables the source after five consecutive source-level or all-item failures.

Configured sources can be manually disabled or re-enabled from the discovery health row. Disabling
prevents adapter construction. Re-enabling clears the last error and resets consecutive failures.

`poll_all_sources(session)` attempts each enabled source independently. A failing source does not stop
remaining sources from being polled.

## Discovery And Kavita Click Workflow

Discovery is optimized for scanning recent updates:

- Desktop uses a two-column update-card layout; mobile collapses to one column.
- Each card shows cover art, a single-line truncated title, source badges, optional source metrics,
  description excerpt, aliases/genres, and the newest 2-3 distinct chapter releases.
- Chapter rows show chapter number/title, source, and relative age such as `5h ago`, `1d ago`, or an
  absolute date.
- The cover/title area links to `/series/{series_id}/open`.
- Chapter rows link to `/chapters/{chapter_id}/open`.
- Buttons for `Reading`, `Interested`, and `Ignore` remain separate form controls.

The `Visible websites` toggles hide source badges and chapter rows first, then hide a card when no
visible source rows remain. Discovery also has server-side source tabs with counts, and the `All`
view interleaves recent series by source while preserving recency for the first item from each
source. MangaFire remains English-only; MangaFire titles with no English chapters are skipped during
discovery.

## Operations And Repair

The top-bar jobs drawer is a compact status surface. It groups queued/running/delayed, completed,
and failed work, preserves expanded section state across SSE refreshes, and caps completed/failed
rows for readability. `/info` is the authoritative job view: it reports true status counts even when
the drawer rows are capped. Info job sections use in-place pagination for active, failed, and
completed grouped jobs; page switches fetch JSON and do not refresh the browser page.

`repair_known_series(session)` backs the Info `Repair known manga` action. It:

1. Runs legacy bad discovery cleanup.
2. Removes bad placeholder chapter rows such as template `{{number}}`, `first chapter`, or
   `latest chapter` when they have no downloaded files.
3. Rescans known source-series rows, prioritizing tracked series.
4. Refreshes source detail metadata, visibly polluted titles, and missing or broken covers.
5. Regenerates pending match candidates for existing source-series rows while preserving exact
   keep-separate rules.
6. Recovers stale running download jobs.
7. Records a repair activity event.

The Info page also exposes `Recover stale jobs`, which requeues old running download jobs without
running the full repair pass.

Kavita behavior:

- If a series is already mapped, `/series/{series_id}/open` redirects to the configured Kavita series
  URL.
- If a chapter is already mapped, `/chapters/{chapter_id}/open` redirects to the configured Kavita
  chapter reader URL.
- If not mapped, Manga Manager queues a focused import and shows a pending notice when the import or
  Kavita scan cannot complete within a short request window.
- No placeholder CBZs are created for undiscovered or undownloaded manga.

MangaFire behavior:

- Default discovery uses current latest/new card HTML parsing with `/manga/<slug>.<id>` and legacy
  `/title/...` link support, controlled by adaptive frontier settings and `MANGAFIRE_RECENT_PAGES`.
- Per-title detail API calls are not made during discovery; enrichment/rescan handles details later.
- Updated-list chapter rows are kept as a fallback when chapter APIs are throttled or incomplete.
- `hot` remains opt-in through `MANGAFIRE_DISCOVERY_MODE=hot` for legacy API parsing/tests.

Visual matching:

- `Build visual fingerprints` reads active local CBZ downloads and stores perceptual hashes for
  multiple overlapping page segments.
- It skips configured first/last pages, extreme aspect ratios, and low-detail images to reduce
  translator/intro/credit noise.
- `Rebuild visual matches` compares cached fingerprints and creates pending `MatchCandidate` rows
  with reason `visual chapter match` only when multiple non-adjacent segment hits are found.
- Visual matches never auto-merge; they are manual review signals.

## Kavita Integration

`app.kavita.KavitaClient` uses Kavita's Auth Key API style with the `x-api-key` header.

Implemented endpoints:

- `/api/Plugin/authkey-expires`
- `/api/Library/scan-folder`
- `/api/Library/scan-all`
- `/api/Series/all-v2`
- `/api/Series/series-detail`
- `/api/want-to-read/v2`
- `/api/want-to-read/add-series`
- `/api/want-to-read/remove-series`

After downloads, Manga Manager prefers `scan-folder` for the affected series folder and falls back to
`scan-all` if folder scanning fails. Mapping uses stored Kavita ID first, then folder path, then
external IDs, then a unique normalized title match. Chapter mapping uses normalized chapter numbers
from Kavita series detail.

Kavita sync jobs are also discovered when the user clicks `Run Kavita sync` or when the scheduled
Kavita drain runs. Discovery is limited to tracked series with status `reading` or `interested` that
have downloaded chapters missing Kavita chapter IDs. This covers the case where chapters were
downloaded before Kavita was configured. Sync jobs store a folder path for targeted scans and folder
based Kavita series matching: an explicit fresh-download folder is preferred, otherwise the first
downloaded chapter's CBZ parent is used, with `LIBRARY_ROOT/Manga/<series>` as a final fallback.

`run_next_kavita_sync()` checks Kavita configuration before claiming work. If Kavita becomes
unconfigured after jobs are queued, queued jobs remain `queued` and can run when configuration is
restored. The retry action also recovers older skipped jobs whose error is `Kavita is not configured`.
The per-job Kavita sync retry button is a manual operator override and can requeue any displayed
failed or skipped sync job.

Kavita URL templates are configurable because Kavita UI routes can change between versions.

## Metadata Merge Behavior

When a source item is refreshed:

- Source row title and URL are updated.
- Source row cover, description, aliases, genres, popularity, and external IDs are refreshed/merged.
- Parent `Series` metadata is also refreshed/merged.

When a newly discovered source row matches an existing series:

- Cover and description are updated when the incoming item has values.
- Aliases and genres are merged.
- Popularity takes the maximum known value.
- External IDs are merged.

This is important because metadata may arrive later from richer source detail endpoints.

Cached cover files are keyed by source series and cover URL hash. If a source reports a new cover URL,
the service downloads the new cover into a same-directory temp file, atomically replaces the final
cover, and updates `cover_path`; if that download or write fails, the previous cached cover remains
active. Cached cover bytes are validated as real images before the cover path is updated.

## HTTP Adapter Behavior

`HttpSourceClient`:

- Reuses one `httpx.AsyncClient` per adapter instance.
- Sends the configured user agent.
- Follows redirects.
- Supports optional per-source throttle delays.
- Streams image downloads and enforces `MAX_PAGE_BYTES`.
- Cover image downloads are streamed and enforce `MAX_COVER_BYTES`.
- Rejects non-image content for page downloads.
- Retries partial/incomplete page body failures up to three times before surfacing a temporary
  download error.
- Applies source-wide page semaphores so job concurrency multiplied by page concurrency does not
  create unbounded CDN requests.
- Exposes `aclose()`; service code closes adapter clients after poll/download use.

Adapters currently implemented:

- `AsuraAdapter`
- `MangaFireAdapter`
- `KingOfShojoAdapter`

## Health And Operations

`/healthz` returns:

- `ok`: false if scheduler creation, startup, or job inspection failed.
- `scheduler.configured`
- `scheduler.running`
- `scheduler.start_scheduled`
- `scheduler.create_error`
- `scheduler.start_error`
- `scheduler.jobs`
- `scheduler.jobs_error`
- `enabled_sources[]`
- `sources[]` with source name, enabled state, last poll time, last error, and consecutive failures.
- `runtime` download caps for global, per-series, and per-source scheduling.
- `download_jobs` with counts by job status.
- `kavita_sync_jobs` with counts by job status.
- `kavita.configured`
- `kavita.pending_sync_series`

This endpoint is intended for server readiness/visibility, not deep external dependency checks.

## Raspberry Pi Deployment Notes

Target low-power deployment is Raspberry Pi 4 8GB with Docker Compose and an external SSD.

Operational expectations:

- Use a 64-bit Raspberry Pi OS.
- Keep Postgres data, Kavita data, and Manga Manager `storage/` on the external SSD.
- Avoid running the database or library on a microSD card except for short tests.
- Downloads are sequential by default, which is preferable on Pi-class CPU and I/O.
- Kavita folder scans are preferred over full-library scans to reduce CPU and disk pressure.
- Long multi-chapter imports return a pending state instead of tying up a browser request.

## Recent Robustness Changes

The latest robustness pass implemented:

- Safe migration handling for non-SQLite unstamped existing schemas.
- SQLite compatibility migration for legacy local databases.
- `DownloadJob.chapter_release_id` uniqueness.
- Race-tolerant `queue_downloads()`.
- Atomic `run_next_download()` job claiming.
- Stale running job recovery preserved.
- Scheduler startup failure recording.
- Scheduler creation failure recording while keeping manual web routes available.
- Expanded `/healthz` scheduler and source health data.
- Resilient `/poll-all` through `poll_all_sources()`.
- More consistent metadata refresh for existing and newly matched source rows.
- Asura reader extraction supports both normal `chapters/` and older `chapters-stitched/` CDN paths.
- `MAX_PAGE_BYTES` enforcement for image downloads.
- `MAX_COVER_BYTES` enforcement for cover downloads.
- Strict downloaded-page image validation before CBZ writes.
- Atomic cached-cover file replacement.
- Basic per-source request throttling.
- Reusable and explicitly closed HTTP clients in source adapters.
- Timezone normalization for SQLite-loaded datetimes before Python comparisons.
- Alembic migration rollout for download job uniqueness on existing versioned DBs.
- Scheduler jobs coalesce missed runs and avoid overlapping instances.
- `/healthz` and source route validation use configured source names without instantiating adapters.
- Deployment docs call out the lack of built-in authentication.
- Download execution instantiates only the needed source adapter and fails jobs cleanly if the source
  is disabled or unknown.
- Download execution revalidates queued jobs after claim and marks obsolete work as `skipped`.
- CBZ files are streamed into same-directory staging instead of building a full chapter page list in
  memory.
- Staged CBZ filenames include job and release identity to avoid temporary-file collisions and
  cross-filesystem rename failures.
- All-item poll failures now count toward source auto-disable.
- Source health can be manually disabled and re-enabled from the UI.
- Duplicate download-job cleanup keeps the most useful job state for each release.
- Focused regression tests for the above.
- Kavita series/chapter mapping fields and migration.
- Kavita folder-scan client with scan-all fallback.
- Kavita series/chapter deep-link route helpers.
- Pending Kavita sync discovery for tracked downloaded/unmapped series.
- Queued Kavita sync jobs remain queued when Kavita is unconfigured instead of being claimed and
  skipped.
- Kavita sync jobs derive targeted scan folders from downloaded chapter CBZ paths when no explicit
  folder is provided.
- Retry recovery includes skipped Kavita sync jobs from older local databases where the error is
  `Kavita is not configured`.
- Discovery update-card layout with separate series and chapter click targets.
- Focused newest-3 series import and exact chapter import routes.
- MangaFire `new` vs `hot` discovery mode with `new` as the default.
- Lower-priority source fallback after best-source max failures.
- Raspberry Pi deployment assumptions documented for Docker Compose on external SSD.
- MangaFire `/updated` pagination and HTML detail fallback.
- KingOfShojo description-prefix alias extraction and cleanup.
- Existing source-series match candidate regeneration during repair.
- Per-series download concurrency caps and running job heartbeat updates.
- Manual stale download job recovery from Info.
- Adaptive source frontier scanning across Asura, MangaFire, and KingOfShojo.
- MangaFire current `/manga/<slug>.<id>` discovery parsing.
- KingOfShojo placeholder/logo cover rejection and source cover fallback repair.
- Asura polluted alias/helper chapter cleanup during repair.
- Manual text match rebuild, visual fingerprint build, and visual match rebuild actions.
- New Chapters thumbnails.

## Test Coverage

Current tests cover:

- Domain normalization and source priority.
- Adapter parsing for Asura, MangaFire, and King of Shojo.
- MangaFire metadata parsing.
- Oversized page rejection.
- Oversized cover rejection.
- Series matching and external ID matching.
- Chapter release upsert and best-source selection.
- Download queueing rules and duplicate job prevention.
- Duplicate cleanup preserving the best existing job state.
- Atomic download job claiming.
- Disabled-source download job failure.
- Obsolete download jobs being skipped after best-source/downloaded-source revalidation.
- Future-due claimed jobs being moved back to `delayed`.
- Staging filename uniqueness for jobs targeting the same final chapter path.
- Skipped jobs not being retried by the failed-job retry action.
- Streamed CBZ writing and too-few-page rejection.
- Corrupt downloaded image rejection with no final CBZ left behind.
- Temporary unavailable handling.
- Replacement archiving and downloaded-file deactivation.
- Kavita scan failure after successful download.
- Retry backoff and stale running recovery.
- Manual merge recomputation and orphan-series cleanup.
- Poll item failure handling.
- Poll item cover-cache ordering so failed item details do not leave dangling cover paths.
- Source auto-disable after repeated all-item poll failures.
- Poll-all continuing after one source fails.
- Metadata refresh/merge behavior.
- Non-SQLite migration safety guard.
- Fresh and legacy SQLite migration smoke checks.
- SQLite naive datetime handling for delayed releases and stale jobs.
- Alembic previous-head upgrade for download-job uniqueness.
- `/healthz` response shape.
- `/healthz` unhealthy reporting when scheduler job inspection fails.
- Source health re-enable route behavior.
- Source health disable route behavior.
- Re-enable route rejection for unconfigured source names.
- Disable route rejection for unconfigured source names.
- Disabled source health skipping adapter construction.
- Cover cache refresh on changed cover URLs and preservation on refresh failures.
- Corrupt/unsupported cached-cover rejection and valid cached-cover extension detection.
- Docker Compose database fallback preservation when `.env` leaves `DATABASE_URL` unset.
- Discovery poll buttons rendering only for configured sources while retaining old health rows.
- Scheduler creation failure health reporting.
- FastAPI lifespan scheduler-start scheduling.
- Scheduler non-overlap/coalescing defaults.
- Kavita client folder scan, series listing, series detail, Want to Read, and URL rendering.
- Kavita pending sync discovery, derived sync folders, unconfigured queue preservation, and retry
  recovery for unconfigured skipped jobs.
- Kavita health visibility for configuration state and tracked downloaded/unmapped series still
  needing sync.
- `/run-kavita-sync` and scheduler Kavita drain queue pending sync work before draining jobs.
- Discovery update-card rendering and series/chapter links.
- MangaFire default new-mode query and explicit hot-mode query.
- Source fallback after best-source max attempts.

Validation commands:

```bash
UV_CACHE_DIR=.uv-cache uv run pytest -q
UV_CACHE_DIR=.uv-cache uv run ruff check .
UV_CACHE_DIR=.uv-cache uv run python -m compileall -q app tests
git diff --check
```

Latest known result:

- `pytest`: 108 passed.
- `ruff`: passed.
- `compileall`: passed.
- `git diff --check`: passed.

## Running Locally

Install dependencies:

```bash
UV_CACHE_DIR=.uv-cache uv sync --extra dev
```

Run the app:

```bash
UV_CACHE_DIR=.uv-cache uv run uvicorn app.main:app --reload
```

Open:

```text
http://127.0.0.1:8000
```

Run with Docker:

```bash
cp .env.example .env
docker compose up --build
```

For Docker usage, leave `DATABASE_URL` unset in `.env` to use the Compose Postgres default. Set it
only when intentionally overriding the database. The Docker image includes a `/healthz` healthcheck
and reports unhealthy when scheduler creation, startup, or job inspection fails.

## Important Development Notes

- Use `uv` for dependency and test commands.
- Keep SQLite compatibility for local/prototype databases.
- For Postgres/server DBs, prefer explicit Alembic migrations and avoid silent schema stamping.
- Do not bypass site access controls.
- Tests should avoid real network calls.
- Source HTML/API formats can change, so adapter parser tests should be updated when selectors or
  payloads change.
- Download job state must remain concurrency-safe; do not reintroduce a read-then-write claim flow.
- Be careful with session state in concurrent job tests. Use file-backed SQLite or a real database
  when testing cross-session behavior.

## Remaining Work

High priority:

- Add/update Alembic revisions for any future model changes instead of relying on compatibility code.
- Consider a real Postgres migration smoke test for CI or release validation.
- Confirm the configured Kavita series/chapter URL templates against the exact Kavita version in use.

Medium priority:

- Add richer source health controls with per-source poll status history.
- Add structured scheduler/job metrics beyond `/healthz` and the Library Operations panel.
- Add structured logging for polls, downloads, retries, and source disabling.
- Implement full Kavita Want to Read sync if you want Kavita to drive tracked-series selection for
  already imported series.

Lower priority:

- Improve match candidate UX and bulk actions.
- Add more source adapters.
- Add user-configurable source priority.
- Add retention/cleanup tooling for archived replaced files.
- Add richer ComicInfo metadata.
- Add CI configuration once the repository is committed.

## Suggested Next Steps

1. Add a short deployment guide for Postgres-backed Docker usage, including backup/restore.
2. Add a real Postgres migration smoke test for CI or release validation.
3. Add richer source health status in the HTML UI.
