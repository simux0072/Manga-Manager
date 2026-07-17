# Codebase Architecture and Optimization Audit

Status: static audit completed 2026-07-16. This document describes the current tracked code, not
the legacy data or a particular staging database. Findings marked **measured** come from repository
size/build artifacts; findings marked **inferred** come from tracing code paths and should be
confirmed with the profiling plan below.

The 2026-07-17 live PostgreSQL follow-up, newly confirmed defects, and the ordered implementation
roadmap are recorded in `database-and-runtime-improvement-plan.md`. That plan supersedes this
document's remaining roadmap; the architecture and module inventory below remain the reference.

## Executive summary

Manga Manager is a private, single-user manga discovery, download, repair, matching, and Kavita
synchronization service. Its architecture is appropriate: a React client, a FastAPI API, PostgreSQL
as the durable catalog/job coordinator, provider-specific adapters, and content-addressed CBZ
storage. A full rewrite would add risk without addressing the dominant costs, which are database
round trips, repeated archive I/O, remote-site latency, and broad browser refreshes.

The highest-return work is a targeted hot-path refactor:

1. Make the interface snappy: stop synchronous database/filesystem work from blocking async event
   loops, send narrow SSE deltas, generate small cover thumbnails, and remove API N+1 queries.
2. Increase throughput: batch telemetry and job progress, reuse HTTP connections, ingest chapters in
   bulk, shortlist cover matches in SQL, and reduce idle queue polling.
3. Improve reliability after those paths are simpler: centralize provider failure policy, add
   graceful overload behavior, and make performance regressions part of acceptance tests.

The strongest inferred bottlenecks are:

- each provider request reserves a PostgreSQL schedule slot and then synchronously writes a separate
  telemetry transaction;
- each downloaded page writes a job row and a `job_event`, which triggers broad frontend query
  invalidation through SSE;
- freshly downloaded CBZs are read repeatedly for hashing and validation;
- synchronous ZIP, Pillow/OpenCV, and SQLAlchemy work runs inside `async def` handlers/routes;
- catalog ingest queries per chapter and recomputes latest-release state inside the chapter loop;
- cover evidence scans all cross-provider identities and performs per-candidate queries;
- matches, job grouping, and job serialization contain catalog-sized work or N+1 queries.

## Optimization pass implemented 2026-07-16

The first code pass completed the following high-return items from this audit:

- runtime provider telemetry is bounded and flushed in batches instead of opening one transaction
  per request;
- download and repair progress is throttled, while durable events are limited to phase/five-percent
  milestones and terminal state remains durable;
- streamed CBZs are finalized with one checksum pass and an atomic same-filesystem move instead of
  revalidating and rehashing a freshly written archive repeatedly;
- catalog chapters/releases are loaded and upserted as sets, latest-release state is recomputed once
  per ingest, and alias locking occurs once per synchronization;
- shared matching evidence is loaded in bounded set queries for manual ranking, catalog refresh, and
  cover candidates rather than once per candidate;
- job queue positions, group representatives/status totals, match latest chapters/blockers, and bulk
  reading state no longer issue routine per-row queries;
- the worker reuses one HTTP client-backed adapter per provider and gently backs off empty pool
  polling;
- frontend SSE invalidation is narrower, fallback polling is less aggressive, and the matching
  workspace is route-split from the initial bundle.
- HTTP responses expose request/SQL timings and query counts, `/metrics` retains bounded route
  counters, and the scale environment enforces latency and query-count ceilings across all primary
  read workspaces.

The 2026-07-17 follow-up also added latest-release repair migration `0019`, durable provider
observation versions, a bounded database audit, purpose-gated maintenance, SQL cover shortlisting,
coalesced Kavita progress batches and list caching, role-specific PostgreSQL pools/timeouts,
LISTEN/NOTIFY worker wakeups, a bounded HDD executor, local WebP derivatives, request cancellation,
readiness probes, and queue/provider/job-duration metrics. Structural file moves remain optional:
they do not change runtime behavior and should follow the container characterization suite rather
than being mixed into the measured hot-path repair.

## Repository and runtime shape

The tracked Python/TypeScript/CSS/shell/test surface is about 27,600 lines. The principal directories
are:

| Path | Responsibility |
| --- | --- |
| `app/adapters/` | Provider HTTP clients, parsing, frontier discovery, and page downloads |
| `app/domain.py` | Provider-facing `SeriesItem` and `ChapterItem` transfer objects |
| `app/kavita.py` | Kavita HTTP API and path mapping |
| `manga_manager/domain/` | Durable job payloads, canonical chapter/title rules, provider registry |
| `manga_manager/application/` | Pull, download, repair, matching, recovery, and Kavita use cases |
| `manga_manager/infrastructure/` | SQLAlchemy models/repositories, queue, storage, permits, telemetry |
| `manga_manager/worker/` | Pool construction, leases, retries, scheduler leadership |
| `manga_manager/web/` | FastAPI application, JSON API, merge transaction |
| `frontend/src/` | React SPA, React Query state, SSE updates, responsive CSS |
| `manga_manager/migrations/` | Nineteen ordered PostgreSQL/Alembic migrations |
| `tests/`, `frontend/tests/` | Unit, PostgreSQL, API, component, and browser behavior |
| `scripts/` | Local staging, Kavita E2E, reset, seed, scale, memory, and final validation tools |

Runtime data flow:

```text
provider listing -> source_pull job -> source_refresh jobs -> canonical PostgreSQL catalog
                                                        -> cover evidence/match decisions
tracked series -> download plan -> provider chapter job -> blob -> library/Kavita hardlinks
                                                        -> metadata repair -> Kavita sync
React UI <-> /api/v2 <-> PostgreSQL jobs/catalog <-> SSE job events
```

PostgreSQL is also the inter-process coordinator. Partial unique indexes enforce active job
deduplication and one leased mutation per series. Leased permits enforce provider and global
capacity. The worker currently creates one pull slot per provider, one Asura download slot, two
MangaFire slots, two KingOfShojo slots, and one slot each for Kavita, repair, health, and cover
backfill.

## Data and contract reference

### Integration contracts

| Component | Input | Output / side effect |
| --- | --- | --- |
| `SourceAdapter.list_recent[_frontier]` | optional stable `(source_id, latest_chapter)` sentinels | bounded `SeriesItem` observations |
| `SourceAdapter.get_series_detail` | listing `SeriesItem` | enriched series metadata |
| `SourceAdapter.get_chapters` | enriched series | ordered `ChapterItem` observations |
| `SourceAdapter.iter_chapter_pages` | one `ChapterItem`, optional progress callback | async ordered image-byte stream |
| `HttpSourceClient.request/get_soup/get_json/get_bytes` | provider URL, headers/referer | checked response, parsed body, or bounded bytes |
| `KavitaClient` | local folder/series/chapter IDs and progress mutations | Kavita scan, mapping, reading state, Want to Read, and cover updates |

`AsuraAdapter`, `MangaFireAdapter`, and `KingOfShojoAdapter` implement the same contract. Their parser
helpers remove helper/template links, normalize titles/numbers, extract safe image URLs, and stop
frontier traversal after stable sentinels. Asura additionally separates a rotating URL revision from
its stable source ID. MangaFire supports JSON API and HTML fallbacks.

### Domain records

- `SeriesItem` and `ChapterItem`: immutable provider observations consumed by catalog ingestion.
- `JobKind`, `JobState`, and Pydantic payload classes: validate every durable job at enqueue and
  lease time. Payload input is JSON; output is a typed `JobLease`.
- `CatalogSeries`: canonical manga plus denormalized latest release, cover, integrity, tracking, and
  Kavita mapping state.
- `CatalogSourceSeries`: one identity per provider per canonical manga.
- `CatalogChapter` / `CatalogChapterRelease`: canonical chapter and provider-specific release.
- `SeriesDownloadPlan` / `ChapterDownloadIntent`: first/latest bootstrap and rolling backfill state.
- `ArtifactBlob` / `ChapterArtifact` / `LibraryProjection` / `KavitaProjection`: content-addressed
  archive, active artifact, and filesystem projections.
- `WorkJob`, `JobPermit`, `WorkloadCycle`, `JobEvent`, `WorkerHeartbeat`: durable execution and UI
  progress model.
- `ProviderPolicy`, `ProviderEndpointState`, `ProviderRequestSample`, `ProviderBenchmarkRun`: learned
  concurrency, pacing, cooldown, and observations.
- `CatalogMatchDecision`, cover assets/signatures, training labels, and observations: explainable
  matching and quarantine history.

### Application use cases

| Class/function | Input | Output / purpose |
| --- | --- | --- |
| `SourcePullHandler.__call__` | leased `SourcePullPayload` | reads frontier, enqueues changed-series refreshes, advances source state |
| `SourceRefreshHandler.__call__` | leased `SourceRefreshPayload` | fetches detail/chapters, ingests catalog, refreshes cover evidence |
| `DownloadPlanCoordinator.track/reconcile` | session + canonical series | creates/updates intents and enqueues best available releases |
| `fallback_release/reroute/bypass_cooling_source` | failed release/provider state | moves the same logical job to an alternate provider or cooldown |
| `ChapterDownloadHandler.__call__` | leased release ID | bounded page stream, valid CBZ blob, active artifact, projected file, repair job |
| `CoverEvidenceService.refresh_for_source_series` | source identity ID | cached dHash/ORB signature and pending cross-provider decisions |
| `score_series_pair/strongest_candidate_score` | two or selected canonical IDs | weighted score and explainable title/cover/description/chapter features |
| `LibraryRepairPlanner/Handler` | canonical series and reasons | coalesced ComicInfo rewrite, projection repair, Kavita projection |
| `KavitaSyncPlanner/Handler` | tracked downloaded series | scan/map, covers, Want to Read, and bidirectional reading progress |
| `MaintenanceHandler` | probe action | storage/database or provider recovery probe |
| `CatalogRecovery`, `LegacyCbzImporter`, `LegacyRepair` | legacy DB/files and apply flag | reports, backup-safe repair/import, quarantine and rollback evidence |
| `ProviderIdentityRepair`, `RefreshQueueReconciler`, `StorageReconciler` | current DB/storage | dry-run/apply normalization or projection reconciliation |
| `build_diagnostic_bundle` | engine/storage | bounded credential-redacted operational snapshot |

Application error outputs are typed: `DeferredJobError` preserves attempts while waiting,
`RetryableJobError` consumes a retry, `PermanentJobError` terminates, `ReroutedJobError` records a
provider switch, and `LeaseLostError` prevents stale publication.

### Infrastructure services

| Service | Main operations | Contract |
| --- | --- | --- |
| `CatalogRepository` | `ingest`, poll success/failure, alias/external-ID/chapter sync | provider observations in; canonical rows and match decisions out |
| `JobQueue` | enqueue, claim, heartbeat, progress, reroute, defer, retry, finish | transaction-owned state changes; methods flush but do not commit |
| `ProviderRequestScheduler.reserve/wait` | source, traffic class, requested interval | atomically reserved request start time and async delay |
| `ProviderTelemetry` | observe, benchmark, finalize, effective cadence | samples in; learned policy/cooldown/poll interval out |
| `ContentAddressedStorage` | validate/store/materialize | files/page stream in; checksum-addressed blob/projection out |
| `StorageCapacityCoordinator` | reserve/refresh | lease-bound byte reservation and global pause state |
| `ArtifactRepository.activate` | chapter/release/blob/projection | exactly one active artifact and projection |
| `WorkerRegistry` / scheduler leadership | worker heartbeat / PostgreSQL connection | live worker state and one active scheduler |

### Worker and web entry points

- `manga-manager worker` builds handlers, installs the global provider waiter/observer, starts
  `WorkerService`, and runs `SourcePollScheduler` under advisory-lock leadership.
- `JobWorker.run_once` claims in a short transaction, runs the handler without holding a session,
  renews its lease, and applies typed retry/failure semantics.
- `create_app` mounts the built frontend, `/api/v2`, `/healthz`, and legacy redirects.
- API reads cover providers, discovery, library, updates, matches, merge candidates, jobs/groups,
  activity, operations, and SSE. Mutations cover tracking/reading, merges, decisions, retries, pulls,
  probes, and Kavita sync.
- `serialize_series` batch-loads providers/aliases/progress. `serialize_jobs` resolves chapter or
  series context and computes status/progress/queue position.

The CLI additionally exposes migrations, doctor/stage checks, diagnostics, provider benchmarks,
legacy audit/repair/validation/import, storage reconciliation, catalog recovery, matching export,
and queue repair tools. These are operational tools rather than a second runtime.

### Frontend components

| Component | Inputs | Output / behavior |
| --- | --- | --- |
| `App` | route, operations query, SSE events | shell, navigation, toasts, job drawer, route workspace |
| `Discovery` / `DiscoveryCard` | debounced query, multi-source filters, cursor pages | live search, infinite grid, optimistic tracking |
| `LibraryPage` / `LibraryCard` | filters, view mode, reading counts | grid/list library and status mutations |
| `Updates` / `UpdateCard` | unread grouped releases | expandable chapters and read mutations |
| `MatchesWorkspace` | suggested decisions or selected library series | batch review, ranked manual candidates, merge preview |
| `ActivityPage` / `ActivityRow` | event filters/cursor | grouped contextual timeline |
| `Operations` / `JobTable` | health, workers, active/failed/recent jobs | operational dashboard and retries |
| `JobDrawer` / `JobGroupCard` / `JobCard` | state tab, group/child cursors, cycle | live grouped workload with nested progress |
| `Cover`, `SourceChips`, `StatusPill`, `AutoPager` | display/filter props | reusable media/filter/status/infinite-scroll primitives |

React Query owns server state; mutations invalidate affected views. An SSE connection receives job
events, while the drawer and Operations retain 20/30-second recovery polling. Infinite feeds keep at
most ten pages and abort superseded requests. The current production JavaScript bundle is about
310 KiB uncompressed and CSS about 29 KiB (**measured**); the matching workspace is lazy-loaded.

## Module inventory

This is the file-level ownership map. Small private functions in each file implement the parsing,
normalization, serialization, or transaction named in the final column.

### Integrations and domain

| Module | Public surface | Input -> output |
| --- | --- | --- |
| `app/adapters/__init__.py` | `adapter_for_source` | provider name -> concrete adapter or `None` |
| `app/adapters/base.py` | `FrontierSentinel`, source exceptions, `SourceAdapter` | common provider protocol and typed temporary failures |
| `app/adapters/asura.py` | `AsuraAdapter`, revision/URL/chapter helpers | Asura HTML -> stable series, chapters, ordered pages |
| `app/adapters/mangafire.py` | `MangaFireAdapter`, API/HTML normalization helpers | MangaFire JSON/HTML -> series, chapters, ordered pages |
| `app/adapters/kingofshojo.py` | `KingOfShojoAdapter`, template/cover guards | KingOfShojo HTML -> series, chapters, ordered pages |
| `app/adapters/http.py` | `HttpSourceClient`, ordered byte iterator, dynamic limiters | scheduled HTTP requests -> bounded checked responses/page stream |
| `app/adapters/parsing.py` | attribute/date/title/image extractors | loose HTML/script/JSON -> normalized safe values/URLs |
| `app/domain.py` | provider DTOs and similarity/normalization | scraped strings -> transport records and comparison values |
| `app/kavita.py` | Kavita DTOs/client/parsers/factory | Kavita JSON and mutations <-> typed series/chapter/progress |
| `app/settings.py` | integration `Settings` | environment -> HTTP, provider, and Kavita configuration |
| `manga_manager/domain/catalog.py` | canonical normalization/sort helpers | title/chapter strings -> canonical keys and numeric order |
| `manga_manager/domain/jobs.py` | job enums, payloads, parser, lease | untrusted job JSON -> kind-specific validated model |
| `manga_manager/domain/matching.py` | stable provider identity functions | source IDs/URLs -> equivalence decision |
| `manga_manager/domain/providers.py` | provider registry and origin lookup | configured/static provider data -> names, priorities, owner |

### Application modules

| Module | Main callable(s) | Input -> output |
| --- | --- | --- |
| `catalog_recovery.py` | `CatalogRecovery.run`, report writer | legacy tracked rows -> current matches, repairs, review pairs |
| `cbz_import.py` | `LegacyCbzImporter` | CBZ tree/legacy DB -> validated blobs and catalog artifacts |
| `chapter_download.py` | `ChapterDownloadHandler`, snapshot/ComicInfo helpers | release lease -> artifact/projection or typed retry/reroute |
| `cover_backfill.py` | planner and handler | missing/stale signatures -> bounded low-priority jobs |
| `cover_evidence.py` | dHash/ORB functions, `CoverEvidenceService` | image bytes/cached signatures -> visual evidence and decisions |
| `diagnostics.py` | `build_diagnostic_bundle` | engine/storage -> redacted bounded JSON-ready diagnostics |
| `download_plans.py` | `DownloadPlanCoordinator` | tracking/catalog/provider state -> tiered chapter intents/jobs |
| `job_handlers.py` | `JobContext` and typed execution errors | lease + failure -> publication guard and worker disposition |
| `job_retention.py` | `JobRetention.prune` | aged terminal jobs/events -> daily aggregates and deletions |
| `kavita_sync.py` | planner, handler, `match_series` | projections/catalog/Kavita state -> mapping, covers, reading sync |
| `legacy_repair.py` | `LegacyRepair`, report/manifest helpers | legacy SQLite/files -> idempotent actions, archive, rollback report |
| `library_repair.py` | enqueue/planner/handler, ComicInfo helpers | series artifacts/reasons -> canonical metadata and projections |
| `maintenance.py` | `MaintenanceHandler` | probe action -> storage/database/provider health update |
| `match_training.py` | label recorder/exporter | reviewed decision -> reproducible feature/identity training row |
| `matching_score.py` | pair/strongest scorer | canonical IDs -> weighted score and component breakdown |
| `provider_health.py` | transient classifiers/failure recorder | HTTP outcome -> endpoint/source cooldown and bypass action |
| `provider_identity_repair.py` | audit/apply service | normalized provider evidence -> consolidation/quarantine actions |
| `refresh_queue_reconcile.py` | audit/apply service | legacy queued payloads -> compatible coalesced refresh jobs |
| `source_pull.py` | pull/refresh handlers and frontier helpers | listing/detail observations -> jobs and canonical catalog |
| `storage_reconcile.py` | `StorageReconciler.run` | DB projection snapshot + disk -> restored/missing/stale report |

### Infrastructure, web, worker, and delivery

| Module | Main callable(s) | Input -> output |
| --- | --- | --- |
| `artifact_repository.py` | `ArtifactRepository.activate` | stored blob + chapter -> one active artifact/projection |
| `catalog_repository.py` | ingest/poll state/matching sync | provider observations -> canonical relational state |
| `database.py` | engine/session/migration factories | URL/config -> PostgreSQL engine, sessions, upgraded schema |
| `db_models.py` | declarative model set | domain state -> constrained/indexed PostgreSQL tables |
| `job_queue.py` | durable queue transition methods | transaction + command -> validated state/event/permit change |
| `provider_scheduler.py` | global request reservation | source/class/interval -> atomic next request time |
| `provider_telemetry.py` | sampling/benchmark/policy functions | request samples -> learned limits, spacing, poll cadence |
| `scheduler_leadership.py` | advisory-lock leadership | engine -> exclusive live scheduler handle or `None` |
| `storage.py` | CBZ validator/store/materializer | archive/page bytes -> checksum blob and hardlink/copy projection |
| `storage_capacity.py` | lease-bound reservations | requested bytes/current disk -> reservation or paused state |
| `worker_registry.py` | register/heartbeat/live query | worker identity/status -> operational liveness rows |
| `worker/runtime.py` | `JobWorker` | claimable job -> handler execution and terminal/retry transition |
| `worker/service.py` | `WorkerService` | settings/handlers/pool selection -> concurrent worker slots |
| `worker/scheduler.py` | `SourcePollScheduler` | time/policy/state -> due pulls, repair/backfill/Kavita jobs |
| `web/app.py` | application factory and merge transaction | config/series IDs -> FastAPI app or atomic canonical merge |
| `web/api.py` | API router, serializers, cursor helpers | HTTP query/body -> paginated JSON, mutations, or SSE stream |
| `cli.py` | parser, dispatch, worker/benchmark/storage factories | CLI arguments/environment -> operation and exit status |
| `frontend/src/api.ts` | typed fetch facade and query encoder | client parameters -> `/api/v2` promises |
| `frontend/src/types.ts` | server response contracts | JSON shape -> compile-time UI types |
| `frontend/src/App.tsx` | shell and primary feature pages | React Query/router state -> interactive application |
| `frontend/src/MatchesWorkspace.tsx` | suggested/manual match UI | match/candidate pages -> decisions and merge mutations |
| `frontend/src/styles.css` | responsive design system | semantic classes/breakpoints -> desktop/mobile presentation |
| `manga_manager/migrations/versions/` | `upgrade`/`downgrade` revisions | old schema -> ordered compatible schema state |
| `scripts/` | shell/Python operational entry points | developer command -> staging, reset, seed, scale, E2E report |
| `tests/` and frontend tests | pytest/Vitest/Playwright cases | fixtures/runtime -> behavioral and regression assertions |

## Performance findings

### P0: responsiveness and throughput

#### 1. Remove synchronous work from async event loops

**Evidence:** FastAPI routes are `async def` but use synchronous SQLAlchemy sessions. Download,
repair, cover-signature, hashing, Pillow, OpenCV, ZIP, and filesystem operations are also called
directly by async handlers.

**Impact (inferred):** one slow query, HDD read, ZIP rewrite, or ORB comparison can pause unrelated
requests or all jobs sharing the worker loop.

**Change:** first make DB-bound HTTP routes normal `def` endpoints so FastAPI runs them in its
threadpool, and move bounded filesystem/CPU calls to dedicated executors with explicit concurrency.
Then evaluate an async SQLAlchemy/psycopg API migration. Keep storage mutation concurrency low on
the HDD while allowing network fetches to proceed.

#### 2. Coalesce progress and telemetry writes

**Evidence:** every page callback opens a transaction, updates `job`, and inserts `job_event`.
`ProviderTelemetry.active_observer` synchronously queries for a benchmark and inserts a sample on
every HTTP response. Provider scheduling already performs a separate transaction per request.

**Impact (inferred):** avoidable WAL/fsync pressure and event-loop stalls; one page can cause an SSE
event followed by several browser refetches.

**Change:** keep latest progress in memory and flush at most every 500 ms or 5 pages, always flushing
phase/terminal changes. Do not create durable `progress` events for every tick; emit transient SSE
progress deltas from the job row. Feed telemetry into a bounded queue and batch-insert every second;
outside explicit benchmarks retain failures plus sampled/aggregated successes. Preserve one atomic
request reservation, but replace its ORM read sequence with a compact SQL upsert/update-returning
operation.

#### 3. Reuse provider and Kavita HTTP connections

**Evidence:** each pull/refresh/download/cover job constructs and closes its adapter client.

**Impact (inferred):** repeated DNS, TCP, and TLS setup increases latency and load on providers.

**Change:** create process-lifetime clients per provider/traffic class, inject them into adapters,
set conservative connection limits and keepalive expiry, and close them during graceful worker
shutdown. Do the same for Kavita. Adapters should remain stateless parsers around shared clients.

#### 4. Make catalog ingest set-oriented

**Evidence:** `_upsert_chapter` performs chapter and release lookups per observation and invokes
`_recompute_latest` inside that loop. Alias/external-ID synchronization also performs repeated
read-before-write queries.

**Impact (inferred):** ingest grows at least linearly with a large constant and latest computation
adds quadratic-like repeated work for long series.

**Change:** materialize chapter input once; prefetch existing chapters/releases in two queries; use
PostgreSQL `INSERT .. ON CONFLICT .. DO UPDATE .. RETURNING`; synchronize aliases/IDs in batches;
recompute latest exactly once after all releases are present. Acquire the alias advisory lock once
per series, not once per alias.

#### 5. Shortlist matching candidates before image scoring

**Evidence:** cover refresh loads every identity from other providers, then fetches signatures and
scores candidates one by one. Manual merge loads all tracked series and computes the full shared
scorer before sorting/pagination.

**Impact (inferred):** catalog-wide CPU/DB cost on each refresh and manual search; ORB work can block
the worker loop.

**Change:** build a SQL shortlist (shared external ID, trigram title/alias top K, and four small dHash
band indexes). Batch-load candidate signatures and chapter sets, then run ORB only on the shortlist
in the cover worker executor. Store current scorer output on the proposal so list endpoints never
recompute it. Manual matching should rank a bounded top 100–250 candidates, not the entire library.
No transformer is warranted unless this indexed matcher is measured to miss unacceptable cases.

#### 6. Eliminate API N+1 and full-queue work

Confirmed structural examples:

- `serialize_jobs` calculates queue position with one count query per queued row;
- `/job-groups` performs representative, all-status, serialization, and optional plan queries for
  each group;
- `/matches` loads and groups the entire pending queue for every page, then queries blockers and two
  provider latest chapters per proposal;
- `provider_merge_conflicts` queries chapter sets per identity pair;
- “mark series read” calls `session.get` once per chapter;
- `/merge-candidates` scores all eligible tracked series before applying its cursor.

**Change:** use window functions for queue position; one aggregate plus one representative window
query for job groups; a canonical-pair proposal read model; grouped provider-latest and blocker
queries; bulk upsert reading state; and keyset pagination over persisted candidate scores. Add
`EXPLAIN (ANALYZE, BUFFERS)` fixtures at 1k series/100k chapters/100k jobs.

#### 7. Reduce archive passes and isolate HDD work

**Evidence:** `store_pages` validates each page, writes staging, calls `store_existing`, hashes the
whole archive, reads every ZIP member for validation, and hashes the hard-linked temporary again.
Library repair rewrites every changed archive synchronously.

**Impact (inferred):** redundant full-file reads are particularly costly on the mechanical disk.

**Change:** add a trusted “fresh stream” finalize path: validate pages while received, close ZIP,
hash it once, atomically hardlink/rename it, and record already-known image count/bytes. Keep the full
validator for imports and stage checks. Skip checksum re-reading when source and temporary are the
same inode. Run one bounded storage executor and coalesce series metadata rewrites as today.

### P1: browser and API snappiness

1. Replace broad SSE invalidation with typed deltas. A progress event should update only its cached
   job/group/cycle; catalog/Kavita terminal events should invalidate only affected series. Debounce a
   burst into one refresh. Move SSE to PostgreSQL `LISTEN/NOTIFY` (with heartbeat/replay by event ID)
   instead of querying every two seconds per client.
2. Generate content-addressed 320/640 px WebP/AVIF cover thumbnails. Serve immutable cache headers,
   `srcset`, dimensions, and an ETag. Current cards may download full provider covers.
3. Lazy-load routes (`React.lazy`) and split Matches/Operations/Job Center from the initial bundle.
   Add `content-visibility:auto` to long card feeds and virtualize only after retained infinite pages
   exceed a measured threshold.
4. Bound React Query infinite-page retention, pass `AbortSignal` to every search/filter API call,
   and preserve scroll anchors while pruning old pages.
5. Add response compression at the reverse proxy, immutable caching for hashed assets, and short
   ETags for stable provider/operations payloads.
6. Split the 1,766-line API module and dense, mostly one-line `App.tsx` into feature modules. This is
   primarily maintainability, but it makes route splitting, focused profiling, and safe caching much
   easier.
7. Avoid expensive fixed-element `backdrop-filter` on low-power clients when `prefers-reduced-motion`
   or a low-effects setting is enabled. Keep the current visual design as the default.

### P1: queue and scheduler efficiency

The worker has roughly twelve independent slots. When idle, each opens a claim transaction every
second. Every claim also expires permits and scans exhausted leases before selecting work, and may
retry up to fifty candidates when permits reject the front of a queue.

Recommended sequence:

1. Add `NOTIFY job_ready` on newly claimable work and one process-level listener that wakes relevant
   pool conditions; retain a 15–30 second recovery poll.
2. Move lease/permit cleanup to the scheduler rather than every claim.
3. Replace the fifty-candidate loop with a permit-aware SQL candidate query or claim a small locked
   candidate batch once.
4. If profiling still shows contention, use one process dispatcher to claim and feed bounded
   in-memory pool queues. PostgreSQL remains the authority; the dispatcher never acknowledges before
   the durable lease exists.

This is preferable to three bespoke queue implementations: the existing `pool` column already
provides independent lanes and preserves atomic recovery/fairness.

### P2: Kavita efficiency

- A per-series job can request a scan, enumerate series, query every chapter’s progress, and upload
  a series plus chapter covers. Coalesce filesystem changes into one scan barrier, then run per-series
  mapping/progress work after the scan generation advances.
- Batch or bound chapter-cover uploads (four at a time) and skip them by checksum, as the model
  already supports. Do not upload the same data URL sequentially for a long series.
- Fetch reading progress through a bulk Kavita endpoint if its installed API exposes one; otherwise
  keep the bounded four-request window.
- Cache list-series results for a scan generation. Avoid a full library list per series.
- Keep folder-per-canonical-series projections. Ambiguous folder mappings correctly fall back to a
  library scan but should be diagnosed rather than repeated indefinitely.

### P2: reliability and operability

1. Consolidate duplicated HTTP exception/circuit logic from source pull, source refresh, download,
   and probes into one provider outcome policy returning `defer`, `reroute`, `retry`, or `fail`.
2. Make all event/progress buffers bounded and define loss behavior: terminal state is never lost;
   intermediate progress may be replaced by the newest value.
3. Add role-specific DB pool sizes, connection acquisition timeouts, statement timeouts, and clear
   overload errors. The current engine uses SQLAlchemy defaults for every role.
4. Ensure GET endpoints are read-only. `/matches` currently canonicalizes already-merged pending
   decisions during a GET transaction; move that cleanup to merge/maintenance.
5. Add graceful client shutdown, worker drain state, and readiness separate from liveness.
6. Add Prometheus-compatible counters/histograms (or a compact `/metrics` alternative) for API
   latency, query count, claim latency, request scheduling wait, provider latency/status, pages/sec,
   archive finalize seconds, queue age, and Kavita scan duration.
7. Keep the extensive existing tests and operational scripts. Legacy repair/import code is still
   useful for migration and should not be deleted merely to reduce source size. It can be moved to
   an optional `manga-manager[legacy]` dependency/build layer after the migration window closes.

## Rewrite options

### Recommended: modular Python refactor

Keep Python, PostgreSQL, React, and the durable queue. Split web features, introduce shared clients,
set-oriented repositories, executor boundaries, batched observations, and read models. This attacks
the actual costs with the smallest compatibility risk.

### Viable later: async API persistence layer

Convert the web process (not necessarily workers) to SQLAlchemy async sessions/psycopg. This can
increase concurrent API responsiveness, but only after query counts are fixed; making N+1 queries
async does not make them cheap. A threadpooled synchronous API is the safer first step.

### Not recommended now: Rust/Go service rewrite

Provider waits, PostgreSQL, Kavita, ZIP/HDD I/O, and already-native Pillow/OpenCV dominate. A Rust
worker could reduce Python scheduling overhead but would duplicate parsers, migration contracts,
error semantics, and tests. Reconsider only if profiles after the P0 work show sustained Python CPU
outside native libraries. A small native extension is unnecessary because ORB and image decoding
are already native.

### Interesting but premature ideas

- Redis is unnecessary for one Pi while PostgreSQL already owns atomic leases and recovery.
- A service worker/offline shell could make navigation instant during restarts, but stale mutations
  and private-network caching need careful UX.
- A local vector model may improve difficult cover/title matches, but indexed dHash bands plus ORB
  should be evaluated first; transformer inference and vector storage add memory and training burden.
- PostgreSQL table partitioning for events/samples becomes useful only after retention and batched
  writes are measured at millions of rows.

## Implementation roadmap

### Phase 0: establish budgets and profiles

- Add request middleware that records duration, SQL query count/time, response bytes, and route.
- Add worker timers around claim, provider wait/request, parse, page stream, archive finalize,
  catalog ingest, repair, and Kavita operations.
- Extend `scripts/verify-scale-api.py` with repeatable 1k-series/100k-chapter/100k-job fixtures.
- Capture `EXPLAIN (ANALYZE, BUFFERS)` for discovery, library, updates, matches, job groups, activity,
  and operations. Capture browser LCP/INP and bundle transfer size in Playwright.

Initial Raspberry Pi targets (single interactive user): warm p95 discovery/library under 200 ms,
search response under 300 ms, job progress visible within one second, no event-loop stall above
100 ms, and UI interaction during downloads without visible jank.

### Phase 1: low-risk, highest return

- Rate-limit job progress and batch provider telemetry.
- Persist terminal events only plus coarse phase changes.
- Recompute latest once per catalog ingest and batch chapter/alias/ID upserts.
- Reuse HTTP clients and offload archive/image CPU/I/O.
- Fix `serialize_jobs`, job groups, matches, and reading-state N+1 queries.
- Add thumbnail generation and targeted/debounced frontend cache updates.

### Phase 2: scalable read and match paths

- Add match proposal/candidate read models and SQL shortlisting.
- Move API features into route/service/query modules and lazy-load frontend routes.
- Add LISTEN/NOTIFY-backed SSE and bounded query-page retention.
- Batch Kavita scans, list caching, progress, and cover updates.

### Phase 3: queue wakeups and reliability

- Add pool-aware job notifications and scheduled cleanup.
- Centralize provider outcome policy and configure role-specific DB pools/timeouts.
- Add drain/readiness, metrics, fault-injection tests, and recovery assertions.

Each phase should be benchmarked before and after on the small deterministic environment and the
synthetic scale environment. Do not tune provider concurrency from local CPU speed: remote rate
limits remain authoritative and must continue to adapt from bounded experiments.

## Validation and acceptance checklist

- Python unit/PostgreSQL suites, Ruff, frontend Vitest/build, Firefox Playwright, and ARM64 build pass.
- API scale tests assert query-count ceilings as well as latency; pagination never loads/sorts the
  full catalog in Python.
- A 100-page download produces bounded progress/event writes and stays within the 256 MiB in-flight
  budget.
- UI remains responsive while downloads, repair, matching, and Kavita sync overlap.
- Worker restart recovers jobs, permits, storage reservations, and buffered terminal observations.
- Provider cooldown/fallback behavior and one-identity-per-provider invariants remain unchanged.
- Every active artifact still validates, has one library projection, and maps to at most one Kavita
  projection.
- Kavita reading state remains bidirectional without oscillation, and scans/covers are idempotent.

## Decisions and assumptions

- Optimize for a private single-user Raspberry Pi deployment with PostgreSQL and a mechanical disk;
  network providers, not local CPU, govern safe download concurrency.
- Preserve current URLs, storage/database compatibility, repair reports, and progressive failure
  recovery.
- Prefer deletion only for generated artifacts or proven dead code. Tests, migrations, recovery
  commands, and staging scripts are active safety infrastructure.
- Profile before considering a language/framework rewrite. The current stack can meet the target
  with substantially less I/O and fewer round trips.
