# Database and Runtime Improvement Plan

Status: implementation complete; container characterization pending 2026-07-17. This plan
supersedes the remaining performance items in `codebase-architecture-and-optimization-audit.md`;
completed repair, UI, concurrency, and minimal-data work remains valid.

## Implementation result

- Migration `0019` repairs latest-release aggregates and adds observation, cover-band, and telemetry
  indexes. `database-audit` and the cheap `stage-check` relational checks detect regressions.
- Pulls persist listing observations and refresh only new, advanced, or seven-day-stale identities.
  Scheduler recovery, repair, telemetry, retention, cover, and Kavita work use separate due gates.
- Kavita uses one persistent client, a scan-generation list cache, bounded cover requests, and one
  coalesced progress batch instead of periodic per-series jobs.
- Cover matching uses indexed hash/title shortlists. Pillow/OpenCV generation and HDD ZIP work run
  in explicit bounded executors, while streamed archives retain the single-pass finalize path.
- Normal FastAPI routes run in the thread pool; GETs are read-only. PostgreSQL uses role-specific
  pools/timeouts, worker enqueue uses `LISTEN/NOTIFY` with polling recovery, and health/readiness plus
  durable Prometheus metrics are available.
- Checksum-addressed WebP thumbnails, immutable asset caching, abortable queries, and ten-page feed
  retention bound browser memory and transfer work.

The proposed source-file splits were deliberately not coupled to this performance change: moving
working public interfaces between modules has no runtime benefit and would make the required
container/browser characterization harder to attribute. The existing application boundaries and
contracts remain stable; a later formatting-only refactor can proceed after deployment validation.

## Evidence and decisions

The running PostgreSQL database was inspected read-only at migration
`0018_workflow_progress_identity`. It is 147 MiB and contains 1,135 canonical series, 1,190 provider
identities, 111,908 chapters, 117,270 releases, and 100 active artifacts. There are no duplicate
provider identities, orphaned active blobs, missing library/Kavita projections, expired leases,
expired permits, expired reservations, or reading-state aggregate inconsistencies.

Concrete problems remain:

- 28 canonical latest-chapter fields are stale. `autoflush=False` lets `_recompute_latest` query
  before newly added releases are flushed, so existing titles lag by one chapter and new titles can
  remain blank.
- Pulls confuse a five-row traversal frontier with the complete known catalog. The latest Asura
  pull refreshed 31/42 rows and KingOfShojo refreshed 132/134, including unchanged series.
- Six tracked series produced 640 successful Kavita jobs in 24 hours. Each per-series refresh can
  scan Kavita, list its library, upload covers, and fetch chapter progress.
- Cover refresh compares one identity with every cross-provider signature and loads large ORB blobs
  before shortlisting.
- The scheduler reboots every tracked download plan every 30 seconds. Retention aggregates jobs one
  row at a time; telemetry cleanup also runs every scheduler pass and lacks a `created_at` index.
- The database already holds 6,112 jobs, 40,797 events, 15,587 request samples, and 522 workload
  cycles after a short staging run. Current retention bounds eventual growth but creates avoidable
  write amplification.
- Non-streaming FastAPI routes are `async def` functions that execute synchronous SQLAlchemy work on
  the event loop. Chapter ZIP writes and several worker state transitions can block unrelated
  provider lanes on a slow disk.
- Suggested proposals and manual merge candidates still perform catalog-sized Python grouping or
  scoring. `/matches` also changes obsolete decisions during a GET request.
- Kavita opens a new HTTP client for nearly every API call. The web/worker engines use generic
  SQLAlchemy pool defaults and have no role-specific acquisition, statement, or lock timeouts.

The provider, PostgreSQL, ZIP, HDD, and Kavita waits dominate; Rust/Go would not remove them. Keep
Python, FastAPI, PostgreSQL, React, migrations, storage formats, and API URLs. Perform a modular
internal rewrite only where it removes measured work.

## Phase 1 — correctness and bounded diagnosis

1. Flush releases before latest recomputation and add migration `0019` to recompute every canonical
   latest number/source/time transactionally. Test first ingest, advancing chapters, decimals,
   nonnumeric fallbacks, merges, and idempotency.
2. Add a read-only `database-audit` command with JSON/Markdown output, a short statement timeout,
   and checks for denormalized latest values, identities, artifacts/projections, reading state,
   leases/permits/reservations, workload cycles, source hygiene, table/index size, dead tuples, and
   analyze age. Default `EXPLAIN` must not execute queries; an explicit synthetic-profile flag may
   use `EXPLAIN (ANALYZE, BUFFERS)`.
3. Extend `stage-check` with the cheap relational checks. Keep archive CRC/image validation separate
   because it is intentionally I/O-heavy.
4. Preserve the new Kavita credential recovery: an absent local credential file plus HTTP 401/403
   may recreate only the disposable Kavita config volume. Existing credentials remain
   non-destructive on failure.

## Phase 2 — stop work amplification

1. Persist a provider observation version on each source identity. Use the frontier only to bound
   listing traversal; bulk-compare every returned `(stable source ID, latest chapter)` against
   PostgreSQL and enqueue refreshes only for new, advanced, or explicitly stale identities. Refresh
   unchanged metadata on a slow seven-day cadence. Store 20 frontier sentinels so busy feeds still
   stop promptly.
2. Reconcile a tracked download plan when its source refresh adds chapters and when a download
   settles. Replace the 30-second all-tracked bootstrap with a bounded recovery cursor run at startup
   and infrequently thereafter.
3. Replace per-series periodic Kavita work with a coalesced scan generation and one batch progress
   refresh. Mutation jobs remain per series only when projections, covers, Want-to-Read, or an
   outbound reading command changes. Reuse one persistent HTTP client, cache the Kavita series list
   for the scan generation, bulk-load local reading rows, and upload changed chapter covers through
   a bounded four-request window. Prefer an installed bulk-progress endpoint when available.
4. Add indexed cover-hash bands and title-trigram SQL shortlisting. Load ORB descriptor blobs and run
   exact scoring only for the bounded shortlist. Offload Pillow/OpenCV work and cover-file writes
   from the async loop.
5. Gate housekeeping by purpose: policy finalization each minute, repair/download planning at its
   required cadence, retention every ten minutes, and telemetry deletion hourly in bounded batches.
   Aggregate retention buckets with one set-based upsert/delete and prune old settled cycles.

Acceptance: a clean pull creates zero refresh jobs; one changed listing creates one; a periodic
metadata audit is bounded. Six unchanged tracked series create at most one Kavita batch job per
configured interval and no repeated library scans. Latest mismatch count is zero.

## Phase 3 — responsiveness and queue efficiency

1. Move synchronous non-streaming FastAPI endpoints onto FastAPI's thread pool, leaving SSE async.
   Split `web/api.py` into catalog, library/reading, matching, jobs/operations, and events routers;
   move merge transactions from `web/app.py` into an application service. GET handlers become
   strictly read-only.
2. Replace Python-wide match proposal grouping with a PostgreSQL canonical-pair read query. Shortlist
   manual candidates from shared external IDs, existing decisions, trigram titles/aliases, and cover
   bands, then exact-score only the shortlist with a stable keyset cursor.
3. Move worker claim/heartbeat/finalize database calls and chapter image validation/ZIP writes off
   the event loop. Keep the existing 256 MiB byte budget and add a small bounded disk executor so
   provider downloads overlap without causing uncontrolled HDD seeks.
4. Add pool-aware PostgreSQL `LISTEN/NOTIFY` wakeups with delayed polling as a recovery fallback.
   Expired lease/permit cleanup moves out of every empty claim and into bounded maintenance.
5. Configure role-specific database pools and `connect_timeout`, `pool_timeout`, `statement_timeout`,
   `lock_timeout`, application names, and keepalives. Add `/livez` for the process and `/readyz` for
   database/migration readiness; retain `/healthz` compatibility.

Acceptance: no event-loop stall above 100 ms during concurrent downloads, idle workers generate
negligible claim traffic, warm page p95 remains below 200 ms, search below 300 ms, and all queue
lease/crash/fairness tests continue to pass.

## Phase 4 — media and maintainability

1. Generate local 320–480 px WebP cover derivatives while fingerprinting. Serve immutable,
   checksum-addressed thumbnails with ETags; keep original covers for matching and Kavita. This
   removes provider-sized images from grids and stabilizes discovery/library rendering.
2. Pass cancellation signals through every frontend query, add bounded page retention or
   virtualization for very long feeds, cache hashed static assets, and lazy-load the remaining
   route workspaces.
3. Format and split the dense `App.tsx`; split `job_queue.py` into claim/permit, lifecycle/event, and
   workload services. Preserve public interfaces until characterization tests pass. Consider moving
   the generic `app/` integration package under `manga_manager/integrations/` only after hot-path
   work is measured; that rename alone provides no speed benefit.
4. Add worker/provider/Kavita duration and queue-age metrics. Record database-audit and route-profile
   deltas in `validation-history.md` after each phase.

## Validation and rollout

- Use the generated small environment for correctness and the disposable 2,000-series/25,000-job
  profile for every iteration. Add a busy-feed fixture and at least 100,000 synthetic chapters for
  final read-path profiling.
- Run Python/PostgreSQL tests, Ruff, Vitest, production build, Firefox Playwright, migration
  round-trip, backup/restore, restart/lease recovery, memory stress, isolated Kavita, and ARM64.
- Compare job/event/sample creation rates and SQL query counts before and after. Do not raise remote
  concurrency from local CPU results; learned provider limits remain authoritative.
- Deploy one phase at a time. Back up PostgreSQL before `0019`; run `database-audit` immediately
  after migration and again after a 24-hour soak. No Raspberry Pi is required to complete these
  code and synthetic checks; Pi-only performance confirmation remains a later deployment gate.
