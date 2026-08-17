# Provider concurrency tuning

Normal starting limits are Asura 1 job/1 page, MangaDex 2 jobs/4 pages, MangaFire 2 jobs/4 pages,
and KingOfShojo 2 jobs/4 pages, with one chapter per canonical series and eight chapter jobs
globally. Twelve shared network workers borrow work across all providers. PostgreSQL permits retain
one listing pull per provider and bound concurrent per-title refreshes to Asura 2, MangaDex 8,
MangaFire 4, and KingOfShojo 4. These are useful-work ceilings, not request-rate overrides; the
provider-global request scheduler still paces every origin and CDN request.
Workers in one process reuse provider adapters and their HTTP connection pools. The clients support
concurrent requests, MangaFire serializes its mutable VRF/token refresh state, and the one-pull
provider permit keeps listing diagnostics single-writer while per-title refreshes run concurrently.
On memory-constrained backfill deployments, `V2_ENABLE_COVER_PROCESSING=false` defers native cover
fingerprinting during refreshes and prevents automatic cover-backfill claims, while
`V2_ENABLE_LIBRARY_REPAIR=false` leaves bulk CBZ metadata rewrites queued. Provider metadata and
chapter downloads continue normally; restore both defaults after the initial backlog and run the
deferred maintenance under observation.

A source pull reads the provider's update-ordered feed and persists its frontier. Asura is scoped to
the `Latest Updates` section (not the preceding trending shelf), MangaDex uses its official English
chapter feed, KingOfShojo uses `/manga/?order=update`, and MangaFire uses its chapter-update JSON
ordering. Every row on a fetched
page is inspected; following pages are fetched until three saved series/chapter sentinels agree, the
listing ends, or the configured recent-page safety window is reached. Changed series become
deduplicated `source_refresh` jobs in a separate provider refresh pool, preventing one malformed or
slow series from restarting an entire site scan. Those per-title jobs may execute concurrently and
borrow every shared slot that is not needed by higher-priority work.

This is an incremental update scan, not a complete hourly catalog recrawl. If a provider batch-touches
enough entries to fill the safety window before the frontier is found, Manga Manager additionally
queues a direct refresh for every tracked series omitted from that window. The same fallback applies
when Asura's finite Latest Updates feed no longer contains the saved frontier. This preserves tracked
updates without repeatedly walking MangaFire's tens of thousands of titles. Operations reports
`Caught up`, `Feed end`, `Feed window + tracked fallback`, or `Window limit` together with the pages
and titles inspected. A persistent `Window limit` means the configured `*_RECENT_PAGES` value should
be reviewed.

Every HTTP request records status, latency, bytes, host, `Retry-After`, and whether it was origin or
CDN traffic. PostgreSQL-backed endpoint schedules make pacing global across worker processes.
Limiting signals reduce capacity, increase request spacing, open a cooldown/circuit breaker, and
bypass waiting chapters through alternate providers. Recovery probes run in the affected provider's
pull pool, so a slow site cannot block database/storage maintenance. Clean bounded experiments can
promote limits; Asura concurrency two is allowed only inside a benchmark and is abandoned on the
first limit.

Run bounded experiments only against content you may access:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run manga-manager benchmark-workers --source asura --concurrency 1 --traffic both
UV_CACHE_DIR=/tmp/uv-cache uv run manga-manager benchmark-workers --source mangadex --concurrency 2 --traffic both
UV_CACHE_DIR=/tmp/uv-cache uv run manga-manager benchmark-workers --source mangafire --concurrency 2 --traffic both
UV_CACHE_DIR=/tmp/uv-cache uv run manga-manager benchmark-workers --source kingofshojo --concurrency 2 --traffic both
```

Operations exposes learned jobs/pages, request intervals, endpoint cooldowns, recent benchmarks,
frontier counts, workers, and leased permits. Do not raise static limits based on a short clean run;
provider policies automatically expire and are re-explored conservatively.

Network admission is non-preemptive and strictly ordered: chapter downloads, acquisition refreshes
needed by newly tracked titles, overdue listing pulls, ordinary per-title refreshes, then provider
health work. A download may consume every eligible shared slot; work already in flight completes,
then each freed slot takes the highest eligible job. Provider cooldowns and full provider permits
make unrelated work eligible immediately instead of parking a worker in a long sleep.

Storage/CPU lanes remain separate. Cover backfill, automatic projection repair, and match rescoring
pause while a chapter is leased or immediately runnable. A future retry on a cooled-down/disabled
provider or a storage pause releases those background lanes instead of idling the machine. Kavita
periodic synchronization is deliberately stricter and waits until the current download queue
settles.
Fallback changes the provider on the same logical job in the order Asura, MangaDex, MangaFire,
KingOfShojo, remembers attempted sources, and waits for the applicable cooldown instead of
oscillating through cancel/recreate loops.

Metadata normalization is incremental, not a final whole-library phase. Download, tracking, merge,
recovery, and automatic repair requests coalesce into one active repair per canonical series. The
scheduler also collapses older per-artifact repair backlogs, retaining one series job and preserving
all obsolete storage keys required by completed merges. A repair already in progress receives at
most one follow-up pass when genuinely new merge cleanup arrives.

The Job Center groups work only after applying its selected state tab. Provider polls share one
workflow key with their discovered refreshes; chapter downloads group by workload cycle and canonical
manga. Metadata repair, Kavita synchronization, cover evidence, and health work group by kind within
the current workload cycle, while a single task remains a normal job card. Group progress includes
terminal siblings even when the selected tab shows only active children. Group and child feeds use
keyset cursors so live SSE invalidations cannot shift offset pages.
Succeeded, cancelled, and failed rows are rolled into daily aggregates after 14 days; active rows
are never pruned and aggregate history is retained for 365 days. Only the newest unresolved attempt
for a logical job appears in the Failed tab. Operators can dismiss one failure or all unresolved
failures from the live view without deleting its activity/event audit trail.
