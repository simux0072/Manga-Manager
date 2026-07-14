# Provider concurrency tuning

Normal starting limits are Asura 1 job/1 page, MangaFire 2 jobs/4 pages, and KingOfShojo 2 jobs/4
pages, with one chapter per canonical series and eight chapter jobs globally. Source pulls use one
independent pool per provider, so all three may pull concurrently.

A source pull only reads a bounded recent listing and persists its frontier. Changed series become
deduplicated `source_refresh` jobs in the same provider pool, preventing one malformed or slow series
from restarting an entire site scan.

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
UV_CACHE_DIR=/tmp/uv-cache uv run manga-manager benchmark-workers --source mangafire --concurrency 2 --traffic both
UV_CACHE_DIR=/tmp/uv-cache uv run manga-manager benchmark-workers --source kingofshojo --concurrency 2 --traffic both
```

Operations exposes learned jobs/pages, request intervals, endpoint cooldowns, recent benchmarks,
frontier counts, workers, and leased permits. Do not raise static limits based on a short clean run;
provider policies automatically expire and are re-explored conservatively.

Health probes, cover backfill, library repair, Kavita, provider pulls, and each provider's downloads
use independent PostgreSQL claim lanes. This permits useful work to overlap without bypassing global
limits. Cover backfill has one low-priority worker and is scheduled only below 25 active chapter jobs.
Fallback changes the provider on the same logical job, remembers attempted sources, and waits for the
earliest cooldown instead of oscillating through cancel/recreate loops.

Metadata normalization is incremental, not a final whole-library phase. Download, tracking, merge,
recovery, and automatic repair requests coalesce into one active repair per canonical series. The
scheduler also collapses older per-artifact repair backlogs, retaining one series job and preserving
all obsolete storage keys required by completed merges. A repair already in progress receives at
most one follow-up pass when genuinely new merge cleanup arrives.

The Job Center groups work only after applying its selected state tab. Provider polls share one
workflow key with their discovered refreshes; chapter downloads group by workload cycle and canonical
manga. Group and child feeds use keyset cursors so live SSE invalidations cannot shift offset pages.
Succeeded/cancelled rows are rolled into daily aggregates after 14 days and failures after 90 days;
active rows are never pruned and aggregate history is retained for 365 days.
