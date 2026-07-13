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
