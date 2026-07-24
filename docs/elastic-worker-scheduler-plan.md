# Elastic Worker Scheduler Plan

This file preserves the approved implementation plan across context compaction. Work must continue until the implementation, tests, documentation, review, and commit are complete. If a required heavy or permission-blocked validation cannot run locally, finish all other work and report the exact command for the user to run.

## Objective

Replace rigid one-worker-per-provider polling with a work-conserving shared network pool. Python remains appropriate because provider work is I/O-bound. CPU-heavy image comparison stays in a separately bounded lane.

## Network Scheduling

- Shared workers may claim download, acquisition-refresh, listing-pull, and ordinary-refresh jobs.
- Scheduling is non-preemptive. A newly queued download does not cancel active work, but every newly free worker chooses the highest eligible class.
- Priority is:
  1. eligible chapter downloads;
  2. acquisition-critical refreshes needed to enumerate newly tracked manga;
  3. overdue provider listing pulls;
  4. ordinary catalog refreshes;
  5. cover backfill and network health.
- Downloads may consume all useful eligible capacity. Background work uses only capacity that downloads cannot use.
- Provider, traffic-class, per-series, worker-wide byte, and job-kind permits remain atomic in PostgreSQL. One listing pull per provider is allowed, while its per-title refresh jobs may execute concurrently.
- Long provider waits release jobs, workers, and permits into `retry_wait`; only short intra-request pacing waits remain inside a worker.
- Provider fairness guarantees progress for other providers without weakening download priority.

## Provider Fallback

The exact source order is Asura, MangaDex, MangaFire, then KingOfShojo.

For 403/429 responses, transient 5xx/521 responses, connection failures, missing releases/404s, too-few-page results, invalid images, or corrupt responses:

- record the applicable provider/origin/CDN cooldown;
- discard incomplete temporary output;
- reroute the same logical normalized chapter to the next eligible release;
- preserve attempted sources so fallback cannot loop;
- use only identities already verified as one canonical manga.

If no verified alternative is available, defer the job and release all permits. Pending match suggestions must never become implicit fallback evidence.

## Other Resource Lanes

- Storage priority: new CBZ finalization, Kavita projection, user repair, background validation.
- CPU priority: active-download validation, user cover comparison, match suggestions, background cover backfill.
- Queue recovery and light health checks may run during downloads. Bulk normalization, audits, and backfill pause while downloads are active.
- Kavita synchronization remains a local-network lane and runs after currently queued manga downloads settle.

## Rollout and Acceptance

- Add settings for shared worker capacity and per-provider useful concurrency.
- Migrate queued/retry source-refresh jobs from legacy pull pools.
- Expose shared-worker and permit state in operations diagnostics.
- Verify strict priority, borrowing, provider caps, fairness, cooldown release, crash recovery, exact-chapter fallback, and migration idempotency.
- Run targeted unit tests, PostgreSQL queue/concurrency tests, full Python tests, frontend tests/build where affected, linting, and diff checks.
- Update concurrency, staging, and deployment documentation, review the whole change set, and commit it.

## Implementation Record

Implemented on 2026-07-24:

- twelve configurable shared network slots with PostgreSQL-atomic global, provider, traffic, and
  per-series permits;
- durable priority tiers, acquisition-refresh promotion, provider-aware borrowing, and LISTEN/NOTIFY
  wakeups;
- exact-provider fallback with immediate rerouting and attempt-preserving cooldown deferral;
- migration `0024`, scheduler diagnostics, Operations capacity reporting, and rollout documentation;
- runnable-download-aware background gating so future cooldowns, disabled providers, exhausted
  jobs, expired leases, and storage pauses release unrelated capacity.

Local verification completed with 249 Python tests passing (12 environment-dependent skips),
72 targeted scheduler/migration tests passing, Ruff, bytecode compilation, diff validation,
8 frontend tests, and a production frontend build. The PostgreSQL concurrency suite remains part
of CI and the disk-heavy local validation command because this host's mechanical disk made creation
of a disposable migrated PostgreSQL database impractically slow; the live staging database was not
used for destructive test setup.
