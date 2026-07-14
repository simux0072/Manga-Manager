# Validation history

Generated logs and databases are intentionally not committed. This file records only bounded,
reviewable outcomes.

## 2026-07-14 Minimal-data acceptance baseline

- Full Python suite: 192 passed, 10 skipped without optional external state.
- PostgreSQL concurrency/diagnostics/migration suite: 9 passed.
- Ruff, Vitest, TypeScript, and Vite production build: passed.
- Firefox Playwright responsive/browser suite: 32 passed.
- Isolated staging: migrations, repair dry-run/apply, stage-check, 1 GiB memory stress, PostgreSQL
  backup/restore, restart recovery, and deterministic probe passed.
- ARM64 image: built and ran migrations/doctor under emulation.
- Generated small fixture: four valid three-page CBZs, duplicate-cover evidence, reading state,
  matching evidence, and representative jobs; a second seed was idempotent.
- Generated scale fixture: 2,000 series and 25,000 jobs; all 1,600 discovery rows traversed exactly
  once, maintenance work formed one group, and first pages returned in 284 ms and 389 ms on the
  mechanical-disk host.

The large legacy archive was not used as a release fixture because mechanical-disk validation was
I/O-bound. Subsequent acceptance uses generated small and database-only scale profiles.

## 2026-07-14 post-reset acceptance

- The 48 MB custom PostgreSQL archive passed SHA-256 verification, JSON diagnostics validation, and
  a disposable restore at migration `0018_workflow_progress_identity` before the old media reset.
- The isolated small environment passed CBZ integrity, Kavita series/chapter cover checks,
  PostgreSQL backup/restore, deterministic probe execution, and all 32 Firefox viewport tests.
- The isolated scale environment traversed 1,600 discovery rows exactly once and grouped 1,563
  queued metadata tasks into one entry; first pages returned in 55 ms and 318 ms.
- Runtime memory was 85 MiB worker, 93 MiB web, and 52 MiB PostgreSQL, within the configured 1 GiB,
  256 MiB, and 384 MiB limits.
