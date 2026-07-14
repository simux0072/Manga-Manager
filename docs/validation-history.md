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
