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

## 2026-07-16 measured optimization acceptance

- Full lightweight Python suite: 212 passed, 10 skipped; Ruff passed.
- Firefox Playwright responsive/browser suite: 32 passed.
- Four active CBZs passed archive, image, ComicInfo, projection, backup/restore, and exact served
  Kavita cover-byte checks.
- The disposable scale catalog contained 2,000 series and 25,000 jobs. First-page measurements were
  175 ms/3 SQL statements for Discovery, 102 ms/6 for Job groups, 15 ms/5 for Library, 21 ms/7 for
  Matches, 80 ms/15 for Operations, 16 ms/5 for Updates, and 8 ms/2 for Activity.
- Runtime memory was 86 MiB worker, 93 MiB web, and 74 MiB PostgreSQL.

## 2026-07-17 live staging database audit

- PostgreSQL migration `0018_workflow_progress_identity` occupied 147 MiB for 1,135 series, 1,190
  identities, 111,908 chapters, and 117,270 releases.
- All 100 active artifacts had blobs plus library and Kavita projections. Provider uniqueness,
  reading aggregates, leases, permits, and reservations were consistent.
- The audit found 28 stale denormalized latest chapters, excessive frontier-driven source refreshes,
  and 640 Kavita sync jobs for six tracked series in 24 hours. These are implementation defects and
  work-amplification issues, not storage corruption; their repair plan is
  `docs/database-and-runtime-improvement-plan.md`.

## 2026-07-17 runtime repair implementation

- Full non-container Python suite (excluding the host-only FastAPI thread-bridge limitation):
  198 passed and 10 skipped; the targeted executor/provider/queue group passed 71 tests with one
  optional PostgreSQL case skipped. Ruff and `git diff --check` passed.
- Focused catalog/database/audit/source/cover tests passed (26 tests); queue, lease, worker, and
  Kavita compatibility tests passed (39 tests).
- The bounded HDD executor passed chapter download, storage import, and library repair coverage;
  the combined storage/cover group passed 25 tests. Python Ruff checks passed for each changed
  group.
- Frontend TypeScript production build passed at 309.42 KiB JavaScript and 28.65 KiB CSS; all four
  Vitest interaction tests passed.
- Local Python 3.14 has a broken default asyncio thread-executor bridge on this old host. Production
  remains Python 3.12, and storage, cover, request-scheduling, and telemetry lanes use explicit
  bounded executors with a portable completion bridge. The full FastAPI/PostgreSQL/Firefox/Kavita/
  ARM64 characterization is therefore intentionally delegated to the isolated container validation
  command after rebuilding migration `0019`.
- Frontend Vitest passed all four interaction tests. TypeScript and the production Vite build passed
  with a 309.42 KiB main JavaScript bundle, a 12.33 KiB lazy Matches workspace, and 28.65 KiB CSS.
