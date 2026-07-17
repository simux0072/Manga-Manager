# Minimal Data Reset and Portable Test Environment Plan

## Execution directive

Implement every item in this document without stopping at partial completion. If a command cannot
run because of permissions or host limitations, continue all other work and report the exact command
needed at handoff. Persist this file across context compaction and reread it before continuing.

After implementation, review the changes for mistakes, fix them, then review the wider codebase and
fix any additional concrete defects before finishing. Commit all intended changes and keep GitHub
payloads small: no databases, CBZs, manga covers, raw logs, screenshots, generated bundles, or
runtime storage.

## Retention and cleanup

- Keep source, migrations, tests, frontend, lockfiles, CI, operational scripts, documentation, and
  recovery/import tools in Git.
- Before reset, preserve locally one compressed PostgreSQL dump, SHA-256 checksum, and sanitized
  bounded diagnostic summary. Verify the dump through a disposable restore.
- Delete legacy/v2 manga storage, SQLite databases and backups, screenshots, raw validation logs,
  old Kavita test state, generated test/build output, stale project containers/volumes/images, and
  Docker build cache older than 24 hours.
- Preserve local `.env`, dependency environments, and package caches to keep iteration fast.
- Delete the temporary PostgreSQL archive after Raspberry Pi acceptance succeeds.

## Implementation

- Finish direct virtual-environment runtime commands across Dockerfile, Compose, staging, health,
  CI ARM, and validation paths. Add bounded container log rotation.
- Add a credential-redacted `diagnostic-bundle` CLI with migration/catalog/job/provider/worker and
  storage summaries plus bounded recent classified failures.
- Add preview-first, explicitly applied reset tooling that verifies the archive and deletes only
  known Manga Manager resources; legacy deletion requires a separate explicit flag.
- Parameterize staging/Kavita names, ports, state, storage, containers, and volumes.
- Add one portable `test-environment.sh` for `up`, `check`, `scale-check`, `down`, and `reset`.
- Generate two synthetic manga at runtime, represented by three provider records, with tiny valid
  CBZs/covers/read state/matching evidence and representative jobs. Commit no media binaries.
- Add an isolated database-only scale profile with at least 2,000 series and 25,000 jobs, a stopped
  worker, bounded performance checks, and automatic cleanup.
- Replace stale execution documentation with concise implementation status and validation guidance.

## Validation and rollout

- Test migrations, seed idempotency, API/UI, CBZ integrity, metadata repair, job grouping/progress,
  infinite scrolling, drawer scroll isolation, Kavita covers, restart recovery, backup/restore,
  diagnostics redaction, and reset idempotency.
- Scale-test cursor completeness, grouping/deduplication, first-page latency below one second, and
  the 1 GiB worker limit.
- Run Python/PostgreSQL, Ruff, Vitest, Playwright, production build, isolated staging, and ARM64
  checks. Use the same deterministic environment on the Raspberry Pi with isolated Kavita; keep live
  provider checks bounded and opt-in.
- Commit in three concerns: runtime/diagnostics safeguards, portable test environments, then
  documentation/cleanup/validation. Push `master` only after validation.
- After reset, iterate on the small environment; test production on the Pi with two real manga for
  24 hours before gradually expanding.

## Fixed assumptions

- Existing manga images and legacy SQLite data may be deleted after the PostgreSQL archive verifies.
- The database archive stays local and ignored; it is never transferred to GitHub or the Pi.
- Synthetic media is generated and contains no copyrighted manga content.
- Pi Kavita acceptance uses an isolated instance.
- Offline deterministic checks are mandatory; live provider checks are opt-in.

## Implementation status

- Runtime entrypoints, bounded logs, diagnostics, reset safeguards, generated fixtures, isolated
  Kavita/staging orchestration, scale verification, CI contracts, and documentation are implemented.
- Local Python, PostgreSQL, frontend, image, idempotency, and scale checks pass; exact outcomes are
  recorded in `docs/validation-history.md`.
- The real archive/reset, disposable restore, isolated Kavita/browser rehearsal, and post-reset
  acceptance were completed. The verified archive remains local and ignored until later Raspberry
  Pi acceptance; no runtime database or media was committed.
