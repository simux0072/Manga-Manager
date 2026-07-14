# Matching, Provider Identity, Ranking, and Job Operations Repair Plan

## Execution Directive

Implement every item in this document and keep this file current across context compactions. Prefer
coding and lightweight validation while the live queue is active. Defer checks that require a quiet
database, and list any commands that cannot be run because of permissions or live-state safety at
handoff. Do not stop at partial implementation. Review the completed work against every checklist
item, then commit all intended repository changes, including pre-existing uncommitted work that is
part of this implementation.

## Completion Checklist

- [x] Stable Asura provider identities, revision consensus, repair, aliases, and latest metadata
- [x] Unified matching scorer, cover evidence, manual 2-N selection, and bulk Suggested review
- [x] Durable workflow groups, workload cycles, and overall/group/job progress
- [x] Pull coalescing, payload compatibility repair, and bounded queue behavior
- [x] In-place provider rerouting, fair independent pools, and health isolation
- [x] Grouped, paginated, live Job Center with three progress layers
- [x] Terminal retention, daily aggregates, rollout docs, and operational safeguards
- [x] Python, PostgreSQL, frontend, browser, lint, build, staging, and recovery validation
- [x] Final diff review and structured commits

## Current Implementation State

- Implemented: every catalog, matching, queue, worker-pool, progress, retention, UI, repair, staging,
  and documentation item below. Duplicate release repair now preserves artifacts, attempts, and job
  payload references. Staging and unattended validation use isolated names and single-run locking.
- Complete: the isolated staging rehearsal passed migrations, repair dry-run/apply pairs,
  stage-check, the 1 GiB memory stress test, backup/restore, restart recovery, and probe execution.
- Final validation: 180 Python tests plus eight PostgreSQL scheduling/recovery tests, migration
  round-trip, Ruff, Vitest, production build, workflow parsing, all 32 Firefox Playwright tests,
  local ARM64 runtime, and GitHub Verify run `29302565178` pass.

## Provider Identity and Chapter Metadata

- Represent Asura series by a stable path with the trailing eight-character revision removed. Store
  the consensus revision in `source_state_v2.cursor_json`; retain per-series overrides when needed.
- Accept a new global revision only when three distinct homepage entries agree. Construct current
  series/chapter URLs from stable identity plus the selected revision.
- Update existing identities and release URLs transactionally instead of inserting revision-based
  duplicates.
- Add dry-run/apply provider repair with JSON reporting. Consolidate the 23 known Asura pairs only
  when stable path, title, and chapter/cover evidence agree; quarantine ambiguous pairs.
- Compute canonical and provider-specific latest chapters from maximum numeric `sort_number`, using
  publication time only for nonnumeric releases and chronological feeds.
- Remove stale provider-derived aliases, including `Asura Scans Home`, and expose provider-specific
  latest chapters on match cards.

## Matching, Scoring, and Review

- Collapse raw decisions into one proposal per unordered canonical pair; close decisions already in
  the same canonical manga. Accept/reject all underlying decisions together and merge connected
  components once.
- Provide individual and entire-queue selection (including unloaded pages), batch preview, Merge,
  Keep Separate, confirmation, and explicit blockers. Safe components proceed; blocked proposals
  remain pending.
- Manual candidates accept `selected_id[]` and allow 2-N records up to configured provider count.
  Selected records occupy all their provider slots; exclude conflicts and all-provider records.
- Share one scorer: external ID 99%; otherwise title/alias 35%, cover 35%, description 15%, chapter
  overlap 15%; strong cover plus description/chapter agreement has an 88% floor. Return evidence
  breakdowns and prevent generated/stale aliases inflating scores.
- Keep ORB and perceptual hashes with crop/scale-tolerant comparisons; no transformer.
- Add one low-priority cover-signature backfill worker using bounded batches only while active chapter
  work is below 25 and respecting provider-global throttling.

## Durable Workflows and Progress

- Add durable workflow/group IDs: downloads by cycle+canonical manga; pull root and refresh children
  by exact provider poll run; repairs, Kavita, cover backfill, and health by finite invocation.
- Maintain a persistent workload cycle for the known finite backlog. Newly discovered work joins it
  and increases the total. Future scheduled work is excluded. Settle only when no member is active
  and no discovery member can add children.
- Store total, success, failed, cancelled, and remaining logical units independently of job retention.
- Expose an overall cycle bar, group bars, and individual running-job bars. Failed work is a red
  terminal segment. Queued work shows position/availability; indeterminate progress is used only
  before a total is knowable.
- Download totals come from `ChapterDownloadIntent`, pull totals from roots plus discovered refresh
  candidates, and other finite workflows from explicit item totals.

## Pull Deduplication and Scheduling

- Enforce one active root pull per provider and one active refresh per provider identity.
- Coalesce newer observations into queued/retrying refresh jobs. If leased, retain one materially
  newer pending observation for processing after the lease.
- Version refresh payloads/parsers. Preserve and group compatible queued work; cancel/rebuild only
  incompatible work. Never discard frontier-discovered work solely because the frontier advanced.
- Keep PostgreSQL provider/pool lanes: independent pull providers, download providers, Kavita,
  maintenance, cover backfill, and a lightweight health pool. Health must not wait for repair.
- Preserve provider-global limits (Asura 1, KingOfShojo 2, MangaFire 2), memory/global ceilings,
  per-series exclusion, adaptive cooldowns, circuit breakers, renewal, and recovery unless benchmark
  evidence justifies a change.
- Reroute fallback in place: emit `rerouted`, retain attempted providers, exclude cooling/tried
  providers, wait until earliest cooldown if none is usable, and never downgrade preferred upgrades.
  Preserve the logical job ID and prevent provider oscillation.
- Claim eligible provider lanes fairly so one blocked provider cannot stall another.

## Job Center and Retention

- Serve cursor-paginated group summaries and independently paginated children. Group after applying
  the selected state tab; single jobs keep their existing card.
- Active ordering is running first, then actual claim order: priority, `available_at`, ID.
- Add infinite scrolling for groups and children and merge SSE changes without duplicates or cursor
  jumps.
- Download groups show manga cover, providers, chapter range, counts, errors, progress, and children.
  Pulls group by poll workflow. Separate running, queued, completed, and failed presentation.
- Top badge shows active groups and raw tasks, e.g. `42 groups · 718 tasks`.
- Retain succeeded/cancelled jobs 14 days and failures 90 days. Before bounded transactional deletion,
  create idempotent daily source/kind/status/error/duration/attempt aggregates retained 365 days.
  Never prune active jobs.

## Interfaces, Rollout, and Verification

- Add ordered migrations for identity normalization, backfill jobs, groups/cycles/progress, payload
  versions, and aggregates. Extend payloads backward-compatibly.
- Add grouped job/cycle APIs and SSE deltas. Change manual matching to `selected_id[]` while retaining
  compatibility for old links/forms. Preserve one identity per provider in every merge.
- Back up PostgreSQL and create a storage manifest before applying provider repair. Migrate, dry-run
  and apply repair, recompute latest fields/aliases, reconcile payload compatibility, then allow
  deferred work to settle before final stage-check.
- Re-audit Kavita stale-ID recovery, bounded stage-check, CI fixes, leases, restart recovery, and
  rollback.
- Test revisions/consensus/repair; numeric latest; matching collapse and batch blockers; Noble Lady
  scoring; crop/zoom covers; dynamic providers; pull coalescing; in-place fallback; mixed-provider
  concurrency; all progress layers; grouping/pagination/SSE; retention; migrations and rollback.
- Run full Python/PostgreSQL, Ruff, Vitest, Playwright, production build, ARM64 image, staging,
  restart-recovery, and rollback checks where the environment permits.

## Fixed Assumptions

- Entire-queue selection includes unloaded Suggested proposals.
- Bulk merge applies safe components and leaves blocked proposals visible.
- Verified Asura revisions may be repaired automatically; ambiguous duplicates remain quarantined.
- Compatible queued refresh work is preserved rather than deleted to reduce displayed counts.
- Overall progress covers currently known work, not future scheduler runs.
- Provider limits increase only with benchmark evidence.
- Cover backfill is subordinate to chapter downloads.
