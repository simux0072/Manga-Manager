# Catalog repair

Always audit and preserve the generated manifest before applying a repair:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run manga-manager audit-legacy manga_manager.db \
  --storage-root storage --report reports/legacy-audit.json
UV_CACHE_DIR=/tmp/uv-cache uv run manga-manager repair-legacy manga_manager.db \
  --storage-root storage --backup-dir backups --report reports/legacy-repair.json --apply
```

Applied repair uses a SQLite backup, deterministic action records, same-filesystem archive renames,
and a storage manifest. Re-running the applied repair must report zero changes. Keep the backup,
report, manifest, and timestamped repair archive together until the 30-day retention period expires.
Use `cleanup-repair-archives storage --retain-days 30` only after validating the imported library.

Validate all retained archives after repair:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run manga-manager validate-legacy manga_manager.db \
  --storage-root storage --report reports/legacy-validation.json
```

The validator checks ZIP CRC, `ComicInfo.xml`, readable images, artifact uniqueness, and projections.
Restore by stopping writers, restoring the reported SQLite backup, and renaming archived files back
using each report action's rollback fields.

After importing repaired CBZs into PostgreSQL, recover the tracking intent of every legacy series
that has an active downloaded file (including legacy `ignored` rows, per the selected policy):

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run manga-manager audit-catalog-recovery manga_manager.db \
  --report reports/catalog-recovery-audit.json
UV_CACHE_DIR=/tmp/uv-cache uv run manga-manager repair-catalog-recovery manga_manager.db \
  --report reports/catalog-recovery.json --apply
```

The apply pass tracks exact provider identities, leaves ambiguous groups for manual review, recreates
unresolved provider identities in an attention state, and queues canonical metadata/Kavita repair.
It does not mutate the legacy database. Keep both reports with the PostgreSQL/storage backup.

If tracking is already correct or the legacy database is unavailable, enqueue the same canonical
archive/projection work directly with `manga-manager enqueue-library-repair --all-tracked` (or pass
one numeric series ID). Jobs are resumable and reserve storage one archive at a time.
The scheduler also discovers tracked active artifacts without a Kavita projection in bounded batches,
so this command is an explicit accelerator rather than a required upgrade step.

Run `stage-check` only after chapter-download, library-repair, and Kavita queues settle. The command
returns a compact `"busy": true` response instead of scanning a moving storage snapshot. Its default
failure details are bounded; use `--full-details` when preparing an offline repair manifest.

To build a future image-matching dataset from operator decisions without introducing a transformer:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run manga-manager export-match-training training-data
```

The JSONL manifest contains immutable feature and identity snapshots. Cached covers are hardlinked
where possible, so exporting does not duplicate a large image collection on the same filesystem.
The current scorer normalizes covers to a shared feature resolution, uses four crop-aware difference
hashes as a cheap candidate gate, then applies ORB matching with RANSAC geometry. This handles
provider thumbnails, translated title overlays, and modest crop/zoom changes without a transformer.
Cover evidence is weighted above title similarity; latest numeric chapters provide supporting
evidence when providers differ by no more than two chapters. Every proposal remains operator-reviewed
regardless of score. Pending decisions are rescored after a signature is created or replaced, and
accepted/rejected labels are snapshotted before identities are merged or deleted.

Scoring evaluates every provider cover attached to either canonical manga and stores the exact
source-identity pair that produced the strongest evidence. Suggested Matches renders that pair with
a provenance label, so the visible images always correspond to the displayed cover result. Distinct
same-provider catalog records are collapsed in review only when provider identity agrees, or when
close cached hashes, shared title tokens, and at least five strongly overlapping numeric chapters
agree. This collapse never performs an automatic catalog merge. A bounded, low-priority maintenance
job upgrades older pending decisions to the current scorer so existing suggestions gain the same
cover provenance without doing image work in an HTTP request. Rescoring is read-only, never blocks
a manual merge, and is admitted through a rolling ten-job maintenance window so a scorer-version
change cannot flood the live queue.

## PostgreSQL provider identity repair

Do not run identity repair against an unbacked-up catalog. Capture a PostgreSQL backup and paired
storage manifest, then audit before applying:

```bash
pg_dump --format=custom --file=backups/manga-manager-before-provider-repair.dump "$DATABASE_URL"
find storage-v2 -type f -printf '%P\t%s\n' | sort > backups/storage-before-provider-repair.tsv
UV_CACHE_DIR=/tmp/uv-cache uv run manga-manager repair-provider-identities \
  --report reports/provider-identities-dry-run.json
UV_CACHE_DIR=/tmp/uv-cache uv run manga-manager repair-provider-identities \
  --report reports/provider-identities-applied.json --apply
UV_CACHE_DIR=/tmp/uv-cache uv run manga-manager reconcile-refresh-queue \
  --report reports/refresh-queue-dry-run.json
UV_CACHE_DIR=/tmp/uv-cache uv run manga-manager reconcile-refresh-queue \
  --report reports/refresh-queue-applied.json --apply
```

The provider report consolidates revision-independent Asura identities and same-provider alternate
listings only when the evidence is strong. Alternate listings require the same normalized title, at
least five comparable chapters, at least 90% chapter overlap, and no conflicting shared external ID.
The secondary source ID is retained in `alternate_source_listing_v2`, so later pulls update the
primary identity instead of recreating a canonical manga. Ambiguous provider collisions are
quarantined. The repair also rewrites Asura release URLs, recomputes numeric latest chapters, closes
redundant match decisions, and removes polluted provider aliases. Refresh reconciliation is bounded
to active rows and does not discard frontier-discovered compatible work. Restore the database dump
and its paired storage snapshot together if post-repair validation fails.

Suggested Matches applies the same equivalence policy while rendering its queue. Until the repair is
applied, decisions involving two verified aliases are presented as one proposal and accepting it
merges the complete connected component once. Do not replace this with title-only UI deduplication:
unrelated works can legitimately share a translated title.
