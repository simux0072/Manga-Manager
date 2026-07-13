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
