# PostgreSQL backup and restore

Pair every database backup with a snapshot or backup of `V2_STORAGE_ROOT`. Stop the worker before
the final pair so no artifact row can be committed between the two snapshots.

For the local pre-reset archive, prefer `scripts/reset-local-data.sh archive`. It creates a custom
dump, SHA-256 checksum, credential-redacted diagnostic bundle, and proves the dump can be restored
before reporting success. Its output under `local-archives/` is ignored by Git.

```bash
docker exec manga-manager-postgres pg_dump -U manga -d manga_manager -Fc \
  -f /tmp/manga-manager.dump
docker cp manga-manager-postgres:/tmp/manga-manager.dump backups/manga-manager.dump
```

Restore into a new database first, migrate it, and run `stage-check` before changing service URLs:

```bash
docker exec manga-manager-postgres createdb -U manga manga_manager_restore
docker cp backups/manga-manager.dump manga-manager-postgres:/tmp/manga-manager.dump
docker exec manga-manager-postgres pg_restore -U manga -d manga_manager_restore \
  /tmp/manga-manager.dump
V2_DATABASE_URL=postgresql+psycopg://manga:manga@127.0.0.1:5432/manga_manager_restore \
  UV_CACHE_DIR=/tmp/uv-cache uv run manga-manager migrate
```

Restore the paired storage tree before starting workers. Rollback means stopping web/worker,
pointing both at the previous database and storage pair, then starting web before worker and running
`stage-check --json`.
