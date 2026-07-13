# Local direct-Docker staging

The host does not need the Compose plugin:

For a persistent development stack without the full rehearsal:

```bash
scripts/stage-local.sh serve --build
scripts/stage-local.sh down
```

For an automatically provisioned local Kavita integration:

```bash
scripts/kavita-local.sh up
scripts/kavita-local.sh credentials
scripts/kavita-local.sh status
scripts/kavita-local.sh down
```

Credentials are stored in ignored `.local/kavita.env` with mode `0600`. Manga Manager is exposed
on port 18000 and Kavita on port 15000. `STAGE_MIN_FREE_BYTES` overrides the 1 GiB staging reserve.

For the full import and rollback rehearsal:

```bash
STAGE_LEGACY_DATABASE="$PWD/manga_manager.db" \
STAGE_LEGACY_STORAGE_ROOT="$PWD/storage" \
STAGE_STORAGE_ROOT="$PWD/storage-v2-stage" \
scripts/stage-local.sh
```

The preflight refuses legacy import unless a source CBZ can be hardlinked into staging and at least
1 GiB remains free. This prevents a copy-based import from filling a mechanical disk. Import reports
are resumable: if a run stops, execute the same command and already activated identities are skipped.
The legacy import container disables the normal 5 GiB production watermark only after this hardlink
preflight; staged web/worker downloads retain the 1 GiB reserve.

The rehearsal builds the selected platform image, migrates PostgreSQL, imports/reconciles content,
starts web and worker under Pi-sized memory limits, runs health/queue/memory checks, rehearses
backup/restore, restarts services, and proves lease recovery. Set `STAGE_SMOKE_SOURCE=mangafire` for
an optional live pull. The integrity step reads every active CBZ and can take a long time on a large
library stored on a mechanical disk. Tear down containers with `scripts/stage-local.sh down`; append
`--volumes` only when the staged database is no longer needed.

Run frontend browser checks against the staged server with:

```bash
cd frontend
PLAYWRIGHT_BASE_URL=http://127.0.0.1:18000 npm run test:browser
```
