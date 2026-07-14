# Local direct-Docker staging

The host does not need the Compose plugin:

Set `STAGE_PROJECT` to isolate all container, network, image, and volume names when another staging
stack is running. For example, use
`STAGE_PROJECT=manga-manager-check STAGE_STORAGE_ROOT=/tmp/manga-manager-check scripts/stage-local.sh`.
Use the same project value for teardown: `STAGE_PROJECT=manga-manager-check
scripts/stage-local.sh down --volumes`.

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
on port 18000 and Kavita on port 15000. Kavita reads only `kavita-library/`; untracked series are
removed from that projection while content-addressed blobs remain intact. `STAGE_MIN_FREE_BYTES`
overrides the 1 GiB staging reserve. Web readiness retries quietly for 120 seconds; set
`STAGE_WEB_WAIT_ATTEMPTS` to a larger number on unusually slow storage.

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
recovers legacy tracking, runs provider-identity and refresh-payload repair in dry-run/apply pairs,
normalizes canonical archive metadata, starts web and worker under Pi-sized memory limits, runs
health/queue/memory checks, rehearses
backup/restore, restarts services, and proves lease recovery. Set `STAGE_SMOKE_SOURCE=mangafire` for
an optional live pull. The integrity step reads every active CBZ and can take a long time on a large
library stored on a mechanical disk. Tear down containers with `scripts/stage-local.sh down`; append
`--volumes` only when the staged database is no longer needed.

Full rehearsals disable scheduled live sources unless `STAGE_SMOKE_SOURCE` or
`STAGE_ENABLE_SOURCES=true` is set. Persistent `serve` mode enables them by default.

The full rehearsal waits for `library_repair` jobs before its integrity check. On a mechanical disk,
raise `STAGE_REPAIR_WAIT_ATTEMPTS` if canonical metadata rewriting legitimately takes over two hours.
`serve` mode does not wait; the Operations page shows repair and Kavita progress in the background.
The standalone `stage-check` prints archive-validation progress to stderr and one final JSON result
to stdout. It now fails fast with `"busy": true` while chapter downloads, `library_repair`, or
Kavita synchronization jobs are queued, leased, or waiting to retry; those jobs can change files or
projections during a slow scan. Wait for them to settle, briefly stop the worker, and run the check
from the web container for a stable result. Failure output includes counts and at most 25 paths per
category by default. Pass `--full-details` only when a complete machine-readable failure manifest is
needed.

Run frontend browser checks against the staged server with:

```bash
cd frontend
PLAYWRIGHT_BASE_URL=http://127.0.0.1:18000 npm run test:browser
```

For an unattended final rehearsal plus ARM64 image/runtime check, run
`scripts/final-validation.sh`. It uses isolated staging names, writes timestamped files under
`logs/`, continues to the ARM check if staging fails, captures container diagnostics, and cleans up
only its isolated staging containers and volume.

After a repair and Kavita sync completes, an individual series/chapter cover pair can be checked
byte-for-byte with the IDs shown by Kavita's API:

```bash
set -a
. .local/kavita.env
set +a
UV_CACHE_DIR=/tmp/uv-cache uv run python scripts/kavita-cover-check.py \
  --url http://127.0.0.1:15000 --api-key "$KAVITA_API_KEY" \
  --series-id SERIES_ID --chapter-id CHAPTER_ID
```

The check fails if Kavita still exposes its generated first-page thumbnail for the chapter.
