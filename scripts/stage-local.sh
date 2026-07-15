#!/bin/sh
set -eu

project="${STAGE_PROJECT:-manga-manager-stage}"
state_dir="${STAGE_STATE_DIR:-$PWD/.local}"
mkdir -p "$state_dir"
exec 9>"$state_dir/$project-stage.lock"
if ! flock -n 9; then
  echo "another $project startup or teardown is already running" >&2
  exit 75
fi
network="$project-net"
postgres="$project-postgres"
web="$project-web"
worker="$project-worker"
image="$project:local"
data_dir="${STAGE_STORAGE_ROOT:-$PWD/storage-v2-stage}"
db_volume="$project-db"
database_url="postgresql+psycopg://manga:manga@$postgres:5432/manga_manager"
stage_min_free_bytes="${STAGE_MIN_FREE_BYTES:-1073741824}"
mode="${1:-rehearse}"
build_requested=false
if [ "$mode" = "serve" ] && [ "${2:-}" = "--build" ]; then build_requested=true; fi
if [ "$mode" != "serve" ] && [ "$mode" != "down" ]; then build_requested=true; fi
run_repairs="${STAGE_RUN_REPAIRS:-}"
if [ -z "$run_repairs" ]; then
  if [ "$mode" = "serve" ]; then run_repairs=false; else run_repairs=true; fi
fi
sources_enabled="${STAGE_ENABLE_SOURCES:-}"
if [ -z "$sources_enabled" ]; then
  if [ "$mode" = "serve" ] || [ -n "${STAGE_SMOKE_SOURCE:-}" ]; then
    sources_enabled=true
  else
    sources_enabled=false
  fi
fi

remove_postgres() {
  if docker container inspect "$postgres" >/dev/null 2>&1; then
    # PostgreSQL can need several minutes to checkpoint on the supported mechanical-disk host.
    # A forced removal makes the next startup repeat that work as crash recovery.
    docker stop --time "${STAGE_POSTGRES_STOP_SECONDS:-300}" "$postgres" >/dev/null
    docker rm "$postgres" >/dev/null
  fi
}

teardown() {
  docker rm -f "$worker" "$web" 2>/dev/null || true
  remove_postgres 2>/dev/null || true
  docker network rm "$network" 2>/dev/null || true
  if [ "${1:-}" = "--volumes" ]; then
    docker volume rm "$db_volume" 2>/dev/null || true
  fi
}

wait_for_job() {
  job_id="$1"
  attempts=0
  while :; do
    status=$(docker exec "$postgres" psql -U manga -d manga_manager -Atc \
      "SELECT status FROM job WHERE id=$job_id")
    case "$status" in
      succeeded) return 0 ;;
      failed|cancelled) docker exec "$postgres" psql -U manga -d manga_manager -c \
        "SELECT id,status,error_code,error_message FROM job WHERE id=$job_id"; return 1 ;;
    esac
    attempts=$((attempts + 1))
    [ "$attempts" -lt "${STAGE_JOB_WAIT_ATTEMPTS:-120}" ] || return 1
    sleep 1
  done
}

wait_for_kind() {
  kind="$1"
  attempts=0
  while :; do
    active=$(docker exec "$postgres" psql -U manga -d manga_manager -Atc \
      "SELECT count(*) FROM job WHERE kind='$kind' AND status IN ('queued','leased','retry_wait')")
    failed=$(docker exec "$postgres" psql -U manga -d manga_manager -Atc \
      "SELECT count(*) FROM job WHERE kind='$kind' AND status='failed'")
    [ "${failed:-0}" -eq 0 ] || {
      docker exec "$postgres" psql -U manga -d manga_manager -c \
        "SELECT id,status,error_code,error_message FROM job WHERE kind='$kind' AND status='failed'"
      return 1
    }
    [ "${active:-0}" -gt 0 ] || return 0
    attempts=$((attempts + 1))
    [ "$attempts" -lt "${STAGE_REPAIR_WAIT_ATTEMPTS:-7200}" ] || return 1
    sleep 1
  done
}

if [ "$mode" = "down" ]; then
  teardown "${2:-}"
  exit 0
fi

mkdir -p "$data_dir"
if [ -n "${STAGE_LEGACY_DATABASE:-}" ]; then
  legacy_storage_preflight=$(cd "${STAGE_LEGACY_STORAGE_ROOT:-storage}" && pwd)
  sample_file=$(find "$legacy_storage_preflight" -type f -name '*.cbz' -print -quit)
  [ -n "$sample_file" ] || {
    echo "legacy staging preflight failed: no CBZ files under $legacy_storage_preflight" >&2
    exit 1
  }
  link_probe="$data_dir/.hardlink-preflight.$$"
  if ! ln "$sample_file" "$link_probe" 2>/dev/null; then
    echo "legacy staging preflight failed: source and staging storage do not support hardlinks" >&2
    echo "refusing a copy-based import that could exhaust the disk" >&2
    exit 1
  fi
  rm -f "$link_probe"
  free_kib=$(df -Pk "$data_dir" | awk 'NR==2 {print $4}')
  [ "${free_kib:-0}" -ge 1048576 ] || {
    echo "legacy staging preflight failed: less than 1 GiB free for database and cover metadata" >&2
    exit 1
  }
fi
if [ "$build_requested" = true ] || ! docker image inspect "$image" >/dev/null 2>&1; then
  docker build --platform "${STAGE_PLATFORM:-linux/amd64}" -t "$image" .
fi
docker network inspect "$network" >/dev/null 2>&1 || docker network create "$network" >/dev/null
docker volume inspect "$db_volume" >/dev/null 2>&1 || docker volume create "$db_volume" >/dev/null
docker rm -f "$web" "$worker" >/dev/null 2>&1 || true
remove_postgres
docker run -d --name "$postgres" --network "$network" --memory 384m \
  --log-opt max-size=10m --log-opt max-file=3 \
  -e POSTGRES_DB=manga_manager -e POSTGRES_USER=manga -e POSTGRES_PASSWORD=manga \
  -v "$db_volume:/var/lib/postgresql/data" postgres:16-alpine >/dev/null

attempt=0
until docker exec "$postgres" pg_isready -h 127.0.0.1 -U manga -d manga_manager >/dev/null 2>&1; do
  attempt=$((attempt + 1)); [ "$attempt" -lt "${STAGE_POSTGRES_WAIT_ATTEMPTS:-300}" ] || {
    docker logs "$postgres"
    exit 1
  }
  sleep 1
done

docker run --rm --network "$network" --memory 256m -e "V2_DATABASE_URL=$database_url" "$image" \
  manga-manager migrate

if [ -n "${STAGE_IMPORT_ROOT:-}" ]; then
  import_root=$(cd "$STAGE_IMPORT_ROOT" && pwd)
  docker run --rm --network "$network" --memory 256m -e "V2_DATABASE_URL=$database_url" \
    -e V2_STORAGE_ROOT=/data -v "$data_dir:/data" -v "$import_root:/import:ro" "$image" \
    manga-manager import-cbz /import --report /data/stage-import-report.json
  docker run --rm --network "$network" --memory 256m -e "V2_DATABASE_URL=$database_url" \
    -e V2_STORAGE_ROOT=/data -v "$data_dir:/data" "$image" \
    manga-manager reconcile-storage
fi
if [ -n "${STAGE_LEGACY_DATABASE:-}" ]; then
  legacy_database=$(cd "$(dirname "$STAGE_LEGACY_DATABASE")" && pwd)/$(basename "$STAGE_LEGACY_DATABASE")
  legacy_storage=$(cd "${STAGE_LEGACY_STORAGE_ROOT:-storage}" && pwd)
  case "$legacy_database:$legacy_storage:$data_dir" in
    "$PWD"/*:"$PWD"/*:"$PWD"/*)
      container_database="/host/${legacy_database#"$PWD"/}"
      container_storage="/host/${legacy_storage#"$PWD"/}"
      container_data="/host/${data_dir#"$PWD"/}"
      docker run --rm --network "$network" --memory 256m -e "V2_DATABASE_URL=$database_url" \
        -e "V2_STORAGE_ROOT=$container_data" -e V2_MIN_FREE_BYTES=0 \
        -v "$PWD:/host" "$image" manga-manager migrate-legacy-library "$container_database" \
        --storage-root "$container_storage" \
        --report "$container_data/legacy-library-import.json" --apply
      docker run --rm --network "$network" --memory 256m -e "V2_DATABASE_URL=$database_url" \
        -e "V2_STORAGE_ROOT=$container_data" -v "$PWD:/host" "$image" \
        manga-manager repair-catalog-recovery "$container_database" \
        --report "$container_data/catalog-recovery.json" --apply
      ;;
    *)
      docker run --rm --network "$network" --memory 256m -e "V2_DATABASE_URL=$database_url" \
        -e V2_STORAGE_ROOT=/data -e V2_MIN_FREE_BYTES=0 -v "$data_dir:/data" \
        -v "$legacy_database:/legacy/catalog.db:ro" \
        -v "$legacy_storage:/legacy/storage:ro" "$image" manga-manager \
        migrate-legacy-library /legacy/catalog.db --storage-root /legacy/storage \
        --report /data/legacy-library-import.json --apply
      docker run --rm --network "$network" --memory 256m -e "V2_DATABASE_URL=$database_url" \
        -e V2_STORAGE_ROOT=/data -v "$data_dir:/data" \
        -v "$legacy_database:/legacy/catalog.db:ro" "$image" \
        manga-manager repair-catalog-recovery /legacy/catalog.db \
        --report /data/catalog-recovery.json --apply
      ;;
  esac
  docker run --rm --network "$network" --memory 256m -e "V2_DATABASE_URL=$database_url" \
    -e V2_STORAGE_ROOT=/data -e V2_MIN_FREE_BYTES=0 -v "$data_dir:/data" "$image" \
    manga-manager reconcile-storage
fi
if [ "$run_repairs" = true ]; then
  docker run --rm --network "$network" --memory 256m -e "V2_DATABASE_URL=$database_url" \
    -e V2_STORAGE_ROOT=/data -v "$data_dir:/data" "$image" manga-manager \
    repair-provider-identities --report /data/provider-identities-dry-run.json
  docker run --rm --network "$network" --memory 256m -e "V2_DATABASE_URL=$database_url" \
    -e V2_STORAGE_ROOT=/data -v "$data_dir:/data" "$image" manga-manager \
    repair-provider-identities --report /data/provider-identities-applied.json --apply
  docker run --rm --network "$network" --memory 256m -e "V2_DATABASE_URL=$database_url" \
    -e V2_STORAGE_ROOT=/data -v "$data_dir:/data" "$image" manga-manager \
    reconcile-refresh-queue --report /data/refresh-queue-dry-run.json
  docker run --rm --network "$network" --memory 256m -e "V2_DATABASE_URL=$database_url" \
    -e V2_STORAGE_ROOT=/data -v "$data_dir:/data" "$image" manga-manager \
    reconcile-refresh-queue --report /data/refresh-queue-applied.json --apply
fi
docker run -d --name "$web" --network "$network" --memory 256m -p "${STAGE_PORT:-18000}:8000" \
  --log-opt max-size=10m --log-opt max-file=3 \
  -e "V2_DATABASE_URL=$database_url" -e V2_STORAGE_ROOT=/data \
  -e "V2_ENABLE_ASURA=$sources_enabled" -e "V2_ENABLE_MANGAFIRE=$sources_enabled" \
  -e "V2_ENABLE_KINGOFSHOJO=$sources_enabled" \
  -e "V2_MIN_FREE_BYTES=$stage_min_free_bytes" \
  -e "KAVITA_URL=${KAVITA_URL:-}" -e "KAVITA_API_KEY=${KAVITA_API_KEY:-}" \
  -e "KAVITA_LIBRARY_ROOT=${KAVITA_LIBRARY_ROOT:-}" \
  -v "$data_dir:/data" "$image" \
  uvicorn manga_manager.web.app:app --host 0.0.0.0 --port 8000 >/dev/null
docker run -d --name "$worker" --network "$network" --memory 1g \
  --log-opt max-size=10m --log-opt max-file=3 \
  --health-cmd "manga-manager doctor" --health-interval 30s \
  --health-timeout 10s --health-start-period 30s --health-retries 3 \
  -e "V2_DATABASE_URL=$database_url" -e V2_STORAGE_ROOT=/data \
  -e "V2_ENABLE_ASURA=$sources_enabled" -e "V2_ENABLE_MANGAFIRE=$sources_enabled" \
  -e "V2_ENABLE_KINGOFSHOJO=$sources_enabled" \
  -e "V2_MIN_FREE_BYTES=$stage_min_free_bytes" \
  -e "KAVITA_URL=${KAVITA_URL:-}" -e "KAVITA_API_KEY=${KAVITA_API_KEY:-}" \
  -e "KAVITA_LIBRARY_ROOT=${KAVITA_LIBRARY_ROOT:-}" \
  -v "$data_dir:/data" "$image" \
  manga-manager worker >/dev/null

attempt=0
until docker exec "$web" python -c "import json,urllib.request; assert json.load(urllib.request.urlopen('http://127.0.0.1:8000/healthz'))['ok']" >/dev/null 2>&1; do
  attempt=$((attempt + 1)); [ "$attempt" -lt "${STAGE_WEB_WAIT_ATTEMPTS:-120}" ] || {
    docker logs "$web"
    exit 1
  }
  sleep 1
done
if [ "$mode" = "serve" ]; then
  printf '%s\n' "Manga Manager: http://127.0.0.1:${STAGE_PORT:-18000}" \
    "Stop: scripts/stage-local.sh down"
  exit 0
fi
if [ -n "${STAGE_LEGACY_DATABASE:-}" ]; then
  wait_for_kind library_repair
fi
if [ -n "${KAVITA_URL:-}" ] && [ -n "${KAVITA_API_KEY:-}" ]; then
  wait_for_kind kavita_sync
fi
docker run --rm --network "$network" --memory 256m -e "V2_DATABASE_URL=$database_url" \
  -e V2_STORAGE_ROOT=/data -v "$data_dir:/data" "$image" manga-manager stage-check --json

probe_output=$(docker run --rm --network "$network" -e "V2_DATABASE_URL=$database_url" "$image" \
  manga-manager enqueue-probe)
probe_id=$(printf '%s\n' "$probe_output" | sed -n 's/^job_id=\([0-9][0-9]*\).*/\1/p')
[ -n "$probe_id" ] && wait_for_job "$probe_id"
docker run --rm --memory 1g "$image" python scripts/stress-download-memory.py

if [ -n "${STAGE_SMOKE_SOURCE:-}" ]; then
  smoke_output=$(docker run --rm --network "$network" -e "V2_DATABASE_URL=$database_url" "$image" \
    manga-manager enqueue-pull "$STAGE_SMOKE_SOURCE")
  smoke_id=$(printf '%s\n' "$smoke_output" | sed -n 's/^job_id=\([0-9][0-9]*\).*/\1/p')
  [ -n "$smoke_id" ] && wait_for_job "$smoke_id"
  docker run --rm --network "$network" -e "V2_DATABASE_URL=$database_url" "$image" \
    manga-manager stage-check --json
fi

# Rehearse a logical backup/restore into a disposable sibling database.
docker exec "$postgres" pg_dump -U manga -d manga_manager -Fc -f /tmp/stage-rollback.dump
docker exec "$postgres" dropdb -U manga --if-exists manga_manager_restore
docker exec "$postgres" createdb -U manga manga_manager_restore
docker exec "$postgres" pg_restore --single-transaction -U manga -d manga_manager_restore \
  /tmp/stage-rollback.dump
docker exec "$postgres" psql -U manga -d manga_manager_restore -Atc "SELECT version_num FROM alembic_version"
docker exec "$postgres" dropdb -U manga manga_manager_restore

docker restart "$web" "$worker" >/dev/null
attempt=0
until docker exec "$web" python -c "import json,urllib.request; assert json.load(urllib.request.urlopen('http://127.0.0.1:8000/healthz'))['ok']" >/dev/null 2>&1; do
  attempt=$((attempt + 1)); [ "$attempt" -lt "${STAGE_WEB_WAIT_ATTEMPTS:-120}" ] || {
    docker logs "$web"
    exit 1
  }
  sleep 1
done
recovery_output=$(docker run --rm --network "$network" -e "V2_DATABASE_URL=$database_url" "$image" \
  manga-manager enqueue-probe)
recovery_id=$(printf '%s\n' "$recovery_output" | sed -n 's/^job_id=\([0-9][0-9]*\).*/\1/p')
[ -n "$recovery_id" ] && wait_for_job "$recovery_id"
printf '%s\n' "staging ready: http://127.0.0.1:${STAGE_PORT:-18000}" "teardown: scripts/stage-local.sh down"
