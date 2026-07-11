#!/bin/sh
set -eu

project=manga-manager-stage
network="$project-net"
postgres="$project-postgres"
web="$project-web"
worker="$project-worker"
image="$project:local"
data_dir="${STAGE_STORAGE_ROOT:-$PWD/storage-v2-stage}"
db_volume="$project-db"
database_url="postgresql+psycopg://manga:manga@$postgres:5432/manga_manager"

teardown() {
  docker rm -f "$worker" "$web" "$postgres" 2>/dev/null || true
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

if [ "${1:-}" = "down" ]; then
  teardown "${2:-}"
  exit 0
fi

mkdir -p "$data_dir"
docker build --platform "${STAGE_PLATFORM:-linux/amd64}" -t "$image" .
docker network inspect "$network" >/dev/null 2>&1 || docker network create "$network" >/dev/null
docker volume inspect "$db_volume" >/dev/null 2>&1 || docker volume create "$db_volume" >/dev/null
docker rm -f "$postgres" "$web" "$worker" 2>/dev/null || true
docker run -d --name "$postgres" --network "$network" --memory 384m \
  -e POSTGRES_DB=manga_manager -e POSTGRES_USER=manga -e POSTGRES_PASSWORD=manga \
  -v "$db_volume:/var/lib/postgresql/data" postgres:16-alpine >/dev/null

attempt=0
until docker exec "$postgres" pg_isready -h 127.0.0.1 -U manga -d manga_manager >/dev/null 2>&1; do
  attempt=$((attempt + 1)); [ "$attempt" -lt 40 ] || { docker logs "$postgres"; exit 1; }
  sleep 1
done

docker run --rm --network "$network" --memory 256m -e "V2_DATABASE_URL=$database_url" "$image" \
  uv run --frozen manga-manager migrate

if [ -n "${STAGE_IMPORT_ROOT:-}" ]; then
  import_root=$(cd "$STAGE_IMPORT_ROOT" && pwd)
  docker run --rm --network "$network" --memory 256m -e "V2_DATABASE_URL=$database_url" \
    -e V2_STORAGE_ROOT=/data -v "$data_dir:/data" -v "$import_root:/import:ro" "$image" \
    uv run --frozen manga-manager import-cbz /import --report /data/stage-import-report.json
  docker run --rm --network "$network" --memory 256m -e "V2_DATABASE_URL=$database_url" \
    -e V2_STORAGE_ROOT=/data -v "$data_dir:/data" "$image" \
    uv run --frozen manga-manager reconcile-storage
fi
docker run -d --name "$web" --network "$network" --memory 256m -p "${STAGE_PORT:-18000}:8000" \
  -e "V2_DATABASE_URL=$database_url" -e V2_STORAGE_ROOT=/data -v "$data_dir:/data" "$image" \
  uv run --frozen uvicorn manga_manager.web.app:app --host 0.0.0.0 --port 8000 >/dev/null
docker run -d --name "$worker" --network "$network" --memory 1g \
  --health-cmd "uv run --frozen manga-manager doctor" --health-interval 30s \
  --health-timeout 10s --health-start-period 30s --health-retries 3 \
  -e "V2_DATABASE_URL=$database_url" -e V2_STORAGE_ROOT=/data -v "$data_dir:/data" "$image" \
  uv run --frozen manga-manager worker >/dev/null

attempt=0
until docker exec "$web" uv run --frozen python -c "import json,urllib.request; assert json.load(urllib.request.urlopen('http://127.0.0.1:8000/healthz'))['ok']"; do
  attempt=$((attempt + 1)); [ "$attempt" -lt 30 ] || { docker logs "$web"; exit 1; }
  sleep 1
done
docker run --rm --network "$network" --memory 256m -e "V2_DATABASE_URL=$database_url" \
  -e V2_STORAGE_ROOT=/data -v "$data_dir:/data" "$image" uv run --frozen manga-manager stage-check --json

probe_output=$(docker run --rm --network "$network" -e "V2_DATABASE_URL=$database_url" "$image" \
  uv run --frozen manga-manager enqueue-probe)
probe_id=$(printf '%s\n' "$probe_output" | sed -n 's/^job_id=\([0-9][0-9]*\).*/\1/p')
[ -n "$probe_id" ] && wait_for_job "$probe_id"
docker run --rm --memory 1g "$image" uv run --frozen python scripts/stress-download-memory.py

if [ -n "${STAGE_SMOKE_SOURCE:-}" ]; then
  smoke_output=$(docker run --rm --network "$network" -e "V2_DATABASE_URL=$database_url" "$image" \
    uv run --frozen manga-manager enqueue-pull "$STAGE_SMOKE_SOURCE")
  smoke_id=$(printf '%s\n' "$smoke_output" | sed -n 's/^job_id=\([0-9][0-9]*\).*/\1/p')
  [ -n "$smoke_id" ] && wait_for_job "$smoke_id"
  docker run --rm --network "$network" -e "V2_DATABASE_URL=$database_url" "$image" \
    uv run --frozen manga-manager stage-check --json
fi

# Rehearse a logical backup/restore into a disposable sibling database.
docker exec "$postgres" pg_dump -U manga -d manga_manager -Fc -f /tmp/stage-rollback.dump
docker exec "$postgres" dropdb -U manga --if-exists manga_manager_restore
docker exec "$postgres" createdb -U manga manga_manager_restore
docker exec "$postgres" pg_restore -U manga -d manga_manager_restore /tmp/stage-rollback.dump
docker exec "$postgres" psql -U manga -d manga_manager_restore -Atc "SELECT version_num FROM alembic_version"
docker exec "$postgres" dropdb -U manga manga_manager_restore

docker restart "$web" "$worker" >/dev/null
attempt=0
until docker exec "$web" uv run --frozen python -c "import json,urllib.request; assert json.load(urllib.request.urlopen('http://127.0.0.1:8000/healthz'))['ok']"; do
  attempt=$((attempt + 1)); [ "$attempt" -lt 30 ] || { docker logs "$web"; exit 1; }
  sleep 1
done
recovery_output=$(docker run --rm --network "$network" -e "V2_DATABASE_URL=$database_url" "$image" \
  uv run --frozen manga-manager enqueue-probe)
recovery_id=$(printf '%s\n' "$recovery_output" | sed -n 's/^job_id=\([0-9][0-9]*\).*/\1/p')
[ -n "$recovery_id" ] && wait_for_job "$recovery_id"
printf '%s\n' "staging ready: http://127.0.0.1:${STAGE_PORT:-18000}" "teardown: scripts/stage-local.sh down"
