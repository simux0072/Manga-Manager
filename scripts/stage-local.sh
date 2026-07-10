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
docker run -d --name "$web" --network "$network" --memory 256m -p "${STAGE_PORT:-18000}:8000" \
  -e "V2_DATABASE_URL=$database_url" -e V2_STORAGE_ROOT=/data -v "$data_dir:/data" "$image" \
  uv run --frozen uvicorn manga_manager.web.app:app --host 0.0.0.0 --port 8000 >/dev/null
docker run -d --name "$worker" --network "$network" --memory 1g \
  -e "V2_DATABASE_URL=$database_url" -e V2_STORAGE_ROOT=/data -v "$data_dir:/data" "$image" \
  uv run --frozen manga-manager worker >/dev/null

attempt=0
until docker exec "$web" uv run --frozen python -c "import json,urllib.request; assert json.load(urllib.request.urlopen('http://127.0.0.1:8000/healthz'))['ok']"; do
  attempt=$((attempt + 1)); [ "$attempt" -lt 30 ] || { docker logs "$web"; exit 1; }
  sleep 1
done
docker run --rm --network "$network" --memory 256m -e "V2_DATABASE_URL=$database_url" \
  -e V2_STORAGE_ROOT=/data -v "$data_dir:/data" "$image" uv run --frozen manga-manager stage-check --json
printf '%s\n' "staging ready: http://127.0.0.1:${STAGE_PORT:-18000}" "teardown: scripts/stage-local.sh down"
