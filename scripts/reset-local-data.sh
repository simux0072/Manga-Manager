#!/bin/sh
set -eu

command="${1:-preview}"
shift || true
project="${STAGE_PROJECT:-manga-manager-stage}"
postgres="$project-postgres"
web="$project-web"
worker="$project-worker"
network="$project-net"
db_volume="$project-db"
image="$project:local"
kavita_container="${KAVITA_CONTAINER:-$project-kavita}"
kavita_volume="${KAVITA_CONFIG_VOLUME:-$project-kavita-config}"
legacy_kavita_container=""
legacy_kavita_volume=""
if [ "$project" = "manga-manager-stage" ] && [ -z "${KAVITA_CONTAINER:-}" ] && \
  [ -z "${KAVITA_CONFIG_VOLUME:-}" ]; then
  legacy_kavita_container=manga-manager-kavita
  legacy_kavita_volume=manga-manager-kavita-config
fi
storage="${STAGE_STORAGE_ROOT:-$PWD/storage-v2-stage}"
state_dir="${STAGE_STATE_DIR:-$PWD/.local}"
archive_dir=""
include_legacy=false
confirmed=false

while [ "$#" -gt 0 ]; do
  case "$1" in
    --archive-dir) archive_dir="$2"; shift 2 ;;
    --include-legacy) include_legacy=true; shift ;;
    --yes) confirmed=true; shift ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

usage() {
  cat <<'EOF'
usage: scripts/reset-local-data.sh preview
       scripts/reset-local-data.sh archive [--archive-dir PATH]
       scripts/reset-local-data.sh apply --archive-dir PATH --yes [--include-legacy]

archive stops staging writers, creates a PostgreSQL custom dump and diagnostic JSON, and proves the
dump can be restored. apply removes only the named staging project by default. --include-legacy also
removes the repository's ignored legacy SQLite databases, backups, pictures, logs, and storage/.
The verified local archive is never removed by apply.
EOF
}

safe_repo_path() {
  candidate=$(realpath -m "$1")
  repository=$(realpath "$PWD")
  case "$candidate" in
    "$repository"/*) printf '%s\n' "$candidate" ;;
    *) echo "refusing path outside repository: $candidate" >&2; exit 1 ;;
  esac
}

show_preview() {
  printf '%s\n' "Project: $project" "Staging storage: $(safe_repo_path "$storage")" \
    "State directory: $(safe_repo_path "$state_dir")" \
    "Application image: $image" "PostgreSQL volume: $db_volume" \
    "Kavita volume: $kavita_volume" \
    "Legacy data included: $include_legacy"
  if [ -n "$legacy_kavita_container" ]; then
    printf '%s\n' "Legacy Kavita container: $legacy_kavita_container" \
      "Legacy Kavita volume: $legacy_kavita_volume"
  fi
  for path in "$storage" "$state_dir"; do
    [ -e "$path" ] && du -sh "$path" || true
  done
  if [ "$include_legacy" = true ]; then
    for path in storage storage-v2 backups pictures logs manga_manager.db manga_manager.db.bak-* \
      frontend/manga_manager.db frontend/test-results frontend/dist alembic; do
      [ -e "$path" ] && du -sh "$path" || true
    done
  fi
}

ensure_postgres() {
  if docker container inspect "$postgres" >/dev/null 2>&1; then
    docker start "$postgres" >/dev/null
  else
    docker volume inspect "$db_volume" >/dev/null 2>&1 || {
      echo "PostgreSQL volume not found: $db_volume" >&2
      exit 1
    }
    docker network inspect "$network" >/dev/null 2>&1 || docker network create "$network" >/dev/null
    docker run -d --name "$postgres" --network "$network" --memory 384m \
      --log-opt max-size=10m --log-opt max-file=3 \
      -e POSTGRES_DB=manga_manager -e POSTGRES_USER=manga -e POSTGRES_PASSWORD=manga \
      -v "$db_volume:/var/lib/postgresql/data" postgres:16-alpine >/dev/null
  fi
  attempts=0
  until docker exec "$postgres" pg_isready -U manga -d manga_manager >/dev/null 2>&1; do
    attempts=$((attempts + 1))
    [ "$attempts" -lt 120 ] || { docker logs "$postgres"; exit 1; }
    sleep 1
  done
}

require_migrated_database() {
  migration_table=$(docker exec "$postgres" psql -U manga -d manga_manager -Atc \
    "SELECT to_regclass('public.alembic_version')")
  [ "$migration_table" = "alembic_version" ] || {
    echo "database is not migrated; run 'manga-manager migrate' before archiving" >&2
    exit 1
  }
}

create_archive() {
  timestamp=$(date -u +%Y%m%dT%H%M%SZ)
  [ -n "$archive_dir" ] || archive_dir="$PWD/local-archives/pre-reset-$timestamp"
  archive_dir=$(safe_repo_path "$archive_dir")
  mkdir -p "$archive_dir"
  chmod 700 "$archive_dir"

  docker stop "$worker" "$web" "$kavita_container" ${legacy_kavita_container:+"$legacy_kavita_container"} \
    >/dev/null 2>&1 || true
  ensure_postgres
  require_migrated_database
  docker image inspect "$image" >/dev/null 2>&1 || {
    echo "application image not found: $image (build it before archiving)" >&2
    exit 1
  }

  dump_name="manga-manager-pre-reset.dump"
  docker exec "$postgres" pg_dump -U manga -d manga_manager -Fc -f "/tmp/$dump_name"
  docker cp "$postgres:/tmp/$dump_name" "$archive_dir/$dump_name" >/dev/null
  (cd "$archive_dir" && sha256sum "$dump_name" >"$dump_name.sha256")

  mkdir -p "$storage"
  docker run --rm --user "$(id -u):$(id -g)" --network "$network" \
    -e V2_DATABASE_URL="postgresql+psycopg://manga:manga@$postgres:5432/manga_manager" \
    -e V2_STORAGE_ROOT=/data -v "$storage:/data:ro" -v "$archive_dir:/archive" "$image" \
    manga-manager diagnostic-bundle --output /archive/diagnostics.json --recent-failures 200

  restore_db="manga_manager_reset_verify"
  docker exec "$postgres" dropdb -U manga --if-exists "$restore_db"
  docker exec "$postgres" createdb -U manga "$restore_db"
  docker cp "$archive_dir/$dump_name" "$postgres:/tmp/$dump_name" >/dev/null
  docker exec "$postgres" pg_restore --single-transaction -U manga -d "$restore_db" \
    "/tmp/$dump_name"
  migration=$(docker exec "$postgres" psql -U manga -d "$restore_db" -Atc \
    "SELECT version_num FROM alembic_version")
  docker exec "$postgres" dropdb -U manga "$restore_db"
  {
    printf 'created_at=%s\n' "$timestamp"
    printf 'git_commit=%s\n' "$(git rev-parse HEAD)"
    printf 'migration=%s\n' "$migration"
    printf 'project=%s\n' "$project"
    printf 'dump=%s\n' "$dump_name"
    printf 'diagnostics=diagnostics.json\n'
  } >"$archive_dir/manifest.txt"
  chmod 600 "$archive_dir"/*
  printf 'verified_archive=%s\n' "$archive_dir"
}

verify_archive() {
  [ -n "$archive_dir" ] || { echo "--archive-dir is required" >&2; exit 2; }
  archive_dir=$(safe_repo_path "$archive_dir")
  [ -f "$archive_dir/manga-manager-pre-reset.dump" ]
  [ -f "$archive_dir/manga-manager-pre-reset.dump.sha256" ]
  [ -f "$archive_dir/diagnostics.json" ]
  [ -f "$archive_dir/manifest.txt" ]
  (cd "$archive_dir" && sha256sum -c manga-manager-pre-reset.dump.sha256)
  docker run --rm -v "$archive_dir:/archive:ro" postgres:16-alpine \
    pg_restore --list /archive/manga-manager-pre-reset.dump >/dev/null
}

remove_repo_tree() {
  path=$(safe_repo_path "$1")
  [ -e "$path" ] || return 0
  rm -rf -- "$path"
}

apply_reset() {
  [ "$confirmed" = true ] || {
    echo "apply requires --yes after reviewing preview output" >&2
    exit 2
  }
  verify_archive
  docker rm -f "$worker" "$web" "$postgres" "$kavita_container" \
    ${legacy_kavita_container:+"$legacy_kavita_container"} >/dev/null 2>&1 || true
  docker volume rm "$db_volume" "$kavita_volume" \
    ${legacy_kavita_volume:+"$legacy_kavita_volume"} >/dev/null 2>&1 || true
  docker network rm "$network" >/dev/null 2>&1 || true
  docker image rm "$image" >/dev/null 2>&1 || true
  remove_repo_tree "$storage"
  remove_repo_tree "$state_dir"
  if [ "$include_legacy" = true ]; then
    remove_repo_tree storage
    remove_repo_tree storage-v2
    remove_repo_tree backups
    remove_repo_tree pictures
    remove_repo_tree logs
    remove_repo_tree frontend/test-results
    remove_repo_tree frontend/dist
    remove_repo_tree alembic
    for path in manga_manager.db manga_manager.db-shm manga_manager.db-wal manga_manager.db.bak-*; do
      [ -e "$path" ] && rm -f -- "$path"
    done
    for path in frontend/manga_manager.db frontend/manga_manager.db-shm \
      frontend/manga_manager.db-wal frontend/tsconfig.app.tsbuildinfo \
      frontend/tsconfig.node.tsbuildinfo frontend/vite.config.js frontend/vite.config.d.ts; do
      [ -e "$path" ] && rm -f -- "$path"
    done
  fi
  printf 'reset_complete=true archive=%s\n' "$archive_dir"
}

case "$command" in
  preview) show_preview ;;
  archive) create_archive ;;
  apply) apply_reset ;;
  -h|--help|help) usage ;;
  *) usage >&2; exit 2 ;;
esac
