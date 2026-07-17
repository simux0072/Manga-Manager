#!/bin/sh
set -eu

command="${1:-status}"
root="${TEST_ENV_ROOT:-$PWD/.local/test-environment}"
project="${TEST_ENV_PROJECT:-manga-manager-test}"
storage="$root/storage"
state="$root/state"
port="${TEST_ENV_PORT:-18001}"
kavita_port="${TEST_KAVITA_PORT:-15001}"
postgres="$project-postgres"
web="$project-web"
worker="$project-worker"
network="$project-net"
image="$project:local"
kavita_container="$project-kavita"
kavita_volume="$project-kavita-config"
database_url="postgresql+psycopg://manga:manga@$postgres:5432/manga_manager"

safe_test_path() {
  candidate=$(realpath -m "$1")
  repository=$(realpath "$PWD")
  case "$candidate" in
    "$repository"/*) printf '%s\n' "$candidate" ;;
    *) echo "refusing test path outside repository: $candidate" >&2; exit 1 ;;
  esac
}

root=$(safe_test_path "$root")

remove_test_root() {
  # Worker and seed containers deliberately run as root, so generated bind-mounted files can be
  # root-owned. Use the already-built test image to clear only the path guarded by safe_test_path.
  if [ -d "$root" ] && docker image inspect "$image" >/dev/null 2>&1; then
    docker run --rm -v "$root:/cleanup" "$image" sh -c \
      'rm -rf /cleanup/* /cleanup/.[!.]* /cleanup/..?*'
  fi
  rm -rf -- "$root"
}

export STAGE_PROJECT="$project"
export STAGE_STORAGE_ROOT="$storage"
export STAGE_STATE_DIR="$state"
export STAGE_PORT="$port"
export STAGE_ENABLE_SOURCES=false
export STAGE_MIN_FREE_BYTES=0
export KAVITA_PORT="$kavita_port"
export KAVITA_CONTAINER="$kavita_container"
export KAVITA_CONFIG_VOLUME="$kavita_volume"
export KAVITA_ENV_FILE="$state/kavita.env"

run_cli() {
  docker run --rm --network "$network" -e "V2_DATABASE_URL=$database_url" \
    -e V2_STORAGE_ROOT=/data -e V2_MIN_FREE_BYTES=0 -v "$storage:/data" "$image" "$@"
}

seed() {
  profile="$1"
  run_cli env MANGA_MANAGER_ALLOW_TEST_SEED=1 python scripts/seed-test-data.py \
    --profile "$profile"
}

wait_for_mutations() {
  attempts=0
  while :; do
    active=$(docker exec "$postgres" psql -U manga -d manga_manager -Atc \
      "SELECT count(*) FROM job WHERE kind IN ('library_repair','kavita_sync') AND status IN ('queued','leased','retry_wait')")
    [ "${active:-0}" -eq 0 ] && return 0
    attempts=$((attempts + 1))
    [ "$attempts" -lt "${TEST_ENV_WAIT_ATTEMPTS:-900}" ] || {
      docker exec "$postgres" psql -U manga -d manga_manager -c \
        "SELECT id,kind,status,error_code,error_message FROM job WHERE kind IN ('library_repair','kavita_sync') AND status IN ('queued','leased','retry_wait','failed') ORDER BY id"
      return 1
    }
    sleep 1
  done
}

wait_for_stage_check() {
  attempts=0
  while :; do
    if output=$(run_cli manga-manager stage-check --json 2>&1); then
      printf '%s\n' "$output"
      return 0
    fi
    printf '%s\n' "$output"
    printf '%s\n' "$output" | grep -q '"busy": true' || return 1
    attempts=$((attempts + 1))
    [ "$attempts" -lt "${TEST_ENV_STAGE_CHECK_ATTEMPTS:-120}" ] || return 1
    wait_for_mutations
    sleep 1
  done
}

case "$command" in
  up)
    mkdir -p "$root"
    KAVITA_BUILD="${TEST_ENV_BUILD:-true}" scripts/kavita-local.sh up
    seed small
    # Prove seed idempotency before the worker mutates the generated catalog.
    seed small
    run_cli manga-manager enqueue-library-repair --all-tracked
    printf '%s\n' "Test Manga Manager: http://127.0.0.1:$port" \
      "Test Kavita: http://127.0.0.1:$kavita_port" \
      "Validate: scripts/test-environment.sh check"
    ;;
  check)
    curl -fsS "http://127.0.0.1:$port/healthz" >/dev/null
    headers=$(curl -fsS -D - -o /dev/null "http://127.0.0.1:$port/api/v2/library?limit=3")
    printf '%s\n' "$headers" | tr -d '\r' | grep -qi '^X-SQL-Query-Count: [0-9][0-9]*$' || {
      echo "API SQL measurement header is missing" >&2
      exit 1
    }
    curl -fsS "http://127.0.0.1:$port/metrics" | \
      grep -q 'route="/api/v2/library"' || {
      echo "API route metrics are missing" >&2
      exit 1
    }
    echo "api_metrics=ok"
    wait_for_mutations
    wait_for_stage_check
    probe=$(run_cli manga-manager enqueue-probe)
    probe_id=$(printf '%s\n' "$probe" | sed -n 's/^job_id=\([0-9][0-9]*\).*/\1/p')
    attempts=0
    while [ -n "$probe_id" ]; do
      status=$(docker exec "$postgres" psql -U manga -d manga_manager -Atc \
        "SELECT status FROM job WHERE id=$probe_id")
      [ "$status" = succeeded ] && break
      [ "$status" != failed ] && [ "$status" != cancelled ] || exit 1
      attempts=$((attempts + 1)); [ "$attempts" -lt 120 ] || exit 1
      sleep 1
    done
    cover_issues=$(docker exec "$postgres" psql -U manga -d manga_manager -Atc \
      "SELECT count(*) FROM chapter_v2 c JOIN series_v2 s ON s.id=c.series_id WHERE s.status IN ('interested','reading','caught_up','paused') AND (s.kavita_series_id IS NULL OR s.kavita_cover_checksum='' OR c.kavita_cover_checksum<>s.kavita_cover_checksum)")
    [ "${cover_issues:-1}" -eq 0 ] || {
      echo "Kavita cover verification failed for $cover_issues chapters" >&2
      exit 1
    }
    api_key=$(sed -n 's/^KAVITA_API_KEY=//p' "$KAVITA_ENV_FILE")
    [ -n "$api_key" ] || {
      echo "Kavita API key is missing from $KAVITA_ENV_FILE" >&2
      exit 1
    }
    cover_pairs=$(docker exec "$postgres" psql -U manga -d manga_manager -AtF '|' -c \
      "SELECT DISTINCT ON (s.id) s.kavita_series_id,c.kavita_chapter_id
       FROM series_v2 s JOIN chapter_v2 c ON c.series_id=s.id
       WHERE s.status IN ('interested','reading','caught_up','paused')
         AND s.kavita_series_id IS NOT NULL AND c.kavita_chapter_id IS NOT NULL
       ORDER BY s.id,c.id")
    [ -n "$cover_pairs" ] || {
      echo "Kavita did not expose any tracked series/chapter mapping" >&2
      exit 1
    }
    checked_covers=0
    while IFS='|' read -r kavita_series_id kavita_chapter_id; do
      [ -n "$kavita_series_id" ] && [ -n "$kavita_chapter_id" ] || continue
      run_cli python scripts/kavita-cover-check.py \
        --url "http://$kavita_container:5000" --api-key "$api_key" \
        --series-id "$kavita_series_id" --chapter-id "$kavita_chapter_id"
      checked_covers=$((checked_covers + 1))
    done <<EOF
$cover_pairs
EOF
    [ "$checked_covers" -gt 0 ] || {
      echo "Kavita cover endpoint verification did not inspect any mapping" >&2
      exit 1
    }
    echo "kavita_covers=ok pairs=$checked_covers"
    docker exec "$postgres" pg_dump -U manga -d manga_manager -Fc -f /tmp/test-environment.dump
    docker exec "$postgres" dropdb -U manga --if-exists manga_manager_test_restore
    docker exec "$postgres" createdb -U manga manga_manager_test_restore
    docker exec "$postgres" pg_restore --single-transaction -U manga \
      -d manga_manager_test_restore /tmp/test-environment.dump
    docker exec "$postgres" psql -U manga -d manga_manager_test_restore -Atc \
      "SELECT version_num FROM alembic_version"
    docker exec "$postgres" dropdb -U manga manga_manager_test_restore
    if [ "${TEST_ENV_SKIP_BROWSER:-false}" != true ]; then
      (cd frontend && PLAYWRIGHT_BASE_URL="http://127.0.0.1:$port" npm run test:browser)
    fi
    worker_limit=$(docker inspect -f '{{.HostConfig.Memory}}' "$worker")
    [ "$worker_limit" -eq 1073741824 ] || {
      echo "unexpected worker memory limit: $worker_limit" >&2
      exit 1
    }
    docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' "$worker" "$web" "$postgres"
    ;;
  scale-check)
    scale_root=$(safe_test_path "${TEST_SCALE_ROOT:-$PWD/.local/scale-environment}")
    scale_project="${TEST_SCALE_PROJECT:-manga-manager-scale}"
    cleanup_scale() {
      STAGE_PROJECT="$scale_project" STAGE_STORAGE_ROOT="$scale_root/storage" \
        STAGE_STATE_DIR="$scale_root/state" scripts/stage-local.sh down --volumes \
        >/dev/null 2>&1 || true
      rm -rf -- "$scale_root"
    }
    trap cleanup_scale EXIT INT TERM
    STAGE_PROJECT="$scale_project" STAGE_STORAGE_ROOT="$scale_root/storage" \
      STAGE_STATE_DIR="$scale_root/state" STAGE_PORT="${TEST_SCALE_PORT:-18002}" \
      STAGE_ENABLE_SOURCES=false STAGE_MIN_FREE_BYTES=0 scripts/stage-local.sh serve --build
    docker stop "$scale_project-worker" >/dev/null
    worker_limit=$(docker inspect -f '{{.HostConfig.Memory}}' "$scale_project-worker")
    [ "$worker_limit" -eq 1073741824 ] || {
      echo "unexpected scale worker memory limit: $worker_limit" >&2
      exit 1
    }
    scale_series_count="${TEST_SCALE_SERIES_COUNT:-2000}"
    scale_chapter_count="${TEST_SCALE_CHAPTER_COUNT:-0}"
    scale_job_count="${TEST_SCALE_JOB_COUNT:-25000}"
    expected_discovery=$((scale_series_count - (scale_series_count + 4) / 5))
    docker run --rm --network "$scale_project-net" \
      -e V2_DATABASE_URL="postgresql+psycopg://manga:manga@$scale_project-postgres:5432/manga_manager" \
      -e V2_STORAGE_ROOT=/data -e MANGA_MANAGER_ALLOW_TEST_SEED=1 \
      -v "$scale_root/storage:/data" "$scale_project:local" \
      python scripts/seed-test-data.py --profile scale --series-count "$scale_series_count" \
      --chapter-count "$scale_chapter_count" --job-count "$scale_job_count"
    python scripts/verify-scale-api.py --base-url "http://127.0.0.1:${TEST_SCALE_PORT:-18002}" \
      --expected-series "$expected_discovery"
    cleanup_scale
    trap - EXIT INT TERM
    ;;
  performance-check)
    TEST_SCALE_SERIES_COUNT="${TEST_SCALE_SERIES_COUNT:-2000}" \
      TEST_SCALE_CHAPTER_COUNT="${TEST_SCALE_CHAPTER_COUNT:-100000}" \
      TEST_SCALE_JOB_COUNT="${TEST_SCALE_JOB_COUNT:-100000}" \
      "$0" scale-check
    ;;
  validate)
    self=$(realpath "$0")
    cleanup_validation() {
      "$self" down >/dev/null 2>&1 || true
    }
    trap cleanup_validation EXIT INT TERM
    "$self" reset --yes
    "$self" up
    "$self" check
    "$self" scale-check
    "$self" down
    trap - EXIT INT TERM
    echo "small_validation=passed"
    ;;
  down)
    scripts/kavita-local.sh down
    scripts/stage-local.sh down
    ;;
  reset)
    [ "${2:-}" = "--yes" ] || {
      echo "usage: scripts/test-environment.sh reset --yes" >&2
      exit 2
    }
    scripts/kavita-local.sh down
    scripts/stage-local.sh down --volumes
    docker volume rm "$kavita_volume" >/dev/null 2>&1 || true
    remove_test_root
    ;;
  status)
    docker ps -a --filter "name=^/$postgres$" --filter "name=^/$web$" \
      --filter "name=^/$worker$" --filter "name=^/$kavita_container$" \
      --format '{{.Names}} {{.Status}} {{.Ports}}'
    ;;
  *)
    echo "usage: scripts/test-environment.sh up|check|scale-check|performance-check|validate|down|reset --yes|status" >&2
    exit 2
    ;;
esac
