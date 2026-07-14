#!/usr/bin/env bash
set -Eeuo pipefail

root=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"

mkdir -p "$root/.local"
exec 9>"$root/.local/final-validation.lock"
if ! flock -n 9; then
  printf 'Another final-validation run is already active. Exiting without changing its resources.\n' >&2
  exit 75
fi

log_dir="${VALIDATION_LOG_DIR:-$root/logs}"
project="${VALIDATION_STAGE_PROJECT:-manga-manager-plan-check}"
storage="${VALIDATION_STAGE_STORAGE:-/tmp/manga-manager-plan-check}"
port="${VALIDATION_STAGE_PORT:-18002}"
arm_image="${VALIDATION_ARM_IMAGE:-manga-manager:arm64-final}"
mkdir -p "$log_dir"

timestamp=$(date -u +%Y%m%dT%H%M%SZ)
stage_log="$log_dir/final-isolated-staging-$timestamp.log"
arm_log="$log_dir/final-arm64-$timestamp.log"
summary_log="$log_dir/final-validation-$timestamp.summary"

stage_status=0
arm_status=0

printf 'Starting isolated staging rehearsal. Log: %s\n' "$stage_log"
set +e
STAGE_PROJECT="$project" \
STAGE_STORAGE_ROOT="$storage" \
STAGE_PORT="$port" \
STAGE_PLATFORM=linux/amd64 \
scripts/stage-local.sh 2>&1 | tee "$stage_log"
stage_status=${PIPESTATUS[0]}
set -e

if (( stage_status != 0 )); then
  {
    printf '\n--- isolated container diagnostics ---\n'
    for container in "$project-postgres" "$project-web" "$project-worker"; do
      if docker container inspect "$container" >/dev/null 2>&1; then
        printf '\n### %s\n' "$container"
        docker logs --tail 500 "$container" 2>&1 || true
      fi
    done
  } >>"$stage_log"
fi

# The project name is unique to this validation run. Remove its containers/network/volume even
# after failure so an unattended run cannot leave services consuming memory indefinitely.
STAGE_PROJECT="$project" scripts/stage-local.sh down --volumes >>"$stage_log" 2>&1 || true

printf 'Starting ARM64 build and runtime check. Log: %s\n' "$arm_log"
set +e
{
  docker buildx build --platform linux/arm64 --load --tag "$arm_image" .
  docker image inspect "$arm_image" --format 'architecture={{.Architecture}} os={{.Os}}'
  docker run --rm --platform linux/arm64 "$arm_image" \
    manga-manager --help
} 2>&1 | tee "$arm_log"
arm_status=${PIPESTATUS[0]}
set -e

{
  printf 'timestamp=%s\n' "$timestamp"
  printf 'stage_status=%s\n' "$stage_status"
  printf 'arm_status=%s\n' "$arm_status"
  printf 'stage_log=%s\n' "$stage_log"
  printf 'arm_log=%s\n' "$arm_log"
} | tee "$summary_log"

if (( stage_status != 0 || arm_status != 0 )); then
  printf 'Validation finished with failures. See %s\n' "$summary_log" >&2
  exit 1
fi

printf 'All unattended validation checks passed. Summary: %s\n' "$summary_log"
