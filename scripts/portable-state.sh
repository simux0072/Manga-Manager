#!/bin/sh
set -eu

command="${1:-}"
path="${2:-}"
confirmation="${3:-}"
project="${STAGE_PROJECT:-manga-manager-stage}"
worker="${STAGE_WORKER_CONTAINER:-$project-worker}"

usage() {
  echo "usage: scripts/portable-state.sh export OUTPUT | preview INPUT | import INPUT --yes" >&2
  exit 2
}

[ -n "$command" ] && [ -n "$path" ] || usage
case "$command" in
  export|preview|import) ;;
  *) usage ;;
esac
if [ "$command" = "import" ] && [ "$confirmation" != "--yes" ]; then
  echo "import queues fresh catalog refreshes and downloads; append --yes to continue" >&2
  exit 2
fi
docker inspect "$worker" >/dev/null 2>&1 || {
  echo "worker container is not available: $worker" >&2
  exit 1
}

temporary="/tmp/manga-manager-portable-state-$$.json"
cleanup() {
  docker exec "$worker" rm -f "$temporary" >/dev/null 2>&1 || true
}
trap cleanup EXIT HUP INT TERM

case "$command" in
  export)
    output_dir=$(dirname "$path")
    mkdir -p "$output_dir"
    docker exec "$worker" manga-manager export-portable-state "$temporary"
    docker cp "$worker:$temporary" "$path" >/dev/null
    chmod 600 "$path"
    echo "portable state written to $path"
    ;;
  preview)
    [ -f "$path" ] || { echo "portable state does not exist: $path" >&2; exit 1; }
    docker cp "$path" "$worker:$temporary" >/dev/null
    docker exec "$worker" manga-manager import-portable-state "$temporary"
    ;;
  import)
    [ -f "$path" ] || { echo "portable state does not exist: $path" >&2; exit 1; }
    docker cp "$path" "$worker:$temporary" >/dev/null
    docker exec "$worker" manga-manager import-portable-state "$temporary" --apply
    ;;
esac
