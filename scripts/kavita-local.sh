#!/bin/sh
set -eu

command="${1:-status}"
project="${STAGE_PROJECT:-manga-manager-stage}"
network="${STAGE_NETWORK:-$project-net}"
container="${KAVITA_CONTAINER:-$project-kavita}"
volume="${KAVITA_CONFIG_VOLUME:-$project-kavita-config}"
state_dir="${STAGE_STATE_DIR:-$PWD/.local}"
env_file="${KAVITA_ENV_FILE:-$state_dir/kavita.env}"
env_dir=$(dirname "$env_file")
env_name=$(basename "$env_file")
storage="${STAGE_STORAGE_ROOT:-$PWD/storage-v2-stage}"
image="${KAVITA_IMAGE:-jvmilazz0/kavita:0.9.0.2}"

case "$command" in
  down)
    docker rm -f "$container" >/dev/null 2>&1 || true
    exit 0
    ;;
  status)
    docker ps -a --filter "name=^/$container$" --format '{{.Names}} {{.Status}} {{.Ports}}'
    exit 0
    ;;
  credentials)
    [ -f "$env_file" ] || { echo "Kavita has not been provisioned; run: scripts/kavita-local.sh up" >&2; exit 1; }
    cat "$env_file"
    exit 0
    ;;
  up) ;;
  *) echo "usage: scripts/kavita-local.sh up|down|status|credentials" >&2; exit 2 ;;
esac

mkdir -p "$state_dir" "$env_dir" "$storage/kavita-library"
chmod 700 "$state_dir"
exec 8>"$state_dir/$project-kavita-local.lock"
if ! flock -n 8; then
  echo "another Kavita provisioning operation is already running" >&2
  exit 75
fi
docker network inspect "$network" >/dev/null 2>&1 || docker network create "$network" >/dev/null
docker volume inspect "$volume" >/dev/null 2>&1 || docker volume create "$volume" >/dev/null
expected_mount=$(cd "$storage/kavita-library" && pwd)
if docker container inspect "$container" >/dev/null 2>&1; then
  current_mount=$(docker inspect -f \
    '{{range .Mounts}}{{if eq .Destination "/manga"}}{{.Source}}{{end}}{{end}}' \
    "$container")
  if [ "$current_mount" != "$expected_mount" ]; then
    docker rm -f "$container" >/dev/null
  else
    docker start "$container" >/dev/null
  fi
fi
if ! docker container inspect "$container" >/dev/null 2>&1; then
  docker run -d --name "$container" --network "$network" -p "${KAVITA_PORT:-15000}:5000" \
    --log-opt max-size=10m --log-opt max-file=3 \
    -e "TZ=${TZ:-UTC}" -v "$volume:/kavita/config" -v "$storage/kavita-library:/manga:ro" \
    --restart unless-stopped "$image" >/dev/null
fi

username=manga-manager-local
password=""
api_key=""
if [ -f "$env_file" ]; then
  password=$(sed -n 's/^KAVITA_PASSWORD=//p' "$env_file")
  api_key=$(sed -n 's/^KAVITA_API_KEY=//p' "$env_file")
fi
[ -n "$password" ] || password=$(od -An -N18 -tx1 /dev/urandom | tr -d ' \n')

app_image="$project:local"
# Provisioning and the staged service must use the current checkout, not a stale local tag.
docker build -t "$app_image" .
docker run --rm --user "$(id -u):$(id -g)" --network "$network" -v "$env_dir:/state" \
  -v "$PWD/scripts/kavita-e2e-setup.py:/app/scripts/kavita-e2e-setup.py:ro" \
  -e UV_CACHE_DIR=/tmp/uv-cache \
  -e KAVITA_E2E_URL="http://$container:5000" -e KAVITA_E2E_USERNAME="$username" \
  -e KAVITA_E2E_PASSWORD="$password" -e KAVITA_E2E_API_KEY="$api_key" \
  -e "KAVITA_ENV_OUTPUT=/state/$env_name" "$app_image" \
  /app/.venv/bin/python scripts/kavita-e2e-setup.py >/dev/null
set -a
. "$env_file"
set +a
export KAVITA_URL="http://$container:5000"
export KAVITA_LIBRARY_ROOT=/manga
scripts/stage-local.sh serve
printf '%s\n' "Manga Manager: http://127.0.0.1:${STAGE_PORT:-18000}" \
  "Kavita: http://127.0.0.1:${KAVITA_PORT:-15000}" \
  "Credentials: scripts/kavita-local.sh credentials"
