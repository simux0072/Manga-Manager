#!/bin/sh
set -eu

command="${1:-status}"
project="${STAGE_PROJECT:-manga-manager-stage}"
network="${STAGE_NETWORK:-$project-net}"
container="${KAVITA_CONTAINER:-$project-kavita}"
volume="${KAVITA_CONFIG_VOLUME:-$project-kavita-config}"
state_dir="${STAGE_STATE_DIR:-$PWD/.local}"
lock_dir="${STAGE_LOCK_DIR:-$state_dir}"
legacy_env_file="$state_dir/kavita.env"
if [ -n "${KAVITA_ENV_FILE:-}" ]; then
  env_file="$KAVITA_ENV_FILE"
elif [ -f "$legacy_env_file" ]; then
  # Preserve credentials created by older launchers, but isolate newly provisioned
  # environments by project so parallel staging stacks cannot overwrite each other.
  env_file="$legacy_env_file"
else
  env_file="$state_dir/$project-kavita.env"
fi
env_dir=$(dirname "$env_file")
env_name=$(basename "$env_file")
pending_env="$state_dir/$project-kavita-pending.env"
storage="${STAGE_STORAGE_ROOT:-$PWD/storage-v2-stage}"
# Temporary test-only pin: 0.9.0.2 cannot initialize an empty database reliably.
image="${KAVITA_IMAGE:-jvmilazz0/kavita:0.8.9}"
kavita_bind_address="${KAVITA_BIND_ADDRESS:-${STAGE_BIND_ADDRESS:-0.0.0.0}}"

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
  reset-config)
    [ "${2:-}" = "--yes" ] || {
      echo "usage: scripts/kavita-local.sh reset-config --yes" >&2
      echo "This removes only the disposable Kavita config volume and local credentials." >&2
      exit 2
    }
    docker rm -f "$container" >/dev/null 2>&1 || true
    docker volume rm "$volume" >/dev/null 2>&1 || true
    rm -f "$env_file" "$pending_env"
    exit 0
    ;;
  up) ;;
  *) echo "usage: scripts/kavita-local.sh up|down|status|credentials|reset-config --yes" >&2; exit 2 ;;
esac

mkdir -p "$state_dir" "$lock_dir" "$env_dir" "$storage/kavita-library"
chmod 700 "$state_dir"
exec 8>"$lock_dir/$project-kavita-local.lock"
if ! flock -n 8; then
  echo "another Kavita provisioning operation is already running" >&2
  exit 75
fi
docker network inspect "$network" >/dev/null 2>&1 || docker network create "$network" >/dev/null
docker volume inspect "$volume" >/dev/null 2>&1 || docker volume create "$volume" >/dev/null
app_image="$project:local"
# Finish the I/O-heavy application build before Kavita initializes SQLite on slower disks.
if [ "${KAVITA_BUILD:-false}" = "true" ] || ! docker image inspect "$app_image" >/dev/null 2>&1; then
  docker build -t "$app_image" .
fi
expected_mount=$(cd "$storage/kavita-library" && pwd)

start_kavita() {
  if docker container inspect "$container" >/dev/null 2>&1; then
    current_mount=$(docker inspect -f \
      '{{range .Mounts}}{{if eq .Destination "/manga"}}{{.Source}}{{end}}{{end}}' \
      "$container")
    current_image=$(docker inspect -f '{{.Config.Image}}' "$container")
    current_bind=$(docker inspect -f \
      '{{(index (index .HostConfig.PortBindings "5000/tcp") 0).HostIp}}' "$container")
    [ -n "$current_bind" ] || current_bind=0.0.0.0
    if [ "$current_mount" != "$expected_mount" ] || [ "$current_image" != "$image" ] || \
      [ "$current_bind" != "$kavita_bind_address" ]; then
      docker rm -f "$container" >/dev/null
    else
      docker start "$container" >/dev/null
    fi
  fi
  if ! docker container inspect "$container" >/dev/null 2>&1; then
    docker run -d --name "$container" --network "$network" \
      -p "$kavita_bind_address:${KAVITA_PORT:-15000}:5000" \
      --log-opt max-size=10m --log-opt max-file=3 \
      -e "TZ=${TZ:-UTC}" -v "$volume:/kavita/config" -v "$storage/kavita-library:/manga:ro" \
      --restart unless-stopped "$image" >/dev/null
  fi
}

start_kavita

username=manga-manager-local
password=""
api_key=""
had_credentials=false
if [ -f "$env_file" ]; then
  had_credentials=true
  password=$(sed -n 's/^KAVITA_PASSWORD=//p' "$env_file")
  api_key=$(sed -n 's/^KAVITA_API_KEY=//p' "$env_file")
elif [ -f "$pending_env" ]; then
  password=$(sed -n 's/^KAVITA_PASSWORD=//p' "$pending_env")
fi
[ -n "$password" ] || password=$(od -An -N18 -tx1 /dev/urandom | tr -d ' \n')
if [ ! -f "$env_file" ]; then
  (umask 077; printf 'KAVITA_PASSWORD=%s\n' "$password" >"$pending_env")
fi

provision_kavita() {
  docker run --rm --user "$(id -u):$(id -g)" --network "$network" -v "$env_dir:/state" \
    -v "$PWD/scripts/kavita-e2e-setup.py:/app/scripts/kavita-e2e-setup.py:ro" \
    -e UV_CACHE_DIR=/tmp/uv-cache \
    -e KAVITA_E2E_URL="http://$container:5000" -e KAVITA_E2E_USERNAME="$username" \
    -e KAVITA_E2E_PASSWORD="$password" -e KAVITA_E2E_API_KEY="$api_key" \
    -e "KAVITA_ENV_OUTPUT=/state/$env_name" \
    -e "KAVITA_WAIT_SECONDS=${KAVITA_WAIT_SECONDS:-900}" "$app_image" \
    /app/.venv/bin/python scripts/kavita-e2e-setup.py >/dev/null
}

set +e
provision_kavita
provision_status=$?
set -e
if [ "$provision_status" -eq 42 ] && [ "$had_credentials" = false ]; then
  echo "Kavita credentials were lost while its disposable config volume remained; recreating only that volume." >&2
  docker rm -f "$container" >/dev/null 2>&1 || true
  docker volume rm "$volume" >/dev/null 2>&1 || true
  docker volume create "$volume" >/dev/null
  start_kavita
  provision_kavita
elif [ "$provision_status" -ne 0 ]; then
  exit "$provision_status"
fi
rm -f "$pending_env"
set -a
. "$env_file"
set +a
export KAVITA_URL="http://$container:5000"
export KAVITA_LIBRARY_ROOT=/manga
scripts/stage-local.sh serve
lan_ip=$(hostname -I 2>/dev/null | awk '{print $1}' || true)
[ -n "$lan_ip" ] || lan_ip='<host-ip>'
printf '%s\n' "Manga Manager (this computer): http://127.0.0.1:${STAGE_PORT:-18000}" \
  "Manga Manager (local network): http://$lan_ip:${STAGE_PORT:-18000}" \
  "Kavita (this computer): http://127.0.0.1:${KAVITA_PORT:-15000}" \
  "Kavita (local network): http://$lan_ip:${KAVITA_PORT:-15000}" \
  "Credentials: scripts/kavita-local.sh credentials"
